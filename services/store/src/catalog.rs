//! The catalog: what datasets the store's volumes hold, which days each
//! covers, how much space they use, and how full each volume is. It only
//! reads. It recognises the store layout
//! (`<Provider>/<DATASET>/[<symbol>/]<schema>/<YYYY-MM-DD>.dbn.zst`, which
//! follows Databento's file naming) and Databento batch downloads (folders
//! holding `metadata.json` and `manifest.json`). Every other DBN file on the
//! volume is counted from its own header, as held but not filed, so no data
//! on a volume goes uncounted (decision 0019).
//!
//! Each dataset belongs to a provider, named for the volume's top-level folder
//! it sits in (case ignored: `Databento/` and `databento/` are one provider),
//! and is listed by provider, id and symbol, never merged across providers
//! (decision 0019). Data outside every provider's folder (a batch download
//! at the top of a volume, a backup, a recording) has no provider. A folder
//! that can't be read is an error of the volume, not an empty folder.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::SystemTime;

use dbn::decode::DbnMetadata as _;

use serde_json::{Value, json};

#[derive(Clone, Copy)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct Thresholds {
    pub warning: f64,
    pub critical: f64,
    pub floor: f64,
}

pub fn severity(status: &str) -> u8 {
    match status {
        "warning" => 1,
        "critical" => 2,
        "floor" => 3,
        _ => 0,
    }
}

/// One root: `{ name, path, present, volume: { total_bytes, available_bytes,
/// used_fraction, status }, datasets: [{ provider, id, symbol, filed, stray, sources,
/// schemas: { schema: { files, bytes, symbols, days: { "YYYY-MM-DD": bytes },
/// partial, unfiled } } }], dataset_bytes, unrecognised: [names], errors,
/// pending_files }`.
#[cfg(test)]
pub fn scan_root(name: &str, path: &Path, t: Thresholds) -> Value {
    scan_volume(name, path, t, &[]).0
}

/// A served file found by a scan: its path within the volume, and its size.
pub type Served = (String, u64);

/// As [`scan_root`], skipping the `ignored` paths, with every place found
/// (the index `/coverage` answers from) and every file the store serves,
/// sorted (the index `/files` answers from).
pub fn scan_volume(
    name: &str,
    path: &Path,
    t: Thresholds,
    ignored: &[PathBuf],
) -> (Value, Vec<Place>, Vec<Served>) {
    scan_tree(name, path, t, ignored, false)
}

/// A content-addressed volume's view (decision 0035), read exactly as a
/// volume in the store layout is: the view is its names, each a link to its
/// object, so what the store says of a migrated volume is what it said of
/// the tree it came from. Its days' places are marked `cas` (nothing is
/// written beside them).
pub fn scan_view(name: &str, view: &Path, t: Thresholds) -> (Value, Vec<Place>, Vec<Served>) {
    let (root, mut places, served) = scan_tree(name, view, t, &[], true);
    for p in &mut places {
        if let Place::Days { cas, .. } = p {
            *cas = true;
        }
    }
    (root, places, served)
}

fn scan_tree(
    name: &str,
    path: &Path,
    t: Thresholds,
    ignored: &[PathBuf],
    follow: bool,
) -> (Value, Vec<Place>, Vec<Served>) {
    if !path.is_dir() {
        let root = json!({ "name": name, "path": path, "present": false, "datasets": [], "unrecognised": [], "errors": [], "pending_files": [] });
        return (root, Vec::new(), Vec::new());
    }
    let volume = match shared_folder_filesystem(path) {
        // Folders shared into a container by Docker Desktop report the
        // virtual machine's disk, not the drive they live on.
        Some(fs) => json!({
            "status": "unknown",
            "reason": format!("disk space is not visible through a {fs} mount (Docker Desktop); run the catalog natively to see it"),
        }),
        None => volume(path, t),
    };

    let mut scan = Scan {
        skip: ignored.iter().cloned().collect(),
        root: path.to_path_buf(),
        follow,
        ..Scan::default()
    };
    let mut unrecognised = Vec::new();
    let mut other = Vec::new();
    for entry in listing(path, true, &mut scan) {
        let found = top(&entry, &mut scan);
        if !found {
            let name = file_name(&entry);
            let u = usage_with(&entry, scan.follow);
            other.push(
                json!({ "name": name, "bytes": u.bytes, "files": u.files, "modified": u.modified }),
            );
            unrecognised.push(name);
        }
    }
    let Scan {
        datasets,
        errors,
        pending,
        places,
        foreign,
        unsettled,
        mut served,
        ..
    } = scan;
    served.sort();
    served.dedup();
    // Counted as space used, never as data held (once: a top-level folder
    // holding nothing else is already counted whole).
    for ((folder, id), u) in foreign {
        let top = Path::new(&folder).components().next();
        if top.is_some_and(|t| {
            unrecognised
                .iter()
                .any(|u| Path::new(u).as_os_str() == t.as_os_str())
        }) {
            continue;
        }
        other.push(json!({ "name": folder, "dataset": id, "bytes": u.bytes, "files": u.files, "modified": u.modified }));
    }
    let errors = described(errors, name);
    let other_bytes: u64 = other.iter().filter_map(|o| o["bytes"].as_u64()).sum();
    let dataset_bytes: u64 = datasets
        .values()
        .flat_map(|d| d.schemas.values())
        .map(|s| s.bytes)
        .sum();
    let root = json!({
        "name": name,
        "path": path,
        "present": true,
        "volume": volume,
        "datasets": datasets.iter().map(|(k, d)| d.to_json(k)).collect::<Vec<_>>(),
        "dataset_bytes": dataset_bytes,
        "other": other,
        "other_bytes": other_bytes,
        "unrecognised": unrecognised,
        "errors": errors,
        "pending_files": pending,
        "pending_merges": merges_json(&unsettled),
    });
    (root, places, served)
}

fn merges_json(unsettled: &[(PathBuf, String)]) -> Value {
    json!(
        unsettled
            .iter()
            .map(|(dir, day)| json!({ "folder": dir, "day": day }))
            .collect::<Vec<_>>()
    )
}

/// One schema folder in the store layout, read again after the store wrote to
/// it: what a full scan would find there, to update a scan's answer in place
/// (a filing, an import, a merge or a settled one) without scanning the
/// volume again.
pub struct Folder {
    dir: PathBuf,
    dataset_dir: PathBuf,
    key: Key,
    schema: String,
    stats: Option<Schema>,
    place: Option<Place>,
    /// The folder within the volume, and the served files of its days.
    within: String,
    served: Vec<Served>,
    errors: Vec<(PathBuf, String)>,
    unsettled: Vec<(PathBuf, String)>,
}

impl Folder {
    /// Read the schema folder `dir` (`<Provider>/<DATASET>/[<symbol>/]<schema>`)
    /// of the volume at `volume`; `None` if it isn't one.
    #[must_use]
    pub fn read(volume: &Path, dir: &Path) -> Option<Self> {
        let rel = dir.strip_prefix(volume).ok()?;
        let parts: Vec<String> = rel
            .components()
            .map(|c| c.as_os_str().to_string_lossy().into_owned())
            .collect();
        let at = parts
            .iter()
            .position(|p| is_dataset_name(p))
            .filter(|&i| i <= 1)?;
        let provider = if at == 1 {
            provider_name(&parts[0])
        } else {
            None
        };
        let (symbol, schema) = match &parts[at + 1..] {
            [schema] => (None, schema.clone()),
            [symbol, schema] => (Some(symbol.as_str()), schema.clone()),
            _ => return None,
        };
        let key = key(provider.as_deref(), &parts[at], symbol);
        let mut scan = Scan {
            root: volume.to_path_buf(),
            ..Scan::default()
        };
        add_days(dir, &key, &schema, &mut scan);
        let stats = scan
            .datasets
            .remove(&key)
            .and_then(|mut d| d.schemas.remove(&schema));
        Some(Self {
            dir: dir.to_path_buf(),
            dataset_dir: volume.join(parts[..=at].iter().collect::<PathBuf>()),
            key,
            schema,
            stats,
            place: scan.places.pop(),
            within: rel.to_string_lossy().into_owned(),
            served: scan.served,
            errors: scan.errors,
            unsettled: scan.unsettled,
        })
    }

    /// Put what the folder holds now in a scan's places of its volume.
    pub fn apply_places(&self, places: &mut Vec<Place>) {
        places.retain(|p| !matches!(p, Place::Days { dir, .. } if *dir == self.dir));
        places.extend(self.place.clone());
    }

    /// Put the served files of its days now in a scan's list of its
    /// volume's (sorted), in place of those it had.
    pub fn apply_files(&self, files: &mut Vec<Served>) {
        files.retain(|(f, _)| {
            let day = f
                .strip_prefix(&self.within)
                .and_then(|r| r.strip_prefix('/'))
                .and_then(|r| r.get(..10));
            !day.is_some_and(is_iso_date)
        });
        files.extend(self.served.iter().cloned());
        files.sort();
        files.dedup();
    }

    /// Put what the folder holds now in a scan's root of its volume: its days
    /// in its dataset's schema, beside what other sources give it.
    pub fn apply_root(&self, root: &mut Value, volume: &str) {
        let pending = root["pending_merges"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        let mut merges: Vec<Value> = pending
            .into_iter()
            .filter(|m| m["folder"] != json!(self.dir))
            .collect();
        merges.extend(
            merges_json(&self.unsettled)
                .as_array()
                .into_iter()
                .flatten()
                .cloned(),
        );
        root["pending_merges"] = json!(merges);
        if let Some(errors) = root["errors"].as_array_mut() {
            for e in described(self.errors.clone(), volume) {
                if !errors.contains(&json!(e)) {
                    errors.push(json!(e));
                }
            }
        }
        let Some(stats) = &self.stats else {
            return;
        };
        let (provider, id, symbol) = &self.key;
        let Some(datasets) = root["datasets"].as_array_mut() else {
            return;
        };
        let at = datasets.iter().position(|d| {
            d["provider"] == json!(provider)
                && d["id"] == id.as_str()
                && d["symbol"] == json!(symbol)
        });
        let dataset = match at {
            Some(i) => &mut datasets[i],
            None => {
                datasets.push(
                    json!({ "provider": provider, "id": id, "symbol": symbol, "filed": true, "stray": false,
                    "sources": [format!("store: {}", self.dataset_dir.display())], "schemas": {} }),
                );
                datasets.last_mut().expect("just pushed")
            }
        };
        let folder = stats.to_json();
        let bytes_delta = match dataset["schemas"].get_mut(&self.schema) {
            None => {
                dataset["schemas"][&self.schema] = folder;
                i64::try_from(stats.bytes).unwrap_or(i64::MAX)
            }
            Some(old) => merge_schema(old, stats),
        };
        let filed = dataset["schemas"].as_object().is_none_or(|o| {
            o.values()
                .all(|s| s["unfiled"].as_array().is_none_or(Vec::is_empty))
        });
        dataset["filed"] = json!(filed);
        root["dataset_bytes"] = json!(
            root["dataset_bytes"]
                .as_i64()
                .unwrap_or(0)
                .saturating_add(bytes_delta)
                .max(0)
        );
    }
}

/// Merge a folder's days into a schema's: the folder's are as it holds them
/// now, the other days (from other sources) as they were. Returns the bytes
/// added (less, if a day shrank).
fn merge_schema(old: &mut Value, f: &Schema) -> i64 {
    let mut added = 0i64;
    let mut new_days = 0u64;
    for (day, size) in &f.days {
        let was = old["days"][day].as_i64();
        if was.is_none() {
            new_days += 1;
        }
        added += i64::try_from(*size).unwrap_or(i64::MAX) - was.unwrap_or(0);
        old["days"][day] = json!(size);
    }
    let ours = |d: &Value| d.as_str().is_some_and(|d| f.days.contains_key(d));
    for (list, now) in [
        ("partial", &f.partial),
        ("degraded", &f.degraded),
        ("missing", &f.missing),
        ("unfiled", &f.unfiled),
    ] {
        let mut kept: Vec<Value> = old[list]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|d| !ours(d))
            .collect();
        kept.extend(now.iter().map(|d| json!(d)));
        old[list] = json!(kept);
    }
    for e in &f.encodings {
        if let Some(list) = old["encodings"].as_array_mut()
            && !list.contains(&json!(e))
        {
            list.push(json!(e));
        }
    }
    old["files"] = json!(old["files"].as_u64().unwrap_or(0) + new_days);
    old["bytes"] = json!(
        old["bytes"]
            .as_i64()
            .unwrap_or(0)
            .saturating_add(added)
            .max(0)
    );
    added
}

/// Each error, once, with how to skip its path if it should be: an operator
/// can ignore a path the store can't read (`POST /volumes/{name}/ignore`).
fn described(mut errors: Vec<(PathBuf, String)>, volume: &str) -> Vec<String> {
    errors.sort();
    errors.dedup();
    errors
        .into_iter()
        .map(|(path, e)| {
            format!(
                "{e} (to skip {0} for good: POST /volumes/{volume}/ignore with {{\"path\": \"{0}\", \"reason\": \"…\"}})",
                path.display()
            )
        })
        .collect()
}

/// The store's name for the provider whose top-level folder this is, case
/// ignored (`Databento` and `databento` are both `databento`).
#[must_use]
pub fn provider_name(folder: &str) -> Option<String> {
    crate::ingest::provider_dir(folder).map(str::to_ascii_lowercase)
}

/// A schema folder's day entries (or why its index couldn't be read), each
/// day's or why its sidecar couldn't be.
pub type Entries = std::sync::Arc<Result<BTreeMap<String, Result<Value, String>>, String>>;

/// Where data was found on a volume, with the provider it belongs to: a
/// dataset in the store layout, a provider's batch download (or a download
/// folder without its manifest, by its query), or any other DBN file, by its
/// header.
#[derive(Clone)]
pub enum Place {
    /// A schema folder in the store layout, with its day files (or older
    /// day folders, not filed) and their sizes, and each day's served files
    /// (paths within the volume; a day folder's data first).
    Days {
        provider: Option<String>,
        dataset: String,
        symbol: Option<String>,
        schema: String,
        dir: PathBuf,
        days: Vec<(String, u64, bool)>,
        /// The manifest's entry of each filed day that has one, as
        /// `/coverage` reports it, read when the folder was: `/coverage`
        /// answers from memory, and every write to the folder reads it again.
        entries: Entries,
        files: BTreeMap<String, Vec<String>>,
        /// Read from a content-addressed volume's index (decision 0035):
        /// `dir` is only its name, and nothing writes there.
        cas: bool,
    },
    /// A batch download, with the folder it is in (within the volume).
    Batch {
        provider: Option<String>,
        batch: Batch,
        dir: String,
    },
    /// Any other DBN file, with its path within the volume.
    Loose {
        provider: Option<String>,
        header: Header,
        size: u64,
        path: String,
    },
}

/// Every place on the volume at `root` holding datasets, found as the
/// catalog finds them.
///
/// # Errors
/// If a folder can't be read, or a manifest, a batch's metadata or a DBN
/// file's header is unreadable.
#[cfg(test)]
pub fn places(root: &Path) -> Result<Vec<Place>, String> {
    let (r, places, _) = scan_volume(
        "v",
        root,
        Thresholds {
            warning: 100.0,
            critical: 100.0,
            floor: 100.0,
        },
        &[],
    );
    match r["errors"][0].as_str() {
        Some(e) => Err(e.to_owned()),
        None => Ok(places),
    }
}

/// Look for datasets in a top-level entry of a volume: the layouts first,
/// then every DBN file they didn't claim. Returns whether any were found.
fn top(entry: &Path, scan: &mut Scan) -> bool {
    scan.top = entry.to_path_buf();
    let provider = provider_name(&file_name(entry));
    // The store's own records in a provider's folder hold no data.
    if provider.is_some() {
        for own in OWN {
            scan.skip.insert(entry.join(own));
        }
    }
    let found = visit(entry, 0, provider.as_deref(), scan);
    loose(entry, provider.as_deref(), scan) || found
}

/// Space on the volume holding `path`, classified against the thresholds.
pub fn volume(path: &Path, t: Thresholds) -> Value {
    let total = fs4::total_space(path).unwrap_or(0);
    let available = fs4::available_space(path).unwrap_or(0);
    let used = if total > 0 {
        1.0 - available as f64 / total as f64
    } else {
        0.0
    };
    let pct = used * 100.0;
    let status = if pct >= t.floor {
        "floor"
    } else if pct >= t.critical {
        "critical"
    } else if pct >= t.warning {
        "warning"
    } else {
        "ok"
    };
    json!({ "total_bytes": total, "available_bytes": available, "used_fraction": used, "status": status })
}

/// The filesystem type of `path`'s mount when it is a host folder shared
/// into a container (where space figures describe something else), read
/// from `/proc/mounts`. `None` elsewhere, including on macOS.
fn shared_folder_filesystem(path: &Path) -> Option<String> {
    const SHARED: [&str; 5] = ["fakeowner", "virtiofs", "grpcfuse", "osxfs", "9p"];
    let mounts = std::fs::read_to_string("/proc/mounts").ok()?;
    let path = path.canonicalize().ok()?;
    mounts
        .lines()
        .filter_map(|l| {
            let mut f = l.split_whitespace();
            let (_, point, fs) = (f.next()?, f.next()?, f.next()?);
            path.starts_with(point)
                .then(|| (point.len(), fs.to_owned()))
        })
        .max_by_key(|(len, _)| *len)
        .map(|(_, fs)| fs)
        .filter(|fs| SHARED.contains(&fs.as_str()))
}

#[derive(Default)]
struct Dataset {
    sources: Vec<String>,
    schemas: BTreeMap<String, Schema>,
}

/// The key a dataset is listed under: its provider, its id, and its symbol
/// when the layout has a symbol level.
type Key = (Option<String>, String, Option<String>);

fn key(provider: Option<&str>, id: &str, symbol: Option<&str>) -> Key {
    (
        provider.map(str::to_owned),
        id.to_owned(),
        symbol.map(str::to_owned),
    )
}

/// What a walk of a volume found so far.
#[derive(Default)]
struct Scan {
    datasets: BTreeMap<Key, Dataset>,
    /// Folders, manifests, metadata or headers that couldn't be read, by path.
    errors: Vec<(PathBuf, String)>,
    /// Paths never counted: the store's own folders, and those an operator
    /// ignores.
    skip: HashSet<PathBuf>,
    places: Vec<Place>,
    /// Files and folders the layouts have counted.
    claimed: HashSet<PathBuf>,
    /// DBN files still being written, read again on the next scan.
    pending: Vec<String>,
    /// Links are followed: a content-addressed volume's view (its names,
    /// each a link to its object), read as a tree is.
    follow: bool,
    /// The volume, and the top-level entry being read.
    root: PathBuf,
    top: PathBuf,
    /// DBN files that hold no dataset (another program's recordings, an
    /// index), by folder and the id their header gives.
    foreign: BTreeMap<(String, String), Usage>,
    /// Days with a merge left pending (folder, day), for the store to settle.
    unsettled: Vec<(PathBuf, String)>,
    /// Every file the store serves, read or not.
    served: Vec<Served>,
}

/// Folders the store writes in a provider's folder: receipts, its ledger
/// and partial days a complete day superseded, none of it data held.
/// (Uploads wait in the hidden `.incoming`.)
const OWN: [&str; 3] = ["provenance", "migrations", SUPERSEDED];
/// Where partial days a complete day superseded are kept (decision 0018).
pub(crate) const SUPERSEDED: &str = "superseded";

impl Scan {
    fn fail(&mut self, path: &Path, e: String) {
        self.errors.push((path.to_path_buf(), e));
    }

    /// A path's metadata (following links), or `None` when it isn't there. Any
    /// other failure (an I/O error, a drive dropping out) is the scan's error,
    /// never "not there": a scan with errors doesn't change what the volume is
    /// remembered to hold (P0, 2026-11-10).
    fn stat(&mut self, path: &Path) -> Option<std::fs::Metadata> {
        match std::fs::metadata(path) {
            Ok(m) => Some(m),
            // In a content-addressed volume's view, a link to an object
            // that isn't there is a missing object, never "not there"
            // (review of #160).
            Err(e)
                if e.kind() == std::io::ErrorKind::NotFound
                    && self.follow
                    && std::fs::symlink_metadata(path).is_ok() =>
            {
                self.fail(path, "its object is missing".into());
                None
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => {
                self.fail(path, e.to_string());
                None
            }
        }
    }

    /// As [`Scan::stat`], not following a link.
    fn lstat(&mut self, path: &Path) -> Option<std::fs::Metadata> {
        match std::fs::symlink_metadata(path) {
            Ok(m) => Some(m),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => {
                self.fail(path, e.to_string());
                None
            }
        }
    }

    fn is_dir(&mut self, path: &Path) -> bool {
        self.stat(path).is_some_and(|m| m.is_dir())
    }

    fn is_file(&mut self, path: &Path) -> bool {
        self.stat(path).is_some_and(|m| m.is_file())
    }

    /// `path` within the volume, as `/files` names it.
    fn within(&self, path: &Path) -> String {
        path.strip_prefix(&self.root)
            .unwrap_or(path)
            .to_string_lossy()
            .into_owned()
    }
}

/// How recently a file whose header can't be read must have changed to be
/// taken as still being written.
const WRITING: std::time::Duration = std::time::Duration::from_secs(120);

/// A schema's files, days and bytes, as the catalog lists them.
#[derive(Default)]
#[cfg_attr(test, derive(schemars::JsonSchema), schemars(rename = "SchemaHeld"))]
pub struct Schema {
    files: u64,
    bytes: u64,
    symbols: Vec<String>,
    encodings: Vec<String>,
    days: BTreeMap<String, u64>,
    /// Days holding only some contracts (decision 0018); the rest are complete.
    partial: Vec<String>,
    /// Days not in the store layout: in a batch download, or a day folder.
    unfiled: Vec<String>,
    /// Days the provider calls degraded, or missing (their condition).
    degraded: Vec<String>,
    missing: Vec<String>,
}

impl Schema {
    fn to_json(&self) -> Value {
        json!({ "files": self.files, "bytes": self.bytes, "symbols": self.symbols, "encodings": self.encodings, "days": self.days, "partial": self.partial, "unfiled": self.unfiled, "degraded": self.degraded, "missing": self.missing })
    }
}

impl Dataset {
    fn to_json(&self, (provider, id, symbol): &Key) -> Value {
        let schemas: serde_json::Map<String, Value> = self
            .schemas
            .iter()
            .map(|(k, s)| (k.clone(), s.to_json()))
            .collect();
        let filed = self.schemas.values().all(|s| s.unfiled.is_empty());
        // Held only in DBN files outside every provider's folder and every
        // layout: scratch output, a shadow run's, a recording. Counted, as
        // all data is, but not a dataset the store keeps.
        let stray = provider.is_none() && self.sources.iter().all(|s| s.starts_with("files in "));
        json!({ "provider": provider, "id": id, "symbol": symbol, "filed": filed, "stray": stray, "sources": self.sources, "schemas": schemas })
    }
}

/// Look for datasets under `dir`, in `provider`'s folder if any; returns
/// whether any were found.
fn visit(dir: &Path, depth: usize, provider: Option<&str>, scan: &mut Scan) -> bool {
    if !scan.is_dir(dir) {
        return false;
    }
    // Deeper, the files are counted by their headers.
    if depth > 3 {
        return false;
    }
    let name = file_name(dir);
    if is_dataset_name(&name) {
        return store_dataset(dir, &name, provider, scan);
    }
    if scan.is_file(&dir.join("metadata.json")) && scan.is_file(&dir.join("manifest.json")) {
        return databento_batch(dir, provider, scan);
    }
    let mut found = false;
    for child in listing(dir, true, scan) {
        found |= visit(&child, depth + 1, provider, scan);
    }
    found
}

/// Count every DBN file under `path` that the layouts didn't claim, from its
/// header, as `provider`'s if in its folder (decision 0019), and a download
/// folder without its manifest by its query. Hidden folders (such as
/// `.incoming`) are skipped. A file whose header can't be read yet, changed
/// in the last two minutes, is still being written: it is listed as pending
/// and read again on the next scan. An empty file holds nothing. Returns
/// whether any were found.
fn loose(path: &Path, provider: Option<&str>, scan: &mut Scan) -> bool {
    let meta = if scan.follow {
        scan.stat(path)
    } else {
        scan.lstat(path)
    };
    let Some(meta) = meta else {
        return false;
    };
    if scan.claimed.contains(path) {
        return false;
    }
    if meta.is_dir() {
        let mut found = scan.is_file(&path.join("metadata.json"))
            && scan.stat(&path.join("manifest.json")).is_none()
            && unmanifested(path, provider, scan);
        for child in listing(path, true, scan) {
            found |= loose(&child, provider, scan);
        }
        return found;
    }
    let name = file_name(path);
    let Some(encoding) = ["dbn.zst", "dbn"]
        .into_iter()
        .find(|e| name.ends_with(&format!(".{e}")))
    else {
        return false;
    };
    if !meta.is_file() {
        return false;
    }
    scan.served.push((scan.within(path), meta.len()));
    if meta.len() == 0 {
        return false;
    }
    let h = match header(path) {
        Ok(h) => h,
        Err(e) => {
            let recent = meta
                .modified()
                .ok()
                .and_then(|t| t.elapsed().ok())
                .is_some_and(|age| age < WRITING);
            if recent {
                scan.pending.push(path.display().to_string());
            } else {
                scan.fail(path, e);
            }
            return false;
        }
    };
    if !holds_data(&name, &h) {
        let folder = path.parent().unwrap_or(path);
        let folder = folder
            .strip_prefix(&scan.root)
            .unwrap_or(folder)
            .display()
            .to_string();
        let u = scan.foreign.entry((folder, h.dataset)).or_default();
        u.files += 1;
        u.bytes += meta.len();
        if let Some(t) = meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        {
            u.modified = u.modified.max(t.as_secs_f64());
        }
        return false;
    }
    let raw = h.stype_in.as_deref() == Some("raw_symbol");
    let k = match h.symbols.as_slice() {
        [one] if !raw => key(provider, &h.dataset, Some(one)),
        // A header naming no symbol: listed under the one its folder names.
        [] => key(
            provider,
            &h.dataset,
            folder_symbol(path, &h.dataset, scan).as_deref(),
        ),
        _ => key(provider, &h.dataset, None),
    };
    let d = scan.datasets.entry(k).or_default();
    let source = format!("files in {}", path.parent().unwrap_or(path).display());
    if !d.sources.contains(&source) {
        d.sources.push(source);
    }
    let s = d
        .schemas
        .entry(h.schema.clone().unwrap_or_else(|| "mixed".into()))
        .or_default();
    note_encoding(s, encoding);
    s.files += 1;
    s.bytes += meta.len();
    let days = h.days();
    let n = days.len();
    for (i, day) in days.into_iter().enumerate() {
        *s.days.entry(day.clone()).or_default() += share(meta.len(), n, i);
        if raw && !s.partial.contains(&day) {
            s.partial.push(day.clone());
        }
        if !s.unfiled.contains(&day) {
            s.unfiled.push(day);
        }
    }
    for sym in &h.symbols {
        if !s.symbols.contains(sym) {
            s.symbols.push(sym.clone());
        }
    }
    let within = scan.within(path);
    scan.places.push(Place::Loose {
        provider: provider.map(str::to_owned),
        header: h,
        size: meta.len(),
        path: within,
    });
    true
}

/// Day `i`'s share of `bytes` held over `days` days: an even split, the
/// remainder on the first, so no day a file holds is counted as empty.
#[must_use]
pub fn share(bytes: u64, days: usize, i: usize) -> u64 {
    let n = days.max(1) as u64;
    bytes / n + if i == 0 { bytes % n } else { 0 }
}

/// A download folder with `metadata.json` but no manifest, such as one
/// extracted by hand: its DBN files are counted by their headers; when it
/// has none, or has data in other encodings, every day of its query counts.
fn unmanifested(dir: &Path, provider: Option<&str>, scan: &mut Scan) -> bool {
    let path = dir.join("metadata.json");
    let meta: Value = match std::fs::read_to_string(&path)
        .map_err(|e| e.to_string())
        .and_then(|t| serde_json::from_str(&t).map_err(|e| e.to_string()))
    {
        Ok(m) => m,
        Err(e) => {
            scan.fail(dir, format!("{}: {e}", path.display()));
            return false;
        }
    };
    let q = &meta["query"];
    let Some(dataset) = q["dataset"].as_str() else {
        return false;
    };
    let names: Vec<String> = listing(dir, true, scan)
        .iter()
        .map(|p| file_name(p))
        .collect();
    let dbn = names
        .iter()
        .any(|n| n.ends_with(".dbn") || n.ends_with(".dbn.zst"));
    let other = names.iter().any(|n| {
        ENCODINGS[1..].iter().any(|e| {
            !e.starts_with("dbn")
                && n.strip_suffix(&format!(".{e}"))
                    .is_some_and(|stem| stem.contains('.'))
        })
    });
    if dbn && !other {
        return false;
    }
    let (Some(days), Some(schema)) = (query_days(q), q["schema"].as_str()) else {
        scan.fail(dir, format!(
            "{}: a download without its manifest, whose metadata.json gives no query start, end and schema",
            dir.display()
        ));
        return false;
    };
    let batch = Batch {
        dataset: dataset.to_owned(),
        symbols: strings(&q["symbols"]),
        stype_in: q["stype_in"].as_str().map(str::to_owned),
        job: meta["job_id"]
            .as_str()
            .map_or_else(|| file_name(dir), str::to_owned),
        files: days
            .into_iter()
            .map(|d| (d, schema.to_owned(), "query", 0))
            .collect(),
        names: BTreeMap::new(),
    };
    count_batch(batch, dir, provider, scan)
}

/// What a DBN file's header says it holds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Header {
    pub dataset: String,
    /// `None` for a file of several schemas.
    pub schema: Option<String>,
    pub stype_in: Option<String>,
    pub symbols: Vec<String>,
    pub start: u64,
    /// Its end (excluded); when the header has none, the end of the day
    /// its file is named for, or of its start's day.
    pub end: u64,
}

impl Header {
    /// The days from its start to its end (excluded).
    #[must_use]
    pub fn days(&self) -> Vec<String> {
        days_between(
            &crate::ingest::day_of(self.start),
            &crate::ingest::day_of(self.end.saturating_sub(1).max(self.start)),
        )
    }
}

/// Headers read, by path, kept while the file's size and last change stay
/// the same, so a rescan doesn't read them again.
type HeaderCache = HashMap<PathBuf, (u64, SystemTime, Header)>;

fn headers() -> &'static Mutex<HeaderCache> {
    static CACHE: OnceLock<Mutex<HeaderCache>> = OnceLock::new();
    CACHE.get_or_init(Mutex::default)
}

/// A DBN file's header, from the cache while the file is unchanged.
///
/// # Errors
/// If the file or its header can't be read.
pub fn header(path: &Path) -> Result<Header, String> {
    let fail = |e: &dyn std::fmt::Display| format!("{}: {e}", path.display());
    let meta = std::fs::metadata(path).map_err(|e| fail(&e))?;
    let changed = meta.modified().map_err(|e| fail(&e))?;
    if let Some((size, at, h)) = tinytrader_observe::lock(headers()).get(path)
        && *size == meta.len()
        && *at == changed
    {
        return Ok(h.clone());
    }
    let d = dbn::decode::DynDecoder::from_file(path, dbn::VersionUpgradePolicy::AsIs)
        .map_err(|e| fail(&format!("not a readable DBN file: {e}")))?;
    let m = d.metadata();
    let last_change = changed
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |t| u64::try_from(t.as_nanos()).unwrap_or(u64::MAX));
    // A header without its start (0) or end takes the day its file is
    // named for (`2026-05-29.spot.trades.dbn.zst`), not every day from 1970
    // or until its last change. Without such a name, a header with no end
    // holds its start's day: how far it reaches is unknown, and every day
    // up to its last change would be counted with nothing in it.
    const DAY: u64 = 86_400_000_000_000;
    let named = file_name(path)
        .get(..10)
        .filter(|d| is_iso_date(d))
        .and_then(crate::ingest::day_ns);
    let start = match named {
        Some(day) if m.start == 0 => day,
        _ => m.start,
    };
    let end = match (m.end, named) {
        (Some(end), _) => end.get(),
        (None, Some(day)) if day + DAY > start => day + DAY,
        (None, _) if start > 0 => (start / DAY + 1) * DAY,
        (None, _) => last_change,
    };
    let h = Header {
        dataset: m.dataset.clone(),
        schema: m.schema.map(|s| s.as_str().to_owned()),
        stype_in: m.stype_in.map(|s| s.as_str().to_owned()),
        symbols: m.symbols.clone(),
        start,
        end,
    };
    tinytrader_observe::lock(headers())
        .insert(path.to_path_buf(), (meta.len(), changed, h.clone()));
    Ok(h)
}

/// `<DATASET>/<schema>/<day>`, or with a symbol level in between, where a
/// day is `<YYYY-MM-DD>.dbn.zst` or a `<YYYY-MM-DD>/` folder holding that
/// day's files (an older layout).
fn store_dataset(dir: &Path, id: &str, provider: Option<&str>, scan: &mut Scan) -> bool {
    let mut found = false;
    let source = format!("store: {}", dir.display());
    let note = |scan: &mut Scan, k: Key| {
        let d = scan.datasets.entry(k).or_default();
        if !d.sources.contains(&source) {
            d.sources.push(source.clone());
        }
    };
    for child in listing(dir, true, scan) {
        if !scan.is_dir(&child) {
            continue;
        }
        let k = key(provider, id, None);
        if add_days(&child, &k, &file_name(&child), scan) {
            note(scan, k);
            found = true;
            continue;
        }
        let sym = file_name(&child);
        for grandchild in listing(&child, true, scan) {
            if !scan.is_dir(&grandchild) {
                continue;
            }
            let k = key(provider, id, Some(&sym));
            if add_days(&grandchild, &k, &file_name(&grandchild), scan) {
                note(scan, k);
                found = true;
            }
        }
    }
    found
}

/// Add every day in `dir` (a `<YYYY-MM-DD>.dbn.zst` file or a `<YYYY-MM-DD>/`
/// folder) to the schema of the dataset listed under `k`. A day is held only
/// with data the store can serve: never an empty day file, nor a day folder
/// holding only receipts or an `.archive`.
fn add_days(dir: &Path, k: &Key, schema: &str, scan: &mut Scan) -> bool {
    let mut any = false;
    // Partial days are those the folder's manifest lists (its coverage); an
    // unreadable manifest is an error of the volume.
    let (partial, unreadable) = match crate::manifest::read_index(dir) {
        Ok(index) => (index, None),
        Err(e) => {
            scan.fail(dir, e.clone());
            (BTreeMap::new(), Some(e))
        }
    };
    // Merges left pending, for the store to settle.
    let pending = crate::manifest::pending_days(dir);
    for day in &pending {
        scan.unsettled.push((dir.to_path_buf(), day.clone()));
    }
    for (day, e) in &partial {
        if e.get("pending").is_some() {
            scan.unsettled.push((dir.to_path_buf(), day.clone()));
        }
    }
    let mut held = Vec::new();
    let mut files = BTreeMap::new();
    for f in listing(dir, true, scan) {
        let n = file_name(&f);
        let (day, size, encoding, served) = if let Some(day) =
            n.strip_suffix(".dbn.zst").filter(|d| is_iso_date(d))
            && scan.is_file(&f)
        {
            let size = scan.stat(&f).map_or(0, |m| m.len());
            let within = scan.within(&f);
            scan.served.push((within.clone(), size));
            let served = if size > 0 { vec![within] } else { Vec::new() };
            (day.to_owned(), size, "dbn.zst", served)
        } else if is_iso_date(&n) && scan.is_dir(&f) {
            let mut found = Vec::new();
            crate::read::walk_with(&scan.root, &f, &mut found, scan.follow);
            scan.served.extend(found.iter().cloned());
            (
                n.clone(),
                usage_with(&f, scan.follow).bytes,
                "day folders",
                day_folder_files(found, schema),
            )
        } else {
            continue;
        };
        scan.claimed.insert(f.clone());
        if served.is_empty() {
            continue;
        }
        files.insert(day.clone(), served);
        held.push((day.clone(), size, encoding != "day folders"));
        let s = scan
            .datasets
            .entry(k.clone())
            .or_default()
            .schemas
            .entry(schema.to_owned())
            .or_default();
        note_encoding(s, encoding);
        s.files += 1;
        s.bytes += size;
        let entry = partial.get(&day);
        match entry.and_then(|e| e["condition"]["state"].as_str()) {
            Some("degraded") if !s.degraded.contains(&day) => s.degraded.push(day.clone()),
            Some("missing") if !s.missing.contains(&day) => s.missing.push(day.clone()),
            _ => {}
        }
        if entry.is_some_and(crate::partial::is_partial) && !s.partial.contains(&day) {
            s.partial.push(day.clone());
        }
        if encoding == "day folders" && !s.unfiled.contains(&day) {
            s.unfiled.push(day.clone());
        }
        *s.days.entry(day).or_default() += size;
        any = true;
    }
    if any {
        // Each filed day's entry, its sidecar read once here rather than on
        // every `/coverage`.
        let entries = match unreadable {
            Some(e) => Err(e),
            None => Ok(held
                .iter()
                .filter(|(_, _, filed)| *filed)
                .filter_map(|(day, _, _)| {
                    let indexed = partial.get(day)?;
                    let listed = pending.contains(day);
                    let entry = crate::manifest::resolve_listed(dir, day, indexed, listed);
                    Some((day.clone(), entry.map(|e| crate::partial::reported(&e))))
                })
                .collect()),
        };
        let (provider, dataset, symbol) = k.clone();
        scan.places.push(Place::Days {
            provider,
            dataset,
            symbol,
            schema: schema.to_owned(),
            dir: dir.to_path_buf(),
            days: held,
            entries: std::sync::Arc::new(entries),
            files,
            cas: false,
        });
    }
    any
}

/// The files of a day folder (the older layout) the store serves, its data
/// first, then sidecars such as the day's definitions; none when it holds no
/// data (only receipts, an `.archive`, a sidecar, or empty files).
fn day_folder_files(found: Vec<Served>, schema: &str) -> Vec<String> {
    let sidecar = |within: &str| {
        schema != "definition" && file_name(Path::new(within)).starts_with("definition.")
    };
    let mut names: Vec<String> = found
        .into_iter()
        .filter(|(_, bytes)| *bytes > 0)
        .map(|(within, _)| within)
        .collect();
    if names.iter().all(|n| sidecar(n)) {
        return Vec::new();
    }
    names.sort_by_key(|n| (sidecar(n), n.clone()));
    names
}

/// Space used under a path, its file count and its newest change.
#[derive(Default, Clone, Copy)]
pub struct Usage {
    pub bytes: u64,
    pub files: u64,
    pub modified: f64,
}

/// Walk `path`, adding up its files: links not followed, unless `follow`
/// (a content-addressed volume's view, whose files are links to objects).
fn usage_with(path: &Path, follow: bool) -> Usage {
    let mut u = Usage::default();
    let mut stack = vec![path.to_path_buf()];
    while let Some(p) = stack.pop() {
        let meta = if follow {
            std::fs::metadata(&p)
        } else {
            std::fs::symlink_metadata(&p)
        };
        let Ok(meta) = meta else {
            continue;
        };
        if meta.is_dir() {
            // Hidden folders count: older layouts keep data in `.archive`.
            stack.extend(
                std::fs::read_dir(&p)
                    .into_iter()
                    .flatten()
                    .flatten()
                    .map(|e| e.path()),
            );
        } else if meta.is_file() {
            u.bytes += meta.len();
            u.files += 1;
            if let Some(t) = meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            {
                u.modified = u.modified.max(t.as_secs_f64());
            }
        }
    }
    u
}

/// File encodings Databento delivers, longest first.
const ENCODINGS: [&str; 6] = ["dbn.zst", "csv.zst", "json.zst", "dbn", "csv", "json"];

fn note_encoding(s: &mut Schema, encoding: &str) {
    if !s.encodings.iter().any(|e| e == encoding) {
        s.encodings.push(encoding.to_owned());
    }
}

/// What a Databento batch download holds, from its `metadata.json` and the
/// names of its `<dataset>-<YYYYMMDD>.<schema>.<encoding>` files.
#[derive(Clone, Debug)]
pub struct Batch {
    pub dataset: String,
    pub symbols: Vec<String>,
    pub stype_in: Option<String>,
    pub job: String,
    /// Each day of each schema it holds, with the encoding and the bytes
    /// counted to it (a file covering several days shared among them).
    pub files: Vec<(String, String, &'static str, u64)>,
    /// The DBN files holding each day of each schema, by name.
    pub names: BTreeMap<(String, String), Vec<String>>,
}

/// The days a batch file's name says it holds: `<dataset>-<YYYYMMDD>`, or
/// `<dataset>-<YYYYMMDD>-<YYYYMMDD>` for a range (both ends included).
fn named_days(head: &str) -> Option<Vec<String>> {
    let date = |d: &str| {
        (d.len() == 8 && d.bytes().all(|b| b.is_ascii_digit()))
            .then(|| format!("{}-{}-{}", &d[..4], &d[4..6], &d[6..]))
            .filter(|d| crate::ingest::day_ns(d).is_some())
    };
    let mut parts = head.rsplit('-');
    let last = date(parts.next()?)?;
    let first = parts.next().and_then(date).unwrap_or_else(|| last.clone());
    let days = days_between(&first, &last);
    (!days.is_empty()).then_some(days)
}

/// Every day from `first` to `last`, both included.
fn days_between(first: &str, last: &str) -> Vec<String> {
    const DAY: u64 = 86_400_000_000_000;
    let (Some(a), Some(b)) = (crate::ingest::day_ns(first), crate::ingest::day_ns(last)) else {
        return Vec::new();
    };
    (a / DAY..=b / DAY)
        .map(|d| crate::ingest::day_of(d * DAY))
        .collect()
}

/// The days of a batch's query, from its `metadata.json`: `start` to `end`,
/// the end excluded (as Databento means it), as nanoseconds or as dates
/// (`YYYY-MM-DD`, with a time or not).
fn query_days(q: &Value) -> Option<Vec<String>> {
    let day = |v: &Value, end: bool| -> Option<String> {
        let ns = v.as_u64().or_else(|| {
            v.as_str()
                .filter(|s| s.bytes().all(|b| b.is_ascii_digit()))
                .and_then(|s| s.parse().ok())
        });
        match ns {
            Some(ns) => Some(crate::ingest::day_of(if end {
                ns.saturating_sub(1)
            } else {
                ns
            })),
            None => {
                let s = v.as_str()?;
                let ns = crate::ingest::day_ns(s.get(..10)?)?;
                // An end at midnight excludes that day; later in it, includes it.
                let time = s[10..].trim_start_matches(['T', ' ']);
                let time = &time[..time.find(['Z', '+', '-']).unwrap_or(time.len())];
                let midnight = !time.bytes().any(|b| (b'1'..=b'9').contains(&b));
                Some(crate::ingest::day_of(if end && midnight {
                    ns.saturating_sub(1)
                } else {
                    ns
                }))
            }
        }
    };
    let days = days_between(&day(&q["start"], false)?, &day(&q["end"], true)?);
    (!days.is_empty()).then_some(days)
}

/// Read the batch download in `dir`; `None` if its metadata names no dataset.
///
/// # Errors
/// If its `metadata.json` or the folder can't be read.
pub fn read_batch(dir: &Path) -> Result<Option<Batch>, String> {
    let path = dir.join("metadata.json");
    let meta: Value = std::fs::read_to_string(&path)
        .map_err(|e| e.to_string())
        .and_then(|t| serde_json::from_str(&t).map_err(|e| e.to_string()))
        .map_err(|e| format!("{}: {e}", path.display()))?;
    let Some(dataset) = meta["query"]["dataset"].as_str() else {
        return Ok(None);
    };
    let q = &meta["query"];
    let mut files = Vec::new();
    let mut names: BTreeMap<(String, String), Vec<String>> = BTreeMap::new();
    // Files split by symbol, or whose names don't say their days.
    let mut unnamed: Option<(&'static str, u64)> = None;
    let mut unnamed_names = Vec::new();
    for f in read_entries(dir)? {
        let n = file_name(&f);
        let Some((stem, encoding)) = ENCODINGS
            .iter()
            .find_map(|e| n.strip_suffix(&format!(".{e}")).map(|stem| (stem, *e)))
        else {
            continue;
        };
        // Gone since it was listed: not there. Any other failure is an error
        // of the batch, never an empty file (P0, 2026-11-10).
        let size = match f.metadata() {
            Ok(m) => m.len(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(format!("{}: {e}", f.display())),
        };
        // An empty file holds no day.
        if size == 0 {
            continue;
        }
        let dbn = encoding.starts_with("dbn");
        // <dataset>-<YYYYMMDD>[-<YYYYMMDD>].<schema>; receipts such as
        // `metadata.json` have no schema.
        let named = stem
            .rsplit_once('.')
            .and_then(|(head, schema)| Some((named_days(head)?, schema)));
        match named {
            Some((days, schema)) if meta["customizations"]["split_symbols"] != true => {
                let count = days.len();
                for (i, day) in days.into_iter().enumerate() {
                    if dbn {
                        names
                            .entry((day.clone(), schema.to_owned()))
                            .or_default()
                            .push(n.clone());
                    }
                    files.push((day, schema.to_owned(), encoding, share(size, count, i)));
                }
            }
            _ if stem.contains('.') || dbn => {
                let (_, bytes) = unnamed.get_or_insert((encoding, 0));
                *bytes += size;
                if dbn {
                    unnamed_names.push(n.clone());
                }
            }
            _ => {}
        }
    }
    // Their days are the query's, all of them.
    if let Some((encoding, bytes)) = unnamed {
        let (Some(days), Some(schema)) = (query_days(q), q["schema"].as_str()) else {
            return Err(format!(
                "{}: some files' names don't say their days, and metadata.json gives no query start, end and schema",
                dir.display()
            ));
        };
        let count = days.len();
        for (i, day) in days.into_iter().enumerate() {
            if !files.iter().any(|f| f.0 == day && f.1 == schema) {
                names
                    .entry((day.clone(), schema.to_owned()))
                    .or_default()
                    .extend(unnamed_names.iter().cloned());
                files.push((day, schema.to_owned(), encoding, share(bytes, count, i)));
            }
        }
    }
    Ok(Some(Batch {
        dataset: dataset.to_owned(),
        symbols: strings(&meta["query"]["symbols"]),
        stype_in: meta["query"]["stype_in"].as_str().map(str::to_owned),
        job: meta["job_id"]
            .as_str()
            .map_or_else(|| file_name(dir), str::to_owned),
        files,
        names,
    }))
}

/// A Databento batch download, listed under its one symbol, or under none
/// when it has several. Its days are unfiled.
fn databento_batch(dir: &Path, provider: Option<&str>, scan: &mut Scan) -> bool {
    let b = match read_batch(dir) {
        Ok(Some(b)) if !b.files.is_empty() => b,
        Ok(_) => return false,
        Err(e) => {
            scan.fail(dir, e);
            return false;
        }
    };
    scan.claimed.insert(dir.to_path_buf());
    let mut found = Vec::new();
    crate::read::walk_with(&scan.root, dir, &mut found, scan.follow);
    scan.served.extend(found);
    count_batch(b, dir, provider, scan)
}

/// Count a batch's days, in `dir`, unfiled, under its one symbol or under
/// none.
fn count_batch(b: Batch, dir: &Path, provider: Option<&str>, scan: &mut Scan) -> bool {
    let k = match b.symbols.as_slice() {
        [one] => key(provider, &b.dataset, Some(one)),
        _ => key(provider, &b.dataset, None),
    };
    let d = scan.datasets.entry(k).or_default();
    for (day, schema, encoding, size) in &b.files {
        let s = d.schemas.entry(schema.clone()).or_default();
        note_encoding(s, encoding);
        s.files += 1;
        s.bytes += size;
        *s.days.entry(day.clone()).or_default() += size;
        if !s.unfiled.contains(day) {
            s.unfiled.push(day.clone());
        }
        for sym in &b.symbols {
            if !s.symbols.contains(sym) {
                s.symbols.push(sym.clone());
            }
        }
    }
    d.sources.push(format!("databento batch {}", b.job));
    let dir = scan.within(dir);
    scan.places.push(Place::Batch {
        provider: provider.map(str::to_owned),
        batch: b,
        dir,
    });
    true
}

/// The strings of a JSON array.
fn strings(v: &Value) -> Vec<String> {
    v.as_array()
        .into_iter()
        .flatten()
        .filter_map(|s| s.as_str().map(str::to_owned))
        .collect()
}

/// `VENUE.FEED`, e.g. `OPRA.PILLAR`.
pub(crate) fn is_dataset_name(n: &str) -> bool {
    n.split_once('.').is_some_and(|(a, b)| {
        let ok = |s: &str| {
            !s.is_empty()
                && s.bytes()
                    .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == b'_')
        };
        ok(a) && ok(b)
    })
}

/// Whether a DBN file that no layout claims holds a dataset: its header
/// names a dataset id (`VENUE.FEED` in either case, as every vendor's is;
/// not, say, `bandit-chain`), and its name doesn't mark it an index
/// (`2026-07-01.poll.idx.dbn`). Other files are another program's: counted
/// as space used, never as data held. No vendor's id can be spelled so a
/// file of it is taken for one of these.
fn holds_data(name: &str, h: &Header) -> bool {
    is_dataset_name(&h.dataset.to_ascii_uppercase()) && !name.split('.').any(|part| part == "idx")
}

/// The symbol the folder holding `path` names (`Alpaca/UVXY/…`), for a file
/// whose header names none: a folder below the top-level one, named as a
/// symbol is (`UVXY`, `SPY.OPT`, `BRK.B`), not a date or the dataset's id.
fn folder_symbol(path: &Path, dataset: &str, scan: &Scan) -> Option<String> {
    let dir = path.parent()?;
    if dir == scan.top || dir == scan.root || !dir.starts_with(&scan.top) {
        return None;
    }
    let n = file_name(dir);
    let symbolic = n.len() <= 24
        && n.bytes().next().is_some_and(|c| c.is_ascii_uppercase())
        && n.bytes().all(|c| {
            c.is_ascii_uppercase() || c.is_ascii_digit() || matches!(c, b'.' | b'_' | b'-')
        });
    (symbolic && n != dataset).then_some(n)
}

fn is_iso_date(d: &str) -> bool {
    let b = d.as_bytes();
    b.len() == 10
        && b[4] == b'-'
        && b[7] == b'-'
        && b.iter()
            .enumerate()
            .all(|(i, c)| i == 4 || i == 7 || c.is_ascii_digit())
}

/// Visible entries of a directory, sorted.
///
/// # Errors
/// If the directory can't be read.
pub fn read_entries(dir: &Path) -> Result<Vec<PathBuf>, String> {
    let read = std::fs::read_dir(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    let mut out = Vec::new();
    for e in read {
        let p = e.map_err(|e| format!("{}: {e}", dir.display()))?.path();
        if !file_name(&p).starts_with('.') {
            out.push(p);
        }
    }
    out.sort();
    Ok(out)
}

/// As [`read_entries`]; when `strict` (a provider's or a dataset's folder),
/// a folder that can't be read is noted as an error.
/// Paths the scan skips are left out.
fn listing(dir: &Path, strict: bool, scan: &mut Scan) -> Vec<PathBuf> {
    match read_entries(dir) {
        Ok(mut entries) => {
            entries.retain(|e| !scan.skip.contains(e));
            entries
        }
        Err(e) => {
            if strict {
                scan.fail(dir, e);
            }
            Vec::new()
        }
    }
}

fn file_name(p: &Path) -> String {
    p.file_name()
        .map_or_else(String::new, |n| n.to_string_lossy().into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognises_both_layouts() {
        let root = std::env::temp_dir().join(format!("tinytrader-catalog-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let day = root.join("OPRA.PILLAR/SPY.OPT/definition");
        std::fs::create_dir_all(&day).unwrap();
        std::fs::write(day.join("2026-09-01.dbn.zst"), b"abc").unwrap();
        std::fs::write(day.join("notes.txt"), b"x").unwrap();
        let batch = root.join("downloads/OPRA-20261003-X");
        std::fs::create_dir_all(&batch).unwrap();
        std::fs::write(batch.join("metadata.json"), r#"{"job_id":"OPRA-20261003-X","query":{"dataset":"OPRA.PILLAR","symbols":["SPY.OPT"]}}"#).unwrap();
        std::fs::write(batch.join("manifest.json"), "{}").unwrap();
        std::fs::write(batch.join("opra-pillar-20261002.cmbp-1.dbn.zst"), b"hello").unwrap();
        let csv = root.join("XNAS-1");
        std::fs::create_dir_all(&csv).unwrap();
        std::fs::write(
            csv.join("metadata.json"),
            r#"{"job_id":"XNAS-1","query":{"dataset":"XNAS.ITCH","symbols":["MSFT"]}}"#,
        )
        .unwrap();
        std::fs::write(csv.join("manifest.json"), "{}").unwrap();
        std::fs::write(csv.join("xnas-itch-20220610.trades.csv"), b"a,b").unwrap();
        std::fs::create_dir_all(root.join("misc/.archive")).unwrap();
        std::fs::write(root.join("misc/.archive/old.bin"), b"12345678").unwrap();
        // An older layout: each day a folder of that day's files.
        let older = root.join("ALPACA.SIP/QQQ/cmbp-1/2026-09-02");
        std::fs::create_dir_all(&older).unwrap();
        std::fs::write(older.join("cmbp-1.dbn.zst"), b"0123456789").unwrap();
        std::fs::write(older.join("manifest.json"), b"{}").unwrap();

        let t = Thresholds {
            warning: 80.0,
            critical: 90.0,
            floor: 97.0,
        };
        let r = scan_root("test", &root, t);
        assert_eq!(r["present"], true);
        let find = |id: &str| {
            r["datasets"]
                .as_array()
                .unwrap()
                .iter()
                .find(|d| d["id"] == id)
                .unwrap()
                .clone()
        };
        // Outside every provider's folder, the store layout and the batch
        // download beside it have no provider, and are one dataset.
        let ds = &find("OPRA.PILLAR");
        assert_eq!(ds["id"], "OPRA.PILLAR");
        assert_eq!(
            (ds["provider"].clone(), ds["filed"].clone()),
            (Value::Null, json!(false))
        );
        assert_eq!(ds["schemas"]["definition"]["days"]["2026-09-01"], 3);
        assert_eq!(ds["symbol"], "SPY.OPT");
        assert_eq!(ds["schemas"]["cmbp-1"]["days"]["2026-10-02"], 5);
        assert_eq!(ds["schemas"]["cmbp-1"]["unfiled"], json!(["2026-10-02"]));
        assert_eq!(ds["sources"].as_array().unwrap().len(), 2);
        assert_eq!(r["unrecognised"], json!(["misc"]));
        assert_eq!(r["other"][0]["bytes"], 8, "hidden folders count");
        let older = r["datasets"]
            .as_array()
            .unwrap()
            .iter()
            .find(|d| d["id"] == "ALPACA.SIP")
            .unwrap();
        assert_eq!(older["symbol"], "QQQ");
        assert_eq!(older["schemas"]["cmbp-1"]["days"]["2026-09-02"], 12);
        assert_eq!(older["filed"], false, "the older layout's day folders");
        let xnas = find("XNAS.ITCH");
        assert_eq!(xnas["schemas"]["trades"]["encodings"], json!(["csv"]));
        assert_eq!(r["dataset_bytes"], 23);
        std::fs::remove_dir_all(&root).unwrap();

        assert_eq!(scan_root("gone", &root, t)["present"], false);
    }

    #[test]
    fn names_and_dates() {
        assert!(is_dataset_name("OPRA.PILLAR"));
        assert!(is_dataset_name("GLBX.MDP3"));
        assert!(!is_dataset_name("opra.pillar"));
        assert!(!is_dataset_name("OPRA-20261003-X"));
        assert!(is_iso_date("2026-10-05"));
        assert!(!is_iso_date("20261005"));
    }
}
