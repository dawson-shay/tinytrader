//! Ingest: provider downloads into the standard layout (decision 0002).
//!
//! A batch is uploaded file by file into `<volume>/<Provider>/.incoming/<batch>/`,
//! each file written to a `.partial`, hashed, flushed and only then renamed
//! into place. Filing the batch hashes every file again, checks it against
//! the provider's manifest, and moves each data file to
//! `<Provider>/<DATASET>/[<symbol>/]<schema>/<YYYY-MM-DD>.dbn.zst`.
//!
//! **Existing data is never replaced.** A day already held, on any volume, is
//! either byte-identical (SHA-256 recomputed on both sides; the incoming copy
//! is dropped) or the whole batch is refused. Moves are hard links, which
//! cannot replace a file even one that appeared after the checks. Receipts go
//! to `<Provider>/provenance/<batch>/` (download URLs removed) on the same
//! terms, and every step is recorded in `<Provider>/migrations/`. Filing can
//! be repeated, so an interrupted run resumes. Nothing is deleted except a
//! proven duplicate and the store's own incoming files.
//!
//! **One exception: a complete day supersedes a partial day** (decision
//! 0018) of the same parent, when the batch is the parent's whole chain
//! (`stype_in = parent`) and its day holds every record of the partial day,
//! checked record by record. The partial day is kept, in
//! `<Provider>/superseded/<batch>/`, never deleted.

use std::fs;
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};

use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};

/// A volume the store manages.
#[derive(Clone, Debug)]
pub struct Volume {
    pub name: String,
    pub path: PathBuf,
    pub writable: bool,
    /// Its path must be a mount point (decision 0019): true for drives found
    /// in the mount directory and paths under `/Volumes`.
    pub require_mount: bool,
}

/// Where incoming files wait to be incorporated.
pub const INCOMING: &str = ".incoming";

/// A provider's directory name, for the providers the store ingests. Every
/// provider's batches have Databento's shape (a `manifest.json` of files with
/// sizes and SHA-256, a `metadata.json` with the query, day files named
/// `<name>-YYYYMMDD.<schema>.dbn.zst`): Databento's own, and Alpaca's history
/// as tinytrader's `alpaca-history` packs it (decision 0014).
#[must_use]
pub fn provider_dir(provider: &str) -> Option<&'static str> {
    match provider.to_ascii_lowercase().as_str() {
        "databento" => Some("Databento"),
        "alpaca" => Some("Alpaca"),
        _ => None,
    }
}

/// Schemas that aren't DBN's own: their files' headers name no schema.
/// `conditions` holds `tinytrader.trade-conditions/1` records and
/// `quote-conditions` `tinytrader.quote-conditions/1` (decision 0014).
const EXTENSION_SCHEMAS: &[&str] = &["conditions", "quote-conditions"];

/// A single path component safe to use as a batch or file name.
#[must_use]
pub fn safe(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 200
        && !name.starts_with('.')
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The SHA-256 of a file, as lowercase hex.
pub fn hash_file(path: &Path) -> Result<String, String> {
    let mut f = fs::File::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let mut h = Sha256::new();
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = f
            .read(&mut buf)
            .map_err(|e| format!("{}: {e}", path.display()))?;
        if n == 0 {
            return Ok(hex(&h.finalize()));
        }
        h.update(&buf[..n]);
    }
}

/// Flush a directory's entries (a new or removed name) to disk. A file
/// system that doesn't support it (some report EINVAL or ENOTSUP) isn't an
/// error; any other failure is.
pub(crate) fn sync_dir(dir: &Path) -> std::io::Result<()> {
    match fs::File::open(dir)?.sync_all() {
        Err(e)
            if matches!(e.raw_os_error(), Some(22 | 45 | 102))
                || e.kind() == std::io::ErrorKind::Unsupported =>
        {
            Ok(())
        }
        r => r,
    }
}

/// Writes one uploaded file: chunks go to a `.partial`, which is checked and
/// renamed into place once complete.
pub struct Upload {
    dir: PathBuf,
    name: String,
    partial: PathBuf,
    file: fs::File,
    hash: Sha256,
    size: u64,
}

impl Upload {
    /// # Errors
    /// If the incoming directory or the partial file can't be created.
    pub fn start(dir: &Path, name: &str) -> Result<Self, String> {
        fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
        let partial = dir.join(format!("{name}.partial"));
        let file = fs::File::create(&partial).map_err(|e| format!("{}: {e}", partial.display()))?;
        Ok(Self {
            dir: dir.to_owned(),
            name: name.to_owned(),
            partial,
            file,
            hash: Sha256::new(),
            size: 0,
        })
    }

    /// # Errors
    /// On a write failure.
    pub fn write(&mut self, chunk: &[u8]) -> Result<(), String> {
        self.hash.update(chunk);
        self.size += chunk.len() as u64;
        self.file.write_all(chunk).map_err(|e| e.to_string())
    }

    /// Flush, check against what was expected, and rename into place.
    /// Returns the size and SHA-256.
    ///
    /// # Errors
    /// If the size or hash differs (the partial is removed), or on I/O failure.
    pub fn finish(self, size: Option<u64>, sha256: Option<&str>) -> Result<(u64, String), String> {
        self.file.sync_all().map_err(|e| e.to_string())?;
        let got = hex(&self.hash.finalize());
        let want = sha256.map(|s| s.trim_start_matches("sha256:").to_ascii_lowercase());
        if size.is_some_and(|s| s != self.size) || want.as_ref().is_some_and(|w| *w != got) {
            let _ = fs::remove_file(&self.partial);
            return Err(format!(
                "{}: received {} bytes with SHA-256 {got}, expected {} bytes with {}",
                self.name,
                self.size,
                size.map_or("any".into(), |s| s.to_string()),
                want.unwrap_or_else(|| "any".into())
            ));
        }
        let final_path = self.dir.join(&self.name);
        fs::rename(&self.partial, &final_path).map_err(|e| e.to_string())?;
        sync_dir(&self.dir).map_err(|e| format!("{}: {e}", self.dir.display()))?;
        Ok((self.size, got))
    }
}

/// Appends step records to a migration ledger, one copy on each volume
/// involved, so every volume records what happened to its files.
pub(crate) struct Ledger {
    files: Vec<fs::File>,
}

impl Ledger {
    pub(crate) fn open(paths: &[PathBuf]) -> Result<Self, String> {
        let mut files = Vec::new();
        for path in paths {
            if let Some(dir) = path.parent() {
                fs::create_dir_all(dir).map_err(|e| e.to_string())?;
            }
            files.push(
                fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)
                    .map_err(|e| e.to_string())?,
            );
        }
        Ok(Self { files })
    }
    /// Keep a copy on one more volume too, from here on.
    pub(crate) fn add(&mut self, path: PathBuf) -> Result<(), String> {
        self.files.extend(Self::open(&[path])?.files);
        Ok(())
    }
    /// Append a step to every copy of the ledger, flushed. A filing whose
    /// record can't be written stops: what moved must always be on record.
    pub(crate) fn record(&mut self, mut event: Value) -> Result<(), String> {
        event["at"] = json!(tinytrader_observe::unix_seconds());
        for file in &mut self.files {
            writeln!(file, "{event}").map_err(|e| format!("migration ledger: {e}"))?;
            file.sync_all()
                .map_err(|e| format!("migration ledger: {e}"))?;
        }
        Ok(())
    }
}

/// Whether an earlier filing of a batch, by its records in `migrations/` on
/// these roots, never completed: its event ledger is there (it began), and no
/// root has its summary or a `complete` step. A batch with no ledger at all
/// (its provenance from before ledgers, or put there by hand) isn't taken as
/// interrupted.
fn interrupted_earlier(roots: &[&Path], batch: &str) -> Result<bool, String> {
    let ledger_name = format!("-incorporate-{batch}.events.jsonl");
    let summary_name = format!("-incorporate-{batch}.json");
    let summary_again = format!("-incorporate-{batch}-");
    let (mut began, mut completed) = (false, false);
    for root in roots {
        let Ok(entries) = fs::read_dir(root.join("migrations")) else {
            continue;
        };
        for e in entries.flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            if name.ends_with(&ledger_name) {
                began = true;
                let text =
                    fs::read_to_string(e.path()).map_err(|e| format!("migration ledger: {e}"))?;
                completed |= text
                    .lines()
                    .filter_map(|l| serde_json::from_str::<Value>(l).ok())
                    .any(|v| v["step"] == "complete");
            } else if name.ends_with(&summary_name)
                || name.rsplit_once(&summary_again).is_some_and(|(_, rest)| {
                    rest.strip_suffix(".json")
                        .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
                })
            {
                completed = true;
            }
        }
    }
    Ok(began && !completed)
}

/// `YYYY-MM-DD` of a Databento file name such as
/// `opra-pillar-20251001.cmbp-1.dbn.zst`, if it is a daily DBN file of `schema`.
pub(crate) fn databento_day(name: &str, schema: &str) -> Option<String> {
    let (stem, rest) = name.split_once('.')?;
    let day = stem.rsplit('-').next()?;
    if day.len() != 8 || !day.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    (rest.strip_prefix(schema)? == ".dbn.zst")
        .then(|| format!("{}-{}-{}", &day[..4], &day[4..6], &day[6..]))
}

/// Check a day file's own DBN header against where it's being filed: its
/// dataset, its schema, and a time range that includes the day. The manifest
/// already vouched for its bytes; this catches a file that is intact but
/// isn't what its name says.
pub(crate) fn check_header(
    path: &Path,
    dataset: &str,
    schema: &str,
    day: &str,
) -> Result<(), String> {
    let decoder = dbn::decode::dbn::Decoder::from_zstd_file(path)
        .map_err(|e| format!("not a readable DBN file: {e}"))?;
    let m = dbn::decode::DbnMetadata::metadata(&decoder);
    if m.dataset != dataset {
        return Err(format!(
            "its header says dataset {}, not {dataset}",
            m.dataset
        ));
    }
    // An extension schema (trade conditions) is written as a header without a
    // schema; DBN's own schemas must match.
    let want: Option<dbn::Schema> = if EXTENSION_SCHEMAS.contains(&schema) {
        None
    } else {
        Some(
            schema
                .parse()
                .map_err(|_| format!("unknown schema {schema}"))?,
        )
    };
    if m.schema != want {
        return Err(format!(
            "its header says schema {:?}, not {schema}",
            m.schema.map(|s| s.as_str())
        ));
    }
    let start = day_ns(day).ok_or("bad day")?;
    let end = start + 86_400 * 1_000_000_000;
    if m.start >= end || m.end.is_some_and(|e| e.get() <= start) {
        return Err(format!(
            "its header covers {}..{}, not {day}",
            m.start,
            m.end.map_or(0, |e| e.get())
        ));
    }
    Ok(())
}

/// Nanoseconds since the epoch at 00:00 UTC of a `YYYY-MM-DD` day.
/// `YYYY-MM-DD` of a time in nanoseconds since the epoch (UTC).
pub(crate) fn day_of(ns: u64) -> String {
    let z = (ns / 86_400_000_000_000) as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    format!("{:04}-{m:02}-{d:02}", yoe + era * 400 + i64::from(m <= 2))
}

pub(crate) fn day_ns(day: &str) -> Option<u64> {
    let mut it = day.split('-').map(str::parse::<i64>);
    let (y, m, d) = (it.next()?.ok()?, it.next()?.ok()?, it.next()?.ok()?);
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    u64::try_from((era * 146_097 + doe - 719_468) * 86_400)
        .ok()
        .map(|s| s * 1_000_000_000)
}

/// Move `from` to `to` without ever replacing an existing `to`: a hard link
/// (which fails if `to` exists, atomically) then removing `from`. Both are on
/// the same volume.
pub(crate) fn place_new(from: &Path, to: &Path) -> std::io::Result<()> {
    let dir = to
        .parent()
        .ok_or_else(|| std::io::Error::other("no parent directory"))?;
    fs::create_dir_all(dir)?;
    fs::hard_link(from, to)?;
    sync_dir(dir)?;
    fs::remove_file(from)?;
    if let Some(d) = from.parent() {
        sync_dir(d)?;
    }
    Ok(())
}

/// Write `bytes` to a new file at `to`, never replacing one.
pub(crate) fn write_new(to: &Path, bytes: &[u8]) -> std::io::Result<()> {
    if let Some(dir) = to.parent() {
        fs::create_dir_all(dir)?;
    }
    let mut f = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(to)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    if let Some(dir) = to.parent() {
        sync_dir(dir)?;
    }
    Ok(())
}

/// Where a data file goes, and what is already there.
enum Plan {
    /// Nothing holds this day yet: move the file here.
    Place {
        from: PathBuf,
        to: PathBuf,
        sha256: String,
    },
    /// An identical file (same SHA-256, recomputed) is already in place,
    /// here or on another volume: the incoming copy is a provable duplicate.
    Duplicate {
        from: PathBuf,
        existing: PathBuf,
        sha256: String,
        entry: Option<Value>,
    },
    /// A partial day of the parent is held, and this whole chain's day
    /// holds every one of its records: it takes the partial day's place.
    Supersede(Box<Supersede>),
}

/// A complete day to put in place of a partial day.
struct Supersede {
    /// The incoming file, and its copies on the batch's other volumes.
    from: PathBuf,
    copies: Vec<PathBuf>,
    /// The partial day held, and the provider directory holding it.
    at: PathBuf,
    root: PathBuf,
    day: String,
    name: String,
    size: u64,
    sha256: String,
    /// The partial day's SHA-256, its manifest entry, and what the check found.
    old: String,
    entry: Value,
    held: crate::partial::Held,
}

/// Can the whole chain's day file `new` supersede the partial day held at
/// `held` (SHA-256 `old`, manifest entry `entry`)? The day held must be the
/// file its entry records, and every one of its records must be in `new`.
fn supersedable(
    held: &Path,
    old: &str,
    entry: &Value,
    new: &Path,
) -> Result<crate::partial::Held, String> {
    let size = fs::metadata(held).map_err(|e| e.to_string())?.len();
    if entry["size"].as_u64().is_some_and(|s| s != size)
        || entry["sha256"].as_str().is_some_and(|h| h != old)
    {
        return Err(format!(
            "{} is {size} bytes, SHA-256 {old}, but its manifest records {} bytes, SHA-256 {}; nothing supersedes it until that is looked at",
            held.display(),
            entry["size"],
            entry["sha256"]
        ));
    }
    let cov = crate::partial::Coverage::from_json(&entry["coverage"])
        .ok_or_else(|| format!("{}: its coverage is unreadable", held.display()))?;
    let found = crate::partial::holds_all(held, &cov, new)
        .map_err(|e| format!("the partial day {} isn't superseded: {e}", held.display()))?;
    if let Some(n) = entry["records"].as_u64().filter(|n| *n != found.records) {
        return Err(format!(
            "the partial day {} holds {} records, but its manifest records {n}",
            held.display(),
            found.records
        ));
    }
    Ok(found)
}

/// Put a complete day in place of the partial day it holds every record of,
/// as a merge is swapped in (decision 0018): the new file staged on the
/// partial day's volume (so the swap is a rename), the entry it will have
/// recorded as pending, the partial day kept (linked into
/// `superseded/<batch>/`, with its entry), the new file renamed over it, and
/// its entry made current. An interrupted one is settled from the day
/// file's hash to before or after.
fn supersede(
    s: &Supersede,
    batch: &str,
    level: &Path,
    ledger: &mut Ledger,
) -> Result<Value, String> {
    use crate::partial::crash;
    fn io(p: &Path, e: std::io::Error) -> String {
        format!("{}: {e}", p.display())
    }
    let dir = s.at.parent().ok_or("no folder")?;
    let staged = s.root.join(INCOMING).join(batch).join(&s.name);
    if staged != s.from && (!staged.exists() || hash_file(&staged)? != s.sha256) {
        let tmp = staged.with_file_name(format!("{}.partial", s.name));
        fs::create_dir_all(staged.parent().ok_or("no folder")?).map_err(|e| io(&staged, e))?;
        fs::copy(&s.from, &tmp).map_err(|e| io(&tmp, e))?;
        fs::File::open(&tmp)
            .and_then(|f| f.sync_all())
            .map_err(|e| io(&tmp, e))?;
        if hash_file(&tmp)? != s.sha256 {
            let _ = fs::remove_file(&tmp);
            return Err(format!(
                "{}: copied with a different SHA-256",
                tmp.display()
            ));
        }
        fs::rename(&tmp, &staged).map_err(|e| io(&staged, e))?;
        ledger.record(
            json!({ "step": "copied", "from": s.from, "to": staged, "sha256": s.sha256 }),
        )?;
    }
    crash("copied")?;
    let kept = s
        .root
        .join(crate::catalog::SUPERSEDED)
        .join(batch)
        .join(level)
        .join(format!("{}.dbn.zst", s.day));
    let mut next = json!({ "file": format!("{}.dbn.zst", s.day), "size": s.size, "sha256": s.sha256, "records": s.held.total, "source": format!("batch/{batch}"), "superseded": { "path": kept, "sha256": s.old } });
    if let Some(c) = s.entry.get("condition") {
        next["condition"] = c.clone();
    }
    crate::manifest::write_pending(dir, &s.day, &next)?;
    // The partial day is kept: its entry, and its file by a second name (a
    // crash before the swap leaves only that name for the day as it was).
    let kept_entry = kept.with_file_name(format!("{}.json", s.day));
    if !kept_entry.exists() {
        let text = serde_json::to_string_pretty(&s.entry).map_err(|e| e.to_string())?;
        write_new(&kept_entry, text.as_bytes()).map_err(|e| io(&kept_entry, e))?;
    }
    match fs::hard_link(&s.at, &kept) {
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            if hash_file(&kept)? != s.old {
                return Err(format!(
                    "{} already holds a different file; nothing was superseded",
                    kept.display()
                ));
            }
        }
        r => r.map_err(|e| io(&kept, e))?,
    }
    sync_dir(kept.parent().ok_or("no folder")?).map_err(|e| io(&kept, e))?;
    ledger.record(json!({ "step": "superseded", "day": s.day,
        "old": { "path": s.at, "sha256": s.old, "kept": kept, "records": s.held.records, "contracts": s.held.contracts },
        "new": { "path": s.at, "from": staged, "sha256": s.sha256, "records": s.held.total },
        "matched": s.held.records }))?;
    crash("pending")?;
    fs::rename(&staged, &s.at).map_err(|e| io(&s.at, e))?;
    sync_dir(dir).map_err(|e| io(dir, e))?;
    crash("swapped")?;
    crate::manifest::settle(dir, &s.day)?;
    // Other copies are proven duplicates of the file just swapped in.
    for c in std::iter::once(&s.from).chain(&s.copies) {
        if *c != staged && hash_file(c).is_ok_and(|h| h == s.sha256) {
            fs::remove_file(c).map_err(|e| io(c, e))?;
        }
    }
    let _ = fs::remove_dir(staged.parent().unwrap_or(dir));
    let _ = fs::remove_dir(s.root.join(INCOMING));
    ledger.record(json!({ "step": "swapped", "path": s.at, "sha256": s.sha256 }))?;
    Ok(
        json!({ "path": s.at, "sha256": s.sha256, "filed": "superseded", "superseded": { "path": kept, "sha256": s.old, "records": s.held.records } }),
    )
}

/// One volume's share of a batch: the folder holding its files, and that
/// volume's provider directory, where they are filed. A batch may be spread
/// over several volumes (decision 0007: each file is placed whole on the
/// volume with the most room); each file is filed on the volume it is on, so
/// every move is a hard link.
#[derive(Clone, Debug)]
pub struct Part {
    pub inc: PathBuf,
    pub root: PathBuf,
}

/// Incorporate a Databento batch waiting in `.incoming/<batch>` on one or more
/// volumes (`parts`). `elsewhere` are the same provider's directories on the
/// store's other volumes (read-only ones included), which are checked for the
/// same days. Returns a summary of what happened.
///
/// Existing data is never replaced. A day already held anywhere is either the
/// same bytes (the incoming copy is dropped as a duplicate) or the whole batch
/// is refused. Every check runs before anything moves, and each move cannot
/// replace a file even if one appeared since the check. Receipts and the
/// migration record go to every volume holding part of the batch.
///
/// # Errors
/// If the batch is incomplete, a file doesn't match the manifest, the
/// request shape isn't supported, or a destination holds a different file.
#[cfg(test)]
pub fn incorporate(
    parts: &[Part],
    elsewhere: &[PathBuf],
    batch: &str,
    today: &str,
) -> Result<Value, String> {
    incorporate_on(parts, elsewhere, &[], batch, today)
}

/// As [`incorporate`], `read_only` naming the provider directories that
/// can't be written. A parent's whole chain is filed day by day: a day that
/// can't be filed (held complete as a different file, a partial day it
/// doesn't hold every record of, or one on a read-only volume) is refused
/// alone, listed in the summary's `refused`, and changes nothing; the
/// others are filed, and the batch stays in incoming to be filed again.
///
/// # Errors
/// As [`incorporate`], for the batch as a whole.
pub fn incorporate_on(
    parts: &[Part],
    elsewhere: &[PathBuf],
    read_only: &[PathBuf],
    batch: &str,
    today: &str,
) -> Result<Value, String> {
    file_batch(parts, elsewhere, read_only, true, batch, today)
}

/// File a Databento download folder where it already is (`dir`, on the same
/// volume as the provider directory `root`), on exactly the same terms as an
/// incoming batch. Nothing is copied: files move by link on the same volume.
/// If any check fails, the folder is left exactly as it was.
///
/// # Errors
/// As [`incorporate`].
pub fn adopt(dir: &Path, root: &Path, elsewhere: &[PathBuf], today: &str) -> Result<Value, String> {
    let batch = dir
        .file_name()
        .and_then(|n| n.to_str())
        .filter(|n| safe(n))
        .ok_or("not a batch folder")?;
    let part = Part {
        inc: dir.to_path_buf(),
        root: root.to_path_buf(),
    };
    file_batch(&[part], elsewhere, &[], false, batch, today)
}

/// Keep a receipt in `provenance/<batch>/` on every root: linked on its own
/// volume, copied (and checked) to the others, never over a different one.
/// The incoming copy is gone afterwards.
fn keep_receipt(
    from: &Path,
    own: &Path,
    roots: &[&Path],
    batch: &str,
    name: &str,
) -> Result<Vec<PathBuf>, String> {
    let bytes = fs::read(from).map_err(|e| format!("{}: {e}", from.display()))?;
    let mut kept = Vec::new();
    for root in roots.iter().filter(|r| **r != own) {
        let to = root.join("provenance").join(batch).join(name);
        if !to.exists() {
            write_new(&to, &bytes).map_err(|e| format!("{}: {e}", to.display()))?;
        }
        kept.push(to);
    }
    let to = own.join("provenance").join(batch).join(name);
    if to.exists() {
        fs::remove_file(from).map_err(|e| e.to_string())?;
    } else {
        place_new(from, &to).map_err(|e| format!("{}: {e}", to.display()))?;
    }
    kept.push(to);
    Ok(kept)
}

#[expect(
    clippy::too_many_lines,
    reason = "one pass of checks, then one of moves, kept together so nothing moves before every check"
)]
fn file_batch(
    parts: &[Part],
    elsewhere: &[PathBuf],
    read_only: &[PathBuf],
    by_day: bool,
    batch: &str,
    today: &str,
) -> Result<Value, String> {
    let roots: Vec<&Path> = parts.iter().map(|p| p.root.as_path()).collect();
    let first = parts.first().ok_or("no part of the batch was found")?;
    // The part holding a file, if any does.
    let holder = |name: &str| parts.iter().find(|p| p.inc.join(name).exists());
    let read_json = |name: &str| -> Result<Value, String> {
        let p = holder(name)
            .ok_or_else(|| format!("{name}: missing"))?
            .inc
            .join(name);
        let text = fs::read_to_string(&p).map_err(|e| format!("{name}: {e}"))?;
        serde_json::from_str(&text).map_err(|e| format!("{name}: {e}"))
    };
    let manifest = read_json("manifest.json")?;
    let metadata = read_json("metadata.json")?;
    let q = &metadata["query"];
    let (dataset, schema) = (
        q["dataset"]
            .as_str()
            .ok_or("metadata.json has no dataset")?,
        q["schema"].as_str().ok_or("metadata.json has no schema")?,
    );
    if !safe(dataset) || !safe(schema) {
        return Err("metadata.json names an unsafe dataset or schema".into());
    }
    if q["encoding"] != "dbn" || q["compression"] != "zstd" {
        return Err(
            "only DBN batches compressed with zstd are filed (the store's format is .dbn.zst)"
                .into(),
        );
    }
    let c = &metadata["customizations"];
    if c["split_symbols"] == true || c["split_duration"] != "day" {
        return Err("only batches split by day, and not by symbol, are filed".into());
    }
    let symbols: Vec<&str> = q["symbols"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect();
    // A file at <DATASET>/<schema>/ holds every symbol; one requested symbol
    // (such as a parent, SPY.OPT) gets its own level. Anything else would
    // file a partial set of symbols where all of them are expected.
    let level: PathBuf = match symbols.as_slice() {
        ["ALL_SYMBOLS"] => PathBuf::from(dataset).join(schema),
        [one] if safe(one) => PathBuf::from(dataset).join(one).join(schema),
        _ => {
            return Err(format!(
                "this batch requests {} symbols; only batches for one symbol (or all symbols) can be filed, so request each symbol separately",
                symbols.len()
            ));
        }
    };
    // A parent's whole chain, whose days may supersede partial days of it.
    let whole = q["stype_in"] == "parent" && symbols != ["ALL_SYMBOLS"];
    // Its days are filed one by one, each refused alone.
    let by_day = by_day && whole;
    let prov = |root: &Path| root.join("provenance").join(batch);
    // Provenance an earlier filing of this batch left without completing
    // (interrupted: an I/O error, a crash) is replaced, set aside and on
    // record, rather than refusing every later filing over its receipts.
    // Read before this filing's own ledger is opened.
    let replace_provenance =
        interrupted_earlier(&roots, batch)? && roots.iter().any(|r| prov(r).exists());
    let mut ledger = Ledger::open(
        &roots
            .iter()
            .map(|r| {
                r.join("migrations")
                    .join(format!("{today}-incorporate-{batch}.events.jsonl"))
            })
            .collect::<Vec<_>>(),
    )?;
    ledger.record(json!({ "step": "begin", "batch": batch, "dataset": dataset, "schema": schema, "symbols": symbols, "parts": parts.iter().map(|p| &p.inc).collect::<Vec<_>>() }))?;

    // The manifest as kept: without its download URLs.
    let mut kept_manifest = manifest.clone();
    for f in kept_manifest["files"].as_array_mut().into_iter().flatten() {
        if let Some(o) = f.as_object_mut() {
            o.remove("urls");
        }
    }
    let kept_manifest = serde_json::to_string_pretty(&kept_manifest).map_err(|e| e.to_string())?;

    // Every provider directory a day could already be in, on any volume.
    let mut everywhere: Vec<PathBuf> = roots.iter().map(|r| r.to_path_buf()).collect();
    everywhere.extend(
        elsewhere
            .iter()
            .filter(|e| !roots.contains(&e.as_path()))
            .cloned(),
    );

    // Check everything, and plan every move, before moving anything.
    let files = manifest["files"]
        .as_array()
        .ok_or("manifest.json lists no files")?;
    let mut plans = Vec::new();
    let mut earlier = Vec::new();
    let mut receipts: Vec<(String, PathBuf, PathBuf)> = Vec::new();
    let mut problems = Vec::new();
    let mut check = |f: &Value| -> Result<(), String> {
        let name = f["filename"].as_str().unwrap_or_default();
        if !safe(name) {
            return Err(format!("unsafe file name {name:?}"));
        }
        let want = f["hash"]
            .as_str()
            .unwrap_or_default()
            .trim_start_matches("sha256:")
            .to_ascii_lowercase();
        let part = holder(name).unwrap_or(first);
        let path = part.inc.join(name);
        if name.ends_with(".json") {
            if !path.exists() && !replace_provenance {
                // Kept by an earlier, interrupted run?
                for root in &roots {
                    let kept = prov(root).join(name);
                    if kept.exists() && hash_file(&kept)? == want {
                        return Ok(());
                    }
                }
                return Err(format!("{name}: missing"));
            }
            if !path.exists() {
                return Err(format!("{name}: missing"));
            }
            let got = hash_file(&path)?;
            for root in roots.iter().filter(|_| !replace_provenance) {
                let kept = prov(root).join(name);
                if kept.exists() && hash_file(&kept)? != got {
                    return Err(format!(
                        "{}: provenance already holds a different {name}",
                        prov(root).display()
                    ));
                }
            }
        }
        let day = if name.ends_with(".json") {
            None
        } else {
            Some(
                databento_day(name, schema)
                    .ok_or_else(|| format!("{name}: not a daily {schema} .dbn.zst file"))?,
            )
        };
        let rel = day.as_ref().map(|d| level.join(format!("{d}.dbn.zst")));
        // Each copy held, settled first if a merge or a supersede into it
        // was interrupted, with its manifest entry.
        let existing: Vec<(PathBuf, String, Option<Value>)> = match (&rel, &day) {
            (Some(rel), Some(d)) => everywhere
                .iter()
                .map(|r| r.join(rel))
                .filter(|p| p.exists())
                .map(|p| {
                    let entry = crate::manifest::settle(p.parent().ok_or("no folder")?, d)?;
                    hash_file(&p).map(|h| (p, h, entry))
                })
                .collect::<Result<_, String>>()?,
            _ => Vec::new(),
        };
        // A different file is refused, unless it is a partial day, held once,
        // that this whole chain may supersede.
        let mut partial_held = None;
        if let Some((p, h, e)) = existing.iter().find(|(_, h, _)| *h != want) {
            match e
                .as_ref()
                .filter(|e| whole && existing.len() == 1 && crate::manifest::is_partial(e))
            {
                Some(_) if read_only.iter().any(|r| p.starts_with(r)) => {
                    return Err(format!(
                        "{name}: the partial day {} is on a read-only volume; nothing supersedes it until that volume can be written",
                        p.display()
                    ));
                }
                Some(e) => partial_held = Some((p.clone(), h.clone(), e.clone())),
                None => {
                    return Err(format!(
                        "{name}: {} already holds a different file (SHA-256 {h}, this batch has {want})",
                        p.display()
                    ));
                }
            }
        }
        let Ok(meta) = fs::metadata(&path) else {
            // Filed by an earlier run, interrupted or with days refused?
            if existing.is_empty() || partial_held.is_some() {
                return Err(format!("{name}: missing"));
            }
            earlier.push(json!({ "path": existing[0].0, "sha256": want, "already_present": true, "filed": "already present" }));
            return Ok(());
        };
        if f["size"].as_u64().is_some_and(|s| s != meta.len()) {
            return Err(format!(
                "{name}: {} bytes, manifest says {}",
                meta.len(),
                f["size"]
            ));
        }
        let got = hash_file(&path)?;
        if got != want {
            return Err(format!("{name}: SHA-256 {got}, manifest says {want}"));
        }
        if let Some(d) = &day {
            check_header(&path, dataset, schema, d).map_err(|e| format!("{name}: {e}"))?;
        }
        let mut verified =
            json!({ "step": "verified", "file": path, "bytes": meta.len(), "sha256": got });
        if let (Some((at, old, entry)), Some(day)) = (partial_held, &day) {
            let held =
                supersedable(&at, &old, &entry, &path).map_err(|e| format!("{name}: {e}"))?;
            verified["records"] = json!(held.total);
            verified["supersedes"] = json!({ "path": at, "sha256": old, "records": held.records, "contracts": held.contracts });
            ledger.record(verified)?;
            let root = everywhere
                .iter()
                .find(|r| at.starts_with(r))
                .ok_or("the partial day isn't under a provider folder")?
                .clone();
            plans.push(Plan::Supersede(Box::new(Supersede {
                copies: parts
                    .iter()
                    .map(|p| p.inc.join(name))
                    .filter(|p| *p != path && p.exists())
                    .collect(),
                from: path,
                at,
                root,
                day: day.clone(),
                name: name.to_owned(),
                size: meta.len(),
                sha256: got,
                old,
                entry,
                held,
            })));
            return Ok(());
        }
        ledger.record(verified)?;
        match (rel, existing.into_iter().next()) {
            (None, _) => receipts.push((name.to_owned(), path, part.root.clone())),
            (Some(_), Some((p, _, entry))) => plans.push(Plan::Duplicate {
                from: path,
                existing: p,
                sha256: got,
                entry,
            }),
            (Some(rel), None) => plans.push(Plan::Place {
                from: path,
                to: part.root.join(rel),
                sha256: got,
            }),
        }
        Ok(())
    };
    let mut refused = Vec::new();
    for f in files {
        if let Err(e) = check(f) {
            let name = f["filename"].as_str().unwrap_or_default();
            if by_day && !name.ends_with(".json") {
                refused.push(json!({ "file": name, "day": databento_day(name, schema), "why": e }));
            } else {
                problems.push(e);
            }
        }
    }
    for root in roots.iter().filter(|_| !replace_provenance) {
        let kept = prov(root).join("manifest.json");
        if kept.exists() && fs::read_to_string(&kept).map_err(|e| e.to_string())? != kept_manifest {
            problems.push(format!(
                "{}: provenance already holds a different manifest.json",
                prov(root).display()
            ));
        }
    }
    if !problems.is_empty() {
        ledger.record(json!({ "step": "refused", "problems": problems }))?;
        return Err(problems.join("; "));
    }
    for r in &refused {
        ledger.record(json!({ "step": "refused-day", "file": r["file"], "why": r["why"] }))?;
    }
    // Every check passed: the interrupted filing's provenance goes aside
    // (never deleted), so this filing's receipts are the batch's.
    if replace_provenance {
        let at = tinytrader_observe::unix_seconds() as u64;
        for root in roots.iter().filter(|r| prov(r).exists()) {
            let aside = root
                .join("provenance")
                .join(format!("{batch}.interrupted-{at}"));
            fs::rename(prov(root), &aside).map_err(|e| format!("{}: {e}", prov(root).display()))?;
            ledger.record(json!({ "step": "provenance-set-aside", "from": prov(root), "to": aside,
                                  "why": "an earlier filing of this batch began and never completed" }))?;
        }
    }

    // A partial day superseded on a volume holding none of the batch gets
    // the ledger and the receipts too.
    let mut extra: Vec<PathBuf> = Vec::new();
    for p in &plans {
        if let Plan::Supersede(s) = p
            && !roots.contains(&s.root.as_path())
            && !extra.contains(&s.root)
        {
            ledger.add(
                s.root
                    .join("migrations")
                    .join(format!("{today}-incorporate-{batch}.events.jsonl")),
            )?;
            extra.push(s.root.clone());
        }
    }
    if !extra.is_empty() {
        ledger.record(json!({ "step": "begin", "batch": batch, "also": extra }))?;
    }

    // Every check passed: move the data into place, never over anything
    // but a partial day superseded.
    let mut placed = Vec::new();
    for plan in plans {
        match plan {
            Plan::Place { from, to, sha256 } => {
                if let Err(e) = place_new(&from, &to) {
                    let why = if e.kind() == std::io::ErrorKind::AlreadyExists {
                        format!(
                            "{} appeared while filing; nothing was replaced, file the batch again",
                            to.display()
                        )
                    } else {
                        format!("{} -> {}: {e}", from.display(), to.display())
                    };
                    if by_day {
                        ledger
                            .record(json!({ "step": "refused-day", "file": from, "why": why }))?;
                        refused.push(json!({ "file": from, "why": why }));
                        continue;
                    }
                    ledger.record(json!({ "step": "stopped", "error": why }))?;
                    return Err(why);
                }
                ledger
                    .record(json!({ "step": "moved", "from": from, "to": to, "sha256": sha256 }))?;
                placed.push(json!({ "path": to, "sha256": sha256, "filed": "new" }));
            }
            Plan::Duplicate {
                from,
                existing,
                sha256,
                entry,
            } => {
                fs::remove_file(&from).map_err(|e| e.to_string())?;
                ledger.record(json!({ "step": "duplicate-dropped", "file": from, "identical_to": existing, "sha256": sha256 }))?;
                let mut f = json!({ "path": existing, "sha256": sha256, "already_present": true, "filed": "already present" });
                // Superseded by an earlier run of this batch?
                if let Some(e) = entry.filter(|e| e["source"] == format!("batch/{batch}").as_str())
                {
                    f["superseded"] = e["superseded"].clone();
                }
                placed.push(f);
            }
            Plan::Supersede(s) => match supersede(&s, batch, &level, &mut ledger) {
                Ok(f) => placed.push(f),
                // Settled back to the partial day, unless it was swapped in.
                Err(why) if by_day && !(cfg!(test) && why.starts_with("crashed")) => {
                    crate::manifest::settle(s.at.parent().ok_or("no folder")?, &s.day)?;
                    ledger.record(json!({ "step": "refused-day", "file": s.from, "why": why }))?;
                    refused.push(json!({ "file": s.name, "day": s.day, "why": why }));
                }
                Err(why) => {
                    ledger.record(json!({ "step": "stopped", "error": why }))?;
                    return Err(why);
                }
            },
        }
    }
    placed.extend(earlier);
    let roots: Vec<&Path> = roots
        .iter()
        .copied()
        .chain(extra.iter().map(PathBuf::as_path))
        .collect();

    // Each filed day's condition, from the batch's condition.json.
    let mut dirs: Vec<PathBuf> = parts.iter().map(|p| p.inc.clone()).collect();
    dirs.extend(roots.iter().map(|r| prov(r)));
    crate::conditions::record_batch(&dirs, &json!({ "files": placed }), batch)?;

    // Receipts go to provenance on every volume holding part of the batch,
    // never over a different one (checked above). With days refused, the
    // batch stays in incoming to be filed again: its receipts are copied.
    let receipt_names: Vec<String> = receipts.iter().map(|(n, _, _)| n.clone()).collect();
    for (name, from, own) in receipts.iter().filter(|(n, _, _)| n != "manifest.json") {
        if !refused.is_empty() {
            let bytes = fs::read(from).map_err(|e| format!("{}: {e}", from.display()))?;
            for to in roots
                .iter()
                .map(|r| prov(r).join(name))
                .filter(|t| !t.exists())
            {
                write_new(&to, &bytes).map_err(|e| format!("{}: {e}", to.display()))?;
                ledger.record(json!({ "step": "receipt", "file": to }))?;
            }
            continue;
        }
        for to in keep_receipt(from, own, &roots, batch, name)? {
            ledger.record(json!({ "step": "receipt", "file": to }))?;
        }
    }
    // Other receipts left in a folder (such as a downloader's own listing)
    // are kept beside the provider's; anything else stays where it is.
    for part in parts.iter().filter(|_| refused.is_empty()) {
        for e in fs::read_dir(&part.inc).into_iter().flatten().flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            if name == "manifest.json" || !name.ends_with(".json") || !safe(&name) {
                continue;
            }
            let own = prov(&part.root).join(&name);
            if own.exists() {
                if hash_file(&own)? == hash_file(&e.path())? {
                    fs::remove_file(e.path()).map_err(|e| e.to_string())?;
                }
                continue;
            }
            for to in keep_receipt(&e.path(), &part.root, &roots, batch, &name)? {
                ledger.record(json!({ "step": "receipt", "file": to }))?;
            }
        }
    }
    // The manifest doesn't list itself; it is always kept, on every volume.
    for root in &roots {
        let kept = prov(root).join("manifest.json");
        if !kept.exists() {
            write_new(&kept, kept_manifest.as_bytes())
                .map_err(|e| format!("{}: {e}", kept.display()))?;
        }
        ledger.record(json!({ "step": "receipt", "file": kept }))?;
    }
    // The folders are empty now; remove them (never recursively). Anything
    // the store didn't recognise keeps its folder there, and is reported.
    let mut left = Vec::new();
    for part in parts.iter().filter(|_| refused.is_empty()) {
        let _ = fs::remove_file(part.inc.join("manifest.json"));
        left.extend(
            fs::read_dir(&part.inc)
                .into_iter()
                .flatten()
                .flatten()
                .map(|e| part.inc.join(e.file_name()).display().to_string()),
        );
        let _ = fs::remove_dir(&part.inc);
        let _ = fs::remove_dir(part.root.join(INCOMING));
    }
    let summary = json!({
        "batch": batch, "dataset": dataset, "schema": schema, "symbols": symbols,
        "files": placed, "refused": refused, "receipts": receipt_names,
        "provenance": roots.iter().map(|r| prov(r)).collect::<Vec<_>>(),
        "from": parts.iter().map(|p| &p.inc).collect::<Vec<_>>(),
        "left_behind": left, "completed_at": tinytrader_observe::unix_seconds(),
    });
    let text = serde_json::to_string_pretty(&summary).map_err(|e| e.to_string())?;
    for root in &roots {
        let mut out = root
            .join("migrations")
            .join(format!("{today}-incorporate-{batch}.json"));
        if out.exists() {
            out = root.join("migrations").join(format!(
                "{today}-incorporate-{batch}-{}.json",
                tinytrader_observe::unix_seconds() as u64
            ));
        }
        write_new(&out, text.as_bytes()).map_err(|e| format!("{}: {e}", out.display()))?;
    }
    ledger.record(json!({ "step": "complete" }))?;
    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!(
            "tinytrader-store-test-{name}-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&d);
        fs::create_dir_all(&d).unwrap();
        d
    }

    fn upload(dir: &Path, name: &str, bytes: &[u8]) -> String {
        let mut u = Upload::start(dir, name).unwrap();
        u.write(bytes).unwrap();
        u.finish(Some(bytes.len() as u64), None).unwrap().1
    }

    /// Stage a Databento batch of one day for `symbols` in `root`.
    /// A real zstd DBN file for OPRA.PILLAR cmbp-1 on 2025-10-01; `label`
    /// makes different labels give different bytes.
    fn dbn_day(label: &[u8]) -> Vec<u8> {
        dbn_file("OPRA.PILLAR", dbn::Schema::Cmbp1, "2025-10-01", label)
    }

    fn dbn_file(dataset: &str, schema: dbn::Schema, day: &str, label: &[u8]) -> Vec<u8> {
        let start = day_ns(day).unwrap() + label.iter().map(|b| u64::from(*b)).sum::<u64>();
        let metadata = dbn::Metadata::builder()
            .dataset(dataset.to_owned())
            .schema(Some(schema))
            .start(start)
            .end(std::num::NonZeroU64::new(
                day_ns(day).unwrap() + 86_400 * 1_000_000_000,
            ))
            .stype_in(Some(dbn::SType::Parent))
            .stype_out(dbn::SType::InstrumentId)
            .build();
        let mut out = Vec::new();
        // The zstd stream is finished when the encoder is dropped.
        drop(dbn::encode::dbn::Encoder::with_zstd(&mut out, &metadata).unwrap());
        out
    }

    fn stage(root: &Path, batch: &str, symbols: &[&str], label: &[u8]) {
        stage_bytes(root, batch, symbols, &dbn_day(label));
    }

    fn stage_bytes(root: &Path, batch: &str, symbols: &[&str], data: &[u8]) {
        let inc = root.join(INCOMING).join(batch);
        let data_sha = upload(&inc, "opra-pillar-20251001.cmbp-1.dbn.zst", data);
        let metadata = json!({
            "job_id": batch,
            "query": { "dataset": "OPRA.PILLAR", "schema": "cmbp-1", "symbols": symbols, "encoding": "dbn", "compression": "zstd" },
            "customizations": { "split_symbols": false, "split_duration": "day" },
        })
        .to_string();
        let meta_sha = upload(&inc, "metadata.json", metadata.as_bytes());
        let manifest = json!({ "job_id": batch, "files": [
            { "filename": "opra-pillar-20251001.cmbp-1.dbn.zst", "size": data.len(), "hash": format!("sha256:{data_sha}"), "urls": { "https": "secret" } },
            { "filename": "metadata.json", "size": metadata.len(), "hash": format!("sha256:{meta_sha}"), "urls": { "https": "secret" } },
        ] })
        .to_string();
        upload(&inc, "manifest.json", manifest.as_bytes());
    }

    const DAY: &str = "OPRA.PILLAR/SPX.OPT/cmbp-1/2025-10-01.dbn.zst";

    /// A batch incoming on one volume.
    fn one(root: &Path, batch: &str) -> Vec<Part> {
        vec![Part {
            inc: root.join(INCOMING).join(batch),
            root: root.to_path_buf(),
        }]
    }

    #[test]
    fn files_a_batch_spread_over_two_volumes() {
        // Day one and the receipts arrived on volume a, day two on volume b
        // (each placed where there was most room); each day is filed on its
        // own volume, and both volumes get the receipts and the record.
        let d = scratch("spread");
        let (a, b) = (d.join("a/Databento"), d.join("b/Databento"));
        let (ia, ib) = (
            a.join(INCOMING).join("OPRA-S"),
            b.join(INCOMING).join("OPRA-S"),
        );
        let one_day = dbn_day(b"one");
        let two_day = dbn_file("OPRA.PILLAR", dbn::Schema::Cmbp1, "2025-10-02", b"two");
        let sha1 = upload(&ia, "opra-pillar-20251001.cmbp-1.dbn.zst", &one_day);
        let sha2 = upload(&ib, "opra-pillar-20251002.cmbp-1.dbn.zst", &two_day);
        let metadata = json!({
            "query": { "dataset": "OPRA.PILLAR", "schema": "cmbp-1", "symbols": ["SPX.OPT"], "encoding": "dbn", "compression": "zstd" },
            "customizations": { "split_symbols": false, "split_duration": "day" },
        })
        .to_string();
        let msha = upload(&ia, "metadata.json", metadata.as_bytes());
        let manifest = json!({ "files": [
            { "filename": "opra-pillar-20251001.cmbp-1.dbn.zst", "size": one_day.len(), "hash": format!("sha256:{sha1}") },
            { "filename": "opra-pillar-20251002.cmbp-1.dbn.zst", "size": two_day.len(), "hash": format!("sha256:{sha2}") },
            { "filename": "metadata.json", "size": metadata.len(), "hash": format!("sha256:{msha}") },
        ] })
        .to_string();
        upload(&ia, "manifest.json", manifest.as_bytes());
        let parts = vec![
            Part {
                inc: ia.clone(),
                root: a.clone(),
            },
            Part {
                inc: ib.clone(),
                root: b.clone(),
            },
        ];
        let s = incorporate(&parts, &[], "OPRA-S", "2026-10-06").unwrap();
        assert_eq!(s["files"].as_array().unwrap().len(), 2);
        assert_eq!(fs::read(a.join(DAY)).unwrap(), one_day);
        assert_eq!(
            fs::read(b.join("OPRA.PILLAR/SPX.OPT/cmbp-1/2025-10-02.dbn.zst")).unwrap(),
            two_day
        );
        assert!(
            !b.join(DAY).exists()
                && !a
                    .join("OPRA.PILLAR/SPX.OPT/cmbp-1/2025-10-02.dbn.zst")
                    .exists()
        );
        for root in [&a, &b] {
            assert!(
                root.join("provenance/OPRA-S/manifest.json").exists(),
                "{}",
                root.display()
            );
            assert!(
                root.join("provenance/OPRA-S/metadata.json").exists(),
                "{}",
                root.display()
            );
            assert!(
                root.join("migrations/2026-10-06-incorporate-OPRA-S.json")
                    .exists()
            );
            assert!(!root.join(INCOMING).exists());
        }
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn a_day_on_another_volume_is_still_checked() {
        // A day already filed on b, different bytes, refuses the part on a.
        let d = scratch("spread-conflict");
        let (a, b) = (d.join("a/Databento"), d.join("b/Databento"));
        fs::create_dir_all(b.join(DAY).parent().unwrap()).unwrap();
        fs::write(b.join(DAY), b"what b had").unwrap();
        stage(&a, "OPRA-T", &["SPX.OPT"], b"other");
        let parts = one(&a, "OPRA-T");
        let err =
            incorporate(&parts, std::slice::from_ref(&b), "OPRA-T", "2026-10-06").unwrap_err();
        assert!(err.contains("already holds a different file"), "{err}");
        assert_eq!(fs::read(b.join(DAY)).unwrap(), b"what b had");
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn refuses_a_file_that_does_not_match() {
        let d = scratch("mismatch");
        let mut u = Upload::start(&d, "x.dbn.zst").unwrap();
        u.write(b"abc").unwrap();
        assert!(u.finish(Some(3), Some("sha256:00")).is_err());
        assert!(!d.join("x.dbn.zst").exists() && !d.join("x.dbn.zst.partial").exists());
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn files_a_batch_in_the_standard_layout() {
        let root = scratch("batch").join("Databento");
        stage(&root, "OPRA-1", &["SPX.OPT"], b"day one");
        let s = incorporate(&one(&root, "OPRA-1"), &[], "OPRA-1", "2026-10-06").unwrap();
        assert_eq!(fs::read(root.join(DAY)).unwrap(), dbn_day(b"day one"));
        assert_eq!(s["files"].as_array().unwrap().len(), 1);
        let kept = fs::read_to_string(root.join("provenance/OPRA-1/manifest.json")).unwrap();
        assert!(!kept.contains("secret"), "download URLs removed");
        assert!(root.join("provenance/OPRA-1/metadata.json").exists());
        assert!(!root.join(INCOMING).exists(), "incoming removed once empty");
        assert!(
            root.join("migrations/2026-10-06-incorporate-OPRA-1.json")
                .exists()
        );
        let _ = fs::remove_dir_all(root.parent().unwrap());
    }

    #[test]
    fn records_each_filed_days_condition() {
        let root = scratch("conditions").join("Databento");
        stage(&root, "OPRA-C", &["SPX.OPT"], b"day one");
        let rows = json!([
            { "date": "2025-10-01", "condition": "degraded", "last_modified_date": "2025-10-03" },
            { "date": "2025-10-02", "condition": "available", "last_modified_date": "2025-10-03" },
        ]);
        fs::write(
            root.join(INCOMING).join("OPRA-C/condition.json"),
            rows.to_string(),
        )
        .unwrap();
        incorporate(&one(&root, "OPRA-C"), &[], "OPRA-C", "2026-10-06").unwrap();
        let days = crate::partial::read_manifest(root.join(DAY).parent().unwrap()).unwrap();
        let c = &days["2025-10-01"]["condition"];
        assert_eq!(
            (
                c["state"].clone(),
                c["last_modified"].clone(),
                c["source"].clone()
            ),
            (
                json!("degraded"),
                json!("2025-10-03"),
                json!("batch OPRA-C")
            )
        );
        assert_eq!(days.len(), 1, "only the days filed");
        assert!(
            root.join("provenance/OPRA-C/condition.json").exists(),
            "the receipt is kept"
        );
        let vol = root.parent().unwrap();
        let v = [("v".to_owned(), vol.to_path_buf())];
        let cov = crate::partial::coverage(
            &v,
            "databento",
            "OPRA.PILLAR",
            Some("SPX.OPT"),
            "cmbp-1",
            "2025-10-01",
            "2025-10-01",
        )
        .unwrap();
        assert_eq!(
            (cov[0]["kind"].clone(), cov[0]["condition"]["state"].clone()),
            (json!("complete"), json!("degraded"))
        );
        let _ = fs::remove_dir_all(vol);
    }

    /// An Alpaca sidecar batch (its header names no schema) is filed under
    /// its extension schema, trade and quote conditions alike; a header that
    /// names a schema is refused there, and an unknown schema is refused.
    #[test]
    fn files_extension_schema_sidecars() {
        let vol = scratch("sidecars");
        let root = vol.join("Alpaca");
        let day = "2026-10-01";
        let sidecar = |schema: Option<dbn::Schema>| {
            let start = day_ns(day).unwrap();
            let metadata = dbn::Metadata::builder()
                .dataset("ALPACA.SIP".to_owned())
                .schema(schema)
                .start(start)
                .end(std::num::NonZeroU64::new(start + 86_400 * 1_000_000_000))
                .stype_in(Some(dbn::SType::RawSymbol))
                .stype_out(dbn::SType::InstrumentId)
                .build();
            let mut out = Vec::new();
            drop(dbn::encode::dbn::Encoder::with_zstd(&mut out, &metadata).unwrap());
            out
        };
        let stage = |root: &Path, batch: &str, schema: &str, data: &[u8]| {
            let inc = root.join(INCOMING).join(batch);
            let name = format!("alpaca-sip-20261001.{schema}.dbn.zst");
            let sha = upload(&inc, &name, data);
            let metadata = json!({
                "job_id": batch,
                "query": { "dataset": "ALPACA.SIP", "schema": schema, "symbols": ["SPY"], "stype_in": "raw_symbol", "encoding": "dbn", "compression": "zstd" },
                "customizations": { "split_symbols": false, "split_duration": "day" },
            })
            .to_string();
            let msha = upload(&inc, "metadata.json", metadata.as_bytes());
            let manifest = json!({ "job_id": batch, "files": [
                { "filename": name, "size": data.len(), "hash": format!("sha256:{sha}") },
                { "filename": "metadata.json", "size": metadata.len(), "hash": format!("sha256:{msha}") },
            ] })
            .to_string();
            upload(&inc, "manifest.json", manifest.as_bytes());
        };
        for (batch, schema) in [
            ("ALPACA-SIP-SPY-20261001-conditions", "conditions"),
            (
                "ALPACA-SIP-SPY-20261001-quote-conditions",
                "quote-conditions",
            ),
        ] {
            let data = sidecar(None);
            stage(&root, batch, schema, &data);
            incorporate(&one(&root, batch), &[], batch, "2026-10-06").unwrap();
            assert_eq!(
                fs::read(root.join(format!("ALPACA.SIP/SPY/{schema}/{day}.dbn.zst"))).unwrap(),
                data,
                "{schema}"
            );
        }
        // A sidecar whose header names a DBN schema isn't what its name says.
        let other = vol.join("other/Alpaca");
        stage(
            &other,
            "QC-NAMED",
            "quote-conditions",
            &sidecar(Some(dbn::Schema::Cmbp1)),
        );
        let err = incorporate(&one(&other, "QC-NAMED"), &[], "QC-NAMED", "2026-10-06").unwrap_err();
        assert!(err.contains("its header says schema"), "{err}");
        // Nor is an extension the store doesn't know.
        stage(&other, "QC-UNKNOWN", "book-conditions", &sidecar(None));
        let err =
            incorporate(&one(&other, "QC-UNKNOWN"), &[], "QC-UNKNOWN", "2026-10-06").unwrap_err();
        assert!(err.contains("unknown schema"), "{err}");
        let _ = fs::remove_dir_all(vol);
    }

    #[test]
    fn never_replaces_a_different_day() {
        let root = scratch("conflict").join("Databento");
        fs::create_dir_all(root.join(DAY).parent().unwrap()).unwrap();
        fs::write(root.join(DAY), b"what we had").unwrap();
        stage(&root, "OPRA-2", &["SPX.OPT"], b"something else");
        let err = incorporate(&one(&root, "OPRA-2"), &[], "OPRA-2", "2026-10-06").unwrap_err();
        assert!(err.contains("already holds a different file"), "{err}");
        assert_eq!(fs::read(root.join(DAY)).unwrap(), b"what we had");
        assert!(
            root.join(INCOMING)
                .join("OPRA-2/opra-pillar-20251001.cmbp-1.dbn.zst")
                .exists(),
            "nothing moved"
        );
        assert!(
            !root.join("provenance/OPRA-2").exists(),
            "no receipts filed"
        );
        let _ = fs::remove_dir_all(root.parent().unwrap());
    }

    #[test]
    fn checks_the_other_volumes_too() {
        let base = scratch("volumes");
        let (here, there) = (base.join("new/Databento"), base.join("old/Databento"));
        fs::create_dir_all(there.join(DAY).parent().unwrap()).unwrap();
        fs::write(there.join(DAY), dbn_day(b"held elsewhere")).unwrap();

        // A different day elsewhere: refused.
        stage(&here, "OPRA-3", &["SPX.OPT"], b"not the same");
        let err = incorporate(
            &one(&here, "OPRA-3"),
            std::slice::from_ref(&there),
            "OPRA-3",
            "2026-10-06",
        )
        .unwrap_err();
        assert!(err.contains("already holds a different file"), "{err}");
        assert!(!here.join(DAY).exists());

        // The same bytes elsewhere: a proven duplicate, not a second copy.
        let other = base.join("other/Databento");
        stage(&other, "OPRA-4", &["SPX.OPT"], b"held elsewhere");
        let s = incorporate(
            &one(&other, "OPRA-4"),
            std::slice::from_ref(&there),
            "OPRA-4",
            "2026-10-06",
        )
        .unwrap();
        assert!(!other.join(DAY).exists(), "no second copy");
        assert_eq!(s["files"][0]["already_present"], true);
        assert_eq!(
            fs::read(there.join(DAY)).unwrap(),
            dbn_day(b"held elsewhere")
        );
        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn refuses_batches_of_several_symbols() {
        let root = scratch("symbols").join("Databento");
        stage(&root, "OPRA-5", &["SPY.OPT", "QQQ.OPT"], b"two parents");
        let err = incorporate(&one(&root, "OPRA-5"), &[], "OPRA-5", "2026-10-06").unwrap_err();
        assert!(err.contains("one symbol"), "{err}");
        assert!(!root.join("OPRA.PILLAR").exists());
        let _ = fs::remove_dir_all(root.parent().unwrap());
    }

    #[test]
    fn filing_the_same_batch_again_changes_nothing() {
        let root = scratch("again").join("Databento");
        stage(&root, "OPRA-6", &["SPX.OPT"], b"day one");
        incorporate(&one(&root, "OPRA-6"), &[], "OPRA-6", "2026-10-06").unwrap();
        let receipt = fs::read(root.join("provenance/OPRA-6/manifest.json")).unwrap();
        stage(&root, "OPRA-6", &["SPX.OPT"], b"day one");
        let s = incorporate(&one(&root, "OPRA-6"), &[], "OPRA-6", "2026-10-06").unwrap();
        assert_eq!(s["files"][0]["already_present"], true);
        assert_eq!(fs::read(root.join(DAY)).unwrap(), dbn_day(b"day one"));
        assert_eq!(
            fs::read(root.join("provenance/OPRA-6/manifest.json")).unwrap(),
            receipt
        );
        let summaries = fs::read_dir(root.join("migrations"))
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".json")
            })
            .count();
        assert_eq!(summaries, 2, "the first summary is kept, not rewritten");
        let _ = fs::remove_dir_all(root.parent().unwrap());
    }

    /// A filing that began and never completed (an I/O error mid-filing) left
    /// receipts that differ from the refetched batch's: they're set aside, on
    /// record, and the batch files. One that completed still refuses.
    #[test]
    fn replaces_the_provenance_of_an_interrupted_filing() {
        let root = scratch("interrupted").join("Databento");
        fs::create_dir_all(root.join("provenance/OPRA-I")).unwrap();
        fs::write(root.join("provenance/OPRA-I/manifest.json"), "{}").unwrap();
        fs::create_dir_all(root.join("migrations")).unwrap();
        fs::write(
            root.join("migrations/2026-10-05-incorporate-OPRA-I.events.jsonl"),
            "{\"step\":\"begin\",\"batch\":\"OPRA-I\"}\n",
        )
        .unwrap();
        stage(&root, "OPRA-I", &["SPX.OPT"], b"day one");
        incorporate(&one(&root, "OPRA-I"), &[], "OPRA-I", "2026-10-06").unwrap();
        assert!(root.join(DAY).exists());
        assert_ne!(
            fs::read_to_string(root.join("provenance/OPRA-I/manifest.json")).unwrap(),
            "{}"
        );
        let aside: Vec<PathBuf> = fs::read_dir(root.join("provenance"))
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.to_string_lossy().contains("OPRA-I.interrupted-"))
            .collect();
        assert_eq!(aside.len(), 1, "set aside, not deleted");
        assert_eq!(
            fs::read_to_string(aside[0].join("manifest.json")).unwrap(),
            "{}"
        );
        let ledger =
            fs::read_to_string(root.join("migrations/2026-10-06-incorporate-OPRA-I.events.jsonl"))
                .unwrap();
        assert!(ledger.contains("provenance-set-aside"), "{ledger}");
        let _ = fs::remove_dir_all(root.parent().unwrap());

        // Completed earlier (its ledger says so): a different receipt still refuses.
        let root = scratch("completed").join("Databento");
        fs::create_dir_all(root.join("provenance/OPRA-J")).unwrap();
        fs::write(root.join("provenance/OPRA-J/manifest.json"), "{}").unwrap();
        fs::create_dir_all(root.join("migrations")).unwrap();
        fs::write(
            root.join("migrations/2026-10-05-incorporate-OPRA-J.events.jsonl"),
            "{\"step\":\"begin\"}\n{\"step\":\"complete\"}\n",
        )
        .unwrap();
        stage(&root, "OPRA-J", &["SPX.OPT"], b"day one");
        let err = incorporate(&one(&root, "OPRA-J"), &[], "OPRA-J", "2026-10-06").unwrap_err();
        assert!(err.contains("different manifest.json"), "{err}");
        assert!(!root.join(DAY).exists());
        let _ = fs::remove_dir_all(root.parent().unwrap());
    }

    #[test]
    fn refuses_a_different_receipt_for_the_same_batch() {
        let root = scratch("receipt").join("Databento");
        fs::create_dir_all(root.join("provenance/OPRA-7")).unwrap();
        fs::write(root.join("provenance/OPRA-7/manifest.json"), "{}").unwrap();
        stage(&root, "OPRA-7", &["SPX.OPT"], b"day one");
        let err = incorporate(&one(&root, "OPRA-7"), &[], "OPRA-7", "2026-10-06").unwrap_err();
        assert!(err.contains("different manifest.json"), "{err}");
        assert!(!root.join(DAY).exists());
        assert_eq!(
            fs::read_to_string(root.join("provenance/OPRA-7/manifest.json")).unwrap(),
            "{}"
        );
        let _ = fs::remove_dir_all(root.parent().unwrap());
    }

    #[test]
    fn resumes_after_an_interruption() {
        let root = scratch("resume").join("Databento");
        stage(&root, "OPRA-8", &["SPX.OPT"], b"day one");
        // As if a run stopped right after moving the data file.
        fs::create_dir_all(root.join(DAY).parent().unwrap()).unwrap();
        fs::rename(
            root.join(INCOMING)
                .join("OPRA-8/opra-pillar-20251001.cmbp-1.dbn.zst"),
            root.join(DAY),
        )
        .unwrap();
        incorporate(&one(&root, "OPRA-8"), &[], "OPRA-8", "2026-10-06").unwrap();
        assert_eq!(fs::read(root.join(DAY)).unwrap(), dbn_day(b"day one"));
        assert!(root.join("provenance/OPRA-8/manifest.json").exists());
        assert!(!root.join(INCOMING).exists());
        let _ = fs::remove_dir_all(root.parent().unwrap());
    }

    #[test]
    fn adopts_a_download_folder_in_place() {
        let base = scratch("adopt");
        let root = base.join("Databento");
        // A download folder at the top of the volume, with an extra receipt.
        stage(&base, "OPRA-9", &["SPX.OPT"], b"day one");
        let dir = base.join("OPRA-9");
        fs::rename(base.join(INCOMING).join("OPRA-9"), &dir).unwrap();
        fs::write(dir.join("_batch_listing.json"), "[]").unwrap();
        let s = adopt(&dir, &root, &[], "2026-10-06").unwrap();
        assert_eq!(fs::read(root.join(DAY)).unwrap(), dbn_day(b"day one"));
        assert!(root.join("provenance/OPRA-9/_batch_listing.json").exists());
        assert!(!dir.exists(), "the folder is gone once filed");
        assert_eq!(s["left_behind"], json!([]));

        // A folder that fails a check is left exactly as it was.
        stage(&base, "OPRA-10", &["SPX.OPT"], b"different day one");
        let bad = base.join("OPRA-10");
        fs::rename(base.join(INCOMING).join("OPRA-10"), &bad).unwrap();
        assert!(adopt(&bad, &root, &[], "2026-10-06").is_err());
        assert_eq!(fs::read_dir(&bad).unwrap().count(), 3);
        assert_eq!(fs::read(root.join(DAY)).unwrap(), dbn_day(b"day one"));
        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn refuses_a_file_whose_header_disagrees() {
        let root = scratch("header").join("Databento");
        // Intact (the manifest vouches for it) but really a trades file for another day.
        stage_bytes(
            &root,
            "OPRA-11",
            &["SPX.OPT"],
            &dbn_file("OPRA.PILLAR", dbn::Schema::Trades, "2025-10-01", b""),
        );
        let err = incorporate(&one(&root, "OPRA-11"), &[], "OPRA-11", "2026-10-06").unwrap_err();
        assert!(err.contains("schema"), "{err}");
        stage_bytes(
            &root,
            "OPRA-12",
            &["SPX.OPT"],
            &dbn_file("OPRA.PILLAR", dbn::Schema::Cmbp1, "2025-10-02", b""),
        );
        let err = incorporate(&one(&root, "OPRA-12"), &[], "OPRA-12", "2026-10-06").unwrap_err();
        assert!(err.contains("not 2025-10-01"), "{err}");
        stage_bytes(&root, "OPRA-13", &["SPX.OPT"], b"not dbn at all");
        assert!(
            incorporate(&one(&root, "OPRA-13"), &[], "OPRA-13", "2026-10-06")
                .unwrap_err()
                .contains("not a readable DBN file")
        );
        assert!(!root.join(DAY).exists(), "nothing filed");
        let _ = fs::remove_dir_all(root.parent().unwrap());
    }

    /// Against a real filed day (DBN as Databento delivers it, which may be an
    /// older DBN version): `TT_REAL_DBN=<path> TT_REAL_DAY=<YYYY-MM-DD> TT_REAL_SCHEMA=<schema>
    /// cargo test -p store real_file -- --ignored`.
    #[test]
    #[ignore = "needs a real Databento file"]
    fn real_file_passes_the_header_check() {
        let (path, day, schema) = (
            std::env::var("TT_REAL_DBN").unwrap(),
            std::env::var("TT_REAL_DAY").unwrap(),
            std::env::var("TT_REAL_SCHEMA").unwrap(),
        );
        check_header(Path::new(&path), "OPRA.PILLAR", &schema, &day).unwrap();
        assert!(check_header(Path::new(&path), "OPRA.PILLAR", &schema, "2020-01-02").is_err());
    }

    mod supersede {
        use super::*;
        use crate::partial::tests::{
            C1, C2, CRASH, DAY, at, day_bytes, import, name, stage, whole_bytes,
        };

        const WHOLE: &str = "opra-pillar-20261002.cbbo-1m.dbn.zst";
        const AT: &str = "OPRA.PILLAR/SPY.OPT/cbbo-1m";

        /// The partial day held: C1 (11) and C2 (12) from 13:00 UTC.
        fn partial() -> Vec<u8> {
            day_bytes(&[(C1, 11), (C2, 12)], &[(11, 0), (12, 0), (11, 1)])
        }

        /// The whole chain: the partial day's records, C1 before its hours,
        /// and two other contracts.
        fn chain() -> Vec<(u32, u64)> {
            let early = day_ns(DAY).unwrap() + 3_600_000_000_000;
            vec![
                (11, early),
                (11, at(0)),
                (12, at(0)),
                (13, at(0)),
                (11, at(1)),
                (14, at(2)),
            ]
        }

        fn whole() -> Vec<u8> {
            whole_bytes(&chain())
        }

        /// A volume holding the partial day, filed by import.
        fn holding(root: &Path) {
            let part = stage(root, "I1", &[(&name(), partial())]);
            import(&[part], &[], "I1", "2026-10-19").unwrap();
        }

        /// A batch of `SPY.OPT`'s chain for `DAY`, requested by `stype_in`.
        fn stage_whole(root: &Path, batch: &str, data: &[u8], stype_in: &str) -> Vec<Part> {
            stage_days(root, batch, &[(WHOLE, data.to_vec())], stype_in)
        }

        /// A batch of `SPY.OPT`'s chain, its day files `(name, bytes)`.
        fn stage_days(
            root: &Path,
            batch: &str,
            days: &[(&str, Vec<u8>)],
            stype_in: &str,
        ) -> Vec<Part> {
            let inc = root.join(INCOMING).join(batch);
            let metadata = json!({
                "query": { "dataset": "OPRA.PILLAR", "schema": "cbbo-1m", "symbols": ["SPY.OPT"], "stype_in": stype_in, "encoding": "dbn", "compression": "zstd" },
                "customizations": { "split_symbols": false, "split_duration": "day" },
            })
            .to_string();
            let mut files: Vec<Value> = days
                .iter()
                .map(|(name, data)| {
                    let sha = upload(&inc, name, data);
                    json!({ "filename": name, "size": data.len(), "hash": format!("sha256:{sha}") })
                })
                .collect();
            let msha = upload(&inc, "metadata.json", metadata.as_bytes());
            files.push(json!({ "filename": "metadata.json", "size": metadata.len(), "hash": format!("sha256:{msha}") }));
            upload(
                &inc,
                "manifest.json",
                json!({ "files": files }).to_string().as_bytes(),
            );
            one(root, batch)
        }

        fn day(root: &Path) -> PathBuf {
            root.join(AT).join(format!("{DAY}.dbn.zst"))
        }

        fn kept(root: &Path, batch: &str) -> PathBuf {
            root.join("superseded")
                .join(batch)
                .join(AT)
                .join(format!("{DAY}.dbn.zst"))
        }

        fn entry(root: &Path) -> Value {
            crate::manifest::read_day(&root.join(AT), DAY)
                .unwrap()
                .unwrap()
        }

        fn steps(root: &Path, batch: &str) -> Vec<Value> {
            let path = root.join(format!(
                "migrations/2026-10-27-incorporate-{batch}.events.jsonl"
            ));
            fs::read_to_string(path)
                .unwrap_or_default()
                .lines()
                .map(|l| serde_json::from_str(l).unwrap())
                .collect()
        }

        /// The day superseded on `held`, filed from `root`: the whole chain
        /// in place, complete, and the partial day kept, readable, with its
        /// entry.
        fn superseded(root: &Path, held: &Path, batch: &str) {
            assert_eq!(fs::read(day(held)).unwrap(), whole());
            let e = entry(held);
            assert!(!crate::manifest::is_partial(&e), "{e}");
            assert_eq!(
                (e["records"].clone(), e["source"].clone()),
                (json!(6), json!(format!("batch/{batch}")))
            );
            assert!(
                !held
                    .join(AT)
                    .join(format!("manifest.d/{DAY}.json"))
                    .exists()
            );
            assert_eq!(fs::read(kept(held, batch)).unwrap(), partial());
            let (cov, n) = crate::partial::inspect(&kept(held, batch)).unwrap();
            assert_eq!(
                (cov.symbols().len(), n),
                (2, 3),
                "the old file reads as it did"
            );
            let old: Value = serde_json::from_str(
                &fs::read_to_string(kept(held, batch).with_file_name(format!("{DAY}.json")))
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(old["source"], "import/I1");
            assert!(
                root.join(format!("provenance/{batch}/manifest.json"))
                    .exists()
            );
            assert!(!root.join(INCOMING).exists());
        }

        #[test]
        fn a_whole_chain_supersedes_a_partial_day() {
            let root = scratch("supersede").join("Databento");
            holding(&root);
            let sha = hash_file(&day(&root)).unwrap();
            let parts = stage_whole(&root, "OPRA-W", &whole(), "parent");
            let s = incorporate(&parts, &[], "OPRA-W", "2026-10-27").unwrap();
            superseded(&root, &root, "OPRA-W");
            assert_eq!(s["files"][0]["superseded"]["sha256"], sha.as_str());
            let step = steps(&root, "OPRA-W")
                .into_iter()
                .find(|e| e["step"] == "superseded")
                .unwrap();
            assert_eq!(
                (
                    step["old"]["sha256"].clone(),
                    step["old"]["records"].clone(),
                    step["new"]["records"].clone(),
                    step["matched"].clone()
                ),
                (json!(sha), json!(3), json!(6), json!(3))
            );
            let vol = root.parent().unwrap();
            let v = [("v".to_owned(), vol.to_path_buf())];
            let cov = crate::partial::coverage(
                &v,
                "databento",
                "OPRA.PILLAR",
                Some("SPY.OPT"),
                "cbbo-1m",
                DAY,
                DAY,
            )
            .unwrap();
            assert_eq!(
                (
                    cov.len(),
                    cov[0]["kind"].clone(),
                    cov[0]["conflict"].clone()
                ),
                (1, json!("complete"), json!(false)),
                "the kept file isn't counted"
            );

            // Again, after success: its own work, recognised.
            let parts = stage_whole(&root, "OPRA-W", &whole(), "parent");
            let s = incorporate(&parts, &[], "OPRA-W", "2026-10-27").unwrap();
            assert_eq!(s["files"][0]["already_present"], true);
            assert_eq!(s["files"][0]["superseded"]["sha256"], sha.as_str());
            superseded(&root, &root, "OPRA-W");
            let _ = fs::remove_dir_all(vol);
        }

        #[test]
        fn refuses_a_chain_without_every_record_of_the_partial_day() {
            let root = scratch("supersede-missing").join("Databento");
            holding(&root);
            let missing: Vec<_> = chain().into_iter().filter(|r| *r != (12, at(0))).collect();
            let mut extra = chain();
            extra.push((11, at(5)));
            for (batch, bytes, stype, why) in [
                (
                    "OPRA-M",
                    whole_bytes(&missing),
                    "parent",
                    format!("has 1 records of {C2} over its hours and this file 0"),
                ),
                (
                    "OPRA-X",
                    whole_bytes(&extra),
                    "parent",
                    format!("has 2 records of {C1} over its hours and this file 3"),
                ),
                // Not a parent's whole chain: refused as before.
                (
                    "OPRA-L",
                    whole(),
                    "raw_symbol",
                    "already holds a different file".to_owned(),
                ),
            ] {
                let parts = stage_whole(&root, batch, &bytes, stype);
                // A whole chain's day is refused alone; any other batch whole.
                let err = match incorporate(&parts, &[], batch, "2026-10-27") {
                    Ok(s) => {
                        assert_eq!(s["files"], json!([]), "{s}");
                        s["refused"][0]["why"].as_str().unwrap().to_owned()
                    }
                    Err(e) => e,
                };
                assert!(err.contains(&why), "{err}");
                assert_eq!(fs::read(day(&root)).unwrap(), partial(), "unchanged");
                assert!(crate::manifest::is_partial(&entry(&root)));
                assert!(
                    root.join(INCOMING).join(batch).join(WHOLE).exists(),
                    "nothing moved"
                );
            }
            assert!(!root.join("superseded").exists());
            let _ = fs::remove_dir_all(root.parent().unwrap());
        }

        #[test]
        fn a_whole_chain_refuses_its_days_one_by_one() {
            let root = scratch("supersede-by-day").join("Databento");
            holding(&root);
            // 10-05 held complete, as an older route filed it; 10-06 not held.
            let held = root.join(AT).join("2026-10-05.dbn.zst");
            fs::write(&held, b"filed by an older route").unwrap();
            let day_of = |d: &str| dbn_file("OPRA.PILLAR", dbn::Schema::Cbbo1M, d, b"chain");
            let (five, six) = (
                "opra-pillar-20261005.cbbo-1m.dbn.zst",
                "opra-pillar-20261006.cbbo-1m.dbn.zst",
            );
            let days = [
                (WHOLE, whole()),
                (five, day_of("2026-10-05")),
                (six, day_of("2026-10-06")),
            ];
            let parts = stage_days(&root, "OPRA-D", &days, "parent");
            let filed = |s: &Value| -> Vec<(String, String)> {
                let mut v: Vec<_> = s["files"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|f| {
                        let p = f["path"].as_str().unwrap();
                        (
                            p[p.len() - 18..].to_owned(),
                            f["filed"].as_str().unwrap().to_owned(),
                        )
                    })
                    .collect();
                v.sort();
                v
            };
            let refused = |s: &Value| -> Vec<String> {
                s["refused"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|r| {
                        format!(
                            "{} {}",
                            r["day"].as_str().unwrap(),
                            r["why"].as_str().unwrap()
                        )
                    })
                    .collect()
            };
            // The partial day's volume read-only: refused up front, as is
            // 10-05; 10-06 is filed, and the batch waits in incoming.
            let s = incorporate_on(
                &parts,
                &[],
                std::slice::from_ref(&root),
                "OPRA-D",
                "2026-10-27",
            )
            .unwrap();
            assert_eq!(
                filed(&s),
                [("2026-10-06.dbn.zst".to_owned(), "new".to_owned())]
            );
            let r = refused(&s);
            assert!(
                r[0].starts_with("2026-10-02") && r[0].contains("read-only volume"),
                "{r:?}"
            );
            assert!(
                r[1].starts_with("2026-10-05") && r[1].contains("already holds a different file"),
                "{r:?}"
            );
            assert_eq!(fs::read(day(&root)).unwrap(), partial(), "unchanged");
            assert_eq!(fs::read(&held).unwrap(), b"filed by an older route");
            let inc = root.join(INCOMING).join("OPRA-D");
            assert!(inc.join(WHOLE).exists() && inc.join(five).exists() && !inc.join(six).exists());
            assert!(inc.join("manifest.json").exists() && inc.join("metadata.json").exists());
            assert!(root.join("provenance/OPRA-D/manifest.json").exists());
            assert!(root.join("provenance/OPRA-D/metadata.json").exists());

            // Again, writable: 10-02 superseded, 10-06 recognised, 10-05 still refused.
            let s = incorporate_on(&parts, &[], &[], "OPRA-D", "2026-10-27").unwrap();
            assert_eq!(
                filed(&s),
                [
                    ("2026-10-02.dbn.zst".to_owned(), "superseded".to_owned()),
                    (
                        "2026-10-06.dbn.zst".to_owned(),
                        "already present".to_owned()
                    )
                ]
            );
            assert_eq!(refused(&s).len(), 1);
            assert_eq!(fs::read(day(&root)).unwrap(), whole());
            assert_eq!(fs::read(kept(&root, "OPRA-D")).unwrap(), partial());
            assert!(inc.join(five).exists() && !inc.join(WHOLE).exists());
            let _ = fs::remove_dir_all(root.parent().unwrap());
        }

        #[test]
        fn an_interrupted_supersede_settles_and_files_again() {
            for (step, swapped) in [("copied", false), ("pending", false), ("swapped", true)] {
                let root = scratch(&format!("supersede-crash-{step}")).join("Databento");
                holding(&root);
                let parts = stage_whole(&root, "OPRA-C", &whole(), "parent");
                CRASH.with(|c| c.set(Some(step)));
                let err = incorporate(&parts, &[], "OPRA-C", "2026-10-27").unwrap_err();
                CRASH.with(|c| c.set(None));
                assert_eq!(err, format!("crashed {step}"));
                // As the next scan settles it: to before, or after.
                crate::manifest::settle(&root.join(AT), DAY).unwrap();
                let e = entry(&root);
                assert_eq!(crate::manifest::is_partial(&e), !swapped, "{step}: {e}");
                assert_eq!(
                    fs::read(day(&root)).unwrap(),
                    if swapped { whole() } else { partial() },
                    "{step}"
                );
                assert_eq!(e["sha256"], hash_file(&day(&root)).unwrap().as_str());
                incorporate(&parts, &[], "OPRA-C", "2026-10-27").unwrap();
                superseded(&root, &root, "OPRA-C");
                let _ = fs::remove_dir_all(root.parent().unwrap());
            }
        }

        #[test]
        fn supersedes_a_partial_day_on_another_volume() {
            let d = scratch("supersede-volumes");
            let (a, b) = (d.join("a/Databento"), d.join("b/Databento"));
            holding(&b);
            let parts = stage_whole(&a, "OPRA-V", &whole(), "parent");
            let elsewhere = std::slice::from_ref(&b);
            CRASH.with(|c| c.set(Some("copied")));
            assert!(incorporate(&parts, elsewhere, "OPRA-V", "2026-10-27").is_err());
            CRASH.with(|c| c.set(None));
            assert_eq!(
                fs::read(day(&b)).unwrap(),
                partial(),
                "a crash after the copy changes nothing"
            );
            // Filed again as the store finds it: b holds part of it now, the
            // copy (listed first, so filed from there).
            let mut both = one(&b, "OPRA-V");
            both.extend(parts);
            incorporate(&both, &[], "OPRA-V", "2026-10-27").unwrap();
            superseded(&a, &b, "OPRA-V");
            assert!(!day(&a).exists() && !b.join(INCOMING).exists());
            assert!(
                steps(&b, "OPRA-V").iter().any(|e| e["step"] == "swapped"),
                "b's ledger records it"
            );
            assert!(b.join("provenance/OPRA-V/metadata.json").exists());
            let _ = fs::remove_dir_all(&d);
        }
    }

    #[test]
    fn a_move_cannot_replace_a_file() {
        let d = scratch("link");
        fs::write(d.join("a"), b"incoming").unwrap();
        fs::write(d.join("b"), b"already here").unwrap();
        assert_eq!(
            place_new(&d.join("a"), &d.join("b")).unwrap_err().kind(),
            std::io::ErrorKind::AlreadyExists
        );
        assert_eq!(fs::read(d.join("b")).unwrap(), b"already here");
        assert!(d.join("a").exists());
        let _ = fs::remove_dir_all(&d);
    }
}
