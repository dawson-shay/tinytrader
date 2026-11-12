//! The content-addressed volume format (decisions 0035 and 0039,
//! `tinytrader.storage/1`), with no dependency on the store, so a checker
//! (`tt-fsck`) or a later rewrite reads it from the disk alone.
//!
//! A volume in this format holds, under its root:
//!
//! - `cas/FORMAT`: the format and the volume's id (`{ "format":
//!   "tinytrader.storage/1", "volume": "<id>" }`).
//! - `cas/objects/<aa>/<bb>/<sha256>.<ext>`: each object, named by the
//!   SHA-256 of its bytes. Written to a temporary name, flushed, renamed into
//!   place, its folder flushed, then made read-only; never modified (S1).
//! - `cas/index/bindings.jsonl`: the append-only bindings log, one JSON event
//!   a line, each flushed (S2). A torn last line, from a crash mid-write, is
//!   the only damage tolerated: it's ignored and reported.
//! - `cas/index/snapshot.json`: the index as of some length of the log, a
//!   cache; the log is the truth.
//!
//! The index maps a *name* (today's logical path, such as
//! `Databento/OPRA.PILLAR/SPY.OPT/cbbo-1m/2026-10-02.dbn.zst`) to its current
//! version: hash, size, kind and source, with the hash of the version before.

mod log;
mod objects;
pub mod tree;

pub use log::{Event, Log, Replay};
pub use objects::{ObjectRef, object_path};

/// The SHA-256 of a file, as lowercase hex.
///
/// # Errors
/// If it can't be read.
pub fn hash_file(path: &Path) -> Result<String, String> {
    objects::hash_file(path)
}

use std::collections::BTreeMap;
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// The format this crate writes and reads.
pub const FORMAT: &str = "tinytrader.storage/1";

/// The folder of a volume that holds the format.
pub const DIR: &str = "cas";

/// One name's current version in a volume's index.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    pub version: u64,
    pub sha256: String,
    pub size: u64,
    /// The object's extension, to find it.
    #[serde(default)]
    pub ext: String,
    /// What it is: `day`, `range`, `receipt`, …
    pub kind: String,
    /// Where it came from: `batch OPRA-…`, `import …`, `migrated from …`.
    pub source: String,
    /// The hash of the version before, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous: Option<String>,
}

/// A volume's index, as its log says.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Index {
    pub names: BTreeMap<String, Entry>,
    /// Objects named in the log but bound to no name (a batch uploaded and
    /// not yet filed): sha256 → what they're pending for.
    pub pending: BTreeMap<String, String>,
    /// Log events that contradict the index (a version out of order, a
    /// `previous` that isn't the current hash): listed, never applied.
    pub conflicts: Vec<String>,
    /// How many bytes of the log this covers.
    pub log_len: u64,
    /// The log's chain value at `log_len`.
    #[serde(default)]
    pub chain: String,
}

impl Index {
    /// Apply one event of the log. A contradiction is recorded in
    /// `conflicts` and changes nothing.
    pub fn apply(&mut self, e: &Event) {
        match e {
            Event::Bind {
                name,
                version,
                sha256,
                size,
                ext,
                kind,
                source,
                previous,
                ..
            } => {
                let cur = self.names.get(name);
                let want = cur.map_or(1, |c| c.version + 1);
                let prev_ok = previous.as_deref() == cur.map(|c| c.sha256.as_str());
                if *version != want || !prev_ok {
                    // The same binding written twice (a retried append) is
                    // no contradiction.
                    if cur.is_some_and(|c| c.version == *version && c.sha256 == *sha256) {
                        return;
                    }
                    self.conflicts.push(format!(
                        "{name}: version {version} (previous {}) after {}",
                        previous.as_deref().unwrap_or("none"),
                        cur.map_or_else(
                            || "nothing".to_owned(),
                            |c| format!("version {} ({})", c.version, c.sha256)
                        )
                    ));
                    return;
                }
                self.pending.remove(sha256);
                self.names.insert(
                    name.clone(),
                    Entry {
                        version: *version,
                        sha256: sha256.clone(),
                        size: *size,
                        ext: ext.clone(),
                        kind: kind.clone(),
                        source: source.clone(),
                        previous: previous.clone(),
                    },
                );
            }
            Event::Pending { sha256, for_, .. } => {
                if !self.names.values().any(|e| e.sha256 == *sha256) {
                    self.pending.insert(sha256.clone(), for_.clone());
                }
            }
            Event::Retire { name, sha256, .. } => match self.names.get(name) {
                Some(c) if c.sha256 == *sha256 => {
                    self.names.remove(name);
                }
                _ => self.conflicts.push(format!(
                    "{name}: retiring {sha256}, which isn't its current version"
                )),
            },
            Event::Verified { .. } | Event::TornNoted { .. } => {}
        }
    }
}

/// A volume in the format.
#[derive(Clone, Debug)]
pub struct Volume {
    /// The volume's own folder (the one holding `cas/`).
    pub root: PathBuf,
    pub id: String,
}

impl Volume {
    fn dir(&self) -> PathBuf {
        self.root.join(DIR)
    }

    /// The bindings log's path.
    #[must_use]
    pub fn log_path(&self) -> PathBuf {
        self.dir().join("index").join("bindings.jsonl")
    }

    fn snapshot_path(&self) -> PathBuf {
        self.dir().join("index").join("snapshot.json")
    }

    /// Make `root` a volume in the format, with this id. A volume already
    /// there with the same id is opened; with another id, or another format,
    /// it's an error.
    ///
    /// # Errors
    /// If it can't be written, or holds another volume.
    pub fn init(root: &Path, id: &str) -> Result<Self, String> {
        if id.is_empty() || id.contains(['/', '\\']) {
            return Err(format!("{id:?} isn't a volume id"));
        }
        let format = root.join(DIR).join("FORMAT");
        if format.exists() {
            let v = Self::open(root)?;
            return if v.id == id {
                Ok(v)
            } else {
                Err(format!("{} already holds volume {}", root.display(), v.id))
            };
        }
        for d in ["objects", "index", "tmp"] {
            fs::create_dir_all(root.join(DIR).join(d))
                .map_err(|e| format!("{}: {e}", root.display()))?;
        }
        let body = serde_json::to_vec_pretty(&json!({ "format": FORMAT, "volume": id }))
            .map_err(|e| e.to_string())?;
        objects::write_whole(&format, &body).map_err(|e| format!("{}: {e}", format.display()))?;
        objects::sync_dir(root).map_err(|e| format!("{}: {e}", root.display()))?;
        Ok(Self {
            root: root.to_path_buf(),
            id: id.to_owned(),
        })
    }

    /// Open a volume in the format.
    ///
    /// # Errors
    /// If `root` isn't one, or is in another format.
    pub fn open(root: &Path) -> Result<Self, String> {
        let format = root.join(DIR).join("FORMAT");
        let text = fs::read_to_string(&format).map_err(|e| format!("{}: {e}", format.display()))?;
        let v: Value =
            serde_json::from_str(&text).map_err(|e| format!("{}: {e}", format.display()))?;
        if v["format"] != FORMAT {
            return Err(format!(
                "{}: format {}, not {FORMAT}",
                format.display(),
                v["format"]
            ));
        }
        let id = v["volume"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| format!("{}: no volume id", format.display()))?;
        Ok(Self {
            root: root.to_path_buf(),
            id: id.to_owned(),
        })
    }

    /// Write the file at `src` as an object, hashing it as it's copied. With
    /// `expect`, an object whose hash differs is refused and nothing is
    /// kept. An object already held is kept as it is (its size is checked).
    ///
    /// # Errors
    /// On I/O failure, or a hash other than `expect`.
    pub fn put_file(
        &self,
        src: &Path,
        ext: &str,
        expect: Option<&str>,
    ) -> Result<ObjectRef, String> {
        let f = fs::File::open(src).map_err(|e| format!("{}: {e}", src.display()))?;
        self.put(f, ext, expect)
            .map_err(|e| format!("{}: {e}", src.display()))
    }

    /// As [`Volume::put_file`], from any reader.
    ///
    /// # Errors
    /// As [`Volume::put_file`].
    pub fn put(
        &self,
        r: impl std::io::Read,
        ext: &str,
        expect: Option<&str>,
    ) -> Result<ObjectRef, String> {
        objects::put(&self.dir(), r, ext, expect)
    }

    /// The path of the object with this hash and extension, if held.
    #[must_use]
    pub fn object(&self, sha256: &str, ext: &str) -> Option<PathBuf> {
        if !objects::is_hash(sha256) || !objects::is_ext(ext) {
            return None;
        }
        let p = object_path(&self.dir(), sha256, ext);
        p.is_file().then_some(p)
    }

    /// The bindings log, for appending.
    ///
    /// # Errors
    /// If it can't be opened.
    pub fn log(&self) -> Result<Log, String> {
        Log::open(&self.log_path())
    }

    /// The index: the snapshot, if it's sound, with the log replayed from
    /// where it ends; else the whole log replayed.
    ///
    /// # Errors
    /// If the log can't be read, or holds a damaged line other than its
    /// last.
    pub fn index(&self) -> Result<Index, String> {
        let snap = self.read_snapshot().ok().flatten();
        let log_len = fs::metadata(self.log_path()).map_or(0, |m| m.len());
        let (mut index, from) = match snap {
            Some(s) if s.log_len <= log_len => {
                let from = s.log_len;
                (s, from)
            }
            _ => (Index::default(), 0),
        };
        let start = index.chain.clone();
        let replay = log::replay(&self.log_path(), from, &start)?;
        for e in &replay.events {
            index.apply(e);
        }
        index.log_len = replay.end;
        index.chain = replay.chain;
        Ok(index)
    }

    /// The snapshot, if there is one and its checksum holds: `None` when
    /// there's none; an error when it's unreadable or its checksum fails (it
    /// is then ignored by [`Volume::index`], and reported by
    /// [`Volume::verify`]).
    fn read_snapshot(&self) -> Result<Option<Index>, String> {
        let text = match fs::read(self.snapshot_path()) {
            Ok(t) => t,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(format!("snapshot: {e}")),
        };
        let v: Value = serde_json::from_slice(&text).map_err(|e| format!("snapshot: {e}"))?;
        let index: Index =
            serde_json::from_value(v["index"].clone()).map_err(|e| format!("snapshot: {e}"))?;
        let body = serde_json::to_vec(&index).map_err(|e| e.to_string())?;
        if v["sha256"].as_str() != Some(objects::hex_of(&body).as_str()) {
            return Err("snapshot: its checksum doesn't match its content".into());
        }
        Ok(Some(index))
    }

    /// Rewrite the snapshot from the whole log (it's a cache; this never
    /// changes what the index says), with its checksum.
    ///
    /// # Errors
    /// If the log can't be read or the snapshot written.
    pub fn snapshot(&self) -> Result<Index, String> {
        let replay = log::replay(&self.log_path(), 0, "")?;
        let mut index = Index::default();
        for e in &replay.events {
            index.apply(e);
        }
        index.log_len = replay.end;
        index.chain = replay.chain;
        let body = serde_json::to_vec(&index).map_err(|e| e.to_string())?;
        let doc = json!({ "index": &index, "sha256": objects::hex_of(&body) });
        let bytes = serde_json::to_vec(&doc).map_err(|e| e.to_string())?;
        objects::write_whole(&self.snapshot_path(), &bytes)
            .map_err(|e| format!("{}: {e}", self.snapshot_path().display()))?;
        Ok(index)
    }

    /// Check the volume against its log (0039 S1–S2): every name's object is
    /// there at its size (and, with `hash`, has its hash); every object is
    /// named or pending (else an orphan, kept and reported). Writes nothing.
    ///
    /// # Errors
    /// If the log can't be read.
    pub fn verify(&self, hash: bool) -> Result<Report, String> {
        let replay = log::replay(&self.log_path(), 0, "")?;
        let mut index = Index::default();
        for e in &replay.events {
            index.apply(e);
        }
        let mut r = Report {
            torn_last_line: replay.torn.clone(),
            conflicts: index.conflicts.clone(),
            hashed: hash,
            ..Report::default()
        };
        // A line cut short before the last is a crash's remnant only if the
        // next writer said so (`torn_noted`); otherwise it's damage.
        let noted: std::collections::HashSet<u64> = replay
            .events
            .iter()
            .filter_map(|e| match e {
                Event::TornNoted { offset, .. } => Some(*offset),
                _ => None,
            })
            .collect();
        for (offset, text) in &replay.torn_lines {
            let line = format!("byte {offset}: {text}");
            if noted.contains(offset) {
                r.torn_lines.push(line);
            } else {
                r.log_damage.push(line);
            }
        }
        // The snapshot must be what the log says, as of its length.
        match self.read_snapshot() {
            Ok(None) => {}
            Err(e) => r.snapshot.push(e),
            Ok(Some(snap)) => {
                let mut prefix = Index::default();
                for (e, end) in replay.events.iter().zip(&replay.ends) {
                    if *end <= snap.log_len {
                        prefix.apply(e);
                    }
                }
                let mut chain = String::new();
                for (e, end) in replay.events.iter().zip(&replay.ends) {
                    if *end <= snap.log_len {
                        chain = log::chain_of(&chain, e);
                    }
                }
                if snap.names != prefix.names
                    || snap.pending != prefix.pending
                    || snap.conflicts != prefix.conflicts
                    || snap.chain != chain
                {
                    r.snapshot.push(format!(
                        "snapshot: doesn't match the log's first {} bytes ({} names there, {} in the snapshot)",
                        snap.log_len,
                        prefix.names.len(),
                        snap.names.len()
                    ));
                }
            }
        }
        let held =
            objects::list(&self.dir()).map_err(|e| format!("{}: {e}", self.root.display()))?;
        let (held, wrong): (Vec<objects::Held>, Vec<objects::Held>) =
            held.into_iter().partition(|h| h.named_right);
        r.misnamed = wrong.iter().map(|h| h.path.display().to_string()).collect();
        r.objects = held.len();
        // Every object, bound or not, is hashed (with `hash`): an earlier
        // version or a pending one is data too.
        let mut by_key: BTreeMap<(&str, &str), &objects::Held> = BTreeMap::new();
        for h in &held {
            by_key.insert((h.sha256.as_str(), h.ext.as_str()), h);
            if hash {
                match objects::hash_file(&h.path) {
                    Ok(got) if got == h.sha256 => {}
                    Ok(got) => r
                        .damaged
                        .push(format!("object {} hashes to {got}", h.path.display())),
                    Err(err) => r.damaged.push(err),
                }
            }
        }
        for (name, e) in &index.names {
            match by_key.get(&(e.sha256.as_str(), e.ext.as_str())) {
                None => r
                    .missing
                    .push(format!("{name}: object {}.{} isn't there", e.sha256, e.ext)),
                Some(h) if h.size != e.size => r.damaged.push(format!(
                    "{name}: object {} is {} bytes, bound as {}",
                    e.sha256, h.size, e.size
                )),
                Some(_) => {}
            }
        }
        // A pending object is the only copy of what it holds (a conflict's
        // other version): it must be there too.
        for e in &replay.events {
            if let Event::Pending {
                sha256, ext, for_, ..
            } = e
                && index.pending.contains_key(sha256)
                && !by_key.contains_key(&(sha256.as_str(), ext.as_str()))
                && !index.names.values().any(|n| n.sha256 == *sha256)
            {
                r.missing.push(format!(
                    "pending object {sha256}.{ext} ({for_}) isn't there"
                ));
            }
        }
        r.missing.sort();
        r.missing.dedup();
        let current: std::collections::HashSet<(&str, &str)> = index
            .names
            .values()
            .map(|e| (e.sha256.as_str(), e.ext.as_str()))
            .collect();
        let pending: std::collections::HashSet<&str> =
            index.pending.keys().map(String::as_str).collect();
        // An object no name points at now may be an earlier version: it's
        // an orphan only if no event ever named it.
        let ever: std::collections::HashSet<(&str, &str)> = replay
            .events
            .iter()
            .filter_map(|e| match e {
                Event::Bind { sha256, ext, .. } | Event::Pending { sha256, ext, .. } => {
                    Some((sha256.as_str(), ext.as_str()))
                }
                _ => None,
            })
            .collect();
        for h in &held {
            let key = (h.sha256.as_str(), h.ext.as_str());
            if current.contains(&key) || pending.contains(h.sha256.as_str()) {
                continue;
            }
            if ever.contains(&key) {
                r.unreferenced += 1;
            } else {
                r.orphans.push(h.path.display().to_string());
            }
        }
        r.names = index.names.len();
        Ok(r)
    }
}

/// A hold on a volume (`cas/LOCK`): one writer at a time, and no checker
/// while it writes. Released when dropped.
#[derive(Debug)]
pub struct Lock {
    _file: Option<fs::File>,
}

impl Volume {
    /// Hold the volume for writing: refused while another writer or a
    /// checker holds it.
    ///
    /// # Errors
    /// If it's held, or the lock can't be made.
    pub fn lock_writer(&self) -> Result<Lock, String> {
        let path = self.dir().join("LOCK");
        let f = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|e| format!("{}: {e}", path.display()))?;
        match f.try_lock() {
            Ok(()) => Ok(Lock { _file: Some(f) }),
            Err(fs::TryLockError::WouldBlock) => Err(format!(
                "volume {} is held by another program (a writer or a check); try when it's done",
                self.id
            )),
            Err(fs::TryLockError::Error(e)) => Err(format!("{}: {e}", path.display())),
        }
    }

    /// Hold the volume for checking: refused while a writer holds it. A
    /// volume mounted read-only without a lock file can't be written, so
    /// it's held as it is.
    ///
    /// # Errors
    /// If a writer holds it.
    pub fn lock_reader(&self) -> Result<Lock, String> {
        let path = self.dir().join("LOCK");
        let Ok(f) = fs::File::open(&path) else {
            return Ok(Lock { _file: None });
        };
        match f.try_lock_shared() {
            Ok(()) => Ok(Lock { _file: Some(f) }),
            Err(fs::TryLockError::WouldBlock) => Err(format!(
                "volume {} is being written; check it when that's done",
                self.id
            )),
            Err(fs::TryLockError::Error(e)) => Err(format!("{}: {e}", path.display())),
        }
    }
}

impl Volume {
    fn receipt_path(&self, source: &str) -> PathBuf {
        self.dir().join("sources").join(format!("{source}.json"))
    }

    /// The receipt of what was read from the source `source` (by
    /// `store migrate-cas`): `{ source, root, runs: [...], files: { rel:
    /// { size, mtime_ns, sha256, outcome } } }`, or none.
    ///
    /// # Errors
    /// If it's there but unreadable.
    pub fn receipt(&self, source: &str) -> Result<Option<Value>, String> {
        let p = self.receipt_path(source);
        match fs::read(&p) {
            Ok(t) => serde_json::from_slice(&t)
                .map(Some)
                .map_err(|e| format!("{}: {e}", p.display())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(format!("{}: {e}", p.display())),
        }
    }

    /// Every source receipt kept, by source name.
    ///
    /// # Errors
    /// If one is unreadable.
    pub fn receipts(&self) -> Result<BTreeMap<String, Value>, String> {
        let mut out = BTreeMap::new();
        let dir = self.dir().join("sources");
        let Ok(rd) = fs::read_dir(&dir) else {
            return Ok(out);
        };
        for e in rd.flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            if let Some(src) = name.strip_suffix(".json")
                && let Some(r) = self.receipt(src)?
            {
                out.insert(src.to_owned(), r);
            }
        }
        Ok(out)
    }

    /// Write a source's receipt whole.
    ///
    /// # Errors
    /// If it can't be written.
    pub fn write_receipt(&self, source: &str, receipt: &Value) -> Result<(), String> {
        if source.is_empty() || source.contains(['/', '\\']) || source.starts_with('.') {
            return Err(format!("{source:?} isn't a source name"));
        }
        let p = self.receipt_path(source);
        let body = serde_json::to_vec(receipt).map_err(|e| e.to_string())?;
        objects::write_whole(&p, &body).map_err(|e| format!("{}: {e}", p.display()))
    }
}

/// What [`Volume::verify`] found.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Report {
    pub names: usize,
    pub objects: usize,
    /// Names whose object isn't there.
    pub missing: Vec<String>,
    /// Names whose object is there at another size or hash.
    pub damaged: Vec<String>,
    /// Objects no binding ever named (kept, reported).
    pub orphans: Vec<String>,
    /// Objects named only by an earlier version (kept until an operator's
    /// garbage collection).
    pub unreferenced: usize,
    /// Files in `objects/` whose name isn't a hash in its place.
    pub misnamed: Vec<String>,
    pub conflicts: Vec<String>,
    /// The log's last line, torn by a crash mid-write, if it was.
    pub torn_last_line: Option<String>,
    /// Lines before the last cut short by a crash and noted by the next
    /// writer (tolerated, listed with their offsets).
    pub torn_lines: Vec<String>,
    /// Lines before the last cut short and never noted: damage.
    pub log_damage: Vec<String>,
    /// The snapshot unreadable, its checksum failing, or unlike the log.
    pub snapshot: Vec<String>,
    /// Whether objects were hashed (else sizes only).
    pub hashed: bool,
}

impl Report {
    /// Whether nothing is wrong (a torn last line is tolerated; reported).
    #[must_use]
    pub fn sound(&self) -> bool {
        self.missing.is_empty()
            && self.damaged.is_empty()
            && self.orphans.is_empty()
            && self.misnamed.is_empty()
            && self.conflicts.is_empty()
            && self.log_damage.is_empty()
            && self.snapshot.is_empty()
    }
}

/// One name across volumes: the version that wins, and which volumes hold
/// it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Merged {
    pub entry: Entry,
    pub copies: Vec<String>,
}

/// Merge volumes' indexes by name (0035 section 2): the highest version
/// wins; two different hashes at one version are a conflict, reported, and
/// the name is left out until it's resolved.
#[must_use]
pub fn merge(indexes: &[(String, Index)]) -> (BTreeMap<String, Merged>, Vec<String>) {
    let mut out: BTreeMap<String, Merged> = BTreeMap::new();
    let mut conflicts = Vec::new();
    let mut conflicted = std::collections::BTreeSet::new();
    for (vol, index) in indexes {
        for (name, e) in &index.names {
            match out.get_mut(name) {
                None => {
                    out.insert(
                        name.clone(),
                        Merged {
                            entry: e.clone(),
                            copies: vec![vol.clone()],
                        },
                    );
                }
                Some(m) if m.entry.version == e.version => {
                    if m.entry.sha256 == e.sha256 {
                        m.copies.push(vol.clone());
                    } else {
                        conflicts.push(format!(
                            "{name}: version {} is {} on {} but {} on {vol}",
                            e.version,
                            m.entry.sha256,
                            m.copies.join(","),
                            e.sha256
                        ));
                        conflicted.insert(name.clone());
                    }
                }
                Some(m) if m.entry.version < e.version => {
                    *m = Merged {
                        entry: e.clone(),
                        copies: vec![vol.clone()],
                    };
                }
                Some(_) => {}
            }
        }
    }
    for n in conflicted {
        out.remove(&n);
    }
    (out, conflicts)
}

/// Append one line, flushed, to a log file (used by [`Log`]).
fn append_line(f: &mut fs::File, line: &str) -> std::io::Result<()> {
    f.write_all(line.as_bytes())?;
    f.write_all(b"\n")?;
    f.sync_data()
}

#[cfg(test)]
mod tests;
