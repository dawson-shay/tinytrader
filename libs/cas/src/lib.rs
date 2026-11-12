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

pub use log::{Event, Log, Replay};
pub use objects::{ObjectRef, object_path};

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
            Event::Verified { .. } => {}
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
        let snap: Option<Index> = fs::read_to_string(self.snapshot_path())
            .ok()
            .and_then(|t| serde_json::from_str(&t).ok());
        let log_len = fs::metadata(self.log_path()).map_or(0, |m| m.len());
        let (mut index, from) = match snap {
            Some(s) if s.log_len <= log_len => {
                let from = s.log_len;
                (s, from)
            }
            _ => (Index::default(), 0),
        };
        let replay = log::replay(&self.log_path(), from)?;
        for e in &replay.events {
            index.apply(e);
        }
        index.log_len = replay.end;
        Ok(index)
    }

    /// Rewrite the snapshot from the log (it's a cache; this never changes
    /// what the index says).
    ///
    /// # Errors
    /// If the log can't be read or the snapshot written.
    pub fn snapshot(&self) -> Result<Index, String> {
        let index = self.index()?;
        let body = serde_json::to_vec(&index).map_err(|e| e.to_string())?;
        objects::write_whole(&self.snapshot_path(), &body)
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
        let replay = log::replay(&self.log_path(), 0)?;
        let mut index = Index::default();
        for e in &replay.events {
            index.apply(e);
        }
        let mut r = Report {
            torn_last_line: replay.torn,
            conflicts: index.conflicts.clone(),
            ..Report::default()
        };
        let held =
            objects::list(&self.dir()).map_err(|e| format!("{}: {e}", self.root.display()))?;
        let mut by_hash: BTreeMap<&str, &objects::Held> = BTreeMap::new();
        let (held, wrong): (Vec<objects::Held>, Vec<objects::Held>) =
            held.into_iter().partition(|h| h.named_right);
        r.misnamed = wrong.iter().map(|h| h.path.display().to_string()).collect();
        r.objects = held.len();
        for h in &held {
            by_hash.insert(&h.sha256, h);
        }
        for (name, e) in &index.names {
            match by_hash.get(e.sha256.as_str()) {
                None => r
                    .missing
                    .push(format!("{name}: object {} isn't there", e.sha256)),
                Some(h) if h.size != e.size => r.damaged.push(format!(
                    "{name}: object {} is {} bytes, bound as {}",
                    e.sha256, h.size, e.size
                )),
                Some(h) if hash => match objects::hash_file(&h.path) {
                    Ok(got) if got == e.sha256 => {}
                    Ok(got) => r
                        .damaged
                        .push(format!("{name}: object {} hashes to {got}", e.sha256)),
                    Err(err) => r.damaged.push(format!("{name}: {err}")),
                },
                Some(_) => {}
            }
        }
        let named: std::collections::HashSet<&str> = index
            .names
            .values()
            .map(|e| e.sha256.as_str())
            .chain(index.pending.keys().map(String::as_str))
            .collect();
        // An object no name points at now may be an earlier version: it's
        // an orphan only if no binding ever named it.
        let ever: std::collections::HashSet<&str> = replay
            .events
            .iter()
            .filter_map(|e| match e {
                Event::Bind { sha256, .. } | Event::Pending { sha256, .. } => Some(sha256.as_str()),
                _ => None,
            })
            .collect();
        for h in &held {
            if !named.contains(h.sha256.as_str()) {
                if ever.contains(h.sha256.as_str()) {
                    r.unreferenced += 1;
                } else {
                    r.orphans.push(h.path.display().to_string());
                }
            }
        }
        r.names = index.names.len();
        Ok(r)
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
