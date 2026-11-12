//! The bindings log (0039 S2): append-only, one JSON event a line, each
//! flushed. The index is replayed from it.

use std::fs;
use std::io::Read as _;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// One event of the log. Every event says who wrote it (`writer`, with its
/// version) and when (`at`, Unix seconds).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum Event {
    /// `name`'s version `version` is the object `sha256`.
    Bind {
        name: String,
        version: u64,
        sha256: String,
        size: u64,
        /// The object's extension, to find it.
        ext: String,
        kind: String,
        source: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        previous: Option<String>,
        writer: String,
        at: u64,
    },
    /// An object held for something not yet named (a batch being filed).
    Pending {
        sha256: String,
        ext: String,
        #[serde(rename = "for")]
        for_: String,
        writer: String,
        at: u64,
    },
    /// `name` (at `sha256`) is no longer held under that name.
    Retire {
        name: String,
        sha256: String,
        why: String,
        writer: String,
        at: u64,
    },
    /// The object `sha256` was hashed again and found whole.
    Verified {
        sha256: String,
        writer: String,
        at: u64,
    },
}

/// A log open for appending.
pub struct Log {
    file: fs::File,
    path: PathBuf,
}

impl Log {
    /// Open (or create) the log at `path` for appending.
    ///
    /// # Errors
    /// If it can't be opened.
    pub fn open(path: &Path) -> Result<Self, String> {
        if let Some(d) = path.parent() {
            fs::create_dir_all(d).map_err(|e| format!("{}: {e}", d.display()))?;
        }
        let created = !path.exists();
        let file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| format!("{}: {e}", path.display()))?;
        if created && let Some(d) = path.parent() {
            crate::objects::sync_dir(d).map_err(|e| format!("{}: {e}", d.display()))?;
        }
        let mut log = Self {
            file,
            path: path.to_path_buf(),
        };
        log.heal()?;
        Ok(log)
    }

    /// A torn last line (a crash mid-append) is ended, so the next event
    /// starts a line of its own; the torn line stays, ignored on replay.
    fn heal(&mut self) -> Result<(), String> {
        let mut text = Vec::new();
        fs::File::open(&self.path)
            .and_then(|mut f| f.read_to_end(&mut text))
            .map_err(|e| format!("{}: {e}", self.path.display()))?;
        if !text.is_empty() && text.last() != Some(&b'\n') {
            std::io::Write::write_all(&mut self.file, b"\n")
                .and_then(|()| self.file.sync_data())
                .map_err(|e| format!("{}: {e}", self.path.display()))?;
        }
        Ok(())
    }

    /// Append an event, flushed before this returns.
    ///
    /// # Errors
    /// If it can't be written.
    pub fn append(&mut self, e: &Event) -> Result<(), String> {
        let line = serde_json::to_string(e).map_err(|e| e.to_string())?;
        crate::append_line(&mut self.file, &line)
            .map_err(|e| format!("{}: {e}", self.path.display()))
    }
}

/// What a replay of the log read.
#[derive(Debug, Default)]
pub struct Replay {
    pub events: Vec<Event>,
    /// The byte length read up to (the end of the last whole line).
    pub end: u64,
    /// A torn last line, if there was one.
    pub torn: Option<String>,
}

/// Replay the log from byte `from` (a line's start). A missing log is
/// empty. A line that isn't an event is an error, unless it's the last and
/// unterminated (torn), or a torn line a later append ended.
///
/// # Errors
/// If it can't be read, or a line other than a torn one isn't an event.
pub fn replay(path: &Path, from: u64) -> Result<Replay, String> {
    let mut text = Vec::new();
    match fs::File::open(path) {
        Ok(mut f) => {
            f.read_to_end(&mut text)
                .map_err(|e| format!("{}: {e}", path.display()))?;
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Replay::default()),
        Err(e) => return Err(format!("{}: {e}", path.display())),
    }
    let from = usize::try_from(from).unwrap_or(usize::MAX).min(text.len());
    let mut r = Replay {
        end: from as u64,
        ..Replay::default()
    };
    let mut at = from;
    while at < text.len() {
        let Some(nl) = text[at..].iter().position(|b| *b == b'\n') else {
            r.torn = Some(String::from_utf8_lossy(&text[at..]).into_owned());
            break;
        };
        let line = &text[at..at + nl];
        let next = at + nl + 1;
        match serde_json::from_slice::<Event>(line) {
            Ok(e) => r.events.push(e),
            // A torn line that a later append ended (Log::heal): it isn't
            // JSON at all, and was cut, not written wrong.
            Err(_)
                if serde_json::from_slice::<serde_json::Value>(line).is_err() && is_cut(line) => {}
            Err(e) => {
                return Err(format!(
                    "{}: byte {at}: not a bindings event ({e}): {}",
                    path.display(),
                    String::from_utf8_lossy(line)
                ));
            }
        }
        at = next;
        r.end = at as u64;
    }
    Ok(r)
}

/// A line cut short: a JSON object that was begun and never closed.
fn is_cut(line: &[u8]) -> bool {
    line.first() == Some(&b'{') && line.last() != Some(&b'}')
}
