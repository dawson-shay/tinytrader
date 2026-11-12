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
    /// The line at byte `offset` was cut short (a crash mid-append) and
    /// ended by the next writer: the event it held never completed, so what
    /// it recorded wasn't reported done.
    TornNoted {
        offset: u64,
        text: String,
        writer: String,
        at: u64,
    },
}

/// A log open for appending.
pub struct Log {
    file: fs::File,
    path: PathBuf,
    /// The chain value of the last event (see [`chain_of`]).
    chain: String,
}

/// Each line's `chain`: the SHA-256 of the line before's chain, a newline,
/// and this event's canonical JSON. An edited line, or one inserted or
/// removed, breaks every chain after it (review: the log is the truth, so
/// it's checked like the objects).
#[must_use]
pub fn chain_of(previous: &str, event: &Event) -> String {
    use sha2::Digest as _;
    let canonical = serde_json::to_string(event).unwrap_or_default();
    let mut h = sha2::Sha256::new();
    h.update(previous.as_bytes());
    h.update(b"\n");
    h.update(canonical.as_bytes());
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
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
        // The log is read whole first: its chain checked to its end, so an
        // append never extends a log that's been edited.
        let before = replay(path, 0, "")?;
        let mut log = Self {
            file,
            path: path.to_path_buf(),
            chain: before.chain.clone(),
        };
        log.heal()?;
        // A crash between ending a torn line and noting it leaves it
        // un-noted, and last in the log: noted now. One with events after
        // it wasn't left by that crash (damage), and stays un-noted.
        let noted: std::collections::HashSet<u64> = before
            .events
            .iter()
            .filter_map(|e| match e {
                Event::TornNoted { offset, .. } => Some(*offset),
                _ => None,
            })
            .collect();
        let last_event_end = before.ends.last().copied().unwrap_or(0);
        for (offset, text) in before
            .torn_lines
            .iter()
            .filter(|(o, _)| !noted.contains(o) && *o >= last_event_end)
        {
            log.note_torn(*offset, text.clone())?;
        }
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
            // Said, so a check can tell this crash's remnant from damage.
            let offset = text.iter().rposition(|b| *b == b'\n').map_or(0, |i| i + 1);
            self.note_torn(
                offset as u64,
                String::from_utf8_lossy(&text[offset..]).into_owned(),
            )?;
        }
        Ok(())
    }

    fn note_torn(&mut self, offset: u64, text: String) -> Result<(), String> {
        self.append(&Event::TornNoted {
            offset,
            text,
            writer: concat!("tinytrader-cas ", env!("CARGO_PKG_VERSION")).into(),
            at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_secs()),
        })
    }

    /// Append an event, flushed before this returns.
    ///
    /// # Errors
    /// If it can't be written.
    pub fn append(&mut self, e: &Event) -> Result<(), String> {
        let chain = chain_of(&self.chain, e);
        let mut v = serde_json::to_value(e).map_err(|e| e.to_string())?;
        v["chain"] = serde_json::Value::String(chain.clone());
        let line = serde_json::to_string(&v).map_err(|e| e.to_string())?;
        crate::append_line(&mut self.file, &line)
            .map_err(|e| format!("{}: {e}", self.path.display()))?;
        self.chain = chain;
        Ok(())
    }
}

/// What a replay of the log read.
#[derive(Debug, Default)]
pub struct Replay {
    pub events: Vec<Event>,
    /// Where each event's line ends (the byte after its newline).
    pub ends: Vec<u64>,
    /// Lines cut short before the last, with their offsets.
    pub torn_lines: Vec<(u64, String)>,
    /// The byte length read up to (the end of the last whole line).
    pub end: u64,
    /// The chain value at `end`.
    pub chain: String,
    /// A torn last line, if there was one.
    pub torn: Option<String>,
}

/// Replay the log from byte `from` (a line's start). A missing log is
/// empty. A line that isn't an event is an error, unless it's the last and
/// unterminated (torn), or a torn line a later append ended.
///
/// # Errors
/// If it can't be read, or a line other than a torn one isn't an event.
pub fn replay(path: &Path, from: u64, chain: &str) -> Result<Replay, String> {
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
        chain: chain.to_owned(),
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
        let parsed = serde_json::from_slice::<serde_json::Value>(line)
            .ok()
            .and_then(|mut v| {
                let chain = v.as_object_mut()?.remove("chain")?;
                Some((serde_json::from_value::<Event>(v), chain))
            });
        match parsed {
            Some((Ok(e), serde_json::Value::String(chain))) => {
                let want = chain_of(&r.chain, &e);
                if chain != want {
                    return Err(format!(
                        "{}: byte {at}: this line fails its checksum (edited, damaged, or a line before it was): {}",
                        path.display(),
                        String::from_utf8_lossy(line)
                    ));
                }
                r.chain = chain;
                r.events.push(e);
                r.ends.push(next as u64);
            }
            // A line cut short (not JSON at all, begun and never closed):
            // kept with its offset for the check to judge, never dropped
            // unseen.
            _ if serde_json::from_slice::<serde_json::Value>(line).is_err() && is_cut(line) => {
                r.torn_lines
                    .push((at as u64, String::from_utf8_lossy(line).into_owned()));
            }
            other => {
                let e = match other {
                    Some((Err(e), _)) => e.to_string(),
                    Some(_) => "its chain isn't a string".into(),
                    None => "not a JSON object with a chain".into(),
                };
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
