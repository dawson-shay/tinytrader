//! The submissions ledger: every submission to Databento is written down,
//! and flushed to disk, before it is sent, so a purchase is never forgotten.
//!
//! One JSON line per step, appended, never rewritten:
//!
//! - `approved`: a person approved this paid request (who, and the quote),
//!   written before it is sent;
//! - `sending`: about to submit this exact request;
//! - `submitted`: Databento answered with this job ID;
//! - `uncertain`: the submission failed or its outcome is unknown;
//! - `not-submitted`: the operator found it was never made
//!   (`POST /ledger/resolve`); nothing writes this on its own.
//!
//! A request whose latest step is `sending` or `uncertain` may already have
//! been bought, so it blocks every purchase it overlaps until it is found on
//! Databento's job list (then `submitted`, with its job) or resolved by the
//! operator. Never by time alone: a job that doesn't show yet may still be
//! one (decision 0019). A `not-submitted` that an earlier version wrote on
//! its own, after a timeout, counts as unresolved again.
//!
//! One process at a time: the ledger's `.lock` file is held exclusively for
//! as long as it is open, so a second service on the same ledger can't
//! start.

use std::collections::HashSet;
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Entry {
    /// Unix seconds.
    pub at: f64,
    /// The request's fingerprint (see [`fingerprint`]).
    pub request: String,
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_usd: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    /// Who approved a paid purchase (an `approved` step).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub by: Option<String>,
    /// The quote it was approved from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quote: Option<String>,
}

/// What identifies a request across a submission and Databento's job list:
/// dataset, schema, symbols (sorted) and the days it covers.
#[must_use]
pub fn fingerprint(
    dataset: &str,
    schema: &str,
    symbols: &[String],
    start: &str,
    end: &str,
) -> String {
    let mut s = symbols.to_vec();
    s.sort();
    format!("{dataset}|{schema}|{}|{start}|{end}", s.join(","))
}

/// The note an earlier version wrote with a `not-submitted` it decided on
/// its own.
const WRITTEN_OFF: &str = "no such job appeared in Databento's job list";

/// Whether a step leaves its request unresolved.
fn open_step(e: &Entry) -> bool {
    e.state == "sending"
        || e.state == "uncertain"
        || (e.state == "not-submitted" && e.note.as_deref() == Some(WRITTEN_OFF))
}

/// Why the ledger's lock couldn't be taken.
fn lock_error(e: &fs::TryLockError, path: &Path, lock: &Path) -> String {
    match e {
        fs::TryLockError::WouldBlock => format!(
            "another process holds the submissions ledger {} (lock {}); only one databento-jobs may use it",
            path.display(),
            lock.display()
        ),
        fs::TryLockError::Error(e) => format!(
            "{}: {e}; this filesystem doesn't support file locking, so the submissions ledger must be on a local disk",
            lock.display()
        ),
    }
}

pub struct Ledger {
    path: PathBuf,
    entries: Vec<Entry>,
    /// Held exclusively while open.
    _lock: fs::File,
}

impl Ledger {
    /// Open (or start) the ledger at `path`.
    ///
    /// # Errors
    /// If another process has it open, or the file exists but can't be
    /// read, or a line isn't an entry.
    pub fn open(path: &Path) -> Result<Self, String> {
        if let Some(dir) = path.parent() {
            fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
        }
        let lock_path = PathBuf::from(format!("{}.lock", path.display()));
        let lock = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&lock_path)
            .map_err(|e| format!("{}: {e}", lock_path.display()))?;
        lock.try_lock()
            .map_err(|e| lock_error(&e, path, &lock_path))?;
        let entries = match fs::read_to_string(path) {
            Ok(text) => text
                .lines()
                .filter(|l| !l.trim().is_empty())
                .map(|l| serde_json::from_str(l).map_err(|e| format!("{}: {e}", path.display())))
                .collect::<Result<_, _>>()?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(format!("{}: {e}", path.display())),
        };
        Ok(Self {
            path: path.to_owned(),
            entries,
            _lock: lock,
        })
    }

    /// Append a step, flushed to disk before returning.
    ///
    /// # Errors
    /// If it can't be written; the caller must then not submit.
    pub fn append(&mut self, e: Entry) -> Result<(), String> {
        let new = !self.path.exists();
        let mut f = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|e| format!("{}: {e}", self.path.display()))?;
        let line = serde_json::to_string(&e).map_err(|e| e.to_string())?;
        writeln!(f, "{line}").map_err(|e| format!("{}: {e}", self.path.display()))?;
        f.sync_all()
            .map_err(|e| format!("{}: {e}", self.path.display()))?;
        // A new file's name is durable only once its folder is.
        if new && let Some(dir) = self.path.parent() {
            fs::File::open(dir)
                .and_then(|d| d.sync_all())
                .map_err(|e| format!("{}: {e}", dir.display()))?;
        }
        self.entries.push(e);
        Ok(())
    }

    fn latest(&self, request: &str) -> Option<&Entry> {
        self.entries.iter().rev().find(|e| e.request == request)
    }

    /// The latest step of `request`, if it is unresolved.
    #[must_use]
    pub fn unresolved_step(&self, request: &str) -> Option<&Entry> {
        self.latest(request).filter(|e| open_step(e))
    }

    /// When the latest attempt at `request` was sent (its last `sending`
    /// step): a job it made was received after this.
    #[must_use]
    pub fn sent_at(&self, request: &str) -> Option<f64> {
        self.entries
            .iter()
            .rev()
            .find(|e| e.request == request && e.state == "sending")
            .map(|e| e.at)
    }

    /// When the attempt `e` (an unresolved step) was sent: its `sending`
    /// step, or `e` itself when earlier.
    #[must_use]
    pub fn attempt_at(&self, e: &Entry) -> f64 {
        self.sent_at(&e.request).unwrap_or(e.at).min(e.at)
    }

    /// Requests an earlier version wrote off as not submitted on its own,
    /// now unresolved again.
    #[must_use]
    pub fn written_off(&self) -> Vec<Entry> {
        self.unresolved()
            .into_iter()
            .filter(|e| e.state == "not-submitted")
            .collect()
    }

    /// Every request whose latest step is unresolved.
    #[must_use]
    pub fn unresolved(&self) -> Vec<Entry> {
        let mut seen = HashSet::new();
        self.entries
            .iter()
            .rev()
            .filter(|e| seen.insert(e.request.clone()))
            .filter(|e| open_step(e))
            .cloned()
            .collect()
    }

    /// Every purchase made here, or that may have been: each step with a job
    /// (whatever its state), and each request whose latest step is
    /// unresolved.
    #[must_use]
    pub fn purchases(&self) -> Vec<Entry> {
        let mut out: Vec<Entry> = self
            .entries
            .iter()
            .filter(|e| e.job.is_some())
            .cloned()
            .collect();
        out.extend(self.unresolved());
        out
    }

    /// Every job ID submitted here.
    #[must_use]
    pub fn submitted(&self) -> HashSet<String> {
        self.entries.iter().filter_map(|e| e.job.clone()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(request: &str, state: &str, job: Option<&str>) -> Entry {
        Entry {
            at: 1.0,
            request: request.into(),
            state: state.into(),
            job: job.map(Into::into),
            cost_usd: None,
            note: None,
            ..Default::default()
        }
    }

    #[test]
    fn an_unresolved_submission_blocks_the_same_request_until_reconciled() {
        let d = scratch("unresolved");
        let path = d.join("submissions.jsonl");
        let r = fingerprint(
            "OPRA.PILLAR",
            "cmbp-1",
            &["SPY.OPT".into()],
            "2026-10-01",
            "2026-10-02",
        );
        {
            let mut l = Ledger::open(&path).unwrap();
            l.append(entry(&r, "sending", None)).unwrap();
            assert!(l.unresolved_step(&r).is_some());
            l.append(entry(&r, "uncertain", None)).unwrap();
        }
        // Reopened (a restart): still a purchase, and listed for reconciliation.
        let mut l = Ledger::open(&path).unwrap();
        assert_eq!(
            l.unresolved_step(&r).map(|e| e.state.as_str()),
            Some("uncertain")
        );
        assert_eq!((l.unresolved().len(), l.purchases().len()), (1, 1));
        l.append(entry(&r, "submitted", Some("OPRA-1"))).unwrap();
        assert!(l.unresolved_step(&r).is_none());
        assert!(l.unresolved().is_empty());
        assert!(l.submitted().contains("OPRA-1"));
        // Still a purchase once found, and after its job is gone.
        assert_eq!(l.purchases().len(), 1);
        assert!(
            l.unresolved_step(&fingerprint(
                "OPRA.PILLAR",
                "cmbp-1",
                &["QQQ.OPT".into()],
                "2026-10-01",
                "2026-10-02"
            ))
            .is_none()
        );
        let _ = fs::remove_dir_all(&d);
    }

    /// A new, empty folder of this test's own: made exclusively (`create_dir`
    /// fails on one that exists), under a name no other test or process shares,
    /// so tests running at once in a shared temp dir never meet.
    fn scratch(name: &str) -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        for _ in 0..1000 {
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos());
            let d = std::env::temp_dir().join(format!(
                "tinytrader-ledger-{name}-{}-{nanos}-{}",
                std::process::id(),
                N.fetch_add(1, Ordering::SeqCst)
            ));
            if fs::create_dir(&d).is_ok() {
                return d;
            }
        }
        panic!("no new folder in {}", std::env::temp_dir().display());
    }

    #[test]
    fn only_one_process_may_hold_the_ledger() {
        let d = scratch("lock");
        let path = d.join("submissions.jsonl");
        let first = Ledger::open(&path).unwrap();
        let second = Ledger::open(&path).err().unwrap();
        assert!(second.contains("another process"), "{second}");
        drop(first);
        assert!(Ledger::open(&path).is_ok());
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn a_filesystem_without_locks_is_named() {
        let e = fs::TryLockError::Error(std::io::Error::from(std::io::ErrorKind::Unsupported));
        let why = lock_error(&e, Path::new("l"), Path::new("l.lock"));
        assert!(why.contains("must be on a local disk"), "{why}");
    }

    #[test]
    fn an_old_automatic_write_off_is_unresolved_again() {
        let d = scratch("written-off");
        let mut l = Ledger::open(&d.join("submissions.jsonl")).unwrap();
        let mut off = entry("R", "not-submitted", None);
        off.note = Some(WRITTEN_OFF.into());
        l.append(entry("R", "sending", None)).unwrap();
        l.append(entry("R", "uncertain", None)).unwrap();
        l.append(off).unwrap();
        assert_eq!(l.unresolved().len(), 1);
        assert_eq!(l.purchases().len(), 1);
        assert_eq!(l.written_off().len(), 1);
        assert!(l.unresolved_step("R").is_some());
        // The operator's word resolves it.
        let mut by_hand = entry("R", "not-submitted", None);
        by_hand.note = Some("resolved by the operator: not in the portal".into());
        l.append(by_hand).unwrap();
        assert!(l.unresolved().is_empty() && l.purchases().is_empty());
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn symbols_are_compared_in_any_order() {
        assert_eq!(
            fingerprint("D", "s", &["B".into(), "A".into()], "x", "y"),
            fingerprint("D", "s", &["A".into(), "B".into()], "x", "y")
        );
    }
}
