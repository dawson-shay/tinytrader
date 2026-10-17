//! The trading journal: what happened on a day (what was done, the state of
//! mind, the mistakes), so patterns show up over time. Entries are kept in
//! one JSON file, written whole to a temporary file, flushed and renamed over
//! the old one, so a save is never half done. A file that is there but can't
//! be read is never replaced: writes are refused until it's fixed.
//!
//! An entry is for a day (`YYYY-MM-DD`, New York), with the time it was
//! written, free text, tags, an optional discipline rating (1 to 5), and the
//! account it refers to (`null`: all accounts). Several a day are fine.
//!
//! `GET /api/journal?from=&to=` lists the entries whose day is in the range
//! (both ends optional, inclusive), newest first. `POST /api/journal` adds
//! one, `PUT /api/journal/{id}` changes the fields it names, and
//! `DELETE /api/journal/{id}` removes one. Like the settings, the writes need
//! no more than the console itself. Bounded: 8 KiB of text an entry, 16 tags,
//! and 8 MiB in all.

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use serde_json::Value;

const MAX_TEXT: usize = 8 << 10;
const MAX_TAGS: usize = 16;
const MAX_TAG: usize = 32;
const MAX_ACCOUNT: usize = 64;
const MAX_TOTAL: usize = 8 << 20;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Entry {
    pub id: String,
    /// The New York day the entry is about, `YYYY-MM-DD`.
    pub day: String,
    /// When it was written, in milliseconds since the epoch.
    pub at: u64,
    /// The account it refers to; `None` for all accounts.
    pub account: Option<String>,
    pub text: String,
    pub tags: Vec<String>,
    /// Discipline, 1 to 5, if rated.
    pub rating: Option<u8>,
    /// When it was last changed, in milliseconds since the epoch.
    pub updated: u64,
}

#[derive(Serialize, Deserialize, Default)]
struct File {
    entries: Vec<Entry>,
}

/// Why a change was refused.
#[derive(Debug, PartialEq)]
pub enum Refused {
    /// A field is wrong, or the file would pass its size limit.
    Bad(String),
    /// No entry has that id.
    NotFound,
}

pub struct Journal {
    path: PathBuf,
    state: Mutex<File>,
}

impl Journal {
    /// Open the journal at `path`: empty if there's no file yet.
    ///
    /// # Errors
    /// The file is there but can't be read as a journal; it is left as it is.
    pub fn open(path: PathBuf) -> Result<Self, String> {
        let file = match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice::<File>(&bytes)
                .map_err(|e| format!("{} can't be read: {e}", path.display()))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => File::default(),
            Err(e) => return Err(format!("{} can't be read: {e}", path.display())),
        };
        Ok(Self {
            path,
            state: Mutex::new(file),
        })
    }

    /// The entries whose day is in `from..=to` (either end open), newest first.
    pub fn list(&self, from: Option<&str>, to: Option<&str>) -> Vec<Entry> {
        let mut out: Vec<Entry> = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entries
            .iter()
            .filter(|e| from.is_none_or(|f| e.day.as_str() >= f))
            .filter(|e| to.is_none_or(|t| e.day.as_str() <= t))
            .cloned()
            .collect();
        out.sort_by(|a, b| b.day.cmp(&a.day).then(b.at.cmp(&a.at)));
        out
    }

    /// Add an entry from a request body; `day` is required.
    ///
    /// # Errors
    /// A field is wrong, or the file would pass its size limit.
    pub fn create(&self, body: &Value) -> Result<Entry, Refused> {
        let now = now_ms();
        let mut state = self.lock();
        let fields = Fields::parse(body, None)?;
        let entry = Entry {
            id: new_id(now, &state.entries),
            day: fields.day,
            at: now,
            account: fields.account,
            text: fields.text,
            tags: fields.tags,
            rating: fields.rating,
            updated: now,
        };
        let mut next = state.entries.clone();
        next.push(entry.clone());
        self.save(&next)?;
        state.entries = next;
        Ok(entry)
    }

    /// Change the fields a request body names; the rest stay as they were.
    ///
    /// # Errors
    /// No entry has `id`; a field is wrong, or the file would pass its size limit.
    pub fn update(&self, id: &str, body: &Value) -> Result<Entry, Refused> {
        let now = now_ms();
        let mut state = self.lock();
        let i = state
            .entries
            .iter()
            .position(|e| e.id == id)
            .ok_or(Refused::NotFound)?;
        let fields = Fields::parse(body, Some(&state.entries[i]))?;
        let mut next = state.entries.clone();
        let e = &mut next[i];
        e.day = fields.day;
        e.account = fields.account;
        e.text = fields.text;
        e.tags = fields.tags;
        e.rating = fields.rating;
        e.updated = now;
        let entry = e.clone();
        self.save(&next)?;
        state.entries = next;
        Ok(entry)
    }

    /// Remove an entry.
    ///
    /// # Errors
    /// No entry has `id`, or the file can't be written.
    pub fn delete(&self, id: &str) -> Result<(), Refused> {
        let mut state = self.lock();
        let mut next = state.entries.clone();
        let before = next.len();
        next.retain(|e| e.id != id);
        if next.len() == before {
            return Err(Refused::NotFound);
        }
        self.save(&next)?;
        state.entries = next;
        Ok(())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, File> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Write the whole journal: a temporary file, flushed, then renamed over
    /// the old one. Nothing changes in memory unless this succeeds.
    fn save(&self, entries: &[Entry]) -> Result<(), Refused> {
        let bytes = serde_json::to_vec_pretty(&File {
            entries: entries.to_vec(),
        })
        .map_err(|e| Refused::Bad(e.to_string()))?;
        if bytes.len() > MAX_TOTAL {
            return Err(Refused::Bad("the journal would pass 8 MiB".into()));
        }
        let io = |e: std::io::Error| Refused::Bad(e.to_string());
        if let Some(dir) = self.path.parent() {
            std::fs::create_dir_all(dir).map_err(io)?;
        }
        let tmp = self.path.with_extension("json.tmp");
        std::fs::write(&tmp, &bytes).map_err(io)?;
        std::fs::File::open(&tmp)
            .and_then(|f| f.sync_all())
            .map_err(io)?;
        crash_point();
        std::fs::rename(&tmp, &self.path).map_err(io)
    }
}

/// Tests only: a save killed here leaves the temporary file and the old journal.
#[cfg(test)]
fn crash_point() {
    if std::env::var_os("TT_JOURNAL_CRASH_AFTER_TMP").is_some() {
        std::process::abort();
    }
}

#[cfg(not(test))]
fn crash_point() {}

/// The fields a request sets, checked. On an update, the fields it leaves out
/// are the entry's own.
struct Fields {
    day: String,
    text: String,
    tags: Vec<String>,
    rating: Option<u8>,
    account: Option<String>,
}

impl Fields {
    fn parse(body: &Value, base: Option<&Entry>) -> Result<Self, Refused> {
        let bad = |m: &str| Refused::Bad(m.to_owned());
        let obj = body.as_object().ok_or_else(|| bad("a JSON object"))?;
        let day = match obj.get("day") {
            Some(v) => {
                let d = v.as_str().ok_or_else(|| bad("day is YYYY-MM-DD"))?;
                if !valid_day(d) {
                    return Err(bad("day is YYYY-MM-DD, a real date"));
                }
                d.to_owned()
            }
            None => base
                .map(|b| b.day.clone())
                .ok_or_else(|| bad("day is required"))?,
        };
        let text = match obj.get("text") {
            Some(v) => {
                let t = v.as_str().ok_or_else(|| bad("text is a string"))?;
                if t.len() > MAX_TEXT {
                    return Err(bad("text is more than 8 KiB"));
                }
                if t.trim().is_empty() {
                    return Err(bad("text is empty"));
                }
                t.to_owned()
            }
            None => base
                .map(|b| b.text.clone())
                .ok_or_else(|| bad("text is required"))?,
        };
        let tags = match obj.get("tags") {
            Some(v) => tags_of(v)?,
            None => base.map(|b| b.tags.clone()).unwrap_or_default(),
        };
        let rating = match obj.get("rating") {
            Some(Value::Null) => None,
            Some(v) => match v.as_u64() {
                Some(r @ 1..=5) => Some(u8::try_from(r).unwrap_or(5)),
                _ => return Err(bad("rating is 1 to 5, or null")),
            },
            None => base.and_then(|b| b.rating),
        };
        let account = match obj.get("account") {
            Some(Value::Null) => None,
            Some(v) => {
                let a = v
                    .as_str()
                    .ok_or_else(|| bad("account is a name, or null for all"))?;
                if a.is_empty() || a.len() > MAX_ACCOUNT || a.chars().any(char::is_control) {
                    return Err(bad("account is a name of up to 64 characters"));
                }
                Some(a.to_owned())
            }
            None => base.and_then(|b| b.account.clone()),
        };
        Ok(Self {
            day,
            text,
            tags,
            rating,
            account,
        })
    }
}

/// Tags: trimmed, no repeats, each up to 32 characters, at most 16.
fn tags_of(v: &Value) -> Result<Vec<String>, Refused> {
    let bad = |m: &str| Refused::Bad(m.to_owned());
    let list = v.as_array().ok_or_else(|| bad("tags is a list of names"))?;
    let mut out: Vec<String> = Vec::new();
    for t in list {
        let s = t
            .as_str()
            .ok_or_else(|| bad("tags is a list of names"))?
            .trim();
        if s.is_empty() || s.len() > MAX_TAG || s.chars().any(char::is_control) {
            return Err(bad("a tag is a name of up to 32 characters"));
        }
        if !out.iter().any(|x| x == s) {
            out.push(s.to_owned());
        }
    }
    if out.len() > MAX_TAGS {
        return Err(bad("at most 16 tags"));
    }
    Ok(out)
}

/// A real calendar date, `YYYY-MM-DD`.
fn valid_day(d: &str) -> bool {
    let b = d.as_bytes();
    if b.len() != 10 || b[4] != b'-' || b[7] != b'-' {
        return false;
    }
    let num = |s: &str| {
        s.bytes()
            .all(|c| c.is_ascii_digit())
            .then(|| s.parse::<u32>().ok())
            .flatten()
    };
    let (Some(y), Some(m), Some(day)) = (num(&d[0..4]), num(&d[5..7]), num(&d[8..10])) else {
        return false;
    };
    let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    let days = match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => return false,
    };
    (1..=days).contains(&day)
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// An id unused in the journal: the time in hex, bumped while taken.
fn new_id(ms: u64, entries: &[Entry]) -> String {
    let mut n = ms;
    loop {
        let id = format!("{n:x}");
        if !entries.iter().any(|e| e.id == id) {
            return id;
        }
        n += 1;
    }
}

/// The journal's path beside the reminders, unless named (`CONSOLE_JOURNAL`).
#[must_use]
pub fn default_path(reminders: Option<&Path>) -> Option<PathBuf> {
    reminders
        .and_then(Path::parent)
        .map(|d| d.join("journal.json"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn dir(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("tt-journal-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn note(day: &str, text: &str) -> Value {
        json!({ "day": day, "text": text, "tags": ["revenge"], "rating": 2, "account": "paper-1" })
    }

    #[test]
    fn create_update_delete_and_reload() {
        let d = dir("crud");
        let path = d.join("journal.json");
        let j = Journal::open(path.clone()).unwrap();
        let a = j
            .create(&note("2026-10-16", "Apathetic, bet too large"))
            .unwrap();
        assert_eq!(a.account.as_deref(), Some("paper-1"));
        assert_eq!(a.rating, Some(2));
        let b = j
            .create(&json!({ "day": "2026-10-16", "text": "second note" }))
            .unwrap();
        assert_eq!(b.account, None, "no account given is all accounts");
        assert_eq!(b.rating, None);

        // An update changes only what it names.
        let c = j
            .update(&a.id, &json!({ "rating": 5, "tags": ["followed plan"] }))
            .unwrap();
        assert_eq!(c.text, "Apathetic, bet too large");
        assert_eq!(c.rating, Some(5));
        assert_eq!(c.tags, vec!["followed plan".to_string()]);
        assert!(c.updated >= a.updated);
        // Null clears the rating and the account.
        let e = j
            .update(&a.id, &json!({ "rating": null, "account": null }))
            .unwrap();
        assert_eq!((e.rating, e.account.clone()), (None, None));

        j.delete(&b.id).unwrap();
        assert_eq!(j.delete(&b.id), Err(Refused::NotFound));
        assert_eq!(
            j.update("nope", &json!({ "text": "x" })),
            Err(Refused::NotFound)
        );

        // Reloaded from disk: the same entries.
        let again = Journal::open(path).unwrap();
        let all = again.list(None, None);
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].id, a.id);
        assert_eq!(all[0].text, "Apathetic, bet too large");
        let _ = std::fs::remove_dir_all(d);
    }

    #[test]
    fn lists_by_range_newest_first() {
        let d = dir("list");
        let j = Journal::open(d.join("journal.json")).unwrap();
        for day in ["2026-10-14", "2026-10-16", "2026-10-15"] {
            j.create(&note(day, day)).unwrap();
        }
        // Two on the same day: the later one first.
        j.create(&note("2026-10-16", "later that day")).unwrap();
        let days: Vec<String> = j.list(None, None).into_iter().map(|e| e.day).collect();
        assert_eq!(
            days,
            ["2026-10-16", "2026-10-16", "2026-10-15", "2026-10-14"]
        );
        let mid = j.list(Some("2026-10-15"), Some("2026-10-15"));
        assert_eq!(mid.len(), 1);
        let _ = std::fs::remove_dir_all(d);
    }

    #[test]
    fn refuses_bad_fields() {
        let d = dir("bad");
        let j = Journal::open(d.join("journal.json")).unwrap();
        let bad = [
            json!({ "day": "2026-02-30", "text": "x" }),
            json!({ "day": "2026-13-01", "text": "x" }),
            json!({ "day": "16-10-2026", "text": "x" }),
            json!({ "text": "no day" }),
            json!({ "day": "2026-10-16", "text": "   " }),
            json!({ "day": "2026-10-16", "text": "x", "rating": 0 }),
            json!({ "day": "2026-10-16", "text": "x", "rating": 6 }),
            json!({ "day": "2026-10-16", "text": "x", "rating": 3.5 }),
            json!({ "day": "2026-10-16", "text": "x", "account": "a\nb" }),
            json!({ "day": "2026-10-16", "text": "x", "account": "a".repeat(65) }),
            json!({ "day": "2026-10-16", "text": "x", "tags": ["a", 3] }),
            json!({ "day": "2026-10-16", "text": "x", "tags": (0..17).map(|i| format!("t{i}")).collect::<Vec<_>>() }),
        ];
        for body in &bad {
            assert!(
                matches!(j.create(body), Err(Refused::Bad(_))),
                "accepted {body}"
            );
        }
        // Leap days are real.
        assert!(
            j.create(&json!({ "day": "2028-02-29", "text": "leap" }))
                .is_ok()
        );
        assert!(
            j.create(&json!({ "day": "2026-02-29", "text": "no" }))
                .is_err()
        );
        // Duplicate tags collapse.
        let e = j
            .create(
                &json!({ "day": "2026-10-16", "text": "x", "tags": ["cut early", " cut early "] }),
            )
            .unwrap();
        assert_eq!(e.tags, vec!["cut early".to_string()]);
        assert_eq!(j.list(None, None).len(), 2, "refused entries are not kept");
        let _ = std::fs::remove_dir_all(d);
    }

    #[test]
    fn size_limits_hold_and_leave_the_file_as_it_was() {
        let d = dir("size");
        let path = d.join("journal.json");
        let j = Journal::open(path.clone()).unwrap();
        // Text over 8 KiB is refused; text at 8 KiB is fine.
        assert!(matches!(
            j.create(&json!({ "day": "2026-10-16", "text": "x".repeat((8 << 10) + 1) })),
            Err(Refused::Bad(_))
        ));
        j.create(&json!({ "day": "2026-10-16", "text": "x".repeat(8 << 10) }))
            .unwrap();
        // A journal written to within one entry of the 8 MiB total (in one go, to keep the test quick).
        let block = "y".repeat(8 << 10);
        let entry = |i: usize| Entry {
            id: format!("seed{i}"),
            day: "2026-10-16".into(),
            at: 1,
            account: None,
            text: block.clone(),
            tags: vec![],
            rating: None,
            updated: 1,
        };
        // As many as fit: less than one more entry's room is left.
        let size = |n: usize| {
            serde_json::to_vec_pretty(&File {
                entries: (0..n).map(entry).collect(),
            })
            .unwrap()
            .len()
        };
        let one = size(1) - size(0);
        let n = (MAX_TOTAL - size(0)) / one;
        std::fs::write(
            &path,
            serde_json::to_vec_pretty(&File {
                entries: (0..n).map(entry).collect(),
            })
            .unwrap(),
        )
        .unwrap();
        let j = Journal::open(path.clone()).unwrap();
        let before = std::fs::read(&path).unwrap();
        assert!(before.len() <= MAX_TOTAL, "{} bytes", before.len());
        // One more 8 KiB entry would pass the total: refused, and the file is untouched.
        let refused = j.create(&json!({ "day": "2026-10-16", "text": block }));
        assert!(
            matches!(refused, Err(Refused::Bad(ref m)) if m.contains("8 MiB")),
            "{refused:?}"
        );
        assert_eq!(std::fs::read(&path).unwrap(), before);
        let _ = std::fs::remove_dir_all(d);
    }

    #[test]
    fn a_save_killed_midway_keeps_the_old_journal() {
        // Run as a child process: it aborts between writing the temporary file
        // and renaming it over the journal.
        let d = dir("crash");
        let path = d.join("journal.json");
        {
            let j = Journal::open(path.clone()).unwrap();
            j.create(&note("2026-10-16", "saved before the crash"))
                .unwrap();
        }
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "journal::tests::crash_child", "--nocapture"])
            .env("TT_JOURNAL_CRASH_PATH", &path)
            .env("TT_JOURNAL_CRASH_AFTER_TMP", "1")
            .status()
            .unwrap();
        assert!(!status.success(), "the child should have been killed");
        // The journal is the one from before: the new entry isn't there, and the
        // leftover temporary file is ignored.
        let after = Journal::open(path).unwrap().list(None, None);
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].text, "saved before the crash");
        let _ = std::fs::remove_dir_all(d);
    }

    /// The child of `a_save_killed_midway_keeps_the_old_journal`; does nothing otherwise.
    #[test]
    fn crash_child() {
        let Some(path) = std::env::var_os("TT_JOURNAL_CRASH_PATH") else {
            return;
        };
        let j = Journal::open(PathBuf::from(path)).unwrap();
        let _ = j.create(&note("2026-10-16", "never saved"));
        // Not reached: the save aborts the process.
    }
}
