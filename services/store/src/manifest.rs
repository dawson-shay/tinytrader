//! A schema folder's manifest (decisions 0018 and 0019): what the store
//! records about its days beyond the files themselves.
//!
//! - `manifest.json` is a small index: one entry per day that has anything
//!   recorded. A complete day's entry (its condition, say) is kept there
//!   whole. A partial day's entry is a stub, `{ sidecar, partial: true,
//!   condition }`, and the day's full entry, with its coverage and every
//!   contract, is in `manifest.d/<day>.json`. Filing or merging a day rewrites
//!   that day's file, and the index only when its stub changes, so the cost
//!   doesn't grow with the days the folder holds. An index written before
//!   sidecars, with partial entries inline, is still read, and each such entry
//!   moves to its sidecar the first time the index is written.
//! - A merge in progress, or a complete day superseding a partial one, is
//!   `manifest.d/<day>.pending.json`: the entry the day will have once the
//!   new file is in place. It is made current by
//!   renaming it over `<day>.json`, or dropped, by [`settle`].
//!
//! Every file is written whole: beside it, flushed, renamed over it, and the
//! folder flushed.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};

use crate::ingest::{hash_file, sync_dir, write_new};

/// A schema folder's index of its days.
pub const MANIFEST: &str = "manifest.json";
const SCHEMA: &str = "tinytrader.store-manifest/1";
/// Where partial days' full entries are kept.
pub const SIDECARS: &str = "manifest.d";

/// Is this entry (or index stub) a partial day? Only one with its coverage
/// is; one holding only the day's condition is a complete day.
#[must_use]
pub fn is_partial(entry: &Value) -> bool {
    entry.get("coverage").is_some() || entry["partial"] == true
}

fn sidecar(dir: &Path, day: &str) -> PathBuf {
    dir.join(SIDECARS).join(format!("{day}.json"))
}

fn pending_path(dir: &Path, day: &str) -> PathBuf {
    dir.join(SIDECARS).join(format!("{day}.pending.json"))
}

fn read_json(path: &Path) -> Result<Option<Value>, String> {
    match fs::read_to_string(path) {
        Ok(text) => serde_json::from_str(&text)
            .map(Some)
            .map_err(|e| format!("{}: {e}", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(format!("{}: {e}", path.display())),
    }
}

/// Write `v` to `path` whole.
fn write_json(path: &Path, v: &Value) -> Result<(), String> {
    let dir = path.parent().ok_or("no folder")?;
    fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    let text = serde_json::to_string_pretty(v).map_err(|e| e.to_string())?;
    let name = path
        .file_name()
        .map_or_else(String::new, |n| n.to_string_lossy().into_owned());
    let tmp = dir.join(format!(".{name}.new"));
    let _ = fs::remove_file(&tmp);
    write_new(&tmp, text.as_bytes()).map_err(|e| format!("{}: {e}", tmp.display()))?;
    fs::rename(&tmp, path).map_err(|e| format!("{}: {e}", path.display()))?;
    sync_dir(dir).map_err(|e| format!("{}: {e}", dir.display()))
}

/// The index: day → its entry, or its stub for a partial day. A missing file
/// is an empty one.
///
/// # Errors
/// If it can't be read or isn't JSON.
pub fn read_index(dir: &Path) -> Result<BTreeMap<String, Value>, String> {
    Ok(read_json(&dir.join(MANIFEST))?
        .and_then(|v| v["days"].as_object().cloned())
        .map(|o| o.into_iter().collect())
        .unwrap_or_default())
}

/// The days of the folder with a merge in progress.
pub fn pending_days(dir: &Path) -> Vec<String> {
    let mut out: Vec<String> = fs::read_dir(dir.join(SIDECARS))
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|e| {
            let n = e.file_name().to_string_lossy().into_owned();
            n.strip_suffix(".pending.json").map(str::to_owned)
        })
        .collect();
    out.sort();
    out
}

/// One day's full entry, given its index entry: the sidecar's, if it has
/// one, with a merge in progress as its `pending`.
///
/// # Errors
/// If a sidecar can't be read.
pub fn resolve(dir: &Path, day: &str, indexed: &Value) -> Result<Value, String> {
    resolve_listed(dir, day, indexed, true)
}

/// As [`resolve`], reading the day's merge in progress only if `pending`
/// (as [`pending_days`] listed it).
///
/// # Errors
/// If a sidecar can't be read.
pub fn resolve_listed(
    dir: &Path,
    day: &str,
    indexed: &Value,
    pending: bool,
) -> Result<Value, String> {
    let mut entry = if indexed.get("sidecar").is_some() {
        read_json(&sidecar(dir, day))?
            .ok_or_else(|| format!("{}: missing", sidecar(dir, day).display()))?
    } else {
        indexed.clone()
    };
    if pending && let Some(p) = read_json(&pending_path(dir, day))? {
        entry["pending"] = p;
    }
    Ok(entry)
}

/// One day's full entry, if it has one.
///
/// # Errors
/// As [`read_index`] and [`resolve`].
pub fn read_day(dir: &Path, day: &str) -> Result<Option<Value>, String> {
    let index = read_index(dir)?;
    match index.get(day) {
        Some(e) => resolve(dir, day, e).map(Some),
        // A merge recorded before its first entry (never written so, but
        // read so if it is).
        None => Ok(read_json(&pending_path(dir, day))?.map(|p| json!({ "pending": p }))),
    }
}

/// Every day's full entry. Reads every sidecar: for one day, [`read_day`].
///
/// # Errors
/// As [`read_day`].
#[cfg(test)]
pub fn read_manifest(dir: &Path) -> Result<BTreeMap<String, Value>, String> {
    let index = read_index(dir)?;
    index
        .iter()
        .map(|(day, e)| resolve(dir, day, e).map(|full| (day.clone(), full)))
        .collect()
}

/// Set (or with `None`, remove) these days' entries, with one write of the
/// index at most: a partial day's entry goes to its sidecar, and its stub to
/// the index. Entries written before sidecars move to theirs.
///
/// # Errors
/// If a file can't be read or written.
pub fn write_days(dir: &Path, days: &[(&str, Option<Value>)]) -> Result<(), String> {
    let mut index = read_index(dir)?;
    let before = index.clone();
    // Inline partial entries, as written before sidecars, move out.
    for (day, e) in &mut index {
        if e.get("coverage").is_some() {
            let mut full = e.clone();
            // A merge left pending inline stays pending, in its own file.
            if let Some(p) = full.as_object_mut().and_then(|o| o.remove("pending")) {
                write_pending(dir, day, &p)?;
            }
            write_json(&sidecar(dir, day), &full)?;
            *e = stub(day, &full);
        }
    }
    for (day, entry) in days {
        match entry {
            Some(e) if is_partial(e) => {
                let mut full = e.clone();
                full.as_object_mut().map(|o| o.remove("pending"));
                write_json(&sidecar(dir, day), &full)?;
                index.insert((*day).to_owned(), stub(day, &full));
            }
            Some(e) => {
                let _ = fs::remove_file(sidecar(dir, day));
                index.insert((*day).to_owned(), e.clone());
            }
            None => {
                let _ = fs::remove_file(sidecar(dir, day));
                index.remove(*day);
            }
        }
    }
    if index != before || !dir.join(MANIFEST).exists() {
        write_json(
            &dir.join(MANIFEST),
            &json!({ "schema": SCHEMA, "days": index }),
        )?;
    }
    Ok(())
}

/// A partial day's stub in the index.
fn stub(day: &str, full: &Value) -> Value {
    let mut s = json!({ "sidecar": format!("{SIDECARS}/{day}.json"), "partial": true });
    if let Some(c) = full.get("condition") {
        s["condition"] = c.clone();
    }
    s
}

/// Set (or remove) one day's entry.
///
/// # Errors
/// As [`write_days`].
pub fn set_entry(dir: &Path, day: &str, entry: Option<Value>) -> Result<(), String> {
    write_days(dir, &[(day, entry)])
}

/// Record the entry a day will have once its merged file is in place.
///
/// # Errors
/// If it can't be written.
pub fn write_pending(dir: &Path, day: &str, entry: &Value) -> Result<(), String> {
    write_json(&pending_path(dir, day), entry)
}

/// Make a day's pending entry current: renamed over its sidecar, so the
/// index stays as it is (a partial day stays partial).
///
/// # Errors
/// If it can't be renamed.
pub fn promote_pending(dir: &Path, day: &str) -> Result<(), String> {
    let (from, to) = (pending_path(dir, day), sidecar(dir, day));
    fs::rename(&from, &to).map_err(|e| format!("{} -> {}: {e}", from.display(), to.display()))?;
    sync_dir(&dir.join(SIDECARS)).map_err(|e| e.to_string())
}

/// Settle a day whose merge was interrupted: the file's hash says whether the
/// swap happened (the pending entry becomes current) or not (it's dropped).
/// Returns the day's entry as it now stands.
///
/// # Errors
/// If the file is neither its recorded one nor its pending merge.
pub fn settle(dir: &Path, day: &str) -> Result<Option<Value>, String> {
    let Some(mut entry) = read_day(dir, day)? else {
        return Ok(None);
    };
    let Some(pending) = entry.get("pending").cloned() else {
        return Ok(Some(entry));
    };
    let file = dir.join(format!("{day}.dbn.zst"));
    let got = hash_file(&file)?;
    let as_sidecar = pending_path(dir, day).exists();
    if pending["sha256"] == got.as_str() {
        let stubbed = read_index(dir)?
            .get(day)
            .is_some_and(|e| e.get("sidecar").is_some());
        if as_sidecar && stubbed && is_partial(&pending) {
            promote_pending(dir, day)?;
        } else {
            // Recorded inline, before sidecars, or a complete day superseding
            // a partial one: written as an entry.
            set_entry(dir, day, Some(pending.clone()))?;
            let _ = fs::remove_file(pending_path(dir, day));
        }
        return Ok(Some(pending));
    }
    if entry["sha256"] == got.as_str() {
        entry.as_object_mut().map(|o| o.remove("pending"));
        if !as_sidecar {
            // Recorded inline, before sidecars (writing moves it to its file).
            set_entry(dir, day, Some(entry.clone()))?;
        }
        fs::remove_file(pending_path(dir, day)).map_err(|e| e.to_string())?;
        return Ok(Some(entry));
    }
    Err(format!(
        "{}: SHA-256 {got} is neither its recorded file nor its pending merge",
        file.display()
    ))
}

/// Several copies of one schema folder's manifest index (`manifest.json`),
/// as `store migrate-cas` finds them on different drives, merged into one:
/// every day any copy has an entry for. A day whose entries differ is a
/// conflict, never resolved by choosing: its entry becomes
/// `{"condition": {"state": "unknown", "reason": …}, "conflicting":
/// [{"source", "entry"}, …]}`, so its coverage and condition read as unknown
/// until an operator settles it. A conflicting entry merged again keeps its
/// versions (not nested). Returns the merged index and the days in conflict.
#[must_use]
pub fn merge_indexes(copies: &[(String, Value)]) -> (Value, Vec<String>) {
    let mut days: BTreeMap<String, Vec<(String, Value)>> = BTreeMap::new();
    for (source, m) in copies {
        for (day, e) in m["days"].as_object().into_iter().flatten() {
            let found = days.entry(day.clone()).or_default();
            let versions: Vec<(String, Value)> = match e["conflicting"].as_array() {
                Some(c) => c
                    .iter()
                    .map(|x| {
                        (
                            x["source"].as_str().unwrap_or(source).to_owned(),
                            x["entry"].clone(),
                        )
                    })
                    .collect(),
                None => vec![(source.clone(), e.clone())],
            };
            for (s, v) in versions {
                if !found.iter().any(|(_, f)| *f == v) {
                    found.push((s, v));
                }
            }
        }
    }
    let mut out = serde_json::Map::new();
    let mut conflicts = Vec::new();
    for (day, mut versions) in days {
        if versions.len() == 1 {
            out.insert(day, versions.remove(0).1);
            continue;
        }
        conflicts.push(day.clone());
        out.insert(
            day,
            json!({
                "condition": { "state": "unknown", "reason": "its manifest entries differ between copies of this folder (store migrate-cas); an operator settles which holds" },
                "conflicting": versions.iter().map(|(s, e)| json!({ "source": s, "entry": e })).collect::<Vec<_>>(),
            }),
        );
    }
    (json!({ "schema": SCHEMA, "days": out }), conflicts)
}

#[cfg(test)]
mod merge_tests {
    use super::*;

    #[test]
    fn copies_merge_by_day_and_disagreements_are_unknown() {
        let a = json!({ "schema": SCHEMA, "days": {
            "2026-10-01": { "condition": { "state": "degraded" } },
            "2026-10-02": { "sidecar": "manifest.d/2026-10-02.json", "partial": true },
        } });
        let b = json!({ "schema": SCHEMA, "days": {
            "2026-10-02": { "sidecar": "manifest.d/2026-10-02.json", "partial": true },
            "2026-10-03": { "condition": { "state": "missing" } },
            "2026-10-01": { "condition": { "state": "missing" } },
        } });
        let (m, c) = merge_indexes(&[("data".into(), a.clone()), ("tt03".into(), b.clone())]);
        assert_eq!(c, ["2026-10-01"]);
        assert_eq!(m["days"]["2026-10-02"]["partial"], true);
        assert_eq!(m["days"]["2026-10-03"]["condition"]["state"], "missing");
        assert_eq!(m["days"]["2026-10-01"]["condition"]["state"], "unknown");
        assert_eq!(
            m["days"]["2026-10-01"]["conflicting"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        // Merged again with a copy: the same, not nested.
        let (again, c2) = merge_indexes(&[("bound".into(), m.clone()), ("data".into(), a)]);
        assert_eq!((again, c2), (m, vec!["2026-10-01".to_owned()]));
        // One copy: itself.
        let (one, none) = merge_indexes(&[("tt03".into(), b.clone())]);
        assert_eq!((one["days"].clone(), none.len()), (b["days"].clone(), 0));
    }
}
