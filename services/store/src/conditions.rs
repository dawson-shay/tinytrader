//! A day's condition (decision 0002, "Coverage and gaps"): whether the
//! provider calls it `available`, `degraded` or `missing`. It is kept with
//! the day, in its schema folder's manifest entry, as `condition: { state,
//! last_modified, source, checked_at }`. A day with an entry holding only its
//! condition is still a complete day; only an entry with `coverage` is
//! partial.
//!
//! Conditions come from a batch's `condition.json` when it is filed (source
//! `batch <id>`), from `POST /conditions/{provider}` (source `provider api`),
//! and once from the `condition.json` receipts already kept in `provenance/`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};

use crate::manifest::{read_day, write_days, write_pending};

/// The states Databento reports.
const STATES: [&str; 3] = ["available", "degraded", "missing"];

/// One day's condition as a provider reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Row {
    pub state: String,
    pub last_modified: Option<String>,
}

/// The rows of a `condition.json` (`[{ date, condition, last_modified_date
/// }]`, as Databento writes it; `day`, `state` and `last_modified` are taken
/// too), by day.
///
/// # Errors
/// If it isn't a list of such rows.
pub fn parse(v: &Value) -> Result<BTreeMap<String, Row>, String> {
    let mut out = BTreeMap::new();
    for r in v.as_array().ok_or("a condition list is a JSON array")? {
        let day = r["date"]
            .as_str()
            .or_else(|| r["day"].as_str())
            .filter(|d| d.len() == 10 && crate::ingest::day_ns(d).is_some())
            .ok_or_else(|| format!("no usable day in {r}"))?;
        let state = r["condition"]
            .as_str()
            .or_else(|| r["state"].as_str())
            .filter(|s| STATES.contains(s))
            .ok_or_else(|| format!("{day}: the state must be one of {}", STATES.join(", ")))?;
        let last_modified = r["last_modified_date"]
            .as_str()
            .or_else(|| r["last_modified"].as_str())
            .map(str::to_owned);
        out.insert(
            day.to_owned(),
            Row {
                state: state.to_owned(),
                last_modified,
            },
        );
    }
    Ok(out)
}

/// Record `row` as the condition of `day` in the schema folder `dir`, unless
/// it already says so. Returns the state it replaced (`null` when there was
/// none) when it changed anything.
///
/// # Errors
/// If the manifest can't be read or written.
#[cfg(test)]
pub fn record(dir: &Path, day: &str, row: &Row, source: &str) -> Result<Option<Value>, String> {
    Ok(record_many(dir, &[(day, row)], source)?
        .pop()
        .map(|(_, old)| old))
}

/// As [`record`], for several days of one folder, with one write of its
/// index at most. Returns each day changed, with the state it replaced.
///
/// # Errors
/// As [`record`].
pub fn record_many(
    dir: &Path,
    rows: &[(&str, &Row)],
    source: &str,
) -> Result<Vec<(String, Value)>, String> {
    let mut writes = Vec::new();
    let mut changed = Vec::new();
    for &(day, row) in rows {
        let mut entry = read_day(dir, day)?.unwrap_or_else(|| json!({}));
        let old = entry.get("condition").cloned().unwrap_or(Value::Null);
        if old["state"] == row.state.as_str() && old["last_modified"] == json!(row.last_modified) {
            continue;
        }
        let condition = json!({
            "state": row.state, "last_modified": row.last_modified, "source": source,
            "checked_at": tinytrader_observe::unix_seconds(),
        });
        entry["condition"] = condition.clone();
        // A merge in progress keeps it when it becomes the entry.
        if let Some(mut p) = entry.as_object_mut().and_then(|o| o.remove("pending")) {
            p["condition"] = condition;
            write_pending(dir, day, &p)?;
        }
        changed.push((day.to_owned(), old["state"].clone()));
        writes.push((day, Some(entry)));
    }
    if !writes.is_empty() {
        write_days(dir, &writes)?;
    }
    Ok(changed)
}

/// Record the condition of each day file in `filed` (paths of
/// `<schema folder>/<day>.dbn.zst`) that `rows` gives.
///
/// # Errors
/// As [`record`].
pub fn record_filed<'a>(
    filed: impl IntoIterator<Item = &'a Path>,
    rows: &BTreeMap<String, Row>,
    source: &str,
) -> Result<usize, String> {
    let mut by_dir: BTreeMap<&Path, Vec<(&str, &Row)>> = BTreeMap::new();
    for path in filed {
        let Some(day) = path
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(|n| n.strip_suffix(".dbn.zst"))
        else {
            continue;
        };
        if let (Some((day, row)), Some(dir)) = (rows.get_key_value(day), path.parent()) {
            by_dir.entry(dir).or_default().push((day.as_str(), row));
        }
    }
    let mut n = 0;
    for (dir, rows) in by_dir {
        n += rows.len();
        record_many(dir, &rows, source)?;
    }
    Ok(n)
}

/// The rows of the first `condition.json` in `dirs` (a batch's incoming
/// folders, then its receipts), if there is one.
///
/// # Errors
/// If it can't be read or parsed.
pub fn find(dirs: &[PathBuf]) -> Result<Option<BTreeMap<String, Row>>, String> {
    let Some(path) = dirs
        .iter()
        .map(|d| d.join("condition.json"))
        .find(|p| p.is_file())
    else {
        return Ok(None);
    };
    let text = std::fs::read_to_string(&path).map_err(|e| format!("{}: {e}", path.display()))?;
    let v: Value = serde_json::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))?;
    parse(&v)
        .map(Some)
        .map_err(|e| format!("{}: {e}", path.display()))
}

/// Record a batch's conditions for the day files it filed (`summary`'s
/// `files` or `days`, each with its `path`).
///
/// # Errors
/// As [`find`] and [`record`].
pub fn record_batch(dirs: &[PathBuf], summary: &Value, batch: &str) -> Result<usize, String> {
    let Some(rows) = find(dirs)? else {
        return Ok(0);
    };
    let paths: Vec<PathBuf> = ["files", "days"]
        .iter()
        .flat_map(|k| summary[k].as_array().into_iter().flatten())
        .filter_map(|f| f["path"].as_str().map(PathBuf::from))
        .collect();
    record_filed(
        paths.iter().map(PathBuf::as_path),
        &rows,
        &format!("batch {batch}"),
    )
}

/// What applying conditions did: `{ changed: [{ dataset, day, volume,
/// folder, from, to }], unchanged, not_held: [{ dataset, day }] }`.
#[derive(Default)]
pub struct Applied {
    pub changed: Vec<Value>,
    pub unchanged: usize,
    pub not_held: Vec<Value>,
}

/// A day's condition to record: its dataset, day and row.
type Day<'a> = (&'a str, &'a str, &'a Row);

/// Record each dataset's day conditions (`rows`, by dataset) for every
/// schema and symbol of that dataset `provider` holds on that day, on any
/// volume of `index` (the places the last scan found): a provider's
/// condition is of the whole dataset that day.
///
/// # Errors
/// If a manifest can't be read or written.
pub fn apply(
    index: &[(String, Vec<crate::catalog::Place>)],
    provider: &str,
    rows: &BTreeMap<String, BTreeMap<String, Row>>,
    source: &str,
    out: &mut Applied,
) -> Result<(), String> {
    // Each folder's days, so each folder's index is written once.
    let mut by_dir: BTreeMap<(&str, &Path), Vec<Day<'_>>> = BTreeMap::new();
    for (dataset, days) in rows {
        for (day, row) in days {
            let mut held = false;
            for (volume, places) in index {
                for place in places {
                    let crate::catalog::Place::Days {
                        provider: p,
                        dataset: d,
                        dir,
                        days: filed,
                        cas,
                        ..
                    } = place
                    else {
                        continue;
                    };
                    // A content-addressed volume's days take conditions once
                    // filing goes through its index (0035 step 3); until
                    // then nothing is written beside them.
                    if *cas {
                        continue;
                    }
                    if crate::partial::ours(p.as_deref(), provider)
                        && d.eq_ignore_ascii_case(dataset)
                        && filed.iter().any(|(f, _, file)| f == day && *file)
                    {
                        held = true;
                        by_dir
                            .entry((volume.as_str(), dir.as_path()))
                            .or_default()
                            .push((dataset.as_str(), day.as_str(), row));
                    }
                }
            }
            if !held {
                out.not_held.push(json!({ "dataset": dataset, "day": day }));
            }
        }
    }
    for ((volume, dir), list) in by_dir {
        let days: Vec<(&str, &Row)> = list.iter().map(|&(_, d, r)| (d, r)).collect();
        let changed = record_many(dir, &days, source)?;
        out.unchanged += list.len() - changed.len();
        for (day, from) in changed {
            let &(dataset, _, row) = list.iter().find(|x| x.1 == day).ok_or("lost a day")?;
            out.changed.push(json!({
                "dataset": dataset, "day": day, "volume": volume, "folder": dir,
                "from": from, "to": row.state,
            }));
        }
    }
    Ok(())
}

/// A kept `condition.json`: its batch, the dataset, and its rows by day.
pub type Kept = (String, Option<String>, BTreeMap<String, Row>);

/// Each provider batch's `condition.json` kept in `provenance/` under the
/// provider folder `root`, with the batch, and the dataset its
/// `metadata.json` (or an import's `import.json`) names; `None` for one
/// whose dataset isn't known.
///
/// # Errors
/// If the folder, or a `condition.json`, can't be read.
pub fn kept(root: &Path) -> Result<Vec<Kept>, String> {
    let prov = root.join("provenance");
    if !prov.is_dir() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for dir in crate::catalog::read_entries(&prov)? {
        let Some(rows) = find(std::slice::from_ref(&dir))? else {
            continue;
        };
        let read = |name: &str| -> Value {
            std::fs::read_to_string(dir.join(name))
                .ok()
                .and_then(|t| serde_json::from_str(&t).ok())
                .unwrap_or_default()
        };
        let dataset = read("metadata.json")["query"]["dataset"]
            .as_str()
            .map(str::to_owned)
            .or_else(|| {
                read(crate::partial::IMPORT)["dataset"]
                    .as_str()
                    .map(str::to_owned)
            });
        let batch = dir
            .file_name()
            .map_or_else(String::new, |n| n.to_string_lossy().into_owned());
        out.push((batch, dataset, rows));
    }
    Ok(out)
}
