//! What the store remembers between runs (decision 0019): every volume it
//! has used, so a drive that is missing when the store starts shows as
//! missing rather than as never having been there, and what each held when
//! last seen. A volume away for good is retired explicitly, which keeps a
//! record of what it held.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::ingest::sync_dir;

const SCHEMA: &str = "tinytrader.store-state/1";

/// The state file: `{ schema, volumes: { name: { path, writable,
/// require_mount, held } }, retired: [{ name, path, retired_at, held }],
/// accepted_empty: [...], ignored: [...] }`.
#[derive(Serialize, Deserialize, Clone, Default, PartialEq, Debug)]
pub struct Remembered {
    #[serde(default)]
    pub schema: String,
    #[serde(default)]
    pub volumes: BTreeMap<String, Known>,
    #[serde(default)]
    pub retired: Vec<Value>,
    /// Volumes accepted as empty: `{ name, path, accepted_at, by,
    /// held_before }`.
    #[serde(default)]
    pub accepted_empty: Vec<Value>,
    /// Paths an operator said to skip: `{ volume, path, reason, by, at }`.
    #[serde(default)]
    pub ignored: Vec<Value>,
    /// Listings an operator gave for a volume away, and what each added to
    /// its `held`: `{ volume, by, at, files, bytes, listing_sha256, added,
    /// unparsed }`.
    #[serde(default)]
    pub held_listings: Vec<Value>,
}

/// A volume the store has used.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct Known {
    pub path: PathBuf,
    pub writable: bool,
    /// Its path must be a mount point; true unless recorded otherwise.
    #[serde(default = "yes")]
    pub require_mount: bool,
    /// Its datasets when last scanned: `{ "provider/id/symbol": { schema:
    /// { days, first, last, bytes } } }`.
    #[serde(default)]
    pub held: Value,
}

fn yes() -> bool {
    true
}

/// Whether the volume held any datasets when last read in full.
#[must_use]
pub fn held_any(k: &Known) -> bool {
    k.held.as_object().is_some_and(|o| !o.is_empty())
}

/// Where the state file is by default.
#[must_use]
pub fn default_path() -> PathBuf {
    let home = std::env::var_os("HOME").map_or_else(|| PathBuf::from("."), PathBuf::from);
    home.join("Library/Application Support/tinytrader/store-state.json")
}

/// Read the state file; a missing one is empty.
///
/// # Errors
/// If it exists but can't be read or isn't a state file.
pub fn load(path: &Path) -> Result<Remembered, String> {
    match fs::read_to_string(path) {
        Ok(text) => serde_json::from_str(&text).map_err(|e| format!("{}: {e}", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Remembered::default()),
        Err(e) => Err(format!("{}: {e}", path.display())),
    }
}

/// Replace the state file: written beside it, flushed, renamed over it, and
/// its folder flushed, so it is always one whole version.
///
/// # Errors
/// If it can't be written.
pub fn save(path: &Path, state: &Remembered) -> Result<(), String> {
    let dir = path.parent().unwrap_or(Path::new("."));
    fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    let mut doc = state.clone();
    doc.schema = SCHEMA.to_owned();
    let text = serde_json::to_string_pretty(&doc).map_err(|e| e.to_string())?;
    let name = path
        .file_name()
        .map_or_else(|| "state".into(), |n| n.to_string_lossy().into_owned());
    let tmp = dir.join(format!(".{name}.new"));
    let write = || -> std::io::Result<()> {
        let mut f = fs::File::create(&tmp)?;
        std::io::Write::write_all(&mut f, text.as_bytes())?;
        f.sync_all()?;
        fs::rename(&tmp, path)?;
        sync_dir(dir)
    };
    write().map_err(|e| format!("{}: {e}", path.display()))
}

/// One file of an operator's listing of a volume away: its path within the
/// volume, its size, and its sha256 where known.
#[derive(Deserialize, Clone, Debug)]
pub struct Listed {
    pub path: String,
    #[serde(default)]
    pub size: u64,
    #[serde(default)]
    pub sha256: Option<String>,
}

/// What a listing says a volume holds, in `held`'s shape, from the store
/// layout's names (`Provider/DATASET/SYMBOL/schema/YYYY-MM-DD.ext`, or
/// without the symbol for a whole dataset), and the paths that aren't in it
/// (the store's own folders, other layouts), returned so none is dropped
/// unseen.
#[must_use]
pub fn held_of_listing(files: &[Listed]) -> (Value, Vec<String>) {
    let mut days: BTreeMap<(String, String), (std::collections::BTreeSet<String>, u64)> =
        BTreeMap::new();
    let mut unparsed = Vec::new();
    for f in files {
        let path = f.path.trim_start_matches("./");
        let parts: Vec<&str> = path.split('/').collect();
        let parsed = (|| {
            let (&file, dirs) = parts.split_last()?;
            let provider = crate::catalog::provider_name(dirs.first()?)?;
            let (dataset, symbol, schema) = match dirs {
                [_, d, s, sc] => (*d, *s, *sc),
                [_, d, sc] => (*d, "-", *sc),
                _ => return None,
            };
            if !crate::catalog::is_dataset_name(dataset)
                || [symbol, schema]
                    .iter()
                    .any(|x| x.starts_with('.') || x.is_empty())
            {
                return None;
            }
            let day = file
                .get(..10)
                .filter(|d| crate::ingest::day_ns(d).is_some())?;
            file[10..].starts_with('.').then_some(())?;
            Some((
                format!("{provider}/{dataset}/{symbol}"),
                schema.to_owned(),
                day.to_owned(),
            ))
        })();
        match parsed {
            Some((key, schema, day)) => {
                let e = days.entry((key, schema)).or_default();
                e.0.insert(day);
                e.1 += f.size;
            }
            None => unparsed.push(f.path.clone()),
        }
    }
    let mut out = serde_json::Map::new();
    for ((key, schema), (d, bytes)) in days {
        let entry = out.entry(key).or_insert_with(|| json!({}));
        entry[schema] = json!({
            "days": d.len(), "first": d.first(), "last": d.last(), "bytes": bytes,
        });
    }
    (Value::Object(out), unparsed)
}

/// `held` widened by `add`: each schema's first and last the outermost of
/// the two, and its days and bytes the larger. Nothing is ever narrowed.
#[must_use]
pub fn widen(held: &Value, add: &Value) -> Value {
    let mut out = held.as_object().cloned().unwrap_or_default();
    for (key, schemas) in add.as_object().into_iter().flatten() {
        let entry = out.entry(key.clone()).or_insert_with(|| json!({}));
        for (schema, a) in schemas.as_object().into_iter().flatten() {
            let merged = match entry.get(schema) {
                Some(h) if h.is_object() => {
                    let mut m = h.clone();
                    let pick = |x: &Value, y: &Value, earlier: bool| match (x.as_str(), y.as_str())
                    {
                        (Some(p), Some(q)) => json!(if (p < q) == earlier { p } else { q }),
                        (Some(p), None) => json!(p),
                        (None, _) => y.clone(),
                    };
                    m["first"] = pick(&h["first"], &a["first"], true);
                    m["last"] = pick(&h["last"], &a["last"], false);
                    for n in ["days", "bytes"] {
                        m[n] = json!(h[n].as_u64().unwrap_or(0).max(a[n].as_u64().unwrap_or(0)));
                    }
                    m
                }
                _ => a.clone(),
            };
            entry[schema] = merged;
        }
    }
    Value::Object(out)
}

/// A volume's `held` from a scan read in full, against what it held before:
/// a schema that vanished or whose first..last narrowed keeps its earlier
/// extent, marked `suspect` (with `suspect_since`), since days don't leave a
/// volume on their own and a shrink may be a scan's mistake. Returns the new
/// `held` and, for each schema newly suspect, a line saying so. A suspect
/// schema the scan covers again is no longer suspect.
#[must_use]
pub fn keep_shrunk(before: &Value, scanned: Value, at: f64) -> (Value, Vec<String>) {
    let mut out = scanned.as_object().cloned().unwrap_or_default();
    let mut newly = Vec::new();
    for (key, schemas) in before.as_object().into_iter().flatten() {
        for (schema, was) in schemas.as_object().into_iter().flatten() {
            let now = out.get(key).and_then(|s| s.get(schema));
            let (wf, wl) = (was["first"].as_str(), was["last"].as_str());
            let covered = now.is_some_and(|n| {
                let (nf, nl) = (n["first"].as_str(), n["last"].as_str());
                wf.is_none_or(|w| nf.is_some_and(|n| n <= w))
                    && wl.is_none_or(|w| nl.is_some_and(|n| n >= w))
            });
            if covered {
                continue;
            }
            let mut kept = match now {
                Some(n) => {
                    widen(&json!({ "k": { "s": n } }), &json!({ "k": { "s": was } }))["k"]["s"]
                        .clone()
                }
                None => was.clone(),
            };
            kept["suspect"] = json!(true);
            kept["suspect_since"] = was
                .get("suspect_since")
                .cloned()
                .unwrap_or_else(|| json!(at));
            if was["suspect"] != true {
                newly.push(format!(
                    "{key} {schema}: held {}..{}, the scan found {}",
                    wf.unwrap_or("?"),
                    wl.unwrap_or("?"),
                    now.map_or_else(
                        || "none".to_owned(),
                        |n| format!(
                            "{}..{}",
                            n["first"].as_str().unwrap_or("?"),
                            n["last"].as_str().unwrap_or("?")
                        )
                    )
                ));
            }
            out.entry(key.clone()).or_insert_with(|| json!({}))[schema] = kept;
        }
    }
    (Value::Object(out), newly)
}

/// What a scanned root holds, in brief: for each dataset and schema, how
/// many days, the first and last, and the bytes.
#[must_use]
pub fn held(root: &Value) -> Value {
    let mut out = serde_json::Map::new();
    for d in root["datasets"].as_array().into_iter().flatten() {
        let name = [&d["provider"], &d["id"], &d["symbol"]]
            .iter()
            .map(|v| v.as_str().unwrap_or("-"))
            .collect::<Vec<_>>()
            .join("/");
        let entry = out.entry(name).or_insert_with(|| json!({}));
        for (schema, s) in d["schemas"].as_object().into_iter().flatten() {
            let days = s["days"].as_object();
            entry[schema] = json!({
                "days": days.map_or(0, serde_json::Map::len),
                "first": days.and_then(|d| d.keys().next()),
                "last": days.and_then(|d| d.keys().next_back()),
                "bytes": s["bytes"],
            });
        }
    }
    Value::Object(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn listed(path: &str) -> Listed {
        Listed {
            path: path.into(),
            size: 1,
            sha256: None,
        }
    }

    #[test]
    fn a_listing_in_the_store_layout_is_read_and_the_rest_reported() {
        let (held, unparsed) = held_of_listing(&[
            listed("./Databento/OPRA.PILLAR/SPY.OPT/cbbo-1m/2023-05-01.dbn.zst"),
            listed("Databento/OPRA.PILLAR/SPY.OPT/cbbo-1m/2022-09-07.dbn.zst"),
            listed("./Databento/GLBX.MDP3/bbo-1m/2022-10-02.dbn.zst"),
            listed("./Databento/.incoming/X/opra-pillar-20251112.tcbbo.dbn.zst"),
            listed("./Alpaca/UVXY/2026-06-19.definitions.dbn.zst"),
            listed("./Databento/OPRA.PILLAR/SPY.OPT/cbbo-1m/manifest.json"),
        ]);
        let spy = &held["databento/OPRA.PILLAR/SPY.OPT"]["cbbo-1m"];
        assert_eq!(
            (
                spy["days"].clone(),
                spy["first"].clone(),
                spy["last"].clone()
            ),
            (json!(2), json!("2022-09-07"), json!("2023-05-01"))
        );
        assert_eq!(held["databento/GLBX.MDP3/-"]["bbo-1m"]["days"], 1);
        assert_eq!(unparsed.len(), 3, "{unparsed:?}");
    }

    #[test]
    fn widening_never_narrows() {
        let held = json!({ "k": { "s": { "days": 5, "first": "2022-01-01", "last": "2022-12-31", "bytes": 9 } } });
        let add = json!({ "k": { "s": { "days": 2, "first": "2022-06-01", "last": "2023-01-02", "bytes": 1 } } });
        let w = widen(&held, &add);
        assert_eq!(
            w["k"]["s"],
            json!({ "days": 5, "first": "2022-01-01", "last": "2023-01-02", "bytes": 9 })
        );
    }

    #[test]
    fn a_narrowed_schema_keeps_its_extent_once() {
        let before = json!({ "k": { "s": { "days": 3, "first": "2022-01-01", "last": "2022-01-03", "bytes": 3 } } });
        let scanned = json!({ "k": { "s": { "days": 1, "first": "2022-01-01", "last": "2022-01-01", "bytes": 1 } } });
        let (held, newly) = keep_shrunk(&before, scanned.clone(), 10.0);
        assert_eq!(
            (
                held["k"]["s"]["last"].clone(),
                held["k"]["s"]["suspect"].clone()
            ),
            (json!("2022-01-03"), json!(true))
        );
        assert_eq!(newly.len(), 1);
        let (again, newly) = keep_shrunk(&held, scanned, 20.0);
        assert_eq!(again["k"]["s"]["suspect_since"], 10.0);
        assert!(newly.is_empty(), "said once");
        let (grown, _) = keep_shrunk(
            &before,
            json!({ "k": { "s": { "days": 4, "first": "2021-12-31", "last": "2022-01-03", "bytes": 4 } } }),
            30.0,
        );
        assert_eq!(grown["k"]["s"].get("suspect"), None);
    }
}
