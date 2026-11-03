//! Holdings in a local folder, read-only:
//!
//! - a `databento-jobs` cache job (`POST /cache/{job}`): the batch's files,
//!   `metadata.json` and `manifest.json` as Databento sent them, read only
//!   once `COMPLETE` says the copy is whole. The manifest's list of files is
//!   the job: each DBN file it names must be in the folder with a SHA-256,
//!   which is checked. A file holds the days its name says (one day, or a
//!   range, both ends included); a file split by symbol holds only that
//!   symbol's. A job bought as a list of contracts, or over less than a whole
//!   day, holds partial days.
//! - a folder in the store's layout, such as a copy of a volume:
//!   `<Provider>/<DATASET>/<symbol>/<schema>/<day>.dbn.zst`, or a folder per
//!   day holding its DBN files (the older layout). No hashes are stated, so
//!   they are recorded, not checked.
use crate::{Day, FileRef, Holdings, Query};
use serde_json::Value;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

/// One file of a cache job, from its manifest.
struct JobFile {
    name: String,
    sha256: String,
    /// The days it holds, `YYYYMMDD`, both ends included.
    days: (String, String),
    /// The symbol a split-by-symbol file is for.
    symbol: Option<String>,
}

enum Kind {
    /// A cache job, by its manifest.
    Job {
        id: String,
        dataset: String,
        schema: String,
        symbols: Vec<String>,
        /// Bought as a list of contracts: every day is partial.
        contracts: bool,
        /// The query's window, ns.
        window: (u64, u64),
        files: Vec<JobFile>,
    },
    /// A volume in the store's layout.
    Layout,
}

/// A local folder of DBN days.
pub struct Folder {
    root: PathBuf,
    kind: Kind,
}

fn is_dbn(name: &str) -> bool {
    name.ends_with(".dbn.zst") || name.ends_with(".dbn")
}

fn read_json(path: &Path) -> Result<Value, String> {
    let text = fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    serde_json::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))
}

fn is_ymd(s: &str) -> bool {
    s.len() == 8 && s.bytes().all(|b| b.is_ascii_digit())
}

/// A Databento batch file name's days and split symbol:
/// `opra-pillar-20261027.cbbo-1m.dbn.zst`, `…-20261001-20261031.cbbo-1m.dbn.zst`,
/// `…-20261027.cbbo-1m.SPY.dbn.zst`.
fn batch_name(name: &str, schema: &str) -> Option<((String, String), Option<String>)> {
    let stem = name
        .strip_suffix(".zst")
        .unwrap_or(name)
        .strip_suffix(".dbn")?;
    let (head, tail) = stem.split_once(&format!(".{schema}"))?;
    let symbol = tail.strip_prefix('.').map(str::to_owned);
    let mut parts = head.rsplit('-');
    let last = parts.next()?;
    if !is_ymd(last) {
        return None;
    }
    let days = match parts.next() {
        Some(first) if is_ymd(first) => (first.to_owned(), last.to_owned()),
        _ => (last.to_owned(), last.to_owned()),
    };
    Some((days, symbol))
}

/// A query time, in ns: a number, or text: digits, `YYYY-MM-DD`, or RFC 3339
/// (`2026-10-27T13:30:00Z`, fractions and `±HH:MM` offsets allowed).
fn query_ns(v: &Value) -> Option<u64> {
    if let Some(n) = v.as_u64() {
        return Some(n);
    }
    let t = v.as_str()?.trim();
    if !t.is_empty() && t.bytes().all(|b| b.is_ascii_digit()) {
        return t.parse().ok();
    }
    let day = crate::day_ns(t.get(..10)?)?;
    let rest = &t[10..];
    if rest.is_empty() {
        return Some(day);
    }
    let rest = rest.strip_prefix(['T', 't', ' '])?;
    let num = |s: &str| s.parse::<u64>().ok();
    let (hms, zone) = match rest.find(['Z', 'z', '+', '-']) {
        Some(i) => rest.split_at(i),
        None => return None,
    };
    let (hms, frac) = hms.split_once('.').unwrap_or((hms, ""));
    let mut it = hms.split(':');
    let h = num(it.next()?)?;
    let m = num(it.next()?)?;
    let sec = it.next().map_or(Some(0), num)?;
    if h > 23 || m > 59 || sec > 60 || it.next().is_some() {
        return None;
    }
    let frac_ns = if frac.is_empty() {
        0
    } else {
        let digits: String = frac.chars().take(9).collect();
        num(&digits)? * 10_u64.pow(u32::try_from(9 - digits.len()).ok()?)
    };
    let offset: i64 = match zone {
        "Z" | "z" => 0,
        z => {
            let sign = if z.starts_with('-') { -1 } else { 1 };
            let (oh, om) = z[1..].split_once(':')?;
            sign * i64::try_from(num(oh)? * 3600 + num(om)? * 60).ok()?
        }
    };
    let local = i64::try_from(day + (h * 3600 + m * 60 + sec) * 1_000_000_000 + frac_ns).ok()?;
    u64::try_from(local - offset * 1_000_000_000).ok()
}

impl Folder {
    /// A finished `databento-jobs` cache job, read by its manifest.
    ///
    /// # Errors
    /// It has no `COMPLETE` marker yet; its metadata or manifest can't be read;
    /// a DBN file the manifest lists is absent, has no hash, or has a name
    /// whose days can't be read.
    pub fn job(root: &Path) -> Result<Self, String> {
        let id = root
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        if !root.join("COMPLETE").is_file() {
            return Err(format!(
                "job {id} isn't complete yet: no COMPLETE marker in {}",
                root.display()
            ));
        }
        let meta = read_json(&root.join("metadata.json"))?;
        let q = meta.get("query").unwrap_or(&meta);
        let text = |k: &str| q[k].as_str().unwrap_or_default().to_owned();
        let symbols = match &q["symbols"] {
            Value::Array(a) => a
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect(),
            Value::String(s) => s.split(',').map(str::to_owned).collect(),
            _ => Vec::new(),
        };
        let schema = text("schema");
        let contracts = q["stype_in"].as_str() == Some("raw_symbol");
        let bound = |k: &str, open: u64| match &q[k] {
            Value::Null => Ok(open),
            v => query_ns(v).ok_or_else(|| format!("job {id}: can't read the query's {k}: {v}")),
        };
        let window = (bound("start", 0)?, bound("end", u64::MAX)?);
        let manifest = read_json(&root.join("manifest.json"))?;
        let mut files = Vec::new();
        for f in manifest["files"].as_array().into_iter().flatten() {
            let Some(name) = f["filename"].as_str() else {
                continue;
            };
            if !is_dbn(name) {
                continue;
            }
            if !root.join(name).is_file() {
                return Err(format!(
                    "job {id}: {name} is in the manifest but not in the folder"
                ));
            }
            let sha256 = f["hash"]
                .as_str()
                .and_then(|h| h.strip_prefix("sha256:"))
                .ok_or_else(|| format!("job {id}: {name} has no SHA-256 in the manifest"))?
                .to_owned();
            let (days, symbol) = batch_name(name, &schema)
                .ok_or_else(|| format!("job {id}: can't read the days of {name}"))?;
            files.push(JobFile {
                name: name.to_owned(),
                sha256,
                days,
                symbol,
            });
        }
        files.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(Self {
            root: root.to_owned(),
            kind: Kind::Job {
                id,
                dataset: text("dataset"),
                schema,
                symbols,
                contracts,
                window,
                files,
            },
        })
    }

    /// A folder in the store's layout (a volume's root).
    #[must_use]
    pub fn layout(root: &Path) -> Self {
        Self {
            root: root.to_owned(),
            kind: Kind::Layout,
        }
    }

    /// The schema folder for `q`, matching the provider folder's name in any case.
    fn schema_dir(&self, q: &Query) -> Option<PathBuf> {
        let provider = fs::read_dir(&self.root).ok()?.flatten().find(|e| {
            e.file_name()
                .to_string_lossy()
                .eq_ignore_ascii_case(&q.provider)
        })?;
        let dir = provider
            .path()
            .join(&q.dataset)
            .join(&q.symbol)
            .join(&q.schema);
        dir.is_dir().then_some(dir)
    }
}

impl Holdings for Folder {
    fn describe(&self) -> String {
        match &self.kind {
            Kind::Job { id, .. } => format!("job {id} at {}", self.root.display()),
            Kind::Layout => format!("folder {}", self.root.display()),
        }
    }

    fn plan(&self, q: &Query, days: &[String]) -> Result<Vec<Day>, String> {
        match &self.kind {
            Kind::Job {
                id,
                dataset,
                schema,
                symbols,
                contracts,
                window,
                files,
            } => {
                if !dataset.eq_ignore_ascii_case(&q.dataset)
                    || schema != &q.schema
                    || !symbols.iter().any(|s| s == &q.symbol)
                {
                    return Err(format!(
                        "job {id} holds {dataset} {schema} {}, not {} {} {}",
                        symbols.join(","),
                        q.dataset,
                        q.schema,
                        q.symbol
                    ));
                }
                Ok(days
                    .iter()
                    .map(|d| {
                        let ymd = d.replace('-', "");
                        let held: Vec<FileRef> = files
                            .iter()
                            .filter(|f| f.days.0 <= ymd && ymd <= f.days.1)
                            .filter(|f| f.symbol.as_ref().is_none_or(|s| s == &q.symbol))
                            .map(|f| FileRef {
                                route: Vec::new(),
                                name: f.name.clone(),
                                sha256: Some(f.sha256.clone()),
                            })
                            .collect();
                        let day0 = crate::day_ns(d).unwrap_or(0);
                        let whole = window.0 <= day0 && day0 + 86_400_000_000_000 <= window.1;
                        if held.is_empty() {
                            Day::Missing {
                                reason: format!("not in job {id}"),
                            }
                        } else {
                            Day::Held {
                                files: held,
                                partial: *contracts || !whole,
                            }
                        }
                    })
                    .collect())
            }
            Kind::Layout => {
                let Some(dir) = self.schema_dir(q) else {
                    let reason = format!(
                        "no {} {} {} {} folder",
                        q.provider, q.dataset, q.symbol, q.schema
                    );
                    return Ok(days
                        .iter()
                        .map(|_| Day::Missing {
                            reason: reason.clone(),
                        })
                        .collect());
                };
                let rel = |p: &Path| {
                    p.strip_prefix(&self.root)
                        .unwrap_or(p)
                        .to_string_lossy()
                        .into_owned()
                };
                Ok(days
                    .iter()
                    .map(|d| {
                        let mut files: Vec<PathBuf> = ["dbn.zst", "dbn"]
                            .iter()
                            .map(|ext| dir.join(format!("{d}.{ext}")))
                            .filter(|p| p.is_file())
                            .take(1)
                            .collect();
                        if files.is_empty()
                            && let Ok(rd) = fs::read_dir(dir.join(d))
                        {
                            files = rd
                                .flatten()
                                .map(|e| e.path())
                                .filter(|p| p.is_file() && is_dbn(&p.to_string_lossy()))
                                .collect();
                            files.sort();
                        }
                        // An empty file is padding, not a day.
                        files.retain(|p| fs::metadata(p).is_ok_and(|m| m.len() > 0));
                        if files.is_empty() {
                            Day::Missing {
                                reason: "not in the folder".into(),
                            }
                        } else {
                            Day::Held {
                                files: files
                                    .iter()
                                    .map(|p| FileRef {
                                        route: Vec::new(),
                                        name: rel(p),
                                        sha256: None,
                                    })
                                    .collect(),
                                partial: false,
                            }
                        }
                    })
                    .collect())
            }
        }
    }

    fn open(&self, file: &FileRef) -> Result<Box<dyn Read + Send>, String> {
        let root = self
            .root
            .canonicalize()
            .map_err(|e| format!("{}: {e}", self.root.display()))?;
        let path = root
            .join(&file.name)
            .canonicalize()
            .map_err(|e| format!("{}: {e}", file.name))?;
        // Links included: nothing outside the folder is read.
        if !path.starts_with(&root) {
            return Err(format!("{}: outside the folder", file.name));
        }
        let f = fs::File::open(&path).map_err(|e| format!("{}: {e}", path.display()))?;
        Ok(Box::new(f))
    }

    fn local(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::query_ns;
    use serde_json::json;

    #[test]
    fn query_times_parse_in_every_form() {
        let d27 = 1_793_059_200_000_000_000_u64;
        let h = 3_600_000_000_000_u64;
        assert_eq!(query_ns(&json!(d27)), Some(d27));
        assert_eq!(query_ns(&json!(d27.to_string())), Some(d27));
        assert_eq!(query_ns(&json!("2026-10-27")), Some(d27));
        assert_eq!(
            query_ns(&json!("2026-10-27T13:30:00Z")),
            Some(d27 + 13 * h + h / 2)
        );
        assert_eq!(
            query_ns(&json!("2026-10-27T13:30:00.5Z")),
            Some(d27 + 13 * h + h / 2 + 500_000_000)
        );
        assert_eq!(
            query_ns(&json!("2026-10-27T09:30:00-04:00")),
            Some(d27 + 13 * h + h / 2)
        );
        assert_eq!(query_ns(&json!("2026-10-27T13:30")), None);
        assert_eq!(query_ns(&json!("not a time")), None);
    }
}
