//! Holdings in the store service: `GET /coverage` plans the days, `GET
//! /files/{name}` streams each file. A `503` (a volume away, a scan under way)
//! is asked again on a bounded schedule, then fails saying it gave up.
use crate::{Day, FileRef, Holdings, Query};
use serde_json::Value;
use std::collections::HashMap;
use std::io::Read;
use std::time::Duration;

/// How often a `503` or a dropped connection is asked again.
#[derive(Clone, Debug)]
pub struct Retry {
    /// The waits between attempts; the attempts are one more than these.
    pub waits: Vec<Duration>,
}

impl Default for Retry {
    fn default() -> Self {
        let s = Duration::from_secs;
        Self {
            waits: vec![s(2), s(4), s(8), s(16), s(30), s(30), s(30)],
        }
    }
}

/// The store service at `base` (`http://host:9120`).
pub struct StoreApi {
    base: String,
    agent: ureq::Agent,
    retry: Retry,
}

/// Escapes a query value or path segment.
fn escape(s: &str, keep_slash: bool) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                char::from(b).to_string()
            }
            b'/' if keep_slash => "/".into(),
            _ => format!("%{b:02X}"),
        })
        .collect()
}

impl StoreApi {
    /// The store at `base`, with the default retry.
    #[must_use]
    pub fn new(base: &str) -> Self {
        Self::with_retry(base, Retry::default())
    }

    /// The store at `base`, asking again on `retry`'s schedule.
    #[must_use]
    pub fn with_retry(base: &str, retry: Retry) -> Self {
        Self {
            base: base.trim_end_matches('/').to_owned(),
            agent: ureq::AgentBuilder::new()
                .timeout_connect(Duration::from_secs(10))
                .timeout_read(Duration::from_secs(120))
                .build(),
            retry,
        }
    }

    fn get(&self, path: &str) -> Result<ureq::Response, String> {
        let url = format!("{}{path}", self.base);
        let mut waits = self.retry.waits.iter();
        loop {
            let why = match self.agent.get(&url).call() {
                Ok(r) => return Ok(r),
                Err(ureq::Error::Status(503, r)) => {
                    format!("503 {}", r.into_string().unwrap_or_default())
                }
                Err(ureq::Error::Status(code, r)) => {
                    return Err(format!(
                        "{url}: {code} {}",
                        r.into_string().unwrap_or_default()
                    ));
                }
                Err(e) => e.to_string(),
            };
            match waits.next() {
                Some(w) => std::thread::sleep(*w),
                None => {
                    let n = self.retry.waits.len() + 1;
                    return Err(format!("{url}: gave up after {n} attempts: {}", why.trim()));
                }
            }
        }
    }
}

impl Holdings for StoreApi {
    fn describe(&self) -> String {
        format!("store {}", self.base)
    }

    fn plan(&self, q: &Query, days: &[String]) -> Result<Vec<Day>, String> {
        let (Some(start), Some(end)) = (days.first(), days.last()) else {
            return Ok(Vec::new());
        };
        let path = format!(
            "/coverage?provider={}&dataset={}&symbol={}&schema={}&start={start}&end={end}",
            escape(&q.provider, false),
            escape(&q.dataset, false),
            escape(&q.symbol, false),
            escape(&q.schema, false),
        );
        let v: Value = serde_json::from_reader(self.get(&path)?.into_reader())
            .map_err(|e| format!("{path}: {e}"))?;
        let rows = v["days"]
            .as_array()
            .ok_or_else(|| format!("{path}: no days"))?;
        let missing: Vec<&str> = v["missing_volumes"]
            .as_array()
            .map(|a| a.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default();
        let mut held: HashMap<&str, Day> = HashMap::new();
        for r in rows {
            let Some(day) = r["day"].as_str() else {
                continue;
            };
            if !days.iter().any(|d| d == day) {
                continue;
            }
            if r["conflict"].as_bool() == Some(true) {
                return Err(format!(
                    "{day}: the store's copies differ (conflict); resolve them before reading it"
                ));
            }
            let sha = r["sha256"].as_str().map(str::to_owned);
            let all: Vec<&str> = r["files"]
                .as_array()
                .map(|a| a.iter().filter_map(Value::as_str).collect())
                .unwrap_or_default();
            // One copy: the files on the day's own volume (or the first listed),
            // never two copies merged into duplicates.
            let vol = |n: &str| n.split('/').next().unwrap_or_default().to_owned();
            let primary = r["volume"]
                .as_str()
                .map(str::to_owned)
                .filter(|v| all.iter().any(|n| vol(n) == *v))
                .or_else(|| all.first().map(|n| vol(n)));
            let names: Vec<&str> = all
                .iter()
                .copied()
                .filter(|n| Some(vol(n)) == primary)
                .collect();
            if names.is_empty() {
                continue;
            }
            // The store states one hash per day, only where one file holds it.
            let files = names
                .iter()
                .map(|n| FileRef {
                    route: Vec::new(),
                    name: (*n).to_owned(),
                    sha256: sha.clone().filter(|_| names.len() == 1),
                })
                .collect();
            let partial = r["kind"].as_str() == Some("partial");
            held.insert(day, Day::Held { files, partial });
        }
        Ok(days
            .iter()
            .map(|d| {
                held.remove(d.as_str()).unwrap_or_else(|| Day::Missing {
                    reason: if missing.is_empty() {
                        "not held".into()
                    } else {
                        format!("not held while volumes are missing: {}", missing.join(","))
                    },
                })
            })
            .collect())
    }

    fn open(&self, file: &FileRef) -> Result<Box<dyn Read + Send>, String> {
        if file
            .name
            .split('/')
            .any(|seg| seg == ".." || seg == "." || seg.is_empty())
        {
            return Err(format!("{}: not a store file name", file.name));
        }
        let r = self.get(&format!("/files/{}", escape(&file.name, true)))?;
        Ok(Box::new(r.into_reader()))
    }
}
