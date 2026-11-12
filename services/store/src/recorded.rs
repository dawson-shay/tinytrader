//! The SHA-256 the store recorded for a file it serves (ingress review (E)):
//! sent with `GET /files` as `X-Content-SHA256` and `Digest`, so a reader can
//! check what it got against what was filed.
//!
//! It's taken from what filing already records, never computed on a read:
//! - the day's manifest entry (partial days, supersedes);
//! - the filing ledgers' `moved` and `swapped` steps
//!   (`<provider>/migrations/*.events.jsonl`): every day filed from a batch;
//! - the batch's receipt (`<provider>/provenance/<batch>/manifest.json`, the
//!   provider's own hashes), for a day the ledger says came from a batch file.
//!
//! Where several say, they must agree. A disagreement sends no hash and is
//! reported; a file nothing recorded sends none.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde_json::Value;

/// What filing recorded about a file, from the ledgers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ledgered {
    pub sha256: String,
    /// The provider's hash for the batch file it was moved from, when its
    /// receipt is kept.
    pub receipt: Option<String>,
}

/// What a read can say about the file it sends.
#[derive(Debug, PartialEq, Eq)]
pub enum Recorded {
    /// Every source that recorded it agrees.
    Known(String),
    /// Nothing recorded it.
    Unknown,
    /// Sources disagree: why.
    Conflict(String),
}

type Map = HashMap<PathBuf, Ledgered>;
/// Each volume's map, with the filing generation it was built at.
type Cache = Mutex<HashMap<PathBuf, (u64, Arc<Map>)>>;

/// Each volume's map, with the filing generation it was built at.
fn cache() -> &'static Cache {
    static C: std::sync::OnceLock<Cache> = std::sync::OnceLock::new();
    C.get_or_init(Mutex::default)
}

/// The ledgers' record of `volume`'s files (paths within the volume), built
/// again when a filing happened since (`generation` moved on).
#[must_use]
pub fn ledgered(volume: &Path, generation: u64) -> Arc<Map> {
    if let Some((g, m)) = cache()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(volume)
        && *g == generation
    {
        return m.clone();
    }
    let m = Arc::new(build(volume));
    cache()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(volume.to_path_buf(), (generation, m.clone()));
    m
}

fn read_dir_sorted(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<(std::time::SystemTime, PathBuf)> = fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .map(|e| e.path())
        .map(|p| {
            let t = fs::metadata(&p)
                .and_then(|m| m.modified())
                .unwrap_or(std::time::UNIX_EPOCH);
            (t, p)
        })
        .collect();
    v.sort();
    v.into_iter().map(|(_, p)| p).collect()
}

/// A batch receipt's hashes: file name → sha256.
fn receipt(provider: &Path, batch: &str) -> HashMap<String, String> {
    let path = provider
        .join("provenance")
        .join(batch)
        .join("manifest.json");
    let Ok(text) = fs::read_to_string(path) else {
        return HashMap::new();
    };
    let Ok(v) = serde_json::from_str::<Value>(&text) else {
        return HashMap::new();
    };
    v["files"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|f| {
            let name = f["filename"].as_str()?;
            let h = f["hash"].as_str()?.trim_start_matches("sha256:");
            Some((name.to_owned(), h.to_ascii_lowercase()))
        })
        .collect()
}

/// `path` within `volume`, as configured or as resolved (a served file's
/// path is resolved; a ledger's is as the volume was configured).
fn within(volume: &Path, real: Option<&Path>, path: &Path) -> Option<PathBuf> {
    path.strip_prefix(volume)
        .ok()
        .or_else(|| path.strip_prefix(real?).ok())
        .map(Path::to_path_buf)
}

fn build(volume: &Path) -> Map {
    let mut out = Map::new();
    let real = volume.canonicalize().ok();
    let mut receipts: HashMap<(PathBuf, String), HashMap<String, String>> = HashMap::new();
    let providers = fs::read_dir(volume)
        .into_iter()
        .flatten()
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| crate::catalog::provider_name(n).is_some())
        });
    for provider in providers {
        // Oldest first, so a later step (a supersede, a merge) wins.
        for ledger in read_dir_sorted(&provider.join("migrations"))
            .into_iter()
            .filter(|p| p.to_string_lossy().ends_with(".jsonl"))
        {
            let Ok(text) = fs::read_to_string(&ledger) else {
                continue;
            };
            for line in text.lines() {
                // A torn last line (a crash mid-write) says nothing.
                let Ok(step) = serde_json::from_str::<Value>(line) else {
                    continue;
                };
                let (to, from) = match step["step"].as_str() {
                    Some("moved") => (step["to"].as_str(), step["from"].as_str()),
                    Some("swapped") => (step["path"].as_str(), None),
                    _ => continue,
                };
                let (Some(to), Some(sha)) = (to, step["sha256"].as_str()) else {
                    continue;
                };
                let Some(rel) = within(volume, real.as_deref(), Path::new(to)) else {
                    continue;
                };
                let receipt = from.map(Path::new).and_then(|f| {
                    let batch = f.parent()?.file_name()?.to_str()?.to_owned();
                    let name = f.file_name()?.to_str()?;
                    receipts
                        .entry((provider.clone(), batch.clone()))
                        .or_insert_with(|| receipt(&provider, &batch))
                        .get(name)
                        .cloned()
                });
                out.insert(
                    rel,
                    Ledgered {
                        sha256: sha.to_ascii_lowercase(),
                        receipt,
                    },
                );
            }
        }
    }
    out
}

/// The hash recorded for `file` (on `volume`, `size` bytes now), from every
/// source that has one.
#[must_use]
pub fn of(volume: &Path, file: &Path, size: u64, generation: u64) -> Recorded {
    let mut said: Vec<(&str, String)> = Vec::new();
    let name = file
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_default();
    if let (Some(day), Some(dir)) = (
        name.strip_suffix(".dbn.zst")
            .filter(|d| crate::ingest::day_ns(d).is_some()),
        file.parent(),
    ) {
        match crate::manifest::read_day(dir, day) {
            Ok(Some(e)) if e.get("pending").is_none() => {
                let for_this = e["file"].as_str().is_none_or(|f| f == name);
                if for_this {
                    if let Some(s) = e["size"].as_u64().filter(|s| *s != size) {
                        return Recorded::Conflict(format!(
                            "its manifest records {s} bytes; the file is {size}"
                        ));
                    }
                    if let Some(h) = e["sha256"].as_str() {
                        said.push(("manifest", h.to_ascii_lowercase()));
                    }
                }
            }
            Ok(_) => {}
            Err(e) => return Recorded::Conflict(format!("its manifest can't be read: {e}")),
        }
    }
    if let Some(rel) = within(volume, volume.canonicalize().ok().as_deref(), file)
        && let Some(l) = ledgered(volume, generation).get(&rel)
    {
        said.push(("ledger", l.sha256.clone()));
        if let Some(r) = &l.receipt {
            said.push(("receipt", r.clone()));
        }
    }
    match said.first() {
        None => Recorded::Unknown,
        Some((_, h)) if said.iter().all(|(_, x)| x == h) => Recorded::Known(h.clone()),
        Some(_) => Recorded::Conflict(
            said.iter()
                .map(|(s, h)| format!("{s} {h}"))
                .collect::<Vec<_>>()
                .join(", "),
        ),
    }
}

/// Base64 (standard, padded) of a hex digest, for `Digest: sha-256=…`.
#[must_use]
pub fn hex_to_base64(hex: &str) -> Option<String> {
    let bytes: Vec<u8> = (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(hex.get(i..i + 2)?, 16).ok())
        .collect::<Option<_>>()?;
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for c in bytes.chunks(3) {
        let n = (u32::from(c[0]) << 16)
            | (u32::from(*c.get(1).unwrap_or(&0)) << 8)
            | u32::from(*c.get(2).unwrap_or(&0));
        for i in 0..4 {
            if i <= c.len() {
                out.push(A[(n >> (18 - 6 * i) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_of_a_digest() {
        // sha256("") in hex and base64.
        assert_eq!(
            hex_to_base64("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
                .as_deref(),
            Some("47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=")
        );
        assert_eq!(hex_to_base64("ff").as_deref(), Some("/w=="));
        assert_eq!(hex_to_base64("zz"), None);
    }
}
