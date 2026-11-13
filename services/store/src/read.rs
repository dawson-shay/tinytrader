//! Reading data back (decision 0005): list the DBN files the volumes hold,
//! and send one, by its logical name `<volume>/<path within the volume>`.
//! It only reads, and only finished files: DBN files (`.dbn`, `.dbn.zst`),
//! never a `.partial` being written, nothing under a hidden folder (such as
//! `.incoming`), and nothing reached through a link out of the volume.

use std::fs;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};

use crate::catalog::Served;
use crate::ingest::Volume;

/// The suffixes of the files the store serves.
const SERVED: [&str; 2] = [".dbn.zst", ".dbn"];

/// The most files one listing returns, unless asked for fewer.
pub const MAX_LISTED: usize = 100_000;

pub(crate) fn served(name: &str) -> bool {
    SERVED.iter().any(|s| name.ends_with(s))
}

/// Every served file the last scan found (`index`: each volume's, sorted)
/// whose logical name matches `pattern` (`*` any run of characters, `/`
/// included; `?` any one character), and, given a `provider`, that is in
/// that provider's top-level folder (case ignored), at most `limit`: `{
/// files: [{ name, volume, path, bytes }], truncated }`. The volumes aren't
/// walked: a listing is as quick as the index is long.
pub fn list(
    index: &[(&str, &[Served])],
    pattern: &str,
    provider: Option<&str>,
    limit: usize,
) -> Value {
    let mut files = Vec::new();
    let mut truncated = false;
    'volumes: for &(volume, found) in index {
        for (rel, bytes) in found {
            let name = format!("{volume}/{rel}");
            let top = rel.split('/').next().unwrap_or_default();
            if !matches(pattern, &name) || provider.is_some_and(|p| !top.eq_ignore_ascii_case(p)) {
                continue;
            }
            if files.len() == limit {
                truncated = true;
                break 'volumes;
            }
            files.push(json!({ "name": name, "volume": volume, "path": rel, "bytes": bytes }));
        }
    }
    json!({ "files": files, "truncated": truncated })
}

/// Served files under `dir`, as paths relative to `root`, with sizes.
/// Hidden entries are skipped. Links are followed only when `follow` (a
/// content-addressed volume's view, whose files are links to its objects).
pub fn walk_with(root: &Path, dir: &Path, out: &mut Vec<(String, u64)>, follow: bool) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for e in entries.flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        if name.starts_with('.') {
            continue;
        }
        let meta = if follow {
            fs::metadata(e.path())
        } else {
            fs::symlink_metadata(e.path())
        };
        let Ok(meta) = meta else {
            continue;
        };
        if meta.is_dir() {
            walk_with(root, &e.path(), out, follow);
        } else if meta.is_file()
            && served(&name)
            && let Ok(rel) = e.path().strip_prefix(root)
        {
            out.push((rel.to_string_lossy().into_owned(), meta.len()));
        }
    }
}

/// Where the served file `path` of the volume named `volume` is, or why it
/// can't be served.
pub fn locate(volumes: &[Volume], volume: &str, path: &str) -> Result<PathBuf, (u16, String)> {
    let v = volumes
        .iter()
        .find(|v| v.name == volume)
        .ok_or_else(|| (404, format!("no volume {volume}")))?;
    let parts: Vec<&str> = path.split('/').collect();
    if parts
        .iter()
        .any(|p| p.is_empty() || p.starts_with('.') || p.contains('\\'))
    {
        return Err((400, format!("not a served path: {path}")));
    }
    if !served(parts.last().unwrap_or(&"")) {
        return Err((400, format!("only DBN files are served: {path}")));
    }
    let file = parts.iter().fold(v.path.clone(), |p, part| p.join(part));
    let not_found = || (404, format!("no file {volume}/{path}"));
    let real = file.canonicalize().map_err(|_| not_found())?;
    let base = v.path.canonicalize().map_err(|_| not_found())?;
    // A link may not lead out of the volume.
    if !real.starts_with(&base) || !real.is_file() {
        return Err(not_found());
    }
    Ok(real)
}

/// Shell-style matching of the whole `text`: `*` any run, `?` any one.
fn matches(pattern: &str, text: &str) -> bool {
    let (p, t): (Vec<char>, Vec<char>) = (pattern.chars().collect(), text.chars().collect());
    let (mut pi, mut ti) = (0, 0);
    let mut star: Option<(usize, usize)> = None;
    while ti < t.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some((pi, ti));
            pi += 1;
        } else if let Some((sp, st)) = star {
            pi = sp + 1;
            ti = st + 1;
            star = Some((sp, st + 1));
        } else {
            return false;
        }
    }
    p[pi..].iter().all(|c| *c == '*')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn volume(name: &str) -> (Volume, PathBuf) {
        let d = std::env::temp_dir().join(format!("tinytrader-read-{}-{name}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        fs::create_dir_all(&d).unwrap();
        (
            Volume {
                name: name.to_owned(),
                path: d.clone(),
                writable: true,
                require_mount: false,
            },
            d,
        )
    }

    /// The listing `/files` gives, from a scan of `v`.
    fn list_of(v: &Volume, pattern: &str, provider: Option<&str>, limit: usize) -> Value {
        let t = crate::catalog::Thresholds {
            warning: 100.0,
            critical: 100.0,
            floor: 100.0,
        };
        let (_, _, served) = crate::catalog::scan_volume(&v.name, &v.path, t, &[]);
        list(
            &[(v.name.as_str(), served.as_slice())],
            pattern,
            provider,
            limit,
        )
    }

    #[test]
    fn lists_finished_dbn_files_only() {
        let (v, d) = volume("list");
        let day = d.join("Databento/OPRA.PILLAR/SPY.OPT/cmbp-1");
        fs::create_dir_all(&day).unwrap();
        fs::write(day.join("2026-09-01.dbn.zst"), b"abc").unwrap();
        fs::write(day.join("2026-09-02.dbn.partial"), b"x").unwrap();
        fs::write(day.join("manifest.json"), b"{}").unwrap();
        let inc = d.join("Databento/.incoming/B1");
        fs::create_dir_all(&inc).unwrap();
        fs::write(inc.join("opra-20260903.cmbp-1.dbn.zst"), b"y").unwrap();
        fs::write(d.join("loose.dbn"), b"zz").unwrap();

        let all = list_of(&v, "*", None, MAX_LISTED);
        let names: Vec<&str> = all["files"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| f["name"].as_str().unwrap())
            .collect();
        assert_eq!(
            names,
            [
                "list/Databento/OPRA.PILLAR/SPY.OPT/cmbp-1/2026-09-01.dbn.zst",
                "list/loose.dbn"
            ]
        );
        assert_eq!(all["files"][0]["bytes"], 3);
        assert_eq!(all["files"][0]["volume"], "list");
        assert_eq!(all["truncated"], false);

        let some = list_of(&v, "*cmbp-1*", None, MAX_LISTED);
        assert_eq!(some["files"].as_array().unwrap().len(), 1);
        let one = list_of(&v, "*", None, 1);
        assert_eq!(one["files"].as_array().unwrap().len(), 1);
        assert_eq!(one["truncated"], true);
        // Scoped to a provider, by its folder, case ignored.
        let alpaca = d.join("alpaca/OPRA.PILLAR/SPY.OPT/cmbp-1");
        fs::create_dir_all(&alpaca).unwrap();
        fs::write(alpaca.join("2026-09-01.dbn.zst"), b"a").unwrap();
        let pattern = "*/OPRA.PILLAR/SPY.OPT/cmbp-1/*";
        let count = |p: Option<&str>| {
            list_of(&v, pattern, p, MAX_LISTED)["files"]
                .as_array()
                .unwrap()
                .len()
        };
        assert_eq!(
            (count(None), count(Some("databento")), count(Some("Alpaca"))),
            (2, 1, 1)
        );
        fs::remove_dir_all(&d).unwrap();
    }

    #[test]
    fn serves_only_dbn_files_inside_the_volume() {
        let (v, d) = volume("locate");
        let (_, outside) = volume("outside");
        fs::create_dir_all(d.join("Alpaca/ALPACA.SIP/trades")).unwrap();
        fs::write(d.join("Alpaca/ALPACA.SIP/trades/2026-09-01.dbn.zst"), b"a").unwrap();
        fs::write(d.join("Alpaca/notes.json"), b"{}").unwrap();
        fs::create_dir_all(d.join(".incoming")).unwrap();
        fs::write(d.join(".incoming/x.dbn"), b"a").unwrap();
        fs::write(outside.join("secret.dbn"), b"s").unwrap();
        std::os::unix::fs::symlink(outside.join("secret.dbn"), d.join("link.dbn")).unwrap();
        let vols = [v];

        let ok = locate(
            &vols,
            "locate",
            "Alpaca/ALPACA.SIP/trades/2026-09-01.dbn.zst",
        )
        .unwrap();
        assert_eq!(fs::read(ok).unwrap(), b"a");
        let code = |vol: &str, p: &str| locate(&vols, vol, p).unwrap_err().0;
        assert_eq!(code("nope", "a.dbn"), 404);
        assert_eq!(code("locate", "Alpaca/notes.json"), 400);
        assert_eq!(code("locate", ".incoming/x.dbn"), 400);
        assert_eq!(code("locate", "Alpaca/../link.dbn"), 400);
        assert_eq!(code("locate", "Alpaca//x.dbn"), 400);
        assert_eq!(code("locate", "missing.dbn"), 404);
        assert_eq!(code("locate", "link.dbn"), 404, "a link out of the volume");
        fs::remove_dir_all(&d).unwrap();
        fs::remove_dir_all(&outside).unwrap();
    }

    #[test]
    fn patterns() {
        assert!(matches("*", "a/b/c.dbn"));
        assert!(matches(
            "*cmbp-1*",
            "nd/Databento/X.Y/cmbp-1/2026-01-01.dbn.zst"
        ));
        assert!(matches("data/*.dbn", "data/a/b.dbn"));
        assert!(matches("a?c", "abc"));
        assert!(!matches("a?c", "ac"));
        assert!(!matches("*.dbn", "x.dbn.zst"));
        assert!(matches("*2026-09-0?.dbn.zst", "v/x/2026-09-01.dbn.zst"));
    }
}
