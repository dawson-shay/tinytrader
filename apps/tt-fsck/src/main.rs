//! `tt-fsck`: check volumes in the content-addressed format against the
//! storage contract (`docs/storage-contract.md`, decision 0039), from the
//! disk alone, with nothing else running against them. It writes nothing.
//!
//! ```text
//! tt-fsck [--quick] [--json] [--min-copies N] [--against NAME=PATH]... PATH...
//! ```
//!
//! With `--against NAME=PATH` (a source a volume was migrated from, by the
//! name it was given to `store migrate-cas`), the source is walked again,
//! read-only, and every file's path, size and SHA-256 must be bound on some
//! volume given, or kept there as pending (a conflict for an operator):
//! the check that gates deleting or reusing a source drive. A source with
//! no files (not mounted) is refused. Each volume's migration receipts must
//! end clean, too.
//!
//! For each volume (S1, S2): every name's object is there at its size and,
//! unless `--quick`, its hash; every object is named, pending or an earlier
//! version; nothing is misnamed; the log has no conflicts (a torn last line
//! is reported, and tolerated). Across the volumes given (S7): each name has
//! at least `--min-copies` (default 2) volumes holding its current version,
//! and no two volumes disagree about a version.
//!
//! One finding per line, with its rule, on stdout (or one JSON document with
//! `--json`). Exit 0 when sound, 1 with findings, 2 when a volume can't be
//! read or is being written.

use std::path::PathBuf;

use serde_json::{Value, json};
use tinytrader_cas::{Volume, merge};

struct Args {
    against: Vec<(String, PathBuf)>,
    quick: bool,
    json: bool,
    min_copies: usize,
    paths: Vec<PathBuf>,
}

fn args(argv: &[String]) -> Result<Args, String> {
    let mut a = Args {
        against: Vec::new(),
        quick: false,
        json: false,
        min_copies: 2,
        paths: Vec::new(),
    };
    let mut it = argv.iter();
    while let Some(x) = it.next() {
        match x.as_str() {
            "--quick" => a.quick = true,
            "--json" => a.json = true,
            "--min-copies" => {
                a.min_copies = it
                    .next()
                    .and_then(|n| n.parse().ok())
                    .ok_or("--min-copies takes a number")?;
            }
            "--against" => {
                let v = it.next().ok_or("--against takes NAME=PATH")?;
                let (n, p) = v
                    .split_once('=')
                    .filter(|(n, p)| !n.is_empty() && !p.is_empty())
                    .ok_or_else(|| format!("{v}: give a source as NAME=PATH"))?;
                a.against.push((n.to_owned(), PathBuf::from(p)));
            }
            "-h" | "--help" => return Err(String::new()),
            s if s.starts_with('-') => return Err(format!("unknown option {s}")),
            p => a.paths.push(PathBuf::from(p)),
        }
    }
    if a.paths.is_empty() {
        return Err("give at least one volume".into());
    }
    Ok(a)
}

/// The check: the report, and the exit status.
fn check(a: &Args) -> (Value, i32) {
    let mut findings: Vec<Value> = Vec::new();
    let mut unreadable = Vec::new();
    let mut volumes = Vec::new();
    let mut indexes = Vec::new();
    let mut f = |rule: &str, volume: &str, what: String| {
        findings.push(json!({ "rule": rule, "volume": volume, "finding": what }));
    };
    for p in &a.paths {
        let v = match Volume::open(p) {
            Ok(v) => v,
            Err(e) => {
                unreadable.push(e);
                continue;
            }
        };
        // Held for the whole check: no writer meanwhile.
        let held = match v.lock_reader() {
            Ok(h) => h,
            Err(e) => {
                unreadable.push(e);
                continue;
            }
        };
        let (report, index) = match (v.verify(!a.quick), v.index()) {
            (Ok(r), Ok(i)) => (r, i),
            (Err(e), _) | (_, Err(e)) => {
                unreadable.push(format!("{}: {e}", p.display()));
                continue;
            }
        };
        for m in &report.missing {
            f("S1", &v.id, format!("missing: {m}"));
        }
        for d in &report.damaged {
            f("S1", &v.id, format!("damaged: {d}"));
        }
        for o in &report.orphans {
            f("S1", &v.id, format!("orphan (kept): {o}"));
        }
        for m in &report.misnamed {
            f("S1", &v.id, format!("misnamed: {m}"));
        }
        for c in &report.conflicts {
            f("S2", &v.id, format!("conflict: {c}"));
        }
        for l in &report.log_damage {
            f(
                "S2",
                &v.id,
                format!("log line cut short and never noted (damage): {l}"),
            );
        }
        for s in &report.snapshot {
            f("S2", &v.id, s.clone());
        }
        // Each migration into it must have ended clean (review C2).
        match v.receipts() {
            Ok(rs) => {
                for (src, r) in &rs {
                    let last = r["runs"].as_array().and_then(|a| a.last());
                    if last.is_none_or(|l| l["ok"] != true) {
                        f(
                            "S6",
                            &v.id,
                            format!(
                                "migration from {src} didn't end clean ({} conflicts, {} damaged, errors {}): see its report",
                                last.map_or(json!("?"), |l| l["conflicts"].clone()),
                                last.map_or(json!("?"), |l| l["damaged"].clone()),
                                last.map_or(json!("?"), |l| l["errors"].clone())
                            ),
                        );
                    }
                }
            }
            Err(e) => f(
                "S6",
                &v.id,
                format!("its migration receipts can't be read: {e}"),
            ),
        }
        volumes.push(json!({
            "volume": v.id, "path": p, "names": report.names, "objects": report.objects,
            "unreferenced": report.unreferenced, "pending": index.pending.len(),
            "torn_last_line": report.torn_last_line, "torn_lines": report.torn_lines, "hashed": !a.quick,
        }));
        indexes.push((v.id.clone(), index));
        drop(held);
    }
    // Each source, walked again and hashed: every file must be held.
    let mut against = Vec::new();
    for (src, root) in &a.against {
        let w = tinytrader_cas::tree::walk(root);
        if !root.is_dir() || w.files.is_empty() {
            unreadable.push(format!(
                "source {src}: {} holds no files; is the drive mounted?",
                root.display()
            ));
            continue;
        }
        for e in &w.errors {
            f("S6", src, format!("source unreadable: {e}"));
        }
        let (mut held, mut pending) = (0u64, 0u64);
        for file in &w.files {
            let sha = match tinytrader_cas::hash_file(&file.path) {
                Ok(h) => h,
                Err(e) => {
                    f("S6", src, format!("source unreadable: {e}"));
                    continue;
                }
            };
            let bound = indexes.iter().any(|(_, i)| {
                i.names
                    .get(&file.rel)
                    .is_some_and(|e| e.sha256 == sha && e.size == file.size)
            });
            // A manifest kept as it was, merged by day into the folder's
            // manifest (store migrate-cas): held.
            let merged = indexes.iter().any(|(_, i)| {
                i.pending
                    .get(&sha)
                    .is_some_and(|w| w.starts_with(&format!("merged into {} ", file.rel)))
            });
            if bound || merged {
                held += 1;
            } else if indexes.iter().any(|(_, i)| i.pending.contains_key(&sha)) {
                pending += 1;
                f(
                    "S6",
                    src,
                    format!(
                        "{} ({} bytes, {sha}): kept as pending, not bound: an operator's choice",
                        file.rel, file.size
                    ),
                );
            } else {
                f(
                    "S6",
                    src,
                    format!(
                        "{} ({} bytes, {sha}): not held on any volume given",
                        file.rel, file.size
                    ),
                );
            }
        }
        against.push(json!({
            "source": src, "path": root, "files": w.files.len(), "bound": held,
            "pending": pending, "skipped": w.skipped,
        }));
    }
    let (merged, conflicts) = merge(&indexes);
    for c in conflicts {
        f("S7", "*", format!("volumes disagree: {c}"));
    }
    let mut under = 0usize;
    for (name, m) in &merged {
        if m.copies.len() < a.min_copies {
            under += 1;
            f(
                "S7",
                "*",
                format!(
                    "{name}: {} cop{} ({}), fewer than {}",
                    m.copies.len(),
                    if m.copies.len() == 1 { "y" } else { "ies" },
                    m.copies.join(","),
                    a.min_copies
                ),
            );
        }
    }
    let status = if !unreadable.is_empty() {
        2
    } else {
        i32::from(!findings.is_empty())
    };
    let report = json!({
        "contract": tinytrader_cas::FORMAT,
        "against": against,
        "volumes": volumes,
        "names": merged.len(),
        "under_copies": under,
        "min_copies": a.min_copies,
        "findings": findings,
        "unreadable": unreadable,
        "sound": status == 0,
    });
    (report, status)
}

fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let a = match args(&argv) {
        Ok(a) => a,
        Err(e) => {
            if !e.is_empty() {
                eprintln!("tt-fsck: {e}");
            }
            eprintln!(
                "usage: tt-fsck [--quick] [--json] [--min-copies N] [--against NAME=PATH]... PATH..."
            );
            std::process::exit(if e.is_empty() { 0 } else { 2 });
        }
    };
    let (report, status) = check(&a);
    if a.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).unwrap_or_default()
        );
    } else {
        if a.quick {
            println!("QUICK CHECK: sizes only, nothing hashed; not a gate for deleting anything");
        }
        for s in report["against"].as_array().into_iter().flatten() {
            println!(
                "source {}: {} files, {} bound, {} pending",
                s["source"].as_str().unwrap_or_default(),
                s["files"],
                s["bound"],
                s["pending"]
            );
            for k in s["skipped"].as_array().into_iter().flatten() {
                println!(
                    "source {}: not checked, an OS folder at its root: {}",
                    s["source"].as_str().unwrap_or_default(),
                    k.as_str().unwrap_or_default()
                );
            }
        }
        for v in report["volumes"].as_array().into_iter().flatten() {
            println!(
                "volume {}: {} names, {} objects{}",
                v["volume"].as_str().unwrap_or_default(),
                v["names"],
                v["objects"],
                if v["torn_last_line"].is_null() {
                    ""
                } else {
                    "; its log's last line is torn (tolerated)"
                }
            );
        }
        for x in report["findings"].as_array().into_iter().flatten() {
            println!(
                "{} {}: {}",
                x["rule"].as_str().unwrap_or_default(),
                x["volume"].as_str().unwrap_or_default(),
                x["finding"].as_str().unwrap_or_default()
            );
        }
        for u in report["unreadable"].as_array().into_iter().flatten() {
            println!("unreadable: {}", u.as_str().unwrap_or_default());
        }
        println!(
            "{}: {} names across {} volumes, {} under {} copies",
            if status == 0 { "sound" } else { "NOT sound" },
            report["names"],
            report["volumes"].as_array().map_or(0, Vec::len),
            report["under_copies"],
            a.min_copies
        );
    }
    std::process::exit(status);
}

#[cfg(test)]
mod tests {
    use super::*;
    use tinytrader_cas::Event;

    fn scratch(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("tt-fsck-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn volume(root: &std::path::Path, id: &str, days: &[(&str, &[u8])]) -> Volume {
        let v = Volume::init(root, id).unwrap();
        let mut log = v.log().unwrap();
        for (name, body) in days {
            let o = v.put(*body, "dbn.zst", None).unwrap();
            log.append(&Event::Bind {
                name: (*name).into(),
                version: 1,
                sha256: o.sha256,
                size: o.size,
                ext: "dbn.zst".into(),
                kind: "day".into(),
                source: "test".into(),
                previous: None,
                writer: "test".into(),
                at: 1,
            })
            .unwrap();
        }
        v
    }

    fn run(paths: &[&std::path::Path], min: usize) -> (Value, i32) {
        check(&Args {
            against: Vec::new(),
            quick: false,
            json: true,
            min_copies: min,
            paths: paths.iter().map(|p| p.to_path_buf()).collect(),
        })
    }

    #[test]
    fn copies_are_counted_across_volumes_and_disagreements_found() {
        let d = scratch("copies");
        let (a, b) = (d.join("a"), d.join("b"));
        volume(&a, "a", &[("one", b"1"), ("two", b"2")]);
        volume(&b, "b", &[("one", b"1"), ("two", b"X")]);
        let (r, status) = run(&[&a, &b], 2);
        assert_eq!(status, 1, "{r}");
        let f: Vec<&str> = r["findings"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|x| x["finding"].as_str())
            .collect();
        assert!(
            f.iter().any(|x| x.starts_with("volumes disagree: two")),
            "{f:?}"
        );
        assert_eq!(
            r["under_copies"], 0,
            "one has two copies; two is in conflict, not counted"
        );
        let (r, status) = run(&[&a], 2);
        assert_eq!((status, r["under_copies"].clone()), (1, json!(2)));
        let (r, status) = run(&[&a], 1);
        assert_eq!(status, 0, "{r}");
    }

    #[test]
    fn a_volume_being_written_or_unreadable_is_exit_2() {
        let d = scratch("held");
        let a = d.join("a");
        let v = volume(&a, "a", &[("one", b"1")]);
        let w = v.lock_writer().unwrap();
        let (r, status) = run(&[&a], 1);
        assert_eq!(status, 2, "{r}");
        drop(w);
        assert_eq!(run(&[&a], 1).1, 0);
        assert_eq!(run(&[&d.join("nothing")], 1).1, 2);
    }

    #[test]
    fn damage_is_a_finding_of_s1() {
        let d = scratch("damage");
        let a = d.join("a");
        let v = volume(&a, "a", &[("one", b"1")]);
        let p = v
            .object(&v.index().unwrap().names["one"].sha256, "dbn.zst")
            .unwrap();
        let mut perm = std::fs::metadata(&p).unwrap().permissions();
        #[allow(clippy::permissions_set_readonly_false)]
        perm.set_readonly(false);
        std::fs::set_permissions(&p, perm).unwrap();
        std::fs::write(&p, b"2").unwrap();
        let (r, status) = run(&[&a], 1);
        assert_eq!(status, 1);
        assert_eq!(r["findings"][0]["rule"], "S1");
        assert!(
            r["findings"][0]["finding"]
                .as_str()
                .unwrap()
                .starts_with("damaged"),
            "{r}"
        );
    }

    /// Review C2: a source checked against the volumes it went to: every
    /// file held passes; one changed since, or never copied, doesn't; an
    /// unmounted (empty) source is refused, never clean.
    #[test]
    fn a_source_is_checked_file_by_file() {
        let d = scratch("against");
        let (src, a) = (d.join("src"), d.join("a"));
        std::fs::create_dir_all(src.join("x")).unwrap();
        std::fs::write(src.join("x/one"), b"1").unwrap();
        volume(&a, "a", &[("x/one", b"1")]);
        let against = |root: &std::path::Path| {
            check(&Args {
                against: vec![("data".into(), root.to_path_buf())],
                quick: false,
                json: true,
                min_copies: 1,
                paths: vec![a.clone()],
            })
        };
        let (r, status) = against(&src);
        assert_eq!(status, 0, "{r}");
        assert_eq!(r["against"][0]["bound"], 1);
        std::fs::write(src.join("x/two"), b"2").unwrap();
        let (r, status) = against(&src);
        assert_eq!(status, 1);
        assert!(r["findings"].to_string().contains("x/two (1 bytes"), "{r}");
        std::fs::write(src.join("x/two"), b"").unwrap();
        std::fs::remove_file(src.join("x/two")).unwrap();
        std::fs::write(src.join("x/one"), b"9").unwrap();
        assert_eq!(against(&src).1, 1, "same size, other bytes");
        let empty = d.join("mnt-empty");
        std::fs::create_dir_all(&empty).unwrap();
        assert_eq!(against(&empty).1, 2, "an empty source is refused");
    }

    /// A migration that didn't end clean leaves the volume unsound.
    #[test]
    fn an_unclean_migration_receipt_is_a_finding() {
        let d = scratch("receipt");
        let a = d.join("a");
        let v = volume(&a, "a", &[("one", b"1")]);
        v.write_receipt("tt03", &json!({ "runs": [{ "ok": false, "conflicts": 1, "damaged": 0, "errors": [] }], "files": {} })).unwrap();
        let (r, status) = run(&[&a], 1);
        assert_eq!(status, 1);
        assert!(
            r["findings"].to_string().contains("migration from tt03"),
            "{r}"
        );
    }
}
