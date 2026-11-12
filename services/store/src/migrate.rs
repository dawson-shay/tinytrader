//! `store migrate-cas`: today's volumes (files at their names) into a volume
//! in the content-addressed format (decision 0035 section 9).
//!
//! Every regular file on each source volume becomes an object, so nothing is
//! left behind: the store's day files, and its receipts, ledgers, manifests,
//! uploads not yet filed, superseded days, and anything else. Each is bound
//! under its path within the volume (`Databento/OPRA.PILLAR/…`). Only the
//! operating system's own files (`.DS_Store`, `.Spotlight-V100`, …) are left,
//! counted. Sources are only read.
//!
//! - **A hash the store recorded** for a file (its manifest, a filing
//!   ledger, a batch receipt: see the recorded module) must match what's
//!   read; a file that doesn't is reported damaged, and isn't bound.
//! - **One name on several sources:** the same content is one object, with
//!   one binding. Different contents are a conflict: each is kept as a
//!   pending object, none is bound, and it's reported.
//! - **Resumable:** a name already bound at the source file's size, whose
//!   recorded hash (if any) is the bound one, isn't read again.
//! - **`--plan`** writes nothing: what would be copied, what's already
//!   there, the duplicates and conflicts it can tell from sizes and recorded
//!   hashes, and the totals.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};
use tinytrader_cas::{Event, Volume};

/// What a source file is, from where it lies.
fn kind(rel: &str) -> &'static str {
    let parts: Vec<&str> = rel.split('/').collect();
    let has = |p: &str| parts.contains(&p);
    let file = parts.last().copied().unwrap_or_default();
    if has(crate::ingest::INCOMING) {
        "incoming"
    } else if has("provenance") {
        "receipt"
    } else if has("migrations") {
        "ledger"
    } else if has(crate::catalog::SUPERSEDED) {
        "superseded"
    } else if file == crate::manifest::MANIFEST || has(crate::manifest::SIDECARS) {
        "manifest"
    } else if file.ends_with(".dbn.zst") || file.ends_with(".dbn") {
        "day"
    } else {
        "other"
    }
}

/// The operating system's own files and folders, never data.
fn os_own(name: &str) -> bool {
    matches!(
        name,
        ".DS_Store"
            | ".Spotlight-V100"
            | ".fseventsd"
            | ".Trashes"
            | ".TemporaryItems"
            | ".DocumentRevisions-V100"
            | "lost+found"
            | "$RECYCLE.BIN"
            | "System Volume Information"
    ) || name.starts_with("._")
}

/// A file found on a source.
struct Found {
    rel: String,
    path: PathBuf,
    size: u64,
}

/// Every regular file under `root`, with what couldn't be read.
fn walk(root: &Path) -> (Vec<Found>, Vec<String>, usize) {
    let (mut out, mut errors, mut skipped) = (Vec::new(), Vec::new(), 0usize);
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let rd = match std::fs::read_dir(&dir) {
            Ok(rd) => rd,
            Err(e) => {
                errors.push(format!("{}: {e}", dir.display()));
                continue;
            }
        };
        for e in rd {
            let e = match e {
                Ok(e) => e,
                Err(err) => {
                    errors.push(format!("{}: {err}", dir.display()));
                    continue;
                }
            };
            let name = e.file_name().to_string_lossy().into_owned();
            let path = e.path();
            // A CAS volume already here (the destination on the same pool)
            // isn't a source.
            if os_own(&name) || (dir == root && name == tinytrader_cas::DIR) {
                skipped += 1;
                continue;
            }
            match e.file_type() {
                Ok(t) if t.is_dir() => stack.push(path),
                Ok(t) if t.is_file() => match e.metadata() {
                    Ok(m) => out.push(Found {
                        rel: path
                            .strip_prefix(root)
                            .unwrap_or(&path)
                            .to_string_lossy()
                            .into_owned(),
                        path,
                        size: m.len(),
                    }),
                    Err(err) => errors.push(format!("{}: {err}", path.display())),
                },
                Ok(_) => errors.push(format!(
                    "{}: a link or special file; not copied",
                    path.display()
                )),
                Err(err) => errors.push(format!("{}: {err}", path.display())),
            }
        }
    }
    out.sort_by(|a, b| a.rel.cmp(&b.rel));
    (out, errors, skipped)
}

/// The extension an object keeps: the file's, from its first dot (so
/// `2026-10-02.dbn.zst` keeps `dbn.zst`), or `bin`.
fn ext_of(rel: &str) -> String {
    let file = rel.rsplit('/').next().unwrap_or(rel);
    let ext = file.split_once('.').map(|(_, e)| e).unwrap_or_default();
    let ok = !ext.is_empty()
        && !ext.ends_with('.')
        && ext
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'-' || b == b'_');
    if ok { ext.to_owned() } else { "bin".into() }
}

/// One source volume, as `NAME=PATH`.
pub struct Source {
    pub name: String,
    pub root: PathBuf,
}

/// Parse `NAME=PATH`.
///
/// # Errors
/// If it isn't one.
pub fn source(s: &str) -> Result<Source, String> {
    let (name, path) = s
        .split_once('=')
        .filter(|(n, p)| !n.is_empty() && !p.is_empty())
        .ok_or_else(|| format!("{s}: give a source as NAME=PATH"))?;
    Ok(Source {
        name: name.to_owned(),
        root: PathBuf::from(path),
    })
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

const WRITER: &str = concat!("store migrate-cas ", env!("CARGO_PKG_VERSION"));

/// Run the migration (or with `plan`, only say what it would do). Returns
/// the report, and whether everything was copied and bound (or, for a plan,
/// could be).
///
/// # Errors
/// If the destination can't be opened or its log written.
pub fn run(
    sources: &[Source],
    dest: Option<(&Path, &str)>,
    plan: bool,
    progress: &mut dyn FnMut(&str),
) -> Result<(Value, bool), String> {
    let vol = match dest {
        Some((root, id)) if plan => Volume::open(root).ok().filter(|v| v.id == id),
        Some((root, id)) => Some(Volume::init(root, id)?),
        None if plan => None,
        None => return Err("give the destination (--to PATH --id VOLUME_ID)".into()),
    };
    let index = match &vol {
        Some(v) => v.index()?,
        None => tinytrader_cas::Index::default(),
    };
    let mut log = match (&vol, plan) {
        (Some(v), false) => Some(v.log()?),
        _ => None,
    };
    // Each name: every source's file, with its recorded hash.
    struct Copy<'a> {
        source: &'a Source,
        found: Found,
        recorded: Option<String>,
        bad_record: Option<String>,
    }
    let mut names: BTreeMap<String, Vec<Copy>> = BTreeMap::new();
    let mut errors = Vec::new();
    let mut os_skipped = 0usize;
    for s in sources {
        progress(&format!("reading {} ({})", s.name, s.root.display()));
        if !s.root.is_dir() {
            errors.push(format!("{}: {} isn't a folder", s.name, s.root.display()));
            continue;
        }
        let (found, errs, skipped) = walk(&s.root);
        errors.extend(errs.into_iter().map(|e| format!("{}: {e}", s.name)));
        os_skipped += skipped;
        for f in found {
            let (recorded, bad_record) = match crate::recorded::of(&s.root, &f.path, f.size, 0) {
                crate::recorded::Recorded::Known(h) => (Some(h), None),
                crate::recorded::Recorded::Unknown => (None, None),
                crate::recorded::Recorded::Conflict(why) => (None, Some(why)),
            };
            names.entry(f.rel.clone()).or_default().push(Copy {
                source: s,
                found: f,
                recorded,
                bad_record,
            });
        }
    }
    let mut by_kind: BTreeMap<&str, (u64, u64)> = BTreeMap::new();
    let (mut to_copy, mut to_copy_bytes, mut already, mut bound) = (0u64, 0u64, 0u64, 0u64);
    let (mut conflicts, mut damaged, mut unrecorded) = (Vec::new(), Vec::new(), 0u64);
    let (mut duplicates, mut copied_bytes) = (0u64, 0u64);
    for (name, copies) in &names {
        let k = kind(name);
        let e = by_kind.entry(k).or_default();
        e.0 += 1;
        e.1 += copies[0].found.size;
        if copies.len() > 1 {
            duplicates += 1;
        }
        if copies.iter().all(|c| c.recorded.is_none()) {
            unrecorded += 1;
        }
        for c in copies
            .iter()
            .filter_map(|c| c.bad_record.as_ref().map(|w| (c, w)))
        {
            damaged.push(format!(
                "{name} on {}: its recorded hashes disagree ({}); copied, but checked against none",
                c.0.source.name, c.1
            ));
        }
        // Told apart without reading: sizes, and recorded hashes.
        let sizes: std::collections::BTreeSet<u64> = copies.iter().map(|c| c.found.size).collect();
        let hashes: std::collections::BTreeSet<&str> = copies
            .iter()
            .filter_map(|c| c.recorded.as_deref())
            .collect();
        if sizes.len() > 1 || hashes.len() > 1 {
            conflicts.push(json!({
                "name": name,
                "copies": copies.iter().map(|c| json!({ "source": c.source.name, "size": c.found.size, "recorded_sha256": c.recorded })).collect::<Vec<_>>(),
            }));
            if plan {
                continue;
            }
        }
        // Already bound, and nothing says it's another content: done.
        if let Some(b) = index.names.get(name)
            && copies.iter().all(|c| c.found.size == b.size)
            && hashes.iter().all(|h| *h == b.sha256)
        {
            already += 1;
            continue;
        }
        to_copy += 1;
        to_copy_bytes += copies[0].found.size;
        let (Some(v), Some(log)) = (&vol, log.as_mut()) else {
            continue;
        };
        // Copy every source's file; each distinct content is one object.
        let mut objects: Vec<(&Copy, tinytrader_cas::ObjectRef)> = Vec::new();
        for c in copies {
            match v.put_file(&c.found.path, &ext_of(name), c.recorded.as_deref()) {
                Ok(o) => {
                    if !o.existed {
                        copied_bytes += o.size;
                    }
                    objects.push((c, o));
                }
                Err(e) => damaged.push(format!("{name} on {}: {e}", c.source.name)),
            }
        }
        let distinct: std::collections::BTreeSet<&str> =
            objects.iter().map(|(_, o)| o.sha256.as_str()).collect();
        let current = index.names.get(name);
        let fits = |sha: &str| current.is_none_or(|b| b.sha256 == sha);
        if distinct.len() == 1 && objects.len() == copies.len() && fits(&objects[0].1.sha256) {
            let (c, o) = &objects[0];
            if current.is_none() {
                log.append(&Event::Bind {
                    name: name.clone(),
                    version: 1,
                    sha256: o.sha256.clone(),
                    size: o.size,
                    ext: ext_of(name),
                    kind: k.to_owned(),
                    source: format!(
                        "migrated from {}",
                        copies
                            .iter()
                            .map(|c| format!("{}:{name}", c.source.name))
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                    previous: None,
                    writer: WRITER.into(),
                    at: now(),
                })?;
                bound += 1;
            }
            let _ = c;
        } else {
            // Kept, not bound: every content found for the name, pending
            // an operator's choice.
            for (c, o) in &objects {
                log.append(&Event::Pending {
                    sha256: o.sha256.clone(),
                    ext: ext_of(name),
                    for_: format!("conflict over {name} (from {})", c.source.name),
                    writer: WRITER.into(),
                    at: now(),
                })?;
            }
            if sizes.len() <= 1 && hashes.len() <= 1 {
                conflicts.push(json!({
                    "name": name,
                    "copies": objects.iter().map(|(c, o)| json!({ "source": c.source.name, "size": o.size, "sha256": o.sha256 })).collect::<Vec<_>>(),
                    "bound": current.map(|b| &b.sha256),
                }));
            }
        }
        if (bound + already) % 1000 == 0 {
            progress(&format!(
                "{bound} bound, {already} already there, {copied_bytes} bytes written"
            ));
        }
    }
    let verify = match (&vol, plan) {
        (Some(v), false) => {
            v.snapshot()?;
            Some(v.verify(false)?)
        }
        _ => None,
    };
    let ok = errors.is_empty()
        && conflicts.is_empty()
        && damaged.is_empty()
        && verify.as_ref().is_none_or(tinytrader_cas::Report::sound);
    let report = json!({
        "plan": plan,
        "sources": sources.iter().map(|s| json!({ "name": s.name, "path": s.root })).collect::<Vec<_>>(),
        "destination": vol.as_ref().map(|v| json!({ "path": v.root, "volume": v.id })),
        "names": names.len(),
        "by_kind": by_kind.iter().map(|(k, (n, b))| (k.to_string(), json!({ "names": n, "bytes": b }))).collect::<serde_json::Map<_, _>>(),
        "to_copy": { "names": to_copy, "bytes": to_copy_bytes },
        "already_there": already,
        "bound": bound,
        "bytes_written": copied_bytes,
        "on_several_sources": duplicates,
        "without_a_recorded_hash": unrecorded,
        "os_files_skipped": os_skipped,
        "conflicts": conflicts,
        "damaged": damaged,
        "errors": errors,
        "verify": verify,
        "ok": ok,
    });
    Ok((report, ok))
}

/// `store migrate-cas …`: parse its arguments, run, print the report.
pub fn main(args: &[String]) -> i32 {
    let usage = "usage: store migrate-cas [--plan] [--to PATH --id VOLUME_ID] --from NAME=PATH [--from NAME=PATH …]";
    let (mut plan, mut to, mut id, mut from) = (false, None, None, Vec::new());
    let mut it = args.iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--plan" => plan = true,
            "--to" => to = it.next().map(PathBuf::from),
            "--id" => id = it.next().cloned(),
            "--from" => match it.next().map(|s| source(s)) {
                Some(Ok(s)) => from.push(s),
                Some(Err(e)) => {
                    eprintln!("store migrate-cas: {e}");
                    return 2;
                }
                None => {
                    eprintln!("{usage}");
                    return 2;
                }
            },
            "-h" | "--help" => {
                println!("{usage}");
                return 0;
            }
            other => {
                eprintln!("store migrate-cas: unknown argument {other}\n{usage}");
                return 2;
            }
        }
    }
    if from.is_empty() || (!plan && (to.is_none() || id.is_none())) {
        eprintln!("{usage}");
        return 2;
    }
    let dest = match (&to, &id) {
        (Some(t), Some(i)) => Some((t.as_path(), i.as_str())),
        _ => None,
    };
    match run(&from, dest, plan, &mut |m| {
        eprintln!("store migrate-cas: {m}")
    }) {
        Ok((report, ok)) => {
            println!(
                "{}",
                serde_json::to_string_pretty(&report).unwrap_or_default()
            );
            if ok { 0 } else { 1 }
        }
        Err(e) => {
            eprintln!("store migrate-cas: {e}");
            2
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn scratch(name: &str) -> PathBuf {
        let d =
            std::env::temp_dir().join(format!("tinytrader-migrate-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        fs::create_dir_all(&d).unwrap();
        d
    }

    fn put(root: &Path, rel: &str, body: &[u8]) {
        let p = root.join(rel);
        fs::create_dir_all(p.parent().unwrap()).unwrap();
        fs::write(p, body).unwrap();
    }

    const DAY1: &str = "Databento/OPRA.PILLAR/SPY.OPT/cbbo-1m/2022-09-07.dbn.zst";
    const DAY2: &str = "Databento/OPRA.PILLAR/SPY.OPT/cbbo-1m/2022-09-08.dbn.zst";

    #[test]
    fn every_file_is_migrated_duplicates_once_conflicts_kept_unbound() {
        let d = scratch("all");
        let (a, b, dest) = (d.join("data"), d.join("tt03"), d.join("nas"));
        put(&a, DAY1, b"day one");
        put(&a, DAY2, b"day two");
        put(&a, "Databento/provenance/OPRA-X/manifest.json", b"{}");
        put(
            &a,
            "Databento/.incoming/OPRA-Y/opra.tcbbo.dbn.zst",
            b"pending upload",
        );
        put(&a, ".DS_Store", b"junk");
        put(&b, DAY1, b"day one");
        put(&b, DAY2, b"day 2!!");
        put(&b, "notes.txt", b"other");
        let sources = [
            source(&format!("data={}", a.display())).unwrap(),
            source(&format!("tt03={}", b.display())).unwrap(),
        ];
        // The plan writes nothing, and can't tell DAY2 apart by size.
        let (p, ok) = run(&sources, Some((&dest, "nas-1")), true, &mut |_| {}).unwrap();
        assert!(!dest.exists(), "a plan writes nothing");
        assert!(ok, "{p}");
        assert_eq!(
            (
                p["names"].clone(),
                p["to_copy"]["names"].clone(),
                p["on_several_sources"].clone()
            ),
            (json!(5), json!(5), json!(2))
        );
        assert_eq!(p["by_kind"]["day"]["names"], 2);
        assert_eq!(p["by_kind"]["incoming"]["names"], 1);
        assert_eq!(p["os_files_skipped"], 1);
        // The copy: DAY1 once, DAY2's two contents kept and unbound.
        let (r, ok) = run(&sources, Some((&dest, "nas-1")), false, &mut |_| {}).unwrap();
        assert!(!ok, "a conflict isn't ok: {r}");
        assert_eq!(r["bound"], 4, "{r}");
        assert_eq!(r["conflicts"].as_array().unwrap().len(), 1);
        assert_eq!(r["conflicts"][0]["name"], DAY2);
        let v = Volume::open(&dest).unwrap();
        let i = v.index().unwrap();
        assert!(i.names.contains_key(DAY1) && !i.names.contains_key(DAY2));
        assert_eq!(i.names[DAY1].kind, "day");
        assert_eq!(i.pending.len(), 2, "both contents kept");
        assert!(r["verify"]["orphans"].as_array().unwrap().is_empty(), "{r}");
        assert_eq!(
            i.names[DAY1].source,
            format!("migrated from data:{DAY1}, tt03:{DAY1}")
        );
        // The sources weren't touched.
        assert_eq!(fs::read(a.join(DAY2)).unwrap(), b"day two");
        // Again: nothing more is bound or written for what's there.
        let (again, _) = run(&sources, Some((&dest, "nas-1")), false, &mut |_| {}).unwrap();
        assert_eq!(
            (again["bound"].clone(), again["already_there"].clone()),
            (json!(0), json!(4)),
            "{again}"
        );
        assert_eq!(again["bytes_written"], 0);
    }

    #[test]
    fn a_file_unlike_its_recorded_hash_is_not_bound() {
        let d = scratch("recorded");
        let (a, dest) = (d.join("data"), d.join("nas"));
        put(&a, DAY1, b"what's on disk");
        let ledger =
            json!({ "step": "moved", "from": "x", "to": a.join(DAY1), "sha256": "0".repeat(64) });
        put(
            &a,
            "Databento/migrations/2026-10-06-incorporate-B.events.jsonl",
            format!("{ledger}\n").as_bytes(),
        );
        let sources = [source(&format!("data={}", a.display())).unwrap()];
        let (r, ok) = run(&sources, Some((&dest, "nas-1")), false, &mut |_| {}).unwrap();
        assert!(!ok);
        assert!(r["damaged"][0].as_str().unwrap().contains(DAY1), "{r}");
        let i = Volume::open(&dest).unwrap().index().unwrap();
        assert!(!i.names.contains_key(DAY1), "not bound");
        assert!(
            i.names
                .contains_key("Databento/migrations/2026-10-06-incorporate-B.events.jsonl")
        );
    }

    #[test]
    fn kinds_and_extensions() {
        assert_eq!(kind(DAY1), "day");
        assert_eq!(
            kind("Databento/OPRA.PILLAR/SPY.OPT/cbbo-1m/manifest.json"),
            "manifest"
        );
        assert_eq!(
            kind("Databento/OPRA.PILLAR/SPY.OPT/cbbo-1m/manifest.d/2026-01-02.json"),
            "manifest"
        );
        assert_eq!(kind("Databento/superseded/x.dbn.zst"), "superseded");
        assert_eq!(ext_of(DAY1), "dbn.zst");
        assert_eq!(ext_of("README"), "bin");
        assert_eq!(ext_of("a/b c.weird ext"), "bin");
    }
}
