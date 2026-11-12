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
/// If the destination can't be opened or its log written, or a source is
/// empty (not mounted, say).
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
    // One writer at a time, and no check meanwhile (0039).
    let _held = match (&vol, plan) {
        (Some(v), false) => Some(v.lock_writer()?),
        _ => None,
    };
    let index = match &vol {
        Some(v) => v.index()?,
        None => tinytrader_cas::Index::default(),
    };
    let mut log = match (&vol, plan) {
        (Some(v), false) => Some(v.log()?),
        _ => None,
    };
    // What each source gave before (its receipt in the destination): a file
    // is skipped only when this same path, at the same size and time, was
    // hashed into an object still there (review C1).
    let mut receipts: BTreeMap<String, Value> = BTreeMap::new();
    for s in sources {
        let r = match &vol {
            Some(v) => v.receipt(&s.name)?,
            None => None,
        };
        receipts.insert(
            s.name.clone(),
            r.unwrap_or_else(|| json!({ "source": s.name, "runs": [], "files": {} })),
        );
    }
    // What each source is now (its volume id, or its device).
    let identities: BTreeMap<String, String> = sources
        .iter()
        .map(|s| (s.name.clone(), tinytrader_cas::tree::identity(&s.root)))
        .collect();
    struct Copy<'a> {
        source: &'a Source,
        found: tinytrader_cas::tree::Found,
        recorded: Option<String>,
        bad_record: Option<String>,
    }
    let mut names: BTreeMap<String, Vec<Copy>> = BTreeMap::new();
    let (mut errors, mut skipped) = (Vec::new(), Vec::new());
    for s in sources {
        progress(&format!("reading {} ({})", s.name, s.root.display()));
        if !s.root.is_dir() {
            return Err(format!("{}: {} isn't a folder", s.name, s.root.display()));
        }
        let w = tinytrader_cas::tree::walk(&s.root);
        // An empty source is more likely a drive not mounted than a drive
        // with nothing on it: never read as "nothing to copy" (review C2).
        if w.files.is_empty() {
            return Err(format!(
                "{}: {} holds no files; is the drive mounted? Nothing was copied",
                s.name,
                s.root.display()
            ));
        }
        errors.extend(w.errors.into_iter().map(|e| format!("{}: {e}", s.name)));
        skipped.extend(w.skipped.into_iter().map(|e| format!("{}: {e}", s.name)));
        for f in w.files {
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
    let (mut to_read, mut to_read_bytes, mut already, mut bound) = (0u64, 0u64, 0u64, 0u64);
    let (mut conflicts, mut damaged, mut unrecorded) = (Vec::new(), Vec::new(), 0u64);
    let (mut duplicates, mut copied_bytes, mut kept_pending) = (0u64, 0u64, 0u64);
    let (mut manifest_merges, mut manifest_conflicts) = (Vec::new(), Vec::new());
    for (name, copies) in &names {
        let k = kind(name);
        let ext = ext_of(name);
        let e = by_kind.entry(k).or_default();
        e.0 += 1;
        e.1 += copies[0].found.size;
        if copies.len() > 1 {
            duplicates += 1;
        }
        if copies.iter().all(|c| c.recorded.is_none()) {
            unrecorded += 1;
        }
        let current = index.names.get(name);
        // Each copy's content: from its receipt when it's provably the same
        // file read before, else read now.
        let known = |c: &Copy| -> Option<String> {
            let r = &receipts[&c.source.name];
            // A receipt from another drive mounted under this name is never
            // believed (re-review (2)).
            if r["identity"].as_str() != Some(identities[&c.source.name].as_str()) {
                return None;
            }
            let prev = &r["files"][&c.found.rel];
            let sha = prev["sha256"].as_str()?;
            // The same file, unchanged: a rewrite with its time put back, or
            // a copy, has another inode or change time.
            let f = &c.found;
            let same = prev["size"].as_u64() == Some(f.size)
                && prev["mtime_ns"].as_str() == Some(f.mtime_ns.to_string().as_str())
                && prev["dev"].as_u64() == Some(f.dev)
                && prev["ino"].as_u64() == Some(f.ino)
                && prev["ctime_ns"].as_str() == Some(f.ctime_ns.to_string().as_str());
            let held = vol.as_ref().is_some_and(|v| v.object(sha, &ext).is_some());
            (same && held).then(|| sha.to_owned())
        };
        // A schema folder's manifest, on several drives: merged by day
        // (review of #160: Data and tt03 share folders, each with its own
        // manifest, so differing copies are expected). Each copy is kept
        // as it was, "merged into" the name.
        if k == "manifest" && name.ends_with(&format!("/{}", crate::manifest::MANIFEST)) {
            let mut parsed = Vec::new();
            for c in copies {
                match std::fs::read(&c.found.path)
                    .ok()
                    .and_then(|b| serde_json::from_slice::<Value>(&b).ok())
                {
                    Some(m) if m["days"].is_object() => parsed.push((c.source.name.clone(), m)),
                    _ => damaged.push(format!(
                        "{name} on {}: not a manifest that can be read; kept, not merged",
                        c.source.name
                    )),
                }
            }
            if parsed.len() == copies.len() {
                let bound_m: Option<Value> = current
                    .and_then(|b| vol.as_ref()?.object(&b.sha256, &b.ext))
                    .and_then(|p| std::fs::read(p).ok())
                    .and_then(|b| serde_json::from_slice(&b).ok());
                let mut all = parsed.clone();
                if let Some(b) = &bound_m {
                    all.insert(0, ("bound".to_owned(), b.clone()));
                }
                let (merged, day_conflicts) = crate::manifest::merge_indexes(&all);
                let distinct: std::collections::BTreeSet<String> =
                    all.iter().map(|(_, m)| m.to_string()).collect();
                if distinct.len() > 1 {
                    manifest_merges.push(json!({ "name": name, "copies": all.iter().map(|(s, _)| s).collect::<Vec<_>>() }));
                }
                manifest_conflicts.extend(
                    day_conflicts
                        .iter()
                        .map(|d| json!({ "name": name, "day": d })),
                );
                let changes = bound_m.as_ref() != Some(&merged);
                if plan {
                    if changes {
                        to_read += 1;
                        to_read_bytes += copies.iter().map(|c| c.found.size).sum::<u64>();
                    } else {
                        already += 1;
                    }
                    continue;
                }
                let (Some(v), Some(log)) = (&vol, log.as_mut()) else {
                    continue;
                };
                let bytes = serde_json::to_vec_pretty(&merged).unwrap_or_default();
                let merged_sha = if changes {
                    match v.put(&bytes[..], "json", None) {
                        Ok(o) => {
                            log.append(&Event::Bind {
                                name: name.clone(),
                                version: current.map_or(1, |b| b.version + 1),
                                sha256: o.sha256.clone(),
                                size: o.size,
                                ext: "json".into(),
                                kind: k.to_owned(),
                                source: format!(
                                    "merged from {}",
                                    copies
                                        .iter()
                                        .map(|c| format!("{}:{name}", c.source.name))
                                        .collect::<Vec<_>>()
                                        .join(", ")
                                ),
                                previous: current.map(|b| b.sha256.clone()),
                                writer: WRITER.into(),
                                at: now(),
                            })?;
                            bound += 1;
                            o.sha256
                        }
                        Err(e) => {
                            damaged.push(format!(
                                "{name}: the merged manifest couldn't be written: {e}"
                            ));
                            continue;
                        }
                    }
                } else {
                    already += 1;
                    current.map(|b| b.sha256.clone()).unwrap_or_default()
                };
                for c in copies {
                    match v.put_file(&c.found.path, "json", None) {
                        Ok(o) => {
                            if !o.existed {
                                copied_bytes += o.size;
                            }
                            if o.sha256 != merged_sha && !index.pending.contains_key(&o.sha256) {
                                log.append(&Event::Pending {
                                    sha256: o.sha256.clone(),
                                    ext: "json".into(),
                                    for_: format!("merged into {name} (from {})", c.source.name),
                                    writer: WRITER.into(),
                                    at: now(),
                                })?;
                            }
                            receipts
                                .get_mut(&c.source.name)
                                .expect("each source has a receipt")["files"][&c.found.rel] = json!({
                                "size": c.found.size, "mtime_ns": c.found.mtime_ns.to_string(),
                                "dev": c.found.dev, "ino": c.found.ino, "ctime_ns": c.found.ctime_ns.to_string(),
                                "sha256": o.sha256, "outcome": if o.sha256 == merged_sha { "bound" } else { "merged" },
                            });
                        }
                        Err(e) => damaged.push(format!("{name} on {}: {e}", c.source.name)),
                    }
                }
                continue;
            }
        }
        if plan {
            let sizes: std::collections::BTreeSet<u64> =
                copies.iter().map(|c| c.found.size).collect();
            let hashes: std::collections::BTreeSet<&str> = copies
                .iter()
                .filter_map(|c| c.recorded.as_deref())
                .collect();
            if sizes.len() > 1 || hashes.len() > 1 {
                conflicts.push(json!({ "name": name, "copies": copies.iter().map(|c| json!({ "source": c.source.name, "size": c.found.size, "recorded_sha256": c.recorded })).collect::<Vec<_>>() }));
            }
            for c in copies.iter().filter(|c| c.bad_record.is_some()) {
                damaged.push(format!(
                    "{name} on {}: {}",
                    c.source.name,
                    c.bad_record.as_deref().unwrap_or_default()
                ));
            }
            if copies.iter().all(|c| known(c).is_some()) && current.is_some() {
                already += 1;
            } else {
                to_read += 1;
                to_read_bytes += copies.iter().map(|c| c.found.size).sum::<u64>();
            }
            continue;
        }
        let (Some(v), Some(log)) = (&vol, log.as_mut()) else {
            continue;
        };
        // (copy, its content's hash, whether it may be bound)
        let mut got: Vec<(&Copy, String, bool)> = Vec::new();
        for c in copies {
            if let Some(sha) = known(c) {
                got.push((c, sha, c.bad_record.is_none()));
                continue;
            }
            // Read whatever the record says: bytes unlike their record are
            // kept (they may be the only copy), never bound (review H2).
            match v.put_file(&c.found.path, &ext, None) {
                Ok(o) => {
                    if !o.existed {
                        copied_bytes += o.size;
                    }
                    let mut ok = true;
                    if let Some(why) = &c.bad_record {
                        damaged.push(format!(
                            "{name} on {}: its record disagrees ({why}); kept, not bound",
                            c.source.name
                        ));
                        ok = false;
                    }
                    if let Some(want) = c.recorded.as_deref().filter(|w| *w != o.sha256) {
                        damaged.push(format!("{name} on {}: SHA-256 {}, but {want} was recorded when filed; kept, not bound", c.source.name, o.sha256));
                        ok = false;
                    }
                    got.push((c, o.sha256, ok));
                }
                Err(e) => damaged.push(format!("{name} on {}: {e}", c.source.name)),
            }
        }
        let distinct: std::collections::BTreeSet<&str> = got.iter().map(|g| g.1.as_str()).collect();
        let clean = got.len() == copies.len() && got.iter().all(|g| g.2) && distinct.len() == 1;
        let sha = got.first().map(|g| g.1.clone()).unwrap_or_default();
        let fits = current.is_none_or(|b| b.sha256 == sha);
        let outcome = if clean && fits {
            if current.is_none() {
                log.append(&Event::Bind {
                    name: name.clone(),
                    version: 1,
                    sha256: sha.clone(),
                    size: copies[0].found.size,
                    ext: ext.clone(),
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
                "bound"
            } else {
                already += 1;
                "already"
            }
        } else {
            // Every content found for the name is kept, pending an
            // operator's choice; none is bound over another.
            for (c, h, _) in &got {
                if current.is_some_and(|b| b.sha256 == *h) {
                    continue;
                }
                log.append(&Event::Pending {
                    sha256: h.clone(),
                    ext: ext.clone(),
                    for_: format!("conflict over {name} (from {})", c.source.name),
                    writer: WRITER.into(),
                    at: now(),
                })?;
                kept_pending += 1;
            }
            if distinct.len() > 1 || !fits {
                conflicts.push(json!({
                    "name": name,
                    "copies": got.iter().map(|(c, h, _)| json!({ "source": c.source.name, "size": c.found.size, "sha256": h })).collect::<Vec<_>>(),
                    "bound": current.map(|b| &b.sha256),
                }));
            }
            "pending"
        };
        for (c, h, ok) in &got {
            receipts
                .get_mut(&c.source.name)
                .expect("each source has a receipt")["files"][&c.found.rel] = json!({
                "size": c.found.size, "mtime_ns": c.found.mtime_ns.to_string(),
                "dev": c.found.dev, "ino": c.found.ino, "ctime_ns": c.found.ctime_ns.to_string(), "sha256": h,
                "outcome": if *ok { outcome } else { "pending" },
            });
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
        && manifest_conflicts.is_empty()
        && verify.as_ref().is_none_or(tinytrader_cas::Report::sound);
    // Each source's receipt: every file read, and how this run ended, kept
    // in the volume so a check later knows (review C2).
    if let (Some(v), false) = (&vol, plan) {
        for s in sources {
            let r = receipts
                .get_mut(&s.name)
                .expect("each source has a receipt");
            r["root"] = json!(s.root);
            r["identity"] = json!(identities[&s.name]);
            if let Some(runs) = r["runs"].as_array_mut() {
                runs.push(json!({
                    "at": now(), "writer": WRITER, "ok": ok,
                    "conflicts": conflicts.len(), "damaged": damaged.len(),
                    "errors": errors.iter().filter(|e| e.starts_with(&format!("{}: ", s.name))).collect::<Vec<_>>(),
                    "skipped": skipped.iter().filter(|e| e.starts_with(&format!("{}: ", s.name))).collect::<Vec<_>>(),
                }));
            }
            v.write_receipt(&s.name, r)?;
        }
    }
    let report = json!({
        "plan": plan,
        "sources": sources.iter().map(|s| json!({ "name": s.name, "path": s.root })).collect::<Vec<_>>(),
        "destination": vol.as_ref().map(|v| json!({ "path": v.root, "volume": v.id })),
        "names": names.len(),
        "by_kind": by_kind.iter().map(|(k, (n, b))| (k.to_string(), json!({ "names": n, "bytes": b }))).collect::<serde_json::Map<_, _>>(),
        "to_read": { "names": to_read, "bytes": to_read_bytes },
        // Folders whose manifests differ across drives, merged by day; the
        // days whose entries disagree read as `unknown` until settled.
        "manifests": { "merged": manifest_merges, "day_conflicts": manifest_conflicts },
        "already_there": already,
        "bound": bound,
        "kept_pending": kept_pending,
        "bytes_written": copied_bytes,
        "on_several_sources": duplicates,
        "without_a_recorded_hash": unrecorded,
        "os_files_skipped": skipped,
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
        // The OS's folder at the root is skipped (listed); an OS-like name
        // anywhere else is data, copied (re-review (1)).
        put(&a, ".Trashes/501/x", b"trash");
        put(&a, "Databento/x/._notes.dbn.zst", b"not apple's");
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
                p["to_read"]["names"].clone(),
                p["on_several_sources"].clone()
            ),
            (json!(7), json!(7), json!(2))
        );
        assert_eq!(p["by_kind"]["day"]["names"], 3);
        assert_eq!(p["by_kind"]["incoming"]["names"], 1);
        assert_eq!(p["os_files_skipped"].as_array().unwrap().len(), 1, "{p}");
        assert!(
            p["os_files_skipped"][0]
                .as_str()
                .unwrap()
                .ends_with(".Trashes")
        );
        // The copy: DAY1 once, DAY2's two contents kept and unbound.
        let (r, ok) = run(&sources, Some((&dest, "nas-1")), false, &mut |_| {}).unwrap();
        assert!(!ok, "a conflict isn't ok: {r}");
        assert_eq!(r["bound"], 6, "{r}");
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
            (json!(0), json!(6)),
            "{again}"
        );
        assert_eq!(again["bytes_written"], 0);
    }

    /// Review C1: a resumed run doesn't skip another source's file of the
    /// same name and size: it's read, and a different content is a conflict,
    /// kept, not lost.
    #[test]
    fn a_later_source_with_other_bytes_at_the_same_size_is_kept() {
        let d = scratch("c1");
        let (a, b, dest) = (d.join("data"), d.join("tt03"), d.join("nas"));
        put(&a, DAY1, b"day one");
        put(&b, DAY1, b"day 1!!");
        let data = [source(&format!("data={}", a.display())).unwrap()];
        let tt03 = [source(&format!("tt03={}", b.display())).unwrap()];
        assert!(
            run(&data, Some((&dest, "nas-1")), false, &mut |_| {})
                .unwrap()
                .1
        );
        let (r, ok) = run(&tt03, Some((&dest, "nas-1")), false, &mut |_| {}).unwrap();
        assert!(!ok, "{r}");
        assert_eq!(r["conflicts"][0]["name"], DAY1, "{r}");
        let v = Volume::open(&dest).unwrap();
        let i = v.index().unwrap();
        assert_eq!(i.pending.len(), 1, "tt03's version is kept");
        let theirs = i.pending.keys().next().unwrap();
        assert_eq!(
            fs::read(v.object(theirs, "dbn.zst").unwrap()).unwrap(),
            b"day 1!!"
        );
        // Its receipt says what happened.
        let rec = v.receipt("tt03").unwrap().unwrap();
        assert_eq!(rec["files"][DAY1]["outcome"], "pending");
        assert_eq!(rec["runs"][0]["ok"], false);
        // data again, unchanged: skipped from its receipt, nothing read.
        let (again, _) = run(&data, Some((&dest, "nas-1")), false, &mut |_| {}).unwrap();
        assert_eq!(
            (
                again["already_there"].clone(),
                again["bytes_written"].clone()
            ),
            (json!(1), json!(0))
        );
    }

    /// Re-review (2): a rewrite at the same size with its modification time
    /// put back, or a copy mounted under the same name, isn't skipped from
    /// the receipt: it's read, and other bytes are a conflict.
    #[test]
    fn a_rewrite_with_its_time_restored_is_read_again() {
        let d = scratch("mtime");
        let (a, dest) = (d.join("data"), d.join("nas"));
        put(&a, DAY1, b"day one");
        let data = [source(&format!("data={}", a.display())).unwrap()];
        assert!(
            run(&data, Some((&dest, "nas-1")), false, &mut |_| {})
                .unwrap()
                .1
        );
        let when = fs::metadata(a.join(DAY1)).unwrap().modified().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(20));
        let tmp = a.join("rewrite.tmp");
        fs::write(&tmp, b"day 1!!").unwrap();
        fs::File::options()
            .write(true)
            .open(&tmp)
            .unwrap()
            .set_modified(when)
            .unwrap();
        fs::rename(&tmp, a.join(DAY1)).unwrap();
        let (r, ok) = run(&data, Some((&dest, "nas-1")), false, &mut |_| {}).unwrap();
        assert!(!ok, "{r}");
        assert_eq!(r["conflicts"][0]["name"], DAY1, "{r}");
        // A copy of the original, under the same source name: read (another
        // inode), the same bytes, so already there.
        let clone = d.join("clone");
        put(&clone, DAY1, b"day one");
        let again = [source(&format!("data={}", clone.display())).unwrap()];
        let (r, _) = run(&again, Some((&dest, "nas-1")), false, &mut |_| {}).unwrap();
        assert_eq!(
            (
                r["already_there"].clone(),
                r["conflicts"].as_array().unwrap().len()
            ),
            (json!(1), 0),
            "{r}"
        );
    }

    /// Review C2: a source with no files (a drive not mounted) is refused,
    /// never read as nothing to copy.
    #[test]
    fn an_empty_source_is_refused() {
        let d = scratch("empty");
        fs::create_dir_all(d.join("mnt-data")).unwrap();
        let sources = [source(&format!("data={}", d.join("mnt-data").display())).unwrap()];
        let e = run(
            &sources,
            Some((&d.join("nas"), "nas-1")),
            false,
            &mut |_| {},
        )
        .unwrap_err();
        assert!(e.contains("is the drive mounted"), "{e}");
        assert!(
            run(&sources, None, true, &mut |_| {}).is_err(),
            "the plan too"
        );
    }

    /// Review H1 and H2: a ledger written where the drive was mounted then
    /// (`/Volumes/Data/...`) still gives the expected hash read elsewhere;
    /// bytes unlike it, or unlike their manifest's size, are kept, not bound.
    #[test]
    fn records_from_another_mount_point_are_used_and_mismatches_kept() {
        let d = scratch("h1");
        let (a, dest) = (d.join("mnt-data"), d.join("nas"));
        put(&a, DAY1, b"what's on disk");
        put(&a, DAY2, b"right");
        let sha = |b: &[u8]| {
            use sha2::Digest as _;
            format!("{:x}", sha2::Sha256::digest(b))
        };
        let lines = [
            json!({ "step": "moved", "from": "x", "to": format!("/Volumes/Data/{DAY1}"), "sha256": sha(b"what was filed") }),
            json!({ "step": "moved", "from": "x", "to": format!("/Volumes/Data/{DAY2}"), "sha256": sha(b"right") }),
        ];
        put(
            &a,
            "Databento/migrations/2026-10-06-incorporate-B.events.jsonl",
            format!("{}\n{}\n", lines[0], lines[1]).as_bytes(),
        );
        // A partial day whose manifest records another size.
        let day3 = "Databento/OPRA.PILLAR/SPY.OPT/cbbo-1m/2022-09-09.dbn.zst";
        put(&a, day3, b"short");
        crate::manifest::set_entry(
            &a.join("Databento/OPRA.PILLAR/SPY.OPT/cbbo-1m"),
            "2022-09-09",
            Some(json!({ "file": "2022-09-09.dbn.zst", "size": 999, "coverage": {} })),
        )
        .unwrap();
        let sources = [source(&format!("data={}", a.display())).unwrap()];
        let (r, ok) = run(&sources, Some((&dest, "nas-1")), false, &mut |_| {}).unwrap();
        assert!(!ok);
        let damaged = r["damaged"].to_string();
        assert!(
            damaged.contains(DAY1) && damaged.contains("was recorded when filed"),
            "{r}"
        );
        assert!(damaged.contains(day3) && damaged.contains("999"), "{r}");
        let i = Volume::open(&dest).unwrap().index().unwrap();
        assert!(i.names.contains_key(DAY2), "matched its record: bound");
        assert!(
            !i.names.contains_key(DAY1) && !i.names.contains_key(day3),
            "kept, not bound"
        );
        assert!(
            i.pending.contains_key(&sha(b"what's on disk"))
                && i.pending.contains_key(&sha(b"short"))
        );
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
