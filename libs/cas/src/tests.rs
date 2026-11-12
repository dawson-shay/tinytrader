use super::*;

fn scratch(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("tinytrader-cas-{name}-{}", std::process::id()));
    let _ = fs::remove_dir_all(&d);
    fs::create_dir_all(&d).unwrap();
    d
}

fn sha(b: &[u8]) -> String {
    use sha2::Digest as _;
    sha2::Sha256::digest(b)
        .iter()
        .map(|x| format!("{x:02x}"))
        .collect()
}

fn bind(name: &str, version: u64, o: &ObjectRef, previous: Option<&str>) -> Event {
    Event::Bind {
        name: name.into(),
        version,
        sha256: o.sha256.clone(),
        size: o.size,
        ext: "dbn.zst".into(),
        kind: "day".into(),
        source: "test".into(),
        previous: previous.map(str::to_owned),
        writer: "test 0".into(),
        at: 1,
    }
}

const DAY: &str = "Databento/OPRA.PILLAR/SPY.OPT/cbbo-1m/2026-10-02.dbn.zst";

#[test]
fn an_object_is_named_by_its_hash_read_only_and_written_once() {
    let d = scratch("put");
    let v = Volume::init(&d, "vol-a").unwrap();
    let o = v
        .put(&b"day one"[..], "dbn.zst", Some(&sha(b"day one")))
        .unwrap();
    assert_eq!(o.sha256, sha(b"day one"));
    assert_eq!(o.path, object_path(&d.join(DIR), &o.sha256, "dbn.zst"));
    assert_eq!(fs::read(&o.path).unwrap(), b"day one");
    assert!(fs::metadata(&o.path).unwrap().permissions().readonly());
    assert!(!o.existed);
    // The same bytes again: one object, nothing written.
    let again = v.put(&b"day one"[..], "dbn.zst", None).unwrap();
    assert!(again.existed);
    // Bytes that aren't what was expected: refused, nothing kept.
    let e = v
        .put(&b"day two"[..], "dbn.zst", Some(&sha(b"other")))
        .unwrap_err();
    assert!(e.contains("not"), "{e}");
    assert!(v.object(&sha(b"day two"), "dbn.zst").is_none());
    assert_eq!(
        fs::read_dir(d.join("cas/tmp")).unwrap().count(),
        0,
        "no temporary left"
    );
    // A bad extension or expected hash is refused.
    assert!(v.put(&b"x"[..], "../x", None).is_err());
    assert!(v.put(&b"x"[..], "dbn", Some("abc")).is_err());
}

#[test]
fn a_volume_is_its_format_and_its_id() {
    let d = scratch("init");
    Volume::init(&d, "vol-a").unwrap();
    assert_eq!(Volume::open(&d).unwrap().id, "vol-a");
    assert_eq!(Volume::init(&d, "vol-a").unwrap().id, "vol-a");
    assert!(
        Volume::init(&d, "vol-b")
            .unwrap_err()
            .contains("already holds volume vol-a")
    );
    fs::write(
        d.join("cas/FORMAT"),
        r#"{"format":"tinytrader.storage/0","volume":"vol-a"}"#,
    )
    .unwrap();
    assert!(
        Volume::open(&d)
            .unwrap_err()
            .contains("not tinytrader.storage/1")
    );
    assert!(Volume::init(&scratch("init-bad"), "a/b").is_err());
}

#[test]
fn the_index_is_the_log_replayed_with_versions_in_order() {
    let d = scratch("index");
    let v = Volume::init(&d, "vol-a").unwrap();
    let one = v.put(&b"partial"[..], "dbn.zst", None).unwrap();
    let two = v.put(&b"merged"[..], "dbn.zst", None).unwrap();
    let mut log = v.log().unwrap();
    log.append(&bind(DAY, 1, &one, None)).unwrap();
    log.append(&bind(DAY, 2, &two, Some(&one.sha256))).unwrap();
    // Out of order, and a `previous` that isn't current: listed, not applied.
    log.append(&bind(DAY, 4, &one, Some(&two.sha256))).unwrap();
    log.append(&bind(DAY, 3, &one, Some(&one.sha256))).unwrap();
    // The same binding again (a retried append) is no conflict.
    log.append(&bind(DAY, 2, &two, Some(&one.sha256))).unwrap();
    let i = v.index().unwrap();
    let e = &i.names[DAY];
    assert_eq!(
        (e.version, e.sha256.as_str(), e.previous.as_deref()),
        (2, two.sha256.as_str(), Some(one.sha256.as_str()))
    );
    assert_eq!(i.conflicts.len(), 2, "{:?}", i.conflicts);
    // The snapshot is a cache: the same index, then the log replayed on.
    assert_eq!(v.snapshot().unwrap().names, i.names);
    let three = v.put(&b"superseded"[..], "dbn.zst", None).unwrap();
    v.log()
        .unwrap()
        .append(&bind(DAY, 3, &three, Some(&two.sha256)))
        .unwrap();
    assert_eq!(v.index().unwrap().names[DAY].version, 3);
    // A damaged snapshot is ignored.
    fs::write(d.join("cas/index/snapshot.json"), "{").unwrap();
    assert_eq!(v.index().unwrap().names[DAY].sha256, three.sha256);
    // Retired: gone from the index.
    v.log()
        .unwrap()
        .append(&Event::Retire {
            name: DAY.into(),
            sha256: three.sha256.clone(),
            why: "test".into(),
            writer: "t".into(),
            at: 2,
        })
        .unwrap();
    assert!(v.index().unwrap().names.is_empty());
}

#[test]
fn a_torn_last_line_is_reported_and_the_log_goes_on() {
    let d = scratch("torn");
    let v = Volume::init(&d, "vol-a").unwrap();
    let one = v.put(&b"a"[..], "dbn.zst", None).unwrap();
    v.log().unwrap().append(&bind(DAY, 1, &one, None)).unwrap();
    let mut f = fs::OpenOptions::new()
        .append(true)
        .open(v.log_path())
        .unwrap();
    f.write_all(br#"{"event":"bind","name":"x","vers"#).unwrap();
    drop(f);
    let r = v.verify(true).unwrap();
    assert!(r.torn_last_line.is_some());
    assert!(r.sound(), "{r:?}");
    assert_eq!(v.index().unwrap().names.len(), 1);
    // Appending after it: the torn line is ended, and both replay.
    let two = v.put(&b"b"[..], "dbn.zst", None).unwrap();
    v.log()
        .unwrap()
        .append(&bind("other", 1, &two, None))
        .unwrap();
    assert_eq!(v.index().unwrap().names.len(), 2);
    assert!(v.verify(false).unwrap().torn_last_line.is_none());
    // A damaged line that isn't cut short is an error, not skipped.
    let mut f = fs::OpenOptions::new()
        .append(true)
        .open(v.log_path())
        .unwrap();
    f.write_all(b"{\"event\":\"nonsense\"}\n").unwrap();
    drop(f);
    assert!(v.index().unwrap_err().contains("not a bindings event"));
}

#[test]
fn verify_finds_what_is_missing_damaged_orphaned_or_misplaced() {
    let d = scratch("verify");
    let v = Volume::init(&d, "vol-a").unwrap();
    let one = v.put(&b"one"[..], "dbn.zst", None).unwrap();
    let two = v.put(&b"two"[..], "dbn.zst", None).unwrap();
    let old = v.put(&b"old"[..], "dbn.zst", None).unwrap();
    let mut log = v.log().unwrap();
    log.append(&bind("a", 1, &old, None)).unwrap();
    log.append(&bind("a", 2, &one, Some(&old.sha256))).unwrap();
    log.append(&bind("b", 1, &two, None)).unwrap();
    log.append(&Event::Pending {
        sha256: sha(b"never written"),
        ext: "dbn.zst".into(),
        for_: "batch X".into(),
        writer: "t".into(),
        at: 1,
    })
    .unwrap();
    let r = v.verify(true).unwrap();
    assert!(r.sound(), "{r:?}");
    assert_eq!(
        (r.names, r.objects, r.unreferenced),
        (2, 3, 1),
        "the old version is kept, unreferenced"
    );
    // An object nothing ever named; one damaged in place; one gone.
    let stray = v.put(&b"stray"[..], "dbn.zst", None).unwrap();
    let mut p = fs::metadata(&two.path).unwrap().permissions();
    #[allow(clippy::permissions_set_readonly_false)]
    p.set_readonly(false);
    fs::set_permissions(&two.path, p).unwrap();
    fs::write(&two.path, b"tw0").unwrap();
    fs::remove_file(&one.path).unwrap();
    // A file where a folder belongs, and one that isn't named by a hash.
    fs::write(
        d.join("cas/objects").join(&stray.sha256[..2]).join("junk"),
        b"",
    )
    .unwrap();
    fs::create_dir_all(d.join("cas/objects/zz/zz")).unwrap();
    fs::write(d.join("cas/objects/zz/zz/not-a-hash.dbn.zst"), b"").unwrap();
    let r = v.verify(true).unwrap();
    assert!(!r.sound());
    assert_eq!(r.missing.len(), 1, "{r:?}");
    assert_eq!(r.damaged.len(), 1, "{r:?}");
    assert!(r.orphans.iter().any(|o| o.contains(&stray.sha256)), "{r:?}");
    assert_eq!(r.misnamed.len(), 2, "{r:?}");
    // Without hashing, a same-size damage isn't seen; a missing one is.
    let quick = v.verify(false).unwrap();
    assert_eq!((quick.missing.len(), quick.damaged.len()), (1, 0));
}

#[test]
fn volumes_merge_by_name_highest_version_conflicts_reported() {
    let e = |version: u64, sha: &str| Entry {
        version,
        sha256: sha.into(),
        size: 1,
        ext: "dbn.zst".into(),
        kind: "day".into(),
        source: "t".into(),
        previous: None,
    };
    let idx = |pairs: &[(&str, Entry)]| Index {
        names: pairs
            .iter()
            .map(|(n, e)| ((*n).to_owned(), e.clone()))
            .collect(),
        ..Index::default()
    };
    let (m, conflicts) = merge(&[
        (
            "a".into(),
            idx(&[
                ("same", e(1, "x")),
                ("newer", e(1, "old")),
                ("split", e(2, "p")),
            ]),
        ),
        (
            "b".into(),
            idx(&[
                ("same", e(1, "x")),
                ("newer", e(2, "new")),
                ("split", e(2, "q")),
            ]),
        ),
    ]);
    assert_eq!(m["same"].copies, ["a", "b"]);
    assert_eq!(
        (m["newer"].entry.sha256.as_str(), m["newer"].copies.clone()),
        ("new", vec!["b".to_owned()])
    );
    assert!(
        !m.contains_key("split"),
        "a conflict isn't resolved silently"
    );
    assert_eq!(conflicts.len(), 1, "{conflicts:?}");
}
