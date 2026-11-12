# 0039: The storage contract, and `tt-fsck` that checks it from the disk alone

- **Status:** Proposed
- **Date:** 2026-11-11
- **Builds on:** [0035](0035-content-addressed-store.md) (objects, the bindings
  log, the index), [0034](0034-recoverability.md) (labels),
  [0037](0037-time-conventions.md) (clock), [0038](0038-security-master.md)
  (identity)
- **Asked for by the user** on 2026-11-11: "I can rewrite vibe-coded slop,
  but the persistent data on disk needs to be bulletproof."

## Context

The services that write the data will be rewritten, perhaps more than once.
The data must outlive every one of them. Today, what the data on disk must
look like is spread across the store's code, its tests, and decisions 0002,
0007, 0018 and 0035. Nothing outside the store can say whether a volume is
sound, and nothing checks a volume with the store stopped.

## Decision

### 1. One versioned spec of the on-disk invariants

`docs/storage-contract.md` (`tinytrader.storage/1`) states what a volume must
satisfy, whatever program wrote it. Each rule has an id (`S1`, `S2`, …) that
`tt-fsck` reports against. Changing a rule bumps the version, and a volume
records which version it was written under.

- **S1, objects:**
  - each is named by its content (`objects/<aa>/<bb>/<sha256>.<ext>`);
  - written to a temporary name, fsynced, renamed into place, its folder
    fsynced, then made read-only;
  - never modified, and never a link.
- **S2, the bindings log:**
  - per volume, append-only, one JSON line per event, each line fsynced;
  - every line carries its writer and version;
  - a torn last line (a crash mid-write) is the only damage allowed, and it
    is ignored and reported.
  - **The index is a cache:** it is rebuildable from the log and the
    objects, and is never the only record of anything.
- **S3, provenance:** every object has a provenance record, itself an
  object, bound with it:
  - its source and request;
  - when it was fetched or received (0037: UTC nanoseconds, our clock, with
    its recorded offset);
  - the upstream manifest and hashes;
  - the writer and its version.
- **S4, formats:**
  - market data is DBN with its version in the header;
  - reference tables are Parquet (0038);
  - raw source bytes are kept exactly as received (0040);
  - an unknown format is held and reported, never rewritten.
- **S5, identity:** names and symbols resolve through the security master
  (0038). An object's binding records the ids it was filed under.
- **S6, never silent:**
  - every object has a recoverability label (0034);
  - nothing counts as held without at least one verified copy;
  - every failure to verify is an event and a mark on the copy, never
    skipped.
- **S7, protection classes:**
  - at least 2 verified copies on independent disks;
  - at least 3 for `irreplaceable` data: our own recordings, paid data, and
    data outside every free window (0034).
  - Independent means a different physical disk. A ZFS mirror counts as one
    copy per disk only if `tt-fsck` can see both members.

### 2. `tt-fsck`: the conformance test

`tt-fsck <volume>…` reads volumes directly, with the store **not running**
(it refuses if a store writer holds the volume's lock). It writes nothing.

- **Checks:**
  - every object's hash against its name;
  - the log against the objects: bindings to missing objects, and objects
    no binding names (orphans);
  - batches unbound or half-bound (some days named, not all);
  - the index snapshot against a replay of the log;
  - provenance present for every object;
  - copy counts across the volumes given, against each object's class.
- **Reports:** one line per finding, with its rule id, as text and as JSON.
  The exit status is 0 when sound, 1 on findings, and 2 when it couldn't
  read a volume.
- **Paced:** it reads at a set rate, so it can run against a live NAS pool
  during a maintenance window.
- **It's the conformance test:** any rewrite of the store, recorders or
  filers passes when `tt-fsck` is clean on what it wrote. The test suite
  runs it on every volume its tests write.
- **Built:** right after 0035's core, since it validates it. It's a small
  binary with no store dependency, so it can be built years later from the
  spec alone.

### 3. Cold copies: `tt store export`

The user keeps cold drives off-site, topped up from time to time.

`tt store export --class irreplaceable --to <volume>` (any class, or
`--all`):
- **Writes objects and their log lines** to the destination volume, in the
  same layout, so the cold drive is a volume in its own right. It never
  writes a name-tree.
- **Resumes:** an interrupted export picks up where it left off, from what
  the destination's log already binds.
- **Verifies:** each object is hashed as it's written and read back after
  fsync. A copy is added to the index only when verified.
- **Can be checked alone years later:** `tt-fsck <cold drive>` needs nothing
  else. The drive carries a copy of `storage-contract.md` and the `tt-fsck`
  source at the version it was written under.
- **Recorded:** the store records the cold copy (volume id, when verified),
  so S7's counts include it, marked `offline`. `/recoverability` says when
  each cold drive was last topped up.

### 4. Where it runs

The store, its volumes and the recorders run on the Linux NAS under systemd
(ZFS likely, NixOS likely; the user is still deciding).
- **One writer per volume:** a lock file held by the store, which `tt-fsck`
  and `export` respect.
- **Before start:** the unit checks the mount and the volume's identity (its
  id file), and refuses to start on a wrong or missing volume.
- **Scrubs:** ZFS scrubs check blocks. `tt-fsck` checks the contract. Both
  run; neither replaces the other.

## Steps

1. **`storage-contract.md`** with rules S1–S7, written beside 0035's core.
2. **`tt-fsck`** after the core: hashes, log against objects, orphans, the
   half-bound, copy counts. It runs in the suite on every volume tests write.
3. **`tt store export`** with resume, verification, and offline copies in
   the counts.
4. **The deployment's checks:** the systemd unit's mount and identity check,
   and `tt-fsck` in maintenance.

## Open questions

- **How many cold drives, and how often** they're topped up: the user's call.
  The export doesn't depend on it.
