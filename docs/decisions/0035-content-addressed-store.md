# 0035: A content-addressed store, its names routed by an index

- **Status:** Proposed
- **Date:** 2026-11-11
- **Amends:** [0002](0002-data-store.md) (the layout on disk),
  [0007](0007-storage-pool-and-automations.md) (copies),
  [0031](0031-distributed-volumes.md) (volumes, nodes)
- **Absorbs:** the ingress review's (A) stuck uploads, (B) serving pending
  data, (D) the integrity sweep, (E) hashes on reads; and (C) recoverability
  (0034) attaches to it.
- **Decided by the user** on 2026-11-11; this records how it's built.

## Context

The store keeps files at their names, one tree per volume. That tree is its
only record of what it holds, and the drives under it fail:

- **A drive that drops out takes its days with it** until it's back. With no
  second copy known by content, the store can't serve the day from elsewhere.
- **A file is hashed once, on arrival,** and never again, so a file damaged at
  rest is served as if it were whole.
- **Half-done writes look done:** an upload interrupted by a dropout leaves a
  batch in a hidden folder, partly placed, sometimes reported "filed"
  (ingress review P0 4, (A)).
- **Copies are files at the same name on two volumes,** which the store
  compares only when it files.

## Decision

### 1. Objects: immutable, named by their hash

Each volume keeps the data as **objects**:
`objects/<aa>/<bb>/<sha256>.<ext>` (such as `.dbn.zst`), where `aa` and `bb`
are the hash's first bytes.

- **Written to a temporary name, fsynced, checked, and renamed into place**
  before anything points at it. An object is never modified afterwards: it's
  read-only on disk, and its name is its content.
- **One object per content.** The same bytes under two names are one object
  (a receipt filed twice, say).
- **No hard links or symlinks.** Nothing in the data tree is a link.
- **Provenance receipts** (manifests, metadata, `condition.json`) are objects
  too, so they're checked like the data.

### 2. The index: names → hashes → copies

The **index** is the source of truth for what the store holds. Each entry:

```json
{ "name": "Databento/OPRA.PILLAR/SPY.OPT/cbbo-1m/2026-10-02.dbn.zst",
  "version": 3, "sha256": "…", "size": 1169420, "kind": "day",
  "coverage": { … }, "source": "batch OPRA-20261003-X",
  "previous": "…sha256 of version 2…",
  "copies": [ { "volume": "<id>", "verified_at": 1794300000 } ] }
```

- **The name is today's logical path**, so `/files`, `/coverage` and the
  catalog keep their names and contracts.
- **Versions:** a partial day's merge, or a supersede, writes a new object and
  then flips the name to it, with `version` + 1 and `previous` set. The old
  object stays, addressable by its hash and linked from provenance, until an
  operator's garbage collection (section 5).
- **Kept on every volume,** each volume holding the entries for the objects it
  holds. The coordinator (0031) merges them by name. The highest version wins;
  two different hashes at one version are a conflict, reported and never
  resolved silently.
- **Written atomically:** each volume's index is a log (below), plus a
  snapshot rewritten whole (written beside, fsynced, renamed).

### 3. The bindings log: so the index can always be rebuilt

Hashing the objects again recovers what each object is, but not what it was
called: an object doesn't know its name. So every volume also keeps an
append-only, fsynced **bindings log** (`index/bindings.jsonl`). Each line is
one event: a name bound to a hash and version, a copy added or verified, a
name retired. The index snapshot is a cache of that log.

- **Rebuild:** replay the log, then hash every object against its name. An
  object with no binding is listed as an orphan, kept, and reported, never
  deleted.
- **Losing the log** loses names, not data: orphans can be re-named from
  their DBN headers and provenance receipts, by a tool, with an operator
  confirming.
- **The log is small** (one line per filing step), and is copied with the
  volume's objects.

### 4. Writes: object first, then the name

- **Upload:** each file is written as an object (verified on arrival as
  today) into the volume's objects. It's *pending*: bound to its batch in the
  log, not to a name.
- **Filing:** a batch's checks run against the index, not the tree, so
  there's no stat of a dropped-out volume to misread (P0 2 and 3). Then, in
  one step per volume, its days' names are bound, and the batch is filed only
  when every day is. A batch that can't complete stays pending, with
  nothing named. databento-jobs and alpaca-history see it as not filed (P0 4),
  and an event says why within the hour ((A)).
- **The pinned volume:** a batch's objects go to the one volume placement
  chose. If that volume goes away mid-upload, the upload waits for it or is
  placed again whole, never half here and half there.

### 5. Copies, retiring, and garbage

- **Replicate:** copy an object, verify its hash at the destination, then add
  the copy to the index. A 0007 second copy is exactly this.
- **Retire a copy** only when at least N other copies are verified (N from
  the dataset's protection class: 1 for refetchable data, 2 for purchased and
  irreplaceable data).
- **Garbage collection** of objects no name points at (old versions, orphans)
  is an operator's action, listed first as a dry run, never automatic.

### 6. Reads: routed, verified

- **`GET /files/<name>`** resolves the name in the index to any healthy,
  verified copy. If a volume drops out, it reads from another copy (failover).
- **It sends `X-Content-SHA256`** (and `Digest: sha-256=…`).
  `tinytrader-client`, `tt` and research's `tt.py` verify it as they read and
  fail loudly on a mismatch ((E)).
- **Pending objects** are served read-only by hash
  (`GET /objects/<sha256>`), so a copy or backup of an unfiled batch goes
  through the API ((B)).

### 7. The integrity sweep

- **A background pass (0011's class, paced, paused on volume errors)** re-hashes
  every object against its name, weekly, and after every dropout or remount of
  its volume ((D)).
- **It updates `verified_at`.** A mismatch marks the copy bad, which is an
  event and makes the day `suspect` in `/coverage` while no good copy is
  verified. It never deletes or replaces anything; repair is copying a good
  copy over, by an operator.
- **Recoverability (0034)** labels index entries, so the at-risk list is
  "irreplaceable entries with fewer than N verified copies".

### 8. No tree to browse

There's no readable tree on disk. `tt store ls`, `/catalog`, `/coverage` and
`/files` are the view, as they already are for every client (nobody reads the
drives directly). For an emergency read without the store, `tt store export
<name> <dest>` (or the migration tool in reverse) materialises names from a
volume's own log, read-only.

### 9. Migration

- **`store migrate-cas --plan`** (read-only) walks today's catalog. It lists
  every file it would hash into an object and bind, every duplicate (one
  object), every conflict (one name, two different contents across volumes),
  and the totals, and writes nothing.
- **The real migration happens on the NAS, on Sunday 2026-11-15,** as the data
  is copied in. Each file is hashed once, while copied (one pass over the
  bytes), written as an object, then bound in the log. The source drives
  aren't modified: they stay as the fallback until the NAS copy is verified,
  and become second copies afterwards (replicated as objects).
- **Old layouts:** a batch download's range file or a day folder becomes one
  object per file, named as `/files` names it today.

## Where the ingress work lands

| Item | In this design |
|---|---|
| P0 3 (no filing beside a copy it can't see) | ships now as is; under CAS, the checks are against the index |
| (A) stuck uploads | section 4: pending until whole, an event within the hour, the pinned volume |
| (B) serve pending data | section 6: `/objects/<sha256>` |
| (C) recoverability | 0034, on index entries |
| (D) integrity sweep | section 7 |
| (E) hashes on reads | section 6, which can ship before CAS: today's manifests already record each day's sha256 |
| (F) the cache's empty hash | unchanged: databento-jobs, fail closed |

Order: (E) and (F) first (useful now, and unchanged by CAS). Then this
design's core: objects, the log and index, migrate-cas `--plan`, and reads.
Then filing through the index ((A), P0 4, P0 6). Then the sweep (D), and
(B). Building (A) and (D) on today's tree first would be work thrown away on
Sunday.

## Reasons to be careful

- **Sunday is close.** The migration's dry run, the object writer, the log,
  and reads by name must be done and tested before then. Filing through the
  index can follow: until it lands, the NAS is filled by the migration tool,
  and new filings wait or go to today's tree and are migrated after.
- **Two layouts for a while.** Until every volume is migrated, the store reads
  both: a name resolves through the index first, else the tree. The catalog
  says which each volume is.
- **Performance:** an index lookup per read and per filing check is small next
  to the I/O. The log grows by one line per filing step and is compacted into
  the snapshot.

## Steps

1. **The format:** the object writer, the bindings log, and the index
   (snapshot, rebuild, merge across volumes); reads by name, with the hash
   header; `tt store ls`.
2. **`migrate-cas --plan`**, then the copy-and-hash migration onto the NAS.
3. **Filing through the index:** pending objects, binding, versions, and the
   pinned volume.
4. **Replication, retiring and the sweep;** `suspect` days.
5. **`/objects/<sha256>`** for pending and old versions; garbage collection
   as a dry run.
