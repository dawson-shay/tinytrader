# 0031: Volumes with their own identity, a store node per host

- **Status:** Proposed
- **Date:** 2026-11-03
- **Amends:** [0007](0007-storage-pool-and-automations.md) (the pool, the
  marker), [0019](0019-purchase-guards.md) (missing volumes stop purchases),
  [0005](0005-store-service-and-supervision.md) (one store process)
- **Builds on:** [0030](0030-store-filing-owner.md) (one filer per store)

## Context

The store is one process on one host, and it knows a volume by its **name and
path**:

- `STORE_VOLUMES` names volumes as `NAME=PATH[:ro]`. An approved drive carries
  `.tinytrader-volume`, which holds `{ name, approved_at, path }`. 0007 says
  the marker holds "the volume's ID and name", but no ID is written today.
- The state file remembers each volume by name: its path, whether it's
  writable, and `held`, the per-schema day counts from its last full read.
- A remembered volume that isn't mounted is in `missing_volumes`, and the jobs
  service refuses **every** purchase while that list isn't empty (0019).

That was right for one host with its drives attached. It no longer fits how
the drives are used:

- **Drives move between hosts.** A host with power trouble has had its data
  drive moved to another host and mounted read-only there. To the store that
  drive is simply missing, and every purchase stops until it comes back. The
  store on the new host doesn't know it is the same volume.
- **Data will live on more than one host** (host drives now, a NAS later). One
  process can only read the drives attached to its own host.
- **Some volumes must never change.** The purchased corpus is irreplaceable.
  Nothing marks a volume as one that must never be written; `:ro` is only a
  flag in the configuration.
- **macOS writes to every volume it mounts.** Spotlight indexes millions of
  DBN files, and `mds` keeps files open so a drive won't unmount cleanly.
  `fseventsd` writes a change log to the volume. This costs I/O on drives that
  already drop off USB under heavy reads, and writes to volumes that should
  hold only data.
- **The deployment's inventory names data roots by host and path**, which is
  wrong as soon as a drive moves or mounts under another name.

## Decision

### 1. A volume is its marker

Every volume the store uses carries `.tinytrader-volume` at its root, as
`tinytrader.volume/1`:

```json
{ "format": "tinytrader.volume/1", "id": "6f1c…-…", "name": "data",
  "created_at": 1792500000.0, "immutable": false, "mount": "rw",
  "filesystem_uuid": "A1B2…" }
```

- **`id`** is a random UUID, written once and never changed. It is the
  volume's identity everywhere: in the store's state, in coverage and
  manifests, and in the inventory. A path is only where a volume is mounted
  now, and a host is only where it is attached now.
- **`name`** is the label people use, and it stays unique in a deployment.
  It is set at creation and doesn't change: `/files/{volume}/…` names are
  stable (0017).
- **`filesystem_uuid`** (the APFS/HFS volume UUID, from `diskutil info`, or
  `blkid` on Linux) is recorded alongside. It identifies a volume that is
  mounted read-only before its marker has an `id` (see [Migration](#migration)).
- **Two volumes with the same `id`** (a block-level copy, or a clone
  restored from backup) are both refused for writes and reported as a
  conflict until an operator retires one. One mounted copy is never taken
  for another.

### 2. A store node per host, one coordinator

The store splits into two roles. Both are the same binary, chosen by
configuration:

- **A node** runs on each host that has volumes attached:
  - it discovers its volumes by marker, in the mount directory and from its
    configured paths;
  - it scans them and serves their files;
  - it files to its own writable volumes through its own filer (0030).
  
  It knows nothing of other hosts.
- **The coordinator** is the store's one address for clients:
  - it keeps the registry: every volume ever seen, by `id`, with the node it
    was last on and a snapshot of what it held;
  - it merges the nodes' catalogs and coverage into one `/catalog` and
    `/coverage`;
  - it decides where new filings go, and answers the purchase questions.
- **Reads:** `GET /files/{volume}/{path}` on the coordinator answers `307` to
  the node that has that volume now. Clients follow it, and the producer
  already checks every file's hash. A `--proxy-reads` option serves through
  the coordinator instead, for a client that can't reach the nodes.
- **Writes:** these are the experimental endpoints (`/incoming`,
  `/incorporate`, `/adopt`, the volume actions).
  - The coordinator places a batch **whole** before its first file. It
    answers each write with `307` to the node chosen for that batch, so all
    of a batch's files land on one node and its filer orders them (0030).
  - `adopt` goes to the node that already has the folder.
- **One host is still one process.** With no `STORE_NODES` configured, the
  store is coordinator and node at once, as today. A small deployment
  changes nothing.
- **A node that doesn't answer** has its volumes reported as offline, with
  their snapshots (§3). The coordinator never takes a silent node to mean
  "holds nothing".

### 3. Offline but known: a snapshot, not a blank

Today `held` is per-schema counts, too coarse to say whether a particular day
is held. The coordinator instead keeps, per volume, the **manifests it last
read in full**: each dataset's day list with each day's coverage, the same
records `/coverage` serves. This is a few MB per volume, written with the
state file.

| Volume state | Its days in `/coverage` | Purchases |
|---|---|---|
| **online** (a node reads it now) | held, available | proceed |
| **offline, known** (not attached, with a full snapshot) | held, `"available": false`, `"volume_state": "offline"` | proceed: its days count as held and aren't bought again |
| **unknown** (never read in full, its snapshot incomplete, or a node that won't say which volumes it has) | not listed | **refused**, as today |
| **retired** | not held | proceed: its days may be bought again |

- `missing_volumes` keeps its name and its rule for the jobs service: refuse
  while it isn't empty. It now lists only the **unknown** volumes.
  `offline_volumes` is added beside it. The jobs service needs no change: it
  simply stops being blocked by a drive that is known to be away.
- An offline day isn't readable. A session or research job that asks for it
  is told which volume holds it, and that the volume is offline.
- A snapshot that turns out wrong is caught when the volume comes back: its
  full scan is compared with the snapshot. If it holds less than its snapshot
  said, the volume is degraded, and the lost days are listed in `/events`. If
  the drive is gone for good, retiring it (as today) makes its days
  purchasable again.

### 4. Placement, and volumes that are never written

- A new filing goes to a volume that is **online, writable, not immutable**,
  and below its critical threshold. The choice is made in this order:
  1. the dataset's placement rule, if the configuration gives one
     (`STORE_PLACEMENT`, such as `databento/*=archive-*`, or by protection
     class: purchased to the NAS);
  2. otherwise 0007's rule: the most room.
- 0007's second copies go to a different volume, and to a different host
  when one is online.
- **`immutable: true`** in the marker means no node writes to that volume
  again: no filing, no `.incoming`, no provenance, no repair, no marker
  rewrite. It is a property of the volume, so it travels with it to any host.
  - Setting it is a store action, `POST /volumes/{id}/seal`.
  - Clearing it is deliberate: the marker is edited by hand, and the volume
    is approved again.
- **`mount`** (`rw` or `ro`) is the volume's mount policy, and `immutable`
  implies `ro`.
  - The node reports a volume mounted against its policy (writable when it
    should be `ro`) as a warning in `/catalog` and `/health`, and treats it as
    read-only regardless.
  - The deployment's host module enforces the policy at mount time: on macOS
    an `/etc/fstab` entry by filesystem UUID with `ro`; on NixOS a
    `fileSystems` entry.

### 5. Indexing and change logs off

At approval, while the volume is still writable, the store writes:

- `.metadata_never_index`, so Spotlight skips the volume, and runs
  `mdutil -i off` on it;
- `.fseventsd/no_log`, so `fseventsd` keeps no change log on it.

Each node checks these on every scan and reports `indexing: off | on |
unknown` per volume. `on` is a warning, with the command that fixes it. A
volume already mounted read-only can't be given them, so it stays `unknown`
until it is next mounted writable. The deployment's host module may also
exclude store volumes from Time Machine.

### 6. Moving a volume between hosts

1. **Unplug it from host A.** A's node stops reporting it, and it becomes
   **offline, known**. Its days still count as held, so purchases go on.
2. **Plug it into host B.** B's node finds the marker and reports the `id`.
   The coordinator moves the registry entry to B, records `moved {id} A → B`
   in `/events`, and B scans it fully against the snapshot.
3. **Reads redirect to B.** Nothing in a client changes, because
   `/files/{volume}/…` names the volume, not the host.

A volume found on a host other than its **expected host** (§7) is used, and
reported as `unexpected_host` (degraded, not refused). That way an emergency
move works at once, and the inventory is corrected afterwards.

### 7. The deployment names volumes, not paths

In a deployment's inventory, a store data root becomes:

```nix
{ volume = "<id>"; name = "data"; expectedHost = "<host>"; mount = "ro"; }
```

- `path` goes: the node finds the volume by its marker.
- The data-roots check (that a container may only mount the roots it is
  granted) uses `expectedHost` and the mount point the node reports.
- The host module writes the fstab entries from `mount`.

### 8. The store checks what a batch says it belongs to

Today the store files a conditions batch like any other. It never reads
`binding.json` (`tinytrader.alpaca-history.conditions-binding/1`), which names
the main file it belongs to by name, size and SHA-256. So a client that
rebuilds a day's conditions after its main was filed can attach them to a
main they don't describe, if the provider's data changed in between.

- **On `incorporate`, a batch that carries a binding is checked** against the
  main it names, on whichever volume holds it, online or offline (by the
  snapshot's hashes).
- **A mismatch is refused with `409`**, naming both hashes, and nothing is
  filed. A binding whose main isn't held is refused the same way. The rule
  lives with the data, not in each client.
- **Clients still check first.** alpaca-history checks the binding before it
  files conditions alone, so the person deciding sees the problem as a failed
  day rather than as a refused filing.

### Migration

From today's state (a state file keyed by name, markers without `id`, and
volumes named only in `STORE_VOLUMES` without a marker), with no rescan of
the data:

1. **Writable volumes.** On its first start, the new store gives each
   remembered or configured volume an `id`. It writes the full v1 marker,
   keeping the name, or creates one for a `STORE_VOLUMES` volume that has
   none. It records the `filesystem_uuid`, and rekeys the state file by `id`.
   The old marker is kept as `.tinytrader-volume.v0`.
2. **Volumes mounted read-only**, like a data drive moved to another host in
   an emergency, can't be written. They are matched to their remembered entry
   by `filesystem_uuid` if one was recorded, otherwise by name plus the
   v0 marker's `approved_at`. They get their `id` the next time they are
   mounted writable. Until then they are fully usable for reads, and they are
   never chosen for writes.
3. **Snapshots** are taken by the first full scan after the upgrade. Until a
   volume has one, an offline volume is **unknown** and blocks purchases as it
   does today.
4. **Moving the store**: the coordinator moves to its new host with its state
   file. Each old host keeps a node until its drives move, and then its node
   is removed. The NAS joins as a node, or as a mounted share under an
   existing node. The inventory changes from host and path to volume IDs in
   the same step.

## Steps

Each step is its own PR, and each one is useful on its own:

1. **Identity.**
   - The v1 marker, `id` and `filesystem_uuid`.
   - The state file keyed by `id`, the migration above, and duplicate-`id`
     refusal.
   - Still one process. Tests: migration from v0 state and markers, a
     read-only volume matched by filesystem UUID, a clone refused.
2. **Offline but known.**
   - Per-volume snapshots, `offline_volumes`, and `missing_volumes` narrowed
     to unknown volumes.
   - The comparison when a volume returns.
   - This alone ends "a moved drive stops every purchase". Tests: a purchase
     proceeds with a known volume away and is refused with an unknown one; a
     returning volume holding less than its snapshot.
3. **Volume hygiene.**
   - Indexing and change logs off at approval, and reported on each scan.
   - `immutable` and `seal`, and `mount` policy warnings.
   - The deployment module's fstab and `fileSystems` entries.
4. **Node and coordinator.**
   - `STORE_NODES`, merged catalogs, `307` reads and placed writes, and
     moves recorded in `/events`.
   - Tests on two nodes on one machine (two mount directories): a move, a
     silent node, a redirect, and a batch kept whole on one node.
5. **Placement rules and cross-host copies** (finishing 0007's copies).

§8, the binding check on `incorporate`, needs none of the above and can land
at any point.

The deployment's migration (its inventory and the order its hosts move in) is
a runbook in the deployment repository, carried out after steps 1–3.

## Consequences

- **A drive away is no longer an outage for purchases**, only for reading
  that drive's days, and every caller is told which volume and why.
- **Moving a drive is plugging it in.** Configuration catches up later.
- **The coordinator is a single point**, as the one store is today. If it is
  down, reads stop even though the nodes are up. A client may be given a
  node's address directly for reading that node's volumes, but no failover
  is designed here.
- **The registry's snapshots can be wrong** (a drive that died while away).
  The cost is a purchase skipped until someone retires the drive, never a
  purchase made twice: the guards stay on the side of not buying.
- **The `/files` contract is unchanged**, with a `307` in front of it. The
  write endpoints change, but they are experimental and their clients are
  tinytrader's own (databento-jobs, alpaca-history), which already follow
  redirects.

## Open questions

- **Reads by redirect or proxy, by default.** Redirect keeps the coordinator
  off the data path, but every client must reach every node. That is true
  today on the tailnet, but not from a container without tailnet access.
  The recommendation is redirect, with `--proxy-reads` for such clients.
- **Whether offline days may still be bought on purpose.** For example, the
  drive is away for weeks and the data is needed now. The recommendation is
  no special path: retire the volume, or bring it back. A purchase is never
  made by a flag.
- **The coordinator's host.** Wherever the most-read data is, to keep
  redirects local. That is a deployment choice.
