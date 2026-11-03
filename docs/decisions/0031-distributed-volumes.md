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
- **Some data must never change.** The purchased corpus is irreplaceable.
  Nothing marks a volume, or a tree on one, as never to be written; `:ro` is
  only a flag in the configuration, and it covers a whole volume.
- **macOS writes to every volume it mounts.** Spotlight indexes millions of
  DBN files, and `mds` keeps files open so a drive won't unmount cleanly.
  `fseventsd` writes a change log to the volume. This costs I/O on drives that
  already drop off USB under heavy reads, and writes to volumes that should
  hold only data.
- **The deployment's inventory names data roots by host and path**, which is
  wrong as soon as a drive moves or mounts under another name.

## Decision

### 1. A volume has an identity of its own

A volume's identity is a random UUID, its **`id`**, written once and never
changed. It is the volume's identity everywhere: in the store's state, in
coverage and manifests, and in the inventory. A path is only where a volume is
mounted now, and a host is only where it is attached now.

Most volumes carry it in `.tinytrader-volume` at their root, as
`tinytrader.volume/1`:

```json
{ "format": "tinytrader.volume/1", "id": "6f1c…-…", "name": "data",
  "created_at": 1792500000.0, "mount": "rw", "epoch": 12,
  "filesystem_uuid": "A1B2…" }
```

- **`name`** is the label people use. It is unique in a deployment and set at
  creation. `/files/{volume}/…` keeps naming volumes by name, so those names
  stay stable (0017). Every other endpoint that names a volume (retire, seal,
  ignore, accept-empty) takes its `id` or its name, as either.
- **`filesystem_uuid`** (the APFS/HFS volume UUID, from `diskutil info`, or
  `blkid` on Linux) is recorded alongside the `id`, here and in the
  coordinator's registry.
- **`epoch`** counts the times a node has mounted the volume writable. The node
  increments it on the marker before its first write, and the coordinator
  records the highest epoch it has seen. A volume that presents a **lower**
  epoch than recorded is a stale clone, or a copy restored from backup. It is
  quarantined: readable, never written, and reported, until an operator
  retires one of the two. Two volumes with the same `id` online at once are
  quarantined the same way. One copy is never taken for another.
- **An immutable volume carries no new marker.** A volume that must never be
  written (§4) isn't given one. Its identity lives in the coordinator's
  registry: an `id` assigned there and bound to its `filesystem_uuid`, plus
  the name and `approved_at` of its existing marker, if it has one. That is
  weaker than a marker, because a block-level clone has the same filesystem
  UUID, and it has no epoch, since nothing may be written to the volume.
  - The store treats "the same volume online twice" as a conflict, and reads
    only from the first copy seen until an operator decides.
  - **The limit:** a clone of a marker-less volume, mounted while the
    original is away, is taken for the original. That is safe for the data:
    such a volume is never written, so a clone holds exactly what the
    original held when it was cloned, and its full scan is still compared
    with the snapshot (§3). What the store can't do is tell the two drives
    apart, for example to say which one to retire. Drives are labelled
    physically for that, and the coordinator records each mount's host and
    time in `/events`.

### 2. A store node per host, one coordinator

The store splits into two roles. Both are the same binary, chosen by
configuration:

- **A node** runs on each host that has volumes attached:
  - it discovers its volumes by marker, or by filesystem UUID for an immutable
    one;
  - it scans them and serves their files;
  - it files to its own writable volumes through its own filer (0030).

  It knows nothing of other hosts.
- **The coordinator** is the store's one address for clients:
  - it keeps the registry: every volume ever seen, by `id`, with the node it
    is on and its snapshot (§3);
  - it merges the nodes' catalogs and coverage into one `/catalog` and
    `/coverage`;
  - it places new filings (§4), and answers the purchase questions.
- **Nodes authenticate.** Each node is approved once, as a drive is, and gets
  its own credential: a token file the deployment provides, kept like the
  store's write token. The coordinator accepts catalogs, filings and volume
  claims only from approved nodes.
  - A node may claim a volume the registry doesn't know only through an
    operator's approval.
  - A node may claim a known volume that has moved to it (§6); the move is
    recorded.
  - A claim for a volume another node still reports is a conflict (§1).
- **Reads:** `GET /files/{volume}/{path}` on the coordinator answers `307` to
  the node that has that volume now. Reads carry no token, and the producer
  already checks every file's hash, so a redirect loses nothing. A
  `--proxy-reads` option serves through the coordinator instead, for a client
  that can't reach the nodes.
- **Writes don't use redirects.** An HTTP client drops `Authorization` on a
  redirect to another host, so a redirected write would arrive without its
  token. Instead:
  1. The client asks the coordinator to place a batch:
     `POST /place/{provider}/{batch}`, with the write token.
  2. The coordinator answers with the node's address and a **write ticket**:
     short-lived, signed by the coordinator, and good only for that batch on
     that node.
  3. The client uploads to the node directly, with the ticket.

  The whole batch goes to one node, whose filer orders it (0030). `adopt` is
  placed the same way, on the node that already has the folder. The write
  endpoints are experimental, and their clients are tinytrader's own
  (databento-jobs, alpaca-history), so they change with this.
- **One host is still one process.** With no `STORE_NODES` configured, the
  store is coordinator and node at once, as today, with no tickets and no
  redirects. A small deployment changes nothing.
- **A node that doesn't answer** has its volumes reported as offline, with
  their snapshots (§3). The coordinator never takes a silent node to mean
  "holds nothing".

### 3. Offline but known: a snapshot kept current

The coordinator keeps, per volume, a **snapshot**: every manifest entry the
volume holds. That is each dataset's days, with each day's coverage, its
files, and **each file's size and SHA-256**, the same records `/coverage`
serves plus the hashes. This is a few MB per volume, written with the state
file.

**The snapshot is current, not just as of the last scan.** The coordinator
hears of each filing before it happens, and again once it has happened:

1. **Intent, before anything is written.** The node's filer records an
   *intent* with the coordinator: the volume, the batch, and the days it will
   add or change. The coordinator writes the intent to its state file before
   it answers. Without that answer, nothing is written, and the node refuses
   the filing with `503`.
2. **The filing.** The node files, and then reports the manifest entries it
   wrote, with the volume's **snapshot version** (a counter the node keeps in
   the volume's own manifests). The coordinator records them, and resolves
   the intent.
3. **The acknowledgement.** The node answers the client only then.

A volume whose snapshot has an **unresolved intent** counts as **unknown**,
not offline-known, so it blocks purchases. That covers the worst case: a node
writes, crashes before reporting, and the drive is unplugged. Its snapshot
can't say what the interrupted filing left, so the store doesn't vouch for
it. Wherever the volume is next mounted, its node reads the volume's own
manifests, which are the record. It reports what was filed, the intent is
resolved, and the volume is known again. An operator can also resolve an
intent by retiring the volume.

So a day filed and then taken away on its volume is still in the snapshot,
and it is never bought again.

| Volume state | Its days in `/coverage` | Purchases |
|---|---|---|
| **online** (a node reads it now) | held, available | proceed |
| **offline, known** (away, with a current snapshot) | held, `"available": false`, `"volume_state": "offline"` | proceed: its days count as held and aren't bought again |
| **unknown** (never read in full, no current snapshot, an unresolved intent, quarantined, or a node that won't say which volumes it has) | not listed | **refused**, as today |
| **retired** | not held | proceed: its days may be bought again |

- `missing_volumes` keeps its name and its rule for the jobs service: refuse
  while it isn't empty. It now lists only the **unknown** volumes.
  `offline_volumes` is added beside it. The jobs service needs no change: it
  simply stops being blocked by a drive that is known to be away.
- An offline day isn't readable. A session or research job that asks for it
  is told which volume holds it, and that the volume is offline.
- **A volume away too long** is reported:
  - degraded in `/health` past `STORE_OFFLINE_WARN` (7 days by default);
  - the snapshot can't vouch for a drive nobody has seen, so its days stay
    held but are flagged.
- **Data lost while away** is found when the volume returns: its full scan is
  compared with the snapshot. Days it no longer holds, or whose hashes
  differ, are listed in `/events` and on the volume as `lost`.
  - A lost day is **not** made purchasable by the scan. A drive that mounted
    badly would otherwise set off purchases.
  - An operator confirms the loss with `POST /volumes/{id}/accept-loss`, and
    only then are those days not held, and purchasable.
  - Retiring a drive that is gone for good does the same for all its days.
- **A lost state file** is rebuilt from the nodes. Each volume's own
  manifests are the record, and the snapshot is only a copy of them. Online
  volumes are read again. Offline volumes whose snapshots were lost become
  **unknown**, and block purchases until they come back. Losing the state
  file is therefore never a reason to buy anything. The state file is kept
  by the deployment's backups, as other state is.

### 4. Placement, and volumes that are never written

- A new filing goes to a volume that is **online, writable, not immutable**,
  and below its critical threshold. The choice is made in this order:
  1. **a day already held goes where it is.** A filing that changes a day
     already held (0018's partial merges, a supersede) goes to the volume
     holding that day, so that volume's filer orders it with every other
     write to the day. Two nodes never write the same day. If that volume is
     offline, the filing waits (`503`, retried) rather than splitting the day
     across volumes;
  2. the dataset's placement rule, if the configuration gives one
     (`STORE_PLACEMENT`, such as `databento/*=archive-*`, or by protection
     class: purchased to the NAS);
  3. otherwise 0007's rule: the most room.
- **Days are reserved at placement.** When the coordinator places a batch,
  it reserves each day the batch adds or changes on the chosen volume, until
  the filing's intent is resolved. A second batch touching a reserved day is
  placed on the same volume, and waits its turn in that volume's filer. Two
  concurrent batches that both bring a new day can never land it on two
  nodes.
- **A batch whose held days are on several volumes** (some of its days on
  one, some on another) is refused at placement, before it is uploaded, with
  `409` naming the volumes. The client keeps its download. Splitting a batch
  per volume is later work; until then, an operator moves the days onto one
  volume, or files the batch as separate per-volume imports.
- 0007's second copies go to a different volume, and to a different host when
  one is online.
- **One write gate.** Every write the store makes to a volume goes through
  one function. It refuses a write to a volume that is sealed, configured
  read-only, mounted read-only, or quarantined, and a write under a sealed
  path (below).
  - **Once a deployment has migrated** (its state file is v1), the gate
    also refuses, by default, any volume without a v1 marker on it saying
    `"mount": "rw"`. The gate reads that from the volume itself at the time
    of the write, not from the registry. So a registry rebuilt or edited (§3)
    can never make a sealed or `:ro` volume writable: only a marker written
    on purpose can, and the migration never writes one to them.
  - **Before migrating**, the gate keeps today's rule (writable unless `:ro`),
    so upgrading a one-process store changes nothing until its operator runs
    the migration.

  The gate covers filing,
  `.incoming`, provenance, migrations, repairs, markers and the indexing
  files. A test drives every write endpoint against such a volume and checks
  that nothing on it changed. fstab and mount flags (below) are a second
  line, not the only one.
- **Immutability is an operator's choice, never inferred** from the kind of
  data a volume holds. A working volume holding purchased data alongside
  everything else stays writable: its held days still take merges and
  supersedes (0018). What an operator seals is never written:
  - **a whole volume** (`"immutable": true`), such as an archive drive;
  - **a path on a working volume** (`"sealed": ["Databento/OPRA.PILLAR"]` in
    its marker), such as a provider's purchased tree. The gate refuses any
    write under a sealed path, including a merge into one of its days, and
    the rest of the volume stays writable. A filing whose days fall under a
    sealed path is refused at placement (`409`, naming the path), never
    left waiting.
  - **A purchase is refused before it is bought** if any of its days would
    be filed under a sealed path: the jobs service asks the store's guard
    (0019), which knows the seals, before it submits. Nothing is bought to
    be refused at filing. A download the store refused for any reason stays
    visible to the guard as pending (the jobs service's ledger), so its days
    are never taken as missing and bought again.

  A seal is recorded in the coordinator's registry and in the volume's
  marker, so it travels with the volume to any host.
  - **A seal holds if either says so.** A path counts as sealed if the marker
    or the registry seals it, so neither a hand edit of one nor a lost
    registry undoes it. Every write of the marker (an epoch's bump on each
    writable mount, an approval, a migration) keeps its `sealed` list and
    `immutable` exactly; a test writes the marker every way the store does,
    and checks that they survive.
  - **Only an operator unseals:** `POST /volumes/{id}/unseal` with the path
    (or none, for the whole volume) and a reason. That removes it from both,
    and is recorded in `/events` and the state file, with who and why.
  - **Before the migration, a seal by configuration.** `STORE_SEALED` (such
    as `data:Databento`, as `VOLUME:PATH`, comma-separated) seals paths on
    today's store, enforced by the same write gate, with no marker and no
    registry. It is the first thing built (step 0), so a purchased tree is
    protected from now through the migration: no filing, adopt,
    `apply_conditions` or scan settling a merge can write under it in
    between. The migration carries each such seal into the volume's v1
    marker and the registry.
  - **Sealing a path** (`POST /volumes/{id}/seal` with `{ "path": … }`) waits
    for the filer to finish any write under it, checks its files against the
    manifests, and adds it to the marker's `sealed` list. The volume stays
    mounted writable.
  - **Sealing a whole volume** (`POST /volumes/{id}/seal`) is the last write
    the volume takes. It needs a writable mount, so the node:
    1. stops placing new filings on the volume, and waits for its filer to
       be idle;
    2. checks every file against the manifests;
    3. writes the marker with `"immutable": true` and syncs it.

    The operator then remounts the volume read-only, which the host module's
    mount policy makes permanent. A volume already mounted read-only (an
    existing archive) is sealed in the registry only, with no write (§1).
  - Clearing `immutable` is deliberate: an operator edits the registry, or
    the marker by hand, and approves the volume again.
- **`mount`** (`rw` or `ro`) is the volume's mount policy, and `immutable`
  implies `ro`.
  - The node reports a volume mounted against its policy (writable when it
    should be `ro`) as a warning in `/catalog` and `/health`, and the write
    gate treats it as read-only regardless.
  - The deployment's host module enforces the policy at mount time: on macOS
    an `/etc/fstab` entry by filesystem UUID with `ro`; on NixOS a
    `fileSystems` entry.

### 5. Indexing and change logs off

At approval, while the volume is still writable, the store writes (through
the write gate):

- `.metadata_never_index`, so Spotlight skips the volume, and runs
  `mdutil -i off` on it;
- `.fseventsd/no_log`, so `fseventsd` keeps no change log on it.

Both are written before a seal's remount, while the volume is still writable.
`mdutil -i off` itself may write Spotlight's state to the volume, so it is
run only while the volume is writable, never on a read-only one.

Each node checks these on every scan and reports `indexing: off | on |
unknown` per volume. `on` is a warning, with the command that fixes it. A
read-only volume that was never given them stays `unknown`. The host module
excludes it from Spotlight on the host side instead: a privacy exclusion by
mount point, kept in the host's own Spotlight configuration, with no write to
the volume. The deployment's host module may also exclude store volumes from
Time Machine.

### 6. Moving a volume between hosts

1. **Unplug it from host A.** A's node stops reporting it, and it becomes
   **offline, known**. Its days still count as held, so purchases go on.
2. **Plug it into host B.** B's node finds the volume, by marker or
   filesystem UUID, and claims its `id` (§2).
   - The coordinator checks the epoch (§1) and moves the registry entry to B.
   - It records `moved {id} A → B` in `/events`.
   - B scans the volume fully against the snapshot (§3).
3. **Reads redirect to B.** Nothing in a client changes, because
   `/files/{volume}/…` names the volume, not the host.

A volume found on a host other than its **expected host** (§7) is used, and
reported as `unexpected_host` (degraded, not refused). That way an emergency
move works at once, and the inventory is corrected afterwards. If A's node
still reports the volume when B claims it, both are quarantined (§1).

### 7. The deployment names volumes, not paths

In a deployment's inventory, a store data root becomes:

```nix
{ volume = "<id>"; name = "data"; expectedHost = "<host>"; mount = "ro"; }
```

- `path` goes: the node finds the volume itself.
- The data-roots check (that a container may only mount the roots it is
  granted) uses `expectedHost` and the mount point the node reports.
- The host module writes the fstab entries from `mount`.

**Every node must be reachable from the clients that read from it**, once
reads redirect: 0005's "one address" becomes one address to ask, plus the
nodes it points to. A client that can reach only the coordinator uses
`--proxy-reads`.

### 8. The store checks what a batch says it belongs to

Today the store files a conditions batch like any other. It never reads
`binding.json` (`tinytrader.alpaca-history.conditions-binding/1`), which names
the main file it belongs to by name, size and SHA-256. So a client that
rebuilds a day's conditions after its main was filed can attach them to a
main they don't describe, if the provider's data changed in between.

- **On `incorporate`, a batch that carries a binding is checked** against the
  main it names, on whichever volume holds it:
  - online: from the volume's manifest;
  - offline: from the snapshot's per-file hashes (§3).
- **A mismatch is refused with `409`**, naming both hashes, and nothing is
  filed. A binding whose main isn't held is refused the same way. The rule
  lives with the data, not in each client.
- **Clients still check first.** alpaca-history checks the binding before it
  files conditions alone, so the person deciding sees the problem as a failed
  day rather than as a refused filing.

### Migration

From today's state (a state file keyed by name, markers without `id`, and
volumes named only in `STORE_VOLUMES` without a marker), with no rescan of
the data.

1. **A dry run first.** `store migrate --dry-run` lists, for each remembered
   or configured volume, what the migration would do: write a marker, track
   it in the registry only, or skip it, and why. Nothing is written.
   `store migrate --apply VOLUME…` then acts on the volumes named, and only
   those.
   - **`--apply` is one exclusive write.** It takes the filer's turn (0030):
     filing stops, no turn is granted to anything else, and the scan doesn't
     settle merges until the migration has written every marker and the
     state file. A store that crashes part way restarts with the old state
     file and the markers written so far, and `--apply` can be run again.
   - **`STORE_SEALED` paths** (step 0) stay sealed throughout, and are carried
     into the new markers.
2. **No marker is ever written to a volume that must not be written**, and
   `--apply` refuses one even when it is named:
   - one configured `:ro` or mounted read-only;
   - one an operator seals as a whole (`--immutable VOLUME`).

   Each of these gets its `id` in the registry only, bound to its
   `filesystem_uuid` and to its existing v0 marker's name and `approved_at`
   (§1). The write gate keeps it unwritable (§4), and naming it in
   `--apply` can't change that. The dry run lists, for every volume, which
   providers' data it holds, so an operator decides what to seal with that
   in front of them. Nothing is sealed because of what a volume holds.
3. **Working volumes** (writable, not sealed as a whole), once named in
   `--apply`, get the v1 marker, keeping their name. One is created for a
   `STORE_VOLUMES` volume that has none. The old marker is kept as
   `.tinytrader-volume.v0`. Each write goes through the write gate. With
   `--seal VOLUME:PATH`, the same step seals a path on it, such as the
   volume's purchased provider tree: the marker is written with that path in
   `sealed`, so from its first v1 write the tree is never written again,
   while the rest of the volume takes filings as before. From then on, until
   a working volume has its v1 marker, the migrated store refuses to write
   to it (§4), and the dry run says so: migrate the working volumes in the
   same `--apply` as the state file.
4. **The state file** is rekeyed by `id`, the old one kept beside it.
5. **Snapshots** are taken by the first full scan after the upgrade. Until a
   volume has one, an offline volume is **unknown**, and blocks purchases as
   it does today.
6. **Moving the store**: the coordinator moves to its new host with its state
   file. Each old host keeps a node until its drives move, and then its node
   is removed. The NAS joins as a node, or as a mounted share under an
   existing node. The inventory changes from host and path to volume IDs in
   the same step.

The deployment's own runbook (which volumes, which hosts, in what order) is
kept in the deployment repository, and starts from the dry run's output.

## Steps

Each step is its own PR, and each one is useful on its own:

0. **Seals by configuration, today.** `STORE_SEALED`, the one write gate as
   far as today's store needs it (every write path asks it), and the
   purchase guard refusing days under a seal. Tests: a filing, an adopt,
   `apply_conditions` and the scan's merge settling each refused under a
   sealed path; the rest of the volume still written; a purchase of a sealed
   day refused before it's submitted.

1. **Identity and the write gate.**
   - The v1 marker, `id`, `filesystem_uuid` and `epoch`, and the registry
     for volumes without a marker.
   - The state file keyed by `id`.
   - The one write gate, with its test over every write endpoint.
   - `migrate --dry-run` and `--apply`, and quarantine for clones and stale
     epochs.
   - Sealed paths, and placement refusing a filing under one.
   - `unseal`, recorded; a seal held when either the marker or the registry
     says so; every marker writer keeping `sealed` (tested).
   - Still one process. Tests:
     - migration from v0 state and markers;
     - a `:ro` or wholly sealed volume left unwritten, even when named;
     - a working volume holding purchased data migrated and still taking
       merges, with its sealed tree refusing them;
     - a store that hasn't migrated writing as before;
     - a clone and a stale epoch quarantined.
2. **Offline but known.**
   - Per-volume snapshots with file hashes, and filing intents recorded
     before each write and resolved after it.
   - `offline_volumes`, and `missing_volumes` narrowed to unknown volumes.
   - The offline-age warning, `lost` days on return, and `accept-loss`.
   - Rebuilding the state file from the volumes.
   - This alone ends "a moved drive stops every purchase". Tests:
     - a purchase proceeds with a known volume away, and is refused with an
       unknown one;
     - a day filed just before its volume goes away is still held;
     - a node that crashes between writing and reporting leaves its volume
       unknown until the volume's manifests are read again;
     - a volume that returns missing days doesn't make them purchasable
       until the loss is accepted.
3. **Volume hygiene.**
   - Indexing and change logs off, reported on each scan.
   - `seal`, and `mount` policy warnings.
   - The deployment module's fstab and `fileSystems` entries.
4. **The binding check on `incorporate`** (§8).
5. **Node and coordinator.**
   - Node approval and credentials, and `STORE_NODES`.
   - Merged catalogs, `307` reads, and placement with write tickets.
   - A day's writes kept on the volume that holds it, with days reserved at
     placement, and a batch spanning volumes refused.
   - Moves and conflicts recorded in `/events`.
   - Tests on two nodes on one machine (two mount directories): a move, a
     silent node, a stale clone claimed by a second node, a redirect, a
     ticket refused for another batch or node, and a day's merge sent to its
     volume.
6. **Placement rules and cross-host copies** (finishing 0007's copies).

## Consequences

- **A drive away is no longer an outage for purchases**, only for reading
  that drive's days, and every caller is told which volume and why.
- **Moving a drive is plugging it in.** Configuration catches up later.
- **Nothing is bought twice:**
  - a filing is recorded as an intent before it writes, and in the snapshot
    before it is acknowledged; an unresolved intent makes a volume unknown;
  - a returning volume's losses need an operator to accept them;
  - a lost state file makes volumes unknown, not empty.

  The cost is purchases blocked or skipped until someone acts, never a
  purchase made twice.
- **Immutability is enforced in code** by one gate, which reads the volume's
  own marker (whole-volume and path seals), with mount flags behind it. Once
  migrated, it refuses a volume without a v1 marker by default. What is
  sealed is an operator's choice, never inferred: a working volume that holds
  purchases stays writable, and its purchased tree can be sealed on its own.
  No registry change can make a sealed volume or path writable.
- **The coordinator is a single point**, as the one store is today. If it is
  down, reads stop, and so do new filings (§3), even though the nodes are up.
  A client may be given a node's address for reading that node's volumes,
  but no failover is designed here.
- **The `/files` contract is unchanged**, with a `307` in front of it. The
  write endpoints change (placement and tickets), but they are experimental
  and their clients are tinytrader's own.

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
- **How long `STORE_OFFLINE_WARN` should be by default.** 7 days is a guess.
