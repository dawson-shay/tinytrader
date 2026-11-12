# Store API

| | |
|---|---|
| **Purpose** | The one process that touches the data disks ([decision 0005](../decisions/0005-store-service-and-supervision.md)): a catalog of historic DBN, files served by logical name, and verified uploads filed into a standard layout. |
| **Transport** | HTTP/1.1 on `STORE_LISTEN` (default `127.0.0.1:9120`). JSON, except file bodies. |
| **Versioning** | Unversioned paths; changes are additive (new fields, new endpoints). |
| **Auth** | Reads are open. Writes need `Authorization: Bearer <token>` when `STORE_WRITE_TOKEN` (or `STORE_WRITE_TOKEN_FILE`) is set, and are refused from a web page of another origin ([write token](#write-token)). |
| **Errors** | `{"error": "..."}` with the HTTP status (`400`, `401`, `403`, `404`, `409`, `422`, `500`, `503`, `507`). |
| **Writes** | Filing, importing, adopting and applying conditions run one at a time, in the order asked (decision 0030: one filer owns them). Its queue holds 64; a write asked past that is answered `503 {"error": "busy: the store's filing queue is full; try again"}` and nothing is done: retry it. If the filer itself fails, the store logs why and exits (status 70), for its supervisor to start it again (launchd's `KeepAlive` on macOS, systemd on Linux), which redoes its scan and recovery from disk; a write asked meanwhile is answered `503 {"error": "the filer stopped; the store is restarting"}`. |
| **Health** | `/health`, `/metrics`, `/describe`, `/events` ([observe](observe.md)); `degraded` when a volume is missing or past its critical threshold. |
| **Stability** | `/catalog`, `/files` and `/files/{volume}/{path}` are **stable**; the write endpoints are **experimental** (their clients are tinytrader's own services). |

Shared conventions are in the [API overview](README.md).

| Endpoint | | |
|---|---|---|
| `GET /catalog?fresh=` | read | Each volume's datasets by provider, their schemas and days, and how full it is. Each schema lists its `partial` and `unfiled` days ([below](#what-the-store-holds)). |
| `GET /coverage?provider=&dataset=&symbol=&schema=&start=&end=&fresh=` | read | Each day held: `complete`, or `partial` with its contracts and time window, and the files that hold it ([below](#partial-days)). |
| `GET /files` | read | DBN files the last scan found, by logical name (below). |
| `GET /files/{volume}/{path}` | read | One DBN file, as it is on the volume. |
| `GET /incoming` | read | Batches uploaded and waiting to be filed. |
| `GET /batches/{provider}` | read | Which batches the volumes hold, and how. |
| `PUT /incoming/{provider}/{batch}/{file}?size=&sha256=` | write | Upload one file of a batch, verified on arrival. |
| `POST /incorporate/{provider}/{batch}?parent=` | write | File a complete batch into the standard layout; a parent's whole chain supersedes partial days of it ([below](#a-complete-day-supersedes-a-partial-day)). With `parent` (such as `SPY.OPT`), a batch bought as a list of that parent's contracts is filed as partial days, as an import. Besides DBN's own schemas, it files tinytrader's extension schemas `conditions` and `quote-conditions` ([datasets](../datasets.md)), whose files' headers name no schema. |
| `POST /import/{provider}/{id}` | write | File DBN that didn't come from a provider batch, uploaded with an `import.json` ([below](#partial-days)). |
| `POST /adopt/{provider}` | write | File a download folder already on a volume, where it is. |
| `POST /volumes/approve` | write | Approve a drive found in the mount directory. |
| `POST /volumes/{name}/retire` | write | Forget a remembered volume that is away for good ([below](#remembered-volumes)). |
| `POST /conditions/{provider}` | write | Record days' conditions from the provider's API ([below](#day-conditions)). |
| `POST /conditions/backfill` | write | Record the conditions in the `condition.json` receipts already kept. |
| `POST /volumes/{name}/ignore`, `DELETE` | write | Skip, or stop skipping, a path on a volume that can't be read ([below](#post-volumesnameignore)). |
| `POST /volumes/{name}/accept-empty` | write | Accept that a mounted volume which held datasets is now empty ([below](#post-volumesnameaccept-empty)). |

Writes are refused from a web page of another origin.

## Write token

When the store is given a token (`STORE_WRITE_TOKEN`, or better
`STORE_WRITE_TOKEN_FILE` naming a file that holds it, as a systemd credential
or a decrypted secret provides), every write must carry it:

```text
Authorization: Bearer <token>
```

A write without it, or with another, is refused with `401`. Reads stay open:
limit who can reach the store at all with the network (loopback, or a tailnet
policy). With no token configured, writes are open as before, so a deployment
can roll the token out client by client. A token that is configured but
can't be read, or is empty, stops the store from starting.

The clients that write send it when they are given it:

| Client | Writes | Its token |
|---|---|---|
| `databento-jobs` | uploads, filing | `JOBS_STORE_TOKEN` (or `_FILE`) |
| `alpaca-history` | uploads, filing | `HISTORY_STORE_TOKEN` (or `_FILE`) |
| `console` | approving a drive (Storage) | `CONSOLE_STORE_TOKEN` (or `_FILE`) |

`POST /adopt` has no client; send the header by hand
(`curl -H "Authorization: Bearer $(cat token-file)" …`).

Bind the store beyond loopback (`STORE_LISTEN`, or the module's
`observe.bind`) only with a token configured.

## What the store holds

Purchases are decided from `/catalog` and `/coverage` (a day held is not
bought again), so they say what the store holds or say they can't
([decision 0019](../decisions/0019-purchase-guards.md)):

- Both answer `503 {"error": "the store is still scanning"}` until the first
  scan after the store starts is done.
- Both answer from the last scan, which may be minutes old. With `fresh=1`
  they ask for a rescan and answer from a scan that started after the
  request, so data copied onto a drive behind the store's back counts.
  Requests meanwhile share that scan. A full scan of the two drives takes
  about three minutes, so it waits at most 10 minutes (a scan under way, then
  its own), then answers `503`. It is a read: no token. **Purchases ask with `fresh=1`.**
- Both carry `missing_volumes`: the remembered volumes that aren't there now
  ([below](#remembered-volumes)). In `/catalog` a volume whose provider or
  dataset folders couldn't all be read, or that has a malformed manifest, is
  missing too, with the reasons in its root's `errors`. `/coverage` answers
  `500` instead. An empty list means every volume was read.
- Every dataset has a `provider`, the store's name for the volume's top-level
  folder it sits in, case ignored (`Databento/` and `databento/` are both
  `databento`; `Alpaca/` is `alpaca`). Data outside every provider's folder
  (a batch download at the top of a volume, a backup, `tmp`, a crypto
  recording) has no provider (`null`) in the catalog, with the dataset,
  schema and symbols its files say, and counts as Databento's in
  `/coverage`, so a copy of Databento's data anywhere blocks buying it again,
  while a recording of another dataset never matches. Datasets are listed by
  provider, id and symbol, never merged across providers: each vendor's ids
  stay its own.
- `filed` is true when every day of the dataset is in the store layout.
  Days in a batch download, or in the older layout of a folder per day, are
  listed in their schema's `unfiled`.
- A day is held only when a file the store serves holds data for it
  ([Reading files](#reading-files)). An empty day file (padding) holds
  nothing, nor does a day folder of the older layout holding only receipts,
  an `.archive`, its definitions (a `definition.*` sidecar, in a schema other
  than `definition`) or empty files, such as a weekend's shell. An empty file
  in a batch download, or anywhere else, holds no day either.
- `stray` is true for a dataset held only in DBN files outside every
  provider's folder and every layout (scratch output, a shadow run's, a
  recording of another program's). They are counted, as every DBN file is,
  but aren't a dataset the store keeps: a view of the datasets can leave
  them out.
- A batch file named for a range of days (`…-20261001-20261031.cbbo-1m…`)
  counts every day of it, both ends included, its bytes shared evenly among
  them (the remainder on the first), as are those of any file holding
  several days. When a batch is split by
  symbol, or a file's name doesn't say its days, the batch counts every day of
  its query (`start` and `end` in `metadata.json`, the end excluded, as
  Databento means it: an end of `2026-10-10` or `2026-10-10T00:00:00Z` stops
  at 2026-10-09); without them it is an error.
- No DBN file on a volume goes uncounted. Every `.dbn` or `.dbn.zst` file
  the layouts above don't account for (named some other way, deeper than the
  layouts are read, loose) is counted from its own header (dataset, schema,
  symbols, start and end), under the provider folder it is in, if any, as not
  filed. A file of a list of contracts (`stype_in
  raw_symbol`) holds those contracts, partially; otherwise the header's
  symbols, whole. A header that names no symbols holds every symbol in
  `/coverage`; the catalog lists it under the symbol its folder names
  (`Alpaca/UVXY/2026-05-29.spot.trades.dbn.zst` under `UVXY`), when that
  folder is below the top-level one and named as a symbol is. A header
  without its start or its end takes the day its file is named for
  (`2026-05-29.…`). Without such a name, a header with no end holds its
  start's day only: how far it reaches is unknown, and every day to its last
  change would be counted with nothing in it.
  Hidden folders, such as a provider's `.incoming`, are
  skipped. Headers are read once and kept while a file's size and last change
  stay the same. A file whose header can't be read yet and that changed in
  the last two minutes is taken as still being written: it is listed in its
  root's `pending_files` and read again on the next scan.
- A DBN file that holds no dataset, as another program writes them, is
  counted as space used, never as data held: one whose header's dataset
  isn't a dataset id (`VENUE.FEED`, in either case; such as `bandit-chain`),
  or whose name marks it an index (`2026-07-01.poll.idx.dbn`). Each root
  lists them in `other` by folder, with the id in `dataset`:
  `{ "name": "Alpaca/UVXY", "dataset": "bandit-chain", "files": 43, "bytes": …, "modified": … }`.
  No vendor's id can be taken for one of them.
- A download folder with `metadata.json` but no `manifest.json` (one
  extracted by hand) counts by its files' headers, or, when it has no DBN
  files or has data in other encodings, by every day of its query. A folder
  with both and no data holds nothing.
- The store's own folders in a provider's folder, `provenance/` (receipts),
  `migrations/` (its ledger) and `superseded/` (partial days a complete day
  [took the place of](#a-complete-day-supersedes-a-partial-day)), hold no
  data and are never counted.
- A volume is missing, with the reason in its root's `errors`, when a folder
  on it can't be read; when a DBN file's header, a manifest or a batch's
  `metadata.json` can't be read; when its path must be a mount point and
  isn't (an empty folder left in `/Volumes` by a drive that wasn't unmounted
  cleanly); or when it held datasets when last read in full and now holds
  none ([accept it as empty](#post-volumesnameaccept-empty) if it really
  is).
- So is a volume where any file or folder can't be looked at for a reason
  other than its absence (an I/O error, a link loop), and one that went away
  or became another device during the scan (checked again when the scan
  ends). A scan with any error never changes what the volume is remembered to
  hold, so a drive dropping out mid-scan shows as missing, never as holding
  less.
- Each error names the path it is about, and how to
  [ignore](#post-volumesnameignore) that path if it should be.

### `GET /catalog`

```json
{ "generated_at": 1792500000.0, "missing_volumes": [],
  "roots": [{ "name": "data", "path": "/Volumes/Data", "present": true, "writable": true, "errors": [],
    "datasets": [{ "provider": "databento", "id": "OPRA.PILLAR", "symbol": "SPY.OPT", "filed": true, "stray": false,
      "sources": ["store: /Volumes/Data/Databento/OPRA.PILLAR"],
      "schemas": { "cbbo-1m": { "files": 2, "bytes": 1234, "symbols": [], "encodings": ["dbn.zst"],
        "days": { "2026-10-01": 600, "2026-10-02": 634 }, "partial": ["2026-10-02"], "unfiled": [], "degraded": [], "missing": [] } } }],
    "volume": { "…": "…" }, "dataset_bytes": 1234, "other": [], "other_bytes": 0, "unrecognised": [] }],
  "thresholds": { "…": "…" }, "candidates": [], "discover": "/Volumes" }
```

A remembered volume that isn't there (`"present": false`) has no
`datasets`; its root carries `held`, what it held when last read in full
(`{ "databento/OPRA.PILLAR/XSP.OPT": { "cbbo-1m": { "days": 518, "first": "2022-09-01", "last": "2026-10-01", "bytes": … } } }`,
keyed `provider/id/symbol` with `-` for none), so what it holds shows as
away rather than gone.

### Remembered volumes

The store keeps every volume it has used, configured or approved, name and
path, in its state file: `STORE_STATE` (or `--state`), by default
`~/Library/Application Support/tinytrader/store-state.json`. It is written
whole (beside the old one, flushed, then renamed over it), with what each
volume held when last read in full. A remembered volume that isn't mounted
when the store starts, or is unplugged later, stays in `/catalog` with
`"present": false` and in `missing_volumes`, so its days are never taken as
not held. An approved drive mounted under a new path is used there.

Approved drives, and volumes in `STORE_VOLUMES` under `/Volumes`, must be
mount points (on another device than the folder they sit in), and nothing is
written to one that isn't. Add `:anydir` to a `STORE_VOLUMES` entry
(`NAME=PATH:anydir`, with or before `:ro`) to take a plain folder.

#### `POST /volumes/{name}/retire`

For a volume that is away for good. It is forgotten, so it no longer counts
as missing, and the state file keeps a record of it under `retired`:

```json
{ "name": "archive8", "path": "/Volumes/Archive8", "retired_at": 1792500000.0,
  "held": { "databento/OPRA.PILLAR/SPY.OPT": { "cbbo-1m": { "days": 21, "first": "2026-09-01", "last": "2026-09-30", "bytes": 123456 } } } }
```

`held` is what the volume held when it was last read in full. `409` while the
volume is mounted, `404` for a volume the store doesn't know. A volume named
in `STORE_VOLUMES` comes back when the store restarts; take it out of the
configuration too.

#### `POST /volumes/{name}/ignore`

One unreadable folder, a bad `metadata.json` or an old truncated DBN file
makes its whole volume missing, which stops every purchase. When the path
holds nothing that should count (a broken copy, a folder of another
program's), an operator can say so:

```json
{ "path": "/Volumes/Data/backups/old.dbn.zst", "reason": "a truncated copy; the original is filed", "by": "dawson" }
```

`path` is absolute, or relative to the volume, and inside it; `reason` is
required. The state file keeps `{ volume, path, reason, by, at }` under
`ignored`, and each catalog root lists its own in `ignored`. From the rescan
that follows, the path, and everything under it, is skipped: never counted
and never an error. That is a decision to fail open for that path: what it
holds is taken as not held, and may be bought again. `400` for a path
outside the volume or no reason, `404` for an unknown volume, `409` if it is
already ignored.

`DELETE /volumes/{name}/ignore` with `{ "path": … }` stops skipping it
(`404` if it isn't ignored).

#### `POST /volumes/{name}/accept-empty`

A volume that held datasets when last read in full, and now holds none, is
missing: most often the wrong drive, or an empty folder, is mounted there.
When it really has been emptied, say so, optionally with who you are:

```json
{ "by": "dawson" }
```

It is accepted only while it is mounted and scans with no datasets and no
errors (`409` otherwise, `404` for a volume the store doesn't know). It is
then no longer missing, and the state file keeps a record under
`accepted_empty`:

```json
{ "name": "newdrive", "path": "/Volumes/NewDrive", "accepted_at": 1792500000.0, "by": "dawson",
  "held_before": { "databento/OPRA.PILLAR/SPY.OPT": { "cbbo-1m": { "days": 21, "first": "2026-09-01", "last": "2026-09-30", "bytes": 123456 } } } }
```

## Day conditions

A provider says of each day of a dataset whether it is `available`,
`degraded` or `missing` ([decision 0002](../decisions/0002-data-store.md#coverage-and-gaps)).
The store keeps that with each filed day, in its schema folder's
`manifest.json` entry:

```json
"2024-06-03": { "condition": { "state": "degraded", "last_modified": "2024-06-05", "source": "batch OPRA-20240605-X", "checked_at": 1792500000.0 } }
```

An entry holding only its condition is still a complete day. `/coverage`
rows carry the day's `condition` (`null` when unknown); each `/catalog`
schema lists its `degraded` and `missing` days.

- **Filing a batch** (`/incorporate`, with or without `parent`, and
  `/adopt`) records its `condition.json` row for each day it files (source
  `batch <id>`). The receipt is kept in `provenance/` as before.
- **`POST /conditions/{provider}`** with `[{ "dataset", "day", "state",
  "last_modified"? }]` records each row for every schema and symbol of that
  dataset held that day (source `provider api`): a provider's condition is of
  the whole dataset. It is how `databento-jobs` reports conditions for
  imported days, and re-checks, since the store has no provider key. A row
  that says what is already recorded changes nothing. It answers what
  changed:

  ```json
  { "changed": [{ "dataset": "OPRA.PILLAR", "day": "2024-06-03", "volume": "data", "folder": "/Volumes/Data/Databento/OPRA.PILLAR/SPY.OPT/cbbo-1m", "from": "degraded", "to": "available" }],
    "unchanged": 3, "not_held": [{ "dataset": "OPRA.PILLAR", "day": "2024-06-04" }] }
  ```

  `400` for a row without a dataset, a day, or a known state.
- **`POST /conditions/backfill`** records, once, the rows of every
  `provenance/*/condition.json` the volumes keep, each for the dataset its
  `metadata.json` (or `import.json`) names, as from its batch. It answers as
  above, with `batches`, `rows`, and `unknown_dataset` (receipts whose
  dataset isn't known).

Both apply to the days the last scan found, after any rescan a filing asked
for, and one at a time with filings.

## Reading files

A file is named logically, `<volume>/<path within the volume>`, where the
volume is a name from `STORE_VOLUMES` or an approved drive, never a mount
point:

```text
newdrive/Databento/OPRA.PILLAR/SPY.OPT/cmbp-1/2026-09-01.dbn.zst
```

Only finished DBN files are served: names ending `.dbn` or `.dbn.zst`, never a
`.partial` being written, nothing in a hidden folder (such as a provider's
`.incoming`), and nothing a link leads to outside the volume. Sealed files are
immutable, so a file once read can be kept.

### `GET /files?pattern=&provider=&limit=`

```json
{ "files": [{ "name": "newdrive/Databento/…/2026-09-01.dbn.zst", "volume": "newdrive", "path": "Databento/…/2026-09-01.dbn.zst", "bytes": 1234 }], "truncated": false,
  "scanned_at": 1792500000.0, "updated_at": 1792500100.0 }
```

- `pattern` (default `*`) is matched against the whole name: `*` any run of
  characters, `/` included, `?` any one character. `*cmbp-1*` finds every
  `cmbp-1` file in either layout.
- `provider` (optional) keeps only files in that provider's top-level
  folder, case ignored: `?pattern=*/OPRA.PILLAR/SPY.OPT/definition/*&provider=databento`.
- `limit` (default and most 100,000) caps the list; `truncated` says whether
  more matched.
- Sorted by volume, then path.
- From the last scan (`scanned_at`, seconds since the epoch; `updated_at`
  when a filing last updated it), which notes
  every served file on a volume, held or not (all but those in a path an
  operator [ignores](#post-volumesnameignore)): the volumes aren't walked on
  a request, so a listing takes as long as matching the names. A file the store
  files is listed at once (the folders a filing writes are read again in
  place, as for `/coverage`); one copied onto a drive behind the store's back
  is listed after the next scan (every `STORE_RESCAN_SECONDS`). Volumes that aren't there now are left out.
  `503 {"error": "the store is still scanning"}` until the first scan is
  done.

### `GET /files/{volume}/{path}`

The file's bytes (`application/octet-stream`, with its length), streamed.
`400` for a name that isn't served. `404` for an unknown volume, or a file
that isn't on a volume that is mounted and scanned. When the file may be
there but can't be read now, `503`, so a client can ask again:

- `volume newdrive is not mounted (…); ask again when it is`: the volume
  isn't there, or is an empty folder left where a drive was mounted.
  Nothing is served from it.
- `volume newdrive is missing (its errors are in /catalog); ask again when it
  is read`: the last scan found it missing, and the file wasn't found.
- `volume newdrive is being scanned; ask again shortly`: the file wasn't
  found, and the volume hasn't been scanned yet (the store just started, or
  the drive just appeared).

**On a content-addressed volume** (one holding `cas/`, decision 0035, as
`store migrate-cas` writes it), a name is read through the volume's index:
its object, sent with its hash. If that copy can't be read (its object gone,
the wrong size, or the volume away), the same object is read from another
content-addressed volume that's mounted (failover; the store logs which).
With no copy readable, it's `503` (`no copy of … can be read now`), never
`404`. These volumes' DBN names are listed by `GET /files` like any other,
and the catalog's root for each says `cas: {format, volume, names, pending,
conflicts}`; an index that can't be read is an error of the volume.
`/coverage` and filing don't use the index yet (0035 step 3).

**Its hash, as filed:** `X-Content-SHA256: <hex>` and
`Digest: sha-256=<base64>` give the SHA-256 the store recorded when it filed
the file, so a reader checks what it got. The store never hashes on a read.
The hash comes from every source that recorded it:

- the day's manifest entry (partial days, and complete days that superseded
  them; there, the size must match the file too);
- the filing ledgers' `moved` and `swapped` steps
  (`<provider>/migrations/*.events.jsonl`), which cover every day filed from
  a batch;
- the batch's receipt (the provider's own manifest, kept in
  `<provider>/provenance/<batch>/`), for a day the ledger says came from a
  batch file.

If the sources disagree, the file is sent **without** a hash, and an event
(`…: its recorded hashes disagree`) says once which sources said what. A
file nothing recorded is sent without one too. Every file migrated into the
content-addressed store (0035) will have one. `tinytrader-producer` checks it
as it reads, and fails the read on a mismatch.

Each file sent is logged on the store's standard error with its size and
how long it took: `store: sent newdrive/…/2026-09-01.dbn.zst: 1234 bytes in
0.012s (found in 0.001s)`.

Each request reads one whole file. Ranges of records, bulk reads across many
days, and running work beside the data are later steps.

## Partial days

A day file holding a list of contracts (`stype_in = raw_symbol`), for part
of a day, is a **partial day** ([decision 0018](../decisions/0018-partial-coverage.md)).
Its coverage is in the schema folder's manifest; a day whose entry has no
`coverage` (only its [condition](#day-conditions)), or that has no entry, is
complete.

**Coverage is pieces:** each a list of contracts over its own window (ns,
end excluded), one per file that went into the day, those with the same
window combined:

```json
{ "kind": "contracts", "stype": "raw_symbol",
  "pieces": [ { "symbols": ["SPY   261002C00728000", "…"], "window": [1790946000000000000, 1790985600000000000] },
              { "symbols": ["SPY   261002P00650000", "…"], "window": [1790948400000000000, 1790971860000000000] } ],
  "symbols": ["…every contract…"], "window": [1790946000000000000, 1790985600000000000] }
```

`symbols` and `window` are there for readers from before pieces: every
contract over the span of all the windows. They are an approximation, and
claim hours some contracts don't have; read `pieces`. Coverage written before
pieces (`symbols` and `window` only) is read as one piece.

**The manifest** is `manifest.json`, a small index of the folder's days, and
`manifest.d/<day>.json`, each partial day's full entry (its file's size,
SHA-256 and records, its coverage, its source and condition). The index
keeps a stub for a partial day (`{ "sidecar", "partial": true, "condition"
}`) and a complete day's entry whole. A merge writes only its day's file, and
the index at most once per filing. An index from before, with partial days
inline, is read as it is, and moves them to their own files the first time
it is written (a merge left pending moves to its own file too).

**This is one-way.** After the first filing into a folder, a store from
before this layout reads that folder's index as complete days with no
coverage: don't roll the store back past it.

### `POST /import/{provider}/{id}`

Upload each day file, named `YYYY-MM-DD.dbn.zst`, and an `import.json`, with
`PUT /incoming/{provider}/{id}/{file}`, then file them:

```json
{
  "dataset": "OPRA.PILLAR", "schema": "cbbo-1m", "symbol": "SPY.OPT",
  "source": { "api": "timeseries.get_range", "tool": "research/cbbo_fetch.py", "commit": "61f984a", "fetched": "2026-10-08" },
  "files": [ { "filename": "2026-10-02.dbn.zst", "size": 1169420, "hash": "sha256:…" } ]
}
```

Each file is checked against its size and SHA-256 and against its own header
(dataset, schema, day). Every record must map to a listed contract of
`symbol` and fall inside the header's window. Then, for each day:

- **Not held:** filed as a partial day.
- **Held, byte-identical:** the upload is dropped as a duplicate.
- **Held partly, with other contracts, or the same contracts over other
  hours:** the two are merged, and the file's piece is added to the day's
  coverage. The merge streams: it reads both files a record at a time, in
  timestamp order (ties: the day held first, each in its own order), and
  writes as it goes, so memory doesn't grow with the files. The merged file is
  read back, each record sorted to its input (by its instrument, and, for a
  contract in both, by whose hours hold its time), and each input's records
  must all be there, in order (by count and a running SHA-256), the merged
  file's times must never go back, and its records must be the day's
  recorded count plus the file's. It is recorded as pending in
  `manifest.d/<day>.pending.json`, swapped in by rename, and the pending
  entry renamed over the day's.
- **Held complete, or held partly with any of the same contracts over the
  same hours:** `409`. Nothing is filed.
- **The day held isn't its record:** its size or SHA-256 differs from its
  manifest entry: `409`, nothing is merged into it.
- **One instrument ID, two contracts:** the day held and the file map the
  same instrument ID to different contracts, so their records couldn't be
  told apart: `409`.

A partial day that then holds every contract of the day's definitions (the
`definition` day file of the same parent, when the store holds it), each
over the whole day, becomes **complete**: its entry drops its coverage and
records `completed: { by, pieces, at }`.

Each file's coverage is recorded in its import's ledger (`verified`), so what
went into a merged day can be rebuilt.

**Filings are grouped** (decision 0024). The store files one batch at a
time. Imports and batches filed as partial days that wait for the filing
in progress are then filed together, up to 16 as one group (the rest in
the next), and each day the group
touches is written once: the day held and every file for it, merged in
timestamp order (ties: the day held, then the files in the order they were
asked), read back and checked input by input as above, its records the
day's recorded count plus every file's. Each filing still answers for
itself, as if filed alone after the ones asked before it: its checks run
against the days held and what those will file, and one refused (say, for
contracts held over the same hours, or an instrument ID that is another
contract in the day) is refused with the same reason, the others filed
without it. The day's entry names one `source`, the group's first merged
filing; every filing's ledger records the merge (`merged`, its `inputs`
the day held, its own file, then the group's others) and the swap. A group's merge is swapped in as one, so an interrupted one settles
to the day before the group or after all of it, and filing again finishes
each filing: one whose ledger merged its file into the day, and whose
coverage the day holds, is dropped as a duplicate. A filing runs to its
end even if the request asking for it is dropped.

### A complete day supersedes a partial day

A batch of a parent's whole chain (`/incorporate` without `parent`, its
`metadata.json` query `stype_in` `parent`, one symbol such as `SPY.OPT`)
files a day the store holds as a partial day of that parent, schema and
dataset in its place, when:

1. Its day file passes every check a day not held passes (the manifest's
   SHA-256, its own header), and its header is the parent's
   (`stype_in parent`).
2. It holds **every record of the partial day, and nothing else over its
   hours.** Restricted to the partial day's instruments, each over its
   contract's pieces, the new file's records must be the partial day's,
   contract by contract, in order: by count and a running SHA-256. The
   partial day must be the file its entry records (size, SHA-256, records),
   held on one volume, and that volume writable.

**A whole chain is filed day by day.** A day that can't be filed (one held
complete as a different file, a partial day it doesn't hold every record
of, naming the contract that differs, one on a read-only volume, a day
file that fails its own checks) is refused alone and changes nothing; the
others are filed. The answer lists each day under `files`, with `filed`
`new`, `superseded` or `already present`, and each day refused under
`refused` (`{ file, day, why }`). With any day refused it is a `409` (its
`error` says how many), the batch stays in incoming with its receipts
(copied to `provenance/` for the days filed), and filing it again after a
fix files what is left and lists the days already done as `already
present`. A problem with the batch itself (its receipts, its manifest)
refuses the whole batch. Other batches, and `/adopt`, are still filed
whole or not at all.

A batch whose receipts differ from those already in `provenance/<id>/` is
refused, with one exception: an earlier filing of the batch that began and
never completed (its ledger in `migrations/` has no `complete` step, and no
volume has its summary), interrupted by an I/O error, say. Its receipts are
then moved aside to `provenance/<id>.interrupted-<time>/`, never deleted,
the move is recorded in the new filing's ledger (`provenance-set-aside`),
and the batch is filed with its own receipts. Days that the interrupted
filing placed are checked as usual.

The swap is a merge's: the new file is staged on the partial day's volume
(copied there, and checked, if the batch arrived on another), the entry it
will have recorded in `manifest.d/<day>.pending.json`, the partial day
linked into `superseded/`, the new file renamed over the day's, and the
entry made current: a complete day's, with `records`, `source` (`batch/<id>`)
and `superseded: { path, sha256 }`. An interrupted one is settled by the
next scan (or filing) from the day file's SHA-256, to before or after.

**The partial day is kept, never deleted**, on its own volume:

```text
<Provider>/superseded/<batch>/<DATASET>/<symbol>/<schema>/<day>.dbn.zst   the file, as it was
<Provider>/superseded/<batch>/<DATASET>/<symbol>/<schema>/<day>.json      its manifest entry
```

The batch's ledger (copied to the partial day's volume too, with its
receipts) records `superseded`, before the swap: `old: { path, sha256,
kept, records, contracts }`, `new: { path, from, sha256, records }` and
`matched` (the partial day's records found), then `swapped`. Filing the
batch again, after a crash or after success, finds its day in place by its
SHA-256, and reports `superseded` from the day's entry.

### `POST /coverage/rebuild`

Once, for days merged before pieces, whose coverage claims every contract
over the span of all their files' hours. For each merged partial day it
finds what went into it from the import ledgers in `migrations/` (each file
moved to or merged into the day) and what each held: from the ledger, or
from a provider batch's receipt in `provenance/<id>/` (its contracts, and its
query's time range in the day). It answers:

```json
{ "changed": [{ "folder": "/Volumes/Data/Databento/OPRA.PILLAR/SPY.OPT/cbbo-1m", "day": "2024-06-03", "pieces_before": 1, "pieces": 2 }],
  "unchanged": 12,
  "not_rebuilt": [{ "folder": "…", "day": "2023-01-04", "why": "what research-2023 held isn't on record (no coverage in its ledger, no receipt)" }] }
```

A day whose inputs can't all be found, or whose inputs' contracts aren't the
contracts it holds, is left as it is, and listed.

As with batches, every check runs before anything moves, `import.json` is
kept in `provenance/{id}/`, and each step is recorded in `migrations/`.

### `GET /coverage`

`provider` (required, `400` without it: `databento` or `alpaca`), `dataset`,
`symbol` (the symbol level, such as `SPY.OPT`), `schema`, and `start` and
`end` (`YYYY-MM-DD`, inclusive):

```json
{ "days": [
  { "day": "2026-10-01", "volume": "data", "volumes": ["data"], "kind": "complete", "filed": true, "size": 1234, "conflict": false,
    "files": ["data/Databento/OPRA.PILLAR/SPY.OPT/cbbo-1m/2026-10-01.dbn.zst"],
    "condition": { "state": "degraded", "last_modified": "2026-10-03", "source": "batch OPRA-20261003-X", "checked_at": 1792500000.0 } },
  { "day": "2026-10-02", "volume": "data", "volumes": ["data"], "kind": "partial", "filed": true, "size": 4567,
    "records": 68851, "sha256": "…", "files": ["data/Databento/OPRA.PILLAR/SPY.OPT/cbbo-1m/2026-10-02.dbn.zst"],
    "coverage": { "kind": "contracts", "stype": "raw_symbol", "symbols": ["SPY   261002C00728000", "…"], "window": [1790946000000000000, 1790985600000000000] },
    "pending": false, "conflict": false, "condition": null }
], "missing_volumes": [], "scanned_at": 1792500000.0 }
```

Every day the catalog counts for that provider is there: days in the store
layout, complete or partial by the schema folder's manifest, and days in a
batch download or a day folder of the older layout, as `complete` with
`"filed": false`. A batch bought as a list of contracts (`stype_in
raw_symbol`) holds those of the symbol's contracts, for its whole day, as
`partial`. A dataset is matched ignoring case. Data whose symbols can't be
told apart (requested by instrument ID, or with no symbol type) is taken to
hold every symbol of its dataset, whole, for the days it covers.

A day held on several volumes, or in several forms, is one row. `volumes`
names them all. If any copy is complete the day is complete; otherwise its
coverage is the union of the copies' contracts and the span of their
windows, with no single `size`, `sha256` or `records`. `conflict` is true
when the copies differ. `filed` is true when any copy is in the store
layout.

`files` names the files that hold the day, as
[`GET /files/{volume}/{path}`](#get-filesvolumepath) serves them, so a
client needn't know each provider's layout:

| Where the day is | `files` |
|---|---|
| A day file of the store layout (Databento's) | `<volume>/<Provider>/<DATASET>/<symbol>/<schema>/<day>.dbn.zst` |
| A day folder of the older layout (Alpaca's) | Its DBN files, the data first (`…/<schema>/<day>/<schema>.dbn.zst`), then sidecars such as `…/<day>/definition.dbn.zst` |
| A batch download | The batch's DBN files holding that day of the schema; a file of a range, or of every day of the query, may hold other days too |
| Any other DBN file | That file, which may hold other days, schemas or symbols too |

A complete day held in several places names the files of the copy the row
describes; a partial day held in copies that differ names every copy's. A
batch download in CSV or JSON has no DBN files to name: its `files` is
empty.

`pending` is true while a merge into that day (or a supersede) is unfinished. Each scan (and
so the store's start) settles it, unless a filing is running (it may be
between recording its merge and swapping it in; the next scan does): if the day's file is the merged one (by its
SHA-256), the pending entry becomes current; if it is still the recorded
one, the merge is dropped; if it is neither, it is an error of the volume.
The catalog root lists unsettled merges in `pending_merges`.

An unreadable folder, a malformed manifest or a batch's unreadable
`metadata.json` is a `500`, never an answer with days left out.

`/coverage` answers from what the last scan found (`scanned_at`, seconds
since the epoch), and reads the manifests of the matching schema folders as
they are now, so a merge into a partial day shows at once. A filing, an
import, a merge or a day's condition updates the scan's answer for the
schema folders it wrote, in place, before it returns (`updated_at`, also on
`/catalog`): `/catalog` and `/coverage` show it at once, without scanning
every volume again. A filing that is refused, or stops partway, reads the
folders it would have written again too. A scan that was running meanwhile reads those folders
again before it answers. A change of what is ignored asks for a full rescan;
until it is done `/coverage` waits (at most 60 seconds, then `503`). With `fresh=1` it waits for a scan started after the
request ([above](#what-the-store-holds)).

## Migrating to the content-addressed format: `store migrate-cas`

A one-off tool (decision 0035 section 9), run by an operator with the
service stopped or against volumes it doesn't write. It reads today's volumes
(files at their names) and writes one volume in the content-addressed format
([the storage contract](../storage-contract.md)).

```sh
store migrate-cas --plan --from data=/mnt/data --from tt03=/mnt/tt03            # writes nothing
store migrate-cas --to /tank/nas-1 --id nas-1 --from data=/mnt/data --from tt03=/mnt/tt03
```

- **Everything is copied:**
  - every regular file on each source becomes an object, bound under its
    path within the volume;
  - that includes day files, receipts, ledgers, manifests, uploads not yet
    filed, superseded days and anything else;
  - only the operating system's own files are skipped, and counted. Links
    and special files are listed as errors.
- **Sources are only read.** Mount them read-only.
- **Checked against what was recorded:**
  - a file the store recorded a hash for (in its manifest, a filing ledger
    or a batch receipt, as for `/files`) must match what's read;
  - ledgers are matched from the provider's folder on, so a ledger written
    under `/Volumes/Data/...` counts when the drive is read at `/mnt/data`;
  - a file that doesn't match, whose records disagree, or whose size isn't
    its manifest's is listed under `damaged`. Its bytes are **kept as a
    pending object** (they may be the only copy), and it isn't bound.
- **Duplicates and conflicts:**
  - the same name on several sources with one content is one object and
    one binding;
  - different contents are a **conflict**: each content is kept as a
    pending object, none is bound, and the conflict is listed for an
    operator.
- **Resumable:**
  - each source gets a receipt in the destination
    (`cas/sources/<NAME>.json`), recording every file read (its size,
    modification time, SHA-256 and outcome) and how each run ended;
  - a file is skipped on a later run only when that same source path, at
    the same size and time, was hashed into an object still held;
  - anything else is read and hashed again. Another source's file of the
    same name and size with other bytes is a conflict, kept, never skipped.
- **An empty source is refused** (exit 2): a drive that isn't mounted
  shows as an empty folder, never as nothing to copy.
- **The report:** a JSON report goes to standard output, with:
  - counts by kind, and what's to copy;
  - what's already there, and what's bound;
  - bytes written, names without a recorded hash, conflicts, damage and
    errors;
  - after a copy, the destination's `verify`.

  Progress goes to standard error. The exit status is `0` when everything
  was copied and bound, `1` with conflicts, damage or errors, and `2` on bad
  arguments or an unwritable destination.
