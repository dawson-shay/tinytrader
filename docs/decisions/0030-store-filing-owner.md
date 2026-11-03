# 0030: The store's filing owner

- **Status:** Proposed
- **Date:** 2026-11-03
- **Builds on:** [0018](0018-partial-coverage.md) (partial coverage and the store's merge),
  [0024](0024-crate-layout.md) section 8 (filing throughput: R4a done, R4b
  deferred)

## Context

Every store write that files, imports, adopts or applies conditions takes one
global lock: `filing: Arc<tokio::sync::Mutex<()>>` in `services/store/src/main.rs`.
- **The writes that take it:** `file_alone` and `file_partial` (incorporate and
  import), `adopt`, and `apply_conditions`.
- **The scan:** it `try_lock`s it to settle merges.
- **A second lock:** partial-day filings wait in a `queue: Mutex<Vec<…>>`, which
  the lock's next holder files as one group (R4a).

That lock is the one-writer rule. The contributing guide now calls a global lock
over shared work a defect, and lists it as known debt. 0024 planned (c), a lock
per (dataset, symbol, schema, day) under a store-wide read-write lock, and
deferred it. On the 8 GB M2 with USB disks, parallel rewrites contend for the
same disks and memory, while R4a already removed most of the wait.

This decision replaces the lock with an owner, so filing can run in parallel
where the machine allows it, without a map of locks.

## Decision

### 1. One filer task owns write admission

A single task, the **filer**, decides which writes run and when. Every write is
a message to it, carrying:

- **its keys:** the (dataset, symbol, schema, day) day files it writes, from the
  target folders the handlers already compute; or **`Exclusive`** for a
  whole-store write (adopting, applying conditions, the scan's settling);
- **the work:** a blocking closure, as today;
- **where its answer goes:** a oneshot, as today.

The filer keeps what's running and what's waiting. Neither is shared, and no
lock guards either.

### 2. Scheduling

- **At most `writers` writes run at once** (`--filing-writers`, default 1), each
  on `spawn_blocking`. With 1 the store files exactly as today: one at a time,
  in arrival order.
- **Two writes with a key in common never run together.** A waiting write that
  conflicts with a running one, or with an earlier waiting one, waits, so no
  write overtakes an earlier write to the same day.
- **An `Exclusive` write** waits for everything running to finish, holds back
  everything after it, and runs alone.
- **Partial-day filings** (R4a's grouping) waiting for the same keys are filed
  as one group, up to `GROUP`, so each day is still written once. The grouping
  moves from the shared queue into the filer.
- **The scan's settling** asks the filer to settle `if_idle`. The filer settles
  only when nothing runs and nothing waits; otherwise the merge stays listed for
  the next scan, as today.

### 3. Bounded, with a policy

The filer's inbox is a bounded channel (capacity 64). A write request that finds
it full is answered `503` with `Retry-After`, and its HTTP client retries:
databento-jobs already retries a failed filing on its schedule. Waiting writes
are part of that bound. Nothing queues without limit.

### 4. Unchanged

- 0018's merge guarantees, per day file: every input's records in order, counts
  and running hashes, the ledger, and write-beside, read-back, pending, rename.
  So an interrupted write settles to before or after, never between.
- Crash recovery and the scan's settling of pending merges.
- Every HTTP endpoint and its answers, apart from the new `503`.

The store's other locks (`catalog`, `index`, and the counters) stay out of
scope: they're known debt, fixed when touched.

### 5. Parallelism is the deployment's choice

`writers > 1` pays off only where parallel rewrites don't contend for one
disk's bandwidth or one machine's memory: 0024's reason to defer. The default
stays 1. A deployment raises it, with ops, where the store's volumes and memory
allow, and reports the filing rate as R4a did.

## Steps

1. **The filer with `writers = 1`.** It replaces the `filing` mutex and the
   partial-filing queue, with no change in behaviour. The store's tests
   pass unchanged, including the lock-ordering ones, rewritten against the
   filer.
   - New tests: a full inbox answers 503; settling is skipped while a write
     runs or waits.
2. **`writers > 1`.** Writes to different days run together; writes to one day
   never do; an exclusive write runs alone and holds back later ones.
   - Tests for each, plus a crash injected into one of two concurrent writes,
     where each settles alone.
   - The filing rate, measured on a disposable store against R4a's figure.
3. **Enable it on a host** that suits it: a deployment change, with ops and the
   user.

## Consequences

- The store loses its global lock and its queue lock: write admission has one
  owner, reached by messages, as the contributing guide asks.
- Parallel filing becomes a setting, safe by construction (no two writes to one
  day), and off by default on today's hardware.
- A burst of write requests past the filer's inbox gets `503` rather than
  queueing without bound.
