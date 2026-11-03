# 0029: Actor transports

- **Status:** Proposed
- **Date:** 2026-11-03
- **Builds on:** [0009](0009-trading-runtime.md) (the trading runtime: actors,
  virtual time, the `(ts, class, seq)` order), [0022](0022-sessions.md)
  (sessions, lockstep), [0026](0026-external-process-actors.md) (external-process
  actors)

## Context

Sessions (0022) are built. A session merges its sources in the 0009 order, runs
its simulated venues in process, and drives **one** strategy in lockstep:

1. `step{as_of}` and that instant's records;
2. the strategy's `ready{as_of, sent}`;
3. the venues advanced once their gateways have handled `sent` order messages.

But that lockstep is written into the session's TCP loop
(`services/session/src/run.rs`, `serve` and `advance`). The strategy can only be
a TCP client: market data over `tinytrader.relay/1`, control over
`tinytrader.session/1`, and orders straight to each venue's gateway over
`tinytrader.gateway/1`. So none of these is configuration today; each would be
a change to that loop:

- an in-process Rust actor (0009's executor);
- a process over stdin and stdout (0026's wire);
- several actors in one run.

Where an actor runs should be a deployment choice, with the same semantics
everywhere: the same order of deliveries, the same fills, the same audit hash.

## Decision

### 1. The lockstep behind one interface

The session drives every actor through one trait. The session owns the clock,
the merge and the venues; the transport only moves a step's inputs in and an
actor's answer out.

```rust
pub trait Transport {
    /// Starts the actor; its first timers (`ready{as_of: 0}`).
    fn hello(&mut self, run: &RunInfo) -> Result<Vec<Timer>, ActorError>;
    /// One step: its time, the records of that instant, the timers due and the
    /// reports (order events) since the last step.
    fn step(&mut self, step: &Step<'_>) -> Result<(), ActorError>;
    /// The actor's answer to the step, waiting until `deadline`.
    fn ready(&mut self, deadline: Instant) -> Result<Ready, ActorError>;
    /// The run's end: status and audit.
    fn end(&mut self, end: &End) -> Result<(), ActorError>;
}

pub enum ActorError {
    /// No `ready` by the step's deadline.
    Timeout,
    /// Something it sent breaks the protocol: a malformed or oversized answer line,
    /// a `ready` for another time, a `sent` that goes back.
    Protocol(String),
    /// The process exited, or the in-process actor panicked or returned an error.
    Exited(String),
    /// The transport itself failed (a pipe or socket error).
    Io(String),
}
```

- `Step` holds `as_of`, the records as `RecordRef`s, the timers due and the
  reports. The records are borrowed, so the in-process transport copies
  nothing.
- `Ready` holds `as_of`, the timers, and `sent`: for each venue, the order
  messages (submits and cancels, valid or not) the actor has sent there since
  it started. This is 0022's `ready.sent` (`{VENUE: COUNT}`, cumulative), and
  the venue's gateway counts the same messages, so a step's advance waits for
  exactly those.
- The session's loop becomes transport-agnostic: `deliver → ready → advance
  the venues → record the audit`.

### 2. Orders cross the same venue boundary

Orders always reach a venue through its gateway's admission path: the caps, the
kill switch, the journal and the rate clock. No transport bypasses it.

- **`session-socket`:** as today, the actor opens its own gateway connections.
- **`inproc` and `actor-wire`:** the actor hands its orders to the transport. The
  session submits them to the gateway through an in-process gateway client. That
  client sends the same `Input::Request`s a TCP connection would, into the same
  bounded inbox, so the admission code is one code path. Under a session's clock
  the gateway answers nothing `busy`: a request waits for room (as #114 does),
  since a timing-dependent refusal would change the result.

### 3. Three transports

| Transport | Market data and control | Orders |
|---|---|---|
| `session-socket` | `tinytrader.relay/1` and `tinytrader.session/1` over TCP, unchanged (0022) | gateway sockets, unchanged |
| `inproc` | a Rust trait object, called with `&Step` | through the session's in-process gateway client |
| `actor-wire` | 0026's wire: DBN in, NDJSON answers out, over a started process's stdin and stdout, or a Unix or TCP socket | NDJSON answer lines, forwarded by the session |

The existing wire protocols, the Python client and every run config keep
working. A session's `[strategy]` stays as sugar for one `session-socket` or
`actor-wire` actor.

**Threads:**
- **`actor-wire`:** a writer thread feeds the actor's input (a step's DBN) and a
  reader thread reads its answers, so a full pipe in one direction can't
  deadlock the other. An answer line has a stated maximum size, 64 KiB as the
  gateway's lines; a longer one is a `Protocol` error.
- **`inproc`:** the actor's `step` runs on its own thread. `ready(deadline)`
  waits for that thread's answer, and a step past the deadline is a `Timeout`.
  A thread can't be killed, so the run then ends and leaves the stuck thread
  behind; the actor's code runs in no other step.

### 4. Bounded mailboxes, in 0009's policies

Every queue between the session and an actor is bounded, with one of 0009's
mailbox policies (rule 6), or a stated behaviour when full:

- **A step's deliveries:** `Lossless { capacity: max_step_records }`. Lockstep
  is the backpressure: the session sends no step before the last one's `ready`,
  so the mailbox holds one step. Overflow fails the run, naming the limit.
- **Reports** (order events) **waiting for the next step:**
  `Lossless { capacity }` per actor. Overflow fails the run, as a dropped report
  would change what the actor knew.
- **0009's `LatestPer` and `Coalesce`** are for an in-process actor that
  declares them for its market data. What they drop is recorded in the audit
  and reaches the actor as `Dropped`, as 0009 has it. The audit stays
  deterministic, as the policy is applied per step, in virtual time.
- **Orders from an `inproc` or `actor-wire` actor** go into the gateway's
  bounded inbox. Under the session's clock they wait for room and are never
  `busy` (section 2).
- **`actor-wire`'s pipes and sockets** are bounded by the OS. A side blocked past
  the step's deadline is a `Timeout`.

### 5. Failure

Each actor declares what its failure does, as 0026 has it, and an
`ActorError` is never retried silently:
- **In a backtest,** any `ActorError` ends the run, marked incomplete, with the
  error and the actor named in `run.json`.
- **Live,** the default is the same: stop the run, and leave the venue
  boundary's safety rules (cancel-on-disconnect, the kill switch) to act.

### 6. Determinism across transports

The audit hash covers the steps, the readys and the order events, never the
transport. A CI test runs one fixture strategy through all three transports
and requires identical audit hashes and identical `run.json`s, apart from the
transport fields.

### 7. Several actors, one rule

A run may declare several actors (`[[actors]]`: a name, a transport, and the
venues it trades). Each step goes to every actor, and the session waits for
every `ready`. **One rule keeps it deterministic: each venue has one trading
actor.**

Two actors submitting to one venue's gateway over its socket would race, and
their admission order would depend on timing. If actors must share a venue,
their orders go through the session (`inproc` or `actor-wire`), which submits
them in the actors' configured order. `session-socket` actors sharing a venue
are refused at config time.

## Steps

1. **Extract the trait and the `session-socket` transport.** Move `serve` and
   `advance` behind `Transport`, with no behaviour change. First, in a commit of
   its own, check in golden audit hashes and `run.json`s recorded from `main`
   for the session test fixtures. The refactor must reproduce them exactly.
2. **The `inproc` transport,** the session's in-process gateway client, a Rust
   example strategy, and the identical-hash test, `session-socket` against
   `inproc`.
3. **The `actor-wire` transport,** to 0026's wire (stdio first), with
   `run_stdio()` in the Python client, and the identical-hash test across all
   three.
4. **Several actors:** `[[actors]]`, the venue rule, and actor-ordered
   submission for shared venues.
5. **The mailbox bounds** (`max_step_records`, the report bound) and their
   tests.

Each step is its own PR, reviewed before the next.

## Consequences

- An actor's language and where it runs are configuration: a Python strategy
  can run in a notebook, in a run over stdio, or as a TCP session client, with
  the same results.
- 0009's executor gets the session's merge, clock and venues unchanged; it is
  the in-process transport plus a scheduler for several actors.
- An in-process actor costs no serialization or round trip per step, so large
  runs are faster.
- The venue boundary stays single: every order, from any transport, passes the
  gateway's admission.
- `session-socket` actors can't share a venue; that's refused rather than left
  nondeterministic.
