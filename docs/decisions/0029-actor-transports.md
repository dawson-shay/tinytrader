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
    /// The actor's answer to the step: how many order messages it sent, and its
    /// timers. Waits until `deadline`.
    fn ready(&mut self, deadline: Instant) -> Result<Ready, ActorError>;
    /// The run's end: status and audit.
    fn end(&mut self, end: &End) -> Result<(), ActorError>;
}
```

`Step` holds `as_of`, the records as `RecordRef`s (borrowed, so the in-process
transport copies nothing), the timers due and the reports. `Ready` holds
`as_of`, `sent` and the timers. The session's loop becomes transport-agnostic:
`deliver → ready → advance the venues → record the audit`.

### 2. Orders cross the same venue boundary

Orders always reach a venue through its gateway's admission path: the caps, the
kill switch, the journal and the rate clock. No transport bypasses it.

- **TCP:** as today, the actor opens its own gateway connections.
- **In-process and stdin/stdout:** the actor hands its orders to the transport.
  The session submits them to the gateway through an in-process gateway client.
  That client sends the same `Input::Request`s a TCP connection would, into the
  same bounded inbox, so the admission code is one code path.

### 3. Three transports

| Transport | Market data and control | Orders |
|---|---|---|
| `tcp` | `tinytrader.relay/1` and `tinytrader.session/1`, unchanged | gateway sockets, unchanged |
| `inproc` | a Rust trait object, called with `&Step` | through the session's in-process gateway client |
| `stdio` | 0026's wire: DBN on stdin, NDJSON answers on stdout | NDJSON lines on stdout, forwarded by the session |

The existing wire protocols, the Python client and every run config keep
working. A session's `[strategy]` stays as sugar for one `tcp` or `stdio`
actor.

### 4. Bounded mailboxes, with a policy

Every queue between the session and an actor is bounded, and says what happens
when it's full (the contributing guide's rule):

- **A step's deliveries.** Lockstep is the backpressure: the session sends no
  step before the last one's `ready`. So the mailbox holds one step, and a step
  is bounded by `max_step_records`. Past that, the run stops, naming the limit,
  because dropping records would change the result.
- **Reports** (order events) **waiting for the next step** are bounded per actor.
  Past the bound the run stops, naming it; a dropped report would break
  determinism.
- **Orders from an in-process or stdio actor** go into the gateway's inbox. A
  full inbox answers `busy`, as for a TCP client; the actor's `sent` counts it,
  and so does the gateway's clock (as #114 does).
- **stdio pipes** are bounded by the OS. A writer blocked past the step deadline
  ends the run as a strategy timeout.

### 5. Determinism across transports

The audit hash covers the steps, the readys and the order events, never the
transport. A CI test runs one fixture strategy through all three transports
and requires identical audit hashes and identical `run.json`s, apart from the
transport fields.

### 6. Several actors, one rule

A run may declare several actors (`[[actors]]`: a name, a transport, and the
venues it trades). Each step goes to every actor, and the session waits for
every `ready`. **One rule keeps it deterministic: each venue has one trading
actor.**

Two actors submitting to one venue's gateway over TCP would race, and their
admission order would depend on timing. If actors must share a venue, their
orders go through the session (in-process or stdio), which submits them in the
actors' configured order. TCP actors sharing a venue are refused at config time.

## Steps

1. **Extract the trait and the TCP transport.** Move `serve` and `advance`
   behind `Transport`, with no behaviour change: the existing session tests and
   their audit hashes must be unchanged.
2. **The in-process transport,** the session's in-process gateway client, a Rust
   example strategy, and the identical-hash test, TCP against in-process.
3. **The stdio transport,** to 0026's wire, with `run_stdio()` in the Python
   client, and the identical-hash test across all three.
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
- TCP actors can't share a venue; that's refused rather than left
  nondeterministic.
