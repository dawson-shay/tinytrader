# 0032: Live sessions on the relay, on the inputs a backtest reads

- **Status:** Proposed
- **Date:** 2026-11-04
- **Builds on:** [0022](0022-sessions.md) (sessions: `paper` and `live` modes,
  one clock, lockstep), [0029](0029-actor-transports.md) (transports),
  [0006](0006-market-data-relay.md) (the relay)
- **Amends:** the relay's join request (`tinytrader.relay/1`, additively)

## Context

0022 gives a session three modes, but only `backtest` is built. A strategy
researched in a session runs live some other way: smallbook, for example,
polls quotes in a script of its own. So the code that trades isn't the code
that was tested, and its inputs aren't either. The backtest reads `cbbo-1m`
and `tcbbo` from the store, while the live relay carries only `cmbp-1`.

The rule this decision serves is the user's: **backtest and live run
identical code on identical inputs.** That means:

- the same strategy process, speaking the same `tinytrader.session/1` and
  `tinytrader.relay/1`;
- the same schemas (`cbbo-1m`, `tcbbo`), from the same provider, decoded by
  the same code;
- the same steps: an instant's records together, then the venue's reports,
  then timers, in 0009's `(ts, class, seq)` order;
- a way to prove it: a live run recorded and replayed as a backtest gives the
  same steps and the same strategy answers.

## Decision

### 1. The relay carries the schemas a backtest reads

- **`RELAY_SCHEMAS`** (such as `cmbp-1,cbbo-1m,tcbbo`) lists the live schemas
  the relay subscribes to, in one Databento session, for its whole parent
  set. `RELAY_SCHEMA` stays as the one-schema spelling. The first schema is
  the default.
- **The join request names what it wants:** `"schemas": ["cbbo-1m",
  "tcbbo"]`. This is optional, and absent means the relay's default schema,
  so every existing client gets exactly today's stream. The field is
  additive: `tinytrader.relay/1` refuses unknown fields, and this one becomes
  known. A schema the relay doesn't carry is refused at join, naming the
  ones it does.
- **Each client gets only its schemas' records,** plus the reference
  records, symbol mappings and markers every client gets.
  - The filter is by rtype, applied once per distinct schema set in the hub,
    not once per client: clients asking for the same set share one filtered
    stream.
  - A client of several schemas gets a header with `schema` unset (mixed, as
    0022 section 4 requires), and each record's rtype says what it is.
- **`cbbo-1m`'s end-of-interval marker** is passed on. A session recognises
  it by its `SystemCode` (`EndOfInterval`), never by its text, and closes
  that minute's step without waiting for the next minute's first record.
- **A watermark:** the relay sends a heartbeat carrying the upstream
  `ts_recv` it has read up to, so a quiet market still tells a session how
  far time has gone. It is a watermark only, never a record of the market
  (section 2).
- **The lag alert** (approved): in market hours, by the venue calendar (not
  the weekday), the relay is `degraded`, and says so in `/events`, when it is
  more than 10 s behind (wall time minus the last record's `ts_recv`, for a
  schema that should be ticking), or when Databento skipped records
  (`upstream-gap`). It clears when caught up. The metrics
  `relay_lag_seconds{schema}` and `relay_gaps_total` back the console and the
  notifier. The stale-quote alert (#131) stands beside it.
- **Load, measured first:** step 1 measures `cbbo-1m`'s minute burst
  (records and bytes in the second after each minute) and `tcbbo`'s rate on
  the M2, the relay's wired host, over a real day. It sizes the client
  queues from that before anything relies on it.

### 2. A session in `paper` or `live` mode

The session's config gains a mode, and sources that are relays:

```toml
mode = "paper"                      # or "live", "backtest" (the default)
[[sources]]
name = "opra"
relay = "relay-opra-spy"            # found by capability, as clients do (0006)
schemas = ["cbbo-1m", "tcbbo"]
[[venues]]
name = "options"
gateway = "order-gateway-paper-1"   # the account's real gateway, not a simulator
```

- **The clock is the watermark,** not the wall. A step is an index time
  (`ts_recv`), as in a backtest. It is served once nothing earlier can still
  arrive: the watermark (the latest record's `ts_recv`, or a heartbeat's) has
  passed it, or `cbbo-1m`'s end-of-interval marker has closed that minute.
  A step therefore holds exactly the records a backtest would put in it.
- **Timers fire at their exact due time,** in every mode: a timer due at `t`
  is a step at `t` of its own (with no records, unless some are at `t`), served
  once the watermark passes `t`. Backtest, paper and live agree on it. A
  heartbeat moves the watermark; it is never delivered as a record, and never
  makes a step by itself.
- **Inputs are normalised,** so live and the store's files give the same
  steps:
  - **Definitions and symbol mappings.** Live, they arrive in-stream; from
    the store, they come from the day's `definition` files and the file
    header's mappings. Either way the session delivers them the same way: the
    day's mappings as `SymbolMappingMsg` records in the first step, then any
    change in the step it takes effect. A test compares the two for a
    recorded day.
  - **A canonical order within a step:** by `(ts_recv, schema rank, instrument
    id, the record's sequence)`, with `tcbbo` before `cbbo-1m` at the same
    instant (a trade, then the minute's quote), the same in every mode.
  - **Gaps are holes.** An `upstream-gap` marker, or the relay dropping the
    session as a slow client, becomes a 0022 hole: a gap message at its time,
    in the step and the run record. Nothing is filled or guessed.
  - **Intraday corrections.** Databento's historical files can differ from
    what was sent live, where the provider corrected records later. Step 3
    measures how often on recorded days, and the parity test reports it
    rather than failing on it (section 3).
- **Lockstep, unchanged:** `step` (the records, the timers due, and the
  venue's last report sequence) → `ready` → the next step. The session
  can't make the market wait, so steps queue while the strategy works:
  - **The step queue is bounded:** `max_pending_steps`, 0009's `Lossless`.
    A strategy that falls that far behind ends the run (section 4), naming
    the limit, rather than see a different market than the backtest would
    have shown it. No step is coalesced or dropped silently.
  - **The deadline is the same `ready_timeout_s`.**
- **The venue is the account's real gateway** (paper, or live), reached as a
  strategy reaches it today: over its socket and with its admission rules.
  KILL and the other safety controls stay at the gateway. The session doesn't
  advance the gateway, which runs on the wall clock.
  - **Report sequences:** each step carries the gateway's last report
    sequence as the session saw it when it served the step. The session reads
    it over an observer connection with the gateway's observe role and token,
    never a trading one.
  - **The strategy must not read past it:** within a step, a client reads its
    gateway events only up to that number, and leaves later ones for the next
    step. Reading ahead would make its decisions depend on wall timing. The
    Python client and `session::actor` enforce it.
- **Everything delivered is recorded:** the run's tape is the DBN records
  served, with the watermark heartbeats, markers and holes, in order, written
  as 0013's frames. The control messages each way, and the gateway's journal
  events for the run with the report sequence each step carried, complete it.
- **`READY` says `mode=paper`** (or `live`). The strategy reads its addresses
  from it as always, and nothing in the strategy names a mode.

### 3. Parity, tested

Two tests, each with its own question:

- **The same strategy decides the same things.** A paper run's tape is
  replayed as a backtest whose venue is a **recorded venue**. Instead of a
  simulator, it replays the run's own journaled gateway events, delivering
  each step exactly the events up to that step's recorded report sequence,
  and it checks the strategy's order messages against the journal. So the
  replayed strategy sees what it saw live, fills included, for the whole
  run, not only until the first fill. It must give:
  - the same steps, by an audit digest of the step and record lines;
  - the same `ready`s: timers, and the order messages counted;
  - the same order intents, by request, in the same steps.

  This is what "identical code on identical inputs" means for a strategy.
- **Live inputs are the store's inputs.** The run's tape, normalised as in
  section 2, is compared with the same day's files from the store, read the
  way a backtest reads them: records, by step, digest for digest. A
  difference is reported by step and instrument, and corrections
  (section 2) are listed as such. This answers whether a backtest of that
  day would have seen the same market.
- **CI** runs both with a fake relay serving a recorded day, and a fake
  gateway replaying recorded events: the live path end to end, with no
  network.
- **A paper run's record** carries its tape's hash and its journal's, so any
  run can be replayed and checked later.

### 4. When a live run stops, and between days

- **An aborted run leaves no orders working.** A strategy's gateway
  connection in `paper` or `live` mode must set `cancel_on_disconnect`. The
  session refuses a strategy whose welcome (as the observer sees the
  connection) shows it unset. When the run ends early (the strategy falls
  behind or times out, the session or the relay fails), the session closes
  its side, and the strategy's disconnect cancels what was working.
  - **The session never flattens positions.** It holds no admin role at the
    gateway. Open positions stay, the notifier says so (`run ended
    incomplete: N positions open on <account>`), and `run.json` lists them.
- **A crash mid-day** ends the run incomplete, with its tape and record up to
  the last whole step. Restarting is a new run, from the relay's live stream:
  there is no resume into the old run, since the market it would have seen
  has passed. The venue's positions carry over, as they do for any client.
- **A live run is one trading day.** It ends at the relay's close (by the
  venue calendar) or at the config's end time, whichever is first. Several
  days are several runs, each with its day's definitions and mappings.
- **`live` needs the user's OK each time.** A `live` run refuses to start
  unless its config names the live account and carries `live_ok = "<the
  user's name>, <date>"`. That, and the gateway's own live checks, are
  recorded in `run.json`. `paper` needs neither.

### 5. smallbook as a session client

smallbook's live script stops polling. It runs as the session's strategy in
`paper` mode, with the same entry point as in its backtests. That change is
the deployment's (tinytrader-ops), once steps 1–4 are in.

## Steps

1. **Relay:**
   - `RELAY_SCHEMAS`, `schemas` in the join, filtering once per schema set,
     and the mixed header;
   - the end-of-interval marker passed on, and watermark heartbeats;
   - the lag alert by the venue calendar;
   - measuring the burst on the M2.

   Tests:
   - an existing client's stream, byte for byte;
   - a two-schema client's records;
   - an unknown schema refused;
   - lag and gap raising and clearing `degraded`.
2. **Session `paper` mode:**
   - relay sources and the watermark clock, with timers at their due times;
   - the normalised inputs and the canonical order;
   - the bounded step queue;
   - the observer, cancel-on-disconnect required, and the abort path;
   - the tape and the run record.

   Tests against a fake relay and a fake gateway.
3. **The parity tests:** the recorded venue and the replay; the tape against
   the store's files; corrections measured.
4. **`live` mode:** the same code, with `live_ok`, against a live account.
5. **(tinytrader-ops)** smallbook on the session, replacing its polling.

## Consequences

- **One strategy process from research to paper to live,** reading the same
  schemas. A live run can be replayed and must decide the same things, and its
  market can be compared with the store's.
- **A slow strategy ends a live run** rather than trading on a market it
  didn't see. Its limit is stated, in the config and the run record. What it
  leaves open is said, never flattened behind its back.
- **The relay carries more:** `cbbo-1m` is one record per contract a minute,
  and `tcbbo` one per trade, both far less than `cmbp-1`. Step 1 measures them
  on the M2 before anything relies on them. Clients that don't ask for them
  see nothing new.

## Open questions

- **Databento's live billing for the extra schemas** on OPRA. Ops reports no
  separate charge seen for the same schemas on `/session` and `/replay`, and
  live licensing per dataset. The user confirms in the portal before step 1
  is deployed.
