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
  records, symbol mappings and markers every client gets. The filter is by
  rtype, in the hub, on the client's own queue. A client of several schemas
  gets a header with `schema` unset (mixed, as 0022 section 4 requires), and
  each record's rtype says what it is.
- **`cbbo-1m` ends each minute with Databento's end-of-interval marker**
  (`SystemMsg` "End of interval for cbbo-1m"). The relay passes it on, so a
  session can close that minute's step without waiting for the next minute's
  first record.
- **The lag alert** (approved): in market hours, the relay is `degraded`, and
  says so in `/events`, when it is more than 10 s behind (wall time minus the
  last record's `ts_recv`, for a schema that should be ticking), or when
  Databento skipped records (`upstream-gap`). It clears when caught up. The
  metrics `relay_lag_seconds{schema}` and `relay_gaps_total` back the
  console and the notifier.

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

- **The clock is record time,** not the wall. A step is an index time
  (`ts_recv`), as in a backtest, and it is served once it is closed:
  - by the next record at a later time;
  - by `cbbo-1m`'s end-of-interval marker, for that minute;
  - or by a relay heartbeat that says nothing more is coming.

  So a step holds exactly the records a backtest would put in it.
- **Timers fire in record time,** as in a backtest: a timer due at `t`
  fires in the step at `t`, or at the first step after it. In a quiet market
  that is the next heartbeat; the heartbeat's own time comes from the
  relay's clock, recorded like any record.
- **Lockstep, unchanged:** `step` (the records, the timers due, and the
  venue's last report sequence) → `ready` → the next step. The session
  can't make the market wait, so steps queue while the strategy works:
  - **The step queue is bounded:** `max_pending_steps`, 0009's `Lossless`.
    A strategy that falls that far behind ends the run, naming the limit,
    rather than see a different market than the backtest would have shown
    it. No step is coalesced or dropped silently.
  - **The deadline is the same `ready_timeout_s`.**
- **The venue is the account's real gateway** (paper, or live with the
  user's explicit OK), reached as a strategy reaches it today, over its
  socket and with its admission rules. The session doesn't advance it: it
  runs on the wall clock. Each step carries the gateway's last report
  sequence as the session saw it when it served the step, from a read-only
  observer connection. A strategy that reads its events up to that number
  has every report the session knew of, which is 0022's rule, on a live
  account.
- **Everything delivered is recorded:** the run's tape is the DBN records
  served, with heartbeats and markers, in order, written as 0013's frames.
  The control messages each way and the gateway's journal events for the
  run complete it. This is the run record's input, and the parity test's.
- **`READY` says `mode=paper`** (or `live`). The strategy reads its addresses
  from it as always, and nothing in the strategy names a mode.

### 3. Parity, tested

- **Record and replay:** a session in `paper` mode records its tape. The
  same tape, as a `backtest` source, with the same strategy, must give:
  - the same steps, by an audit digest of the step and record lines;
  - the same `ready`s: timers, and the order messages counted;
  - the same order intents sent, by request, in the same steps.

  What may differ is only what the venue did: fills on a real paper account
  aren't the simulator's. The test compares the digests of what the strategy
  saw and sent, and states that.
- **CI** runs it with a fake relay serving a recorded day: the live path end
  to end, with no network.
- **A paper run's record** carries its tape's hash, so any paper run can be
  replayed later as a backtest and checked the same way.

### 4. smallbook as a session client

smallbook's live script stops polling. It runs as the session's strategy in
`paper` mode, with the same entry point as in its backtests. That change is
the deployment's (tinytrader-ops), once steps 1–3 are in.

## Steps

1. **Relay:** `RELAY_SCHEMAS`, `schemas` in the join, per-client rtype
   filtering, the mixed header, the end-of-interval marker passed on, and
   the lag alert. Tests:
   - an existing client's stream byte for byte;
   - a two-schema client's records;
   - an unknown schema refused;
   - lag and gap raising and clearing `degraded`.
2. **Session `paper` mode:**
   - relay sources and record-time steps;
   - timers in record time;
   - the bounded step queue;
   - the real gateway with an observer for report sequences;
   - the tape and the run record.

   Tests against a fake relay and a sim gateway in wall-clock mode.
3. **The parity test:** record, then replay as a backtest, with identical
   digests.
4. **`live` mode:** the same, against a live account, refused unless the run
   config and the user say so. No different code path.
5. **(tinytrader-ops)** smallbook on the session, replacing its polling.

## Consequences

- **One strategy process from research to paper to live,** reading the same
  schemas. A live result can be replayed and compared with what a backtest
  of the same tape says.
- **A slow strategy ends a live run** rather than trading on a market it
  didn't see. Its limit is stated, in the config and the run record.
- **The relay carries more:** `cbbo-1m` is one record per contract a minute,
  and `tcbbo` one per trade, both far less than `cmbp-1` (to be measured on a
  real day in step 1). Clients that don't ask for them see nothing new.

## Open questions

- **Databento's live billing for the extra schemas** on OPRA: whether
  `cbbo-1m` and `tcbbo` subscriptions add to the live cost beyond `cmbp-1`.
  The user decides before step 1 is deployed.
- **A heartbeat's time,** in a quiet market: it comes from the relay's
  clock, so replaying a tape reproduces it, but it isn't a market time. The
  recommendation is to use it only to close steps and fire timers, and never
  to deliver it to the strategy as a record of the market.
