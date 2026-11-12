# 0037: Time, stated once

- **Status:** Proposed
- **Date:** 2026-11-11
- **Collects:** what [0009](0009-trading-runtime.md),
  [0014](0014-history-feeds.md), [0022](0022-sessions.md) and
  [datasets.md](../datasets.md) each say, so that universes (0036), sessions
  and every service follow one statement rather than each its own.

## Decision

1. **Instants are UTC nanoseconds since the epoch**, everywhere: in records
   (`ts_event`, `ts_recv`), in the index, the run records, metrics and
   events. Nothing stores a local time. A time shown to a person is New York
   time, labelled.
2. **A day's file is a UTC day:** `00:00:00Z` to `23:59:59.999999999Z`, as
   Databento's are. A provider day file can therefore be a date that isn't a
   session (under EST, a full day's last after-hours hour is the next UTC
   date: 0014, datasets.md). A UTC day is a storage unit, not a trading day.
3. **A trading session is a New York day** of the venue's calendar: equities
   and options trade from 04:00 to 20:00 New York, with the regular session at
   09:30–16:00, or an early close the calendar names. `tinytrader_schedule`
   is the one place that maps a session to its UTC span, its regular hours and
   its early closes, and every service asks it.
4. **A futures trade date is CME's:** the session from 18:00 New York the day
   before through 17:00, named by the date it ends on. `tinytrader_schedule`
   maps it too (as the simulator already does, 0022). Weekend and holiday
   closes come from the venue's calendar.
5. **Mapping days:** "the data for session D" is the UTC days its span
   touches. `tinytrader_schedule` answers it (`session_utc_days(D)`). The
   store's resolvers (0036) and the session producer use it, and nothing
   computes it by hand.
6. **Every reference fact carries `known_at`:** the UTC instant it became
   known (a filing's acceptance time, a definition's `ts_recv`, a membership
   change's announcement). A point-in-time query as of `t` uses only facts
   with `known_at ≤ t`, so research can't see the future.
7. **Expiries and settlements** are named by the trading date they belong to
   (an option's expiration date, an AM or PM settlement), with the instant in
   UTC beside it. They're never encoded as a UTC midnight, as Databento's
   `expiration` field is (the gateway reads that field with this in mind,
   0016).

## Consequences

- One function answers "which UTC files hold this session" and "which
  session does this instant belong to", and is tested at the DST changes, the
  early closes, and the Friday-evening tail (#136).
- `known_at` becomes a required field of new reference data. Existing
  reference data gets it from its source where known, or from when it was
  filed, marked as such.

## Steps

1. `tinytrader_schedule`: `session_utc_days`, `cme_trade_date`, and tests at
   the DST and early-close edges.
2. `known_at` on the store's reference records, and as a point-in-time
   parameter where reference data is served.
3. datasets.md and the API docs refer here instead of restating.
