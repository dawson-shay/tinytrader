# 0036: Universes: "QQQ and all its constituents", as one request

- **Status:** Proposed
- **Date:** 2026-11-11
- **Builds on:** [0035](0035-content-addressed-store.md) (the index),
  [0034](0034-recoverability.md) (recoverability),
  [0037](0037-time-conventions.md) (time),
  [0019](0019-purchase-guards.md) (purchases),
  [0007](0007-storage-pool-and-automations.md) (protection classes)
- **Built after** 0035's core and 0034.

## Context

A research question ("QQQ and its constituents, 2022–2026") is today a long
manual list: the ETF, its index options, its futures, each member stock and
its options, by day, in several schemas, across providers. Membership changes
over time. Nobody can say at once what of it is held, what's missing, and
what completing it would cost. Research has prototypes of point-in-time
membership (`sp500`, `ndx100`, `russell2000`), built from the ETFs' SEC
N-PORT filings.

## Decision

### 1. A universe is reference data, with a version

A universe definition is a document the store keeps as reference data:

```json
{ "name": "QQQ", "version": 3, "state": "frozen", "frozen_at": "2026-12-15",
  "membership": { "source": "nport", "etf": "QQQ", "index": "Nasdaq-100",
                  "table": "membership/ndx100" },
  "instruments": {
    "etf": ["QQQ"],
    "index_options": ["NDX", "NDXP", "XND"],
    "futures": ["NQ"],
    "volatility": [],
    "members": { "stock": true, "options": true }
  },
  "schemas": {
    "options": ["tcbbo", "cbbo-1m", "definition", "statistics"],
    "futures": ["tbbo", "bbo-1m"],
    "stock": ["sip:trades", "sip:quotes", "sip:conditions"],
    "optional": { "cmbp-1": [] }
  } }
```

- **Membership is a point-in-time table** (a reference dataset in the store,
  with `known_at` on every change, 0037). It's built from the ETF's N-PORT
  filings (SPY → S&P 500, QQQ → Nasdaq-100, IWM → Russell 2000), as research's
  prototypes do. Each row is (date, member, weight), with its source and
  `known_at`. Reconciling against other sources (the prototype's
  `disagreements.csv`) is part of building it.
- **Instrument rules** say what a universe brings in: the ETF, its index's
  options (SPX/SPXW/XSP, NDX/NDXP/XND, RUT/RUTW), futures (ES, NQ, RTY), VIX
  options where the index has them (SPX), and each member's stock and options.
- **Schemas:**
  - options: `tcbbo`, `cbbo-1m`, `definition`, `statistics`;
  - futures: `tbbo`, `bbo-1m`;
  - stock: Alpaca SIP trades, quotes and conditions;
  - `cmbp-1`: opt-in, per instrument.
- **Drafts, then frozen versions.** A definition is a draft until the user
  freezes it, about a month after its last change. Then it's versioned:
  a change makes version n+1, and old versions stay resolvable, so a study
  names the version it used.

### 2. Resolve: from a universe to a manifest

`GET /universes/{name}/resolve?from=&to=&schemas=&version=` answers a
manifest, built in three expansions:

1. **Membership by day:** each member only on its member days, as of what was
   known each day (0037's `known_at`).
2. **Instruments by day:** the rules applied to each day's members. An option
   chain resolves to the parent symbol (`AAPL.OPT`), as the store files it.
3. **Logical names:** each instrument, schema and UTC day (0037) as the
   index's name.

Each name in the manifest carries:
- its `sha256` and copies if held (0035);
- its `status`:
  - `held` (with its copy count);
  - `in download center until <date>` (a Databento job not yet expired);
  - `free to request` (0034: within a free window or entitlement);
  - `paid at $X` (from a price quote, cached per request);
- totals: held, missing (GB), and the cost to complete.

The manifest is large, so it streams (NDJSON) with the totals at the end, and
it's cached by (name, version, from, to, schemas) for a day.

### 3. In Python

```python
u = tt.universe("QQQ", "2026", version=3)
u.status()                                 # totals by status, by schema
u.options("cbbo-1m").between(a, b).load()  # streamed via the index, hash-checked
u.members(on="2026-03-02")                 # who was in, as known then
```

The client reads through `/files` by name. Every file's hash is checked (0035
section 6), and a missing name fails the load naming it, unless
`allow_missing=True`, which reports the holes instead.

### 4. Fill: completing a universe

`POST /universes/{name}/fill` with the same range hands the missing names to
the planner:
- **free items** (Alpaca history, items inside a free window) are fetched
  automatically, as scheduled work (0033), to the volumes the protection
  class places them on;
- **paid items** become **one quote** needing the user's approval, with the
  purchase guards unchanged (0019): the quote, approval, the ledger.

### 5. The unit for protection and timing

- **Protection:** a universe can set a protection class (0007's copies) for
  everything it resolves to, so "QQQ, frozen v3, 2 copies" is one statement.
- **Money windows (0034):** the resolver knows each name's `free_until`, so
  "buy and back up before it ages out of the free year" is one list per
  universe: what to pull this month, and what that costs.

## Steps

1. **Membership tables** as store reference data: the three N-PORT-built
   tables from research's prototypes, reconciled and with `known_at`.
2. **Definitions:** draft, freeze and versions; `GET /universes`.
3. **Resolve**, with statuses (0034, 0035) and costs; streamed and cached.
4. **The Python API.**
5. **Fill**, through the planner, with paid items as one quote.

## Open questions

- **Whether member weights matter to resolution** (only the top N members, by
  weight). The recommendation is to keep the weights in the table, so a
  universe can take only the top N later.
- **Russell 2000 membership is large** (about 2,000 names), and its N-PORT
  filings are quarterly, so a rebalance's exact date comes from
  FTSE Russell's calendar. That calendar is reference data with its own
  `known_at`.
