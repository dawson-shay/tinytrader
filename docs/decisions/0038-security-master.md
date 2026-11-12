# 0038: A security master: what every symbol is, as of when

- **Status:** Proposed
- **Date:** 2026-11-11
- **Builds on:** [0035](0035-content-addressed-store.md) (the CAS: reference
  snapshots are objects too), [0037](0037-time-conventions.md) (`known_at`,
  sessions, trade dates)
- **Under:** [0036](0036-universes.md) (universes resolve to `security_id`s)
- **Decided by the user** on 2026-11-11 (the shape, and "no database server
  for now"). This records how it's built.

## Context

Every service names instruments its own way: Databento instrument ids
(reassigned daily, as #131 found), OPRA roots, Alpaca symbols, CUSIPs in
N-PORT filings, CIKs in EDGAR. A rename (BK → BNY) looks like one company
leaving the S&P 500 and another joining. Options, futures, fundamentals and
events can't be tied to the thing they're about, as of a date, without
guessing. Research's prototypes (`sp500`, `ndx100`, `russell2000`,
`fundamentals`, `events`) each reconcile identifiers by hand.

## Decision

### 1. Five layers

| Layer | Is | Keys |
|---|---|---|
| **entity** | an issuer or underlying: a company, a fund, or an asset (BTC, WTI) | `entity_id`; CIK, LEI where they exist |
| **security** | a share class or instrument | `security_id`; CUSIP, ISIN, FIGI, by date range |
| **listing** | a security on a venue under a symbol, time-ranged | `listing_id`; venue, symbol, from, to |
| **derivative** | an option or future on a security, index or entity | `security_id` of its own, plus its underlying, expiry, strike, right, settlement (AM/PM, cash/physical), multiplier, and for futures the roll |
| **market** | a venue's calendar, sessions, currency, tick and lot, and settlement | ties to 0037 |

- **Internal ids are stable:** `entity_id`, `security_id` and `listing_id`
  never change. Everything else (symbols, CUSIPs, provider ids) maps onto
  them with date ranges.
- **A rename is a listing change** (BK's listing ends, BNY's begins, on the
  same security), not a membership change.
- **Corporate actions** (splits, dividends, spin-offs, mergers) attach to the
  security, with their effective and `known_at` dates.

### 2. Asset classes select a profile

The security's asset class selects a profile: its sources, schemas,
reference data and time rules (0037).

| Class | Built now | Sources | Schemas (0036) |
|---|---|---|---|
| US equity, ETF | yes | SEC (EDGAR), Alpaca (assets, corporate actions) | Alpaca SIP trades, quotes, conditions |
| index | yes | the sponsor's methodology, N-PORT (membership) | (levels where held) |
| equity or index option | yes | Databento OPRA definitions | `tcbbo`, `cbbo-1m`, `definition`, `statistics` |
| future, option on a future | yes | Databento CME definitions | `tbbo`, `bbo-1m` |
| international equity | sketch | a listing per venue, its own calendar and currency | |
| crypto spot, crypto perpetual | sketch | below | |
| commodity, FX | sketch | WTI → CL futures and their options; FX pairs as entities | |

**Crypto, sketched:** BTC is an entity. Its listings are Coinbase and Kraken
spot pairs, Kraken perpetuals (with funding as reference data), CME BTC
futures, and IBIT, an ETF security with its OPRA options, all linked to the
one entity.

**On-chain (DEX), sketched** from the user's earlier registry
(`bandit-schema-662470a8.sql`):
- **The entity is a token,** keyed by `(chain_id, contract address)`.
- **Its listings are pools** (factory and pool address). The venue's metadata
  is the factory: its protocol (V2, V3, Algebra), its fee, and its bytecode
  hash, which detects forks.
- **Bytecode and implementation hashes are identifiers** of the token and the
  pool (EIP-1967 proxies included), since they detect forks and upgrades.
- **Safety status changes over time,** written by concurrent scanners: honeypot
  checks on a forked chain, with their block. That is the operational case for
  a live database (section 4). The master keeps point-in-time snapshots of it,
  with `known_at` as the checked block's time, so "was this token flagged
  when we traded it" has no look-ahead.

### 3. Symbology: every provider's name, by date

One table maps each provider identifier, over a date range, to a
`security_id`. The identifiers are:
- Databento instrument ids (per day: they're reassigned daily);
- OPRA roots and option symbols;
- Alpaca symbols;
- CUSIP, ISIN and FIGI;
- CME product codes.

A lookup is `(provider, identifier, date) → security_id`. Two securities for
one identifier on one date is an error of the table, reported when it's
built, never resolved by guessing.

### 4. Storage: no database server, for now

- **Reference data is point-in-time tables:** each row has a validity window
  (`valid_from`, `valid_to`) and `known_at`. They're stored as **Parquet
  snapshots**, each one an object in the CAS (0035): versioned, hashed,
  verified by the sweep, and backed up like market data. They're queried with
  **DuckDB, embedded**. There's no database server to run, back up or
  upgrade.
- **Raw sources are kept as fetched:** EDGAR `companyfacts` and `submissions`
  JSON, N-PORT XML, econ-calendar pages, Alpaca's asset lists. Each is
  immutable with its provenance (URL, fetched_at, hash), as an object.
  - **Numeric facts are normalised** from them into a facts table:
    `(entity_id, concept, period, value, unit, filing, known_at)`.
  - The tables are rebuildable from the raw objects.
- **Market data stays DBN** in the CAS.
- **Research-derived tables** (bars, signed flow, features) are Parquet,
  rebuildable from the DBN, and never the source of truth.
- **When a server would be right:**
  - **Postgres** only where there are concurrent transactional writers. The
    DEX safety registry is that case, as an operational store whose
    snapshots feed the master.
  - **ClickHouse** (over Timescale, at our scale) only if research needs
    interactive SQL over raw ticks across thousands of names. Even then it
    would be loaded from the derived tables, never the source of truth.
- **The access layer is abstract** (`tt.security`, `tt.universe`, and the
  store's endpoints), so changing the engine later doesn't touch clients.

### 5. Ownership and attachments

- **The store owns ingestion and normalisation** into the master: from SEC,
  Alpaca's corporate actions and assets, Databento definitions, and exchange
  lists. That's scheduled work (0033), on its own schedule per source.
- **What attaches where:**
  - fundamentals and events attach to the entity, through its CIK;
  - options and futures attach to their underlying security (or index or
    entity);
  - corporate actions attach to the security.
- **Universes (0036) resolve to `security_id`s,** then to listings and
  symbols per provider for each day.

### 6. In Python

```python
s = tt.security("AAPL", asof="2024-06-03")
s.listing          # venue, symbol, from..to
s.ids              # CUSIP, ISIN, FIGI, CIK as of then
s.option_roots     # ["AAPL"] (and adjusted roots after a corporate action)
s.earnings()       # events known as of then
s.facts("Revenues")  # normalised facts known as of then
```

Every answer is as of `asof`, using only `known_at ≤ asof` (0037).

## Steps

1. **Tables and storage:**
   - entity, security, listing, derivative and market, plus the symbology
     table, as Parquet snapshots in the CAS, queried with DuckDB;
   - the raw-source objects with provenance.
2. **US equities, ETFs and indices:** from research's prototypes (`sp500`,
   `ndx100`, `russell2000`, `fundamentals`, `events`), reconciled against
   EDGAR and Alpaca, with every conflict listed.
3. **Options and futures:** from Databento definitions, with daily instrument
   ids into the symbology table.
4. **`tt.security`**, and universes resolving through the master (0036).
5. **Sketched, not built:** international equities, crypto, commodities and
   FX, and the on-chain profile.

## Open questions

- **CUSIP licensing:** CUSIPs are licensed identifiers. The store keeps those
  that come in public filings (N-PORT) as attributes of the filing, and uses
  FIGI as the open identifier where both exist. The user confirms that this
  is enough.
