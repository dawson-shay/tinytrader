# Market data relay (`tinytrader.relay/1`)

| | |
|---|---|
| **Purpose** | One connection to a market data provider, served to any number of local clients as DBN, with replay from a start time where the source keeps history. |
| **Transport** | The stream: TCP on `RELAY_LISTEN` (default `127.0.0.1:7600`), a one-line JSON join request, then DBN v3 records. HTTP: JSON on `RELAY_OBSERVE_LISTEN` (default `127.0.0.1:9110`). |
| **Versioning** | The join request names `"protocol": "tinytrader.relay/1"`; anything else is refused. Records are DBN v3, plus `tinytrader.trade-conditions/1` records when asked for. HTTP endpoints are unversioned (see [stability](#stability)). |
| **Auth** | None: bind to loopback or a private network. Access to a relay grants nothing at an order gateway. |
| **Errors** | Stream: one DBN `ErrorMsg`, then the relay closes. HTTP: `{"error": "..."}` with a 4xx or 5xx status. |
| **Health** | `/health`, `/metrics`, `/describe`, `/events` ([observe](observe.md)); `upstream-*` markers in the stream. |
| **Stability** | The stream protocol is **stable**. `/clients`, `/bars`, `/quotes`, `/stats` are **stable**; `/recent`, `/tape`, `/session` and `/replay` are **experimental**. |

Shared conventions are in the [API overview](README.md); how to write a client is in the [strategy client tutorial](../tutorials/strategy-client.md).

The relay forwards and keeps no recording ([decision 0006](../decisions/0006-market-data-relay.md)). Everything below the stream is held in memory and is gone after a restart; the one exception is `/replay`, which writes a replay file to disk when asked to.

A deployment runs one relay per feed. For example:

| Relay | Source (`RELAY_SOURCE`, `RELAY_DATASET`, `RELAY_SCHEMA`) | Carries (`RELAY_SYMBOLS`) | Stream port |
|---|---|---|---|
| an options relay | Databento `OPRA.PILLAR`, `cmbp-1` | one root's options, such as `SPY.OPT` (parent symbology) | 7600 |
| a stocks relay | Alpaca IEX, as `ALPACA.IEX` `mbp-1` | a few stocks | 7610 |

A relay says what it carries in `/describe` (`relay: {protocol, listen, symbols}`), so clients and the console find relays by that capability rather than by name.

Relays run during their hours (`RELAY_HOURS`, such as `09:25-16:20` New York on trading days); outside them the provider session is closed and clients get heartbeats only.

## The stream: `tinytrader.relay/1`

Connect over TCP and send one line of JSON:

```json
{"protocol": "tinytrader.relay/1", "client": "my-strategy", "account": "paper", "start": "2026-10-12T13:30:00Z", "conditions": false}
```

| Field | | |
|---|---|---|
| `protocol` | required | Exactly `tinytrader.relay/1`; another version is refused. |
| `client` | optional | Who is connecting, shown on the console: up to 64 letters, digits, `.`, `_`, `-` and spaces. `name` is accepted as another spelling. |
| `account` | optional | The account the client trades, by its deployment name, shown beside it (same character rules). |
| `start` | optional | RFC 3339. Replay from then, then continue live; absent means live from now. |

Unknown fields are refused. The line must arrive within 5 seconds and be at most 4 KiB. A refused join (bad JSON, another protocol, too many clients) gets a single DBN `ErrorMsg` record, without a metadata header, and the connection closes.
| `conditions` | optional | Interleave a `tinytrader.trade-conditions/1` record after each trade (IEX). |

The relay answers with a DBN v3 stream:

1. The metadata header.
2. Reference records: instrument definitions, and statistics where the source has them.
3. Catch-up from `start`, where the source supports it:
   - **IEX:** Alpaca's historical trades and quotes from `start`, merged by event time and encoded exactly like live data.
   - **OPRA:** none. Databento won't replay `cmbp-1` intraday, so the client is live from joining and the header's `start` says so. Today's quotes for one contract are available from [`/session`](#get-session-todays-replay-of-one-contract).
4. A `SystemMsg` with code `ReplayCompleted`.
5. Live records.

**In-stream markers** (`SystemMsg` text):

| Marker | Meaning |
|---|---|
| `upstream-down since=<ns>` | The provider connection dropped. |
| `upstream-up gap_from=<ns> gap_to=<ns>` | It's back; records between the two times were missed. |
| `upstream-gap gap_from=<ns> gap_to=<ns>` | The provider skipped records because the relay read too slowly. |
| Heartbeat (`SystemMsg` code) | Sent after `RELAY_HEARTBEAT` seconds without data. |

A client that falls behind is warned at half its queue (`SystemMsg` code `SlowReaderWarning`) and, when it falls `RELAY_CLIENT_QUEUE` chunks of up to 64 KiB behind (default 1024), sent `ErrorMsg` `SkippedRecordsAfterSlowReading` and disconnected, so it never slows the provider connection or other clients.

## HTTP

All answers are JSON. Times are Unix seconds unless noted; prices are in dollars.

### `GET /health`, `/metrics`, `/describe`

The common service endpoints: state (`ready`, `degraded`, ...), Prometheus metrics, and what the relay is (role, upstream, the streams listed here, its symbols and schedule).

### `GET /events?after=<n>&limit=<n>`

The relay's recent events (the last 500): provider up and down, gaps, schedule changes. Newest last; `after` is the last sequence number seen.

### `GET /recent`

The most active instruments' trade prices, sampled once a second, for the console's price panels.

### `GET /clients`

Who is connected: `{"clients": [{client, account, peer, since, replay, conditions}]}`: the name and account it gave, its address, when it joined, whether it asked to replay, and whether it takes trade conditions.

### `GET /bars?symbol=<symbol>&days=<1-10>`

One-minute bars, `{"symbol", "interval": 60, "bars": [[t, o, h, l, c, v], ...], "note"}`, `t` the minute's start.

- **The relay's own symbols** (such as `SPY` on the IEX relay): built from the trades it relays, with history before the relay saw them where the source keeps it (Alpaca's).
- **An option on an OPRA relay** (`symbol` in compact OCC, such as `SPY261012C00777000`): today's minutes of the contract's **mid**, from [`/session`](#get-session-todays-replay-of-one-contract)'s `cbbo-1m`. Each bar closes at the mid at the end of its minute and opens at the previous minute's, so high and low are the larger and smaller of the two; volume is 0. `days` is ignored.

### `GET /quotes`

The latest consolidated quote of each contract the relay carries, from the live `cmbp-1` (or IEX's `mbp-1`).

- `?symbols=SPY261016C00775000,...` for those contracts, or `?underlying=ROOT&from=YYYY-MM-DD&to=YYYY-MM-DD` for a root's contracts expiring in that range.
- `{"quotes": {OCC: {bid, ask, bid_size, ask_size, ts}}, "as_of", "last_record"}`: compact OCC symbols, `null` for an empty side, `ts` the provider's receive time.
- A quote changes only when its book does, so a quiet contract's can be old and still current. `last_record` (null before any) says whether the feed itself is live; alpaca-data uses quotes only while it's within 120 seconds.

### `GET /stats`

Each contract's **open interest**, from the day's `statistics` the relay holds for joining clients (OPRA publishes it once a day, before the open, for the previous close). Same selection as `/quotes` (`symbols=` or `underlying=` with `from`/`to`).

```json
{"stats": {"SPY261016C00775000": {"open_interest": 1234, "date": "2026-10-12"}}, "as_of": 1791830000.1}
```

`date` is the trading day the figure is for. alpaca-data puts it on the chain contracts of the roots it has relays for (`open_interest`, and the chain's `open_interest_date`).

### `GET /tape`

Time and sales for the relay's own stock symbols (IEX's `mbp-1`; OPRA's quotes aren't kept): the last 4,000 trades and 4,000 changes to the top of book of each, in memory.

- `?symbol=SPY&after=<seq>` for the ticks after a sequence number, or `?symbol=SPY&limit=<n>` (at most 1000, default 500) for the latest.
- `{"symbol", "ticks": [{s, t, k, b, a, bz, az, p?, z?}], "last", "symbols", "last_record"}`, oldest first: `s` the sequence number, `t` the event time, `k` `T` (a trade, with `p` and `z`, its price and size) or `Q` (a quote change); `b`, `a`, `bz`, `az` the book, before the trade for a trade. `last` is the newest sequence number held, for the next `after`; a smaller one than before means the relay restarted.

### `GET /session`: today's replay of one contract

```
GET /session?symbol=SPY261012C00777000&schema=cbbo-1m&start=2026-10-12T13:30:00Z
```

Today's data for one option contract, from Databento's intraday replay. Each request opens a short second live session for just that contract, subscribed from `start`, reads until Databento reports the replay complete, and closes it.

| Parameter | | |
|---|---|---|
| `symbol` | required | Compact OCC, on one of this relay's roots. Anything else is `404`. |
| `schema` | `cbbo-1m` | `cbbo-1m` or `cbbo-1s` (consolidated quote each minute or second), `tcbbo` (every trade with the quote before it) or `trades`. These are what Databento replays intraday for OPRA; `cmbp-1` it won't. |
| `start` | today's 09:30 New York | RFC 3339, within the last 24 hours (Databento's replay limit). Before 09:30 the default is the last 24 hours. |

```json
{"symbol": "SPY261012C00777000", "schema": "cbbo-1m", "start": 1791811800,
 "fields": ["t", "bid", "ask", "bid_size", "ask_size", "last"],
 "rows": [[1791811860, 2.77, 2.78, 21, 1, 2.73], ...], "note": null}
```

| `schema` | `fields` |
|---|---|
| `cbbo-1m`, `cbbo-1s` | `t` (end of the interval), `bid`, `ask`, `bid_size`, `ask_size`, `last` (the session's last trade) |
| `tcbbo` | `t` (seconds with nanoseconds), `price`, `size`, `side`, `bid`, `ask` |
| `trades` | `t`, `price`, `size`, `side` |

`side` is `A` (sell aggressor), `B` (buy) or `N` (unknown, usual for OPRA). `note` is set when Databento ended the session before the replay completed.

- **Limits:** one replay runs at a time per relay, so a relay holds at most two of the dataset's live sessions (Databento allows 10 per dataset on Standard). Answers are kept for a minute. A replay taking over 30 seconds fails with `502`. While another replay holds the turn (a `/replay` file, or another `/session`) for over 30 seconds, the answer is `503` with `Retry-After: 30`.
- **Sizes:** a busy at-the-money 0DTE contract at midday is about 250 `cbbo-1m` rows and 20,000 `tcbbo` rows.
- **Session and history:** this serves the current session, held in memory. Past days are history: bought from Databento as batch files, filed in the store and read from disk (the Data page's jobs, which quote a price before any purchase).
- **Cost:** intraday replay is assumed to be included in the live plan (decision 0006); confirm against Databento's usage page.

### `POST /replay`: a replay of every symbol, to a file (experimental)

```
POST /replay?schema=cbbo-1m&start=2026-11-02T14:30:00Z
GET  /replay
GET  /replay/{id}
```

Databento's intraday replay of the relay's whole parent set (its `RELAY_SYMBOLS`, such as `SPY.OPT`, in its `RELAY_STYPE_IN` symbology, `parent`), written to a DBN file on the relay's machine, in the background. Like `/session` it opens a short second live session subscribed from `start`, reads until Databento reports the replay complete, and closes it; unlike `/session` it is every contract, kept on disk.

This is not a recording of the stream: the relay still keeps nothing of what it forwards ([decision 0006](../decisions/0006-market-data-relay.md)). A replay file is made only on request, from its own session.

**Off unless `RELAY_REPLAY_DIR` is set**, and only on Databento relays: otherwise all three answer `404` with `{"error": "replay files are off on this relay: set RELAY_REPLAY_DIR"}`.

| Parameter | | |
|---|---|---|
| `schema` | `cbbo-1m` | `cbbo-1m`, `cbbo-1s`, `tcbbo` or `trades`, as for `/session`. One file per schema: run one, then the next. |
| `start` | today's 09:30 New York | RFC 3339, within the last 24 hours, as for `/session`. |

- `POST` answers `202` with `{"id": 1, "file": "<path>", "state": "running"}`. One replay runs at a time, sharing `/session`'s turn so the relay holds at most two of the dataset's sessions: a `POST` while one runs (or while a `/session` replay runs) is `409`. A bad schema or start is `400`.
- `GET /replay` lists the last 32 replays, newest first, as `{"replays": [...]}`; `GET /replay/{id}` gives one: `{id, state, schema, start, file, records, bytes, started, finished, error}`. `state` is `running`, `done` or `failed`; `start`, `started` and `finished` are Unix seconds; `records` and `bytes` count up while it runs. Ids restart with the relay.
- A replay is given up after `RELAY_REPLAY_TIMEOUT` seconds (default 1800).
- **The network:** a replay during market hours shares the machine's link with the live feed, and Databento sends a replay as fast as it is read. `RELAY_REPLAY_MAX_MBPS` (unset: unlimited) paces the reads to that many megabits a second of DBN. Pace it well below Databento's rate, or Databento may count the replay session as a slow reader. Busy `tcbbo` or `cbbo-1s` replays of a whole parent are the heavy ones.

**The file** is `<RELAY_REPLAY_DIR>/<YYYY-MM-DD>/<dataset>-<roots>-<schema>-<start>.dbn.zst`: the date of `start` in New York, `<roots>` the parents without `.OPT` joined by `_`, `<start>` in Unix seconds, such as `2026-11-02/OPRA.PILLAR-SPY-cbbo-1m-1793629800.dbn.zst`.

- DBN v3: the session's metadata, with the schema, `stype_in`, symbols (the parents) and start set to what was asked for (a live gateway's header leaves them out, and readers such as `DBNStore.to_df()` need them), then every record Databento sent except heartbeats, unchanged (symbol mappings, definitions where the schema carries them, and the `ReplayCompleted` system record, which ends a complete file).
- Zstandard, as independent checksummed frames of at most 4 MiB decoded that never split a record, so any zstd or DBN reader reads it and a partial file reads up to its last whole frame. Records go to disk as they arrive.
- Written as `….dbn.zst.tmp` and renamed when the replay completes. A replay that ends otherwise (Databento closes the session, an error, the timeout) is kept as `….partial.dbn.zst`, the job `failed` with the reason in `error`. A complete replay of the same schema and start replaces the earlier file and removes its partial one.
- Beside it, `<file>.json`:

```json
{"format": "tinytrader.replay-file/1", "file": "OPRA.PILLAR-SPY-cbbo-1m-1793629800.dbn.zst",
 "dataset": "OPRA.PILLAR", "parents": ["SPY.OPT"], "stype_in": "parent", "schema": "cbbo-1m",
 "start": "2026-11-02T14:30:00Z", "end": "2026-11-02T20:59:00.000000000Z",
 "records": 81234, "bytes": 80123456, "sha256": "…", "completed": true, "error": null}
```

`end` is the last record's event time (system and error records aside); `bytes` and `sha256` are the file's.

#### Next

Not built yet; the code is laid out for them (a record source, a Databento session from a start until `ReplayCompleted`, apart from the file writer):

- **A replay through the stream.** A join request carrying an optional replay (schema, start): the relay plays that replay's records to the client over `tinytrader.relay/1` as it would live data, then hands over to live or ends. Databento won't replay `cmbp-1` intraday, so a replay join gets `cbbo-1s`, `cbbo-1m`, `tcbbo` or `trades`, not the live `cmbp-1`, and the stream must say so.
- **Recording the relay's output** (such as `RELAY_RECORD_DIR`, off by default): what the relay forwards, or what its Databento session receives, to files by the same writer. This changes decision 0006 ("keeps no recording"), which is amended when it is built. Its design follows Hyphae's recorder (its continuous-recording design and tape writer): sealed segments with checkpoints and an explicit end, as [decision 0013](../decisions/0013-lossless-capture-and-hot-swap.md) describes.

## Stability

- **Stable:** the join request, the record sequence (header, reference, catch-up, `ReplayCompleted`, live) and the `upstream-*` markers; `/clients`, `/bars`, `/quotes`, `/stats`. They change additively: new optional join fields, new JSON fields.
- **Experimental:** `/recent` (the console's price panels), `/tape`, `/session` and `/replay`: shapes may change with the console.
