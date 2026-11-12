# alpaca-data

| | |
|---|---|
| **Purpose** | Market data for any symbol, on request: stock snapshots and bars, daily closes, option chains with IV and greeks computed locally, option bars and contracts, index bars (Cboe, delayed), a screener and news search. |
| **Transport** | HTTP/1.1, JSON, on `ALPACA_DATA_LISTEN` (default `127.0.0.1:9125`; it was `9123`, which the queue also uses). |
| **Versioning** | Unversioned paths; additive changes only. |
| **Auth** | None (all reads). Bind to loopback or a private network. |
| **Errors** | `{"error": "..."}`: `400` for a bad parameter, `429` when its Alpaca budget for the minute is spent, `502` when an upstream fails. A **missing or non-numeric** parameter is refused by the framework with a plain-text `400` ([decision 0017](../decisions/0017-client-contracts.md)). |
| **Health** | [Observe](observe.md) endpoints. `degraded` after Alpaca refuses its key (`401`/`403`). |
| **Stability** | `/snapshots`, `/bars`, `/daily`, `/chain` are **stable**; `/chain_index`, `/option_bars`, `/option_contracts`, `/news`, `/screener` are **experimental**. |

It declares each endpoint in `/describe`'s `streams` (`chain`, `bars`, ...), and the console finds it by those capabilities.

## Configuration

| Variable | Default | |
|---|---|---|
| `APCA_API_KEY_ID`, `APCA_API_SECRET_KEY` (or `_FILE`) | required | A data key. |
| `ALPACA_DATA_FEED` | `iex` | Stock feed: `iex` or `sip`. |
| `ALPACA_OPTIONS_FEED` | `indicative` | `indicative` or `opra`. |
| `ALPACA_HISTORY_FEED` | `sip` | Feed for bars older than 16 minutes. |
| `ALPACA_DATA_QUOTES` | none | OPRA relays to overlay live quotes and open interest: `ROOT=http://relay:9110;...` (the relays' HTTP bases). |
| `ALPACA_DATA_DIR` | none | A disk cache (index minutes, contract lists). |
| `ALPACA_DATA_RECORD` | `XSP,SPX,VIX` | Indexes whose minutes are kept in the cache (only with `ALPACA_DATA_DIR`). |

## Units

Bar times (`t`), `at`, `as_of` and `end` are integer Unix **seconds** (a bar's `t` is its start); dates are `YYYY-MM-DD` (New York). Prices are dollars; `change_pct`, `iv`, `implied_move` and the like are fractions. Bars are `[t, o, h, l, c, v]` arrays; `/bars` appends Alpaca's VWAP and trade count, `[t, o, h, l, c, v, vw, n]` (null where Alpaca gives none).

## Endpoints

| Endpoint | Parameters | Answer |
|---|---|---|
| `GET /snapshots` | `symbols` (1 to 100) | `{"snapshots": [{symbol, price, bid, ask, prev_close, change, change_pct, day: {o,h,l,c,v}, at}], "feed"}` |
| `GET /bars` | `symbol`; `interval` 60 (default), 300, 900, 3600, 86400 or 604800 seconds; `days` (default 1, capped per interval); `end` (Unix s, exclusive); `adjustment` `split` (default), `raw`, `dividend`, `all` | `{"symbol", "interval", "bars": [[t,o,h,l,c,v]], "note", "source"?}`. Index symbols (SPX, VIX, ...) come from Cboe, 15 minutes delayed, `source: "cboe"`, volume 0. |
| `GET /daily` | `symbols` (1 to 200: stocks, crypto pairs such as `BTC/USD`, indexes); `days` (default 300) | `{"as_of", "days", "series": {SYM: {source, closes: [[t, c]], error?}}, "incomplete"}` |
| `GET /chain` | `underlying`; `from`, `to` (default today to 60 days ahead) | `{underlying, spot, spot_from, iv_from, feed, from, to, quotes, opra_quotes, open_interest_date, expiries: [...], contracts: [...], truncated}` |
| `GET /chain_index` | `underlying` | Today's listed strikes per expiry: `{"underlying", "date", "expiries": [{expiry, root, calls, puts}]}` |
| `GET /option_bars` | `symbols` (1 to 20 OCC), `start`, `end` (dates) | `{"bars": {OCC: [[t,o,h,l,c,v]]}}`, daily |
| `GET /option_contracts` | `underlying`, `from`, `to` (all required) | `{"underlying", "contracts": [{symbol, expiry, type, strike}]}`, expired ones included |
| `GET /news` | `symbols` (1 to 50), `limit` (1 to 50) | `{"items": [{id, headline, summary, author, source, url, symbols, created_at, updated_at}]}` |
| `GET /screener` | `min_cap` (default 2e9), `min_price` (5), `scan` (150) | `{as_of, universe_size, matched, min_cap, min_price, pending, options_feed, rows: [...]}` |

### Chains

Each contract: `symbol`, `expiry`, `type` (`call`/`put`), `strike`, `bid`, `ask`, `mid`, `last`, `iv` (annualised, from the mid), `delta`, `gamma`, `theta` (per calendar day), `vega` (per vol point), `quote_src` (`opra` or `indicative`), and `open_interest` where a relay has it. Greeks are computed here (Black-76 on the parity forward), not taken from Alpaca.

Each expiry: `expiry`, `contracts`, `atm_strike`, `atm_iv`, `straddle`, `implied_move`, and `distribution` (the [implied distribution](../implied-distribution.md): quantiles, `cdf`, `quality`, ...).

## Limits

At most 180 requests a minute to Alpaca (the free plan allows 200); past that, `429`. Answers are cached briefly (seconds for quotes, longer for history) in at most 40 MB.
