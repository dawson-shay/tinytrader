# alpaca-history

| | |
|---|---|
| **Purpose** | Fetch past days of Alpaca stock trades and quotes, encode them as DBN (`ALPACA.SIP` `cmbp-1`, `ALPACA.IEX` `mbp-1`, with `tinytrader.trade-conditions/1` sidecars, and for SIP `tinytrader.quote-conditions/1` sidecars) and file them in the [store](store.md) ([decision 0014](../decisions/0014-history-feeds.md)). |
| **Transport** | HTTP/1.1, JSON, on `HISTORY_LISTEN` (default `127.0.0.1:9124`). |
| **Versioning** | Unversioned paths. Files it writes carry `tinytrader.alpaca-history.receipts/1`, `tinytrader.alpaca-history.conditions-binding/1` and `tinytrader.alpaca-history.quote-conditions-binding/1`. |
| **Auth** | Writes are refused from a web page of another origin; no token of its own yet. It writes to the store with `HISTORY_STORE_TOKEN` (or `_FILE`). |
| **Errors** | `{"error": "..."}`: `400` invalid run, `404` unknown run, `409` a run that can't resume, `503` a dry run that couldn't ask Alpaca's calendar or the store. |
| **Health** | [Observe](observe.md) endpoints and `/events`. `idle` with nothing queued or while waiting. |
| **Stability** | **Experimental.** |

## Configuration

| Variable | |
|---|---|
| `ALPACA_HISTORY_KEY_ID`, `ALPACA_HISTORY_SECRET_KEY` (or `_FILE`) | Required. |
| `HISTORY_STORE` | The store (default `http://127.0.0.1:9120`). |
| `HISTORY_STATE` | Where runs and the spool are kept (default `/data`). |
| `HISTORY_TOPOFF` | Symbols to top off every evening (none by default). |
| `HISTORY_YIELD_TO`, `HISTORY_CAPTURE_HOURS` | Hold off while capture is live. |
| `HISTORY_MAX_RPM` | Requests a minute (default 190), shared by every day in flight. |
| `HISTORY_CONCURRENCY` | Days in flight at once, 1 to 8 (default 3). |
| `HISTORY_MIN_FREE_GB` | Free space the spool's disk must have before a day starts (default 20); below it days wait, with an event. |
| `HISTORY_DAY_RESERVE_GB` | Disk reserved for each day being built, from its start until it's filed (default 3), counted against the floor. |
| `HISTORY_SPOOL_MAX_AGE_DAYS` | A day's spool untouched this long is removed at startup (default 14), unless a run still needs it. |

## Endpoints

| Endpoint | |
|---|---|
| `GET /runs` | `{"runs": [{id, feed, symbols, start, through, state, note, from, planned, committed, held, failed, bytes, days: {"SYM YYYY-MM-DD": {state, trades?, quotes?, pages?, bytes?, note?, attempts?}}}]}`, newest first. A run's `state` is `queued`, `running`, `done`, `stopped` or `failed`; a day's is `committed`, `held`, `failed`, `deferred` or `conditions_unbound`, and `attempts` counts its failures. |
| `POST /runs` | `{feed: "sip" \| "iex", symbols: [1..50], start, through, order?: "oldest-first" \| "newest-first", origin?: "manual" \| "scheduled", dry_run?: bool}` → the run. With `dry_run`, nothing is queued: the plan below. `origin` (default `manual`) says who asked: see [manual and scheduled runs](#manual-and-scheduled-runs). |
| `POST /runs/{id}/stop` | `{"stopped": id}`, after the days in flight. |
| `POST /runs/{id}/clear/{symbol}/{day}` | `{"cleared": "SYM YYYY-MM-DD", "was": state}`: a day of the run left unfiled (such as `conditions_unbound`, once a person has compared its batches) loses its record and spool; resuming the run fetches it afresh. `409` for a filed or held day. |
| `POST /runs/{id}/resume` | `{"resumed": id}`: a `stopped` or `failed` run is `queued` again. Its `failed` and `deferred` days are tried afresh (their attempts reset); the days it filed or found held are skipped, and every day is checked against the store again before it's fetched. `conditions_unbound` days stay as they are, for a person. `409` for a run in any other state. |

### Dry run

`POST /runs` with `"dry_run": true` plans the run without queueing it:

```json
{ "dry_run": true, "feed": "sip", "symbols": ["SPY", "QQQ"], "start": "2020-01-02", "through": "2026-10-30",
  "order": "oldest-first", "trading_days": 1717, "days": ["2020-01-02", "..."], "symbol_days": 3434,
  "held": 250, "partly_held": 12, "to_fetch": 3184,
  "estimate": { "basis": "the mean of 250 days of this feed fetched", "days_measured": 250,
                "pages_per_day": 320, "requests": 1018880, "minutes_at_max_rpm": 5363, "bytes": 190000000000,
                "partly_held_requests": 3840, "partly_held_note": "12 symbol-days are held without every sidecar ..." } }
```

`through` is cut to the last day that has ended. `held` symbol-days have every batch in the store; `partly_held` have their records but not every sidecar (a SIP day filed before quote conditions), and are fetched again to file the rest: each is a full refetch of both lanes, counted in `to_fetch` and `requests` and called out in `partly_held_requests`. The estimate is the mean pages and bytes of the days this service has committed for the feed (the run's symbols if it has any, else any), or assumed (200 pages, 50 MB) with none.

## Which days

A day is a **UTC day**, `00:00:00Z` to `23:59:59.999999999Z`, as Databento's files are, and each of the market's sessions is one. A session's extended hours run until four hours after its close, so under Eastern Standard Time a full day's 19:00–20:00 New York is 00:00–01:00 UTC on the next date. When that date is a session, it's fetched anyway. When it isn't (after every winter Friday, and before a winter holiday), **it's fetched too, as a day of its own** holding that hour, or the hour would never be. This doesn't happen in summer (20:00 EDT is midnight UTC), or after an early close (whose extended hours end at 17:00). Before 2026-11-11 these hours weren't fetched; a run over those dates now fetches them, as days the store doesn't hold.

## How a day is fetched

- **Pages are spooled as they arrive** (`HISTORY_STATE/spool/lanes/`): each page's body, its receipt and the lane's next page token, synced before the next page is asked for. A restart, a rate limit, an outage or live capture beginning resumes the lane at its next page; nothing fetched is asked for again. A day's pages are removed once its batches are written and checked.
- **Rate limits and outages wait.** A 429, a 5xx or a dropped connection that persists past the pace's own retries makes the lane wait (doubling, up to five minutes) and ask for the same page again; after eight waits the day is left for the run's next pass, its pages kept. `Retry-After` (seconds or an HTTP date) is honoured on 429 and 503. The pace keeps back from Alpaca's advertised remaining count at least as many requests as can be in flight (two a day). A 400 for a page token starts that lane again from its first page (at most twice). Every wait wakes as soon as the service holds for live capture; a lane that fails stops its sibling at its next page, and both are awaited before the day is retried, so no write to a day's spool outlives its attempt.
- **A failed day is tried again**, up to three times in the run (an event says so each time), then left `failed` and its spool removed; `POST /runs/{id}/resume` tries it again. A trade whose conditions the layout can't hold fails the day at once, for good, without fetching again (doing so would only repeat it). A quote whose conditions can't be held (more than four codes, a code that isn't one printable ASCII byte, no tape) never fails it: its quote-conditions record is marked unparsed, counted in the outcome and the binding (`unparsed`), with an event.
- **Streamed.** The day is written from the spool a page at a time: memory doesn't grow with the day, however large. Uploads to the store are streamed from disk too.
- **Checked.** Read back, the main file's trades (`T`) must match the trades lane's pages, and its quotes (`M`) the quotes lane's, each by count and by first and last time; the trade conditions must number its trades, the quote conditions its quotes. Any mismatch fails the day.
- **Concurrent.** Up to `HISTORY_CONCURRENCY` days are in flight, each with both lanes, on the one pace. For a scheduled run, live capture (`HISTORY_YIELD_TO`) is checked before each day and every 30 seconds while days are in flight; when it's live (or can't be reached, which counts as live), each lane stops at its next page, its pages kept.

### Manual and scheduled runs

Decision 0033 (the user's rule): **scheduled work yields; work launched by hand runs at once.**

- **A manual run** (`origin: "manual"`, the default for `POST /runs`) starts at once and never waits for live capture. It is taken before any scheduled run, in the order asked.
- **A scheduled run** (`origin: "scheduled"`, and the nightly top-off) waits while capture is live, as above. While a manual run is queued, a scheduled run in progress stops at its next page (its pages kept) and resumes once no manual run is left.
- **The limits that aren't about priority hold for both:** the free-space floor, Alpaca's pace and rate limits, a busy store, and `HISTORY_CONCURRENCY`.
- A run's `origin` is in `GET /runs`. Runs kept from before origins existed count as scheduled if they came from the top-off, and manual otherwise.
- An automation that asks for runs (a cron script, say) must pass `"origin": "scheduled"`, or it runs at once in market hours.
- **Disk.** A heavy SIP day (SPY in 2020 or 2022) peaks at about 1 GB of spooled pages plus 1 to 2 GB of built batches, so the spool's peak is that times `HISTORY_CONCURRENCY` (about 9 GB at 3). No day starts unless the free space, less `HISTORY_DAY_RESERVE_GB` for each day already being built and for itself, stays above `HISTORY_MIN_FREE_GB`. A day's pages go once its batches are written; its batches once it's filed, refused, or failed for good. At startup, spools untouched for `HISTORY_SPOOL_MAX_AGE_DAYS` are removed, unless a run that isn't finished (queued, running or stopped) covers the day or a run deferred it; complete spools built by another encoder lose their batches (their pages, if any, are kept and rebuilt from). A `conditions_unbound` day's batches are never swept, whatever their age or encoder, until the day is cleared. Each day swept, or kept past its age, is logged and told as an event with its reason.

## Batches

A day is filed as its batches: `ALPACA-<FEED>-<SYM>-<YYYYMMDD>` (the records), `…-conditions` (`tinytrader.trade-conditions/1`, the nth for the nth trade) and, for SIP, `…-quote-conditions` (`tinytrader.quote-conditions/1`, the nth for the nth quote; see [datasets](../datasets.md)). Each sidecar carries a `binding.json` naming the main file's size and SHA-256. When the store already holds a day's records, a sidecar is filed only if it was built against them.
