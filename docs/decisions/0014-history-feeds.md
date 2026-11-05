# 0014: History feeds

- **Status:** Accepted
- **Date:** 2026-10-13
- **Builds on:** [0002](0002-data-store.md) (provider roots, filing batches),
  [0005](0005-store-service-and-supervision.md) (the store is the only
  process that writes the data disks), [0007](0007-storage-pool-and-automations.md)
  (Alpaca history is refetchable: one copy), [0011](0011-bundles-and-work-classes.md)
  (background work yields to capture)

## Context

Alpaca's historical trades and quotes (SIP and IEX) are free, but only page by
page: a SPY day is about a million trades and three to five million quotes, in
about 470 pages. A Python downloader once kept SPY's SIP pages as day archives;
they were deleted for space, and research wanted the years back. Hyphae's feed
fetched the same data with a thorough design (crash-proof slot transactions,
page evidence, byte-for-byte reconciliation), but it writes around tinytrader's
store and is tens of thousands of lines. Porting it was considered and turned
down: that care suits data that can't be fetched again, and this can.

## Decision

A small service, `alpaca-history`, fetches a symbol-day at a time and hands it
to the store as ordinary batches, filed like Databento's.

- **A day counts only when it is whole.** Both lanes (trades and quotes) are
  paged over the exact UTC day until a page comes without a next token; rows
  must stay in time order and inside the day, and a token seen twice stops the
  day. The day is encoded, written, and read back: its record count and first
  and last times must equal what the pages said, and its conditions records
  must number its trades. Otherwise nothing is filed.
- **Days come from Alpaca's market calendar** (sessions only, early closes
  included); without it nothing is planned. A day that hasn't ended (and 15
  minutes more) is never asked for. A day the store already holds is skipped.
- **One pace for every request:** below the configured rate (190 a minute) and
  what Alpaca advertises, waiting out the window when its remaining count
  reaches a small reserve, honouring `Retry-After`. A 429 is never success: it
  pauses the pace, and three in a row stop the run until the next pass. Server
  errors and dropped connections are retried with a doubling backoff.
- **Its own key.** Alpaca's data limit is per key (calls on one key don't
  reduce another's remaining count), so history uses a paper account's key and
  never competes with the console, relays or trading.
- **Files.** A day is two store batches: `<DATASET>/<symbol>/<schema>/<day>.dbn.zst`
  (`ALPACA.SIP` as `cmbp-1`, `ALPACA.IEX` as `mbp-1`) and
  `<DATASET>/<symbol>/conditions/<day>.dbn.zst`, the `tinytrader.trade-conditions/1`
  records, the nth for the nth trade. Their receipts go to the store's
  `provenance/`: every page's rows, first and last times and SHA-256, and a
  binding of the conditions file to its main file. The pages themselves aren't
  kept; they can be asked for again.
- **The store** files Alpaca batches as it files Databento's (same checks,
  never replacing a day, the same ledgers); its only changes are the `Alpaca`
  provider root and the `conditions` schema, whose files' headers name no
  schema.
- **Running it.** Runs are queued over HTTP (`POST /runs`) and listed with each
  day's outcome (`GET /runs`). A failed day is recorded and the run goes on;
  queueing it again retries it. A nightly top-off queues the last ten days of
  its symbols at 20:40 New York. It waits while capture is live or about to be,
  and reports through `tinytrader.observe/1`.

The encoders live in `libs/alpaca`, shared with the relay's live IEX feed.

## Consequences

- Hyphae's SIP days on NewDrive (2025-09-19 to 2026-10-05) aren't adopted:
  they use Hyphae's own instrument and publisher ids. They're fetched again
  into this layout, so a dataset has one encoding.
- A crash mid-day loses that day's work, not its correctness: the day is
  fetched again (about three minutes for SPY).
- A question only the raw pages could answer means fetching them again.

## Amended: a multi-year backfill

Backfilling years of SPY and QQQ needed more than a day at a time:

- Each lane's pages are spooled as they arrive, with the next page token, so
  a crash, a restart, a rate limit or an outage resumes mid-lane and never
  asks for a page twice (as Hyphae's recorder does). A rate limit or an
  outage waits and asks again; a 400 for a page token restarts that lane.
  A failed day is tried again up to three times. This replaces the
  consequence above: a crash loses no fetched pages.
- A day is written from the spool as a stream, and uploaded as one, so memory
  doesn't grow with the day.
- The check reads trades and quotes back separately, each against its own
  lane, so a lost trade and a duplicated quote can't cancel out.
- SIP quote conditions are kept, as `tinytrader.quote-conditions/1` in a
  third batch (schema `quote-conditions`), which the store files as it files
  `conditions`.
- Several days are in flight at once on the one pace (`HISTORY_CONCURRENCY`).
