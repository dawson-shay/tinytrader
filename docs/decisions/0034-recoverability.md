# 0034: What it would take to get each day back

- **Status:** Proposed
- **Date:** 2026-11-11
- **Builds on:** [0007](0007-storage-pool-and-automations.md) (copies by kind
  of data), [0019](0019-purchase-guards.md) (purchases),
  [0031](0031-distributed-volumes.md) (volumes)

## Context

The store holds data of very different value, and doesn't say which is which.

- **Free to get again:** Alpaca's SIP and IEX history, since 2016.
- **Free for a while:** Databento serves some history under a subscription,
  for a rolling window (for OPRA, the trailing twelve months), and a batch job
  can be downloaded again until it expires.
- **Available only under a subscription we don't hold:** for example CME
  futures, where a month's subscription can cost less than buying a large pull
  by the gigabyte.
- **Available only by paying again**, for what is outside every window.
- **Not available anywhere:** what we recorded ourselves, and what the
  provider no longer serves.

On 2026-11-11, five OPRA batches (29 GB) turned out to exist only on one
drive that keeps dropping out. Databento no longer serves them. Nothing had
said they were irreplaceable, so nothing had copied them first.

## Decision

### 1. The facts are the deployment's, with dates

A deployment states its entitlements in one file (`STORE_ENTITLEMENTS`, JSON).
They are facts about its contracts, not the platform's:

```json
{ "entitlements": [
  { "provider": "alpaca", "dataset": "ALPACA.SIP", "kind": "free",
    "from": "2016-01-01", "note": "Alpaca's historical API" },
  { "provider": "databento", "dataset": "OPRA.PILLAR", "kind": "window",
    "months": 12, "held_since": "2026-09-01", "held_until": null,
    "note": "the plan's included OPRA history" },
  { "provider": "databento", "dataset": "GLBX.MDP3", "kind": "subscribable",
    "per_month_usd": 179, "note": "not held; one month covers its whole history" }
] }
```

- **Kinds:**
  - `free`: available at no cost from `from` onwards;
  - `window`: available at no cost for the trailing `months`, while the
    entitlement is held (`held_since`, `held_until`, where null means held);
  - `subscribable`: available under an entitlement we don't hold, with its
    price;
  - `paid` (the default, for anything listed nowhere): only by paying again.
- **Changes** (a subscription ends, a window changes) are edits to the file,
  in the deployment's history. The store reads the file at start, and again
  when it changes.
- **Our own recordings** (capture datasets, 0013) are `irreplaceable` by
  their provider, with no entry needed.

### 2. Every day carries its label, computed as of today

For each day, `/coverage` gives `recoverable`:

```json
"recoverable": { "class": "free_until", "until": "2027-11-03",
                 "by": "databento OPRA.PILLAR window (12 months)" }
```

| `class` | Means |
|---|---|
| `free` | can be fetched again at no cost, with no end in sight |
| `free_until` | at no cost until `until`, then not |
| `entitlement` | only under an entitlement we don't hold (`by` names it, with its price) |
| `paid` | only by paying again |
| `irreplaceable` | nowhere: our own recording, or nothing serves it now |

- **A window rolls:** a day `d` under a 12-month window is `free_until` `d` +
  12 months, while the entitlement is held. After that it's `paid`, or
  `entitlement` if one is listed. The label is computed per request from
  today's date, so it changes on its own.
- **A batch's own job:** while Databento still serves the batch's job (its
  expiry is recorded in the batch's receipts when databento-jobs downloads
  it), its days are at least `free_until` that expiry.
- **The worst copy decides nothing:** the label is about the source, not about
  how many copies the store has. 0007's copy counts stand beside it, so "1 of
  2 copies, irreplaceable" is the line that matters.

### 3. What's at risk, in one place

`GET /recoverability` lists the store's data by risk, worst first:
- irreplaceable data held in fewer than two copies;
- `free_until` data whose `until` is within 30 days and that has one copy;
- the rest, as totals by class.

The console shows it. The notifier warns when irreplaceable data has one copy,
or when a single-copy window is about to close. Backups (0007, and the M4
copies now) are taken in this order.

## Steps

1. **Entitlements and labels:**
   - read the file, with its schema checked, and refuse to start on a bad one;
   - `recoverable` per day in `/coverage`;
   - counts by class per dataset in `/catalog`;
   - tests across a window's roll, an entitlement that ended, and a batch's
     job expiry.
2. **`GET /recoverability`** and the notifier's two warnings.
3. **(tinytrader-ops)** the deployment's entitlements file, and backups taken
   in this order.

## Open questions

- **The exact OPRA window,** and whether the plan's history covers every
  schema. The user confirms it from the Databento portal, and the file
  records it.
- **Whether `paid` should carry an estimated price** (from a quote at the
  time). That's useful for deciding what to back up, but it's another call to
  Databento per day; the recommendation is no, until it's asked for.
