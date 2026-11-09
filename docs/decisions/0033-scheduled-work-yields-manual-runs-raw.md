# 0033: Scheduled work yields; work launched by hand runs at once

- **Status:** Accepted
- **Date:** 2026-11-09
- **Amends:** [0011](0011-bundles-and-work-classes.md) (work classes: what is
  "background")
- **Decided by the user** on 2026-11-06. This records how it's built.

## Context

0011 makes downloads and filing **background** work. Background work yields
while capture is live, doesn't start a long task shortly before capture's
hours, and slows under pressure. It applies by service, not by who asked:

- **alpaca-history** yields its whole worker while a relay is live, so a
  backfill someone starts at 10:00 waits until 16:20. Today it runs unpaused
  from a tmux session on the M2 (`HISTORY_CONCURRENCY=8`) as a stopgap,
  outside the service and its records.
- **databento-jobs**' cache requests wait out the capture hours in the same
  way. A file someone needs for research now arrives in the evening.

The user's rule: **scheduled work runs at the schedule's priorities, yielding
to the relays in market hours, and anything launched by hand runs "raw": at
once, with no yield.**

## Decision

### 1. Every unit of work has an origin

| Origin | What | Examples |
|---|---|---|
| `scheduled` | started by a timer or an automation | alpaca-history's nightly top-off; databento-jobs' scheduled condition checks and the downloads of jobs a schedule submitted; ops' cron scripts |
| `manual` | started by a person, by hand | `POST /runs` from the console or `tt`; `POST /cache/{job}`, `POST /download/{job}`; a purchase submitted by hand, and its download |

- **The request says which:** `"origin": "manual" | "scheduled"` on the HTTP
  call that creates the work. **Absent means `manual`.** That is the rule's
  default: what a person starts runs at once. A caller that runs on a
  schedule must say `scheduled`.
- **The service's own timers** (the top-off, the condition sync) create
  `scheduled` work themselves.
- **Origin is recorded** on the run, request or job, and shown in its listing
  (`GET /runs`, `GET /cache`), with what it is waiting for, if anything.

### 2. Scheduled work is 0011's background, unchanged

- It yields while a capture service is live, checking every 15 s and between
  resumable steps (a page, a file).
- It doesn't start a long task before capture's hours begin.
- It slows under pressure (skips rising, swap).

### 3. Manual work runs raw

- **It starts at once and doesn't yield to capture.** It isn't held for
  capture's hours, and isn't slowed by pressure signals.
- **It goes first.** In a service with one worker or one queue, a manual run
  is taken before any scheduled one. A scheduled run in progress pauses at
  its next resumable point (as it does for capture) while manual work runs,
  and resumes afterwards. Manual runs keep their own order.
- **Safety limits still apply.** Raw is about priority, not protection:
  - the cache's disk floor (#123), the store's thresholds, and a busy store
    (`503`, retried);
  - the provider's rate limits and the service's own retries;
  - the bounded queues (`503` when full);
  - the purchase guards (0019), for anything that buys.
- **Its cost is visible.** A manual run in market hours shares the network and
  disks with the relays. The relay's lag and stale-quote alerts (#131, 0032)
  say if it hurts capture, and the run's listing says it ran raw. The user
  accepted this cost with the rule.
- **The process isn't throttled by the OS.** Under `taskpolicy -b`, the OS
  throttles CPU, disk and network for the whole process, manual work
  included. These two services run at the normal policy instead, and
  scheduled work does its own pacing: the yield, the schedule, and a rate
  cap where one exists.

### 4. Where it's built

- **alpaca-history:**
  - `Run.origin`. `POST /runs` takes `origin` (default `manual`), and the
    top-off creates `scheduled` runs.
  - The worker takes manual runs first.
  - `yielding()` applies only to scheduled runs, and a scheduled run
    re-checks it at each page and each day. `HISTORY_CONCURRENCY` applies to
    both.
  - This replaces the tmux stopgap.
- **databento-jobs:**
  - The cache queue (#123) and downloads carry an origin. `POST /cache/{job}`
    and `POST /download/{job}` default to `manual`; the download of a job
    follows its submission's origin.
  - Manual items skip the capture wait and the "capture starts before this
    file could finish" wait, and go to the front of the queue. The disk floor
    and the store's `503`s still apply.
  - The service's main loop, which pauses refreshes and the condition sync
    during capture, keeps yielding for its own scheduled work.
- **tinytrader-ops:** every cron or automation that calls these endpoints
  passes `origin=scheduled`, and is listed in the runbook.

## Steps

1. **alpaca-history:**
   - origin, manual-first, and yielding for scheduled runs only;
   - the normal process policy;
   - tests: a manual run during live capture starts at once, a scheduled one
     waits, and a manual run arriving mid-scheduled-run goes first.
2. **databento-jobs:**
   - origin on cache and download work, manual at the front without the
     capture waits;
   - tests: during live capture a manual cache request downloads, a
     scheduled one waits, and the disk floor still holds for manual work.
3. **tinytrader-ops:** scripts pass `origin=scheduled`; the tmux stopgap
   goes.

## Consequences

- What a person starts happens now, as they expect, in the service with its
  records, not in a tmux side-channel.
- Scheduled work keeps protecting capture, as 0011 intends.
- A caller that forgets `origin=scheduled` runs raw in market hours. The
  default favours the person at the keyboard, so automations must say what
  they are. The runbook lists them, and the listings show each run's origin,
  so a mislabelled one is easy to see.

## Open questions

- Whether a manual run should be able to ask to yield after all
  (`origin=manual&yield=true`, "do this, but politely"). Not built unless
  asked for.
