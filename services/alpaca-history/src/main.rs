//! Alpaca's historical trades and quotes (SIP or IEX), a day at a time, into
//! the store (decision 0014).
//!
//! - `POST /runs` with `{ feed, symbols, start, through, order }` (days
//!   inclusive; `order` `oldest-first` or `newest-first`) queues a run; with
//!   `"dry_run": true` it only plans it: the trading days, how many the store
//!   already holds, and estimates.
//! - `GET /runs`: every run, its progress, and each day's outcome.
//! - `POST /runs/{id}/stop` stops one after the days in flight.
//! - `POST /runs/{id}/resume` queues a stopped or failed run again.
//!
//! A run plans its days from Alpaca's market calendar (sessions only, early
//! closes included; without the calendar nothing is planned), skips days the
//! store already holds, and never asks for a day that hasn't ended. Up to
//! `HISTORY_CONCURRENCY` days are in flight at once, sharing one pace. Each
//! day is both lanes fetched to their last page (every page spooled as it
//! arrives, so a restart resumes mid-lane), encoded as a stream, read back and
//! checked, and filed by the store as its batches (records, trade conditions,
//! and for SIP quote conditions). A rate limit or outage waits and resumes
//! where it was; a failed day is tried again up to three times, then recorded
//! and the run goes on. A nightly top-off (`HISTORY_TOPOFF`) queues the last
//! ten days for its symbols at 20:40 New York.
//!
//! It is background work (decision 0011): while a capture service is live,
//! or its hours begin soon, it waits. It uses its own Alpaca key
//! (`ALPACA_HISTORY_KEY_ID`, `ALPACA_HISTORY_SECRET_KEY`, or `*_FILE`): the
//! data limit is per key, so it never competes with live use. The store's
//! write token, when the store requires one: `HISTORY_STORE_TOKEN` or
//! `HISTORY_STORE_TOKEN_FILE`.

mod day;
mod fetch;
mod spool;
#[cfg(test)]
mod testing;

use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::{Path as UrlPath, State as AxState};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use clap::Parser;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tinytrader_observe::{Events, Observer, State, listen_address, lock, secret, unix_seconds};

use day::Feed;
use fetch::{Alpaca, Failure, Pace};

#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// The store's address.
    #[arg(long, env = "HISTORY_STORE", default_value = "http://127.0.0.1:9120")]
    store: String,
    /// Where runs and the day being built are kept.
    #[arg(long, env = "HISTORY_STATE", default_value = "/data")]
    state: PathBuf,
    /// Requests a minute, at most (Alpaca allows 200 a key).
    #[arg(long, env = "HISTORY_MAX_RPM", default_value_t = 190)]
    max_rpm: u32,
    /// Alpaca's market data API.
    #[arg(
        long,
        env = "HISTORY_DATA_URL",
        default_value = "https://data.alpaca.markets"
    )]
    data_url: String,
    /// Alpaca's trading API, for the market calendar (the key's account: paper).
    #[arg(
        long,
        env = "HISTORY_CALENDAR_URL",
        default_value = "https://paper-api.alpaca.markets"
    )]
    calendar_url: String,
    /// Capture services to yield to, as NAME=URL of each one's observe
    /// endpoint, separated by `;` (decision 0011).
    #[arg(long, env = "HISTORY_YIELD_TO", value_delimiter = ';')]
    yield_to: Vec<String>,
    /// The capture hours (New York, e.g. `07:55-17:05`): no day is started
    /// within 15 minutes of them beginning.
    #[arg(long, env = "HISTORY_CAPTURE_HOURS")]
    capture_hours: Option<tinytrader_schedule::Schedule>,
    /// Symbols topped off nightly from SIP (`SPY,QQQ`); none if empty.
    #[arg(long, env = "HISTORY_TOPOFF", value_delimiter = ',')]
    topoff: Vec<String>,
    /// Days in flight at once (1 to 8), sharing the one pace.
    #[arg(long, env = "HISTORY_CONCURRENCY", default_value_t = 3, value_parser = clap::value_parser!(u32).range(1..=8))]
    concurrency: u32,
    /// Free space (GB) the spool's disk must have before a day starts; below
    /// it, days wait.
    #[arg(long, env = "HISTORY_MIN_FREE_GB", default_value_t = 20.0)]
    min_free_gb: f64,
    /// Spools untouched this many days are removed at startup.
    #[arg(long, env = "HISTORY_SPOOL_MAX_AGE_DAYS", default_value_t = 14)]
    spool_max_age_days: u64,
}

/// Times a day is tried before it's left failed.
const ATTEMPTS: u32 = 3;
/// How often live capture is checked while days are in flight.
const YIELD_CHECK: Duration = if cfg!(test) {
    Duration::from_millis(50)
} else {
    Duration::from_secs(30)
};
/// How long a run waits after a rate limit, an outage or a busy store.
const BACKOFF: Duration = if cfg!(test) {
    Duration::from_millis(50)
} else {
    Duration::from_secs(60)
};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DayRecord {
    /// committed, held, failed
    state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    trades: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    quotes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pages: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    note: String,
    /// Times the day failed (it's tried again up to [`ATTEMPTS`]).
    #[serde(default, skip_serializing_if = "is_zero")]
    attempts: u32,
}

fn is_zero(n: &u32) -> bool {
    *n == 0
}

impl DayRecord {
    fn noted(state: &str, note: String) -> Self {
        Self {
            state: state.into(),
            trades: None,
            quotes: None,
            pages: None,
            bytes: None,
            note,
            attempts: 0,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Run {
    id: String,
    feed: Feed,
    symbols: Vec<String>,
    start: String,
    through: String,
    newest_first: bool,
    created: f64,
    /// queued, running, done, stopped, failed
    state: String,
    #[serde(default)]
    note: String,
    /// `<SYMBOL> <YYYY-MM-DD>` → outcome.
    #[serde(default)]
    days: BTreeMap<String, DayRecord>,
    #[serde(default)]
    planned: usize,
    #[serde(default)]
    from: String,
}

struct App {
    args: Args,
    alpaca: Alpaca,
    http: reqwest::Client,
    store: String,
    /// The store's write token, if it requires one.
    store_token: Option<String>,
    runs: Mutex<Vec<Run>>,
    events: Events,
    observer: Observer,
    yield_to: Vec<(String, String)>,
    last_topoff: Mutex<String>,
}
type Shared = Arc<App>;

fn runs_path(a: &App) -> PathBuf {
    a.args.state.join("runs.json")
}

/// Keep the runs: written whole to a temporary file and renamed.
fn save(a: &App) {
    let runs = lock(&a.runs).clone();
    let tmp = a.args.state.join("runs.json.tmp");
    let ok = serde_json::to_vec_pretty(&runs)
        .map_err(|e| e.to_string())
        .and_then(|b| std::fs::write(&tmp, b).map_err(|e| e.to_string()))
        .and_then(|()| std::fs::rename(&tmp, runs_path(a)).map_err(|e| e.to_string()));
    if let Err(e) = ok {
        a.observer.record_error(&format!("cannot keep runs: {e}"));
    }
}

/// Today and the time now in New York (`YYYY-MM-DD`, `HH:MM`).
fn ny_now() -> (String, String) {
    let (d, t, _) = tinytrader_schedule::ny_date_time(unix_seconds() as i64);
    (d, t)
}

/// The last UTC day that has ended (15 minutes ago or more).
fn last_complete_day() -> String {
    let t = time::OffsetDateTime::now_utc() - time::Duration::minutes(15) - time::Duration::days(1);
    t.date().to_string()
}

fn valid_day(d: &str) -> bool {
    fetch::day_ns(d).is_some()
}
fn valid_symbol(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 10
        && s.bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'.')
}

#[derive(Deserialize)]
struct Ask {
    feed: Feed,
    symbols: Vec<String>,
    start: String,
    through: String,
    #[serde(default)]
    order: Option<String>,
    /// Only plan the run: nothing is queued.
    #[serde(default)]
    dry_run: bool,
}

/// An asked-for run's symbols and order, checked.
fn checked(ask: &Ask) -> Result<(Vec<String>, bool), String> {
    let symbols: Vec<String> = ask
        .symbols
        .iter()
        .map(|s| s.trim().to_uppercase())
        .collect();
    if symbols.is_empty() || symbols.len() > 50 || !symbols.iter().all(|s| valid_symbol(s)) {
        return Err("symbols: 1 to 50 stock symbols".into());
    }
    if !valid_day(&ask.start) || !valid_day(&ask.through) || ask.start > ask.through {
        return Err("start and through: YYYY-MM-DD, start first".into());
    }
    let newest_first = match ask.order.as_deref() {
        None | Some("oldest-first") => false,
        Some("newest-first") => true,
        Some(o) => return Err(format!("order {o:?}: oldest-first or newest-first")),
    };
    Ok((symbols, newest_first))
}

fn queue(a: &App, ask: Ask, from: &str) -> Result<Run, String> {
    let (symbols, newest_first) = checked(&ask)?;
    let created = unix_seconds();
    let run = Run {
        id: format!("r{}", (created * 1000.0) as u64),
        feed: ask.feed,
        symbols,
        start: ask.start,
        through: ask.through,
        newest_first,
        created,
        state: "queued".into(),
        note: String::new(),
        days: BTreeMap::new(),
        planned: 0,
        from: from.into(),
    };
    lock(&a.runs).push(run.clone());
    save(a);
    a.events.push(
        format!("queued {}", run.id),
        format!(
            "{} {} {}..{}",
            run.feed.name(),
            run.symbols.join(","),
            run.start,
            run.through
        ),
    );
    Ok(run)
}

/// Sessions between two days, inclusive, from Alpaca's calendar.
async fn sessions(a: &App, start: &str, through: &str) -> Result<Vec<String>, String> {
    let body = a
        .alpaca
        .get(
            &format!(
                "{}/v2/calendar?start={start}&end={through}",
                a.args.calendar_url
            ),
            &AtomicBool::new(false),
        )
        .await
        .map_err(|e| format!("market calendar: {e}"))?;
    let v: Vec<Value> =
        serde_json::from_slice(&body).map_err(|e| format!("market calendar: {e}"))?;
    Ok(v.iter()
        .filter_map(|d| d["date"].as_str().map(str::to_owned))
        .collect())
}

/// Batches the store holds for Alpaca.
async fn held(a: &App) -> Result<std::collections::HashSet<String>, String> {
    let v: Value = a
        .http
        .get(format!("{}/batches/alpaca", a.store))
        .send()
        .await
        .map_err(|e| format!("store: {e}"))?
        .json()
        .await
        .map_err(|e| format!("store: {e}"))?;
    Ok(v["batches"]
        .as_object()
        .map(|m| {
            m.iter()
                .filter(|(_, b)| b["how"] == "filed")
                .map(|(k, _)| k.clone())
                .collect()
        })
        .unwrap_or_default())
}

/// The capture service that is live, or when its hours begin too soon to start a day.
async fn yielding(a: &App) -> Option<String> {
    for (name, url) in &a.yield_to {
        // Unreachable, or not saying it's idle, counts as live: fail safe.
        let Ok(r) = a
            .http
            .get(format!("{url}/health"))
            .timeout(Duration::from_secs(3))
            .send()
            .await
        else {
            return Some(format!("{name} is unreachable (counted as live)"));
        };
        let st = r
            .json::<Value>()
            .await
            .ok()
            .and_then(|v| v["state"].as_str().map(str::to_owned));
        if st.as_deref() != Some("idle") {
            return Some(format!("{name} is live"));
        }
    }
    let hours = a.args.capture_hours?;
    let now = (unix_seconds() * 1e9) as u64;
    let (start, _) = hours.next(now, &tinytrader_schedule::Calendar::default())?;
    (start > now && start - now < 15 * 60 * 1_000_000_000).then(|| {
        format!(
            "capture hours begin {}",
            tinytrader_schedule::describe(start)
        )
    })
}

fn set_run(a: &App, id: &str, f: impl FnOnce(&mut Run)) {
    if let Some(r) = lock(&a.runs).iter_mut().find(|r| r.id == id) {
        f(r);
    }
    save(a);
}

/// How a day in flight ended.
#[derive(Debug)]
enum DayEnd {
    /// Its outcome is recorded (filed, held, failed, deferred, ...).
    Recorded,
    /// It has to wait for a later pass, its spool kept: `backoff` after a rate
    /// limit, an outage or a busy store.
    Wait { why: String, backoff: bool },
}

/// The next day of a run to start: one not yet tried first, then a failed one
/// with attempts left; never one already in flight.
fn next_day<'p>(
    plan: &'p [(String, String)],
    run: &Run,
    flying: &HashSet<String>,
) -> Option<&'p (String, String)> {
    let key = |(s, d): &(String, String)| format!("{s} {d}");
    plan.iter()
        .find(|p| !flying.contains(&key(p)) && !run.days.contains_key(&key(p)))
        .or_else(|| {
            plan.iter().find(|p| {
                !flying.contains(&key(p))
                    && run
                        .days
                        .get(&key(p))
                        .is_some_and(|d| d.state == "failed" && d.attempts < ATTEMPTS)
            })
        })
}

/// The days of a run, in its order: each session from Alpaca's calendar, for
/// each symbol.
async fn plan_of(
    a: &App,
    run_start: &str,
    run_through: &str,
    symbols: &[String],
    newest_first: bool,
) -> Result<(Vec<String>, Vec<(String, String)>), String> {
    let through = run_through.to_owned().min(last_complete_day());
    let days = if run_start > through.as_str() {
        Vec::new()
    } else {
        sessions(a, run_start, &through).await?
    };
    let mut plan: Vec<(String, String)> = Vec::new();
    for d in &days {
        for s in symbols {
            plan.push((s.clone(), d.clone()));
        }
    }
    if newest_first {
        plan.reverse();
    }
    Ok((days, plan))
}

/// Work through one run, up to `HISTORY_CONCURRENCY` days at a time; returns
/// when it's finished, stopped, or has to wait.
async fn work(a: &Shared, id: &str) {
    let Some(run) = lock(&a.runs).iter().find(|r| r.id == id).cloned() else {
        return;
    };
    // Live capture: no day starts while it's live, and days in flight stop at
    // their next page (their pages kept) once it is.
    if let Some(why) = yielding(a).await {
        a.observer
            .set_state(State::Idle, &format!("waiting: {why}"));
        return;
    }
    a.alpaca.hold.store(false, Ordering::Relaxed);
    let plan = match plan_of(a, &run.start, &run.through, &run.symbols, run.newest_first).await {
        Ok((_, p)) => p,
        Err(e) => {
            a.observer.set_state(State::Degraded, &e);
            return;
        }
    };
    set_run(a, id, |r| {
        r.planned = plan.len();
        r.state = "running".into();
    });
    let _ = std::fs::create_dir_all(a.args.state.join("spool"));
    let mut set: tokio::task::JoinSet<(String, DayEnd)> = tokio::task::JoinSet::new();
    let mut flying: HashSet<String> = HashSet::new();
    let mut wait: Option<(String, bool)> = None;
    let mut tick = tokio::time::interval(YIELD_CHECK);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tick.tick().await;
    loop {
        while wait.is_none() && set.len() < a.args.concurrency as usize {
            let Some(cur) = lock(&a.runs).iter().find(|r| r.id == id).cloned() else {
                break;
            };
            if cur.state == "stopped" {
                break;
            }
            let Some((sym, d)) = next_day(&plan, &cur, &flying).cloned() else {
                break;
            };
            if let Some(DayRecord { attempts, note, .. }) = cur.days.get(&format!("{sym} {d}")) {
                a.events.push(
                    format!("retrying {sym} {d}"),
                    format!("attempt {} of {ATTEMPTS}, after: {note}", attempts + 1),
                );
            }
            flying.insert(format!("{sym} {d}"));
            set.spawn(day_task(Arc::clone(a), id.to_owned(), run.feed, sym, d));
        }
        if set.is_empty() {
            break;
        }
        tokio::select! {
            joined = set.join_next() => match joined {
                Some(Ok((key, end))) => {
                    flying.remove(&key);
                    if let DayEnd::Wait { why, backoff } = end {
                        wait.get_or_insert((why, backoff));
                    }
                }
                Some(Err(e)) => {
                    a.observer.record_error(&format!("a day's task stopped: {e}"));
                    wait.get_or_insert((format!("a day's task stopped: {e}"), true));
                }
                None => break,
            },
            _ = tick.tick() => {
                if let Some(why) = yielding(a).await {
                    a.alpaca.hold.store(true, Ordering::Relaxed);
                    wait.get_or_insert((format!("waiting: {why}"), false));
                }
            }
        }
    }
    if let Some((why, backoff)) = wait {
        if backoff {
            a.observer
                .set_state(State::Degraded, &format!("{why}; waiting"));
            tokio::time::sleep(BACKOFF).await;
        } else {
            a.observer.set_state(State::Idle, &why);
        }
        return;
    }
    let Some(cur) = lock(&a.runs).iter().find(|r| r.id == id).cloned() else {
        return;
    };
    if cur.state == "stopped" {
        return;
    }
    set_run(a, id, |r| {
        let count = |s: &str| r.days.values().filter(|d| d.state == s).count();
        let (failed, unbound, deferred) = (
            count("failed"),
            count("conditions_unbound"),
            count("deferred"),
        );
        r.state = if failed + unbound + deferred > 0 {
            "failed".into()
        } else {
            "done".into()
        };
        let mut notes = Vec::new();
        if failed > 0 {
            notes.push(format!("{failed} days failed {ATTEMPTS} times; resume the run (POST /runs/{}/resume) to try them again", r.id));
        }
        if deferred > 0 {
            notes.push(format!("{deferred} days deferred with their spools kept; resume the run once the store can answer for them"));
        }
        if unbound > 0 {
            notes.push(format!("{unbound} days' conditions don't bind to the held main; a person must decide (see each day's note)"));
        }
        r.note = notes.join("; ");
    });
}

async fn day_task(a: Shared, id: String, feed: Feed, sym: String, d: String) -> (String, DayEnd) {
    let key = format!("{sym} {d}");
    let end = run_day(&a, &id, feed, &sym, &d).await;
    (key, end)
}

/// One symbol-day: skipped if held, else built (or taken from a complete
/// spool) and filed.
#[allow(clippy::too_many_lines)]
async fn run_day(a: &App, id: &str, feed: Feed, sym: &str, d: &str) -> DayEnd {
    let key = format!("{sym} {d}");
    let record = |rec: DayRecord| {
        set_run(a, id, |r| {
            r.days.insert(key.clone(), rec);
        });
        DayEnd::Recorded
    };
    let spool = a.args.state.join("spool");
    let ids = feed.batches(sym, d);
    let held = match held(a).await {
        Ok(h) => h,
        Err(e) => {
            return DayEnd::Wait {
                why: e,
                backoff: true,
            };
        }
    };
    if ids.iter().all(|b| held.contains(b)) {
        day::clear_spool(feed, sym, d, &spool);
        return record(DayRecord::noted("held", String::new()));
    }
    let main_held = held.contains(&ids[0]);
    a.observer
        .set_state(State::Ready, &format!("{} {sym} {d}", feed.name()));
    // A complete spool kept from an earlier pass is filed as it was built; only
    // without one is the day fetched (or its lanes resumed from their spool).
    let kept = {
        let (sym, d, spool) = (sym.to_owned(), d.to_owned(), spool.clone());
        tokio::task::spawn_blocking(move || day::spooled(feed, &sym, &d, &spool))
            .await
            .unwrap_or_else(|e| Err(format!("reading the spool failed: {e}")))
    };
    // A spool that couldn't be read (not one that's damaged) isn't rebuilt over:
    // the day waits.
    let kept = match kept {
        Ok(k) => k,
        Err(e) => {
            a.events.push(format!("deferred {sym} {d}"), e.clone());
            return record(DayRecord::noted(
                "deferred",
                format!("{e}; the spool is kept, and resuming the run tries it"),
            ));
        }
    };
    let built = match kept {
        Some(kept) => Ok(kept),
        None => {
            // Room for the day's spool and batches first.
            let free = fs4::available_space(&spool).unwrap_or(0);
            let floor = (a.args.min_free_gb * 1e9) as u64;
            if free < floor {
                let why = format!(
                    "{:.1} GB free in {}, under HISTORY_MIN_FREE_GB ({}); {sym} {d} waits",
                    free as f64 / 1e9,
                    spool.display(),
                    a.args.min_free_gb
                );
                a.events
                    .push(format!("waiting for disk {sym} {d}"), why.clone());
                return DayEnd::Wait { why, backoff: true };
            }
            day::build(&a.alpaca, feed, sym, d, &spool).await
        }
    };
    let attempts = lock(&a.runs)
        .iter()
        .find(|r| r.id == id)
        .and_then(|r| r.days.get(&key))
        .map_or(0, |r| r.attempts);
    // A day failed: tried again while it has attempts left (its lanes kept, so
    // a retry resumes them); for good (`last`), its spool goes.
    let fail = |e: String, last: bool| {
        let n = if last { ATTEMPTS } else { attempts + 1 };
        if n >= ATTEMPTS {
            day::clear_spool(feed, sym, d, &spool);
        }
        a.observer.record_error(&format!("{sym} {d}: {e}"));
        let again = if n < ATTEMPTS {
            "; tried again later in the run"
        } else {
            "; no attempts left"
        };
        a.events.push(
            format!("failed {sym} {d}"),
            format!("attempt {n} of {ATTEMPTS}: {e}{again}"),
        );
        let mut rec = DayRecord::noted("failed", e);
        rec.attempts = n;
        record(rec)
    };
    let failed = |e: String| fail(e, false);
    let built = match built {
        Ok(b) => b,
        Err(f) if f.transient() => {
            let backoff = !matches!(f, Failure::Paused);
            return DayEnd::Wait {
                why: format!("{sym} {d}: {f}"),
                backoff,
            };
        }
        // A row the layouts can't hold: fetching again would only repeat it.
        Err(f @ Failure::Encode(_)) => return fail(f.to_string(), true),
        Err(f) => return failed(f.to_string()),
    };
    let filed = async {
        // Main filed on an earlier pass: only its sidecars are left, each filed
        // only if it was built against that main.
        if !main_held {
            day::file(
                &a.http,
                &a.store,
                a.store_token.as_deref(),
                &built.batches[0],
            )
            .await?;
        }
        for (side, bid) in built.batches.iter().zip(&ids).skip(1) {
            if main_held {
                if held.contains(bid) {
                    continue;
                }
                day::check_binding(&a.http, &a.store, feed, sym, d, side).await?;
            }
            day::file(&a.http, &a.store, a.store_token.as_deref(), side).await?;
        }
        Ok::<(), day::FileError>(())
    }
    .await;
    match filed {
        Ok(()) => {
            day::clear_spool(feed, sym, d, &spool);
            let out = built.outcome;
            if out.unparsed_quote_conditions > 0 {
                a.events.push(
                    format!("quote conditions unparsed {sym} {d}"),
                    format!(
                        "{} quotes' conditions couldn't be held; their records are marked unparsed",
                        out.unparsed_quote_conditions
                    ),
                );
            }
            a.events.push(
                format!("filed {sym} {d}"),
                format!(
                    "{} trades, {} quotes, {} pages",
                    out.trades, out.quotes, out.pages
                ),
            );
            record(DayRecord {
                state: "committed".into(),
                trades: Some(out.trades),
                quotes: Some(out.quotes),
                pages: Some(out.pages),
                bytes: Some(out.bytes),
                note: String::new(),
                attempts,
            })
        }
        // A busy store: the day is tried again on the next pass, its spool kept,
        // as for a rate limit.
        Err(e) if e.busy => DayEnd::Wait {
            why: e.message,
            backoff: true,
        },
        // Couldn't check the conditions against the held main yet: the day waits
        // for the run to be resumed, its spool kept.
        Err(e) if e.later => {
            a.events
                .push(format!("deferred {sym} {d}"), e.message.clone());
            record(DayRecord::noted(
                "deferred",
                format!(
                    "{}; the spool is kept, and resuming the run tries it",
                    e.message
                ),
            ))
        }
        // Conditions that don't bind to the held main: kept, for a person to look
        // at and decide. Trying again would only repeat it.
        Err(e) if e.unbound => {
            a.observer
                .record_error(&format!("{sym} {d}: {}", e.message));
            a.events
                .push(format!("conditions unbound {sym} {d}"), e.message.clone());
            let kept: Vec<String> = built
                .batches
                .iter()
                .map(|b| b.display().to_string())
                .collect();
            let note = format!(
                "{}. A person must decide: the rebuilt batches are kept in {}; compare them with the held day (GET /coverage?provider=alpaca&dataset={}&symbol={sym}&start={d}&end={d}). Resuming the run repeats the mismatch.",
                e.message,
                kept.join(", "),
                feed.dataset()
            );
            record(DayRecord::noted("conditions_unbound", note))
        }
        // The store keeps what it filed; the spool goes either way.
        Err(e) => {
            day::clear_spool(feed, sym, d, &spool);
            failed(e.message)
        }
    }
}

async fn worker(a: Shared) {
    loop {
        // The nightly top-off: the last ten days, after 20:40 New York.
        if !a.args.topoff.is_empty() {
            let (today, hhmm) = ny_now();
            if hhmm.as_str() >= "20:40" && *lock(&a.last_topoff) != today {
                *lock(&a.last_topoff) = today.clone();
                let start = time::Date::parse(
                    &today,
                    time::macros::format_description!("[year]-[month]-[day]"),
                )
                .map(|d| (d - time::Duration::days(10)).to_string())
                .unwrap_or_else(|_| today.clone());
                let _ = queue(
                    &a,
                    Ask {
                        feed: Feed::Sip,
                        symbols: a.args.topoff.clone(),
                        start,
                        through: today,
                        order: None,
                        dry_run: false,
                    },
                    "top-off",
                );
            }
        }
        let next = lock(&a.runs)
            .iter()
            .find(|r| r.state == "queued" || r.state == "running")
            .map(|r| r.id.clone());
        match next {
            Some(id) => work(&a, &id).await,
            None => a.observer.set_state(State::Idle, "no runs queued"),
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
}

/// A run planned, not queued: its trading days, how many symbol-days the store
/// already holds, and what fetching the rest would take (from the days this
/// service has fetched, or assumed when it has none).
async fn dry_run(a: &App, ask: &Ask) -> Result<Value, String> {
    let (symbols, newest_first) = checked(ask)?;
    let (days, plan) = plan_of(a, &ask.start, &ask.through, &symbols, newest_first).await?;
    let held = held(a).await?;
    let (mut whole, mut partly) = (0, 0);
    for (s, d) in &plan {
        let ids = ask.feed.batches(s, d);
        let n = ids.iter().filter(|b| held.contains(*b)).count();
        if n == ids.len() {
            whole += 1;
        } else if n > 0 {
            partly += 1;
        }
    }
    let to_fetch = plan.len() - whole;
    // What a day has taken so far, for this feed's symbols (any symbol if none).
    let (pages, bytes, n, basis) = {
        let runs = lock(&a.runs);
        let done: Vec<&DayRecord> = runs
            .iter()
            .filter(|r| r.feed == ask.feed)
            .flat_map(|r| r.days.iter())
            .filter(|(k, d)| {
                d.state == "committed" && symbols.iter().any(|s| k.starts_with(&format!("{s} ")))
            })
            .map(|(_, d)| d)
            .collect();
        let done = if done.is_empty() {
            runs.iter()
                .filter(|r| r.feed == ask.feed)
                .flat_map(|r| r.days.values())
                .filter(|d| d.state == "committed")
                .collect()
        } else {
            done
        };
        let n = done.len();
        if n == 0 {
            (
                200.0,
                50_000_000.0,
                0,
                "assumed: no days of this feed fetched yet".to_owned(),
            )
        } else {
            let pages = done.iter().filter_map(|d| d.pages).sum::<usize>() as f64 / n as f64;
            let bytes = done.iter().filter_map(|d| d.bytes).sum::<u64>() as f64 / n as f64;
            (
                pages,
                bytes,
                n,
                format!("the mean of {n} days of this feed fetched"),
            )
        }
    };
    let requests = (pages * to_fetch as f64).ceil();
    Ok(json!({
        "dry_run": true, "feed": ask.feed, "symbols": symbols, "start": ask.start,
        "through": ask.through.clone().min(last_complete_day()), "order": if newest_first { "newest-first" } else { "oldest-first" },
        "trading_days": days.len(), "days": days, "symbol_days": plan.len(),
        "held": whole, "partly_held": partly, "to_fetch": to_fetch,
        "estimate": {
            "basis": basis, "days_measured": n, "pages_per_day": pages.round(), "requests": requests,
            "partly_held_requests": (pages * partly as f64).ceil(),
            "partly_held_note": format!(
                "{partly} symbol-days are held without every sidecar (a SIP day filed before quote conditions): each is a full refetch of both lanes, counted in to_fetch and requests"
            ),
            "minutes_at_max_rpm": (requests / f64::from(a.args.max_rpm.max(1))).ceil(),
            "bytes": (bytes * to_fetch as f64).round(),
        },
    }))
}

async fn post_run(AxState(a): AxState<Shared>, Json(ask): Json<Ask>) -> (StatusCode, Json<Value>) {
    if ask.dry_run {
        return match dry_run(&a, &ask).await {
            Ok(v) => (StatusCode::OK, Json(v)),
            Err(e) if checked(&ask).is_err() => {
                (StatusCode::BAD_REQUEST, Json(json!({ "error": e })))
            }
            Err(e) => (StatusCode::SERVICE_UNAVAILABLE, Json(json!({ "error": e }))),
        };
    }
    match queue(&a, ask, "request") {
        Ok(r) => (StatusCode::OK, Json(json!(r))),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({ "error": e }))),
    }
}

async fn stop_run(
    AxState(a): AxState<Shared>,
    UrlPath(id): UrlPath<String>,
) -> (StatusCode, Json<Value>) {
    let mut found = false;
    set_run(&a, &id, |r| {
        if r.state == "queued" || r.state == "running" {
            r.state = "stopped".into();
        }
        found = true;
    });
    if found {
        (StatusCode::OK, Json(json!({ "stopped": id })))
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "no such run" })),
        )
    }
}

/// A stopped or failed run, queued again: its failed and deferred days are
/// tried afresh, the days it filed or found held are skipped (and the store
/// is asked again for each day before it's fetched).
async fn resume_run(
    AxState(a): AxState<Shared>,
    UrlPath(id): UrlPath<String>,
) -> (StatusCode, Json<Value>) {
    let mut was: Option<String> = None;
    set_run(&a, &id, |r| {
        was = Some(r.state.clone());
        if r.state == "stopped" || r.state == "failed" {
            r.days
                .retain(|_, d| d.state != "failed" && d.state != "deferred");
            r.state = "queued".into();
            r.note.clear();
        }
    });
    match was.as_deref() {
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "no such run" })),
        ),
        Some("stopped" | "failed") => {
            a.events.push(format!("resumed {id}"), String::new());
            (StatusCode::OK, Json(json!({ "resumed": id })))
        }
        Some(s) => (
            StatusCode::CONFLICT,
            Json(
                json!({ "error": format!("the run is {s}; only a stopped or failed run resumes") }),
            ),
        ),
    }
}

fn runs_view(a: &App) -> Value {
    let runs = lock(&a.runs).clone();
    let list: Vec<Value> = runs
        .iter()
        .rev()
        .map(|r| {
            let count = |s: &str| r.days.values().filter(|d| d.state == s).count();
            json!({
                "id": r.id, "feed": r.feed, "symbols": r.symbols, "start": r.start, "through": r.through,
                "newest_first": r.newest_first, "created": r.created, "state": r.state, "note": r.note, "from": r.from,
                "planned": r.planned, "committed": count("committed"), "held": count("held"), "failed": count("failed"),
                "bytes": r.days.values().filter_map(|d| d.bytes).sum::<u64>(), "days": r.days,
            })
        })
        .collect();
    json!({ "runs": list })
}

/// The service, ready to work: its pace, its store, its runs.
fn build_app(
    args: Args,
    key_id: String,
    secret_key: String,
    store_token: Option<String>,
    runs: Vec<Run>,
) -> Shared {
    let http = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .build()
        .expect("http client");
    let observer = Observer::new("alpaca-history", env!("CARGO_PKG_VERSION"));
    let mut schedule = vec![
        json!({ "name": "Runs", "kind": "event", "when": format!("as queued, up to {} days at a time", args.concurrency) }),
    ];
    if !args.topoff.is_empty() {
        schedule.push(json!({ "name": format!("Top-off: {} (SIP)", args.topoff.join(", ")), "kind": "daily", "when": "20:40 New York, the last ten days" }));
    }
    if !args.yield_to.is_empty() {
        schedule.insert(0, json!({ "name": "Yields to live capture", "kind": "yield", "class": "background", "when": "waits while capture is live" }));
    }
    observer.describe(json!({
        "role": "Alpaca's historical trades and quotes, a day at a time, filed by the store.",
        "upstream": [{ "id": "alpaca", "label": "Alpaca", "sub": "history API", "kind": "flow", "via": "https" }],
        "streams": { "runs": "/runs", "events": "/events" },
        "schedule": schedule,
    }));
    let events = Events::default();
    let app = Arc::new(App {
        alpaca: Alpaca {
            http: http.clone(),
            data: args.data_url.trim_end_matches('/').to_owned(),
            key_id,
            secret: secret_key,
            pace: Pace::new(args.max_rpm, 2 * args.concurrency),
            hold: AtomicBool::new(false),
            events: events.clone(),
        },
        http,
        store: args.store.trim_end_matches('/').to_owned(),
        store_token,
        runs: Mutex::new(runs),
        events,
        observer: observer.clone(),
        yield_to: args
            .yield_to
            .iter()
            .filter_map(|y| {
                y.split_once('=').map(|(n, u)| {
                    (
                        n.trim().to_owned(),
                        u.trim().trim_end_matches('/').to_owned(),
                    )
                })
            })
            .collect(),
        last_topoff: Mutex::new(String::new()),
        args,
    });
    {
        let (req, lim, a2) = (
            app.alpaca.pace.requests.clone(),
            app.alpaca.pace.limited.clone(),
            app.clone(),
        );
        observer.set_metrics(move |w| {
            use std::sync::atomic::Ordering::Relaxed;
            w.counter(
                "tinytrader_history_requests_total",
                "Requests made to Alpaca.",
                &[],
                req.load(Relaxed) as f64,
            );
            w.counter(
                "tinytrader_history_rate_limited_total",
                "429s from Alpaca.",
                &[],
                lim.load(Relaxed) as f64,
            );
            let runs = lock(&a2.runs);
            let days = |s: &str| {
                runs.iter()
                    .flat_map(|r| r.days.values())
                    .filter(|d| d.state == s)
                    .count() as f64
            };
            w.gauge(
                "tinytrader_history_days",
                "Days by outcome, across runs kept.",
                &[("outcome", "committed")],
                days("committed"),
            );
            w.gauge(
                "tinytrader_history_days",
                "Days by outcome, across runs kept.",
                &[("outcome", "failed")],
                days("failed"),
            );
        });
    }
    app
}

fn router(app: &Shared) -> Router {
    Router::new()
        .route(
            "/runs",
            get(|AxState(a): AxState<Shared>| async move { Json(runs_view(&a)) }).post(post_run),
        )
        .route("/runs/{id}/stop", post(stop_run))
        .route("/runs/{id}/resume", post(resume_run))
        // A web page of another origin can't start, stop or resume a run.
        .layer(axum::middleware::from_fn(
            tinytrader_observe::guard::same_origin_writes,
        ))
        .with_state(app.clone())
        .merge(app.observer.router())
        .merge(app.events.router())
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let _ = rustls::crypto::ring::default_provider().install_default();
    let (key_id, secret_key) = match (
        secret("ALPACA_HISTORY_KEY_ID"),
        secret("ALPACA_HISTORY_SECRET_KEY"),
    ) {
        (Ok(k), Ok(s)) => (k, s),
        (Err(e), _) | (_, Err(e)) => {
            eprintln!("alpaca-history: {e}");
            std::process::exit(2);
        }
    };
    let store_token = match tinytrader_observe::optional_secret("HISTORY_STORE_TOKEN") {
        Ok(t) => t,
        Err(e) => {
            eprintln!("alpaca-history: {e}");
            std::process::exit(2);
        }
    };
    if let Err(e) = std::fs::create_dir_all(&args.state) {
        eprintln!("alpaca-history: cannot use {}: {e}", args.state.display());
        std::process::exit(2);
    }
    let runs: Vec<Run> = std::fs::read(args.state.join("runs.json"))
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or_default();
    let spool = args.state.join("spool");
    let max_age = Duration::from_secs(args.spool_max_age_days * 86_400);
    let app = build_app(args, key_id, secret_key, store_token, runs);
    for (what, why) in day::sweep(&spool, max_age) {
        app.events.push(format!("swept {what}"), why);
    }
    let listen = listen_address("HISTORY_LISTEN", "127.0.0.1:9124");
    let router = router(&app);
    let listener = match tokio::net::TcpListener::bind(&listen).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("alpaca-history: cannot listen on {listen}: {e}");
            std::process::exit(2);
        }
    };
    eprintln!(
        "alpaca-history: store at {}; endpoints on http://{listen}",
        app.store
    );
    tokio::spawn(worker(app.clone()));
    if let Err(e) = axum::serve(listener, router).await {
        eprintln!("alpaca-history: http server stopped: {e}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{DAY, FakeAlpaca, FakeStore, Shape, scratch};

    const SMALL: Shape = Shape::new(2, 5);

    fn app_for(f: &FakeAlpaca, s: &FakeStore, name: &str, extra: &[&str]) -> Shared {
        let dir = scratch(name);
        let mut argv = vec![
            "alpaca-history",
            "--store",
            &s.url,
            "--data-url",
            &f.url,
            "--calendar-url",
            &f.url,
            "--max-rpm",
            "100000",
        ];
        argv.extend(["--state", dir.to_str().unwrap()]);
        if !extra.contains(&"--min-free-gb") {
            argv.extend(["--min-free-gb", "0"]);
        }
        argv.extend(extra);
        build_app(
            Args::parse_from(argv),
            "k".into(),
            "s".into(),
            None,
            Vec::new(),
        )
    }

    fn ask(symbols: &[&str], start: &str, through: &str) -> Ask {
        Ask {
            feed: Feed::Sip,
            symbols: symbols.iter().map(|s| (*s).to_owned()).collect(),
            start: start.into(),
            through: through.into(),
            order: None,
            dry_run: false,
        }
    }

    fn run_of(a: &App, id: &str) -> Run {
        lock(&a.runs).iter().find(|r| r.id == id).cloned().unwrap()
    }

    async fn serve(a: &Shared) -> String {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", l.local_addr().unwrap());
        let r = router(a);
        tokio::spawn(async move { axum::serve(l, r).await.unwrap() });
        url
    }

    /// A day that fails is tried again in the same run, and counts once filed.
    #[tokio::test]
    async fn a_failed_day_is_tried_again() {
        let (f, s) = (
            FakeAlpaca::start(SMALL, SMALL).await,
            FakeStore::start().await,
        );
        f.fault("trades", 0, &[403]);
        let a = app_for(&f, &s, "retry", &[]);
        let run = queue(&a, ask(&["SPY"], DAY, DAY), "test").unwrap();
        work(&a, &run.id).await;
        let r = run_of(&a, &run.id);
        let d = &r.days[&format!("SPY {DAY}")];
        assert_eq!(
            (r.state.as_str(), d.state.as_str(), d.attempts),
            ("done", "committed", 1),
            "{r:?}"
        );
        assert_eq!(
            s.filed(),
            Feed::Sip.batches("SPY", DAY).into_iter().collect()
        );
    }

    /// A day failing every time is left failed after three attempts, and the run
    /// says how to try it again.
    #[tokio::test]
    async fn a_day_failing_every_time_stops_after_three_attempts() {
        let (f, s) = (
            FakeAlpaca::start(SMALL, SMALL).await,
            FakeStore::start().await,
        );
        f.fault("trades", 0, &[403; 10]);
        let a = app_for(&f, &s, "give-up", &[]);
        let run = queue(&a, ask(&["SPY"], DAY, DAY), "test").unwrap();
        work(&a, &run.id).await;
        let r = run_of(&a, &run.id);
        let d = &r.days[&format!("SPY {DAY}")];
        assert_eq!(
            (r.state.as_str(), d.state.as_str(), d.attempts),
            ("failed", "failed", ATTEMPTS),
            "{r:?}"
        );
        assert_eq!(
            f.log().iter().filter(|x| x.2 == 403).count(),
            ATTEMPTS as usize
        );
        assert!(r.note.contains("/resume"), "{}", r.note);
    }

    /// Several days in flight at once, each with both lanes, on one pace.
    #[tokio::test]
    async fn days_run_at_once_sharing_the_pace() {
        let (f, s) = (
            FakeAlpaca::start_with(SMALL, SMALL, 40).await,
            FakeStore::start().await,
        );
        let days = ["2026-09-29", "2026-09-30", DAY];
        f.sessions(&days);
        let a = app_for(&f, &s, "concurrent", &["--concurrency", "3"]);
        let run = queue(&a, ask(&["SPY"], days[0], DAY), "test").unwrap();
        work(&a, &run.id).await;
        let r = run_of(&a, &run.id);
        assert_eq!(
            (
                r.state.as_str(),
                r.days.values().filter(|d| d.state == "committed").count()
            ),
            ("done", 3),
            "{r:?}"
        );
        let most = f.most_in_flight.load(std::sync::atomic::Ordering::SeqCst);
        assert!(most > 2, "at most {most} requests in flight");
    }

    /// A dry run plans the run without queueing it: its trading days, what the
    /// store already holds (wholly, or without a sidecar), and estimates.
    #[tokio::test]
    async fn a_dry_run_plans_without_queueing() {
        let (f, s) = (
            FakeAlpaca::start(SMALL, SMALL).await,
            FakeStore::start().await,
        );
        f.sessions(&["2026-09-29", "2026-09-30", DAY]);
        s.hold(&Feed::Sip.batches("SPY", "2026-09-30"));
        s.hold(&Feed::Sip.batches("QQQ", DAY)[..2]);
        let a = app_for(&f, &s, "dry-run", &[]);
        let url = serve(&a).await;
        let http = reqwest::Client::new();
        let body = json!({ "feed": "sip", "symbols": ["SPY", "qqq"], "start": "2026-09-29", "through": DAY, "dry_run": true });
        let r = http
            .post(format!("{url}/runs"))
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
        let v: Value = r.json().await.unwrap();
        assert_eq!(v["days"], json!(["2026-09-29", "2026-09-30", DAY]));
        assert_eq!(
            (
                &v["trading_days"],
                &v["symbol_days"],
                &v["held"],
                &v["partly_held"],
                &v["to_fetch"]
            ),
            (&json!(3), &json!(6), &json!(1), &json!(1), &json!(5))
        );
        assert!(
            v["estimate"]["basis"]
                .as_str()
                .unwrap()
                .starts_with("assumed"),
            "{v}"
        );
        assert_eq!(v["estimate"]["requests"], json!(1000.0));
        assert_eq!(v["estimate"]["partly_held_requests"], json!(200.0));
        assert!(
            v["estimate"]["partly_held_note"]
                .as_str()
                .unwrap()
                .contains("full refetch")
        );
        let runs: Value = http
            .get(format!("{url}/runs"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(runs["runs"], json!([]), "nothing queued");
        let bad = json!({ "feed": "sip", "symbols": [], "start": "2026-09-29", "through": DAY, "dry_run": true });
        assert_eq!(
            http.post(format!("{url}/runs"))
                .json(&bad)
                .send()
                .await
                .unwrap()
                .status(),
            400
        );
    }

    /// A stopped or failed run resumes: queued again, its failed and deferred
    /// days tried afresh, the days it filed skipped; any other run is refused.
    #[tokio::test]
    async fn a_stopped_or_failed_run_resumes() {
        let (f, s) = (
            FakeAlpaca::start(SMALL, SMALL).await,
            FakeStore::start().await,
        );
        let (d1, d2) = ("2026-09-30", DAY);
        f.sessions(&[d1, d2]);
        let a = app_for(&f, &s, "resume", &[]);
        let url = serve(&a).await;
        let http = reqwest::Client::new();
        let id = queue(&a, ask(&["SPY"], d1, d2), "test").unwrap().id;
        let post = |path: String| {
            let http = http.clone();
            async move { http.post(path).send().await.unwrap().status().as_u16() }
        };
        assert_eq!(
            post(format!("{url}/runs/{id}/resume")).await,
            409,
            "a queued run doesn't resume"
        );
        set_run(&a, &id, |r| {
            r.state = "failed".into();
            r.days.insert(
                format!("SPY {d1}"),
                DayRecord::noted("committed", String::new()),
            );
            let mut failed = DayRecord::noted("failed", "refused".into());
            failed.attempts = ATTEMPTS;
            r.days.insert(format!("SPY {d2}"), failed);
        });
        assert_eq!(post(format!("{url}/runs/{id}/resume")).await, 200);
        let r = run_of(&a, &id);
        assert_eq!(
            (r.state.as_str(), r.days.keys().cloned().collect::<Vec<_>>()),
            ("queued", vec![format!("SPY {d1}")])
        );
        work(&a, &id).await;
        let r = run_of(&a, &id);
        assert_eq!(
            (
                r.state.as_str(),
                r.days[&format!("SPY {d2}")].state.as_str()
            ),
            ("done", "committed"),
            "{r:?}"
        );
        assert!(
            f.log().iter().all(|x| x.3 == d2),
            "the day already filed isn't fetched"
        );
        assert_eq!(
            post(format!("{url}/runs/{id}/resume")).await,
            409,
            "a done run doesn't resume"
        );
        let other = queue(&a, ask(&["QQQ"], d1, d2), "test").unwrap().id;
        assert_eq!(post(format!("{url}/runs/{other}/stop")).await, 200);
        assert_eq!(post(format!("{url}/runs/{other}/resume")).await, 200);
        assert_eq!(run_of(&a, &other).state, "queued");
        assert_eq!(post(format!("{url}/runs/nope/resume")).await, 404);
    }

    /// While live capture runs, no day starts; the run waits, nothing recorded.
    #[tokio::test]
    async fn live_capture_holds_the_run() {
        let (f, s) = (
            FakeAlpaca::start(SMALL, SMALL).await,
            FakeStore::start().await,
        );
        let live = axum::Router::new().route(
            "/health",
            get(|| async { axum::Json(json!({ "state": "ready" })) }),
        );
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let yield_to = format!("relay=http://{}", l.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(l, live).await.unwrap() });
        let a = app_for(&f, &s, "yield", &["--yield-to", &yield_to]);
        let id = queue(&a, ask(&["SPY"], DAY, DAY), "test").unwrap().id;
        work(&a, &id).await;
        assert!(run_of(&a, &id).days.is_empty());
        assert!(f.log().is_empty());
    }

    /// A trade the layout can't hold fails the day for good at once: no page
    /// fetched again, no retries, its spool gone.
    #[tokio::test]
    async fn a_day_that_cant_be_encoded_fails_at_once() {
        let t = Shape {
            bad: Some(3),
            ..SMALL
        };
        let (f, s) = (FakeAlpaca::start(t, SMALL).await, FakeStore::start().await);
        let a = app_for(&f, &s, "encode", &[]);
        let id = queue(&a, ask(&["SPY"], DAY, DAY), "test").unwrap().id;
        work(&a, &id).await;
        let r = run_of(&a, &id);
        let d = &r.days[&format!("SPY {DAY}")];
        assert_eq!(
            (r.state.as_str(), d.state.as_str(), d.attempts),
            ("failed", "failed", ATTEMPTS),
            "{r:?}"
        );
        assert!(d.note.contains("can't encode"), "{}", d.note);
        assert!(
            f.log().iter().all(|x| x.2 == 200) && f.log().len() == 4,
            "{:?}",
            f.log()
        );
        let spool = a.args.state.join("spool");
        assert_eq!(
            std::fs::read_dir(&spool).unwrap().count(),
            0,
            "the spool is gone"
        );
    }

    /// Under the free-space floor no day starts; the run waits.
    #[tokio::test]
    async fn a_day_waits_for_disk() {
        let (f, s) = (
            FakeAlpaca::start(SMALL, SMALL).await,
            FakeStore::start().await,
        );
        let a = app_for(&f, &s, "disk", &["--min-free-gb", "1000000000"]);
        let id = queue(&a, ask(&["SPY"], DAY, DAY), "test").unwrap().id;
        work(&a, &id).await;
        let r = run_of(&a, &id);
        assert!(r.days.is_empty() && r.state == "running", "{r:?}");
        assert!(f.log().is_empty());
    }

    /// Capture that can't be reached counts as live: nothing starts.
    #[tokio::test]
    async fn unreachable_capture_counts_as_live() {
        let (f, s) = (
            FakeAlpaca::start(SMALL, SMALL).await,
            FakeStore::start().await,
        );
        let a = app_for(
            &f,
            &s,
            "unreachable",
            &["--yield-to", "relay=http://127.0.0.1:9"],
        );
        let id = queue(&a, ask(&["SPY"], DAY, DAY), "test").unwrap().id;
        work(&a, &id).await;
        assert!(run_of(&a, &id).days.is_empty());
        assert!(f.log().is_empty());
    }
}
