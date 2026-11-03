//! Alpaca's historical trades and quotes (SIP or IEX), a day at a time, into
//! the store (decision 0014).
//!
//! - `POST /runs` with `{ feed, symbols, start, through, order }` (days
//!   inclusive; `order` `oldest-first` or `newest-first`) queues a run.
//! - `GET /runs`: every run, its progress, and each day's outcome.
//! - `POST /runs/{id}/stop` stops one after its current day.
//!
//! A run plans its days from Alpaca's market calendar (sessions only, early
//! closes included; without the calendar nothing is planned), skips days the
//! store already holds, and never asks for a day that hasn't ended. Each day
//! is both lanes fetched to their last page, encoded, read back and checked,
//! and filed by the store as two batches (records and trade conditions). A
//! failed day is recorded and the run goes on; a rate-limited one waits and
//! is tried again. A nightly top-off (`HISTORY_TOPOFF`) queues the last ten
//! days for its symbols at 20:40 New York.
//!
//! It is background work (decision 0011): while a capture service is live,
//! or its hours begin soon, it waits. It uses its own Alpaca key
//! (`ALPACA_HISTORY_KEY_ID`, `ALPACA_HISTORY_SECRET_KEY`, or `*_FILE`): the
//! data limit is per key, so it never competes with live use. The store's
//! write token, when the store requires one: `HISTORY_STORE_TOKEN` or
//! `HISTORY_STORE_TOKEN_FILE`.

mod day;
mod fetch;

use std::collections::BTreeMap;
use std::path::PathBuf;
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
    #[arg(long, env = "HISTORY_DATA_URL", default_value = "https://data.alpaca.markets")]
    data_url: String,
    /// Alpaca's trading API, for the market calendar (the key's account: paper).
    #[arg(long, env = "HISTORY_CALENDAR_URL", default_value = "https://paper-api.alpaca.markets")]
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
}

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
    !s.is_empty() && s.len() <= 10 && s.bytes().all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'.')
}

#[derive(Deserialize)]
struct Ask {
    feed: Feed,
    symbols: Vec<String>,
    start: String,
    through: String,
    #[serde(default)]
    order: Option<String>,
}

fn queue(a: &App, ask: Ask, from: &str) -> Result<Run, String> {
    let symbols: Vec<String> = ask.symbols.iter().map(|s| s.trim().to_uppercase()).collect();
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
    a.events.push(format!("queued {}", run.id), format!("{} {} {}..{}", run.feed.name(), run.symbols.join(","), run.start, run.through));
    Ok(run)
}

/// Sessions between two days, inclusive, from Alpaca's calendar.
async fn sessions(a: &App, start: &str, through: &str) -> Result<Vec<String>, String> {
    let body = a
        .alpaca
        .get(&format!("{}/v2/calendar?start={start}&end={through}", a.args.calendar_url))
        .await
        .map_err(|e| format!("market calendar: {e}"))?;
    let v: Vec<Value> = serde_json::from_slice(&body).map_err(|e| format!("market calendar: {e}"))?;
    Ok(v.iter().filter_map(|d| d["date"].as_str().map(str::to_owned)).collect())
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
        .map(|m| m.iter().filter(|(_, b)| b["how"] == "filed").map(|(k, _)| k.clone()).collect())
        .unwrap_or_default())
}

/// The capture service that is live, or when its hours begin too soon to start a day.
async fn yielding(a: &App) -> Option<String> {
    for (name, url) in &a.yield_to {
        let Ok(r) = a.http.get(format!("{url}/health")).timeout(Duration::from_secs(3)).send().await else {
            continue;
        };
        let st = r.json::<Value>().await.ok().and_then(|v| v["state"].as_str().map(str::to_owned));
        if st.as_deref() != Some("idle") {
            return Some(format!("{name} is live"));
        }
    }
    let hours = a.args.capture_hours?;
    let now = (unix_seconds() * 1e9) as u64;
    let (start, _) = hours.next(now, &tinytrader_schedule::Calendar::default())?;
    (start > now && start - now < 15 * 60 * 1_000_000_000)
        .then(|| format!("capture hours begin {}", tinytrader_schedule::describe(start)))
}

fn set_run(a: &App, id: &str, f: impl FnOnce(&mut Run)) {
    if let Some(r) = lock(&a.runs).iter_mut().find(|r| r.id == id) {
        f(r);
    }
    save(a);
}

/// Work through one run, a day at a time; returns when it's finished,
/// stopped, or has to wait.
async fn work(a: &App, id: &str) {
    let Some(run) = lock(&a.runs).iter().find(|r| r.id == id).cloned() else { return };
    let through = run.through.clone().min(last_complete_day());
    let days = if run.start > through {
        Vec::new()
    } else {
        match sessions(a, &run.start, &through).await {
            Ok(d) => d,
            Err(e) => {
                a.observer.set_state(State::Degraded, &e);
                return;
            }
        }
    };
    let mut plan: Vec<(String, String)> = Vec::new();
    for d in &days {
        for s in &run.symbols {
            plan.push((s.clone(), d.clone()));
        }
    }
    if run.newest_first {
        plan.reverse();
    }
    set_run(a, id, |r| {
        r.planned = plan.len();
        r.state = "running".into();
    });
    let spool = a.args.state.join("spool");
    for (sym, d) in plan {
        let key = format!("{sym} {d}");
        let Some(cur) = lock(&a.runs).iter().find(|r| r.id == id).cloned() else { return };
        if cur.state == "stopped" {
            return;
        }
        // Done, held or failed in this run already (a failed day is retried by
        // queueing the run again).
        if cur.days.contains_key(&key) {
            continue;
        }
        if let Some(why) = yielding(a).await {
            a.observer.set_state(State::Idle, &format!("waiting: {why}"));
            return;
        }
        let (main, cond) = run.feed.batches(&sym, &d);
        match held(a).await {
            Ok(h) if h.contains(&main) && h.contains(&cond) => {
                set_run(a, id, |r| {
                    r.days.insert(key, DayRecord { state: "held".into(), trades: None, quotes: None, pages: None, bytes: None, note: String::new() });
                });
                continue;
            }
            Ok(_) => {}
            Err(e) => {
                a.observer.set_state(State::Degraded, &e);
                return;
            }
        }
        a.observer.set_state(State::Ready, &format!("{} {sym} {d}", run.feed.name()));
        let _ = std::fs::create_dir_all(&spool);
        let result = match day::build(&a.alpaca, run.feed, &sym, &d, &spool).await {
            Ok((m, c, out)) => {
                let filed = async {
                    day::file(&a.http, &a.store, a.store_token.as_deref(), &m).await?;
                    day::file(&a.http, &a.store, a.store_token.as_deref(), &c).await
                }
                .await;
                // A busy store: the day is tried again on the next pass, its spool
                // kept, as for a rate limit.
                if let Err(e) = &filed
                    && e.busy
                {
                    a.observer.set_state(State::Degraded, &format!("{}; waiting", e.message));
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    return;
                }
                // The store keeps what it filed; the spool copies go either way.
                for dir in [&m, &c] {
                    if let Ok(rd) = std::fs::read_dir(dir) {
                        for e in rd.flatten() {
                            let _ = std::fs::remove_file(e.path());
                        }
                    }
                    let _ = std::fs::remove_dir(dir);
                }
                filed.map(|_| out).map_err(|e| (false, e.message))
            }
            Err(Failure::RateLimited) => Err((true, "rate limited".to_owned())),
            Err(e) => Err((false, e.to_string())),
        };
        match result {
            Ok(out) => {
                a.events.push(format!("filed {sym} {d}"), format!("{} trades, {} quotes, {} pages", out.trades, out.quotes, out.pages));
                set_run(a, id, |r| {
                    r.days.insert(key, DayRecord { state: "committed".into(), trades: Some(out.trades), quotes: Some(out.quotes), pages: Some(out.pages), bytes: Some(out.bytes), note: String::new() });
                });
            }
            Err((true, e)) => {
                // Rate limited: the day is tried again on the next pass.
                a.observer.set_state(State::Degraded, &format!("{e}; waiting"));
                tokio::time::sleep(Duration::from_secs(60)).await;
                return;
            }
            Err((false, e)) => {
                a.observer.record_error(&format!("{sym} {d}: {e}"));
                a.events.push(format!("failed {sym} {d}"), e.clone());
                set_run(a, id, |r| {
                    r.days.insert(key, DayRecord { state: "failed".into(), trades: None, quotes: None, pages: None, bytes: None, note: e });
                });
            }
        }
    }
    set_run(a, id, |r| {
        let failed = r.days.values().filter(|d| d.state == "failed").count();
        r.state = if failed > 0 { "failed".into() } else { "done".into() };
        r.note = if failed > 0 { format!("{failed} days failed; queue the run again to retry them") } else { String::new() };
    });
}

async fn worker(a: Shared) {
    loop {
        // The nightly top-off: the last ten days, after 20:40 New York.
        if !a.args.topoff.is_empty() {
            let (today, hhmm) = ny_now();
            if hhmm.as_str() >= "20:40" && *lock(&a.last_topoff) != today {
                *lock(&a.last_topoff) = today.clone();
                let start = time::Date::parse(&today, time::macros::format_description!("[year]-[month]-[day]"))
                    .map(|d| (d - time::Duration::days(10)).to_string())
                    .unwrap_or_else(|_| today.clone());
                let _ = queue(&a, Ask { feed: Feed::Sip, symbols: a.args.topoff.clone(), start, through: today, order: None }, "top-off");
            }
        }
        let next = lock(&a.runs).iter().find(|r| r.state == "queued" || r.state == "running").map(|r| r.id.clone());
        match next {
            Some(id) => work(&a, &id).await,
            None => a.observer.set_state(State::Idle, "no runs queued"),
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
}

async fn post_run(AxState(a): AxState<Shared>, Json(ask): Json<Ask>) -> (StatusCode, Json<Value>) {
    match queue(&a, ask, "request") {
        Ok(r) => (StatusCode::OK, Json(json!(r))),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({ "error": e }))),
    }
}

async fn stop_run(AxState(a): AxState<Shared>, UrlPath(id): UrlPath<String>) -> (StatusCode, Json<Value>) {
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
        (StatusCode::NOT_FOUND, Json(json!({ "error": "no such run" })))
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

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let _ = rustls::crypto::ring::default_provider().install_default();
    let (key_id, secret_key) = match (secret("ALPACA_HISTORY_KEY_ID"), secret("ALPACA_HISTORY_SECRET_KEY")) {
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
    let http = reqwest::Client::builder().connect_timeout(Duration::from_secs(10)).build().expect("http client");
    let runs: Vec<Run> = std::fs::read(args.state.join("runs.json"))
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or_default();
    let observer = Observer::new("alpaca-history", env!("CARGO_PKG_VERSION"));
    let mut schedule = vec![json!({ "name": "Runs", "kind": "event", "when": "as queued, a day at a time" })];
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
    let app = Arc::new(App {
        alpaca: Alpaca { http: http.clone(), data: args.data_url.trim_end_matches('/').to_owned(), key_id, secret: secret_key, pace: Pace::new(args.max_rpm) },
        http,
        store: args.store.trim_end_matches('/').to_owned(),
        store_token,
        runs: Mutex::new(runs),
        events: Events::default(),
        observer: observer.clone(),
        yield_to: args
            .yield_to
            .iter()
            .filter_map(|y| y.split_once('=').map(|(n, u)| (n.trim().to_owned(), u.trim().trim_end_matches('/').to_owned())))
            .collect(),
        last_topoff: Mutex::new(String::new()),
        args,
    });
    {
        let (req, lim, a2) = (app.alpaca.pace.requests.clone(), app.alpaca.pace.limited.clone(), app.clone());
        observer.set_metrics(move |w| {
            use std::sync::atomic::Ordering::Relaxed;
            w.counter("tinytrader_history_requests_total", "Requests made to Alpaca.", &[], req.load(Relaxed) as f64);
            w.counter("tinytrader_history_rate_limited_total", "429s from Alpaca.", &[], lim.load(Relaxed) as f64);
            let runs = lock(&a2.runs);
            let days = |s: &str| runs.iter().flat_map(|r| r.days.values()).filter(|d| d.state == s).count() as f64;
            w.gauge("tinytrader_history_days", "Days by outcome, across runs kept.", &[("outcome", "committed")], days("committed"));
            w.gauge("tinytrader_history_days", "Days by outcome, across runs kept.", &[("outcome", "failed")], days("failed"));
        });
    }
    let listen = listen_address("HISTORY_LISTEN", "127.0.0.1:9124");
    let router = Router::new()
        .route("/runs", get(|AxState(a): AxState<Shared>| async move { Json(runs_view(&a)) }).post(post_run))
        .route("/runs/{id}/stop", post(stop_run))
        // A web page of another origin can't start or stop a run.
        .layer(axum::middleware::from_fn(tinytrader_observe::guard::same_origin_writes))
        .with_state(app.clone())
        .merge(observer.router())
        .merge(app.events.router());
    let listener = match tokio::net::TcpListener::bind(&listen).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("alpaca-history: cannot listen on {listen}: {e}");
            std::process::exit(2);
        }
    };
    eprintln!("alpaca-history: store at {}; endpoints on http://{listen}", app.store);
    tokio::spawn(worker(app.clone()));
    if let Err(e) = axum::serve(listener, router).await {
        eprintln!("alpaca-history: http server stopped: {e}");
    }
}
