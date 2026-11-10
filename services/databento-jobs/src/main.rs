//! Databento batch jobs: price a request, submit it, follow it, and download
//! the finished files into the store.
//!
//! - `GET /jobs`: Databento's jobs (not expired), each with what the store
//!   already holds of it and any download in progress.
//! - `GET /datasets`, `GET /schemas?dataset=`: for the request form.
//! - `POST /price` with `{ dataset, schema, symbol, stype_in?, start, end,
//!   rule? | contracts?, hours?, again? }` (dates, `end` inclusive): prices
//!   only what's missing (see `plan`): the store's holdings, Databento's jobs
//!   and the ledger's purchases are subtracted per day, contract and window.
//!   The quote lists each request (the whole symbol over a run of days, or a
//!   day's missing contracts over the hours they need), what was skipped and
//!   why. Long prices answer `pricing` (202); `GET /quotes/{id}` follows.
//! - `GET /quotes`, `GET /quotes/{id}`: quotes still good, in full.
//!   `DELETE /quotes/{id}` declines one (with the approval token).
//! - `POST /submit` with `{ quote, confirm_usd, again?, approved_by? }`:
//!   plans the quote again under the purchase lock (one purchase at a time)
//!   and submits its requests only if they and the total are unchanged. A
//!   paid one needs a person's approval: the console's bearer token
//!   (`JOBS_APPROVE_TOKEN` or `_FILE`), who approved it, and `confirm_usd`
//!   exactly the price; the ledger records the approval before anything is
//!   sent. Never what's had already, and nothing while any of that can't be
//!   read in full. A purchase in the ledger whose job is expired or no longer
//!   listed is bought again only when the quote's `again` names its job. A
//!   submission is never retried; if its outcome is uncertain it blocks until
//!   found on the job list or resolved with `POST /ledger/resolve`.
//! - `GET /ledger/unresolved`: the submissions whose outcome isn't known,
//!   each with the `request` and `at` that resolving it names.
//! - `POST /ledger/resolve` with `{ request, at, job, note }`: the operator's
//!   word on a submission whose outcome isn't known.
//! - `POST /download/{job}`: streams the job's files into the store, each
//!   checked against Databento's size and SHA-256, then has the store file
//!   the batch. Jobs submitted here download by themselves once done.
//!   Downloading an already purchased job never charges again.
//! - `POST /cache/{job}` / `DELETE /cache/{job}`: a checked copy of a done
//!   job's files in `JOBS_CACHE_DIR`, outside the store, and its removal.
//!
//! The API key comes from `DATABENTO_API_KEY` or `DATABENTO_API_KEY_FILE`.
//! The store's write token, when the store requires one, from
//! `JOBS_STORE_TOKEN` or `JOBS_STORE_TOKEN_FILE`: sent, as a bearer token, on
//! the uploads and filing only, never to Databento.

mod api;
mod conditions;
#[cfg(test)]
mod guard_tests;
mod had;
mod ledger;
mod missing;
#[cfg(test)]
mod openapi;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tinytrader_observe::{lock, secret};

use axum::extract::{Path as UrlPath, Query, State as AxState};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use clap::Parser;
use databento::HistoricalClient;
use databento::dbn::{SType, Schema};
use databento::historical::batch::{BatchJob, JobState, SubmitJobParams};
use databento::historical::metadata::GetCostParams;
use futures_util::StreamExt as _;
use serde::Deserialize;
use serde_json::{Value, json};
use tinytrader_observe::{Events, Observer, State, listen_address};

/// Databento batch jobs, downloaded into the store.
#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// The store's address.
    #[arg(long, env = "JOBS_STORE", default_value = "http://127.0.0.1:9120")]
    store: String,

    /// How often to refresh the jobs list, in seconds.
    #[arg(long, env = "JOBS_REFRESH_SECONDS", default_value_t = 30)]
    refresh: u64,

    /// Capture services to yield to (decision 0011), as NAME=URL of each
    /// one's observe endpoint, separated by `;`. While any of them is live
    /// (its health says anything but idle), the service stays off the
    /// network: it doesn't poll Databento, and downloads wait. One that
    /// doesn't answer isn't running, so isn't capturing.
    #[arg(long, env = "JOBS_YIELD_TO", value_delimiter = ';')]
    yield_to: Vec<String>,

    /// The capture hours (New York time, e.g. `07:55-17:05`, set relative to
    /// the regular session), used to look ahead: a file isn't started if
    /// those hours begin before it could finish. Trading days come from
    /// `--calendar`.
    #[arg(long, env = "JOBS_CAPTURE_HOURS")]
    capture_hours: Option<tinytrader_schedule::Schedule>,

    /// Market calendar (`{ "days": [...] }`); without it, every weekday is a
    /// trading day.
    #[arg(long, env = "JOBS_CALENDAR")]
    calendar: Option<String>,

    /// The submissions ledger (JSON lines): each submission is written down
    /// before it is sent. Without one, nothing is submitted.
    #[arg(long, env = "JOBS_LEDGER")]
    ledger: Option<std::path::PathBuf>,

    /// Jobs downloaded at once (decision 0024). Filing is left to the
    /// store, which files one at a time.
    #[arg(long, env = "JOBS_DOWNLOADS", default_value_t = 4)]
    downloads: usize,

    /// Where `POST /cache/{job}` puts a done job's files, outside the store
    /// (by default `cache/` beside the ledger). Readable by everyone: the
    /// data is what research reads anyway; the key never goes there.
    #[arg(long, env = "JOBS_CACHE_DIR")]
    cache_dir: Option<std::path::PathBuf>,
    /// Free space the cache never takes its volume below, in GB: a download
    /// waits rather than go past it (or past 95% used, whichever keeps more).
    #[arg(long, env = "JOBS_CACHE_MIN_FREE_GB", default_value_t = 20)]
    cache_min_free_gb: u64,
}

/// How long a price stays good for submitting: an hour when it costs
/// anything (it waits for a person's approval), else 15 minutes.
fn quote_ttl(cost_usd: f64) -> Duration {
    Duration::from_secs(if cost_usd == 0.0 { 15 * 60 } else { 60 * 60 })
}

/// Requests priced at once while planning (pricing is free).
const PRICING_AT_ONCE: usize = 16;

/// The time between a quote's submissions: Databento takes at most 20 a
/// minute (and 20 price requests a second, far above `PRICING_AT_ONCE`).
const SUBMIT_GAP: Duration = Duration::from_millis(if cfg!(test) { 0 } else { 3_500 });

/// The most days one request may span.
const MAX_DAYS: i64 = 1_500;

fn parse_day(s: &str) -> Result<time::Date, String> {
    let fmt = time::macros::format_description!("[year]-[month]-[day]");
    time::Date::parse(s.trim(), fmt).map_err(|_| format!("{s:?} is not a date (YYYY-MM-DD)"))
}

/// What to price, and then buy: what's missing of one symbol over some days.
#[derive(Clone, Debug, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
struct PriceRequest {
    dataset: String,
    schema: String,
    /// A parent (`SPY.OPT`: the whole chain) or a raw symbol.
    #[serde(alias = "symbols")]
    symbol: String,
    /// `parent` or `raw_symbol`; by default `parent` for a name with a dot.
    #[serde(default)]
    stype_in: Option<String>,
    /// First day, `YYYY-MM-DD`.
    start: String,
    /// Last day, inclusive.
    end: String,
    /// For an option parent: the contracts wanted each day (decision 0018).
    #[serde(default)]
    rule: Option<missing::Rule>,
    /// For an option parent: exactly these contracts, by day.
    #[serde(default)]
    contracts: Option<BTreeMap<String, Vec<String>>>,
    /// Only these hours of each day.
    #[serde(default)]
    hours: Option<missing::Hours>,
    /// The expired (or no longer listed) jobs bought here whose contracts
    /// and days this buys again, by ID: `["OPRA-…"]`.
    #[serde(default)]
    again: Option<Value>,
    /// Buy every day asked for whole without asking the store what it holds
    /// (it may buy again what the store has), skipping only the days a whole
    /// job of the symbol on Databento's list, or bought here, already
    /// covers. Only a free ($0) quote can then be made or submitted. For the
    /// plan's free data while the store can't answer (a drive away).
    #[serde(default)]
    free_ignore_held: bool,
    /// Deprecated: the same as `free_ignore_held`.
    #[serde(default)]
    free_while_volumes_missing: bool,
    /// Only price it: every day asked for whole, without asking the store
    /// what it holds (so it may include what the store has), at any price.
    /// The quote can never be submitted. For an estimate while the store
    /// can't answer; no rule or contract lists, which need its definitions.
    #[serde(default)]
    price_only: bool,
}

/// A request checked, in the API's terms.
struct Asked {
    dataset: String,
    schema: Schema,
    symbol: String,
    stype: SType,
    start: time::Date,
    /// Exclusive.
    end: time::Date,
    rule: Option<missing::Rule>,
    contracts: Option<BTreeMap<time::Date, BTreeSet<String>>>,
    hours: Option<missing::Hours>,
    /// `free_ignore_held`.
    free_ignore_held: bool,
    /// `price_only`.
    price_only: bool,
}

impl PriceRequest {
    fn check(&self) -> Result<Asked, String> {
        let (start, last) = (parse_day(&self.start)?, parse_day(&self.end)?);
        if last < start {
            return Err("the last day is before the first".into());
        }
        if (last - start).whole_days() >= MAX_DAYS {
            return Err(format!("at most {MAX_DAYS} days at a time"));
        }
        let symbol = self.symbol.trim();
        if symbol.is_empty() {
            // Never a whole dataset by accident.
            return Err("name a symbol".into());
        }
        if symbol.contains(',') {
            // The store files one symbol per batch (decision 0002).
            return Err("one symbol per request: the store files each symbol on its own, so request each separately".into());
        }
        let stype: SType = match &self.stype_in {
            Some(s) => s.parse().map_err(|_| format!("unknown symbol type {s}"))?,
            None if symbol.contains('.') => SType::Parent,
            None => SType::RawSymbol,
        };
        // Only these can be checked against what's held (decision 0019).
        if !matches!(stype, SType::Parent | SType::RawSymbol) {
            return Err(format!(
                "a request by {} can't be checked against what's held, so it isn't bought here; use parent or raw_symbol",
                stype.as_str()
            ));
        }
        let symbol = if stype == SType::Parent {
            symbol.to_ascii_uppercase()
        } else {
            symbol.to_owned()
        };
        let option = stype == SType::Parent && symbol.ends_with(".OPT");
        if !option && (self.rule.is_some() || self.contracts.is_some()) {
            return Err("a rule or contract lists are only for an option parent (ROOT.OPT)".into());
        }
        if self.rule.is_some() && self.contracts.is_some() {
            return Err("name a rule or contract lists, not both".into());
        }
        if self.price_only && (self.rule.is_some() || self.contracts.is_some()) {
            return Err("price_only prices whole days: no rule or contract lists, which need the store's definitions".into());
        }
        let contracts = match &self.contracts {
            None => None,
            Some(m) => {
                let mut out = BTreeMap::new();
                for (k, list) in m {
                    let d = parse_day(k)?;
                    if d < start || d > last {
                        return Err(format!("contracts for {k}, outside {start} to {last}"));
                    }
                    out.insert(d, list.iter().map(|c| c.trim().to_owned()).collect());
                }
                Some(out)
            }
        };
        if let Some(h) = &self.hours {
            h.check()?;
        }
        Ok(Asked {
            dataset: self.dataset.trim().to_ascii_uppercase(),
            schema: self
                .schema
                .parse()
                .map_err(|_| format!("unknown schema {}", self.schema))?,
            symbol,
            stype,
            start,
            end: last.next_day().ok_or("bad last day")?,
            rule: self.rule.clone(),
            contracts,
            hours: self.hours.clone(),
            free_ignore_held: self.free_ignore_held || self.free_while_volumes_missing,
            price_only: self.price_only,
        })
    }
}

/// A price: being worked out, done, or failed.
struct Quote {
    request: Arc<Asked>,
    again: HashSet<String>,
    /// When it was asked for, or, once priced, when that was done.
    at: Instant,
    /// Requests priced so far, and of how many (0 until known).
    priced: Arc<(AtomicUsize, AtomicUsize)>,
    result: Option<Result<Priced, (StatusCode, String)>>,
}

struct Priced {
    parts: Vec<missing::Part>,
    notes: Vec<String>,
    cost_usd: f64,
    /// Databento's condition of each requested day that isn't available.
    conditions: BTreeMap<time::Date, &'static str>,
}

impl Quote {
    fn expired(&self) -> bool {
        let cost = match &self.result {
            None => return false,
            Some(Ok(p)) => p.cost_usd,
            Some(Err(_)) => 0.0,
        };
        self.at.elapsed() >= quote_ttl(cost)
    }

    /// The quote as `/price` and `/quotes` show it.
    fn view(&self, id: &str) -> Value {
        let a = &self.request;
        let mut again: Vec<&String> = self.again.iter().collect();
        again.sort();
        let mut v = json!({
            "quote": id, "dataset": a.dataset, "schema": a.schema.as_str(), "symbol": a.symbol,
            "stype_in": a.stype.as_str(), "start": a.start.to_string(),
            "end": a.end.previous_day().unwrap_or(a.end).to_string(), "hours": a.hours,
            "rule": a.rule.is_some(), "again": again, "price_only": a.price_only,
            "contract_days": a.contracts.as_ref().map(BTreeMap::len),
            "contracts": a.contracts.as_ref().map(|m| m.values().map(BTreeSet::len).sum::<usize>()),
            "priced": self.priced.0.load(Ordering::Relaxed), "of": self.priced.1.load(Ordering::Relaxed),
        });
        match &self.result {
            None => v["state"] = json!("pricing"),
            Some(Err((_, e))) => {
                v["state"] = json!("failed");
                v["error"] = json!(e);
            }
            Some(Ok(p)) => {
                let rows: Vec<Value> = p
                    .parts
                    .iter()
                    .map(|r| {
                        let mut row = r.to_json();
                        // The worst of its days: missing, then pending, then degraded.
                        let worst = p
                            .conditions
                            .range(r.day..=r.last)
                            .map(|(_, c)| *c)
                            .min_by_key(|c| {
                                ["missing", "pending", "degraded"]
                                    .iter()
                                    .position(|x| x == c)
                            });
                        if let Some(c) = worst {
                            row["condition"] = json!(c);
                        }
                        row
                    })
                    .collect();
                v["state"] = json!("ready");
                v["cost_usd"] = json!(p.cost_usd);
                v["billable_bytes"] = json!(p.parts.iter().map(|r| r.bytes).sum::<u64>());
                v["requests"] = json!(rows);
                v["notes"] = json!(p.notes);
                // A price-only quote is an estimate: nothing to approve.
                v["needs_approval"] = json!(p.cost_usd != 0.0 && !a.price_only);
                let left = quote_ttl(p.cost_usd)
                    .saturating_sub(self.at.elapsed())
                    .as_secs();
                v["valid_seconds"] = json!(left);
                v["expires_at"] = json!(tinytrader_observe::unix_seconds() as u64 + left);
            }
        }
        v
    }
}

/// A quote's submission: its requests sent so far, of how many, and how it
/// ended.
struct Submitting {
    quote: Value,
    state: &'static str,
    submitted: usize,
    of: usize,
    /// The jobs made so far, in order.
    jobs: Vec<String>,
    error: Option<String>,
    at: Instant,
}

impl Submitting {
    fn view(&self) -> Value {
        let mut v = self.quote.clone();
        v["state"] = json!(self.state);
        v["submitted"] = json!(self.submitted);
        v["of"] = json!(self.of);
        v["jobs"] = json!(self.jobs);
        if let Some(e) = &self.error {
            v["error"] = json!(e);
        }
        v
    }
}

/// How long a submission's progress is kept.
const SUBMITS_KEPT: Duration = Duration::from_secs(24 * 3600);

/// Tries of a job's download and filing before it is left failed.
const MAX_TRIES: u32 = 5;

/// The wait before a failed job is tried again, doubling each time.
const RETRY_AFTER: Duration = Duration::from_millis(if cfg!(test) { 0 } else { 60_000 });

/// A download into the store.
#[derive(Clone, Default)]
struct Transfer {
    state: String,
    file: String,
    done_bytes: u64,
    total_bytes: u64,
    error: Option<String>,
    /// For a cache request: when it was asked for.
    requested_at: Option<f64>,
}

struct Jobs {
    db: Arc<dyn api::Databento>,
    /// Held across a whole purchase, from its last check to the ledger's
    /// last word, by `/submit`.
    purchase: tokio::sync::Mutex<()>,
    /// Databento's dataset list, and when it was read.
    dataset_list: Mutex<Option<(Instant, Vec<String>)>>,
    /// When the last job-list read that reconcile checked everything against
    /// (every state, nothing unread) began, in Unix seconds.
    checked_at: Mutex<Option<f64>>,
    key: String,
    http: reqwest::Client,
    store: String,
    /// The store's write token, if it requires one.
    store_token: Option<String>,
    quotes: Mutex<HashMap<String, Quote>>,
    /// The token a paid purchase's approval carries (`JOBS_APPROVE_TOKEN`);
    /// without one, paid purchases are refused.
    approve_token: Option<String>,
    /// How long `/price` waits for a price before answering that it is
    /// still being worked out.
    price_wait: Duration,
    /// How long a price may take before it is given up as failed.
    pricing_cap: Duration,
    transfers: Mutex<HashMap<String, Transfer>>,
    /// Where `POST /cache/{job}` downloads to; none, refused.
    cache_dir: Option<std::path::PathBuf>,
    /// Downloads into the cache, by job: their progress. Written only by the
    /// cache worker (and `DELETE /cache` for one that has ended); read by
    /// `GET /cache` and `GET /jobs`.
    caching: Mutex<HashMap<String, Transfer>>,
    /// The cache worker's queue, the one bounded structure of cache requests.
    /// Started on first use.
    cache_queue: std::sync::OnceLock<CacheQueues>,
    /// Free space the cache keeps on its volume: at least this many bytes, and
    /// at least this percentage of the volume.
    cache_min_free: u64,
    cache_min_free_pct: u64,
    /// In tests, a floor that can be raised under a running worker.
    #[cfg(test)]
    cache_floor_for_tests: AtomicU64,
    /// Jobs submitted here, downloaded once done.
    follow: Mutex<HashSet<String>>,
    /// Jobs whose download is queued or running: never a second.
    in_flight: Mutex<HashSet<String>>,
    /// Followed jobs bought by hand (decision 0033): downloaded as soon as
    /// they're done, capture or not. Read from the ledger's `origin`.
    manual: Mutex<HashSet<String>>,
    /// Failed downloads or filings of each job: how many, and when it may
    /// be tried again.
    tries: Mutex<HashMap<String, (u32, Instant)>>,
    /// Quotes being submitted, or submitted (kept a day): their progress.
    submits: Mutex<HashMap<String, Submitting>>,
    /// How long a download waits for the store to answer while it rescans.
    store_wait: Duration,
    /// How long a download waits for room to merge partial days.
    merge_wait: Duration,
    /// How long a job left failed waits before its tries start again.
    rearm_after: Duration,
    /// Done jobs near their expiry already told of.
    expiring: Mutex<HashSet<String>>,
    /// Submissions, written down before they are sent (see `ledger`).
    ledger: Mutex<Option<ledger::Ledger>>,
    /// Full job details, kept once a job is done (they no longer change).
    details: Mutex<HashMap<String, BatchJob>>,
    /// Bytes to download (compressed, as delivered) of done jobs.
    delivered: Mutex<HashMap<String, u64>>,
    listed: Mutex<Value>,
    /// Downloads running at once (`--downloads`), each until its files and
    /// manifest are in the store's incoming folder, not while it files.
    downloads: tokio::sync::Semaphore,
    /// The room each job in flight holds until it files or fails: its
    /// download's bytes not yet in the store (those that are count in its
    /// free room), and its merges' by volume.
    reserved: Mutex<HashMap<String, (u64, ByVolume)>>,
    room_check: tokio::sync::Mutex<()>,
    /// While in the pause window, when it ends (ns since the epoch); else 0.
    /// Nonzero while yielding to live capture.
    paused_until: AtomicU64,
    yield_to: Vec<(String, String)>,
    capture_hours: Option<tinytrader_schedule::Schedule>,
    calendar: Mutex<tinytrader_schedule::Calendar>,
    events: Events,
    next_quote: AtomicU64,
    /// Starts every quote ID, different at each start, so an ID is never
    /// used twice across restarts.
    boot: String,
}

impl Jobs {
    fn new(
        db: Arc<dyn api::Databento>,
        key: String,
        store: &str,
        store_token: Option<String>,
        ledger: Option<ledger::Ledger>,
        http: reqwest::Client,
    ) -> Self {
        Self {
            db: Arc::new(api::Timed::new(db)),
            purchase: tokio::sync::Mutex::new(()),
            dataset_list: Mutex::default(),
            checked_at: Mutex::default(),
            key,
            http,
            store: store.trim_end_matches('/').to_owned(),
            store_token,
            quotes: Mutex::default(),
            approve_token: None,
            price_wait: Duration::from_secs(20),
            transfers: Mutex::default(),
            cache_dir: None,
            caching: Mutex::default(),
            cache_queue: std::sync::OnceLock::new(),
            cache_min_free: 20_000_000_000,
            cache_min_free_pct: 5,
            #[cfg(test)]
            cache_floor_for_tests: AtomicU64::new(0),
            follow: Mutex::default(),
            in_flight: Mutex::default(),
            manual: Mutex::default(),
            tries: Mutex::default(),
            submits: Mutex::default(),
            store_wait: Duration::from_secs(600),
            merge_wait: Duration::from_secs(3600),
            rearm_after: Duration::from_secs(6 * 3600),
            expiring: Mutex::default(),
            ledger: Mutex::new(ledger),
            details: Mutex::default(),
            delivered: Mutex::default(),
            listed: Mutex::new(json!({ "jobs": [], "updated_at": null })),
            downloads: tokio::sync::Semaphore::new(4),
            reserved: Mutex::default(),
            room_check: tokio::sync::Mutex::new(()),
            events: Events::default(),
            next_quote: AtomicU64::new(1),
            boot: {
                use std::hash::{BuildHasher as _, Hasher as _};
                // A randomly keyed hash: random at each start.
                let mut h = std::collections::hash_map::RandomState::new().build_hasher();
                h.write_u128(
                    time::OffsetDateTime::now_utc()
                        .unix_timestamp_nanos()
                        .cast_unsigned(),
                );
                format!("{:08x}", h.finish() as u32)
            },
            pricing_cap: Duration::from_secs(4 * 60 * 60),
            paused_until: AtomicU64::new(0),
            yield_to: Vec::new(),
            capture_hours: None,
            calendar: Mutex::new(tinytrader_schedule::Calendar::default()),
        }
    }

    /// A write to the store, with its token when it requires one.
    fn store_write(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.store_token {
            Some(t) => req.bearer_auth(t),
            None => req,
        }
    }
}

type Shared = Arc<Jobs>;
/// Bytes by volume name.
type ByVolume = HashMap<String, u64>;
type Reply = (StatusCode, Json<Value>);

fn refused(code: StatusCode, why: impl Into<String>) -> Reply {
    (code, Json(json!({ "error": why.into() })))
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let _ = rustls::crypto::ring::default_provider().install_default();
    let key = match secret("DATABENTO_API_KEY") {
        Ok(k) => k,
        Err(e) => {
            eprintln!("databento-jobs: {e}");
            std::process::exit(2);
        }
    };
    let store_token = match tinytrader_observe::optional_secret("JOBS_STORE_TOKEN") {
        Ok(t) => t,
        Err(e) => {
            eprintln!("databento-jobs: {e}");
            std::process::exit(2);
        }
    };
    let db = match (0..PRICING_AT_ONCE)
        .map(|_| {
            HistoricalClient::builder()
                .key(&key)
                .and_then(databento::historical::ClientBuilder::build)
        })
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("databento-jobs: {e}");
            std::process::exit(2);
        }
    };
    let ledger = match &args.ledger {
        Some(path) => match ledger::Ledger::open(path) {
            Ok(l) => Some(l),
            Err(e) => {
                eprintln!("databento-jobs: cannot open the submissions ledger: {e}");
                std::process::exit(2);
            }
        },
        None => None,
    };
    if let Some(l) = &ledger {
        eprintln!("databento-jobs: {}", following(l));
        for e in l.written_off() {
            eprintln!(
                "databento-jobs: {} was written off as not submitted by an earlier version without anyone checking; it blocks again until found on Databento's list or resolved (POST /ledger/resolve)",
                e.request
            );
        }
    }
    let mut jobs = Jobs::new(
        Arc::new(api::Live::new(db)),
        key,
        &args.store,
        store_token,
        ledger,
        reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .build()
            .expect("http client"),
    );
    jobs.cache_min_free = args.cache_min_free_gb.saturating_mul(1_000_000_000);
    jobs.cache_dir = args.cache_dir.clone().or_else(|| {
        args.ledger
            .as_ref()
            .and_then(|l| l.parent())
            .map(|d| d.join("cache"))
    });
    jobs.yield_to = args
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
        .collect();
    jobs.capture_hours = args.capture_hours;
    jobs.downloads = tokio::sync::Semaphore::new(args.downloads.max(1));
    jobs.approve_token = match tinytrader_observe::optional_secret("JOBS_APPROVE_TOKEN") {
        Ok(t) => t,
        Err(e) => {
            eprintln!("databento-jobs: {e}");
            std::process::exit(2);
        }
    };
    if jobs.approve_token.is_none() {
        eprintln!("databento-jobs: paid purchases are off: no approval token (JOBS_APPROVE_TOKEN)");
    }
    let jobs = Arc::new(jobs);
    let listen = listen_address("JOBS_LISTEN", "127.0.0.1:9121");
    let observer = Observer::new("databento-jobs", env!("CARGO_PKG_VERSION"));
    let mut schedule = vec![
        json!({ "name": "Databento job list", "kind": "interval", "every_seconds": args.refresh, "when": format!("every {} s, outside market hours", args.refresh) }),
        json!({ "name": "Download of jobs submitted here", "kind": "event", "when": "when Databento finishes them, outside market hours" }),
    ];
    if !args.yield_to.is_empty() {
        let names: Vec<&str> = args
            .yield_to
            .iter()
            .filter_map(|y| y.split_once('=').map(|(n, _)| n.trim()))
            .collect();
        let mut entry = json!({ "name": "Yields to live capture", "kind": "yield", "class": "background", "yields_to": names, "when": format!("off the network while any of {} is live", names.join(", ")) });
        if let Some(h) = args.capture_hours {
            entry["hours"] = json!(h.to_string());
        }
        schedule.insert(0, entry);
    }
    observer.describe(json!({
        "role": "Databento batch jobs: prices and submits requests, and downloads finished jobs into the store.",
        "upstream": [{ "id": "databento", "label": "databento", "sub": "batch API", "kind": "flow", "via": "https" }],
        "streams": { "jobs": "/jobs", "events": "/events" },
        "schedule": schedule,
    }));

    let app = Router::new()
        .route(
            "/jobs",
            get(|AxState(j): AxState<Shared>| async move { Json(jobs_view(&j)) }),
        )
        .route("/datasets", get(datasets))
        .route("/schemas", get(schemas))
        .route(
            "/price",
            // Contract lists for years of days are large.
            post(price).layer(axum::extract::DefaultBodyLimit::max(64 << 20)),
        )
        .route("/submit", post(submit))
        .route("/quotes", get(quotes_list))
        .route("/quotes/{id}", get(quote_get).delete(quote_delete))
        .route("/ledger/unresolved", get(ledger_unresolved))
        .route("/ledger/resolve", post(resolve))
        .route("/download/{job}", post(download))
        .route("/cache", get(cache_list))
        .route("/cache/{job}", post(cache).delete(uncache))
        // A web page of another origin can't submit (or pay for) anything.
        .layer(axum::middleware::from_fn(
            tinytrader_observe::guard::same_origin_writes,
        ))
        .with_state(jobs.clone())
        .merge(observer.router())
        .merge(jobs.events.router());
    let listener = match tokio::net::TcpListener::bind(&listen).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("databento-jobs: cannot listen on {listen}: {e}");
            std::process::exit(2);
        }
    };
    eprintln!(
        "databento-jobs: store at {}; endpoints on http://{listen}",
        jobs.store
    );
    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            eprintln!("databento-jobs: http server stopped: {e}");
        }
    });

    // Databento's day conditions for what the store holds, daily.
    tokio::spawn(conditions::every_day(jobs.clone()));
    resume_caching(&jobs);
    let every = Duration::from_secs(args.refresh.max(10));
    let mut calendar = (tinytrader_schedule::Calendar::default(), None::<Instant>);
    // When the jobs bought by hand were last checked while capture was live.
    let mut manual_checked: Option<Instant> = None;
    loop {
        if let Some(url) = &args.calendar
            && calendar
                .1
                .is_none_or(|at| at.elapsed() > Duration::from_secs(3600))
            && let Ok(r) = jobs.http.get(url).send().await
            && let Ok(c) = r.json::<tinytrader_schedule::Calendar>().await
            && !c.days.is_empty()
        {
            *lock(&jobs.calendar) = c.clone();
            calendar = (c, Some(Instant::now()));
        }
        // Background work (decision 0011): off the network while capture is live.
        // What was bought by hand still downloads once it's done (decision 0033).
        if let Some(who) = live_capture(&jobs).await {
            jobs.paused_until.store(1, Ordering::Relaxed);
            observer.set_state(State::Idle, &format!("yielding: {who} is live"));
            if manual_checked.is_none_or(|at: Instant| at.elapsed() >= MANUAL_CHECK) {
                start_manual_downloads(&jobs).await;
                manual_checked = Some(Instant::now());
            }
            tokio::time::sleep(Duration::from_secs(15)).await;
            continue;
        }
        jobs.paused_until.store(0, Ordering::Relaxed);
        match refresh(&jobs).await {
            Ok(r) if !r.unread.is_empty() => observer.set_state(
                State::Degraded,
                &format!(
                    "{} jobs; Databento couldn't be read for {}",
                    r.jobs,
                    r.unread.join(", ")
                ),
            ),
            Ok(r) if !r.stale.is_empty() => observer.set_state(
                State::Ready,
                &format!(
                    "{} jobs (Databento slow; last-known details for {})",
                    r.jobs,
                    r.stale.len()
                ),
            ),
            Ok(r) => observer.set_state(State::Ready, &format!("{} jobs", r.jobs)),
            Err(e) => observer.set_state(State::Degraded, &e),
        }
        tokio::time::sleep(every).await;
    }
}

/// How many jobs bought here are followed (downloaded and filed once done).
fn following(l: &ledger::Ledger) -> String {
    format!(
        "following {} jobs submitted here (from the ledger), to download and file once done",
        l.submitted().len()
    )
}

/// Resolve the ledger's unresolved submissions against the job list
/// (`out`: every job read, expired ones included), and follow (download when
/// done) every job submitted here. Nothing is resolved while any job couldn't
/// be read (`complete` false): the unread one may be the match. A submission
/// not found stays unresolved, blocking what it overlaps, until it shows or
/// the operator resolves it; never by time alone. Nor while a purchase is
/// in flight: its job may show before its own answer is written.
/// `started`: when the job list was read.
fn reconcile(jobs: &Shared, out: &[BatchJob], complete: bool, started: f64) {
    let turn = jobs.purchase.try_lock();
    let mut guard = lock(&jobs.ledger);
    let Some(l) = guard.as_mut() else { return };
    let checks = complete && turn.is_ok();
    if checks {
        *lock(&jobs.checked_at) = Some(started);
    }
    for e in l.unresolved().into_iter().filter(|_| checks) {
        let claimed = l.submitted();
        // Its job was received after it was sent (with a margin for clocks).
        let sent = l.attempt_at(&e);
        let Some(j) = out.iter().find(|j| {
            !claimed.contains(&j.id)
                && had::same_request(&e.request, j)
                && j.ts_received.unix_timestamp() as f64 >= sent - 120.0
        }) else {
            continue;
        };
        let step = ledger::Entry {
            at: tinytrader_observe::unix_seconds(),
            request: e.request.clone(),
            state: "submitted".into(),
            job: Some(j.id.clone()),
            cost_usd: e.cost_usd,
            note: Some("found in Databento's job list".into()),
            ..Default::default()
        };
        let text = format!("reconciled a submission: {}", j.id);
        match l.append(step) {
            Ok(()) => jobs.events.push(text.clone(), text),
            Err(err) => eprintln!("databento-jobs: cannot write the ledger: {err}"),
        }
    }
    lock(&jobs.follow).extend(l.submitted());
    lock(&jobs.manual).extend(l.submitted_by_hand());
}

/// The first line of an API error, without an HTML error page.
fn brief(e: &str) -> String {
    e.split('<')
        .next()
        .unwrap_or(e)
        .trim()
        .trim_end_matches(':')
        .chars()
        .take(120)
        .collect()
}

/// How a refresh went: how many jobs, and which Databento couldn't answer for.
struct Refreshed {
    jobs: usize,
    stale: Vec<String>,
    unread: Vec<String>,
}

/// Read the jobs list and the store's holdings, reconcile the ledger, and
/// start downloads of followed jobs that are done.
async fn refresh(jobs: &Shared) -> Result<Refreshed, String> {
    let started = tinytrader_observe::unix_seconds();
    let short = jobs
        .db
        .list_jobs(&api::every_job())
        .await
        .map_err(|e| format!("list jobs: {e}"))?;
    // Expired jobs are read only to match a submission whose outcome isn't
    // known: those received since the earliest was sent.
    let since = lock(&jobs.ledger).as_ref().and_then(|l| {
        l.unresolved()
            .iter()
            .map(|e| l.attempt_at(e))
            .reduce(f64::min)
    });
    // What the store already holds, first: only jobs not on disk need
    // their file lists (for the size a download would take).
    let held: Value = match jobs
        .http
        .get(format!("{}/batches/databento", jobs.store))
        .send()
        .await
    {
        Ok(r) => r
            .json::<Value>()
            .await
            .map(|v| v["batches"].clone())
            .unwrap_or(Value::Null),
        Err(_) => Value::Null,
    };
    let (mut out, mut expired) = (Vec::new(), Vec::new());
    // A finished job's details never change; others are read again each
    // round. When Databento can't answer for one, its last-known details
    // stand in, so one slow call doesn't blank the list.
    let (mut stale, mut unread) = (Vec::new(), Vec::new());
    for s in &short {
        if s.state == JobState::Expired
            && since.is_none_or(|t| (s.ts_received.unix_timestamp() as f64) < t - 120.0)
        {
            continue;
        }
        let known = lock(&jobs.details).get(&s.id).cloned();
        let job = match known {
            Some(j) if matches!(j.state, JobState::Done | JobState::Expired) => BatchJob {
                state: s.state,
                ..j
            },
            known => {
                let fetched = jobs.db.job(&s.id).await;
                match (fetched, known) {
                    (Ok(j), _) => j,
                    (Err(e), Some(j)) => {
                        stale.push(format!("{} ({})", s.id, brief(&e.to_string())));
                        j
                    }
                    (Err(e), None) => {
                        unread.push(format!("{} ({})", s.id, brief(&e.to_string())));
                        continue;
                    }
                }
            }
        };
        lock(&jobs.details).insert(job.id.clone(), job.clone());
        if job.state == JobState::Expired {
            expired.push(job);
            continue;
        }
        if job.state == JobState::Done
            && held[&job.id].is_null()
            && !lock(&jobs.delivered).contains_key(&job.id)
        {
            let files = jobs.db.files(&job.id).await;
            if let Ok(files) = files {
                lock(&jobs.delivered).insert(job.id.clone(), files.iter().map(|f| f.size).sum());
            }
        }
        out.push(job);
    }
    let delivered = lock(&jobs.delivered).clone();
    let rows: Vec<Value> = out
        .iter()
        .map(|j| row(j, &held, delivered.get(&j.id).copied()))
        .collect();
    *lock(&jobs.listed) = json!({ "jobs": rows, "store_reachable": !held.is_null(), "updated_at": tinytrader_observe::unix_seconds() });

    // Submissions whose outcome was unknown: match each to the job it
    // created.
    reconcile(
        jobs,
        &[out.as_slice(), &expired].concat(),
        unread.is_empty(),
        started,
    );

    // A job left failed starts its tries again after a while: what failed
    // may have been fixed.
    for (n, at) in lock(&jobs.tries).values_mut() {
        if *n >= MAX_TRIES && Instant::now() >= *at + jobs.rearm_after {
            *n = 0;
        }
    }
    // A done job not filed within a week of its expiry is told of, once, so
    // a failure that keeps happening is noticed before the data is gone.
    for j in &out {
        let how = &held[&j.id];
        let left = j.ts_expiration.map(|e| e - time::OffsetDateTime::now_utc());
        if j.state == JobState::Done
            && (how.is_null() || how["how"] == "incoming")
            && left.is_some_and(|l| l < time::Duration::days(7))
            && lock(&jobs.expiring).insert(j.id.clone())
        {
            let text = format!(
                "{} isn't filed and expires at Databento in {} days",
                j.id,
                left.map_or(0, |l| l.whole_days())
            );
            jobs.events.push(text.clone(), text);
        }
    }
    // Followed jobs that are done and not filed: not yet in the store, or
    // left in its incoming folder by a filing that failed. Each once at a
    // time, and a failed one again only after a wait, a few times.
    let ready: Vec<String> = out
        .iter()
        .filter(|j| {
            let how = &held[&j.id];
            j.state == JobState::Done
                && lock(&jobs.follow).contains(&j.id)
                && (how.is_null() || how["how"] == "incoming")
                && unfileable(j).is_none()
                && !lock(&jobs.in_flight).contains(&j.id)
                && lock(&jobs.tries)
                    .get(&j.id)
                    .is_none_or(|(n, at)| *n < MAX_TRIES && Instant::now() >= *at)
        })
        .map(|j| j.id.clone())
        .collect();
    for id in ready {
        let manual = lock(&jobs.manual).contains(&id);
        start_download(jobs, id, manual);
    }
    Ok(Refreshed {
        jobs: out.len(),
        stale,
        unread,
    })
}

/// `bytes` is what downloading delivers (compressed); Databento's own job
/// size is the uncompressed size.
/// Why the store would refuse to file this job, if it would. Checked before
/// downloading, so nothing is fetched that can't be filed.
fn unfileable(j: &BatchJob) -> Option<String> {
    use databento::dbn::{Compression, Encoding};
    if j.encoding != Encoding::Dbn || j.compression != Compression::Zstd {
        return Some("not DBN with zstd; the store files only .dbn.zst".into());
    }
    if j.split_symbols || j.split_duration.as_str() != "day" {
        return Some(
            "not split by day (or split by symbol); the store files one file per day".into(),
        );
    }
    let n = j.symbols.to_api_string().split(',').count();
    (n > 1 && contract_parent(j).is_none()).then(|| {
        format!(
            "requests {n} symbols, not all contracts of one option parent; the store files one symbol per batch"
        )
    })
}

/// The option parent (`SPY.OPT`) of a job that lists contracts of one
/// root, by raw symbol, each a strict OCC symbol: it is filed as partial
/// days of that parent (decision 0018).
fn contract_parent(j: &BatchJob) -> Option<String> {
    if j.stype_in != SType::RawSymbol {
        return None;
    }
    let symbols: Vec<String> = j
        .symbols
        .to_api_string()
        .split(',')
        .map(|s| s.trim().to_owned())
        .collect();
    missing::parent_of(&symbols)
}

fn row(j: &BatchJob, held: &Value, delivered: Option<u64>) -> Value {
    let state = match j.state {
        JobState::Queued => "queued",
        JobState::Processing => "processing",
        JobState::Done => "done",
        JobState::Expired => "expired",
    };
    json!({
        "id": j.id,
        "state": state,
        "progress": j.progress,
        "dataset": j.dataset,
        "schema": j.schema.as_str(),
        "symbols": j.symbols.to_api_string(),
        "stype_in": j.stype_in.as_str(),
        "start": j.start.unix_timestamp(),
        "end": j.end.unix_timestamp(),
        "cost_usd": j.cost_usd,
        "bytes": delivered,
        "uncompressed_bytes": j.actual_size,
        "received": j.ts_received.unix_timestamp(),
        "expires": j.ts_expiration.map(time::OffsetDateTime::unix_timestamp),
        "split": j.split_duration.as_str(),
        "split_symbols": j.split_symbols,
        "unfileable": unfileable(j),
        "held": held.get(&j.id).map(|h| json!({ "volume": h["volume"], "how": h["how"] })),
        // Done, not filed, and gone from Databento within a week.
        "expires_soon": j.state == JobState::Done
            && held.get(&j.id).is_none_or(|h| h["how"] == "incoming")
            && j.ts_expiration.is_some_and(|e| e - time::OffsetDateTime::now_utc() < time::Duration::days(7)),
    })
}

fn jobs_view(jobs: &Jobs) -> Value {
    let mut v = lock(&jobs.listed).clone();
    let transfers = lock(&jobs.transfers).clone();
    let follow = lock(&jobs.follow).clone();
    for r in v["jobs"].as_array_mut().into_iter().flatten() {
        let id = r["id"].as_str().unwrap_or_default().to_owned();
        if let Some(t) = transfers.get(&id) {
            r["transfer"] = json!({ "state": t.state, "file": t.file, "done_bytes": t.done_bytes, "total_bytes": t.total_bytes, "error": t.error });
        }
        r["follow"] = json!(follow.contains(&id));
        if let Some(t) = lock(&jobs.caching).get(&id) {
            r["cache"] = json!({ "state": t.state, "file": t.file, "done_bytes": t.done_bytes, "total_bytes": t.total_bytes, "error": t.error });
        }
    }
    v
}

async fn datasets(AxState(jobs): AxState<Shared>) -> Reply {
    match jobs.db.datasets().await {
        Ok(d) => (StatusCode::OK, Json(json!({ "datasets": d }))),
        Err(e) => refused(StatusCode::BAD_GATEWAY, e.to_string()),
    }
}

#[derive(Deserialize)]
struct DatasetQuery {
    dataset: String,
}

async fn schemas(AxState(jobs): AxState<Shared>, Query(q): Query<DatasetQuery>) -> Reply {
    match jobs.db.schemas(&q.dataset).await {
        Ok(s) => (
            StatusCode::OK,
            Json(json!({ "schemas": s.iter().map(Schema::as_str).collect::<Vec<_>>() })),
        ),
        Err(e) => refused(StatusCode::BAD_GATEWAY, e.to_string()),
    }
}

/// The cost of a request, in US dollars, and its billable size.
async fn cost(
    jobs: &Jobs,
    dataset: &str,
    schema: Schema,
    symbols: &[String],
    stype_in: SType,
    w: had::Window,
) -> Result<(f64, u64), String> {
    let params = GetCostParams::builder()
        .dataset(dataset)
        .schema(schema)
        .symbols(symbols.to_vec())
        .stype_in(stype_in)
        .date_time_range((had::at(w.0), had::at(w.1)))
        .build();
    // Pricing is free, so a failure that may pass is tried again.
    for tries in 1.. {
        match jobs.db.cost(&params).await {
            Err(e) if tries < 3 && retryable(&e) => {
                tokio::time::sleep(COST_BACKOFF * tries).await;
            }
            r => return r,
        }
    }
    unreachable!()
}

/// Waits between tries of a price.
const COST_BACKOFF: Duration = Duration::from_millis(if cfg!(test) { 10 } else { 1000 });

/// Whether a failed call may pass when tried again: no answer, a transport
/// failure, or a 5xx from Databento.
fn retryable(e: &str) -> bool {
    e.contains("no answer from Databento")
        || e.starts_with("HTTP error")
        || e.split_whitespace()
            .any(|w| w.len() == 3 && w.starts_with('5') && w.bytes().all(|b| b.is_ascii_digit()))
}

/// Before pricing: refuse a request outside what Databento has for the
/// schema, and warn about requested days it marks degraded, pending or
/// missing.
/// Also each requested day's condition that isn't available.
async fn availability(
    jobs: &Jobs,
    c: &Asked,
) -> Result<(Vec<String>, BTreeMap<time::Date, &'static str>), String> {
    use databento::historical::metadata::{DatasetCondition, GetDatasetConditionParams};
    let mut warnings = Vec::new();
    let mut conditions = BTreeMap::new();
    match jobs.db.range(&c.dataset).await {
        Ok(r) => {
            let (from, to) = r
                .range_by_schema
                .get(&c.schema)
                .map_or((r.start, r.end), |x| (x.start, x.end));
            let (want_from, want_to) = (
                c.start.midnight().assume_utc(),
                c.end.midnight().assume_utc(),
            );
            if want_from < from.replace_time(time::Time::MIDNIGHT) || want_to > to {
                let last = (to - time::Duration::nanoseconds(1)).date();
                return Err(format!(
                    "Databento has {} {} from {} to {last}",
                    c.dataset,
                    c.schema.as_str(),
                    from.date()
                ));
            }
        }
        Err(e) => warnings.push(format!("couldn't check the available range: {e}")),
    }
    let params = GetDatasetConditionParams::builder()
        .dataset(&c.dataset)
        .date_range(c.start..c.end)
        .build();
    match jobs.db.conditions(&params).await {
        Ok(days) => {
            for (cond, label) in [
                (DatasetCondition::Degraded, "degraded"),
                (DatasetCondition::Pending, "pending"),
                (DatasetCondition::Missing, "missing"),
            ] {
                let list: Vec<String> = days
                    .iter()
                    .filter(|d| d.condition == cond)
                    .map(|d| {
                        conditions.insert(d.date, label);
                        d.date.to_string()
                    })
                    .collect();
                if !list.is_empty() {
                    warnings.push(format!(
                        "Databento marks {} {label}: {}",
                        if list.len() == 1 {
                            "1 day"
                        } else {
                            "these days"
                        },
                        list.join(", ")
                    ));
                }
            }
        }
        Err(e) => warnings.push(format!("couldn't check the days' condition: {e}")),
    }
    Ok((warnings, conditions))
}

/// A store answer about what it holds, refused unless it is 200 (the store
/// answers 503 until its first scan) and every volume the store knows is
/// present: `missing_volumes` empty, where its absence means a store too old
/// to say. `fresh`: from a full rescan begun after asking (`fresh=1`), which
/// the store waits up to 120 s for.
async fn store_json(
    jobs: &Jobs,
    path: &str,
    query: &[(&str, &str)],
    fresh: bool,
) -> Result<Value, String> {
    let mut query = query.to_vec();
    if fresh {
        query.push(("fresh", "1"));
    }
    let url = reqwest::Url::parse_with_params(&format!("{}{path}", jobs.store), &query)
        .map_err(|e| format!("the store's {path}: {e}"))?;
    let url = &url;
    let fail = |e: reqwest::Error| (!e.is_timeout(), format!("the store's {path}: {e}"));
    let v: Value = store_retrying(|| async move {
        let r = jobs
            .http
            .get(url.clone())
            .timeout(Duration::from_secs(if fresh { 660 } else { 120 }))
            .send()
            .await
            .map_err(fail)?;
        let status = r.status();
        if !status.is_success() {
            let again = status == StatusCode::SERVICE_UNAVAILABLE;
            return Err((again, format!("the store's {path}: HTTP {status}")));
        }
        r.json().await.map_err(fail)
    })
    .await?;
    match v["missing_volumes"].as_array() {
        Some(m) if m.is_empty() => Ok(v),
        Some(m) => Err(format!(
            "the store is missing volumes ({}), so what it holds isn't known",
            m.iter()
                .map(|x| x.as_str().map_or_else(|| x.to_string(), str::to_owned))
                .collect::<Vec<_>>()
                .join(", ")
        )),
        None => Err(format!(
            "the store's {path} doesn't say which volumes are missing (an older store), so what it holds isn't known"
        )),
    }
}

/// Databento's jobs that aren't expired, in their state as listed now, and
/// their IDs, from the whole job list read now. Fails unless every one of
/// them can be read, so nothing is bought blind.
async fn live_jobs(jobs: &Jobs) -> Result<(Vec<BatchJob>, HashSet<String>), String> {
    let short = jobs
        .db
        .list_jobs(&api::every_job())
        .await
        .map_err(|e| format!("Databento's job list: {e}"))?;
    let mut out = Vec::new();
    for sj in short.iter().filter(|sj| sj.state != JobState::Expired) {
        // A job's request (dataset, schema, symbols, times) never changes.
        let known = lock(&jobs.details).get(&sj.id).cloned();
        let job = match known {
            Some(j) => j,
            None => {
                let j = jobs
                    .db
                    .job(&sj.id)
                    .await
                    .map_err(|e| format!("Databento's job {}: {e}", sj.id))?;
                lock(&jobs.details).insert(j.id.clone(), j.clone());
                j
            }
        };
        out.push(BatchJob {
            state: sj.state,
            ..job
        });
    }
    let ids = out.iter().map(|j| j.id.clone()).collect();
    Ok((out, ids))
}

/// What's had already of `target` (a parent, or a plain symbol) in this
/// dataset and schema from `start` to `end` (exclusive): the store's
/// holdings, Databento's jobs that aren't expired, and every purchase in the
/// ledger, read now (see `had`). Fails when any of it can't be read in full.
/// `fresh`: the store's answers from a rescan begun now (for a purchase),
/// not its last scan (for a price).
async fn had(
    jobs: &Jobs,
    dataset: &str,
    schema: Schema,
    target: &str,
    (start, end): (time::Date, time::Date),
    fresh: bool,
) -> Result<Vec<had::Piece>, String> {
    let last = end.previous_day().unwrap_or(end).to_string();
    let catalog = store_json(jobs, "/catalog", &[], fresh).await?;
    let coverage = store_json(
        jobs,
        "/coverage",
        &[
            ("provider", "databento"),
            ("dataset", dataset),
            ("symbol", target),
            ("schema", schema.as_str()),
            ("start", &start.to_string()),
            ("end", &last),
        ],
        fresh,
    )
    .await?;
    let (live, ids) = live_jobs(jobs).await?;
    let purchases = lock(&jobs.ledger)
        .as_ref()
        .map(ledger::Ledger::purchases)
        .unwrap_or_default();
    let range = (had::day_window(start).0, had::day_window(end).0);
    let schema = schema.as_str();
    let mut pieces = had::store_pieces(&catalog, &coverage, dataset, schema, target, range);
    pieces.extend(had::job_pieces(&live, dataset, schema, target));
    pieces.extend(had::ledger_pieces(
        &purchases, dataset, schema, target, &ids,
    ));
    Ok(pieces)
}

/// What Databento's job list (jobs that aren't expired) and the ledger's
/// purchases cover of `target`, without the store (`free_ignore_held`).
async fn jobs_had(
    jobs: &Jobs,
    dataset: &str,
    schema: Schema,
    target: &str,
) -> Result<Vec<had::Piece>, String> {
    let (live, ids) = live_jobs(jobs).await?;
    let purchases = lock(&jobs.ledger)
        .as_ref()
        .map(ledger::Ledger::purchases)
        .unwrap_or_default();
    let schema = schema.as_str();
    let mut pieces = had::job_pieces(&live, dataset, schema, target);
    pieces.extend(had::ledger_pieces(
        &purchases, dataset, schema, target, &ids,
    ));
    Ok(pieces)
}

/// The dataset in Databento's spelling, refused unless it is on Databento's
/// dataset list (read at most hourly).
async fn known_dataset(jobs: &Jobs, d: &str) -> Result<String, (StatusCode, String)> {
    let d = d.trim().to_ascii_uppercase();
    let cached = lock(&jobs.dataset_list)
        .as_ref()
        .filter(|(at, _)| at.elapsed() < Duration::from_secs(3600))
        .map(|(_, l)| l.clone());
    let list = match cached {
        Some(l) => l,
        None => {
            let l = jobs.db.datasets().await.map_err(|e| {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!("couldn't read Databento's dataset list: {e}"),
                )
            })?;
            *lock(&jobs.dataset_list) = Some((Instant::now(), l.clone()));
            l
        }
    };
    if list.contains(&d) {
        Ok(d)
    } else {
        Err((
            StatusCode::BAD_REQUEST,
            format!("{d} isn't one of Databento's datasets"),
        ))
    }
}

/// Refuses a price that isn't a number of dollars or that changed since the
/// quote; and, when it costs anything (only an exact $0 is free), one
/// without a person's approval (`approval`, see [`approval`]) or not
/// confirmed exactly.
fn check_price(
    now: f64,
    quoted: f64,
    confirm: Option<f64>,
    approval: Option<Reply>,
) -> Option<Reply> {
    let same = |a: f64, b: f64| (a - b).abs() <= 1e-9;
    if !now.is_finite() || now < 0.0 {
        return Some(refused(
            StatusCode::BAD_GATEWAY,
            format!("Databento priced this at {now}, which isn't a price; nothing is submitted"),
        ));
    }
    if !same(now, quoted) {
        return Some(refused(
            StatusCode::CONFLICT,
            format!("the price changed from ${quoted:.2} to ${now:.2}; price the request again"),
        ));
    }
    if now == 0.0 {
        return None;
    }
    if approval.is_some() {
        return approval;
    }
    confirm.is_none_or(|c| !same(c, now)).then(|| {
        refused(
            StatusCode::PRECONDITION_REQUIRED,
            format!("this costs ${now:.2}; confirm that amount to submit it"),
        )
    })
}

/// A quote's price: the sum of its requests', or NaN (never free, never
/// bought) when any isn't a number of dollars or is negative.
fn price_of(parts: &[missing::Part]) -> f64 {
    if parts
        .iter()
        .any(|p| !p.cost_usd.is_finite() || p.cost_usd < 0.0)
    {
        return f64::NAN;
    }
    parts.iter().map(|p| p.cost_usd).sum()
}

/// Whether two secrets are equal, without stopping at the first difference.
fn same_secret(a: &str, b: &str) -> bool {
    a.len() == b.len() && a.bytes().zip(b.bytes()).fold(0u8, |d, (x, y)| d | (x ^ y)) == 0
}

/// Whether the request carries the approval token as its bearer token.
fn approved_by_console(jobs: &Jobs, headers: &axum::http::HeaderMap) -> Result<(), Reply> {
    let Some(want) = &jobs.approve_token else {
        return Err(refused(
            StatusCode::FORBIDDEN,
            "paid purchases need a person's approval in the console, and this service has no approval token (JOBS_APPROVE_TOKEN), so they are off",
        ));
    };
    let sent = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    if sent.is_some_and(|t| same_secret(t.trim(), want)) {
        Ok(())
    } else {
        Err(refused(
            StatusCode::FORBIDDEN,
            "a paid purchase needs a person's approval in the console",
        ))
    }
}

/// Why a paid purchase may not go ahead, if it may not: it needs the
/// console's approval (the approval token) and who gave it.
fn approval(jobs: &Jobs, headers: &axum::http::HeaderMap, by: Option<&str>) -> Option<Reply> {
    if let Err(r) = approved_by_console(jobs, headers) {
        return Some(r);
    }
    by.is_none_or(|b| b.trim().is_empty()).then(|| {
        refused(
            StatusCode::FORBIDDEN,
            "a paid purchase names who approved it (approved_by)",
        )
    })
}

/// Plan a request: what's missing of it, priced. Each day's wanted window
/// is the whole UTC day, or its `hours`. A day with nothing of the symbol
/// had in that window is bought whole (runs of such whole days as one
/// request). Of an option parent, a day partly had (or any day, with a rule
/// or contract lists) is bought as its missing contracts, from the store's
/// definitions of that day, over the hours each still needs. Of any other
/// symbol, a day partly had isn't bought at all. Purchases here that are
/// gone stay had unless `again` names their jobs. Returns the requests, in
/// order, and notes (what was skipped, and why). `priced` counts the
/// requests priced, of how many.
async fn plan(
    jobs: &Jobs,
    a: &Asked,
    again: &HashSet<String>,
    fresh: bool,
    priced: &(AtomicUsize, AtomicUsize),
) -> Result<(Vec<missing::Part>, Vec<String>), (StatusCode, String)> {
    use futures_util::TryStreamExt as _;
    let bad = |e: String| (StatusCode::BAD_REQUEST, e);
    let down = |e: String| (StatusCode::SERVICE_UNAVAILABLE, e);
    let dataset = known_dataset(jobs, &a.dataset).await?;
    // With free_ignore_held the store isn't asked: only whole jobs of the
    // symbol on Databento's list or bought here count, and every other day
    // is bought whole.
    let mut pieces = if a.price_only {
        Vec::new()
    } else if a.free_ignore_held {
        jobs_had(jobs, &dataset, a.schema, &a.symbol)
            .await
            .map_err(down)?
            .into_iter()
            .filter(|p| matches!(p.scope, had::Scope::All) && !matches!(p.kind, had::Kind::Gone(_)))
            .collect()
    } else {
        had(jobs, &dataset, a.schema, &a.symbol, (a.start, a.end), fresh)
            .await
            .map_err(down)?
    };
    let gone: BTreeSet<String> = pieces
        .iter()
        .filter_map(|p| match &p.kind {
            had::Kind::Gone(j) => Some(j.clone()),
            _ => None,
        })
        .collect();
    let mut extra: Vec<&String> = again.iter().filter(|j| !gone.contains(*j)).collect();
    extra.sort();
    if !extra.is_empty() {
        return Err(bad(format!(
            "\"again\" names {}, which this request doesn't buy again",
            json!(extra)
        )));
    }
    pieces.retain(|p| !matches!(&p.kind, had::Kind::Gone(j) if again.contains(j)));
    let mut notes: Vec<String> = gone
        .iter()
        .filter(|j| !again.contains(*j))
        .map(|j| format!("{j}: bought here, expired or no longer on Databento's list; what it bought stays subtracted unless named in \"again\""))
        .collect();
    if a.price_only {
        notes.push("price only: every day priced whole, without checking what the store, Databento's list or this service already holds; it can't be submitted".into());
    }

    let option = a.stype == SType::Parent && a.symbol.ends_with(".OPT");
    let by_contract = a.rule.is_some() || a.contracts.is_some();
    let (mut whole, mut by_day) = (Vec::new(), Vec::new());
    let mut d = a.start;
    while d < a.end {
        let next = d.next_day().ok_or_else(|| bad("bad day".into()))?;
        let window = a
            .hours
            .as_ref()
            .map_or_else(|| had::day_window(d), |h| h.window(d));
        if a.contracts.as_ref().is_some_and(|m| !m.contains_key(&d)) {
            d = next;
            continue;
        }
        let touching: Vec<&had::Piece> = pieces
            .iter()
            .filter(|p| p.window.0 < window.1 && window.0 < p.window.1)
            .collect();
        if a.price_only {
            whole.push((d, window));
        } else if a.free_ignore_held && !by_contract {
            // Only whole jobs count here: a day one covers is skipped, any
            // other is bought whole (never by contract, which needs the
            // store's definitions).
            match touching
                .iter()
                .find(|p| p.window.0 <= window.0 && window.1 <= p.window.1)
            {
                Some(p) => notes.push(format!("{d}: not bought, {} covers it", p.what)),
                None => whole.push((d, window)),
            }
        } else if !by_contract && touching.is_empty() {
            whole.push((d, window));
        } else if option {
            by_day.push((d, window));
        } else {
            let what: Vec<&str> = touching.iter().take(3).map(|p| p.what.as_str()).collect();
            notes.push(format!("{d}: not bought, it overlaps {}", what.join("; ")));
        }
        d = next;
    }

    // Whole days, consecutive ones as one request (whole UTC days only).
    let mut parts: Vec<missing::Part> = Vec::new();
    for (d, window) in whole {
        if let Some(p) = parts.last_mut()
            && a.hours.is_none()
            && p.last.next_day() == Some(d)
        {
            p.last = d;
            p.window.1 = window.1;
            continue;
        }
        parts.push(missing::Part {
            day: d,
            last: d,
            window,
            whole: true,
            symbols: vec![a.symbol.clone()],
            stype: a.stype,
            cost_usd: 0.0,
            bytes: 0,
            skipped_held: 0,
            skipped_pending: 0,
        });
    }
    if !by_day.is_empty() {
        let defs = definitions(jobs, &dataset, &a.symbol).await?;
        let (pieces, defs) = (&pieces, &defs);
        let days: Vec<(Vec<missing::Part>, Option<String>)> = futures_util::stream::iter(by_day)
            .map(|(d, window)| contract_day(jobs, a, pieces, defs, d, window))
            .buffered(PRICING_AT_ONCE)
            .try_collect()
            .await?;
        for (p, note) in days {
            parts.extend(p);
            notes.extend(note);
        }
    }
    parts.sort_by_key(|p| (p.day, p.window, !p.whole));
    if let Some(why) = overlap(&parts) {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("the plan would buy something twice ({why}), so nothing is priced"),
        ));
    }
    priced.1.store(parts.len(), Ordering::Relaxed);
    let dataset = &dataset;
    let parts = futures_util::stream::iter(parts)
        .map(|mut p| async move {
            let (usd, bytes) = cost(jobs, dataset, a.schema, &p.symbols, p.stype, p.window)
                .await
                .map_err(|e| (StatusCode::BAD_GATEWAY, e))?;
            p.cost_usd = usd;
            p.bytes = bytes;
            priced.0.fetch_add(1, Ordering::Relaxed);
            Ok::<_, (StatusCode, String)>(p)
        })
        .buffered(PRICING_AT_ONCE)
        .try_collect()
        .await?;
    Ok((parts, notes))
}

/// Two requests of a plan that would buy the same data twice, if any: they
/// overlap in time, and both are the whole symbol, or one is and the other
/// a contract list, or both list a contract.
fn overlap(parts: &[missing::Part]) -> Option<String> {
    let mut by_start: Vec<&missing::Part> = parts.iter().collect();
    by_start.sort_by_key(|p| p.window.0);
    for (i, p) in by_start.iter().enumerate() {
        let mine: HashSet<&String> = p.symbols.iter().collect();
        for q in by_start[i + 1..]
            .iter()
            .take_while(|q| q.window.0 < p.window.1)
        {
            if p.whole || q.whole || q.symbols.iter().any(|s| mine.contains(s)) {
                return Some(format!(
                    "{} and {}, {} and {}",
                    had::stamp(p.window.0),
                    had::stamp(p.window.1),
                    had::stamp(q.window.0),
                    had::stamp(q.window.1)
                ));
            }
        }
    }
    None
}

/// The store's definition days of `parent`: day → (volume, path).
async fn definitions(
    jobs: &Jobs,
    dataset: &str,
    parent: &str,
) -> Result<HashMap<String, (String, String)>, (StatusCode, String)> {
    let listing: Value = store_get(
        jobs,
        &format!(
            "{}/files?provider=databento&pattern=*/{dataset}/{parent}/definition/*",
            jobs.store
        ),
        |body| serde_json::from_slice(body).map_err(|e| format!("store: {e}")),
    )
    .await?;
    Ok(listing["files"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|f| {
            let path = f["path"].as_str()?;
            let day = path.rsplit('/').next()?.strip_suffix(".dbn.zst")?;
            Some((
                day.to_owned(),
                (f["volume"].as_str()?.to_owned(), path.to_owned()),
            ))
        })
        .collect())
}

/// A read of the store (120 s at most each try), its answer read by
/// `decode`, tried again as [`store_retrying`] says.
async fn store_get<T>(
    jobs: &Jobs,
    url: &str,
    decode: impl Fn(&[u8]) -> Result<T, String>,
) -> Result<T, (StatusCode, String)> {
    let decode = &decode;
    let fail = |e: reqwest::Error| (!e.is_timeout(), format!("store: {e}"));
    store_retrying(|| async move {
        let resp = jobs
            .http
            .get(url)
            .timeout(Duration::from_secs(120))
            .send()
            .await
            .map_err(fail)?;
        let status = resp.status();
        if !status.is_success() {
            let again = status == StatusCode::SERVICE_UNAVAILABLE;
            return Err((again, format!("store: HTTP {status} for {url}")));
        }
        decode(&resp.bytes().await.map_err(fail)?).map_err(|e| (true, e))
    })
    .await
    .map_err(|e| (StatusCode::SERVICE_UNAVAILABLE, e))
}

/// How long a read-only store call waits before its second and third tries.
const STORE_RETRY: [Duration; 2] = if cfg!(test) {
    [Duration::from_millis(10), Duration::from_millis(40)]
} else {
    [Duration::from_secs(2), Duration::from_secs(8)]
};

/// A read-only store call, tried up to three times while it fails in a way
/// worth trying again (`Err((true, why))`): a dropped connection, an answer
/// cut off or unreadable, or a 503 (the store rescanning). Not a timeout,
/// which waited its whole limit already, and never a write.
async fn store_retrying<T, F, Fut>(mut call: F) -> Result<T, String>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, (bool, String)>>,
{
    let mut waits = STORE_RETRY.iter();
    loop {
        match call().await {
            Ok(v) => return Ok(v),
            Err((true, e)) => match waits.next() {
                Some(w) => {
                    eprintln!("databento-jobs: {e}; trying again in {w:?}");
                    tokio::time::sleep(*w).await;
                }
                None => return Err(e),
            },
            Err((false, e)) => return Err(e),
        }
    }
}

/// One day of an option parent bought by contract: the contracts wanted
/// (listed, or by the rule, or the whole chain) less what's had, as
/// requests over the hours each still needs, unpriced; or a note why
/// nothing.
async fn contract_day(
    jobs: &Jobs,
    a: &Asked,
    pieces: &[had::Piece],
    defs: &HashMap<String, (String, String)>,
    d: time::Date,
    window: had::Window,
) -> Result<(Vec<missing::Part>, Option<String>), (StatusCode, String)> {
    let key = d.to_string();
    let Some((volume, path)) = defs.get(&key) else {
        let why = format!(
            "{key}: the store has no {} definitions for this day; buy those first",
            a.symbol
        );
        if a.contracts.is_some() {
            return Err((StatusCode::BAD_REQUEST, why));
        }
        let weekend = matches!(d.weekday(), time::Weekday::Saturday | time::Weekday::Sunday);
        return Ok((Vec::new(), (!weekend).then_some(why)));
    };
    let url = format!("{}/files/{volume}/{path}", jobs.store);
    let all = store_get(jobs, &url, missing::contracts).await?;
    let wanted = match a.contracts.as_ref().and_then(|m| m.get(&d)) {
        Some(list) => {
            let known: HashSet<&str> = all.iter().map(|c| c.symbol.as_str()).collect();
            let unknown: Vec<&str> = list
                .iter()
                .map(String::as_str)
                .filter(|s| !known.contains(s))
                .collect();
            if !unknown.is_empty() {
                return Err((
                    StatusCode::BAD_REQUEST,
                    format!(
                        "{key}: {} of these contracts aren't in the store's definitions for the day: {}",
                        unknown.len(),
                        unknown
                            .iter()
                            .take(5)
                            .copied()
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                ));
            }
            list.clone()
        }
        None => missing::wanted(
            &all,
            d,
            a.rule.as_ref().unwrap_or(&missing::Rule::default()),
        ),
    };
    let need = had::need(&wanted, pieces, window);
    let mut asks: Vec<(had::Window, Vec<String>)> = Vec::new();
    if !need.whole.is_empty() {
        asks.push((window, need.whole));
    }
    for (windows, contracts) in need.hours {
        asks.extend(windows.into_iter().map(|w| (w, contracts.clone())));
    }
    let note = asks.is_empty().then(|| {
        format!(
            "{key}: nothing to buy ({} wanted, {} held, {} pending or bought)",
            wanted.len(),
            need.skipped_held,
            need.skipped_pending
        )
    });
    let mut parts = Vec::new();
    for (w, contracts) in asks {
        for chunk in contracts.chunks(missing::MAX_SYMBOLS) {
            parts.push(missing::Part {
                day: d,
                last: d,
                window: w,
                whole: false,
                symbols: chunk.to_vec(),
                stype: SType::RawSymbol,
                cost_usd: 0.0,
                bytes: 0,
                skipped_held: need.skipped_held,
                skipped_pending: need.skipped_pending,
            });
        }
    }
    Ok((parts, note))
}

/// Price a request (see `plan`). Answers with the quote once priced, or,
/// when that takes longer than `price_wait`, with the quote still
/// `pricing` (202): `GET /quotes/{id}` says when it's ready.
async fn price(AxState(jobs): AxState<Shared>, Json(r): Json<PriceRequest>) -> Reply {
    let asked = match r.check() {
        Ok(a) => a,
        Err(e) => return refused(StatusCode::BAD_REQUEST, e),
    };
    let again = match again_ids(r.again.as_ref()) {
        Ok(a) => a,
        Err(e) => return e,
    };
    if let Err((code, e)) = known_dataset(&jobs, &asked.dataset).await {
        return refused(code, e);
    }
    let (warnings, conditions) = match availability(&jobs, &asked).await {
        Ok(w) => w,
        Err(e) => return refused(StatusCode::BAD_REQUEST, e),
    };
    let id = format!(
        "q-{}-{}",
        jobs.boot,
        jobs.next_quote.fetch_add(1, Ordering::Relaxed)
    );
    let asked = Arc::new(asked);
    let priced = Arc::new((AtomicUsize::new(0), AtomicUsize::new(0)));
    {
        let mut q = lock(&jobs.quotes);
        q.retain(|_, x| !x.expired());
        q.insert(
            id.clone(),
            Quote {
                request: asked.clone(),
                again: again.clone(),
                at: Instant::now(),
                priced: priced.clone(),
                result: None,
            },
        );
    }
    let (done, finished) = tokio::sync::oneshot::channel();
    let task = {
        let (jobs, id) = (jobs.clone(), id.clone());
        async move {
            let planned = tokio::time::timeout(
                jobs.pricing_cap,
                plan(&jobs, &asked, &again, false, &priced),
            )
            .await
            .unwrap_or_else(|_| {
                Err((
                    StatusCode::GATEWAY_TIMEOUT,
                    format!(
                        "pricing took longer than {} minutes, so it was given up",
                        jobs.pricing_cap.as_secs() / 60
                    ),
                ))
            });
            let free_only = asked.free_ignore_held;
            let result = planned
                .map(|(parts, notes)| Priced {
                    cost_usd: price_of(&parts),
                    notes: warnings.into_iter().chain(notes).collect(),
                    parts,
                    conditions,
                })
                .and_then(|p| {
                    if free_only && p.cost_usd > 0.0 {
                        Err((
                            StatusCode::CONFLICT,
                            format!(
                                "it costs ${:.2}; with free_ignore_held only a free purchase is priced",
                                p.cost_usd
                            ),
                        ))
                    } else {
                        Ok(p)
                    }
                });
            if let Some(q) = lock(&jobs.quotes).get_mut(&id) {
                q.result = Some(result);
                q.at = Instant::now();
            }
            let _ = done.send(());
        }
    };
    tokio::spawn(task);
    let _ = tokio::time::timeout(jobs.price_wait, finished).await;
    let mut quotes = lock(&jobs.quotes);
    let Some(q) = quotes.get(&id) else {
        return refused(StatusCode::CONFLICT, "that price was declined");
    };
    match &q.result {
        None => (StatusCode::ACCEPTED, Json(q.view(&id))),
        Some(Err((code, e))) => {
            let r = refused(*code, e.clone());
            quotes.remove(&id);
            r
        }
        Some(Ok(_)) => reply_ok(q.view(&id)),
    }
}

/// Every quote that is still good, or still being priced.
/// Every quote that is still good, or still being priced, and every
/// submission of one (for a day).
async fn quotes_list(AxState(jobs): AxState<Shared>) -> Reply {
    let mut list: Vec<(String, Value)> = {
        let mut q = lock(&jobs.quotes);
        q.retain(|_, x| !x.expired());
        q.iter().map(|(i, x)| (i.clone(), x.view(i))).collect()
    };
    {
        let mut s = lock(&jobs.submits);
        s.retain(|_, x| x.at.elapsed() < SUBMITS_KEPT);
        list.extend(s.iter().map(|(i, x)| (i.clone(), x.view())));
    }
    list.sort_by(|a, b| (a.0.len(), &a.0).cmp(&(b.0.len(), &b.0)));
    reply_ok(json!({ "quotes": list.into_iter().map(|(_, v)| v).collect::<Vec<_>>() }))
}

async fn quote_get(AxState(jobs): AxState<Shared>, UrlPath(id): UrlPath<String>) -> Reply {
    if let Some(q) = lock(&jobs.quotes).get(&id).filter(|q| !q.expired()) {
        return reply_ok(q.view(&id));
    }
    match lock(&jobs.submits)
        .get(&id)
        .filter(|x| x.at.elapsed() < SUBMITS_KEPT)
    {
        Some(x) => reply_ok(x.view()),
        None => refused(StatusCode::NOT_FOUND, "no such quote; it may have expired"),
    }
}

/// Decline a quote: it can't be submitted after this. Needs the approval
/// token, as approving does.
async fn quote_delete(
    AxState(jobs): AxState<Shared>,
    headers: axum::http::HeaderMap,
    UrlPath(id): UrlPath<String>,
) -> Reply {
    if let Err(r) = approved_by_console(&jobs, &headers) {
        return r;
    }
    match lock(&jobs.quotes).remove(&id) {
        Some(_) => reply_ok(json!({ "declined": id })),
        None => refused(StatusCode::NOT_FOUND, "no such quote; it may have expired"),
    }
}

fn reply_ok(v: Value) -> Reply {
    (StatusCode::OK, Json(v))
}

const NO_LEDGER: &str =
    "no submissions ledger is configured (JOBS_LEDGER), so nothing is submitted";

/// Write a step of `request` to the ledger.
fn write_step(
    jobs: &Jobs,
    request: &str,
    state: &str,
    job: Option<String>,
    cost_usd: f64,
    note: Option<String>,
) -> Result<(), String> {
    write_step_origin(jobs, request, state, job, cost_usd, note, None)
}

/// [`write_step`], saying who asked for the submission (decision 0033).
fn write_step_origin(
    jobs: &Jobs,
    request: &str,
    state: &str,
    job: Option<String>,
    cost_usd: f64,
    note: Option<String>,
    origin: Option<&str>,
) -> Result<(), String> {
    lock(&jobs.ledger)
        .as_mut()
        .ok_or_else(|| NO_LEDGER.to_owned())?
        .append(ledger::Entry {
            at: tinytrader_observe::unix_seconds(),
            request: request.to_owned(),
            state: state.into(),
            job,
            cost_usd: Some(cost_usd),
            note,
            origin: origin.map(str::to_owned),
            ..ledger::Entry::default()
        })
}

#[derive(Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
struct Submit {
    quote: String,
    confirm_usd: Option<f64>,
    /// When given, exactly the jobs the quote named in `again`.
    #[serde(default)]
    again: Option<Value>,
    /// Who approved a paid purchase (the console's login).
    #[serde(default)]
    approved_by: Option<String>,
    /// `manual` (the default) or `scheduled` (decision 0033): a job bought by
    /// hand is downloaded as soon as it's done, capture or not.
    #[serde(default)]
    origin: Option<String>,
}

/// The job IDs `again` names; a blanket `true` is refused.
fn again_ids(v: Option<&Value>) -> Result<HashSet<String>, Reply> {
    let refuse = || {
        refused(
            StatusCode::BAD_REQUEST,
            "\"again\" names the expired jobs this buys again, as [\"OPRA-…\"]; a blanket true is no longer accepted (decision 0019)",
        )
    };
    match v {
        None | Some(Value::Null | Value::Bool(false)) => Ok(HashSet::new()),
        Some(Value::Array(a)) => a
            .iter()
            .map(|x| x.as_str().map(str::to_owned).ok_or_else(refuse))
            .collect(),
        Some(_) => Err(refuse()),
    }
}

/// Submit a quote's requests. It runs to its end even if the caller goes
/// away, so a purchase is never left half-written.
async fn submit(
    AxState(jobs): AxState<Shared>,
    headers: axum::http::HeaderMap,
    Json(s): Json<Submit>,
) -> Reply {
    let approval = approval(&jobs, &headers, s.approved_by.as_deref());
    let id = s.quote.clone();
    let reply = tokio::spawn(submit_quote(jobs.clone(), s, approval))
        .await
        .unwrap_or_else(|e| refused(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()));
    let (code, Json(v)) = &reply;
    if !code.is_success() {
        // A refusal or a failure is said, with why, whatever the caller does.
        let why = v["error"].as_str().unwrap_or("no reason given");
        let at = v["progress"]
            .as_str()
            .map_or_else(String::new, |p| format!(" after {p} requests"));
        let text = format!("submission of {id} stopped ({}){at}: {why}", code.as_u16());
        jobs.events.push(text.clone(), text);
    }
    reply
}

async fn submit_quote(jobs: Shared, s: Submit, approval: Option<Reply>) -> Reply {
    // Checked here; send_quote reads it (decision 0033).
    if let Err(r) = manual_origin(s.origin.as_deref()) {
        return r;
    }
    let again = match s
        .again
        .as_ref()
        .filter(|a| !a.is_null())
        .map(|a| again_ids(Some(a)))
        .transpose()
    {
        Ok(a) => a,
        Err(e) => return e,
    };
    // A quote is used once, whatever happens next.
    let quote = {
        let mut quotes = lock(&jobs.quotes);
        match quotes.get(&s.quote) {
            None => {
                return refused(
                    StatusCode::CONFLICT,
                    "that price has expired, was declined or was already used; price the request again",
                );
            }
            Some(q) if q.result.is_none() => {
                return refused(StatusCode::CONFLICT, "that request is still being priced");
            }
            Some(q) if q.request.price_only => {
                return refused(
                    StatusCode::CONFLICT,
                    "a price-only quote can't be submitted; price the request without price_only",
                );
            }
            // A paid one is used up only by an approved, confirmed submission.
            Some(Quote {
                result: Some(Ok(p)),
                ..
            }) if p.cost_usd != 0.0 => {
                let quoted = p.cost_usd;
                if let Some(refusal) = approval {
                    return refusal;
                }
                if !quoted.is_finite() || s.confirm_usd.is_none_or(|c| (c - quoted).abs() > 1e-9) {
                    return refused(
                        StatusCode::PRECONDITION_REQUIRED,
                        format!("this costs ${quoted:.2}; confirm that amount to submit it"),
                    );
                }
                quotes.remove(&s.quote).expect("just found")
            }
            Some(_) => quotes.remove(&s.quote).expect("just found"),
        }
    };
    // From here the quote is used: its progress is kept, to its end.
    let (code, Json(mut v)) = send_quote(&jobs, &s, &quote, again, approval).await;
    let mut submits = lock(&jobs.submits);
    if let Some(x) = submits.get_mut(&s.quote) {
        x.at = Instant::now();
        if code.is_success() {
            x.state = "submitted";
        } else {
            x.state = "failed";
            x.error = v["error"].as_str().map(str::to_owned);
            v["progress"] = json!(format!("{} of {}", x.submitted, x.of));
        }
    }
    (code, Json(v))
}

/// Submit a quote taken for it (see `submit_quote`).
async fn send_quote(
    jobs: &Shared,
    s: &Submit,
    quote: &Quote,
    again: Option<HashSet<String>>,
    approval: Option<Reply>,
) -> Reply {
    // Checked by submit_quote: anything but "scheduled" is by hand.
    let manual = s.origin.as_deref() != Some("scheduled");
    if quote.request.price_only {
        return refused(
            StatusCode::CONFLICT,
            "a price-only quote can't be submitted; price the request without price_only",
        );
    }
    let priced = match quote.result {
        _ if quote.expired() => {
            return refused(
                StatusCode::CONFLICT,
                "that price has expired; price the request again",
            );
        }
        Some(Ok(ref p)) => p,
        _ => return refused(StatusCode::CONFLICT, "that request couldn't be priced"),
    };
    if again.is_some_and(|a| a != quote.again) {
        return refused(
            StatusCode::CONFLICT,
            format!(
                "\"again\" must name exactly the jobs the quote named ({}); price it again",
                json!(quote.again)
            ),
        );
    }
    if priced.parts.is_empty() {
        return refused(
            StatusCode::CONFLICT,
            format!("nothing to buy: {}", priced.notes.join("; ")),
        );
    }
    lock(&jobs.submits).insert(
        s.quote.clone(),
        Submitting {
            quote: quote.view(&s.quote),
            state: "submitting",
            submitted: 0,
            of: priced.parts.len(),
            jobs: Vec::new(),
            error: None,
            at: Instant::now(),
        },
    );
    // One purchase at a time, from the last check to the ledger's last word.
    let _turn = jobs.purchase.lock().await;
    if lock(&jobs.ledger).is_none() {
        return refused(StatusCode::SERVICE_UNAVAILABLE, NO_LEDGER);
    }
    // Plan again: what's held, pending, bought and priced may have changed.
    let a = &quote.request;
    let counts = (AtomicUsize::new(0), AtomicUsize::new(0));
    let parts = match plan(jobs, a, &quote.again, true, &counts).await {
        Ok((parts, _)) => parts,
        Err((_, e)) => {
            return refused(
                StatusCode::SERVICE_UNAVAILABLE,
                format!("couldn't plan it again, so nothing is submitted: {e}"),
            );
        }
    };
    let now = price_of(&parts);
    let same = parts.len() == priced.parts.len()
        && parts.iter().zip(&priced.parts).all(|(x, y)| x.same(y));
    if !same {
        return refused(
            StatusCode::CONFLICT,
            format!(
                "what's missing or its price changed (now ${now:.2} for {} requests); price it again",
                parts.len()
            ),
        );
    }
    if a.free_ignore_held && now > 0.0 {
        return refused(
            StatusCode::CONFLICT,
            format!(
                "it costs ${now:.2} now; with free_ignore_held only a free purchase is submitted"
            ),
        );
    }
    if let Some(r) = check_price(now, priced.cost_usd, s.confirm_usd, approval) {
        return r;
    }
    let by = s.approved_by.as_deref().map(str::trim);
    let mut submitted = Vec::new();
    for (i, p) in parts.iter().enumerate() {
        // Databento takes at most 20 submissions a minute.
        if i > 0 {
            tokio::time::sleep(SUBMIT_GAP).await;
        }
        let request = ledger::fingerprint(
            &a.dataset,
            a.schema.as_str(),
            &p.symbols,
            &had::stamp(p.window.0),
            &had::stamp(p.window.1),
        );
        // A paid purchase: who approved it, before anything is sent.
        let written = if now == 0.0 {
            Ok(())
        } else {
            lock(&jobs.ledger)
                .as_mut()
                .ok_or_else(|| NO_LEDGER.to_owned())
                .and_then(|l| {
                    l.append(ledger::Entry {
                        at: tinytrader_observe::unix_seconds(),
                        request: request.clone(),
                        state: "approved".into(),
                        cost_usd: Some(p.cost_usd),
                        by: by.map(str::to_owned),
                        quote: Some(s.quote.clone()),
                        ..ledger::Entry::default()
                    })
                })
        }
        .and_then(|()| write_step(jobs, &request, "sending", None, p.cost_usd, None));
        if let Err(e) = written {
            return refused(StatusCode::INTERNAL_SERVER_ERROR, json!({ "error": format!("could not write the submissions ledger, so the request for {} wasn't submitted: {e}", p.day), "submitted": submitted }).to_string());
        }
        let params = SubmitJobParams::builder()
            .dataset(&a.dataset)
            .schema(a.schema)
            .symbols(p.symbols.clone())
            .stype_in(p.stype)
            .date_time_range((had::at(p.window.0), had::at(p.window.1)))
            .build();
        let result = jobs.db.submit(&params).await;
        let (state, job, note) = match &result {
            Ok(job) => ("submitted", Some(job.id.clone()), None),
            Err(e) => ("uncertain", None, Some(brief(e))),
        };
        if let Err(e) = write_step_origin(
            jobs,
            &request,
            state,
            job,
            p.cost_usd,
            note,
            Some(if manual { "manual" } else { "scheduled" }),
        ) {
            // Unwritten, the "sending" step still blocks what it overlaps.
            eprintln!("databento-jobs: cannot write the ledger: {e}");
        }
        let what = if p.whole {
            a.symbol.clone()
        } else {
            format!("{} contracts of {}", p.symbols.len(), a.symbol)
        };
        match result {
            Ok(job) => {
                lock(&jobs.follow).insert(job.id.clone());
                if manual {
                    lock(&jobs.manual).insert(job.id.clone());
                }
                jobs.events.push(
                    format!(
                        "submitted {} ({} {} {what}, {} to {}, ${:.2})",
                        job.id,
                        a.dataset,
                        a.schema.as_str(),
                        p.day,
                        p.last,
                        p.cost_usd
                    ),
                    json!({ "job": job.id, "cost_usd": p.cost_usd }).to_string(),
                );
                let mut row = p.to_json();
                row["job"] = json!(job.id);
                submitted.push(row);
                if let Some(x) = lock(&jobs.submits).get_mut(&s.quote) {
                    x.submitted += 1;
                    x.jobs.push(job.id.clone());
                }
            }
            Err(e) => {
                return refused(StatusCode::BAD_GATEWAY, json!({ "error": format!("{what} from {}: submission failed or its outcome is unknown ({e}); it blocks what it overlaps until it shows on Databento's job list or is resolved (POST /ledger/resolve); nothing after it was submitted", p.day), "submitted": submitted }).to_string());
            }
        }
    }
    reply_ok(json!({ "submitted": submitted, "cost_usd": now }))
}

/// The submissions whose outcome isn't known: each one's latest step (its
/// `request` fingerprint and `at`, which `POST /ledger/resolve` names) and
/// when it was sent. Reads only.
async fn ledger_unresolved(AxState(jobs): AxState<Shared>) -> Reply {
    let guard = lock(&jobs.ledger);
    let Some(l) = guard.as_ref() else {
        return refused(StatusCode::SERVICE_UNAVAILABLE, "no ledger configured");
    };
    let list: Vec<Value> = l
        .unresolved()
        .iter()
        .map(|e| {
            let mut v = json!(e);
            v["sent_at"] = json!(l.attempt_at(e));
            v
        })
        .collect();
    reply_ok(json!({ "unresolved": list }))
}

#[derive(Deserialize)]
struct Resolve {
    /// The request's fingerprint, as in the ledger.
    request: String,
    /// When its unresolved step was written, as in the ledger.
    at: f64,
    /// The job it made, or `null` for none.
    job: Option<String>,
    note: String,
}

/// How long after a submission was sent the operator may say it was never
/// made, and then only once a full read of the job list since hasn't found
/// it.
const WRITE_OFF_AFTER: f64 = 30.0 * 60.0;

/// The operator's word on a submission whose outcome isn't known: the job it
/// made (`submitted`), or none (`not-submitted`). Only a request's latest
/// step, if unresolved, can be resolved. A job must be on Databento's list,
/// for the same dataset and schema, and not another submission's. None only
/// once the submission is [`WRITE_OFF_AFTER`] old and a complete read of the
/// job list begun after that didn't find it.
async fn resolve(AxState(jobs): AxState<Shared>, Json(r): Json<Resolve>) -> Reply {
    if r.note.trim().is_empty() {
        return refused(StatusCode::BAD_REQUEST, "say how you know, in note");
    }
    let _turn = jobs.purchase.lock().await;
    let Some((dataset, schema, _, _)) = had::parse_fingerprint(&r.request) else {
        return refused(StatusCode::BAD_REQUEST, "that isn't a request fingerprint");
    };
    if let Some(id) = &r.job {
        let listed = match jobs.db.list_jobs(&api::every_job()).await {
            Ok(l) => l,
            Err(e) => {
                return refused(
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!("Databento's job list: {e}"),
                );
            }
        };
        if !listed.iter().any(|j| &j.id == id) {
            return refused(
                StatusCode::CONFLICT,
                format!("{id} isn't on Databento's job list"),
            );
        }
        let known = lock(&jobs.details).get(id).cloned();
        let job = match known {
            Some(j) => j,
            None => match jobs.db.job(id).await {
                Ok(j) => j,
                Err(e) => {
                    return refused(
                        StatusCode::SERVICE_UNAVAILABLE,
                        format!("Databento's job {id}: {e}"),
                    );
                }
            },
        };
        if !job.dataset.eq_ignore_ascii_case(&dataset) || job.schema.as_str() != schema {
            return refused(
                StatusCode::CONFLICT,
                format!(
                    "{id} is {} {}, not this request's dataset and schema",
                    job.dataset,
                    job.schema.as_str()
                ),
            );
        }
    }
    let mut guard = lock(&jobs.ledger);
    let Some(l) = guard.as_mut() else {
        return refused(StatusCode::SERVICE_UNAVAILABLE, NO_LEDGER);
    };
    let Some(e) = l
        .unresolved_step(&r.request)
        .filter(|e| (e.at - r.at).abs() < 1e-6)
        .cloned()
    else {
        return refused(
            StatusCode::NOT_FOUND,
            "no unresolved submission of that request at that time",
        );
    };
    match &r.job {
        Some(id) if l.submitted().contains(id) => {
            return refused(
                StatusCode::CONFLICT,
                format!("{id} is already another submission's job"),
            );
        }
        Some(_) => {}
        None => {
            let after = l.attempt_at(&e) + WRITE_OFF_AFTER;
            let now = tinytrader_observe::unix_seconds();
            let checked = lock(&jobs.checked_at).is_some_and(|t| t >= after);
            if now < after || !checked {
                return refused(
                    StatusCode::CONFLICT,
                    format!(
                        "it can be written off as never made only after {} and a complete read of Databento's job list begun after then (every 30 s or so) that doesn't find it",
                        tinytrader_schedule::describe((after * 1e9) as u64)
                    ),
                );
            }
        }
    }
    let state = if r.job.is_some() {
        "submitted"
    } else {
        "not-submitted"
    };
    if let Err(err) = l.append(ledger::Entry {
        at: tinytrader_observe::unix_seconds(),
        request: e.request.clone(),
        state: state.into(),
        job: r.job.clone(),
        cost_usd: e.cost_usd,
        note: Some(format!("resolved by the operator: {}", r.note.trim())),
        ..Default::default()
    }) {
        return refused(StatusCode::INTERNAL_SERVER_ERROR, err);
    }
    if let Some(j) = &r.job {
        lock(&jobs.follow).insert(j.clone());
    }
    let text = format!("resolved a submission: {state} ({})", e.request);
    jobs.events.push(text.clone(), text);
    reply_ok(json!({ "request": e.request, "state": state, "job": r.job }))
}

async fn download(
    AxState(jobs): AxState<Shared>,
    UrlPath(job): UrlPath<String>,
    Query(q): Query<OriginQuery>,
) -> Reply {
    let manual = match manual_origin(q.origin.as_deref()) {
        Ok(m) => m,
        Err(r) => return r,
    };
    let details = jobs.db.job(&job).await;
    match details {
        Ok(j) if j.state != JobState::Done => {
            return refused(StatusCode::CONFLICT, "the job isn't done yet");
        }
        Ok(j) => {
            if let Some(why) = unfileable(&j) {
                return refused(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    format!("not downloaded: {why}"),
                );
            }
        }
        Err(e) => return refused(StatusCode::BAD_GATEWAY, format!("job {job}: {e}")),
    }
    let busy = lock(&jobs.transfers)
        .get(&job)
        .is_some_and(|t| t.error.is_none() && t.state != "filed");
    if busy {
        return refused(StatusCode::CONFLICT, "already downloading");
    }
    start_download(&jobs, job.clone(), manual);
    (
        StatusCode::ACCEPTED,
        Json(json!({ "job": job, "origin": if manual { "manual" } else { "scheduled" } })),
    )
}

/// Whether work was asked for by hand (decision 0033): `manual` (the default)
/// runs at once and skips the capture waits; `scheduled` yields. Anything else
/// is refused.
fn manual_origin(origin: Option<&str>) -> Result<bool, Reply> {
    match origin {
        None | Some("manual") => Ok(true),
        Some("scheduled") => Ok(false),
        Some(o) => Err(refused(
            StatusCode::BAD_REQUEST,
            format!("origin {o:?}: manual or scheduled"),
        )),
    }
}

#[derive(Deserialize, Default)]
struct OriginQuery {
    origin: Option<String>,
}

/// How often, while capture is live, the jobs bought by hand are checked for
/// being done.
const MANUAL_CHECK: Duration = Duration::from_secs(if cfg!(test) { 0 } else { 60 });

/// While capture is live (the refresh paused), a job bought by hand is still
/// downloaded as soon as it's done (decision 0033). An expired one is
/// forgotten here; a filed, failing or in-flight one is left alone.
async fn start_manual_downloads(jobs: &Shared) {
    let ids: Vec<String> = lock(&jobs.manual).iter().cloned().collect();
    for id in ids {
        let skip = lock(&jobs.in_flight).contains(&id)
            || lock(&jobs.transfers)
                .get(&id)
                .is_some_and(|t| t.state == "filed")
            || lock(&jobs.tries)
                .get(&id)
                .is_some_and(|(n, at)| *n >= MAX_TRIES || Instant::now() < *at);
        if skip {
            continue;
        }
        match jobs.db.job(&id).await {
            Ok(j) if j.state == JobState::Expired => {
                lock(&jobs.manual).remove(&id);
            }
            Ok(j) if j.state == JobState::Done && unfileable(&j).is_none() => {
                start_download(jobs, id, true);
            }
            _ => {}
        }
    }
}

/// How many cache requests may wait at once; past that, `503`.
const CACHE_QUEUE: usize = 256;

/// The marker a cache request leaves in its folder until it has ended, so a
/// restart picks the request up again.
const REQUESTED: &str = "REQUESTED";

/// Queue a done job's files for the cache folder (`JOBS_CACHE_DIR`), not the
/// store: a copy for reading while the store can't file it. The key stays
/// here; each file is checked against Databento's size and SHA-256 before it
/// takes its name. Files already there and whole are kept.
///
/// One worker downloads them, a job at a time, in the order asked. While
/// capture is live it waits, and a request made then is queued and runs when
/// capture ends. A request survives a restart (its `REQUESTED` marker), and a
/// network error or Databento's 429 or 5xx is tried again later, not failed.
async fn cache(
    AxState(jobs): AxState<Shared>,
    UrlPath(job): UrlPath<String>,
    Query(q): Query<OriginQuery>,
) -> Reply {
    let manual = match manual_origin(q.origin.as_deref()) {
        Ok(m) => m,
        Err(r) => return r,
    };
    let Some(root) = jobs.cache_dir.clone() else {
        return refused(
            StatusCode::SERVICE_UNAVAILABLE,
            "no cache folder is configured (JOBS_CACHE_DIR)",
        );
    };
    if !job.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') || job.is_empty() {
        return refused(StatusCode::BAD_REQUEST, "not a job ID");
    }
    match jobs.db.job(&job).await {
        Ok(j) if j.state != JobState::Done => {
            return refused(StatusCode::CONFLICT, "the job isn't done yet");
        }
        Ok(_) => {}
        Err(e) => return refused(StatusCode::BAD_GATEWAY, format!("job {job}: {e}")),
    }
    let dir = root.join(&job);
    let marker = dir.join(REQUESTED);
    // Its marker is there from the moment it's asked for until it has ended,
    // before the worker has even taken it.
    if marker.exists() || lock(&jobs.caching).get(&job).is_some_and(cache_active) {
        return refused(StatusCode::CONFLICT, "already queued or caching");
    }
    if let Err(e) = std::fs::create_dir_all(&dir).and_then(|()| {
        std::fs::write(
            &marker,
            json!({ "job": job, "requested_at": tinytrader_observe::unix_seconds(),
                    "origin": if manual { "manual" } else { "scheduled" } })
            .to_string(),
        )
    }) {
        return refused(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("{}: {e}", marker.display()),
        );
    }
    if cache_sender(&jobs)
        .of(manual)
        .try_send(CacheAsk {
            job: job.clone(),
            due: None,
            manual,
        })
        .is_err()
    {
        let _ = std::fs::remove_file(&marker);
        return refused(
            StatusCode::SERVICE_UNAVAILABLE,
            format!("busy: {CACHE_QUEUE} cache requests are already waiting; try again later"),
        );
    }
    lock(&jobs.caching).insert(
        job.clone(),
        Transfer {
            state: "queued".into(),
            requested_at: Some(tinytrader_observe::unix_seconds()),
            ..Transfer::default()
        },
    );
    (
        StatusCode::ACCEPTED,
        Json(json!({ "job": job, "folder": dir, "state": "queued",
                     "origin": if manual { "manual" } else { "scheduled" } })),
    )
}

/// Whether a cache request is still to run or running.
fn cache_active(t: &Transfer) -> bool {
    t.error.is_none() && t.state != "cached"
}

/// Every cache request this run has seen: queued (with how many are ahead),
/// waiting for capture to end, downloading, retrying, cached or failed.
async fn cache_list(AxState(jobs): AxState<Shared>) -> Reply {
    let mut rows: Vec<Value> = lock(&jobs.caching)
        .iter()
        .map(|(id, t)| {
            json!({ "job": id, "state": t.state, "position": Value::Null, "file": t.file,
                    "done_bytes": t.done_bytes, "total_bytes": t.total_bytes, "error": t.error,
                    "requested_at": t.requested_at })
        })
        .collect();
    rows.sort_by(|a, b| {
        a["requested_at"]
            .as_f64()
            .partial_cmp(&b["requested_at"].as_f64())
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    // A queued request's position: how many still to run were asked for first.
    let mut ahead = 0;
    for r in &mut rows {
        let state = r["state"].as_str().unwrap_or_default().to_owned();
        if state == "queued" {
            r["position"] = json!(ahead);
        }
        if state != "cached" && state != "failed" {
            ahead += 1;
        }
    }
    let waiting = rows.iter().filter(|r| r["state"] == "queued").count();
    (
        StatusCode::OK,
        Json(json!({ "cache": rows, "queued": waiting, "capacity": CACHE_QUEUE })),
    )
}

/// A cache request in the worker's queue: its job, and when it may next run
/// (none: now). A request that must wait (a transient error, too little disk)
/// goes to the back with a time, so the rest proceed.
struct CacheAsk {
    job: String,
    due: Option<Instant>,
    /// Asked for by hand: taken first, and never waits for capture (0033).
    manual: bool,
}

/// The cache worker's two bounded queues (each `CACHE_QUEUE`): requests asked
/// for by hand, taken first, and scheduled ones.
#[derive(Clone)]
struct CacheQueues {
    manual: tokio::sync::mpsc::Sender<CacheAsk>,
    scheduled: tokio::sync::mpsc::Sender<CacheAsk>,
}

impl CacheQueues {
    fn of(&self, manual: bool) -> &tokio::sync::mpsc::Sender<CacheAsk> {
        if manual {
            &self.manual
        } else {
            &self.scheduled
        }
    }
}

/// The cache worker's queues, starting the worker on first use.
fn cache_sender(jobs: &Shared) -> &CacheQueues {
    jobs.cache_queue.get_or_init(|| {
        let (manual, rm) = tokio::sync::mpsc::channel(CACHE_QUEUE);
        let (scheduled, rs) = tokio::sync::mpsc::channel(CACHE_QUEUE);
        let q = CacheQueues { manual, scheduled };
        tokio::spawn(cache_worker(Arc::clone(jobs), q.clone(), rm, rs));
        q
    })
}

/// At startup, the cache requests a restart interrupted: every folder with a
/// `REQUESTED` marker and no `COMPLETE`, queued again in the order asked. All
/// of them are queued, waiting for room when there are more than the queue
/// holds; until then new requests are refused as busy.
fn resume_caching(jobs: &Shared) {
    let Some(root) = jobs.cache_dir.clone() else {
        return;
    };
    let Ok(entries) = std::fs::read_dir(&root) else {
        return;
    };
    let mut asked: Vec<(f64, String, bool)> = Vec::new();
    for e in entries.flatten() {
        let dir = e.path();
        let Some(job) = dir.file_name().and_then(|n| n.to_str()).map(str::to_owned) else {
            continue;
        };
        let Ok(text) = std::fs::read_to_string(dir.join(REQUESTED)) else {
            continue;
        };
        if dir.join("COMPLETE").exists() {
            let _ = std::fs::remove_file(dir.join(REQUESTED));
            continue;
        }
        let v = serde_json::from_str::<Value>(&text).unwrap_or(Value::Null);
        let at = v["requested_at"].as_f64().unwrap_or(0.0);
        // Markers from before origins were all asked for by hand.
        asked.push((at, job, v["origin"] != "scheduled"));
    }
    asked.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    {
        let mut c = lock(&jobs.caching);
        for (at, job, _) in &asked {
            c.insert(
                job.clone(),
                Transfer {
                    state: "queued".into(),
                    requested_at: Some(*at),
                    ..Transfer::default()
                },
            );
        }
    }
    let q = cache_sender(jobs).clone();
    tokio::spawn(async move {
        for (_, job, manual) in asked {
            let ask = CacheAsk {
                job,
                due: None,
                manual,
            };
            if q.of(manual).send(ask).await.is_err() {
                return;
            }
        }
    });
}

/// The one worker that fills the cache, from the one bounded queue: a job at a
/// time, in order. A job that has to wait goes to the back of the queue with
/// the time it may run again, so a dead or stalled job never holds up the
/// rest.
async fn cache_worker(
    jobs: Shared,
    q: CacheQueues,
    mut rm: tokio::sync::mpsc::Receiver<CacheAsk>,
    mut rs: tokio::sync::mpsc::Receiver<CacheAsk>,
) {
    // Each waiting job's next backoff: only jobs in the queues, so bounded by them.
    let mut backoff: HashMap<String, Duration> = HashMap::new();
    // Requests passed over in a row because they weren't due yet.
    let mut passed = 0usize;
    loop {
        // Asked for by hand first (decision 0033).
        let ask = tokio::select! {
            biased;
            Some(a) = rm.recv() => a,
            Some(a) = rs.recv() => a,
            else => return,
        };
        if let Some(due) = ask.due
            && due > Instant::now()
        {
            passed += 1;
            // Everything queued is waiting: rest a moment before looking again,
            // rather than spin.
            if passed > rm.len() + rs.len() {
                let rest = due.saturating_duration_since(Instant::now());
                tokio::time::sleep(rest.min(Duration::from_secs(1))).await;
                passed = 0;
            }
            match q.of(ask.manual).try_send(ask) {
                Ok(()) => continue,
                // The queue filled meanwhile: this one waits its time and runs.
                Err(e) => {
                    let ask = e.into_inner();
                    if let Some(due) = ask.due {
                        tokio::time::sleep_until(due.into()).await;
                    }
                    run_cache_turn(&jobs, &q, &mut backoff, ask.job, ask.manual).await;
                }
            }
            continue;
        }
        passed = 0;
        run_cache_turn(&jobs, &q, &mut backoff, ask.job, ask.manual).await;
    }
}

/// One turn of a cache request: done, failed, or back in the queue for later.
async fn run_cache_turn(
    jobs: &Jobs,
    q: &CacheQueues,
    backoff: &mut HashMap<String, Duration>,
    mut job: String,
    manual: bool,
) {
    let Some(dir) = jobs.cache_dir.as_ref().map(|r| r.join(&job)) else {
        return;
    };
    loop {
        set_cache(jobs, &job, |t| t.state = "starting".into());
        let (state, wait) = match fetch_to_cache(jobs, &job, &dir, manual).await {
            Ok(n) => {
                backoff.remove(&job);
                let _ = std::fs::remove_file(dir.join(REQUESTED));
                set_cache(jobs, &job, |t| t.state = "cached".into());
                jobs.events.push(
                    format!("cached {job} in {} ({n} files)", dir.display()),
                    job.clone(),
                );
                return;
            }
            Err(CacheError::Permanent(e)) => {
                backoff.remove(&job);
                let _ = std::fs::remove_file(dir.join(REQUESTED));
                set_cache(jobs, &job, |t| {
                    t.state = "failed".into();
                    t.error = Some(e.clone());
                });
                jobs.events.push(format!("caching {job} failed: {e}"), e);
                return;
            }
            Err(CacheError::Transient(e)) => {
                let first = Duration::from_millis(if cfg!(test) { 20 } else { 15_000 });
                let wait = backoff.get(&job).copied().unwrap_or(first);
                backoff.insert(job.clone(), (wait * 2).min(Duration::from_secs(600)));
                eprintln!("databento-jobs: cache {job}: {e}; trying again in {wait:?}");
                (format!("retrying: {e}"), wait)
            }
            Err(CacheError::Capture(why)) => (
                why,
                Duration::from_millis(if cfg!(test) { 20 } else { 15_000 }),
            ),
            Err(CacheError::Disk(e)) => (
                format!("waiting for disk: {e}"),
                Duration::from_millis(if cfg!(test) { 20 } else { 60_000 }),
            ),
        };
        set_cache(jobs, &job, |t| t.state = state);
        let ask = CacheAsk {
            job,
            due: Some(Instant::now() + wait),
            manual,
        };
        match q.of(manual).try_send(ask) {
            Ok(()) => return,
            // No room behind the others (the worker is its own queue's only
            // reader, so it can't wait for room): it waits here instead, and
            // has its next turn now.
            Err(e) => {
                job = e.into_inner().job;
                tokio::time::sleep(wait).await;
            }
        }
    }
}

/// A cache download's error: one worth trying again later (the network,
/// Databento's 429 or 5xx, a file cut short), too little disk for it yet, or
/// one that won't change.
enum CacheError {
    /// A scheduled request while capture is live: back to the queue, waiting.
    Capture(String),
    Transient(String),
    Disk(String),
    Permanent(String),
}

/// Why a job's files couldn't be listed: permanent if the job has expired, is
/// no longer done, or Databento answered a status that asking again won't
/// change (a 4xx but 408 and 429); otherwise transient. Decided on the job's
/// state and the HTTP status, never on the error's text.
async fn listing_error(jobs: &Jobs, job: &str, e: api::CallError) -> CacheError {
    match jobs.db.job_checked(job).await {
        Ok(j) if j.state == JobState::Expired => {
            CacheError::Permanent(format!("the job has expired: {e}"))
        }
        Ok(j) if j.state != JobState::Done => {
            CacheError::Permanent(format!("the job is no longer done: {e}"))
        }
        Err(e2) if e2.permanent() => CacheError::Permanent(format!(
            "the job: Databento answered {}: {e2}",
            e2.status.unwrap_or_default()
        )),
        _ if e.permanent() => CacheError::Permanent(format!(
            "list files: Databento answered {}: {e}",
            e.status.unwrap_or_default()
        )),
        _ => CacheError::Transient(format!("list files: {e}")),
    }
}

/// Whether `need` more bytes fit on the cache's volume, keeping its floor free:
/// at least `cache_min_free` bytes, and `cache_min_free_pct` of the volume.
fn disk_room(jobs: &Jobs, dir: &std::path::Path, need: u64) -> Result<(), CacheError> {
    let (free, total) = (fs4::available_space(dir), fs4::total_space(dir));
    let (Ok(free), Ok(total)) = (free, total) else {
        return Err(CacheError::Disk(format!(
            "{}: can't read its free space",
            dir.display()
        )));
    };
    let floor = jobs
        .cache_min_free
        .max(total / 100 * jobs.cache_min_free_pct);
    #[cfg(test)]
    let floor = floor.max(jobs.cache_floor_for_tests.load(Ordering::SeqCst));
    if free.saturating_sub(need) < floor {
        return Err(CacheError::Disk(format!(
            "{need} bytes to download, {free} free on {}, keeping {floor} free",
            dir.display()
        )));
    }
    Ok(())
}

/// Delete a job's cache folder (refused while it is being filled).
async fn uncache(AxState(jobs): AxState<Shared>, UrlPath(job): UrlPath<String>) -> Reply {
    let Some(root) = jobs.cache_dir.clone() else {
        return refused(
            StatusCode::SERVICE_UNAVAILABLE,
            "no cache folder is configured (JOBS_CACHE_DIR)",
        );
    };
    if !job.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') || job.is_empty() {
        return refused(StatusCode::BAD_REQUEST, "not a job ID");
    }
    if lock(&jobs.caching).get(&job).is_some_and(cache_active) {
        return refused(StatusCode::CONFLICT, "still queued or caching");
    }
    let dir = root.join(&job);
    match std::fs::remove_dir_all(&dir) {
        Ok(()) => {
            lock(&jobs.caching).remove(&job);
            (StatusCode::OK, Json(json!({ "deleted": dir })))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            refused(StatusCode::NOT_FOUND, "nothing cached for that job")
        }
        Err(e) => refused(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("{}: {e}", dir.display()),
        ),
    }
}

fn set_cache(jobs: &Jobs, job: &str, f: impl FnOnce(&mut Transfer)) {
    f(lock(&jobs.caching).entry(job.to_owned()).or_default());
}

/// Every file of a done job into `dir`, then its manifest. Like the store's
/// downloads it waits while capture is live. Returns how many files it
/// downloaded (whole ones already there are kept).
async fn fetch_to_cache(
    jobs: &Jobs,
    job: &str,
    dir: &std::path::Path,
    manual: bool,
) -> Result<usize, CacheError> {
    use CacheError::{Permanent, Transient};
    use std::os::unix::fs::PermissionsExt as _;
    let files = match jobs.db.files_checked(job).await {
        Ok(f) => f,
        Err(e) => return Err(listing_error(jobs, job, e).await),
    };
    std::fs::create_dir_all(dir).map_err(|e| Permanent(format!("{}: {e}", dir.display())))?;
    // The whole job must fit before any of it is fetched (files already there
    // at their size count as had); each file is checked again before it is.
    let missing: u64 = files
        .iter()
        .filter(|f| std::fs::metadata(dir.join(&f.filename)).map_or(true, |m| m.len() != f.size))
        .map(|f| f.size)
        .sum();
    disk_room(jobs, dir, missing)?;
    let _ = std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o755));
    // COMPLETE is written last: until then the folder isn't whole (Databento's
    // own file list has a manifest.json, which arrives like any other file).
    let complete = dir.join("COMPLETE");
    let _ = std::fs::remove_file(&complete);
    let total: u64 = files.iter().map(|f| f.size).sum();
    set_cache(jobs, job, |t| {
        t.state = "downloading".into();
        t.total_bytes = total;
    });
    let (mut done, mut fetched) = (0u64, 0usize);
    for f in &files {
        if f.filename.contains('/') || f.filename.starts_with('.') {
            return Err(Permanent(format!("{}: not a plain file name", f.filename)));
        }
        set_cache(jobs, job, |t| t.file = f.filename.clone());
        let path = dir.join(&f.filename);
        let want = f.hash.trim_start_matches("sha256:").to_ascii_lowercase();
        let whole = std::fs::metadata(&path).is_ok_and(|m| m.len() == f.size)
            && sha256_file(&path).is_ok_and(|h| h == want);
        if !whole {
            // A scheduled request yields to capture by going back to the queue
            // (so the worker takes what was asked for by hand meanwhile); one
            // asked for by hand doesn't wait (decision 0033).
            if !manual {
                let why = match live_capture(jobs).await {
                    Some(who) => Some(format!("waiting: {who} is live")),
                    None => capture_soon(jobs, f.size).map(|at| {
                        format!("waiting: capture starts at {at}, before this file could finish")
                    }),
                };
                if let Some(why) = why {
                    return Err(CacheError::Capture(why));
                }
            }
            disk_room(jobs, dir, f.size)?;
            set_cache(jobs, job, |t| t.state = "downloading".into());
            let url = f
                .urls
                .get("https")
                .ok_or_else(|| Permanent(format!("{}: no https URL", f.filename)))?;
            let mut attempt = 0;
            loop {
                attempt += 1;
                match copy_to_file(jobs, job, url, &path, f.size, &want, done).await {
                    Ok(()) => break,
                    Err(Transient(e)) if attempt < 3 => {
                        eprintln!("databento-jobs: cache {job}/{}: {e}; retrying", f.filename);
                        let wait = if cfg!(test) { 0 } else { 5 * attempt };
                        tokio::time::sleep(Duration::from_secs(wait)).await;
                    }
                    Err(Transient(e)) => return Err(Transient(format!("{}: {e}", f.filename))),
                    Err(Permanent(e)) => return Err(Permanent(format!("{}: {e}", f.filename))),
                    Err(e @ (CacheError::Disk(_) | CacheError::Capture(_))) => return Err(e),
                }
            }
            fetched += 1;
        }
        done += f.size;
        set_cache(jobs, job, |t| t.done_bytes = done);
    }
    if !files.iter().any(|f| f.filename == "manifest.json") {
        let manifest = json!({
            "job_id": job,
            "files": files.iter().map(|f| json!({ "filename": f.filename, "size": f.size, "hash": f.hash })).collect::<Vec<_>>(),
        });
        let path = dir.join("manifest.json");
        std::fs::write(
            &path,
            serde_json::to_vec_pretty(&manifest).unwrap_or_default(),
        )
        .map_err(|e| Permanent(format!("{}: {e}", path.display())))?;
        let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644));
    }
    let done = json!({
        "job_id": job,
        "files": files.len(),
        "bytes": total,
        "finished_at": tinytrader_observe::unix_seconds(),
    });
    std::fs::write(
        &complete,
        serde_json::to_vec_pretty(&done).unwrap_or_default(),
    )
    .map_err(|e| Permanent(format!("{}: {e}", complete.display())))?;
    let _ = std::fs::set_permissions(&complete, std::fs::Permissions::from_mode(0o644));
    Ok(fetched)
}

/// One file into `path`: written to `<path>.part`, checked for its size
/// and SHA-256, then renamed, readable by everyone.
async fn copy_to_file(
    jobs: &Jobs,
    job: &str,
    url: &str,
    path: &std::path::Path,
    size: u64,
    sha256: &str,
    before: u64,
) -> Result<(), CacheError> {
    use CacheError::{Permanent, Transient};
    use sha2::Digest as _;
    use std::io::Write as _;
    use std::os::unix::fs::PermissionsExt as _;
    let part = path.with_extension(format!(
        "{}.part",
        path.extension()
            .and_then(|e| e.to_str())
            .unwrap_or_default()
    ));
    let resp = jobs
        .http
        .get(url)
        .basic_auth(&jobs.key, None::<&str>)
        .send()
        .await
        .map_err(|e| Transient(e.to_string()))?;
    let status = resp.status();
    if !status.is_success() {
        let m = format!("Databento answered {status}");
        return Err(
            if status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS {
                Transient(m)
            } else {
                Permanent(m)
            },
        );
    }
    let mut out =
        std::fs::File::create(&part).map_err(|e| Permanent(format!("{}: {e}", part.display())))?;
    let mut hasher = sha2::Sha256::new();
    let mut got = 0u64;
    let mut stream = resp.bytes_stream();
    let mut tick = Instant::now();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| Transient(e.to_string()))?;
        out.write_all(&chunk)
            .map_err(|e| Permanent(format!("{}: {e}", part.display())))?;
        hasher.update(&chunk);
        got += chunk.len() as u64;
        if tick.elapsed() >= Duration::from_secs(1) {
            set_cache(jobs, job, |t| t.done_bytes = before + got);
            tick = Instant::now();
        }
    }
    out.sync_all()
        .map_err(|e| Permanent(format!("{}: {e}", part.display())))?;
    drop(out);
    let hash: String = hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    if got != size || (!sha256.is_empty() && hash != sha256) {
        let _ = std::fs::remove_file(&part);
        let m =
            format!("got {got} bytes with SHA-256 {hash}; Databento lists {size} bytes, {sha256}");
        // Cut short is the connection's doing; the whole size with another hash isn't.
        return Err(if got < size {
            Transient(m)
        } else {
            Permanent(m)
        });
    }
    let _ = std::fs::set_permissions(&part, std::fs::Permissions::from_mode(0o644));
    std::fs::rename(&part, path).map_err(|e| Permanent(format!("{}: {e}", path.display())))
}

/// The SHA-256 of a file, as lowercase hex.
fn sha256_file(path: &std::path::Path) -> std::io::Result<String> {
    use sha2::Digest as _;
    let mut f = std::fs::File::open(path)?;
    let mut hasher = sha2::Sha256::new();
    std::io::copy(&mut f, &mut hasher)?;
    Ok(hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect())
}

fn set(jobs: &Jobs, job: &str, f: impl FnOnce(&mut Transfer)) {
    f(lock(&jobs.transfers).entry(job.to_owned()).or_default());
}

/// Download and file a job, unless one for it is queued or running. `manual`
/// (asked for by hand, or bought by hand: decision 0033) doesn't wait for
/// capture.
fn start_download(jobs: &Shared, job: String, manual: bool) {
    if !lock(&jobs.in_flight).insert(job.clone()) {
        return;
    }
    set(jobs, &job, |t| {
        *t = Transfer {
            state: "waiting".into(),
            ..Transfer::default()
        }
    });
    let jobs = jobs.clone();
    tokio::spawn(async move {
        let turn = jobs.downloads.acquire().await.ok();
        // Scheduled downloads wait while capture is live.
        while !manual && jobs.paused_until.load(Ordering::Relaxed) != 0 {
            set(&jobs, &job, |t| t.state = "waiting: capture is live".into());
            tokio::time::sleep(Duration::from_secs(15)).await;
        }
        let result = fetch_and_file(&jobs, &job, turn, manual).await;
        lock(&jobs.reserved).remove(&job);
        match result {
            Ok(n) => {
                lock(&jobs.tries).remove(&job);
                set(&jobs, &job, |t| t.state = "filed".into());
                let text = if n == 0 {
                    format!("{job} is already filed in the store")
                } else {
                    format!("filed {job} in the store ({n} files)")
                };
                jobs.events.push(text, job.clone());
            }
            Err(e) => {
                // Tried again later, a few times, waiting longer each time.
                let n = {
                    let mut tries = lock(&jobs.tries);
                    let t = tries.entry(job.clone()).or_insert((0, Instant::now()));
                    t.0 += 1;
                    t.1 = Instant::now() + RETRY_AFTER * 2u32.pow(t.0 - 1);
                    t.0
                };
                set(&jobs, &job, |t| {
                    t.state = "failed".into();
                    t.error = Some(e.clone());
                });
                let then = if n < MAX_TRIES {
                    format!("; tried again later ({n} of {MAX_TRIES})")
                } else {
                    format!("; left failed after {MAX_TRIES} tries")
                };
                jobs.events
                    .push(format!("download or filing of {job} failed: {e}{then}"), e);
            }
        }
        lock(&jobs.in_flight).remove(&job);
    });
}

/// Whether the store has already filed this batch.
async fn filed(jobs: &Jobs, job: &str) -> bool {
    let r = jobs
        .http
        .get(format!("{}/batches/databento", jobs.store))
        .timeout(Duration::from_secs(120))
        .send()
        .await;
    match r {
        Ok(r) if r.status().is_success() => r
            .json::<Value>()
            .await
            .is_ok_and(|v| v["batches"][job]["how"] == "filed"),
        _ => false,
    }
}

/// Stream every file of a done job into the store, then, its download's
/// `turn` given back, have it filed (none, `Ok(0)`, when the store already
/// has).
async fn fetch_and_file(
    jobs: &Jobs,
    job: &str,
    turn: Option<tokio::sync::SemaphorePermit<'_>>,
    manual: bool,
) -> Result<usize, String> {
    if filed(jobs, job).await {
        return Ok(0);
    }
    let files = jobs
        .db
        .files(job)
        .await
        .map_err(|e| format!("list files: {e}"))?;
    // What the store already has of this batch (verified on arrival).
    let incoming: Value = store_read(jobs, "/incoming", Duration::from_secs(60))
        .await?
        .json()
        .await
        .map_err(|e| format!("store: {e}"))?;
    let have: HashMap<String, u64> = incoming["batches"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|b| b["batch"] == job)
        .flat_map(|b| b["files"].as_array().cloned().unwrap_or_default())
        .filter_map(|f| Some((f["name"].as_str()?.to_owned(), f["bytes"].as_u64()?)))
        .collect();
    let total: u64 = files.iter().map(|f| f.size).sum();
    let needed = total
        - files
            .iter()
            .filter(|f| have.get(&f.filename) == Some(&f.size))
            .map(|f| f.size)
            .sum::<u64>();
    // Files are placed one by one on whichever drive has most room
    // (decision 0007): the pool must hold them all, and a drive the largest.
    let largest = files
        .iter()
        .filter(|f| have.get(&f.filename) != Some(&f.size))
        .map(|f| f.size)
        .max()
        .unwrap_or(0);
    // Merging into a day held takes room for both files on its volume.
    let details = lock(&jobs.details).get(job).cloned();
    let need = match details.and_then(|j| contract_parent(&j).map(|p| (j, p))) {
        Some((j, parent)) => merge_room(jobs, &j, &parent, &files).await?,
        None => HashMap::new(),
    };
    // Room net of what the downloads in flight hold (each might land on any
    // drive), checked and reserved as one step; until there is, it waits,
    // saying why.
    let until = Instant::now() + jobs.merge_wait;
    loop {
        let why = {
            let _one_at_a_time = jobs.room_check.lock().await;
            let (pool, one, each) = room(jobs).await?;
            if needed > pool {
                return Err(format!(
                    "needs {}, but the store has room for {} across its drives",
                    gb(needed),
                    gb(pool)
                ));
            }
            if largest > one {
                return Err(format!(
                    "its largest file is {}, but no drive has room for more than {}",
                    gb(largest),
                    gb(one)
                ));
            }
            let mut held = lock(&jobs.reserved);
            let flight: u64 = held.values().map(|r| r.0).sum();
            let merging: u64 = held.values().flat_map(|r| r.1.values()).sum();
            let free = |v: &String| {
                let m: u64 = held.values().filter_map(|r| r.1.get(v)).sum();
                each.get(v).copied().unwrap_or(0).saturating_sub(flight + m)
            };
            let mut short: Vec<String> = need
                .iter()
                .filter(|(v, n)| free(v) < **n)
                .map(|(v, n)| {
                    format!(
                        "{v} needs {} to merge these days, and has room for {}",
                        gb(*n),
                        gb(free(v))
                    )
                })
                .collect();
            if needed > pool.saturating_sub(flight + merging)
                || largest > each.keys().map(free).max().unwrap_or(0)
            {
                short.push("downloads in flight hold the room it needs".into());
            }
            if short.is_empty() {
                held.insert(job.to_owned(), (needed, need.clone()));
                break;
            }
            short.join("; ")
        };
        if Instant::now() >= until {
            return Err(why);
        }
        set(jobs, job, |t| t.state = format!("waiting: {why}"));
        tokio::time::sleep(Duration::from_millis(if cfg!(test) { 10 } else { 60_000 })).await;
    }
    set(jobs, job, |t| {
        t.state = "downloading".into();
        t.total_bytes = total;
    });
    let mut done = 0;
    for f in &files {
        set(jobs, job, |t| t.file = f.filename.clone());
        if have.get(&f.filename) != Some(&f.size) {
            // Each file of a scheduled download starts only while no capture
            // is live, and only if the capture hours don't begin before it
            // could finish. One asked for by hand doesn't wait (decision 0033).
            if !manual {
                loop {
                    let why = match live_capture(jobs).await {
                        Some(who) => Some(format!("waiting: {who} is live")),
                        None => capture_soon(jobs, f.size).map(|at| {
                            format!(
                                "waiting: capture starts at {at}, before this file could finish"
                            )
                        }),
                    };
                    let Some(why) = why else { break };
                    set(jobs, job, |t| t.state = why.clone());
                    tokio::time::sleep(Duration::from_secs(15)).await;
                }
            }
            set(jobs, job, |t| t.state = "downloading".into());
            let url = f
                .urls
                .get("https")
                .ok_or_else(|| format!("{}: no https URL", f.filename))?;
            let mut attempt = 0;
            loop {
                attempt += 1;
                match copy(jobs, job, &f.filename, url, f.size, &f.hash, done).await {
                    Ok(()) => {
                        if let Some(r) = lock(&jobs.reserved).get_mut(job) {
                            r.0 = r.0.saturating_sub(f.size);
                        }
                        break;
                    }
                    Err(e) if attempt < 3 => {
                        eprintln!("databento-jobs: {job}/{}: {e}; retrying", f.filename);
                        tokio::time::sleep(Duration::from_secs(5 * attempt)).await;
                    }
                    Err(e) => return Err(format!("{}: {e}", f.filename)),
                }
            }
        }
        done += f.size;
        set(jobs, job, |t| t.done_bytes = done);
    }
    // Databento's file list is the manifest (its download links aside).
    if !files.iter().any(|f| f.filename == "manifest.json") {
        let manifest = json!({
            "job_id": job,
            "files": files.iter().map(|f| json!({ "filename": f.filename, "size": f.size, "hash": f.hash })).collect::<Vec<_>>(),
        });
        let r = jobs
            .store_write(jobs.http.put(format!(
                "{}/incoming/databento/{job}/manifest.json",
                jobs.store
            )))
            .body(manifest.to_string())
            .send()
            .await
            .map_err(|e| format!("store: {e}"))?;
        if !r.status().is_success() {
            return Err(format!(
                "store refused the manifest: {}",
                r.text().await.unwrap_or_default()
            ));
        }
    }
    drop(turn);
    set(jobs, job, |t| {
        t.state = "filing".into();
        t.file.clear();
    });
    // A job bought as a list of one option parent's contracts is filed as
    // partial days of that parent (decision 0018).
    if filed(jobs, job).await {
        return Ok(0);
    }
    let parent = lock(&jobs.details).get(job).and_then(contract_parent);
    let url = match &parent {
        Some(p) => format!("{}/incorporate/databento/{job}?parent={p}", jobs.store),
        None => format!("{}/incorporate/databento/{job}", jobs.store),
    };
    // A busy store (its filing queue full, 503) is waited for, longer each time,
    // up to `store_wait`: it isn't a failed filing.
    let until = Instant::now() + jobs.store_wait;
    let mut wait = Duration::from_millis(if cfg!(test) { 10 } else { 5_000 });
    let r = loop {
        let r = jobs
            .store_write(jobs.http.post(&url))
            .send()
            .await
            .map_err(|e| format!("store: {e}"))?;
        if r.status() != StatusCode::SERVICE_UNAVAILABLE || Instant::now() + wait > until {
            break r;
        }
        tokio::time::sleep(wait).await;
        wait = (wait * 2).min(Duration::from_secs(60));
    };
    if !r.status().is_success() {
        let body: Value = r.json().await.unwrap_or_default();
        return Err(format!(
            "the store did not file it: {}",
            body["error"].as_str().unwrap_or("unknown error")
        ));
    }
    Ok(files.len())
}

/// What the store can take before its writable volumes reach the critical
/// threshold, in bytes: across them all, and on the roomiest one.
/// A read of the store for a download: while it rescans (it answers 503),
/// it waits, longer each time, up to `store_wait`.
async fn store_read(jobs: &Jobs, path: &str, limit: Duration) -> Result<reqwest::Response, String> {
    let until = Instant::now() + jobs.store_wait;
    let mut wait = Duration::from_millis(if cfg!(test) { 10 } else { 5_000 });
    let r = loop {
        let r = jobs
            .http
            .get(format!("{}{path}", jobs.store))
            .timeout(limit)
            .send()
            .await
            .map_err(|e| format!("store: {e}"))?;
        if r.status() != StatusCode::SERVICE_UNAVAILABLE || Instant::now() + wait > until {
            break r;
        }
        tokio::time::sleep(wait).await;
        wait = (wait * 2).min(Duration::from_secs(60));
    };
    if r.status().is_success() {
        Ok(r)
    } else {
        Err(format!("the store's {path}: HTTP {}", r.status()))
    }
}

/// What the store can take before its writable volumes reach the critical
/// threshold, in bytes: across them all, on the roomiest one, and on each
/// by name.
async fn room(jobs: &Jobs) -> Result<(u64, u64, HashMap<String, u64>), String> {
    let c: Value = store_read(jobs, "/catalog", Duration::from_secs(120))
        .await?
        .json()
        .await
        .map_err(|e| format!("store: {e}"))?;
    let critical = c["thresholds"]["critical"].as_f64().unwrap_or(90.0) / 100.0;
    let each: HashMap<String, u64> = c["roots"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|r| r["writable"] == true)
        .filter_map(|r| {
            let (total, free) = (
                r["volume"]["total_bytes"].as_f64()?,
                r["volume"]["available_bytes"].as_f64()?,
            );
            let name = r["name"].as_str().unwrap_or_default().to_owned();
            Some((name, (free - total * (1.0 - critical)).max(0.0) as u64))
        })
        .collect();
    let (pool, one) = each
        .values()
        .fold((0, 0), |(sum, max), r| (sum + r, max.max(*r)));
    Ok((pool, one, each))
}

/// The room merging a contract-list job's days into the days the store
/// holds of its parent takes, by volume: each such day's new file and the
/// file it is merged with, on each volume holding that day (`/coverage`).
async fn merge_room(
    jobs: &Jobs,
    j: &BatchJob,
    parent: &str,
    files: &[databento::historical::batch::BatchFileDesc],
) -> Result<HashMap<String, u64>, String> {
    // Databento names a day's file `<dataset>-<YYYYMMDD>.<schema>…`.
    let day = |name: &str| -> Option<String> {
        let d = name.split('.').next()?.rsplit('-').next()?;
        (d.len() == 8 && d.bytes().all(|b| b.is_ascii_digit()))
            .then(|| format!("{}-{}-{}", &d[..4], &d[4..6], &d[6..]))
    };
    let sizes: HashMap<String, u64> = files
        .iter()
        .filter_map(|f| Some((day(&f.filename)?, f.size)))
        .collect();
    let (Some(first), Some(last)) = (sizes.keys().min(), sizes.keys().max()) else {
        return Ok(HashMap::new());
    };
    let query = [
        ("provider", "databento"),
        ("dataset", j.dataset.as_str()),
        ("symbol", parent),
        ("schema", j.schema.as_str()),
        ("start", first.as_str()),
        ("end", last.as_str()),
    ];
    let path = reqwest::Url::parse_with_params("http://x/coverage", &query)
        .map(|u| format!("/coverage?{}", u.query().unwrap_or_default()))
        .map_err(|e| e.to_string())?;
    let coverage: Value = store_read(jobs, &path, Duration::from_secs(120))
        .await?
        .json()
        .await
        .map_err(|e| format!("store: {e}"))?;
    let mut need: HashMap<String, u64> = HashMap::new();
    for row in coverage["days"].as_array().into_iter().flatten() {
        let Some(new) = row["day"].as_str().and_then(|d| sizes.get(d)) else {
            continue;
        };
        for v in row["volumes"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
        {
            *need.entry(v.to_owned()).or_default() += new + row["size"].as_u64().unwrap_or(0);
        }
    }
    Ok(need)
}

/// The first capture service that is live, if any: its health says anything
/// but idle. One that doesn't answer isn't running, so isn't capturing; one
/// that answers with something unreadable might be, so counts as live.
async fn live_capture(jobs: &Jobs) -> Option<String> {
    for (name, url) in &jobs.yield_to {
        let Ok(r) = jobs
            .http
            .get(format!("{url}/health"))
            .timeout(Duration::from_secs(3))
            .send()
            .await
        else {
            continue;
        };
        let state = r
            .json::<Value>()
            .await
            .ok()
            .and_then(|v| v["state"].as_str().map(str::to_owned));
        if state.as_deref() != Some("idle") {
            return Some(name.clone());
        }
    }
    None
}

/// When the capture hours begin, if before a file of `size` bytes could be
/// downloaded (at a cautious 20 MB/s, and never less than 15 minutes).
fn capture_soon(jobs: &Jobs, size: u64) -> Option<String> {
    let hours = jobs.capture_hours?;
    let now = (tinytrader_observe::unix_seconds() * 1e9) as u64;
    let need = (size / 20_000_000).max(15 * 60) * 1_000_000_000;
    let (start, _) = hours.next(now, &lock(&jobs.calendar))?;
    (start > now && start - now < need).then(|| tinytrader_schedule::describe(start))
}

fn gb(bytes: u64) -> String {
    format!("{:.1} GB", bytes as f64 / 1e9)
}

/// Stream one file from Databento into the store, which checks its size and
/// hash before keeping it.
async fn copy(
    jobs: &Jobs,
    job: &str,
    name: &str,
    url: &str,
    size: u64,
    hash: &str,
    before: u64,
) -> Result<(), String> {
    let resp = jobs
        .http
        .get(url)
        .basic_auth(&jobs.key, None::<&str>)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    if !resp.status().is_success() {
        return Err(format!("Databento answered {}", resp.status()));
    }
    let progress = Arc::new(AtomicU64::new(0));
    let p = progress.clone();
    let body = resp.bytes_stream().map(move |c| {
        if let Ok(b) = &c {
            p.fetch_add(b.len() as u64, Ordering::Relaxed);
        }
        c
    });
    let sha = hash.trim_start_matches("sha256:");
    let put = jobs
        .store_write(jobs.http.put(format!(
            "{}/incoming/databento/{job}/{name}?size={size}&sha256={sha}",
            jobs.store
        )))
        .body(reqwest::Body::wrap_stream(body))
        .send();
    tokio::pin!(put);
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    let r = loop {
        tokio::select! {
            r = &mut put => break r.map_err(|e| format!("store: {e}"))?,
            _ = tick.tick() => set(jobs, job, |t| t.done_bytes = before + progress.load(Ordering::Relaxed)),
        }
    };
    if r.status().is_success() {
        Ok(())
    } else {
        let body: Value = r.json().await.unwrap_or_default();
        Err(format!(
            "the store refused it: {}",
            body["error"].as_str().unwrap_or("unknown error")
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(symbol: &str, start: &str, end: &str) -> PriceRequest {
        serde_json::from_value(json!({
            "dataset": "OPRA.PILLAR", "schema": "cmbp-1", "symbol": symbol, "start": start, "end": end,
        }))
        .unwrap()
    }

    #[test]
    fn the_last_day_is_inclusive() {
        let c = req("SPX.OPT", "2025-10-01", "2025-10-01").check().unwrap();
        assert_eq!(
            (c.start.to_string(), c.end.to_string()),
            ("2025-10-01".into(), "2025-10-02".into())
        );
        assert_eq!((c.stype, c.symbol.as_str()), (SType::Parent, "SPX.OPT"));
        assert_eq!(
            req("TSLA", "2025-10-01", "2025-10-01")
                .check()
                .unwrap()
                .stype,
            SType::RawSymbol
        );
    }

    #[test]
    fn refuses_requests_without_symbols_or_backwards() {
        assert!(req(" ", "2025-10-01", "2025-10-01").check().is_err());
        assert!(req("SPX.OPT", "2025-10-02", "2025-10-01").check().is_err());
        assert!(
            req("SPY.OPT,QQQ.OPT", "2025-10-01", "2025-10-01")
                .check()
                .is_err()
        );
        assert!(
            req("SPX.OPT", "2020-01-01", "2025-10-01").check().is_err(),
            "too long"
        );
        let with = |extra: Value| -> PriceRequest {
            let mut v = serde_json::to_value(json!({ "dataset": "OPRA.PILLAR", "schema": "cmbp-1", "symbol": "SPY.OPT", "start": "2025-10-01", "end": "2025-10-03" })).unwrap();
            for (k, x) in extra.as_object().unwrap() {
                v[k] = x.clone();
            }
            serde_json::from_value(v).unwrap()
        };
        assert!(with(json!({ "schema": "nope" })).check().is_err());
        assert!(
            with(json!({ "contracts": { "2025-10-04": [] } }))
                .check()
                .is_err(),
            "a day outside"
        );
        assert!(
            with(json!({ "contracts": {}, "rule": {} }))
                .check()
                .is_err(),
            "both"
        );
        assert!(
            with(json!({ "symbol": "TSLA", "rule": {} }))
                .check()
                .is_err(),
            "not an option parent"
        );
        assert!(
            with(json!({ "hours": { "start": "16:00", "end": "09:30" } }))
                .check()
                .is_err()
        );
        assert!(with(json!({ "contracts": { "2025-10-02": [] }, "hours": { "start": "09:30", "end": "16:01" } })).check().is_ok());
    }

    #[test]
    fn only_server_failures_are_priced_again() {
        assert!(retryable("API error: 503 Service Unavailable busy"));
        assert!(retryable("no answer from Databento in 60 s"));
        assert!(retryable("HTTP error: reqwest::Error { kind: Request }"));
        assert!(!retryable("API error: 400 Bad Request symbols"));
        assert!(!retryable("API error: 422 Unprocessable Entity"));
    }
}

#[cfg(test)]
mod price_check_tests {
    use super::*;

    #[test]
    fn only_an_exact_zero_is_free() {
        let no = || Some(refused(StatusCode::FORBIDDEN, "not approved"));
        assert!(check_price(0.0, 0.0, None, no()).is_none());
        assert!(check_price(-0.0, 0.0, None, no()).is_none());
        for c in [0.01, 1e-12] {
            assert_eq!(
                check_price(c, c, Some(c), no()).unwrap().0,
                StatusCode::FORBIDDEN,
                "{c}"
            );
            assert!(
                check_price(c, c, Some(c), None).is_none(),
                "{c} approved and confirmed"
            );
            assert_eq!(
                check_price(c, c, None, None).unwrap().0,
                StatusCode::PRECONDITION_REQUIRED
            );
        }
        for c in [-1.0, f64::NAN, f64::INFINITY] {
            assert_eq!(
                check_price(c, c, Some(c), None).unwrap().0,
                StatusCode::BAD_GATEWAY,
                "{c}"
            );
        }
    }
}
