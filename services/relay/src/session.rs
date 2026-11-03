//! Today's session for one option contract, replayed on request: `GET
//! /session?symbol=SPY261012C00777000&schema=cbbo-1m&start=<RFC 3339>`.
//!
//! Session, not history: this is the current trading day, held in memory
//! and gone with it; past days are history, filed in the store and read from
//! disk (fetching them from Databento is a purchase).
//!
//! The relay's standing session carries `cmbp-1`, which Databento won't
//! replay intraday; it does replay `cbbo-1s`, `cbbo-1m`, `tcbbo` and `trades`
//! (decision 0006, tested 2026-10-05), so those are what this serves. So each request opens a short second live session for
//! just that contract, subscribed from `start` (default: today's 09:30 New
//! York, or the last 24 hours before then; never more than 24 hours back),
//! reads until Databento says the replay is complete, and closes it. One
//! request runs at a time (sharing its turn with `/replay`, replay.rs), so
//! the relay never holds more than two of the
//! dataset's sessions, and answers are kept a minute.
//!
//! Only contracts on this relay's roots (`SPY.OPT` gives SPY) are served.
//! Nothing is recorded: the answer lives in memory for its minute.
//!
//! The answer is `{symbol, schema, start, fields, rows, note}`, `fields`
//! naming each row's columns:
//! - `cbbo-1m`, `cbbo-1s`: `[t, bid, ask, bid_size, ask_size, last]`, `t` the
//!   end of the interval (Unix seconds), `last` the session's last trade.
//! - `tcbbo`: `[t, price, size, side, bid, ask]`, each trade with the
//!   consolidated quote just before it, `t` in seconds with nanoseconds.
//! - `trades`: `[t, price, size, side]`.
//!
//! Prices are in dollars, `null` for an empty side or no trade yet; `side` is
//! `A` (sell aggressor), `B` (buy) or `N`. [`mid_bars`] turns `cbbo-1m` rows
//! into one-minute bars of the mid for `/bars`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use databento::LiveClient;
use databento::dbn::{
    CbboMsg, Cmbp1Msg, SType, Schema, SystemCode, SystemMsg, TradeMsg, UNDEF_PRICE,
    VersionUpgradePolicy,
};
use databento::live::Subscription;
use serde::Deserialize;
use serde_json::{Value, json};
use time::OffsetDateTime;
use tinytrader_schedule::{ny_date_time, ny_to_unix};

/// How long an answer is reused.
const KEEP: Duration = Duration::from_secs(60);
/// The longest a replay may take before it is given up.
const WAIT: Duration = Duration::from_secs(30);
/// Databento replays at most this far back.
pub const REPLAY_LIMIT: i64 = 24 * 3600;
/// Answers kept at once, at most.
const CACHED: usize = 64;

/// Answers by (contract, schema, start): when kept, and the answer.
type Cache = HashMap<(String, Schema, i64), (Instant, Value)>;

pub struct Session {
    key: String,
    dataset: String,
    /// Option roots this relay carries, such as `SPY`.
    roots: Vec<String>,
    cache: Mutex<Cache>,
    /// Held while a replay runs; shared with `/replay` (replay.rs), so the
    /// relay holds at most one session beside its own.
    one: Arc<tokio::sync::Semaphore>,
}

impl Session {
    /// For a Databento relay of option parents (`SPY.OPT`); `None` otherwise.
    #[must_use]
    pub fn new(
        key: &str,
        dataset: &str,
        symbols: &[String],
        one: Arc<tokio::sync::Semaphore>,
    ) -> Option<Arc<Self>> {
        let roots: Vec<String> = symbols
            .iter()
            .filter_map(|s| s.strip_suffix(".OPT").map(str::to_owned))
            .collect();
        (!roots.is_empty()).then(|| {
            Arc::new(Self {
                key: key.to_owned(),
                dataset: dataset.to_owned(),
                roots,
                cache: Mutex::default(),
                one,
            })
        })
    }

    /// Whether `symbol` is a contract this relay can replay.
    #[must_use]
    pub fn serves(&self, symbol: &str) -> bool {
        occ_root(symbol).is_some_and(|r| self.roots.iter().any(|x| x == r))
    }

    /// The contract's quotes from `start` (Unix seconds) to now.
    pub async fn quotes(&self, symbol: &str, schema: Schema, start: i64) -> Result<Value, Failed> {
        let key = (symbol.to_owned(), schema, start);
        if let Some((at, v)) = self.lock().get(&key)
            && at.elapsed() < KEEP
        {
            return Ok(v.clone());
        }
        // A replay to file (`/replay`) may hold the turn for many minutes.
        let _turn = tokio::time::timeout(WAIT, self.one.acquire())
            .await
            .map_err(|_| Failed::Busy)?
            .map_err(|e| Failed::Upstream(e.to_string()))?;
        // Another request may have filled it while this one waited.
        if let Some((at, v)) = self.lock().get(&key)
            && at.elapsed() < KEEP
        {
            return Ok(v.clone());
        }
        let (rows, complete) = tokio::time::timeout(WAIT, self.replay(symbol, schema, start))
            .await
            .map_err(|_| Failed::Upstream(format!("the replay took over {} s", WAIT.as_secs())))?
            .map_err(Failed::Upstream)?;
        let note = (!complete)
            .then(|| "Databento ended the session before the replay completed".to_owned());
        let v = json!({ "symbol": symbol, "schema": schema.to_string(), "start": start, "fields": fields(schema), "rows": rows, "note": note });
        let mut cache = self.lock();
        if cache.len() >= CACHED {
            cache.retain(|_, (at, _)| at.elapsed() < KEEP);
        }
        cache.insert(key, (Instant::now(), v.clone()));
        Ok(v)
    }

    async fn replay(
        &self,
        symbol: &str,
        schema: Schema,
        start: i64,
    ) -> Result<(Vec<Value>, bool), String> {
        let mut client = LiveClient::builder()
            .key(&self.key)
            .map_err(|e| format!("invalid API key: {e}"))?
            .dataset(&self.dataset)
            .upgrade_policy(VersionUpgradePolicy::UpgradeToV3)
            .build()
            .await
            .map_err(|e| format!("cannot connect to Databento: {e}"))?;
        let from = OffsetDateTime::from_unix_timestamp(start).map_err(|e| e.to_string())?;
        client
            .subscribe(
                Subscription::builder()
                    .symbols(opra_symbol(symbol))
                    .schema(schema)
                    .stype_in(SType::RawSymbol)
                    .start(from)
                    .build(),
            )
            .await
            .map_err(|e| format!("cannot subscribe: {e}"))?;
        client
            .start()
            .await
            .map_err(|e| format!("cannot start the replay: {e}"))?;
        let mut rows = Vec::new();
        loop {
            let Some(r) = client
                .next_record()
                .await
                .map_err(|e| format!("replay failed: {e}"))?
            else {
                return Ok((rows, false));
            };
            if let Some(m) = r.get::<SystemMsg>()
                && m.code().ok() == Some(SystemCode::ReplayCompleted)
            {
                return Ok((rows, true));
            }
            if let Some(q) = r.get::<CbboMsg>() {
                rows.push(row(q));
            } else if let Some(t) = r.get::<Cmbp1Msg>() {
                let l = &t.levels[0];
                rows.push(json!([
                    ts(t.hd.ts_event),
                    px(t.price),
                    t.size,
                    side(t.side),
                    px(l.bid_px),
                    px(l.ask_px)
                ]));
            } else if let Some(t) = r.get::<TradeMsg>() {
                rows.push(json!([
                    ts(t.hd.ts_event),
                    px(t.price),
                    t.size,
                    side(t.side)
                ]));
            }
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Cache> {
        self.cache.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Why a replay couldn't be had.
#[derive(Debug)]
pub enum Failed {
    /// Another replay holds the turn: a `/replay` file or another `/session`.
    Busy,
    /// Databento failed or took too long.
    Upstream(String),
}

/// Seconds a busy answer asks the client to wait.
pub const RETRY_AFTER: u64 = 30;

impl Failed {
    /// `503` when busy, `502` when Databento failed.
    #[must_use]
    pub fn status(&self) -> StatusCode {
        match self {
            Self::Busy => StatusCode::SERVICE_UNAVAILABLE,
            Self::Upstream(_) => StatusCode::BAD_GATEWAY,
        }
    }
}

impl std::fmt::Display for Failed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Busy => write!(
                f,
                "another replay is running (a /replay file or another /session); try again in {RETRY_AFTER} s"
            ),
            Self::Upstream(e) => f.write_str(e),
        }
    }
}

impl axum::response::IntoResponse for Failed {
    fn into_response(self) -> axum::response::Response {
        let body = Json(json!({ "error": self.to_string() }));
        match self {
            Self::Busy => (
                self.status(),
                [(axum::http::header::RETRY_AFTER, RETRY_AFTER.to_string())],
                body,
            )
                .into_response(),
            Self::Upstream(_) => (self.status(), body).into_response(),
        }
    }
}

/// Each schema's columns.
fn fields(schema: Schema) -> Value {
    match schema {
        Schema::Tcbbo => json!(["t", "price", "size", "side", "bid", "ask"]),
        Schema::Trades => json!(["t", "price", "size", "side"]),
        _ => json!(["t", "bid", "ask", "bid_size", "ask_size", "last"]),
    }
}

/// A price in dollars, or `None` when undefined.
fn px(p: i64) -> Option<f64> {
    (p != UNDEF_PRICE).then(|| p as f64 / 1e9)
}

/// Nanoseconds as seconds, keeping the nanoseconds.
fn ts(ns: u64) -> f64 {
    ns as f64 / 1e9
}

fn side(c: std::ffi::c_char) -> String {
    char::from(c as u8).to_string()
}

/// `[t, bid, ask, bid_size, ask_size, last]` from a CBBO record.
fn row(q: &CbboMsg) -> Value {
    let l = &q.levels[0];
    json!([
        q.ts_recv / 1_000_000_000,
        px(l.bid_px),
        px(l.ask_px),
        l.bid_sz,
        l.ask_sz,
        px(q.price)
    ])
}

/// One-minute bars `[t, o, h, l, c, 0]` of the mid from `cbbo-1m` quotes:
/// each minute's bar starts at its interval's start, closes at the mid
/// then, and opens at the previous minute's mid. Minutes without both sides
/// are left out.
#[must_use]
pub fn mid_bars(quotes: &[Value]) -> Vec<Value> {
    let mut out = Vec::new();
    let mut prev: Option<f64> = None;
    for q in quotes {
        let (Some(t), Some(b), Some(a)) = (q[0].as_i64(), q[1].as_f64(), q[2].as_f64()) else {
            continue;
        };
        if b <= 0.0 || a < b {
            continue;
        }
        let c = (a + b) / 2.0;
        let o = prev.unwrap_or(c);
        out.push(json!([t - 60, o, o.max(c), o.min(c), c, 0]));
        prev = Some(c);
    }
    out
}

/// The root of a compact OCC symbol (`SPY261012C00777000` gives `SPY`).
#[must_use]
pub fn occ_root(s: &str) -> Option<&str> {
    let n = s.len();
    if !(16..=21).contains(&n) || !s.is_ascii() {
        return None;
    }
    let (root, rest) = s.split_at(n - 15);
    let ok = root.bytes().all(|b| b.is_ascii_uppercase())
        && rest[..6].bytes().all(|b| b.is_ascii_digit())
        && matches!(rest.as_bytes()[6], b'C' | b'P')
        && rest[7..].bytes().all(|b| b.is_ascii_digit());
    ok.then_some(root)
}

/// OPRA's raw symbol: the root padded to six characters.
fn opra_symbol(occ: &str) -> String {
    let (root, rest) = occ.split_at(occ.len() - 15);
    format!("{root:<6}{rest}")
}

/// Today's 09:30 in New York if it has passed, else the last 24 hours;
/// never more than 24 hours back.
#[must_use]
pub fn default_start(now: i64) -> i64 {
    let (date, _, _) = ny_date_time(now);
    let p = |a: usize, b: usize| date[a..b].parse::<i64>().unwrap_or(0);
    let open = ny_to_unix(p(0, 4), p(5, 7), p(8, 10), 9, 30);
    if open <= now {
        open
    } else {
        now - REPLAY_LIMIT + 60
    }
}

/// A schema Databento replays intraday for OPRA (default `cbbo-1m`).
pub fn replay_schema(s: Option<&str>) -> Result<Schema, String> {
    match s.unwrap_or("cbbo-1m") {
        "cbbo-1m" => Ok(Schema::Cbbo1M),
        "cbbo-1s" => Ok(Schema::Cbbo1S),
        "tcbbo" => Ok(Schema::Tcbbo),
        "trades" => Ok(Schema::Trades),
        s => Err(format!("schema {s}: cbbo-1m, cbbo-1s, tcbbo or trades")),
    }
}

/// A replay's start (Unix seconds) from an RFC 3339 time, [`default_start`]
/// when absent; it must be within the last 24 hours (Databento's limit).
pub fn replay_start(s: Option<&str>, now: i64) -> Result<i64, String> {
    let start = match s {
        None => default_start(now),
        Some(s) => OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
            .map_err(|e| format!("start: RFC 3339 ({e})"))?
            .unix_timestamp(),
    };
    if start < now - REPLAY_LIMIT || start >= now {
        return Err("start: within the last 24 hours".to_owned());
    }
    Ok(start)
}

#[derive(Deserialize)]
struct Ask {
    symbol: String,
    schema: Option<String>,
    start: Option<String>,
}

/// `/session?symbol=OCC&schema=cbbo-1m|cbbo-1s|tcbbo|trades&start=<RFC 3339>`.
pub fn router(session: Arc<Session>) -> Router {
    Router::new()
        .route("/session", get(serve))
        .with_state(session)
}

async fn serve(State(h): State<Arc<Session>>, Query(ask): Query<Ask>) -> axum::response::Response {
    use axum::response::IntoResponse as _;
    let refuse = |code, e: String| (code, Json(json!({ "error": e }))).into_response();
    if !h.serves(&ask.symbol) {
        return refuse(
            StatusCode::NOT_FOUND,
            format!(
                "{}: not an option on {} (compact OCC, such as {}261016C00575000)",
                ask.symbol,
                h.roots.join(", "),
                h.roots[0]
            ),
        );
    }
    let schema = match replay_schema(ask.schema.as_deref()) {
        Ok(s) => s,
        Err(e) => return refuse(StatusCode::BAD_REQUEST, e),
    };
    let now = tinytrader_observe::unix_seconds() as i64;
    let start = match replay_start(ask.start.as_deref(), now) {
        Ok(s) => s,
        Err(e) => return refuse(StatusCode::BAD_REQUEST, e),
    };
    match h.quotes(&ask.symbol, schema, start).await {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => e.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn symbols() {
        assert_eq!(occ_root("SPY261012C00777000"), Some("SPY"));
        assert_eq!(occ_root("SPXW261012P05742500"), Some("SPXW"));
        assert_eq!(occ_root("SPY"), None);
        assert_eq!(occ_root("SPY261012X00777000"), None);
        assert_eq!(opra_symbol("SPY261012C00777000"), "SPY   261012C00777000");
        let h = Session::new(
            "k",
            "OPRA.PILLAR",
            &["SPY.OPT".into()],
            Arc::new(tokio::sync::Semaphore::new(1)),
        )
        .unwrap();
        assert!(h.serves("SPY261012C00777000"));
        assert!(!h.serves("QQQ261012C00500000"));
        assert!(
            Session::new(
                "k",
                "XNAS.ITCH",
                &["SPY".into()],
                Arc::new(tokio::sync::Semaphore::new(1))
            )
            .is_none()
        );
    }

    #[test]
    fn starts_at_the_open_or_the_last_day() {
        let open = ny_to_unix(2026, 10, 12, 9, 30);
        assert_eq!(default_start(open + 3600), open);
        // Before the open: the last 24 hours.
        let early = ny_to_unix(2026, 10, 12, 8, 0);
        assert_eq!(default_start(early), early - REPLAY_LIMIT + 60);
    }

    #[test]
    fn busy_is_503_with_retry_after() {
        use axum::response::IntoResponse as _;
        let r = Failed::Busy.into_response();
        assert_eq!(r.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(r.headers()["retry-after"], "30");
        assert_eq!(
            Failed::Upstream("x".into()).into_response().status(),
            StatusCode::BAD_GATEWAY
        );
    }

    #[test]
    fn starts_within_the_last_day() {
        let now = ny_to_unix(2026, 10, 12, 12, 0);
        let open = ny_to_unix(2026, 10, 12, 9, 30);
        assert_eq!(replay_start(None, now), Ok(open));
        assert_eq!(replay_start(Some("2026-10-12T13:30:00Z"), now), Ok(open));
        let day_ago = |d: i64| {
            OffsetDateTime::from_unix_timestamp(now - d)
                .unwrap()
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap()
        };
        assert!(replay_start(Some(&day_ago(REPLAY_LIMIT - 1)), now).is_ok());
        assert!(replay_start(Some(&day_ago(REPLAY_LIMIT + 1)), now).is_err());
        assert!(replay_start(Some(&day_ago(-60)), now).is_err());
        assert!(replay_start(Some("yesterday"), now).is_err());
        assert_eq!(replay_schema(Some("tcbbo")), Ok(Schema::Tcbbo));
        assert!(replay_schema(Some("cmbp-1")).is_err());
    }

    #[test]
    fn mid_bars_open_at_the_last_mid() {
        let q = |t: i64, b: Option<f64>, a: Option<f64>| json!([t, b, a, 1, 1, null]);
        let bars = mid_bars(&[
            q(120, Some(1.0), Some(1.2)),
            q(180, None, Some(1.3)),
            q(240, Some(0.9), Some(0.95)),
        ]);
        assert_eq!(bars.len(), 2);
        assert_eq!(bars[0], json!([60, 1.1, 1.1, 1.1, 1.1, 0]));
        // Minute ending 240 starts at 180, opens at 1.1, closes at 0.925.
        assert_eq!(bars[1], json!([180, 1.1, 1.1, 0.925, 0.925, 0]));
    }
}

/// Against Databento itself: `DATABENTO_API_KEY=… cargo test -p relay -- --ignored live_replay`
/// (`REPLAY_SCHEMA=trades` for another schema).
#[cfg(test)]
mod live {
    use super::*;

    #[tokio::test]
    #[ignore = "needs DATABENTO_API_KEY and a session today"]
    async fn live_replay() {
        let key = std::env::var("DATABENTO_API_KEY").expect("DATABENTO_API_KEY");
        let sym = std::env::var("REPLAY_SYMBOL").unwrap_or_else(|_| "SPY261012C00777000".into());
        let h = Session::new(
            &key,
            "OPRA.PILLAR",
            &["SPY.OPT".into()],
            Arc::new(tokio::sync::Semaphore::new(1)),
        )
        .unwrap();
        let now = tinytrader_observe::unix_seconds() as i64;
        let schema = std::env::var("REPLAY_SCHEMA").map_or(Schema::Cbbo1M, |s| s.parse().unwrap());
        let v = h.quotes(&sym, schema, default_start(now)).await.unwrap();
        let q = v["rows"].as_array().unwrap();
        eprintln!(
            "{} rows, note {:?}; first {} last {}",
            q.len(),
            v["note"],
            q.first().unwrap(),
            q.last().unwrap()
        );
        if matches!(schema, Schema::Cbbo1M) {
            let bars = mid_bars(q);
            eprintln!("{} mid bars; last {}", bars.len(), bars.last().unwrap());
        }
        assert!(!q.is_empty());
    }
}
