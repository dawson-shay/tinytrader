//! Asking Alpaca: one shared pace for every request, and a day's page chain
//! for one lane (trades or quotes), each page spooled with its receipt as it
//! arrives ([`crate::spool`]).
//!
//! The pace (decision 0014) stays below both the configured rate and what
//! Alpaca advertises, waits out the window when `x-ratelimit-remaining`
//! reaches a small reserve, and honours `Retry-After`; every day and lane in
//! flight shares it. A 429 is never success: it pauses the pace. Server errors
//! and dropped connections are retried with a doubling backoff. A lane that
//! is still rate limited or unavailable after that waits, longer each time,
//! and asks for the same page again: the pages it has are kept, never fetched
//! again. A 400 for a page token (a token Alpaca no longer accepts) starts
//! that lane again from its first page, at most twice. Anything else Alpaca
//! refuses is the day's failure.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tinytrader_observe::Events;
use tokio::sync::Mutex;

use crate::spool::{LaneSpool, LaneState, Parse};

/// Requests left in Alpaca's window below which the pace waits for the reset.
const RESERVE: u64 = 6;
const RETRIES: u32 = 5;
/// How often a lane waits out a rate limit or an outage before the day is
/// left for the next pass (its pages kept), and how long the first and
/// longest waits are.
const LANE_WAITS: u32 = 8;
const LANE_WAIT: Duration = if cfg!(test) {
    Duration::from_millis(5)
} else {
    Duration::from_secs(15)
};
const LANE_WAIT_MAX: Duration = if cfg!(test) {
    Duration::from_millis(20)
} else {
    Duration::from_secs(300)
};
/// Times a lane is started again because Alpaca refused its page token.
const RESTARTS: u32 = 2;

/// Why a lane stopped.
#[derive(Debug)]
pub enum Failure {
    /// 429s in a row, and still after waiting: try again later.
    RateLimited,
    /// Alpaca refused the request itself (401, 403, 404, a bad request).
    Refused(String),
    /// A 400: refused, or, for a page token, a lane to start again.
    BadRequest(String),
    /// A page didn't make sense (unreadable, out of order, outside the day).
    PageIntegrity(String),
    /// The network or Alpaca kept failing, and still after waiting.
    Unavailable(String),
    /// Asked to hold (live capture, or the service stopping): its pages kept.
    Paused,
    /// The spool couldn't be written or read.
    Spool(String),
}

impl Failure {
    /// Whether the day waits for a later pass (its spool kept) rather than failing.
    pub fn transient(&self) -> bool {
        matches!(
            self,
            Self::RateLimited | Self::Unavailable(_) | Self::Paused | Self::Spool(_)
        )
    }
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RateLimited => write!(f, "rate limited (429s in a row)"),
            Self::Refused(e) | Self::BadRequest(e) => write!(f, "refused: {e}"),
            Self::PageIntegrity(e) => write!(f, "page integrity: {e}"),
            Self::Unavailable(e) => write!(f, "unavailable: {e}"),
            Self::Paused => write!(f, "paused"),
            Self::Spool(e) => write!(f, "spool: {e}"),
        }
    }
}

/// One pace for every request the service makes.
pub struct Pace {
    gap: Duration,
    state: Mutex<PaceState>,
    /// Requests made, and 429s seen, for `/metrics`.
    pub requests: Arc<AtomicU64>,
    pub limited: Arc<AtomicU64>,
}

struct PaceState {
    next: Instant,
    in_a_row: u32,
}

impl Pace {
    pub fn new(per_minute: u32) -> Self {
        Self {
            gap: Duration::from_secs_f64(60.0 / f64::from(per_minute.max(1))),
            state: Mutex::new(PaceState {
                next: Instant::now(),
                in_a_row: 0,
            }),
            requests: Arc::default(),
            limited: Arc::default(),
        }
    }

    async fn turn(&self) {
        let wait = {
            let mut s = self.state.lock().await;
            let now = Instant::now();
            let at = s.next.max(now);
            s.next = at + self.gap;
            self.requests.fetch_add(1, Ordering::Relaxed);
            at - now
        };
        tokio::time::sleep(wait).await;
    }

    /// After an answer: wait out the window when it's nearly spent.
    async fn heard(&self, resp: &reqwest::Response) {
        let header = |n: &str| {
            resp.headers()
                .get(n)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.trim().parse::<u64>().ok())
        };
        let mut s = self.state.lock().await;
        if resp.status() != reqwest::StatusCode::TOO_MANY_REQUESTS {
            s.in_a_row = 0;
        }
        if let (Some(left), Some(reset)) =
            (header("x-ratelimit-remaining"), header("x-ratelimit-reset"))
            && left <= RESERVE
        {
            let now = tinytrader_observe::unix_seconds();
            let secs = (reset as f64 - now).clamp(0.0, 60.0);
            s.next = s
                .next
                .max(Instant::now() + Duration::from_secs_f64(secs + 0.5));
        }
    }

    /// A 429: pause; the third in a row stops.
    async fn limited(&self, retry_after: Option<u64>) -> Result<(), Failure> {
        let mut s = self.state.lock().await;
        self.limited.fetch_add(1, Ordering::Relaxed);
        s.in_a_row += 1;
        let pause = if cfg!(test) {
            Duration::from_millis(2)
        } else {
            Duration::from_secs(retry_after.unwrap_or(5).clamp(1, 60))
        };
        s.next = s.next.max(Instant::now() + pause);
        if s.in_a_row >= 3 {
            s.in_a_row = 0;
            return Err(Failure::RateLimited);
        }
        Ok(())
    }
}

/// How to reach Alpaca.
pub struct Alpaca {
    pub http: reqwest::Client,
    pub data: String,
    pub key_id: String,
    pub secret: String,
    pub pace: Pace,
    /// Set to stop every lane at its next page (live capture began): their
    /// pages are kept.
    pub hold: AtomicBool,
    /// Where waits and restarts are told.
    pub events: Events,
}

impl Alpaca {
    /// One GET, paced and retried: the body as received.
    pub async fn get(&self, url: &str) -> Result<Vec<u8>, Failure> {
        let mut backoff = Duration::from_millis(if cfg!(test) { 2 } else { 500 });
        let mut last = String::new();
        for _ in 0..RETRIES {
            self.pace.turn().await;
            let sent = self
                .http
                .get(url)
                .header("APCA-API-KEY-ID", &self.key_id)
                .header("APCA-API-SECRET-KEY", &self.secret)
                .timeout(Duration::from_secs(60))
                .send()
                .await;
            match sent {
                Ok(resp) => {
                    self.pace.heard(&resp).await;
                    let status = resp.status();
                    if status.is_success() {
                        match resp.bytes().await {
                            Ok(b) => return Ok(b.to_vec()),
                            Err(e) => last = format!("reading the answer: {e}"),
                        }
                    } else if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                        let after = resp
                            .headers()
                            .get("retry-after")
                            .and_then(|v| v.to_str().ok())
                            .and_then(|v| v.parse().ok());
                        self.pace.limited(after).await?;
                        continue;
                    } else if status.is_server_error() {
                        last = format!("Alpaca answered {status}");
                    } else if status == reqwest::StatusCode::BAD_REQUEST {
                        let body = resp.text().await.unwrap_or_default();
                        return Err(Failure::BadRequest(format!(
                            "{status}: {}",
                            body.chars().take(200).collect::<String>()
                        )));
                    } else {
                        let body = resp.text().await.unwrap_or_default();
                        return Err(Failure::Refused(format!(
                            "{status}: {}",
                            body.chars().take(200).collect::<String>()
                        )));
                    }
                }
                Err(e) => last = e.to_string(),
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(30));
        }
        Err(Failure::Unavailable(last))
    }
}

/// A lane fetched to its last page: every page's receipt, kept in its spool.
#[derive(Debug, Default)]
pub struct LaneDone {
    pub receipts: Vec<Value>,
    pub rows: usize,
    pub first: Option<u64>,
    pub last: Option<u64>,
}

impl LaneDone {
    fn of(receipts: Vec<Value>) -> Self {
        let rows = receipts
            .iter()
            .filter_map(|p| p["rows"].as_u64())
            .sum::<u64>() as usize;
        let first = receipts.iter().filter_map(|p| p["first_ns"].as_u64()).min();
        let last = receipts.iter().filter_map(|p| p["last_ns"].as_u64()).max();
        Self {
            receipts,
            rows,
            first,
            last,
        }
    }
}

/// `YYYY-MM-DD` → nanoseconds at 00:00 UTC.
pub fn day_ns(day: &str) -> Option<u64> {
    let d = time::Date::parse(
        day,
        time::macros::format_description!("[year]-[month]-[day]"),
    )
    .ok()?;
    let s = d.midnight().assume_utc().unix_timestamp();
    u64::try_from(s).ok().map(|s| s * 1_000_000_000)
}

/// A page's URL: the lane's, with its page token percent-encoded (every byte
/// but the unreserved ones), so a token with `+`, `/` or `=` arrives as sent.
pub fn page_url(base: &str, token: Option<&str>) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut url = base.to_owned();
    if let Some(token) = token {
        url.push_str("&page_token=");
        for b in token.bytes() {
            if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
                url.push(char::from(b));
            } else {
                url.push('%');
                url.push(char::from(HEX[usize::from(b >> 4)]));
                url.push(char::from(HEX[usize::from(b & 15)]));
            }
        }
    }
    url
}

/// What a lane is.
pub struct LaneSpec<'a> {
    pub feed: &'a str,
    pub symbol: &'a str,
    pub kind: &'a str,
    pub day: &'a str,
}

/// Check a page and commit it to the spool: its rows in time order, after the
/// lane's last and within the day, and a usable next token. Returns the state
/// after it.
#[allow(clippy::too_many_arguments)]
fn take_page<T>(
    spool: &LaneSpool,
    st: &LaneState,
    kind: &str,
    day: &str,
    end: u64,
    body: &[u8],
    parse: Parse<T>,
    ts: fn(&T) -> u64,
) -> Result<(LaneState, Value), Failure> {
    let bad = |e: String| Failure::PageIntegrity(format!("{kind} page {}: {e}", st.pages));
    let (rows, next) = parse(body).map_err(bad)?;
    let (first, last) = (rows.first().map(ts), rows.last().map(ts));
    let mut prev = st.prev_ns;
    for r in &rows {
        let t = ts(r);
        if t < prev || t >= end {
            return Err(bad(format!(
                "a row at {t} is out of order or outside {day}"
            )));
        }
        prev = t;
    }
    if let Some(t) = &next
        && (t.is_empty() || t.len() > 256 || !t.bytes().all(|b| b.is_ascii_graphic()))
    {
        return Err(Failure::PageIntegrity(format!(
            "{kind}: an unusable page token"
        )));
    }
    // The same token again would never end.
    if next.is_some() && next == st.next_page_token {
        return Err(Failure::PageIntegrity(format!(
            "{kind}: the same page token twice"
        )));
    }
    let receipt = json!({
        "page": st.pages, "rows": rows.len(), "first_ns": first, "last_ns": last,
        "bytes": body.len(), "sha256": format!("{:x}", Sha256::digest(body)), "continues": next.is_some(),
    });
    let after = LaneState {
        pages: st.pages + 1,
        rows: st.rows + rows.len(),
        done: next.is_none(),
        next_page_token: next,
        prev_ns: prev,
    };
    spool
        .commit(st.pages, body, &receipt, &after)
        .map_err(Failure::Spool)?;
    Ok((after, receipt))
}

async fn blocking<R: Send + 'static>(
    f: impl FnOnce() -> Result<R, Failure> + Send + 'static,
) -> Result<R, Failure> {
    tokio::task::spawn_blocking(f)
        .await
        .unwrap_or_else(|e| Err(Failure::Spool(format!("spool task: {e}"))))
}

/// Fetch one lane of one symbol-day into `dir`: every page from where the
/// spool left off to the one without a next token. `parse` turns a page's
/// body into rows and its next token; rows must stay in time order and within
/// the day. Only one page is in memory at a time.
pub async fn lane<T: 'static>(
    alpaca: &Alpaca,
    spec: &LaneSpec<'_>,
    dir: &Path,
    parse: Parse<T>,
    ts: fn(&T) -> u64,
) -> Result<LaneDone, Failure> {
    let LaneSpec {
        feed,
        symbol,
        kind,
        day,
    } = *spec;
    let start = day_ns(day).ok_or_else(|| Failure::PageIntegrity(format!("bad day {day}")))?;
    let end = start + 86_400 * 1_000_000_000;
    let base = format!(
        "{}/v2/stocks/{symbol}/{kind}?start={day}T00:00:00Z&end={day}T23:59:59.999999999Z&limit=10000&sort=asc&feed={feed}",
        alpaca.data
    );
    let spool = LaneSpool::at(dir.to_path_buf());
    let (mut st, mut receipts) = {
        let s = spool.clone();
        blocking(move || s.load().map_err(Failure::Spool)).await?
    };
    if st.pages == 0 {
        st.prev_ns = start;
    } else if !st.done {
        alpaca.events.push(
            format!("resuming {symbol} {day} {kind}"),
            format!(
                "from page {}, the {} pages spooled kept",
                st.pages, st.pages
            ),
        );
    }
    let (mut waits, mut wait, mut restarts) = (0, LANE_WAIT, 0);
    while !st.done {
        if alpaca.hold.load(Ordering::Relaxed) {
            return Err(Failure::Paused);
        }
        match alpaca
            .get(&page_url(&base, st.next_page_token.as_deref()))
            .await
        {
            Ok(body) => {
                (waits, wait) = (0, LANE_WAIT);
                let (s, d, now) = (spool.clone(), day.to_owned(), st.clone());
                let k = kind.to_owned();
                let (after, receipt) =
                    blocking(move || take_page(&s, &now, &k, &d, end, &body, parse, ts)).await?;
                st = after;
                receipts.push(receipt);
            }
            Err(e @ (Failure::RateLimited | Failure::Unavailable(_))) => {
                if waits >= LANE_WAITS {
                    return Err(e);
                }
                alpaca.events.push(
                    format!("waiting {symbol} {day} {kind}"),
                    format!(
                        "page {}: {e}; asking again in {wait:?}, the {} pages fetched kept",
                        st.pages, st.pages
                    ),
                );
                tokio::time::sleep(wait).await;
                waits += 1;
                wait = (wait * 2).min(LANE_WAIT_MAX);
            }
            Err(Failure::BadRequest(e)) if st.next_page_token.is_some() && restarts < RESTARTS => {
                restarts += 1;
                alpaca.events.push(
                    format!("restarting {symbol} {day} {kind}"),
                    format!("Alpaca refused the page token for page {} ({e}); the lane starts again from its first page", st.pages),
                );
                let s = spool.clone();
                blocking(move || s.reset().map_err(Failure::Spool)).await?;
                st = LaneState {
                    prev_ns: start,
                    ..LaneState::default()
                };
                receipts.clear();
            }
            Err(e) => return Err(e),
        }
    }
    Ok(LaneDone::of(receipts))
}

#[cfg(test)]
mod tests {
    use super::page_url;

    #[test]
    fn page_tokens_are_percent_encoded() {
        assert_eq!(page_url("u?a=1", None), "u?a=1");
        assert_eq!(
            page_url("u?a=1", Some("abc+/==&feed=iex#%?")),
            "u?a=1&page_token=abc%2B%2F%3D%3D%26feed%3Diex%23%25%3F"
        );
        assert_eq!(
            page_url("u?a=1", Some("A-z_0.9~")),
            "u?a=1&page_token=A-z_0.9~"
        );
    }
}
