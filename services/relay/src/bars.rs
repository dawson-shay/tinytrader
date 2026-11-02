//! One-minute OHLCV bars of the relay's own symbols (such as SPY and QQQ on
//! the IEX relay), built from the trades it relays and served at
//! `/bars?symbol=SPY&days=2` for the console's candlestick widget. A source
//! with bar history (Alpaca's) fills in the minutes before the relay saw them;
//! its bars stand for every minute they cover, and the relay's own for the
//! minutes after.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::source::Source;

/// One minute: its start (Unix seconds), open, high, low, close and volume.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Bar {
    pub t: u64,
    pub o: f64,
    pub h: f64,
    pub l: f64,
    pub c: f64,
    pub v: u64,
}

impl Bar {
    fn json(&self) -> Value {
        json!([self.t, self.o, self.h, self.l, self.c, self.v])
    }
}

/// Minutes kept per symbol from the live feed: more than a long session.
const KEEP: usize = 1200;
/// How long fetched history is reused before asking again.
const HISTORY_FOR: Duration = Duration::from_secs(60);
/// The most days of history served.
const MAX_DAYS: u64 = 10;

#[derive(Default)]
pub struct Bars {
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    symbols: Vec<String>,
    live: HashMap<String, BTreeMap<u64, Bar>>,
    history: HashMap<String, (Instant, u64, Vec<Bar>)>,
}

impl Bars {
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The symbols bars are kept for.
    pub fn keep(&self, symbols: &[String]) {
        self.lock().symbols = symbols.to_vec();
    }

    /// Add a trade at `ts` (nanoseconds since the epoch).
    pub fn trade(&self, symbol: &str, ts: u64, price: f64, size: u64) {
        let mut inner = self.lock();
        if !inner.symbols.iter().any(|s| s == symbol) || !price.is_finite() {
            return;
        }
        let t = ts / 1_000_000_000 / 60 * 60;
        if !inner.live.contains_key(symbol) {
            inner.live.insert(symbol.to_owned(), Default::default());
        }
        let Some(minutes) = inner.live.get_mut(symbol) else {
            return;
        };
        minutes
            .entry(t)
            .and_modify(|b| {
                b.h = b.h.max(price);
                b.l = b.l.min(price);
                b.c = price;
                b.v += size;
            })
            .or_insert(Bar {
                t,
                o: price,
                h: price,
                l: price,
                c: price,
                v: size,
            });
        while minutes.len() > KEEP {
            minutes.pop_first();
        }
    }

    fn live(&self, symbol: &str) -> Vec<Bar> {
        self.lock()
            .live
            .get(symbol)
            .map(|m| m.values().copied().collect())
            .unwrap_or_default()
    }
}

/// History's bars for every minute they cover, then the live bars after them.
#[must_use]
pub fn merge(history: &[Bar], live: &[Bar]) -> Vec<Bar> {
    let last = history.last().map_or(0, |b| b.t);
    let mut out = history.to_vec();
    out.extend(live.iter().filter(|b| b.t > last));
    out
}

#[derive(Deserialize)]
struct Ask {
    symbol: String,
    days: Option<u64>,
}

struct Ctx {
    bars: Arc<Bars>,
    source: Arc<dyn Source>,
    session: Option<Arc<crate::session::Session>>,
}

/// `/bars?symbol=SPY&days=2`; on an option relay, also `/bars?symbol=OCC`:
/// today's one-minute bars of that contract's mid, from Databento's intraday
/// replay (`session.rs`).
pub fn router(
    bars: Arc<Bars>,
    source: Arc<dyn Source>,
    session: Option<Arc<crate::session::Session>>,
) -> Router {
    Router::new()
        .route("/bars", get(serve))
        .with_state(Arc::new(Ctx {
            bars,
            source,
            session,
        }))
}

async fn serve(State(ctx): State<Arc<Ctx>>, Query(ask): Query<Ask>) -> (StatusCode, Json<Value>) {
    if let Some(h) = ctx.session.as_ref().filter(|h| h.serves(&ask.symbol)) {
        let now = tinytrader_observe::unix_seconds() as i64;
        let start = crate::session::default_start(now);
        return match h
            .quotes(&ask.symbol, databento::dbn::Schema::Cbbo1M, start)
            .await
        {
            Ok(v) => {
                let bars =
                    crate::session::mid_bars(v["rows"].as_array().map_or(&[][..], Vec::as_slice));
                let note = v["note"].as_str().map_or_else(|| "today's mid, minute by minute, from Databento's OPRA consolidated quotes (cbbo-1m)".to_owned(), str::to_owned);
                (
                    StatusCode::OK,
                    Json(
                        json!({ "symbol": ask.symbol, "interval": 60, "bars": bars, "note": note }),
                    ),
                )
            }
            Err(e) => (e.status(), Json(json!({ "error": e.to_string() }))),
        };
    }
    if !ctx.bars.lock().symbols.contains(&ask.symbol) {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("this relay has no bars for {}", ask.symbol) })),
        );
    }
    let days = ask.days.unwrap_or(1).clamp(1, MAX_DAYS);
    let mut note = None;
    // Only a source with bar history ever fills the cache.
    let cached = ctx
        .bars
        .lock()
        .history
        .get(&ask.symbol)
        .filter(|(at, d, _)| at.elapsed() < HISTORY_FOR && *d >= days)
        .map(|(_, _, b)| b.clone());
    let now = tinytrader_observe::unix_seconds() as u64;
    let history = match cached {
        Some(b) => b,
        None => match ctx.source.bars(
            &ask.symbol,
            (now - days * 86_400) * 1_000_000_000,
            now * 1_000_000_000,
        ) {
            None => Vec::new(),
            Some(f) => match f.await {
                Ok(b) => {
                    ctx.bars
                        .lock()
                        .history
                        .insert(ask.symbol.clone(), (Instant::now(), days, b.clone()));
                    b
                }
                Err(e) => {
                    note = Some(format!("history unavailable: {e}"));
                    Vec::new()
                }
            },
        },
    };
    let since = tinytrader_observe::unix_seconds() as u64 - days * 86_400;
    let bars: Vec<Value> = merge(&history, &ctx.bars.live(&ask.symbol))
        .iter()
        .filter(|b| b.t >= since)
        .map(Bar::json)
        .collect();
    (
        StatusCode::OK,
        Json(json!({ "symbol": ask.symbol, "interval": 60, "bars": bars, "note": note })),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIN: u64 = 60_000_000_000;

    #[test]
    fn trades_make_minute_bars_for_kept_symbols_only() {
        let b = Bars::default();
        b.keep(&["SPY".into()]);
        let t0 = 1_791_300_000 * 1_000_000_000 / MIN * MIN;
        b.trade("SPY", t0 + 1, 100.0, 10);
        b.trade("SPY", t0 + 2, 101.5, 5);
        b.trade("SPY", t0 + 3, 99.0, 1);
        b.trade("SPY", t0 + 4, 100.5, 2);
        b.trade("SPY", t0 + MIN, 102.0, 3);
        b.trade("QQQ", t0, 50.0, 1);
        let bars = b.live("SPY");
        assert_eq!(bars.len(), 2);
        assert_eq!(
            bars[0],
            Bar {
                t: t0 / 1_000_000_000,
                o: 100.0,
                h: 101.5,
                l: 99.0,
                c: 100.5,
                v: 18
            }
        );
        assert_eq!(bars[1].o, 102.0);
        assert!(b.live("QQQ").is_empty());
    }

    #[test]
    fn history_covers_its_minutes_and_live_bars_follow() {
        let bar = |t, c| Bar {
            t,
            o: c,
            h: c,
            l: c,
            c,
            v: 1,
        };
        let history = [bar(60, 1.0), bar(120, 2.0)];
        // The relay saw the end of minute 120 only; history's bar stands for it.
        let live = [bar(120, 9.0), bar(180, 3.0)];
        let m = merge(&history, &live);
        assert_eq!(
            m.iter().map(|b| (b.t, b.c)).collect::<Vec<_>>(),
            vec![(60, 1.0), (120, 2.0), (180, 3.0)]
        );
    }
}
