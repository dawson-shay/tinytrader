//! Alpaca IEX source: one websocket stream of quotes and trades, encoded as
//! MBP-1 (with trade conditions for clients that ask), and catch-up from
//! Alpaca's historical API.
//!
//! Alpaca allows one market data stream per account, so this relay must be
//! the only process streaming with its key.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::future::BoxFuture;
use futures_util::{SinkExt as _, StreamExt as _};
use relay::iex::{self, Encoder, RawQuote, RawTrade, Row};
use serde::Deserialize;
use serde_json::json;
use serde_json::value::RawValue;
use tinytrader_dbn::TradeConditions;
use tinytrader_dbn::dbn::{FlagSet, Mbp1Msg, RecordRef, SType};
use tinytrader_observe::{Observer, State};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use crate::bars::Bar;
use crate::hours::{self, Hours};
use crate::hub::Hub;
use crate::protocol;
use crate::source::{Feed, History, Source};

const LIVE_URL: &str = "wss://stream.data.alpaca.markets/v2/iex";
const TEST_URL: &str = "wss://stream.data.alpaca.markets/v2/test";
pub const HISTORY_URL: &str = "https://data.alpaca.markets/v2/stocks";
/// Pages a request may take, refused beyond: 10 days of minutes is 2 pages
/// (72 at 200 a page); 10 million trades or quotes is many busy IEX days.
const BAR_PAGES: usize = 100;
const TICK_PAGES: usize = 1_000;
/// History is sent to clients in chunks of about this many bytes.
const CHUNK: usize = 64 * 1024;

pub struct Alpaca {
    pub key: String,
    pub secret: String,
    pub symbols: Vec<String>,
    /// Use Alpaca's test stream (symbol `FAKEPACA`, any time of day) and
    /// accept its non-IEX venue codes.
    pub test: bool,
    pub http: reqwest::Client,
    /// Alpaca's stock history (`HISTORY_URL`).
    pub history_url: String,
}

/// The next page token (none when absent or empty: the last page), refused
/// if it is malformed, repeats the last, or would go past `cap` pages.
fn next_token(
    kind: &str,
    last: Option<&str>,
    next: Option<String>,
    pages: usize,
    cap: usize,
) -> Result<Option<String>, String> {
    let Some(t) = next.filter(|t| !t.is_empty()) else {
        return Ok(None);
    };
    if t.len() > 256 || !t.bytes().all(|b| b.is_ascii_graphic()) {
        return Err(format!("{kind}: an unusable page token"));
    }
    if last == Some(t.as_str()) {
        return Err(format!("{kind}: the same page token twice"));
    }
    if pages >= cap {
        return Err(format!("{kind}: more than {cap} pages"));
    }
    Ok(Some(t))
}

/// One element of a stream frame: its type and symbol.
#[derive(Deserialize)]
struct Tag<'a> {
    #[serde(rename = "T")]
    kind: &'a str,
    #[serde(rename = "S", default)]
    symbol: Option<&'a str>,
    #[serde(default)]
    msg: Option<&'a str>,
    #[serde(default)]
    code: Option<i64>,
    /// Venue codes: `x` for trades, `bx`/`ax` for quotes.
    #[serde(default)]
    x: Option<&'a str>,
    #[serde(default)]
    bx: Option<&'a str>,
    #[serde(default)]
    ax: Option<&'a str>,
}

/// Encoded records of a frame or a stretch of history, in order, each trade
/// followed by its conditions if any.
struct Encoded {
    records: Vec<(Mbp1Msg, Option<TradeConditions>)>,
}

impl Encoded {
    /// `F_LAST` on every record unless the next is the same instrument at the
    /// same time.
    fn mark_last(&mut self) {
        let n = self.records.len();
        for i in 0..n {
            let (cur, next) = (&self.records[i].0, self.records.get(i + 1).map(|r| &r.0));
            let same = next.is_some_and(|nx| {
                nx.hd.instrument_id == cur.hd.instrument_id && nx.hd.ts_event == cur.hd.ts_event
            });
            if !same {
                self.records[i].0.flags = FlagSet::empty().set_last();
            }
        }
    }

    fn bytes(&self, conditions: bool) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.records.len() * 96);
        for (rec, cond) in &self.records {
            out.extend_from_slice(RecordRef::from(rec).as_ref());
            if conditions && let Some(c) = cond.and_then(|c| c.encode().ok()) {
                out.extend_from_slice(&c);
            }
        }
        out
    }
}

impl Alpaca {
    /// One definition record per symbol.
    fn definitions(&self) -> Vec<u8> {
        let mut out = Vec::new();
        for s in &self.symbols {
            let d = Encoder::new(s).definition(s);
            out.extend_from_slice(RecordRef::from(&d).as_ref());
        }
        out
    }

    async fn stream(
        &self,
        hub: &Hub,
        observer: &Observer,
        encoders: &mut HashMap<String, Encoder>,
    ) -> Result<(), String> {
        let url = if self.test { TEST_URL } else { LIVE_URL };
        let (mut ws, _) = tokio_tungstenite::connect_async(url)
            .await
            .map_err(|e| format!("cannot connect to Alpaca: {e}"))?;
        // Symbols watched for the tape (tape.rs), subscribed on this stream
        // as they're asked for; their ticks go to the tape only.
        let mut watched: HashMap<String, Encoder> = HashMap::new();
        let (mut authed, mut live) = (false, false);
        let mut sweep = tokio::time::interval(Duration::from_secs(60));
        loop {
            let msg = tokio::select! {
                m = ws.next() => match m { Some(m) => m, None => break },
                () = hub.tape.watch.changed.notified() => {
                    if authed { self.resubscribe(&mut ws, hub, &mut watched).await?; }
                    continue;
                }
                _ = sweep.tick() => {
                    if authed { self.resubscribe(&mut ws, hub, &mut watched).await?; }
                    continue;
                }
            };
            let text = match msg.map_err(|e| format!("Alpaca stream failed: {e}"))? {
                Message::Text(t) => t,
                Message::Ping(p) => {
                    let _ = ws.send(Message::Pong(p)).await;
                    continue;
                }
                Message::Close(_) => return Ok(()),
                _ => continue,
            };
            let ts_recv = protocol::now_ns();
            let items: Vec<&RawValue> =
                serde_json::from_str(&text).map_err(|e| format!("unreadable stream frame: {e}"))?;
            let mut frame = Encoded {
                records: Vec::new(),
            };
            let mut refused = 0u64;
            for item in items {
                let tag: Tag = serde_json::from_str(item.get())
                    .map_err(|e| format!("unreadable stream message: {e}"))?;
                match tag.kind {
                    "success" if tag.msg == Some("connected") => {
                        let auth =
                            json!({ "action": "auth", "key": self.key, "secret": self.secret });
                        ws.send(Message::text(auth.to_string()))
                            .await
                            .map_err(|e| format!("cannot authenticate: {e}"))?;
                    }
                    "success" if tag.msg == Some("authenticated") => {
                        let sub = json!({ "action": "subscribe", "trades": self.symbols, "quotes": self.symbols });
                        ws.send(Message::text(sub.to_string()))
                            .await
                            .map_err(|e| format!("cannot subscribe: {e}"))?;
                        authed = true;
                        self.resubscribe(&mut ws, hub, &mut watched).await?;
                    }
                    "subscription" if !live => {
                        live = true;
                        hub.session_started(crate::protocol::now_ns());
                        hub.upstream_up();
                        observer.set_state(State::Ready, "live");
                        eprintln!(
                            "relay: Alpaca {} stream live for {}",
                            if self.test { "test" } else { "IEX" },
                            self.symbols.join(",")
                        );
                    }
                    "error" => {
                        return Err(format!(
                            "Alpaca refused: {} {}",
                            tag.code.unwrap_or(0),
                            tag.msg.unwrap_or("")
                        ));
                    }
                    "q" | "t" if tag.symbol.is_some_and(|s| watched.contains_key(s)) => {
                        let symbol = tag.symbol.unwrap_or_default();
                        if let Some(enc) = watched.get_mut(symbol)
                            && let Some(rec) = Self::encode(
                                enc,
                                tag.kind,
                                tag.x,
                                tag.bx,
                                tag.ax,
                                self.test,
                                item.get(),
                                ts_recv,
                            )
                        {
                            hub.tape
                                .absorb_watched(RecordRef::from(&rec).as_ref(), symbol);
                        }
                    }
                    "q" | "t" => {
                        let Some(enc) = tag.symbol.and_then(|s| encoders.get_mut(s)) else {
                            refused += 1;
                            continue;
                        };
                        let iex =
                            |code: Option<&str>| self.test || matches!(code, None | Some("V"));
                        if tag.kind == "q" {
                            let row: RawQuote =
                                serde_json::from_str(item.get()).map_err(|e| e.to_string())?;
                            match row.parse() {
                                Ok(q) if iex(tag.bx) && iex(tag.ax) => {
                                    frame.records.push((enc.quote(&q, ts_recv), None))
                                }
                                _ => refused += 1,
                            }
                        } else {
                            let row: RawTrade =
                                serde_json::from_str(item.get()).map_err(|e| e.to_string())?;
                            match row.parse() {
                                Ok(t) if iex(tag.x) => {
                                    let (rec, cond) = enc.trade(&t, ts_recv);
                                    frame.records.push((rec, cond.ok()));
                                }
                                _ => refused += 1,
                            }
                        }
                    }
                    _ => {}
                }
            }
            if refused > 0 {
                hub.refused_rows
                    .fetch_add(refused, std::sync::atomic::Ordering::Relaxed);
            }
            if !frame.records.is_empty() {
                frame.mark_last();
                let n = frame.records.len() as u64;
                hub.publish_pair(frame.bytes(false).into(), frame.bytes(true).into(), n);
            }
        }
        Ok(())
    }

    /// Bring the stream's watched symbols in line with the tape's: subscribe
    /// the new, unsubscribe those no longer read.
    async fn resubscribe<S>(
        &self,
        ws: &mut S,
        hub: &Hub,
        watched: &mut HashMap<String, Encoder>,
    ) -> Result<(), String>
    where
        S: futures_util::Sink<Message> + Unpin,
        S::Error: std::fmt::Display,
    {
        let want: Vec<String> = hub
            .tape
            .watch
            .wanted()
            .into_iter()
            .filter(|s| !self.symbols.contains(s))
            .collect();
        let add: Vec<&String> = want.iter().filter(|s| !watched.contains_key(*s)).collect();
        let drop: Vec<String> = watched
            .keys()
            .filter(|s| !want.contains(s))
            .cloned()
            .collect();
        if !add.is_empty() {
            let sub = json!({ "action": "subscribe", "trades": add, "quotes": add });
            ws.send(Message::text(sub.to_string()))
                .await
                .map_err(|e| format!("cannot subscribe: {e}"))?;
            eprintln!(
                "relay: watching {} for the tape",
                add.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(",")
            );
            for s in add {
                watched.insert(s.clone(), Encoder::new(s));
            }
        }
        if !drop.is_empty() {
            let sub = json!({ "action": "unsubscribe", "trades": drop, "quotes": drop });
            ws.send(Message::text(sub.to_string()))
                .await
                .map_err(|e| format!("cannot unsubscribe: {e}"))?;
            for s in &drop {
                watched.remove(s);
            }
        }
        Ok(())
    }

    /// One stream message encoded, if it's an IEX quote or trade that parses.
    #[allow(clippy::too_many_arguments)]
    fn encode(
        enc: &mut Encoder,
        kind: &str,
        x: Option<&str>,
        bx: Option<&str>,
        ax: Option<&str>,
        test: bool,
        raw: &str,
        ts_recv: u64,
    ) -> Option<Mbp1Msg> {
        let iex = |code: Option<&str>| test || matches!(code, None | Some("V"));
        if kind == "q" {
            let q = serde_json::from_str::<RawQuote>(raw).ok()?.parse().ok()?;
            (iex(bx) && iex(ax)).then(|| enc.quote(&q, ts_recv))
        } else {
            let t = serde_json::from_str::<RawTrade>(raw).ok()?.parse().ok()?;
            iex(x).then(|| enc.trade(&t, ts_recv).0)
        }
    }

    async fn history(&self, start: u64, cut: u64) -> Result<Encoded, String> {
        let mut per_symbol: Vec<Encoded> = Vec::new();
        for symbol in &self.symbols {
            let trades = self.pages(symbol, "trades", start, cut).await?;
            let quotes = self.pages(symbol, "quotes", start, cut).await?;
            let mut t = Vec::new();
            for page in &trades {
                let p: Page = serde_json::from_str(page).map_err(|e| format!("trade page: {e}"))?;
                for row in p
                    .trades
                    .and_then(|mut m| m.remove(symbol.as_str()))
                    .unwrap_or_default()
                {
                    t.push(row.parse()?);
                }
            }
            let mut q = Vec::new();
            for page in &quotes {
                let p: Page = serde_json::from_str(page).map_err(|e| format!("quote page: {e}"))?;
                for row in p
                    .quotes
                    .and_then(|mut m| m.remove(symbol.as_str()))
                    .unwrap_or_default()
                {
                    q.push(row.parse()?);
                }
            }
            let rows: Vec<Row> = iex::merge(t, q)?;
            let (records, conds) = iex::encode_day(symbol, &rows)?;
            let mut conds = conds.into_iter();
            let records = records
                .into_iter()
                .map(|r| {
                    let c = (r.action as u8 == b'T').then(|| conds.next()).flatten();
                    (r, c)
                })
                .collect();
            per_symbol.push(Encoded { records });
        }
        // Interleave the symbols by time, keeping each symbol's own order.
        let mut all: Vec<(u64, usize, usize)> = Vec::new();
        for (s, e) in per_symbol.iter().enumerate() {
            all.extend(
                e.records
                    .iter()
                    .enumerate()
                    .map(|(i, r)| (r.0.hd.ts_event, s, i)),
            );
        }
        all.sort_by_key(|&(ts, s, i)| (ts, s, i));
        Ok(Encoded {
            records: all
                .into_iter()
                .map(|(_, s, i)| per_symbol[s].records[i].clone())
                .collect(),
        })
    }

    /// One-minute bars for a symbol from Alpaca's IEX history, oldest first.
    async fn minute_bars(&self, symbol: &str, start: u64, end: u64) -> Result<Vec<Bar>, String> {
        #[derive(Deserialize)]
        struct Raw<'a> {
            t: &'a str,
            o: f64,
            h: f64,
            l: f64,
            c: f64,
            v: f64,
        }
        #[derive(Deserialize)]
        struct BarsPage<'a> {
            #[serde(borrow, default)]
            bars: Option<std::collections::BTreeMap<&'a str, Vec<Raw<'a>>>>,
            next_page_token: Option<String>,
        }
        let (start, end) = (rfc3339(start), rfc3339(end));
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        for n in 1.. {
            let mut req = self
                .http
                .get(format!("{}/bars", self.history_url))
                .header("APCA-API-KEY-ID", &self.key)
                .header("APCA-API-SECRET-KEY", &self.secret)
                .query(&[
                    ("symbols", symbol),
                    ("timeframe", "1Min"),
                    ("feed", "iex"),
                    ("start", &start),
                    ("end", &end),
                    ("limit", "10000"),
                    ("sort", "asc"),
                ]);
            if let Some(t) = &token {
                req = req.query(&[("page_token", t)]);
            }
            let r = req.send().await.map_err(|e| format!("bars: {e}"))?;
            if !r.status().is_success() {
                return Err(format!("bars: Alpaca answered {}", r.status()));
            }
            let text = r.text().await.map_err(|e| format!("bars: {e}"))?;
            let page: BarsPage = serde_json::from_str(&text).map_err(|e| format!("bars: {e}"))?;
            for b in page
                .bars
                .into_iter()
                .flat_map(|m| m.into_values())
                .flatten()
            {
                let t = time::OffsetDateTime::parse(
                    b.t,
                    &time::format_description::well_known::Rfc3339,
                )
                .map_err(|e| format!("bars: {e}"))?;
                out.push(Bar {
                    t: t.unix_timestamp() as u64,
                    o: b.o,
                    h: b.h,
                    l: b.l,
                    c: b.c,
                    v: b.v as u64,
                });
            }
            token = next_token("bars", token.as_deref(), page.next_page_token, n, BAR_PAGES)?;
            if token.is_none() {
                break;
            }
        }
        Ok(out)
    }

    /// Every page of one kind of history for a symbol, oldest first.
    /// Fill tapes from history as they ask (tape.rs): each symbol's latest
    /// IEX trades and quotes of the past week, one request each.
    pub async fn tape_history(self: Arc<Self>, hub: Arc<Hub>) {
        loop {
            hub.tape.watch.history_wanted.notified().await;
            for symbol in hub.tape.watch.take_history() {
                match self.latest(&symbol).await {
                    Ok(chunk) if !chunk.is_empty() => {
                        if hub.tape.seed(&chunk, &symbol) {
                            eprintln!("relay: {symbol} tape filled from history");
                        }
                    }
                    Ok(_) => {}
                    Err(e) => eprintln!("relay: history for the {symbol} tape: {e}"),
                }
            }
        }
    }

    /// A symbol's latest 1,000 trades of the past week, each with the quote
    /// before it (for which side it took), encoded as `mbp-1`, oldest first.
    /// Quotes are paged back until they reach the first trade, at most five
    /// requests.
    async fn latest(&self, symbol: &str) -> Result<Vec<u8>, String> {
        let start = rfc3339(protocol::now_ns().saturating_sub(7 * 86_400 * 1_000_000_000));
        let get = |kind: &'static str, limit: &'static str, token: Option<String>| {
            let mut req = self
                .http
                .get(format!("{}/{kind}", self.history_url))
                .header("APCA-API-KEY-ID", &self.key)
                .header("APCA-API-SECRET-KEY", &self.secret)
                .query(&[
                    ("symbols", symbol),
                    ("feed", "iex"),
                    ("start", &start),
                    ("limit", limit),
                    ("sort", "desc"),
                ]);
            if let Some(t) = token {
                req = req.query(&[("page_token", t)]);
            }
            async move {
                let r = req.send().await.map_err(|e| format!("{kind}: {e}"))?;
                if !r.status().is_success() {
                    return Err(format!("{kind}: Alpaca answered {}", r.status()));
                }
                r.text().await.map_err(|e| format!("{kind}: {e}"))
            }
        };
        #[derive(Deserialize)]
        struct Next {
            next_page_token: Option<String>,
        }
        let text = get("trades", "1000", None).await?;
        let page: Page = serde_json::from_str(&text).map_err(|e| format!("trades: {e}"))?;
        let mut t: Vec<_> = page
            .trades
            .and_then(|mut m| m.remove(symbol))
            .unwrap_or_default()
            .iter()
            .filter_map(|r| r.parse().ok())
            .collect();
        t.reverse();
        let Some(first) = t.first().map(|x| x.ts) else {
            return Ok(Vec::new());
        };
        let mut q = Vec::new();
        let mut token = None;
        for _ in 0..5 {
            let text = get("quotes", "10000", token.take()).await?;
            let page: Page = serde_json::from_str(&text).map_err(|e| format!("quotes: {e}"))?;
            q.extend(
                page.quotes
                    .and_then(|mut m| m.remove(symbol))
                    .unwrap_or_default()
                    .iter()
                    .filter_map(|r| r.parse().ok()),
            );
            token = serde_json::from_str::<Next>(&text)
                .ok()
                .and_then(|n| n.next_page_token);
            if token.is_none() || q.last().is_some_and(|x: &iex::Quote| x.ts <= first) {
                break;
            }
        }
        q.reverse();
        // Each trade with the quote just before it: a tape of trades, not of quotes.
        let rows = iex::merge(t, q)?;
        let keep: Vec<bool> = (0..rows.len())
            .map(|i| {
                matches!(rows[i], iex::Row::Trade(_))
                    || matches!(rows.get(i + 1), Some(iex::Row::Trade(_)))
            })
            .collect();
        let rows: Vec<iex::Row> = rows
            .into_iter()
            .zip(keep)
            .filter_map(|(r, k)| k.then_some(r))
            .collect();
        let (records, _) = iex::encode_day(symbol, &rows)?;
        let mut out = Vec::with_capacity(records.len() * 80);
        for r in &records {
            out.extend_from_slice(RecordRef::from(r).as_ref());
        }
        Ok(out)
    }

    async fn pages(
        &self,
        symbol: &str,
        kind: &str,
        start: u64,
        end: u64,
    ) -> Result<Vec<String>, String> {
        let (start, end) = (rfc3339(start), rfc3339(end));
        let mut pages = Vec::new();
        let mut token: Option<String> = None;
        for n in 1.. {
            let mut req = self
                .http
                .get(format!("{}/{kind}", self.history_url))
                .header("APCA-API-KEY-ID", &self.key)
                .header("APCA-API-SECRET-KEY", &self.secret)
                .query(&[
                    ("symbols", symbol),
                    ("feed", "iex"),
                    ("start", &start),
                    ("end", &end),
                    ("limit", "10000"),
                    ("sort", "asc"),
                ]);
            if let Some(t) = &token {
                req = req.query(&[("page_token", t)]);
            }
            let r = req.send().await.map_err(|e| format!("{kind}: {e}"))?;
            if !r.status().is_success() {
                return Err(format!("{kind}: Alpaca answered {}", r.status()));
            }
            let text = r.text().await.map_err(|e| format!("{kind}: {e}"))?;
            #[derive(Deserialize)]
            struct Next {
                next_page_token: Option<String>,
            }
            let next = serde_json::from_str::<Next>(&text).map_err(|e| format!("{kind}: {e}"))?;
            token = next_token(kind, token.as_deref(), next.next_page_token, n, TICK_PAGES)?;
            pages.push(text);
            if token.is_none() {
                break;
            }
        }
        Ok(pages)
    }
}

impl Source for Alpaca {
    fn feed(&self) -> Feed {
        Feed {
            dataset: iex::DATASET.to_owned(),
            schema: "mbp-1".to_owned(),
            stype_in: SType::RawSymbol,
            provider: "alpaca",
            name: "Alpaca",
            via: "websocket",
        }
    }

    /// Keep the stream up, publishing its records to the hub.
    fn run(
        self: Arc<Self>,
        hub: Arc<Hub>,
        observer: Observer,
        hours: Arc<Hours>,
    ) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            hub.publish(self.definitions(), self.symbols.len() as u64);
            let mut encoders: HashMap<String, Encoder> = self
                .symbols
                .iter()
                .map(|s| (s.clone(), Encoder::new(s)))
                .collect();
            let mut backoff = Duration::from_secs(1);
            loop {
                let end = hours.wait_until_active(&hub, &observer).await;
                let began = Instant::now();
                // At the end of the active window the stream is dropped, which
                // disconnects it.
                let outcome = tokio::select! {
                    r = self.stream(&hub, &observer, &mut encoders) => Some(r),
                    () = hours::until(end) => None,
                };
                let Some(outcome) = outcome else {
                    continue;
                };
                hub.upstream_down();
                let why = match outcome {
                    Ok(()) => "Alpaca closed the stream".to_owned(),
                    Err(e) => e,
                };
                eprintln!("relay: {why}; reconnecting in {}s", backoff.as_secs());
                observer.record_error(&why);
                observer.set_state(State::Degraded, &format!("{why}; reconnecting"));
                if began.elapsed() > Duration::from_secs(60) {
                    backoff = Duration::from_secs(1);
                }
                tokio::time::sleep(backoff).await;
                // Alpaca refuses a second connection for a while after a drop.
                backoff = (backoff * 2).min(Duration::from_secs(60));
            }
        })
    }

    /// Fetch history for every symbol from `start` to `cut` (both in ns since
    /// the epoch) and send it as chunks of records, as the relay encodes it.
    fn catch_up(self: Arc<Self>, start: u64, cut: u64, conditions: bool) -> Option<History> {
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            match self.history(start, cut).await {
                Ok(encoded) => {
                    let bytes = encoded.bytes(conditions);
                    for chunk in bytes.chunks(CHUNK) {
                        if tx.send(Ok(chunk.to_vec())).await.is_err() {
                            return;
                        }
                    }
                }
                Err(e) => {
                    let _ = tx
                        .send(Err(format!("cannot catch up from Alpaca: {e}")))
                        .await;
                }
            }
        });
        Some(rx)
    }

    fn conditions(&self) -> bool {
        true
    }

    /// One-minute bars from Alpaca's IEX history.
    fn bars(
        &self,
        symbol: &str,
        start: u64,
        end: u64,
    ) -> Option<BoxFuture<'_, Result<Vec<Bar>, String>>> {
        let symbol = symbol.to_owned();
        Some(Box::pin(async move {
            self.minute_bars(&symbol, start, end).await
        }))
    }
}

#[derive(Deserialize)]
struct Page<'a> {
    #[serde(borrow, default)]
    quotes: Option<std::collections::BTreeMap<&'a str, Vec<RawQuote<'a>>>>,
    #[serde(borrow, default)]
    trades: Option<std::collections::BTreeMap<&'a str, Vec<RawTrade<'a>>>>,
}

/// Nanoseconds since the epoch as RFC 3339 with nanoseconds.
fn rfc3339(ns: u64) -> String {
    let t = time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(ns))
        .unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:09}Z",
        t.year(),
        u8::from(t.month()),
        t.day(),
        t.hour(),
        t.minute(),
        t.second(),
        t.nanosecond()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// An Alpaca on loopback answering every page with `body(n)`, `n` counting
    /// from 0; and how many it answered.
    async fn fake(body: fn(usize) -> String) -> (Alpaca, Arc<AtomicUsize>) {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let history_url = format!("http://{}", l.local_addr().unwrap());
        let n = Arc::new(AtomicUsize::new(0));
        let seen = n.clone();
        let app = axum::Router::new()
            .fallback(move || std::future::ready(body(seen.fetch_add(1, Ordering::Relaxed))));
        tokio::spawn(async move { axum::serve(l, app).await });
        let (key, secret, symbols) = (String::new(), String::new(), vec!["SPY".into()]);
        let http = reqwest::Client::new();
        (
            Alpaca {
                key,
                secret,
                symbols,
                test: false,
                http,
                history_url,
            },
            n,
        )
    }

    #[tokio::test]
    async fn a_repeated_or_malformed_token_ends_paging_with_an_error() {
        let (a, n) = fake(|_| r#"{"trades":{},"next_page_token":"abc"}"#.into()).await;
        let e = a.pages("SPY", "trades", 0, 1).await.unwrap_err();
        assert!(e.contains("the same page token twice"), "{e}");
        assert_eq!(n.load(Ordering::Relaxed), 2);
        let (a, _) = fake(|_| r#"{"quotes":{},"next_page_token":"a b"}"#.into()).await;
        let e = a.pages("SPY", "quotes", 0, 1).await.unwrap_err();
        assert!(e.contains("unusable page token"), "{e}");
        let (a, _) = fake(|_| r#"{"bars":{},"next_page_token":"é"}"#.into()).await;
        assert!(a.minute_bars("SPY", 0, 1).await.is_err());
        let (a, _) = fake(|_| "not json".into()).await;
        assert!(
            a.pages("SPY", "trades", 0, 1).await.is_err(),
            "malformed is no last page"
        );
    }

    #[tokio::test]
    async fn paging_stops_at_its_cap_with_an_error() {
        let (a, n) = fake(|i| format!(r#"{{"bars":{{}},"next_page_token":"t{i}"}}"#)).await;
        let e = a.minute_bars("SPY", 0, 1).await.unwrap_err();
        assert!(e.contains("more than 100 pages"), "{e}");
        assert_eq!(n.load(Ordering::Relaxed), BAR_PAGES);
        let (a, _) = fake(|_| r#"{"bars":{},"next_page_token":null}"#.into()).await;
        assert_eq!(a.minute_bars("SPY", 0, 1).await, Ok(Vec::new()));
        // An empty token is the last page, as null is.
        let (a, n) = fake(|_| r#"{"trades":{},"next_page_token":""}"#.into()).await;
        assert_eq!(a.pages("SPY", "trades", 0, 1).await.map(|p| p.len()), Ok(1));
        assert_eq!(n.load(Ordering::Relaxed), 1);
    }
}
