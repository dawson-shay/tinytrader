//! `AlpacaPaper`: the Alpaca paper broker behind the `Broker` seam. REST goes through a
//! small `ureq` wrapper, `trade_updates` through one background WebSocket thread; the
//! pure DTOs, bodies and parsing live in `crate::alpaca::trading`. Endpoints are
//! compiled in (paper, or a loopback fake server in tests); there is no URL option.
mod http;
mod stream;
pub mod trading;

use crate::broker::{
    AccountInfo, Broker, BrokerError, BrokerOrder, CancelResult, Capabilities, Closing,
    FlattenResult, MarketClock, OPEN_ORDERS_LIMIT, OrderRef, OrderUpdate, Position, Query, Reply,
    SessionDay, SubmitResult, Update,
};
use crate::intent::{Intent, fmt_cents};
use crate::protocol::{Obj, arr, quote};
use crate::safety::date_string;
use http::{Http, Resp};
use std::collections::HashMap;
use std::fmt::Write as _;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::mpsc::{Receiver, sync_channel};
use std::time::Duration;
use trading::{Credentials, Endpoints, LegSpec, Order, TradeUpdate, compact, elements, member};

/// Network timings; the defaults suit the paper endpoint.
#[derive(Clone, Copy, Debug)]
pub struct AlpacaOptions {
    /// Whole-request REST timeout.
    pub timeout: Duration,
    /// First stream reconnect delay (doubles per failure).
    pub reconnect_min: Duration,
    /// Longest stream reconnect delay.
    pub reconnect_max: Duration,
}

impl Default for AlpacaOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(10),
            reconnect_min: Duration::from_secs(1),
            reconnect_max: Duration::from_secs(30),
        }
    }
}

/// Reads an env-style credential file: mode 600, a regular file, not a symlink.
///
/// # Errors
/// The refusal; it names the path or a variable, never a value.
pub fn load_credentials(path: &Path) -> Result<Credentials, String> {
    let shown = path.display();
    let meta = std::fs::symlink_metadata(path).map_err(|e| format!("credentials {shown}: {e}"))?;
    if !meta.file_type().is_file() || meta.permissions().mode() & 0o077 != 0 {
        return Err(format!(
            "credentials {shown} must be a regular mode-600 file, not a symlink"
        ));
    }
    let text = std::fs::read_to_string(path).map_err(|e| format!("credentials {shown}: {e}"))?;
    Credentials::from_env_text(&text).map_err(|e| format!("credentials {shown}: {e}"))
}

/// The Alpaca paper broker for one account.
pub struct AlpacaPaper {
    http: Http,
    endpoints: Endpoints,
    updates: Receiver<Update>,
    _stream: stream::Stream,
    calendar: HashMap<(String, String), Reply<Vec<SessionDay>>>,
}

impl AlpacaPaper {
    /// The paper endpoints.
    #[must_use]
    pub fn paper(creds: Credentials) -> Self {
        Self::new(Endpoints::paper(), creds, AlpacaOptions::default())
    }

    /// The live endpoints: real money, only for `--allow-live`.
    #[must_use]
    pub fn live(creds: Credentials) -> Self {
        Self::new(Endpoints::live(), creds, AlpacaOptions::default())
    }

    /// Explicit endpoints (paper, live or loopback) and timings. The
    /// stream starts down; `StreamUp` follows the first `listening`.
    #[must_use]
    pub fn new(endpoints: Endpoints, creds: Credentials, opts: AlpacaOptions) -> Self {
        // Bounded: a session that falls behind holds the stream's reader, and the
        // socket pushes back on the broker.
        let (tx, updates) = sync_channel(4096);
        let _ = tx.send(Update::StreamDown);
        let stream = stream::spawn(
            endpoints.stream().to_owned(),
            creds.stream_auth(),
            tx,
            (opts.reconnect_min, opts.reconnect_max),
        );
        Self {
            http: Http::new(endpoints.rest(), creds, opts.timeout),
            endpoints,
            updates,
            _stream: stream,
            calendar: HashMap::new(),
        }
    }

    fn get(&self, path: &str) -> Result<Resp, BrokerError> {
        match self.http.call("GET", path, None) {
            Ok(r) if (200..300).contains(&r.status) => Ok(r),
            Ok(r) => Err(error(&r)),
            Err(f) => Err(BrokerError {
                http_status: 0,
                code: "broker_error".into(),
                message: f.reason().into(),
                raw: "null".into(),
                retry_after_ms: None,
            }),
        }
    }

    fn orders(&self, path: &str) -> Query<Vec<BrokerOrder>> {
        let r = self.get(path)?;
        let list = trading::parse_orders(&r.body).ok_or_else(|| malformed(&r))?;
        Ok(Reply {
            value: list.iter().map(broker_order).collect(),
            raw: compact(&r.body),
        })
    }

    /// One DELETE; never retried.
    fn delete(&self, path: &str) -> CancelResult {
        match self.http.call("DELETE", path, None) {
            Err(f) => CancelResult::Unknown {
                reason: f.reason(),
                retry_after_ms: None,
            },
            Ok(r) => match unsent(&r) {
                Some((reason, retry_after_ms)) => CancelResult::Unknown {
                    reason,
                    retry_after_ms,
                },
                None if (200..300).contains(&r.status) => CancelResult::Accepted(raw(&r.body)),
                None => CancelResult::Rejected(error(&r)),
            },
        }
    }
}

/// A compact JSON body, or the body as a JSON string when it is not JSON.
fn raw(body: &str) -> String {
    if body.trim().is_empty() {
        "null".into()
    } else if serde_json::from_str::<serde_json::Value>(body).is_ok() {
        compact(body)
    } else {
        quote(body)
    }
}

fn error(r: &Resp) -> BrokerError {
    let (code, message) = trading::parse_error(r.status, &r.body);
    BrokerError {
        http_status: r.status,
        code,
        message,
        raw: raw(&r.body),
        retry_after_ms: r.retry_after_ms,
    }
}

fn malformed(r: &Resp) -> BrokerError {
    BrokerError {
        http_status: r.status,
        code: "malformed_response".into(),
        message: "unexpected response body".into(),
        raw: raw(&r.body),
        retry_after_ms: None,
    }
}

/// A 429 or 5xx after the request left: the outcome is unknown.
fn unsent(r: &Resp) -> Option<(&'static str, Option<u64>)> {
    match r.status {
        429 => Some(("http_429_after_send", r.retry_after_ms)),
        500.. => Some(("http_5xx", None)),
        _ => None,
    }
}

/// Percent-encodes a query value.
fn enc(s: &str) -> String {
    s.bytes().fold(String::new(), |mut out, b| {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(b));
        } else {
            let _ = write!(out, "%{b:02X}");
        }
        out
    })
}

/// RFC 3339 UTC seconds.
fn iso(ns: u64) -> String {
    let s = ns / 1_000_000_000;
    let day = date_string(i64::try_from(s / 86_400).unwrap_or(0));
    let t = s % 86_400;
    format!("{day}T{:02}:{:02}:{:02}Z", t / 3600, t / 60 % 60, t % 60)
}

pub(crate) fn broker_order(o: &Order) -> BrokerOrder {
    let legs = o.mleg.then(|| {
        arr(o.legs.iter().map(|l| {
            Obj::new()
                .s("symbol", &l.symbol)
                .n("filled_qty", l.filled_qty)
                .opt_s("avg_price", l.filled_avg_price.as_deref())
                .done()
        }))
    });
    BrokerOrder {
        client_order_id: o.client_order_id.clone(),
        broker_order_id: o.id.clone(),
        status: o.status.clone(),
        filled_qty: o.filled_qty,
        avg_price: o.filled_avg_price.clone(),
        legs,
        updated_ts: o.updated_ns,
        raw: o.raw.clone(),
    }
}

/// A `trade_updates` event as a gateway order update. A fill carries the increment; an
/// MLEG fill reports each leg's cumulative state plus its own execution.
pub(crate) fn order_update(u: TradeUpdate) -> OrderUpdate {
    let is_fill = matches!(u.event.as_str(), "fill" | "partial_fill");
    let fill = u.qty.filter(|_| is_fill && u.price.is_some()).map(|q| {
        Obj::new()
            .opt_s("execution_id", u.execution_id.as_deref())
            .n("qty", q)
            .opt_s("price", u.price.as_deref())
            .done()
    });
    let legs = u.order.mleg.then(|| {
        arr(u.order.legs.iter().map(|l| {
            let mut o = Obj::new()
                .s("symbol", &l.symbol)
                .n("filled_qty", l.filled_qty)
                .opt_s("avg_price", l.filled_avg_price.as_deref());
            if let Some(f) = u.legs.iter().find(|f| f.symbol == l.symbol) {
                let fill = Obj::new()
                    .n("qty", f.qty)
                    .opt_s("price", f.price.as_deref())
                    .opt_s("execution_id", f.execution_id.as_deref());
                o = o.raw("fill", &fill.done());
            }
            o.done()
        }))
    });
    OrderUpdate {
        ts_broker: u.timestamp_ns.unwrap_or(u.order.updated_ns),
        order: broker_order(&u.order),
        event: u.event,
        fill,
        legs,
        position_qty: u.position_qty,
        reason: None,
        source: "stream",
        raw: u.raw,
    }
}

impl Broker for AlpacaPaper {
    fn kind(&self) -> &'static str {
        "alpaca"
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            combo: true,
            futures: false,
            max_legs: 4,
            client_order_id_max: 128,
            idempotency: "broker_client_id",
        }
    }

    fn pins(&self) -> String {
        Obj::new()
            .s("broker", "alpaca")
            .s("environment", self.endpoints.environment())
            .s("rest", self.endpoints.rest())
            .s("stream", self.endpoints.stream())
            .s("trading", "order-gateway alpaca-trading/1")
            .done()
    }

    fn now(&self) -> Option<u64> {
        None
    }

    fn submit(&mut self, intent: &Intent) -> SubmitResult {
        // Alpaca trades no futures, and its gateway refuses them before here; were one
        // to arrive, it is refused here too, with nothing sent.
        if intent
            .legs
            .iter()
            .any(|l| l.class == crate::intent::Class::Future)
        {
            return SubmitResult::Rejected(BrokerError::local(
                "unsupported",
                "Alpaca trades no futures",
            ));
        }
        let legs: Vec<LegSpec<'_>> = intent
            .legs
            .iter()
            .map(|l| LegSpec {
                symbol: &l.symbol,
                ratio: l.ratio,
                buy: l.buy,
                open: l.open,
                equity: l.class == crate::intent::Class::Equity,
            })
            .collect();
        let price = fmt_cents(intent.limit_cents);
        let body = trading::order_body(
            &intent.client_order_id,
            intent.combo,
            &legs,
            intent.qty,
            (!intent.market).then_some(price.as_str()),
            intent.on_close,
        );
        let r = match self.http.call("POST", "/v2/orders", Some(&body)) {
            Ok(r) => r,
            Err(f) => {
                return SubmitResult::Unknown {
                    reason: f.reason(),
                    retry_after_ms: None,
                };
            }
        };
        if let Some((reason, retry_after_ms)) = unsent(&r) {
            return SubmitResult::Unknown {
                reason,
                retry_after_ms,
            };
        }
        if !(200..300).contains(&r.status) {
            return SubmitResult::Rejected(error(&r));
        }
        match trading::parse_order(&r.body) {
            Some(o) => SubmitResult::Accepted(broker_order(&o)),
            None => SubmitResult::Unknown {
                reason: "connection",
                retry_after_ms: None,
            },
        }
    }

    fn cancel(&mut self, broker_order_id: &str) -> CancelResult {
        if broker_order_id.is_empty()
            || !broker_order_id
                .bytes()
                .all(|b| b.is_ascii_hexdigit() || b == b'-')
        {
            return CancelResult::Rejected(BrokerError::local(
                "invalid_request",
                "broker_order_id is not an Alpaca id",
            ));
        }
        self.delete(&format!("/v2/orders/{broker_order_id}"))
    }

    fn cancel_all(&mut self) -> CancelResult {
        self.delete("/v2/orders")
    }

    /// Alpaca's liquidate-all: `DELETE /v2/positions?cancel_orders=true`
    /// cancels open orders and sends a market order closing each position.
    /// Its answer (207) lists each position's `{symbol, status, body}`.
    fn flatten(&mut self) -> FlattenResult {
        match self
            .http
            .call("DELETE", "/v2/positions?cancel_orders=true", None)
        {
            Err(f) => FlattenResult::Unknown { reason: f.reason() },
            Ok(r) => match unsent(&r) {
                Some((reason, _)) => FlattenResult::Unknown { reason },
                None if (200..300).contains(&r.status) => {
                    let body = raw(&r.body);
                    let text = |v: Option<&str>| {
                        v.and_then(|v| serde_json::from_str::<String>(v).ok())
                            .unwrap_or_default()
                    };
                    let closings = elements(&body)
                        .unwrap_or_default()
                        .into_iter()
                        .map(|e| Closing {
                            symbol: text(member(e, "symbol")),
                            status: member(e, "status")
                                .and_then(|v| v.parse().ok())
                                .unwrap_or(0),
                            raw: member(e, "body").unwrap_or("null").to_owned(),
                        })
                        .collect();
                    FlattenResult::Sent(closings, body)
                }
                None => FlattenResult::Rejected(error(&r)),
            },
        }
    }

    fn order(&mut self, target: &OrderRef) -> Query<Option<BrokerOrder>> {
        let path = match target {
            OrderRef::Client(c) => {
                format!(
                    "/v2/orders:by_client_order_id?client_order_id={}&nested=true",
                    enc(c)
                )
            }
            OrderRef::Broker(b) => format!("/v2/orders/{}?nested=true", enc(b)),
        };
        match self.get(&path) {
            Ok(r) => {
                let o = trading::parse_order(&r.body).ok_or_else(|| malformed(&r))?;
                Ok(Reply {
                    value: Some(broker_order(&o)),
                    raw: o.raw,
                })
            }
            Err(e) if e.http_status == 404 => Ok(Reply {
                value: None,
                raw: e.raw,
            }),
            Err(e) => Err(e),
        }
    }

    fn open_orders(&mut self) -> Query<Vec<BrokerOrder>> {
        self.orders(&format!(
            "/v2/orders?status=open&nested=true&limit={OPEN_ORDERS_LIMIT}"
        ))
    }

    fn orders_since(&mut self, after_ts: u64) -> Query<Vec<BrokerOrder>> {
        self.orders(&format!(
            "/v2/orders?status=all&after={}&nested=true&limit=500&direction=asc",
            enc(&iso(after_ts))
        ))
    }

    fn account(&mut self) -> Query<AccountInfo> {
        let r = self.get("/v2/account")?;
        let a = trading::parse_account(&r.body).ok_or_else(|| malformed(&r))?;
        Ok(Reply {
            value: AccountInfo {
                status: a.status,
                cash: a.cash,
                buying_power: a.buying_power,
                options_trading_level: a.options_trading_level,
                blocked: a.blocked,
            },
            raw: compact(&r.body),
        })
    }

    fn positions(&mut self) -> Query<Vec<Position>> {
        let r = self.get("/v2/positions")?;
        let list = trading::parse_positions(&r.body).ok_or_else(|| malformed(&r))?;
        Ok(Reply {
            value: list
                .into_iter()
                .map(|(symbol, qty, avg_entry_price)| Position {
                    symbol,
                    qty,
                    avg_entry_price,
                })
                .collect(),
            raw: compact(&r.body),
        })
    }

    fn activities(
        &mut self,
        types: &[String],
        after: Option<&str>,
        page_token: Option<&str>,
    ) -> Query<(Vec<String>, Option<String>)> {
        const PAGE: usize = 100;
        let mut path = format!("/v2/account/activities?direction=asc&page_size={PAGE}");
        if !types.is_empty() {
            let _ = write!(path, "&activity_types={}", enc(&types.join(",")));
        }
        for (k, v) in [("after", after), ("page_token", page_token)] {
            if let Some(v) = v {
                let _ = write!(path, "&{k}={}", enc(v));
            }
        }
        let r = self.get(&path)?;
        let body = compact(&r.body);
        let items: Vec<String> = trading::elements(&body)
            .ok_or_else(|| malformed(&r))?
            .into_iter()
            .map(String::from)
            .collect();
        let next = (items.len() == PAGE)
            .then(|| items.last().and_then(|i| trading::member(i, "id")))
            .flatten()
            .and_then(|id| serde_json::from_str::<String>(id).ok());
        Ok(Reply {
            value: (items, next),
            raw: body,
        })
    }

    fn clock(&mut self) -> Query<MarketClock> {
        let r = self.get("/v2/clock")?;
        let (ts, is_open, next_open, next_close) =
            trading::parse_clock(&r.body).ok_or_else(|| malformed(&r))?;
        Ok(Reply {
            value: MarketClock {
                ts,
                is_open,
                next_open,
                next_close,
            },
            raw: compact(&r.body),
        })
    }

    /// Cached per range: a published calendar does not change within a process.
    fn calendar(&mut self, start: &str, end: &str) -> Query<Vec<SessionDay>> {
        let key = (start.to_owned(), end.to_owned());
        if let Some(hit) = self.calendar.get(&key) {
            return Ok(hit.clone());
        }
        let r = self.get(&format!(
            "/v2/calendar?start={}&end={}",
            enc(start),
            enc(end)
        ))?;
        let days = trading::parse_calendar(&r.body).ok_or_else(|| malformed(&r))?;
        let reply = Reply {
            value: days
                .into_iter()
                .map(|(date, open, close)| SessionDay { date, open, close })
                .collect(),
            raw: compact(&r.body),
        };
        if self.calendar.len() >= 64 {
            self.calendar.clear();
        }
        self.calendar.insert(key, reply.clone());
        Ok(reply)
    }

    fn poll_updates(&mut self, out: &mut Vec<Update>) {
        out.extend(self.updates.try_iter());
    }
}
