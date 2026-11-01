//! The synchronous `Broker` seam (one type parameter on the account session) and the
//! broker-neutral values it exchanges. Every `raw` field is broker JSON text, verbatim;
//! timestamps are ns; prices and cash are decimal strings.
use crate::intent::Intent;
pub use crate::protocol::OrderRef;

/// Accepted shapes and id rules, sent in `welcome.capabilities`.
#[derive(Clone, Debug)]
pub struct Capabilities {
    pub combo: bool,
    /// Whether it trades futures.
    pub futures: bool,
    pub max_legs: usize,
    pub client_order_id_max: usize,
    pub idempotency: &'static str,
}

/// A refusal or failure; `http_status` 0 means no HTTP exchange, 429 carries back-off.
#[derive(Clone, Debug)]
pub struct BrokerError {
    pub http_status: u16,
    pub code: String,
    pub message: String,
    pub raw: String,
    pub retry_after_ms: Option<u64>,
}

impl BrokerError {
    /// A refusal with no HTTP exchange, e.g. an operation this broker does not offer.
    #[must_use]
    pub fn local(code: &str, message: &str) -> Self {
        Self {
            http_status: 0,
            code: code.into(),
            message: message.into(),
            raw: "null".into(),
            retry_after_ms: None,
        }
    }
}

/// A successful query value with its raw broker JSON.
#[derive(Clone, Debug)]
pub struct Reply<T> {
    pub value: T,
    pub raw: String,
}

/// Query result.
pub type Query<T> = Result<Reply<T>, BrokerError>;

/// Broker order state; `legs` is a JSON array `[{symbol, filled_qty, avg_price}]`.
#[derive(Clone, Debug)]
pub struct BrokerOrder {
    pub client_order_id: String,
    pub broker_order_id: String,
    pub status: String,
    pub filled_qty: u64,
    pub avg_price: Option<String>,
    pub legs: Option<String>,
    pub updated_ts: u64,
    pub raw: String,
}

/// One pushed order change. `event` is the exact broker string; `fill`, `legs` and
/// `reason` are JSON text for the `order` event; `source` is `stream` or `sim`.
#[derive(Clone, Debug)]
pub struct OrderUpdate {
    pub order: BrokerOrder,
    pub event: String,
    pub ts_broker: u64,
    pub fill: Option<String>,
    pub legs: Option<String>,
    pub position_qty: Option<i64>,
    pub reason: Option<String>,
    pub source: &'static str,
    pub raw: String,
}

/// Something the broker pushed since the last poll.
#[derive(Clone, Debug)]
pub enum Update {
    /// An order changed.
    Order(Box<OrderUpdate>),
    /// The update stream dropped; changes may be missing until it returns.
    StreamDown,
    /// The update stream is back; the gateway resyncs through queries.
    StreamUp,
    /// A non-trade account activity: an option expired, exercised or assigned.
    Activity {
        /// `OPEXP`, `OPEXC`, `OPASN`, …
        activity_type: String,
        /// The broker's JSON.
        raw: String,
    },
}

/// Submit result; at most one send attempt was made.
#[derive(Clone, Debug)]
pub enum SubmitResult {
    /// Acknowledged (not a fill).
    Accepted(BrokerOrder),
    /// Refused by the broker. Final.
    Rejected(BrokerError),
    /// May have reached the broker: `timeout`, `connection`, `http_5xx` or
    /// `http_429_after_send` (with back-off).
    Unknown {
        reason: &'static str,
        retry_after_ms: Option<u64>,
    },
}

/// Cancel result; the terminal state arrives later as an update.
#[derive(Clone, Debug)]
pub enum CancelResult {
    /// Accepted with the raw body.
    Accepted(String),
    /// Refused by the broker.
    Rejected(BrokerError),
    /// May have reached the broker (reasons as for submit).
    Unknown {
        reason: &'static str,
        retry_after_ms: Option<u64>,
    },
}

/// Account fields used by the start gate and the `account` query.
#[derive(Clone, Debug)]
pub struct AccountInfo {
    pub status: String,
    pub cash: String,
    pub buying_power: String,
    pub options_trading_level: u32,
    pub blocked: bool,
}

/// One position (signed quantity).
#[derive(Clone, Debug)]
pub struct Position {
    pub symbol: String,
    pub qty: i64,
    pub avg_entry_price: String,
}

/// One position's closing order from `flatten`.
#[derive(Clone, Debug)]
pub struct Closing {
    pub symbol: String,
    /// The broker's HTTP status for this position (200 when its order was placed).
    pub status: u16,
    /// The broker's per-position body (its closing order, or an error).
    pub raw: String,
}

/// Result of closing every position; the fills arrive later as updates.
#[derive(Clone, Debug)]
pub enum FlattenResult {
    /// Sent; one entry per position, and the whole raw answer.
    Sent(Vec<Closing>, String),
    /// Refused as a whole by the broker.
    Rejected(BrokerError),
    /// May have reached the broker (reasons as for submit). Never resent.
    Unknown { reason: &'static str },
}

/// Broker clock.
#[derive(Clone, Copy, Debug)]
pub struct MarketClock {
    pub ts: u64,
    pub is_open: bool,
    pub next_open: u64,
    pub next_close: u64,
}

/// One trading day: `YYYY-MM-DD`, open and close `HH:MM` in `America/New_York`.
#[derive(Clone, Debug)]
pub struct SessionDay {
    pub date: String,
    pub open: String,
    pub close: String,
}

/// The most orders one open-orders listing returns. The listing is one request with no
/// paging, so a listing of this many may be cut short.
pub const OPEN_ORDERS_LIMIT: usize = 500;

/// One broker account. Every call is synchronous and makes at most one send.
#[expect(
    clippy::missing_errors_doc,
    reason = "every query fails only with the broker refusal it carries"
)]
pub trait Broker {
    /// `alpaca` or `sim`.
    fn kind(&self) -> &'static str;
    /// Accepted shapes and id rules.
    fn capabilities(&self) -> Capabilities;
    /// Reproducibility and build pins (JSON object).
    fn pins(&self) -> String;
    /// Market time for a simulated broker; `None` means the gateway uses the wall clock.
    fn now(&self) -> Option<u64>;
    /// One send of a new order. Never retried.
    fn submit(&mut self, intent: &Intent) -> SubmitResult;
    /// Cancel one order by broker id.
    fn cancel(&mut self, broker_order_id: &str) -> CancelResult;
    /// Cancel every open order (`DELETE /v2/orders`).
    fn cancel_all(&mut self) -> CancelResult;
    /// Close every position at market (cancelling open orders first), in one
    /// request that is never retried.
    fn flatten(&mut self) -> FlattenResult;
    /// One order by either id; `None` when the broker does not know it.
    fn order(&mut self, target: &OrderRef) -> Query<Option<BrokerOrder>>;
    /// Open orders.
    fn open_orders(&mut self) -> Query<Vec<BrokerOrder>>;
    /// Orders updated at or after `after_ts` (reconciliation).
    fn orders_since(&mut self, after_ts: u64) -> Query<Vec<BrokerOrder>>;
    /// Account snapshot.
    fn account(&mut self) -> Query<AccountInfo>;
    /// Positions.
    fn positions(&mut self) -> Query<Vec<Position>>;
    /// One page of raw activities and the next page token.
    fn activities(
        &mut self,
        types: &[String],
        after: Option<&str>,
        page_token: Option<&str>,
    ) -> Query<(Vec<String>, Option<String>)>;
    /// Broker clock.
    fn clock(&mut self) -> Query<MarketClock>;
    /// Trading days between two dates inclusive.
    fn calendar(&mut self, start: &str, end: &str) -> Query<Vec<SessionDay>>;
    /// Drains pushed updates without blocking.
    fn poll_updates(&mut self, out: &mut Vec<Update>);
    /// Client-clocked replay (simulator only): applies market input up to `to_ts`.
    ///
    /// # Errors
    /// Brokers without a client clock refuse.
    fn advance(&mut self, to_ts: u64) -> Result<u64, BrokerError> {
        let _ = to_ts;
        Err(BrokerError::local(
            "unsupported",
            "advance needs a simulated broker",
        ))
    }
    /// Whether a session owns this broker's clock in process, so a client's own
    /// `advance` is refused.
    fn session_clocked(&self) -> bool {
        false
    }
}

/// Normalized `event` vocabulary for a broker event string.
pub(crate) fn normalize_event(e: &str) -> &'static str {
    match e {
        "new" => "new",
        "accepted" => "accepted",
        "pending_new" => "pending_new",
        "partial_fill" => "partial_fill",
        "fill" => "fill",
        "pending_cancel" => "pending_cancel",
        "canceled" => "canceled",
        "expired" => "expired",
        "done_for_day" => "done_for_day",
        "replaced" => "replaced",
        "rejected" => "rejected",
        "stopped" => "stopped",
        "suspended" => "suspended",
        "calculated" => "calculated",
        "order_cancel_rejected" | "cancel_rejected" => "cancel_rejected",
        "order_replace_rejected" | "replace_rejected" => "replace_rejected",
        _ => "other",
    }
}

/// The event a reconciled status implies.
pub(crate) fn status_event(status: &str) -> &'static str {
    match status {
        "partially_filled" => "partial_fill",
        "filled" => "fill",
        s => normalize_event(s),
    }
}

/// Statuses after which an order can no longer change.
pub(crate) fn terminal(status: &str) -> bool {
    matches!(
        status,
        "filled" | "canceled" | "expired" | "rejected" | "done_for_day" | "replaced"
    )
}
