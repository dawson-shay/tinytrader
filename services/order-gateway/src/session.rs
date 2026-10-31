//! The account session: one thread owning the broker and the journal. It admits,
//! forwards and correlates requests, never resends, resolves unknown outcomes by
//! lookup, reconciles after restarts and stream drops, and fans out journaled events.
use crate::broker::{
    Broker, BrokerError, BrokerOrder, CancelResult, FlattenResult, OPEN_ORDERS_LIMIT, OrderRef,
    OrderUpdate, SubmitResult, Update, normalize_event, status_event, terminal,
};
use crate::intent::{Intent, hex, max_loss, occ};
use crate::journal::Journal;
use crate::protocol::{
    Invalid, KillLevel, Obj, Op, arr, error_line, invalid, invalid_detail, quote, request_error,
    response,
};
use crate::safety::{Budget, et_day};
use crate::server::{Config, Outbox, Role};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// The requester recorded on a `cleared` line made by the gateway itself.
const GATEWAY: &str = "gateway";

const EVENTS: &str = r#"["new","accepted","pending_new","partial_fill","fill","pending_cancel","canceled","expired","done_for_day","replaced","rejected","stopped","suspended","calculated","cancel_rejected","replace_rejected","other"]"#;

/// Wall-clock nanoseconds since the Unix epoch.
#[must_use]
pub fn wall_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
}

type Parsed = Result<(String, Op), (Option<String>, String, Invalid)>;

pub(crate) enum Input {
    Hello {
        id: u64,
        client: usize,
        outbox: Arc<Outbox>,
        resume: Option<u64>,
        cancel_on_disconnect: bool,
    },
    Request {
        id: u64,
        request: Parsed,
    },
    Closed(u64),
    /// An in-process clock (a session, decision 0022): once `orders` order messages
    /// (submits and cancels) from `client` have been handled, advance the broker to `to`
    /// and answer the market time; at `deadline`, give up and say so.
    Clock {
        client: String,
        orders: u64,
        to: u64,
        deadline: Instant,
        reply: Sender<Result<Advanced, String>>,
    },
    Shutdown,
}

/// What a clock advance did: the market time reached, and the account's last event
/// sequence number once the advance's events were journaled. A strategy that has read
/// its events up to `last_seq` has every report of the advance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Advanced {
    /// Market time after the advance.
    pub market_ts: u64,
    /// The last event sequence number journaled.
    pub last_seq: u64,
    /// The client's order messages handled so far: at least the count waited for.
    pub handled: u64,
    /// Order events journaled so far (submits, cancels, broker order events), which,
    /// unlike `last_seq`, connection and status events never move.
    pub order_events: u64,
}

/// A clock advance waiting for a client's order messages to arrive.
struct ClockWait {
    client: usize,
    orders: u64,
    to: u64,
    deadline: Instant,
    reply: Sender<Result<Advanced, String>>,
}

struct Conn {
    id: u64,
    client: usize,
    outbox: Arc<Outbox>,
    rids: HashSet<String>,
    cod: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Admitted,
    Sent,
    Unknown,
    Done,
    /// Looked up and not found, but still open until a stream event or a lookup that
    /// finds it settles it. A 404 never clears it. Not journaled: after a restart it
    /// is Unknown again.
    Absent,
    /// Cleared after both broker checks showed nothing (section 5). Not open, but the
    /// client order id stays reserved.
    Cleared,
}

struct OrderState {
    client: String,
    request_id: String,
    sha: Option<String>,
    broker_id: Option<String>,
    status: String,
    filled: u64,
    avg: Option<String>,
    legs: Option<String>,
    raw: Option<String>,
    /// The intent's leg symbols, the keys a close is checked against.
    symbols: Vec<String>,
    /// The intent's `time_in_force` (`day` or `cls`). `None` when the journal does not
    /// say, which is never auto-cleared.
    tif: Option<String>,
    /// The submit time (ns), when journaled.
    submitted: Option<u64>,
    phase: Phase,
}

/// Where the live rate clock starts, ns: a day in, so an hour before the start is a
/// time on it.
const RATE_ORIGIN: u64 = 86_400_000_000_000;

/// A duration in ns, saturating.
fn ns(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

fn utc_day(ts: u64) -> i64 {
    i64::try_from(ts / 86_400_000_000_000).unwrap_or(0)
}

/// Whether a journaled `admitted` intent has an opening leg. Anything not clearly
/// all-close counts as opening, so the restored day count fails closed.
fn journal_opens(v: &Value) -> bool {
    let intent = &v["intent"];
    let legs: Vec<&Value> = match intent["legs"].as_array() {
        Some(legs) if !legs.is_empty() => legs.iter().collect(),
        _ => vec![intent],
    };
    legs.iter().any(|l| l["effect"].as_str() != Some("close"))
}

/// The `time_in_force` of a journaled `admitted` intent; `None` when absent.
fn journal_tif(v: &Value) -> Option<String> {
    v["intent"]["time_in_force"].as_str().map(String::from)
}

/// The leg symbols of a journaled `admitted` intent, as `Leg::symbol` has them.
fn journal_symbols(v: &Value) -> Vec<String> {
    let intent = &v["intent"];
    let legs: Vec<&Value> = match intent["legs"].as_array() {
        Some(legs) if !legs.is_empty() => legs.iter().collect(),
        _ => vec![intent],
    };
    legs.iter()
        .filter_map(|l| l["instrument"]["symbol"].as_str().map(String::from))
        .collect()
}

impl OrderState {
    fn admitted(
        client: String,
        request_id: String,
        sha: Option<String>,
        symbols: Vec<String>,
        tif: Option<String>,
        submitted: Option<u64>,
    ) -> Self {
        let (status, phase) = (String::new(), Phase::Admitted);
        let (broker_id, avg, legs, raw) = (None, None, None, None);
        Self {
            client,
            request_id,
            sha,
            broker_id,
            status,
            filled: 0,
            avg,
            legs,
            raw,
            symbols,
            tif,
            submitted,
            phase,
        }
    }
    /// Whether the order counts against the open cap and blocks closes on its legs.
    /// An absent order does: only a stream event or a reconcile settles it.
    fn open(&self) -> bool {
        match self.phase {
            Phase::Admitted | Phase::Sent | Phase::Unknown | Phase::Absent => true,
            Phase::Cleared => false,
            Phase::Done => !settled(&self.status, self.tif.as_deref()) && self.status != "not_sent",
        }
    }
    fn json(&self, cid: &str) -> String {
        order_obj(
            cid,
            self.broker_id.as_deref(),
            &self.status,
            self.filled,
            self.avg.as_deref(),
            self.legs.as_deref(),
            self.sha.as_deref(),
            self.raw.as_deref(),
        )
    }
}

/// Whether a broker status ends an order of this time in force. `done_for_day` ends a day
/// order only: a good-til-canceled or closing order in it may still be live, and an unknown
/// time in force fails closed. Stopped and suspended never end an order.
fn settled(status: &str, tif: Option<&str>) -> bool {
    match status {
        "done_for_day" => tif == Some("day"),
        s => terminal(s),
    }
}

#[allow(clippy::too_many_arguments)]
fn order_obj(
    cid: &str,
    bid: Option<&str>,
    status: &str,
    filled: u64,
    avg: Option<&str>,
    legs: Option<&str>,
    sha: Option<&str>,
    raw: Option<&str>,
) -> String {
    let mut o = Obj::new()
        .s("client_order_id", cid)
        .opt_s("broker_order_id", bid)
        .s("status", status)
        .n("filled_qty", filled)
        .opt_s("avg_price", avg);
    if let Some(l) = legs {
        o = o.raw("legs", l);
    }
    o.opt_s("intent_sha256", sha)
        .raw("broker_raw", raw.unwrap_or("null"))
        .done()
}

enum Aud {
    All,
    Client(String),
    Prefix(String),
}

struct Pending {
    cid: String,
    /// When its outcome became unknown, ns on the rate clock.
    start: u64,
    next: usize,
    /// When its last lookup ran, ns on the rate clock.
    last: Option<u64>,
}

/// One broker call of a reconcile, run in order.
enum Step {
    /// The account gate. Passing it queues the lookups below.
    Gate,
    /// A by-id lookup of an open order.
    Order(String),
    /// The recent-orders listing.
    Recent,
    /// The positions read.
    Positions,
}

/// What a reconcile step did: a broker call, a closed gate after its call, or nothing (a
/// refused token, or no reconcile).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Turn {
    Called,
    Gated,
    Waiting,
}

/// A reconcile in progress. Its calls run one per tick, so a slow broker holds a tick for
/// one call at most; a refused token leaves its call for a later tick.
struct Reconcile {
    steps: VecDeque<Step>,
    from_ts: u64,
    checked: u64,
    /// A startup reconcile journals why its gate is closed, once.
    announce: bool,
    startup: bool,
    /// A reconcile was requested while this one ran: it runs again after, from fresh state.
    /// One flag, so requests during a long reconcile never queue up.
    again: bool,
}

/// The longest wait between two by-id lookups of one unresolved order. It bounds the
/// broker call rate per order and decides nothing about the order.
const RECHECK_CAP: Duration = Duration::from_secs(30);

/// The backoff of one unresolved order's poll lookups: due at `due`, after `interval`.
struct Recheck {
    /// ns on the rate clock.
    due: u64,
    interval: Duration,
}

/// Schedules the next try of a failed order: one poll after the first failure, then twice
/// the last interval, up to `RECHECK_CAP` (or the poll, if that is longer).
fn backoff(map: &mut HashMap<String, Recheck>, cid: &str, now: u64, poll: Duration) {
    let interval = match map.get(cid) {
        Some(r) => (r.interval * 2).min(RECHECK_CAP.max(poll)),
        None => poll,
    };
    map.insert(
        cid.to_owned(),
        Recheck {
            due: now.saturating_add(ns(interval)),
            interval,
        },
    );
}

/// The open-listing check of an automatic clear, kept for its by-id check on a later poll.
struct ClearHalf {
    cid: String,
    open: Seen,
    listed: usize,
}

/// The broker results a clear is decided on.
struct Checks {
    open: Seen,
    listed: usize,
    by_id: Seen,
    raw: String,
    found: Option<BrokerOrder>,
}

/// One unknown-outcome lookup. Only `NotFound` (a 404 answer) says the broker
/// does not have the order; `Failed` says nothing about it.
enum Lookup {
    Found(BrokerOrder),
    NotFound,
    Failed,
}

fn outcome(kind: &str) -> Obj {
    Obj::new().s("outcome", kind)
}

fn not_sent(reason: &str, detail: &str) -> String {
    outcome("not_sent")
        .s("reason", reason)
        .raw("detail", detail)
        .done()
}

fn cap(name: &str) -> String {
    not_sent("cap", &Obj::new().s("cap", name).done())
}

/// Whether a close of `requested` on one leg may be sent. `held` is the broker's signed
/// position (long positive), `None` when it could not be read. A sell closes only a long
/// and a buy only a short, and never more than is held.
fn close_check(held: Option<i64>, requested: u64, buy: bool) -> Result<(), &'static str> {
    let held = i128::from(held.ok_or("position_unknown")?);
    let closable = if buy { -held } else { held };
    if closable >= i128::from(requested) {
        Ok(())
    } else {
        Err("close_exceeds_position")
    }
}

/// An order is past its session once its New York trading date has moved past the date
/// it was submitted. Only a journaled `day` or `cls` order qualifies: good-til-canceled
/// and an unknown time-in-force are never auto-cleared.
pub(crate) fn session_order_expired(tif: Option<&str>, submitted: Option<u64>, now: u64) -> bool {
    match (tif, submitted) {
        (Some("day" | "cls"), Some(at)) => et_day(now) > et_day(at),
        _ => false,
    }
}

/// Whether an open-orders listing of `len` entries may be cut short by its limit. The
/// listing has no paging, so a full page can hide orders.
pub(crate) fn listing_full(len: usize) -> bool {
    len >= OPEN_ORDERS_LIMIT
}

/// One broker check of a client order id: the status when the broker shows the order,
/// a clean absence, the failure's code, or a listing that is too full to rule the order
/// out.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Seen {
    Absent,
    Shows(String),
    Failed(String),
    Unreliable(String),
}

/// Why a clear is refused. `check` names the broker call: `open_orders` or `by_id`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Refusal {
    Shows { check: &'static str, status: String },
    Unreachable { check: &'static str, code: String },
}

/// Clears only when neither check shows the order and both answered. A shown order
/// refuses whichever check saw it, so a failed check never hides one; a failure refuses
/// only when nothing is shown.
pub(crate) fn clear_verdict(open: &Seen, by_id: &Seen) -> Result<(), Refusal> {
    let shows = |check, seen: &Seen| match seen {
        Seen::Shows(status) => Some(Refusal::Shows {
            check,
            status: status.clone(),
        }),
        _ => None,
    };
    if let Some(r) = shows("open_orders", open).or_else(|| shows("by_id", by_id)) {
        return Err(r);
    }
    for (check, seen) in [("open_orders", open), ("by_id", by_id)] {
        if let Seen::Failed(code) | Seen::Unreliable(code) = seen {
            return Err(Refusal::Unreachable {
                check,
                code: code.clone(),
            });
        }
    }
    Ok(())
}

fn not_cleared(reason: &str, detail: &str) -> String {
    outcome("not_cleared")
        .s("reason", reason)
        .raw("detail", detail)
        .done()
}

/// The refusal's reason and detail, as a `not_cleared` outcome and a journal line carry them.
fn refusal_parts(r: &Refusal) -> (&'static str, String) {
    match r {
        Refusal::Shows { check, status } => (
            "broker_shows_order",
            Obj::new().s("check", check).s("status", status).done(),
        ),
        Refusal::Unreachable { check, code } => (
            "broker_unreachable",
            Obj::new().s("check", check).s("code", code).done(),
        ),
    }
}

/// A clear that was not made: the outcome's reason and detail, and the broker results
/// it was decided on, when the broker was consulted.
struct Refused {
    reason: &'static str,
    detail: String,
    broker: Option<String>,
}

impl Refused {
    fn plain(reason: &'static str) -> Self {
        Self {
            reason,
            detail: "null".into(),
            broker: None,
        }
    }
}

/// The two broker checks of a refused clear, as the journal records them.
fn broker_checks(open: &Seen, listed: usize, by_id: &Seen, raw: &str) -> String {
    let check = |seen: &Seen| match seen {
        Seen::Absent => Obj::new().s("result", "absent"),
        Seen::Shows(status) => Obj::new().s("result", "shows").s("status", status),
        Seen::Unreliable(code) => Obj::new().s("result", "unreliable").s("code", code),
        Seen::Failed(code) => Obj::new().s("result", "failed").s("code", code),
    };
    Obj::new()
        .raw("open_orders", &check(open).n("listed", listed).done())
        .raw("by_id", &check(by_id).raw("broker_raw", raw).done())
        .done()
}

fn broker_error_obj(kind: &str, e: &BrokerError) -> Obj {
    outcome(kind)
        .n("http_status", e.http_status)
        .s("code", &e.code)
        .s("message", &e.message)
        .raw("broker_raw", &e.raw)
}

fn op_name(op: &Op) -> &'static str {
    match op {
        Op::Submit(_) => "submit",
        Op::Cancel(_) => "cancel",
        Op::Order(_) => "order",
        Op::OpenOrders => "open_orders",
        Op::Account => "account",
        Op::Positions => "positions",
        Op::Activities { .. } => "activities",
        Op::Clock => "clock",
        Op::Calendar { .. } => "calendar",
        Op::Kill { .. } => "kill",
        Op::Unkill => "unkill",
        Op::Flatten { .. } => "flatten",
        Op::ClearOrder { .. } => "clear_order",
        Op::Advance(_) => "advance",
    }
}

pub(crate) struct Session<B: Broker> {
    b: B,
    j: Journal,
    cfg: Config,
    orders: HashMap<String, OrderState>,
    by_broker: HashMap<String, String>,
    foreign: HashSet<String>,
    conns: Vec<Conn>,
    hold: Option<Vec<(String, Aud)>>,
    gate_ok: bool,
    killed: Option<(KillLevel, String)>,
    stream_down: Option<(Instant, u64)>,
    degraded: bool,
    last_state: &'static str,
    budget: Budget,
    /// Opens and closes submitted in the last minute, ns on the rate clock.
    minute: VecDeque<u64>,
    closes: VecDeque<u64>,
    /// The rate clock's origin live.
    rate_base: Instant,
    day: (i64, usize),
    rejects: u32,
    pending: Vec<Pending>,
    /// Poll lookups of unresolved orders, by client order id. An order without an entry
    /// has not failed a poll lookup yet, so it is due.
    rechecks: HashMap<String, Recheck>,
    /// The same backoff for automatic clears, which a refused clear feeds. Kept apart so a
    /// lookup's backoff never delays a clear of an order past its session.
    clear_retry: HashMap<String, Recheck>,
    /// The open-listing half of an automatic clear, kept until its by-id half runs on a later
    /// clear turn.
    clear_half: Option<ClearHalf>,
    /// Whose turn it is to go first on a poll: the recheck or the automatic clear.
    clear_turn: bool,
    /// The client order id of the last poll lookup or automatic clear, for round-robin.
    cursor: Option<String>,
    cod_timers: Vec<(String, Instant)>,
    /// Cancel-on-disconnect targets still to cancel, as (client, client order id, broker id),
    /// in stable order: one cancel per tick.
    cod_work: VecDeque<(String, String, String)>,
    /// The reconcile in progress, if any.
    reconcile: Option<Reconcile>,
    /// The last poll, ns on the rate clock.
    last_poll: u64,
    /// The last check for the kill sentinel under a session, whose polls follow the
    /// market clock and stop while it is paused.
    last_kill_check: Instant,
    last_seq: Arc<AtomicU64>,
    /// Order messages (submits and cancels, valid or not) handled per client, by its
    /// index in the configuration, for clocks.
    handled: HashMap<usize, u64>,
    /// Clock advances waiting for requests, in arrival order.
    clocks: VecDeque<ClockWait>,
    /// Order events journaled (submit, cancel, order, activity).
    order_events: u64,
}

impl<B: Broker> Session<B> {
    pub(crate) fn new(
        cfg: Config,
        b: B,
        j: Journal,
        recovered: &[Value],
        last_seq: Arc<AtomicU64>,
    ) -> Self {
        let mut s = Self {
            budget: Budget::new(cfg.profile.budget_per_min),
            b,
            j,
            cfg,
            orders: HashMap::new(),
            by_broker: HashMap::new(),
            foreign: HashSet::new(),
            conns: Vec::new(),
            hold: None,
            gate_ok: false,
            killed: None,
            stream_down: None,
            degraded: false,
            last_state: "",
            minute: VecDeque::new(),
            rate_base: Instant::now(),
            closes: VecDeque::new(),
            day: (0, 0),
            rejects: 0,
            pending: Vec::new(),
            rechecks: HashMap::new(),
            clear_retry: HashMap::new(),
            clear_half: None,
            clear_turn: false,
            cursor: None,
            cod_timers: Vec::new(),
            cod_work: VecDeque::new(),
            reconcile: None,
            last_poll: RATE_ORIGIN,
            last_kill_check: Instant::now(),
            last_seq,
            handled: HashMap::new(),
            clocks: VecDeque::new(),
            order_events: 0,
        };
        let today = utc_day(s.ts());
        for r in recovered {
            s.rebuild(r, today);
        }
        s
    }

    fn rebuild(&mut self, v: &Value, today: i64) {
        let text = |k: &str| v[k].as_str().map(String::from);
        let cid = text("client_order_id").unwrap_or_default();
        match v["kind"].as_str().unwrap_or("") {
            "submit" => match v["phase"].as_str().unwrap_or("") {
                "admitted" => {
                    let day = utc_day(v["ts_gateway"].as_u64().unwrap_or(0));
                    if journal_opens(v) && day == today {
                        self.day = (today, self.day.1 + 1);
                    }
                    let (c, r) = (text("client").unwrap_or_default(), text("request_id"));
                    let symbols = journal_symbols(v);
                    let sha = text("intent_sha256");
                    let submitted = v["ts_gateway"].as_u64();
                    let st = OrderState::admitted(
                        c,
                        r.unwrap_or_default(),
                        sha,
                        symbols,
                        journal_tif(v),
                        submitted,
                    );
                    self.orders.insert(cid, st);
                }
                phase => {
                    let Some(st) = self.orders.get_mut(&cid) else {
                        return;
                    };
                    if text("request_id").as_deref() != Some(&st.request_id) {
                        return;
                    }
                    let o = &v["outcome"];
                    match (phase, o["outcome"].as_str().unwrap_or("")) {
                        ("sent", _) => st.phase = Phase::Sent,
                        (_, "accepted") => {
                            st.phase = Phase::Done;
                            st.broker_id = o["broker_order_id"].as_str().map(String::from);
                            st.status = o["status"].as_str().unwrap_or("new").into();
                            st.raw = Some(o["broker_raw"].to_string());
                        }
                        (_, "unknown") => st.phase = Phase::Unknown,
                        (_, kind @ ("rejected" | "not_sent")) => {
                            st.phase = Phase::Done;
                            kind.clone_into(&mut st.status);
                        }
                        _ => {}
                    }
                    if let Some(b) = st.broker_id.clone() {
                        self.by_broker.insert(b, cid);
                    }
                }
            },
            "order" => {
                let Some(st) = self.orders.get_mut(&cid) else {
                    return;
                };
                st.phase = Phase::Done;
                st.broker_id = text("broker_order_id");
                st.status = text("status").unwrap_or_default();
                st.filled = v["filled_qty"].as_u64().unwrap_or(0);
                st.avg = text("avg_price");
                st.legs = v.get("legs").map(Value::to_string);
                st.raw = Some(v["broker_raw"].to_string());
                if let Some(b) = st.broker_id.clone() {
                    self.by_broker.insert(b, cid);
                }
            }
            "cleared" => {
                if let Some(st) = self.orders.get_mut(&cid) {
                    st.phase = Phase::Cleared;
                }
            }
            // `resolution` is not restored. An unknown order stays Unknown across a
            // restart and is looked up again, so an old `absent_after` (written
            // before a lookup error was told apart from a 404) cannot make an
            // order absent.
            "status" => {
                if let Some(k) = v.get("kill") {
                    self.killed = KillLevel::parse(k["level"].as_str().unwrap_or(""))
                        .map(|l| (l, k["reason"].as_str().unwrap_or("").to_string()));
                }
            }
            "foreign" => {
                self.foreign.extend(text("broker_order_id"));
                self.foreign
                    .extend(text("symbol").map(|s| format!("position:{s}")));
            }
            _ => {}
        }
    }

    fn ts(&self) -> u64 {
        self.b.now().unwrap_or_else(wall_ns)
    }

    /// The clock the rate limits run on, ns: the market time when a session clocks
    /// the broker (a backtest replays many minutes a minute, and must run the same
    /// whatever the machine's load), and otherwise a monotonic clock.
    fn rate_now(&self) -> u64 {
        if self.b.session_clocked() {
            self.ts()
        } else {
            ns(self.rate_base.elapsed()).saturating_add(RATE_ORIGIN)
        }
    }

    fn state(&self) -> &'static str {
        if !self.gate_ok {
            "reconciling"
        } else if self.killed.is_some() {
            "killed"
        } else if self.degraded {
            "degraded"
        } else {
            "ready"
        }
    }

    fn emit(&mut self, ts: u64, kind: &str, body: &str, aud: Aud) -> Result<(), String> {
        let (seq, line) = self.j.append(ts, kind, body)?;
        if matches!(kind, "submit" | "cancel" | "order" | "activity") {
            self.order_events += 1;
        }
        self.last_seq.store(seq, SeqCst);
        match &mut self.hold {
            Some(h) => h.push((line, aud)),
            None => self.fan(&line, &aud),
        }
        Ok(())
    }

    fn visible(&self, client: usize, aud: &Aud) -> bool {
        let c = &self.cfg.clients[client];
        c.role != Role::Trade
            || match aud {
                Aud::All => true,
                Aud::Client(n) => *n == c.name,
                Aud::Prefix(cid) => cid.starts_with(&c.prefix),
            }
    }

    fn fan(&self, line: &str, aud: &Aud) {
        for c in &self.conns {
            if self.visible(c.client, aud) {
                c.outbox.push(line.to_owned());
            }
        }
    }

    /// Delivers a response, then the events held while producing it.
    fn release(&mut self, conn: usize, line: String) {
        self.conns[conn].outbox.push(line);
        for (l, a) in self.hold.take().unwrap_or_default() {
            self.fan(&l, &a);
        }
    }

    fn reply(&self, conn: usize, line: String) {
        self.conns[conn].outbox.push(line);
    }

    fn status(&mut self, extra: &str) -> Result<(), String> {
        let stream = match self.stream_down {
            Some((_, since)) => Obj::new()
                .raw("down", &Obj::new().n("since", since).done())
                .done(),
            None => quote("up"),
        };
        self.last_state = self.state();
        let body = Obj::new()
            .s("state", self.last_state)
            .raw("broker_stream", &stream)
            .merge(extra)
            .done();
        self.emit(self.ts(), "status", &body, Aud::All)
    }

    fn sync_state(&mut self) -> Result<(), String> {
        if self.state() == self.last_state {
            Ok(())
        } else {
            self.status("{}")
        }
    }

    pub(crate) fn run(mut self, rx: &Receiver<Input>) -> Result<(), String> {
        let result = self.run_inner(rx);
        let msg = match &result {
            Ok(()) => "the process is stopping".to_string(),
            Err(e) => format!("fatal: {e}"),
        };
        for c in &self.conns {
            c.outbox.close(Some(error_line("shutting_down", &msg)));
        }
        result
    }

    fn run_inner(&mut self, rx: &Receiver<Input>) -> Result<(), String> {
        self.reconcile(true)?;
        self.run_startup_reconcile()?;
        loop {
            match rx.recv_timeout(Duration::from_millis(20)) {
                Ok(Input::Shutdown) | Err(RecvTimeoutError::Disconnected) => return Ok(()),
                Ok(Input::Hello {
                    id,
                    client,
                    outbox,
                    resume,
                    cancel_on_disconnect,
                }) => {
                    self.hello(id, client, outbox, resume, cancel_on_disconnect)?;
                }
                Ok(Input::Request { id, request }) => {
                    let name = match &request {
                        Ok((_, op)) => op_name(op),
                        Err((_, op, _)) => op.as_str(),
                    };
                    if matches!(name, "submit" | "cancel")
                        && let Some(c) = self.conns.iter().find(|c| c.id == id)
                    {
                        *self.handled.entry(c.client).or_default() += 1;
                    }
                    self.request(id, request)?;
                }
                Ok(Input::Closed(id)) => self.closed(id)?,
                Ok(Input::Clock {
                    client,
                    orders,
                    to,
                    deadline,
                    reply,
                }) => match self.cfg.clients.iter().position(|c| c.name == client) {
                    Some(client) => self.clocks.push_back(ClockWait {
                        client,
                        orders,
                        to,
                        deadline,
                        reply,
                    }),
                    None => {
                        let _ = reply.send(Err(format!("no client {client}")));
                    }
                },
                Err(RecvTimeoutError::Timeout) => {}
            }
            self.clock()?;
            self.tick()?;
        }
    }

    /// Runs the waiting clock advances whose order messages have all been handled, in
    /// order. One past its deadline is answered with an error and dropped, so it never
    /// holds up the ones behind it nor runs after its caller has given up.
    fn clock(&mut self) -> Result<(), String> {
        let now = Instant::now();
        let handled = &self.handled;
        self.clocks.retain(|w| {
            // A wait whose order messages have all arrived runs, whatever the time.
            if now < w.deadline || handled.get(&w.client).copied().unwrap_or(0) >= w.orders {
                return true;
            }
            let _ = w.reply.send(Err(format!(
                "no advance to {}: waited for {} order messages, {} arrived",
                w.to,
                w.orders,
                handled.get(&w.client).copied().unwrap_or(0)
            )));
            false
        });
        while let Some(w) = self.clocks.front() {
            if self.handled.get(&w.client).copied().unwrap_or(0) < w.orders {
                return Ok(());
            }
            let Some(w) = self.clocks.pop_front() else {
                break;
            };
            let answer = match self.b.advance(w.to) {
                Ok(market_ts) => {
                    self.drain()?;
                    Ok(Advanced {
                        market_ts,
                        last_seq: self.last_seq.load(SeqCst),
                        handled: self.handled.get(&w.client).copied().unwrap_or(0),
                        order_events: self.order_events,
                    })
                }
                Err(e) => Err(e.message),
            };
            let _ = w.reply.send(answer);
        }
        Ok(())
    }

    /// Runs the startup reconcile before the first request, so the gate opens as soon as the
    /// checks allow. It stops at a closed gate or a refused token; the rest runs on ticks.
    fn run_startup_reconcile(&mut self) -> Result<(), String> {
        while self.reconcile.as_ref().is_some_and(|rc| rc.startup)
            && self.reconcile_step()? == Turn::Called
        {}
        Ok(())
    }

    fn hello(
        &mut self,
        id: u64,
        client: usize,
        outbox: Arc<Outbox>,
        resume: Option<u64>,
        cod: bool,
    ) -> Result<(), String> {
        let (resume_obj, lines) = match resume {
            None => (Obj::new().s("resume", "none").done(), Vec::new()),
            Some(n) => {
                let (first, lines) = self.j.window()?;
                let o = if n + 1 < first {
                    Obj::new()
                        .s("resume", "truncated")
                        .n("first_available_seq", first)
                        .done()
                } else {
                    Obj::new().s("resume", "full").done()
                };
                (o, lines)
            }
        };
        let caps = self.b.capabilities();
        let p = &self.cfg.profile;
        let profile = p.json();
        let capabilities = Obj::new()
            .raw(
                "shapes",
                if caps.combo {
                    r#"["single","combo"]"#
                } else {
                    r#"["single"]"#
                },
            )
            .n("max_legs", caps.max_legs.min(p.max_legs))
            .s("tick", "0.01")
            .n("client_order_id_max", caps.client_order_id_max.min(128))
            .s("idempotency", caps.idempotency)
            .raw("events", EVENTS)
            .done();
        let pins = Obj::new()
            .s(
                "gateway",
                concat!("order-gateway ", env!("CARGO_PKG_VERSION")),
            )
            .s("profile_sha256", &hex(&Sha256::digest(profile.as_bytes())))
            .raw("broker", &self.b.pins())
            .done();
        let welcome = Obj::msg("welcome")
            .s("account", &self.cfg.account)
            .s("broker", self.b.kind())
            .s("environment", &self.cfg.environment)
            .s("state", self.state())
            .n("last_seq", self.j.last_seq)
            .merge(&resume_obj)
            .raw("capabilities", &capabilities)
            .raw("pins", &pins)
            .raw("limits", &profile)
            .done();
        let after = resume.unwrap_or(u64::MAX);
        let mut batch = vec![welcome];
        batch.extend(lines.into_iter().filter(|line| {
            let v: Value = serde_json::from_str(line).unwrap_or_default();
            v["seq"].as_u64().is_some_and(|s| s > after) && self.visible(client, &line_aud(&v))
        }));
        outbox.push_all(batch);
        let name = self.cfg.clients[client].name.clone();
        self.cod_timers.retain(|(n, _)| *n != name);
        self.conns.push(Conn {
            id,
            client,
            outbox,
            rids: HashSet::new(),
            cod,
        });
        let body = Obj::new()
            .s("client", &name)
            .s("phase", "connected")
            .raw(
                "resume_after_seq",
                &resume.map_or("null".into(), |n| n.to_string()),
            )
            .done();
        self.emit(self.ts(), "client", &body, Aud::Client(name))
    }

    fn closed(&mut self, id: u64) -> Result<(), String> {
        let Some(i) = self.conns.iter().position(|c| c.id == id) else {
            return Ok(());
        };
        let c = self.conns.remove(i);
        let name = self.cfg.clients[c.client].name.clone();
        if c.cod {
            self.cod_timers.push((
                name.clone(),
                Instant::now() + self.cfg.timing.disconnect_grace,
            ));
        }
        let body = Obj::new()
            .s("client", &name)
            .s("phase", "disconnected")
            .raw("resume_after_seq", "null")
            .done();
        self.emit(self.ts(), "client", &body, Aud::Client(name))
    }

    fn request(&mut self, id: u64, request: Parsed) -> Result<(), String> {
        let Some(ci) = self.conns.iter().position(|c| c.id == id) else {
            return Ok(());
        };
        let (rid, op) = match request {
            Ok(x) => x,
            Err((rid, op, e)) => {
                let line = request_error(
                    rid.as_deref().unwrap_or(""),
                    &op,
                    "invalid_request",
                    &invalid_detail(&e),
                );
                self.reply(ci, line);
                return Ok(());
            }
        };
        let name = op_name(&op);
        if !self.conns[ci].rids.insert(rid.clone()) {
            self.reply(
                ci,
                request_error(&rid, name, "duplicate_request_id", "null"),
            );
            return Ok(());
        }
        let spec = self.cfg.clients[self.conns[ci].client].clone();
        let allowed = match &op {
            Op::Submit(_) | Op::Advance(_) => spec.role == Role::Trade,
            Op::Cancel(_) => spec.role != Role::Observe,
            Op::Kill { .. } | Op::Unkill | Op::Flatten { .. } | Op::ClearOrder { .. } => {
                spec.role == Role::Admin
            }
            _ => true,
        };
        if !allowed {
            let line = request_error(&rid, name, "forbidden", "null");
            if let Op::ClearOrder {
                client_order_id,
                reason,
            } = &op
            {
                self.hold = Some(Vec::new());
                let journaled = self.refuse(
                    &spec.name,
                    client_order_id,
                    reason,
                    &Refused::plain("forbidden"),
                );
                self.release(ci, line);
                journaled?;
            } else {
                self.reply(ci, line);
            }
            return Ok(());
        }
        match op {
            Op::Submit(v) => {
                self.hold = Some(Vec::new());
                let result = self.submit(&spec.name, &spec.prefix, &rid, v);
                self.release(ci, response(&rid, "submit", &result?));
            }
            Op::Cancel(target) => {
                self.hold = Some(Vec::new());
                let result =
                    self.cancel_request(&spec.name, spec.role, &spec.prefix, &rid, &target);
                let line = match result? {
                    Ok(o) => response(&rid, "cancel", &o),
                    Err(code) => request_error(&rid, "cancel", code, "null"),
                };
                self.release(ci, line);
            }
            Op::Kill { level, reason } => {
                self.kill(level, &reason, &spec.name, &rid)?;
                let o = outcome("ok").s("state", self.state()).done();
                self.reply(ci, response(&rid, name, &o));
            }
            Op::Flatten { reason } => {
                self.hold = Some(Vec::new());
                let result = self.flatten(&spec.name, &rid, &reason);
                self.release(ci, response(&rid, name, &result?));
            }
            Op::ClearOrder {
                client_order_id,
                reason,
            } => {
                self.hold = Some(Vec::new());
                if reason.trim().is_empty() {
                    let detail = invalid_detail(&invalid("reason", "a non-empty reason"));
                    let refusal = Refused {
                        reason: "invalid_request",
                        detail: detail.clone(),
                        broker: None,
                    };
                    let journaled = self.refuse(&spec.name, &client_order_id, &reason, &refusal);
                    self.release(ci, request_error(&rid, name, "invalid_request", &detail));
                    journaled?;
                } else {
                    let result = self.clear_request(&spec.name, &client_order_id, &reason);
                    self.release(ci, response(&rid, name, &result?));
                }
            }
            Op::Unkill => {
                self.killed = None;
                self.status(r#"{"kill":null}"#)?;
                let o = outcome("ok").s("state", self.state()).done();
                self.reply(ci, response(&rid, name, &o));
            }
            Op::Advance(_) if self.b.session_clocked() => {
                // Time belongs to the session that clocks this simulator.
                let line = request_error(&rid, name, "clocked_by_session", "null");
                self.reply(ci, line);
            }
            Op::Advance(to) => {
                let line = match self.b.advance(to) {
                    Ok(m) => {
                        self.drain()?;
                        let o = outcome("ok")
                            .n("market_ts", m)
                            .n("ts_gateway", self.ts())
                            .raw("broker_raw", "null");
                        response(&rid, name, &o.done())
                    }
                    Err(e) => request_error(&rid, name, &e.code, "null"),
                };
                self.reply(ci, line);
            }
            query => {
                let line = self.query(&rid, query);
                self.reply(ci, line);
            }
        }
        Ok(())
    }

    #[expect(clippy::too_many_lines, reason = "one flat table of query mappings")]
    fn query(&mut self, rid: &str, op: Op) -> String {
        let name = op_name(&op);
        if !self.budget.at(self.rate_now()).take(false) {
            return request_error(rid, name, "throttled", "null");
        }
        let order = |o: &BrokerOrder, orders: &HashMap<String, OrderState>| {
            let sha = orders
                .get(&o.client_order_id)
                .and_then(|s| s.sha.as_deref());
            order_obj(
                &o.client_order_id,
                Some(&o.broker_order_id),
                &o.status,
                o.filled_qty,
                o.avg_price.as_deref(),
                o.legs.as_deref(),
                sha,
                Some(&o.raw),
            )
        };
        let result: Result<(String, String), BrokerError> = match op {
            Op::Order(t) => self.b.order(&t).map(|r| {
                let o = r
                    .value
                    .as_ref()
                    .map_or("null".into(), |o| order(o, &self.orders));
                (Obj::new().raw("order", &o).done(), r.raw)
            }),
            Op::OpenOrders => self.b.open_orders().map(|r| {
                let list = arr(r.value.iter().map(|o| order(o, &self.orders)));
                let unresolved = self
                    .orders
                    .values()
                    .filter(|s| matches!(s.phase, Phase::Unknown | Phase::Absent))
                    .count();
                let o = Obj::new().raw("orders", &list).n("unresolved", unresolved);
                (o.done(), r.raw)
            }),
            Op::Account => self.b.account().map(|r| {
                let a = &r.value;
                let o = Obj::new()
                    .s("status", &a.status)
                    .s("cash", &a.cash)
                    .s("buying_power", &a.buying_power)
                    .n("options_trading_level", a.options_trading_level)
                    .b("blocked", a.blocked)
                    .done();
                (Obj::new().raw("account", &o).done(), r.raw)
            }),
            Op::Positions => self.b.positions().map(|r| {
                let list = arr(r.value.iter().map(|p| {
                    Obj::new()
                        .s("symbol", &p.symbol)
                        .n("qty", p.qty)
                        .s("avg_entry_price", &p.avg_entry_price)
                        .done()
                }));
                (Obj::new().raw("positions", &list).done(), r.raw)
            }),
            Op::Activities {
                types,
                after,
                page_token,
            } => self
                .b
                .activities(&types, after.as_deref(), page_token.as_deref())
                .map(|r| {
                    let o = Obj::new()
                        .raw("items", &arr(r.value.0))
                        .opt_s("next_page_token", r.value.1.as_deref());
                    (o.done(), r.raw)
                }),
            Op::Clock => self.b.clock().map(|r| {
                let c = r.value;
                let o = Obj::new()
                    .n("ts", c.ts)
                    .b("is_open", c.is_open)
                    .n("next_open", c.next_open)
                    .n("next_close", c.next_close);
                (Obj::new().raw("clock", &o.done()).done(), r.raw)
            }),
            Op::Calendar { start, end } => self.b.calendar(&start, &end).map(|r| {
                let days = arr(r.value.iter().map(|d| {
                    Obj::new()
                        .s("date", &d.date)
                        .s("open", &d.open)
                        .s("close", &d.close)
                        .done()
                }));
                (Obj::new().raw("days", &days).done(), r.raw)
            }),
            _ => Err(BrokerError::local("unsupported", "")),
        };
        match result {
            Ok((fields, raw)) => {
                let o = outcome("ok")
                    .merge(&fields)
                    .n("ts_gateway", self.ts())
                    .raw("broker_raw", &raw);
                response(rid, name, &o.done())
            }
            Err(e) => {
                if e.http_status == 429 {
                    self.budget.at(self.rate_now()).block(e.retry_after_ms);
                }
                let detail = Obj::new()
                    .n("http_status", e.http_status)
                    .s("code", &e.code)
                    .s("message", &e.message)
                    .done();
                let code = if e.http_status == 0 {
                    &e.code
                } else {
                    "broker_error"
                };
                request_error(rid, name, code, &detail)
            }
        }
    }

    fn submit_event(
        &mut self,
        client: &str,
        rid: &str,
        cid: Option<&str>,
        phase: &str,
        extra: &str,
    ) -> Result<(), String> {
        let body = Obj::new()
            .s("client", client)
            .s("request_id", rid)
            .opt_s("client_order_id", cid)
            .s("phase", phase)
            .merge(extra)
            .done();
        self.emit(self.ts(), "submit", &body, Aud::Client(client.to_owned()))
    }

    fn outcome_event(
        &mut self,
        client: &str,
        rid: &str,
        cid: Option<&str>,
        o: String,
    ) -> Result<String, String> {
        let extra = Obj::new().raw("outcome", &o).done();
        self.submit_event(client, rid, cid, "outcome", &extra)?;
        Ok(o)
    }

    /// Returns the outcome object; the response and the outcome event share it.
    fn submit(
        &mut self,
        client: &str,
        prefix: &str,
        rid: &str,
        v: Value,
    ) -> Result<String, String> {
        let raw_cid = v
            .get("client_order_id")
            .and_then(Value::as_str)
            .map(String::from);
        let intent = match Intent::parse(v) {
            Ok(i) => i,
            Err(e) => {
                let o = not_sent("validation", &invalid_detail(&e));
                return self.outcome_event(client, rid, raw_cid.as_deref(), o);
            }
        };
        if let Some(o) = self.refusal(client, prefix, &intent) {
            return self.outcome_event(client, rid, Some(&intent.client_order_id), o);
        }
        if let Some(o) = self.close_refusal(&intent) {
            return self.outcome_event(client, rid, Some(&intent.client_order_id), o);
        }
        self.forward(client, rid, &intent)
    }

    /// Each close leg against the broker's position, read once before the send. This
    /// does I/O, so it runs after `refusal`. A position that cannot be read refuses.
    fn close_refusal(&mut self, intent: &Intent) -> Option<String> {
        if intent.legs.iter().all(|l| l.open) {
            return None;
        }
        // A 429 backoff holds opens, not closes, so the read takes a close token.
        let read = if self.budget.at(self.rate_now()).take_close() {
            self.b.positions()
        } else {
            Err(BrokerError::local("throttled", ""))
        };
        for l in intent.legs.iter().filter(|l| !l.open) {
            let requested = u64::from(intent.qty) * u64::from(l.ratio);
            let base = Obj::new()
                .s("symbol", &l.symbol)
                .n("requested_qty", requested);
            let (held, detail) = match &read {
                Ok(r) => {
                    let held: i64 = r
                        .value
                        .iter()
                        .filter(|p| p.symbol == l.symbol)
                        .map(|p| p.qty)
                        .sum();
                    (Some(held), base.n("held_qty", held))
                }
                Err(e) => (None, base.s("code", &e.code)),
            };
            if let Err(reason) = close_check(held, requested, l.buy) {
                return Some(not_sent(reason, &detail.done()));
            }
        }
        None
    }

    /// Every check that runs before any I/O, in contract order.
    fn refusal(&mut self, client: &str, prefix: &str, intent: &Intent) -> Option<String> {
        let cid = &intent.client_order_id;
        if !cid.starts_with(prefix) {
            return Some(not_sent("prefix", &Obj::new().s("prefix", prefix).done()));
        }
        if let Some(st) = self.orders.get(cid) {
            return Some(if st.sha.as_deref() == Some(&intent.sha256()) {
                outcome("duplicate").raw("current", &st.json(cid)).done()
            } else {
                not_sent("duplicate_conflict", "null")
            });
        }
        let caps = self.b.capabilities();
        for l in &intent.legs {
            let p = &self.cfg.profile;
            if l.equity {
                if !p.equities.contains(&l.symbol) {
                    let detail = Obj::new().s("equity", &l.symbol).done();
                    return Some(not_sent("unsupported", &detail));
                }
                if l.open && !l.buy && !p.allow_short {
                    let detail = Obj::new().s("short_sale", &l.symbol).done();
                    return Some(not_sent("unsupported", &detail));
                }
                continue;
            }
            let root = occ(&l.symbol).map_or("", |(r, _)| r);
            if !p.underlyings.iter().any(|u| u == root)
                && !p.any_underlying_clients.iter().any(|c| c == client)
            {
                return Some(not_sent(
                    "unsupported",
                    &Obj::new().s("underlying", root).done(),
                ));
            }
        }
        if (intent.combo && !caps.combo)
            || intent.legs.len() > caps.max_legs
            || cid.len() > caps.client_order_id_max
        {
            return Some(not_sent(
                "unsupported",
                &Obj::new().s("broker", self.b.kind()).done(),
            ));
        }
        if !self.gate_ok || self.degraded {
            return Some(not_sent(
                "not_ready",
                &Obj::new().s("state", self.state()).done(),
            ));
        }
        if let Some((level, _)) = self.killed
            && (level == KillLevel::HaltAll || intent.has_open())
        {
            let detail = Obj::new().s("level", level.name()).done();
            return Some(not_sent("killed", &detail));
        }
        // A close waits until no order on its leg is still open or unresolved, so
        // two closes cannot both count the same position. Any such leg refuses the
        // whole intent. Opens are not checked here; the caps below cover them.
        let pending: Vec<String> = intent
            .legs
            .iter()
            .filter(|l| {
                !l.open
                    && self
                        .orders
                        .values()
                        .any(|o| o.open() && o.symbols.contains(&l.symbol))
            })
            .map(|l| quote(&l.symbol))
            .collect();
        if !pending.is_empty() {
            let detail = Obj::new().raw("symbols", &arr(pending)).done();
            return Some(not_sent("leg_pending", &detail));
        }
        let p = &self.cfg.profile;
        let opening = intent.has_open();
        // A market order's reference price does not bound its fill, so no cap can hold a
        // market opening. Refused for every client; a market close is not.
        if opening && intent.market {
            return Some(not_sent("market_opening_refused", "null"));
        }
        // An opening intent is capped at what it can lose; a close is never checked.
        let loss = if opening {
            max_loss(intent).map(|m| m.cents)
        } else {
            Ok(0)
        };
        let notional = loss.unwrap_or(0);
        let open = self.orders.values().filter(|o| o.open()).count();
        let minute_ago = self.rate_now().checked_sub(60_000_000_000);
        self.minute.retain(|t| minute_ago.is_none_or(|m| *t > m));
        self.closes.retain(|t| minute_ago.is_none_or(|m| *t > m));
        let today = utc_day(self.ts());
        if self.day.0 != today {
            self.day = (today, 0);
        }
        // What an order may risk, how many may work at once, and how many opens
        // may be submitted bound what opens: an order that only closes is never
        // kept from closing by them. Closes have their own rate limit, counted
        // apart from the opens.
        let hit = [
            (intent.legs.len() > p.max_legs, "legs"),
            (
                opening && notional > p.max_notional_cents.unsigned_abs(),
                "notional",
            ),
            (opening && open >= p.max_open, "open_orders"),
            (
                opening && self.minute.len() >= p.submits_per_min,
                "submits_per_minute",
            ),
            (
                opening && self.day.1 >= p.submits_per_day,
                "submits_per_day",
            ),
            (
                !opening && self.closes.len() >= p.closes_per_min,
                "closes_per_minute",
            ),
        ];
        if let Some((_, name)) = hit.iter().find(|(h, _)| *h) {
            return Some(cap(name));
        }
        if let Err(reason) = loss {
            let symbols = arr(intent.legs.iter().map(|l| quote(&l.symbol)));
            return Some(not_sent(
                reason,
                &Obj::new().raw("symbols", &symbols).done(),
            ));
        }
        let admitted = if opening {
            self.budget.at(self.rate_now()).take(true)
        } else {
            self.budget.at(self.rate_now()).take_close()
        };
        if !admitted {
            return Some(not_sent("throttled", "null"));
        }
        None
    }

    /// Admits, journals `sent`, makes the one send and journals the outcome.
    fn forward(&mut self, client: &str, rid: &str, intent: &Intent) -> Result<String, String> {
        let cid = intent.client_order_id.clone();
        let sha = intent.sha256();
        let symbols = intent.legs.iter().map(|l| l.symbol.clone()).collect();
        let tif = Some(if intent.on_close { "cls" } else { "day" }.to_owned());
        let st = OrderState::admitted(
            client.into(),
            rid.into(),
            Some(sha.clone()),
            symbols,
            tif,
            Some(self.ts()),
        );
        self.orders.insert(cid.clone(), st);
        if intent.has_open() {
            self.minute.push_back(self.rate_now());
            self.day.1 += 1;
        } else {
            self.closes.push_back(self.rate_now());
        }
        let extra = Obj::new()
            .raw("intent", &intent.canonical())
            .s("intent_sha256", &sha)
            .done();
        self.submit_event(client, rid, Some(&cid), "admitted", &extra)?;
        if self.cfg.stall_after_admitted {
            loop {
                std::thread::sleep(Duration::from_hours(1));
            }
        }
        self.submit_event(client, rid, Some(&cid), "sent", "{}")?;
        let result = self.b.submit(intent);
        let st = self.orders.get_mut(&cid).ok_or("admitted order")?;
        let o = match result {
            SubmitResult::Accepted(bo) => {
                self.rejects = 0;
                st.phase = Phase::Done;
                st.broker_id = Some(bo.broker_order_id.clone());
                st.status.clone_from(&bo.status);
                st.raw = Some(bo.raw.clone());
                self.by_broker
                    .insert(bo.broker_order_id.clone(), cid.clone());
                outcome("accepted")
                    .s("client_order_id", &cid)
                    .s("broker_order_id", &bo.broker_order_id)
                    .s("status", &bo.status)
                    .raw("broker_raw", &bo.raw)
                    .done()
            }
            SubmitResult::Rejected(e) => {
                // An order on a contract that has expired is the strategy's mistake about
                // the calendar, not a sign the broker is refusing it: it doesn't count
                // toward halting opens.
                if e.code != "sim_expired_instrument" {
                    self.rejects += 1;
                }
                st.phase = Phase::Done;
                "rejected".clone_into(&mut st.status);
                if e.http_status == 429 {
                    self.budget.at(self.rate_now()).block(e.retry_after_ms);
                }
                broker_error_obj("rejected", &e).done()
            }
            SubmitResult::Unknown {
                reason,
                retry_after_ms,
            } => {
                st.phase = Phase::Unknown;
                if reason == "http_429_after_send" {
                    self.budget.at(self.rate_now()).block(retry_after_ms);
                }
                self.pending.push(Pending {
                    cid: cid.clone(),
                    start: self.rate_now(),
                    next: 0,
                    last: None,
                });
                outcome("unknown")
                    .s("reason", reason)
                    .s("client_order_id", &cid)
                    .done()
            }
        };
        let o = self.outcome_event(client, rid, Some(&cid), o)?;
        if self.rejects >= self.cfg.profile.max_rejects && self.killed.is_none() {
            self.killed = Some((KillLevel::HaltOpens, "cap:consecutive_rejects".into()));
            let k = Obj::new()
                .s("level", "halt_opens")
                .s("reason", "cap:consecutive_rejects")
                .done();
            let c = Obj::new().s("name", "consecutive_rejects").done();
            self.status(&Obj::new().raw("kill", &k).raw("cap", &c).done())?;
        }
        Ok(o)
    }

    /// `Ok(Ok(outcome))`, or `Ok(Err(code))` for a request error.
    fn cancel_request(
        &mut self,
        client: &str,
        role: Role,
        prefix: &str,
        rid: &str,
        target: &OrderRef,
    ) -> Result<Result<String, &'static str>, String> {
        let known = match target {
            OrderRef::Client(c) => self
                .orders
                .get(c)
                .and_then(|s| s.broker_id.clone())
                .map(|b| (c.clone(), b)),
            OrderRef::Broker(b) => self.by_broker.get(b).map(|c| (c.clone(), b.clone())),
        };
        let (cid, bid) = if let Some(k) = known {
            k
        } else {
            if !self.budget.at(self.rate_now()).take(false) {
                return Ok(Err("throttled"));
            }
            match self.b.order(target).map(|r| r.value) {
                Ok(Some(o)) => (o.client_order_id, o.broker_order_id),
                Ok(None) => return Ok(Err("not_found")),
                Err(_) => return Ok(Err("broker_error")),
            }
        };
        if role == Role::Trade && !cid.starts_with(prefix) {
            return Ok(Err("forbidden"));
        }
        if !self.gate_ok {
            let o = not_sent("not_ready", &Obj::new().s("state", self.state()).done());
            self.cancel_event(
                client,
                rid,
                &cid,
                "outcome",
                &Obj::new().raw("outcome", &o).done(),
            )?;
            return Ok(Ok(o));
        }
        self.do_cancel(client, rid, &cid, &bid).map(Ok)
    }

    fn cancel_event(
        &mut self,
        client: &str,
        rid: &str,
        target: &str,
        phase: &str,
        extra: &str,
    ) -> Result<(), String> {
        let body = Obj::new()
            .s("client", client)
            .s("request_id", rid)
            .s("target", target)
            .s("phase", phase)
            .merge(extra)
            .done();
        self.emit(self.ts(), "cancel", &body, Aud::Client(client.to_owned()))
    }

    fn cancel_outcome(&mut self, r: CancelResult) -> String {
        match r {
            CancelResult::Accepted(raw) => outcome("accepted").raw("broker_raw", &raw).done(),
            CancelResult::Rejected(e) => {
                if e.http_status == 429 {
                    self.budget.at(self.rate_now()).block(e.retry_after_ms);
                }
                broker_error_obj("rejected", &e).done()
            }
            CancelResult::Unknown {
                reason,
                retry_after_ms,
            } => {
                if reason == "http_429_after_send" {
                    self.budget.at(self.rate_now()).block(retry_after_ms);
                }
                outcome("unknown").s("reason", reason).done()
            }
        }
    }

    fn do_cancel(
        &mut self,
        client: &str,
        rid: &str,
        cid: &str,
        bid: &str,
    ) -> Result<String, String> {
        let o = if self.budget.at(self.rate_now()).take(true) {
            self.cancel_event(client, rid, cid, "sent", "{}")?;
            let r = self.b.cancel(bid);
            self.cancel_outcome(r)
        } else {
            not_sent("throttled", "null")
        };
        self.cancel_event(
            client,
            rid,
            cid,
            "outcome",
            &Obj::new().raw("outcome", &o).done(),
        )?;
        Ok(o)
    }

    fn kill(
        &mut self,
        level: KillLevel,
        reason: &str,
        client: &str,
        rid: &str,
    ) -> Result<(), String> {
        self.killed = Some((level, reason.into()));
        let k = Obj::new()
            .s("level", level.name())
            .s("reason", reason)
            .done();
        self.status(&Obj::new().raw("kill", &k).done())?;
        if level == KillLevel::HaltAll {
            self.budget.at(self.rate_now()).charge();
            self.cancel_event(client, rid, "*", "sent", "{}")?;
            let r = self.b.cancel_all();
            let o = self.cancel_outcome(r);
            self.cancel_event(
                client,
                rid,
                "*",
                "outcome",
                &Obj::new().raw("outcome", &o).done(),
            )?;
            self.drain()?;
            self.budget.at(self.rate_now()).charge();
            if let Ok(r) = self.b.open_orders() {
                for bo in r.value {
                    self.do_cancel(client, rid, &bo.client_order_id, &bo.broker_order_id)?;
                }
            }
        }
        Ok(())
    }

    /// Halt opens (unless all is already halted), cancel every open order,
    /// then close every position at market through the broker, in one request
    /// that is never resent. Journaled as `flatten` events for the requesting
    /// admin (observers see every event).
    fn flatten(&mut self, client: &str, rid: &str, reason: &str) -> Result<String, String> {
        let event = |phase: &str| {
            Obj::new()
                .s("client", client)
                .s("request_id", rid)
                .s("phase", phase)
        };
        let aud = || Aud::Client(client.to_owned());
        self.emit(
            self.ts(),
            "flatten",
            &event("requested").s("reason", reason).done(),
            aud(),
        )?;
        if !matches!(self.killed, Some((KillLevel::HaltAll, _))) {
            self.kill(KillLevel::HaltOpens, reason, client, rid)?;
        }
        self.budget.at(self.rate_now()).charge();
        self.cancel_event(client, rid, "*", "sent", "{}")?;
        let r = self.b.cancel_all();
        let o = self.cancel_outcome(r);
        self.cancel_event(
            client,
            rid,
            "*",
            "outcome",
            &Obj::new().raw("outcome", &o).done(),
        )?;
        self.drain()?;
        self.budget.at(self.rate_now()).charge();
        let o = match self.b.flatten() {
            FlattenResult::Sent(closings, raw) => {
                let list = arr(closings.iter().map(|c| {
                    Obj::new()
                        .s("symbol", &c.symbol)
                        .n("status", c.status)
                        .raw("broker_raw", &c.raw)
                        .done()
                }));
                outcome("ok")
                    .s("state", self.state())
                    .raw("positions", &list)
                    .raw("broker_raw", &raw)
                    .done()
            }
            FlattenResult::Rejected(e) => broker_error_obj("rejected", &e)
                .s("state", self.state())
                .done(),
            FlattenResult::Unknown { reason } => outcome("unknown")
                .s("reason", reason)
                .s("state", self.state())
                .done(),
        };
        self.emit(
            self.ts(),
            "flatten",
            &event("outcome").raw("outcome", &o).done(),
            aud(),
        )?;
        self.drain()?;
        Ok(o)
    }

    fn drain(&mut self) -> Result<(), String> {
        let mut updates = Vec::new();
        self.b.poll_updates(&mut updates);
        for u in updates {
            match u {
                Update::Order(o) => self.on_order(*o)?,
                Update::Activity { activity_type, raw } => {
                    let body = Obj::new()
                        .s("activity_type", &activity_type)
                        .raw("broker_raw", &raw)
                        .done();
                    self.emit(self.ts(), "activity", &body, Aud::All)?;
                }
                Update::StreamDown => {
                    if self.stream_down.is_none() {
                        self.stream_down = Some((Instant::now(), self.ts()));
                    }
                }
                Update::StreamUp => {
                    self.stream_down = None;
                    self.degraded = false;
                    self.reconcile(false)?;
                }
            }
        }
        Ok(())
    }

    fn foreign(&mut self, key: &str, fields: &str) -> Result<(), String> {
        if self.foreign.insert(key.to_owned()) {
            self.emit(self.ts(), "foreign", fields, Aud::All)?;
        }
        Ok(())
    }

    fn resolution(&mut self, cid: &str, kind: &str, window: Duration) -> Result<(), String> {
        let body = Obj::new()
            .s("client_order_id", cid)
            .s("outcome", kind)
            .n("window_ms", window.as_millis())
            .done();
        self.emit(self.ts(), "resolution", &body, Aud::Prefix(cid.to_owned()))
    }

    fn on_order(&mut self, up: OrderUpdate) -> Result<(), String> {
        let bo = up.order;
        let cid = bo.client_order_id.clone();
        if !self.orders.contains_key(&cid) {
            let f = Obj::new()
                .s("broker_order_id", &bo.broker_order_id)
                .raw("broker_raw", &bo.raw)
                .done();
            return self.foreign(&bo.broker_order_id, &f);
        }
        if let Some(i) = self.pending.iter().position(|p| p.cid == cid) {
            let p = self.pending.remove(i);
            let w = p
                .next
                .checked_sub(1)
                .map_or(Duration::ZERO, |k| self.cfg.timing.resolve[k]);
            self.resolution(&cid, "found", w)?;
        }
        let event = if up.source == "rest_reconcile" {
            status_event(&up.event)
        } else {
            normalize_event(&up.event)
        };
        let mut o = Obj::new()
            .s("client_order_id", &cid)
            .s("broker_order_id", &bo.broker_order_id)
            .s("event", event)
            .s("status", &bo.status)
            .n("ts_broker", up.ts_broker);
        if let Some(f) = &up.fill {
            o = o.raw("fill", f);
        }
        o = o
            .n("filled_qty", bo.filled_qty)
            .opt_s("avg_price", bo.avg_price.as_deref());
        if let Some(l) = up.legs.as_ref().or(bo.legs.as_ref()) {
            o = o.raw("legs", l);
        }
        if let Some(p) = up.position_qty {
            o = o.n("position_qty", p);
        }
        if let Some(r) = &up.reason {
            o = o.raw("reason", r);
        }
        let body = o
            .s("source", up.source)
            .s("broker_event", &up.event)
            .raw("broker_raw", &up.raw)
            .done();
        let ts = if self.b.now().is_some() {
            up.ts_broker
        } else {
            wall_ns()
        };
        self.by_broker
            .insert(bo.broker_order_id.clone(), cid.clone());
        let st = self.orders.get_mut(&cid).ok_or("order state")?;
        st.phase = Phase::Done;
        st.broker_id = Some(bo.broker_order_id);
        st.status = bo.status;
        st.filled = bo.filled_qty;
        st.avg = bo.avg_price;
        st.legs = bo.legs;
        st.raw = Some(bo.raw);
        self.emit(ts, "order", &body, Aud::Prefix(cid))
    }

    /// Applies a REST answer for an order. An answer that matches what the gateway holds
    /// changes nothing, and neither does one that would move a settled order back to a
    /// working status: REST can lag the stream, so that answer is stale.
    fn reconcile_order(&mut self, bo: BrokerOrder) -> Result<(), String> {
        let skip = self.orders.get(&bo.client_order_id).is_some_and(|st| {
            st.phase == Phase::Done
                && ((st.status == bo.status && st.filled == bo.filled_qty)
                    || (settled(&st.status, st.tif.as_deref())
                        && !settled(&bo.status, st.tif.as_deref())))
        });
        if skip {
            return Ok(());
        }
        self.on_order(OrderUpdate {
            event: bo.status.clone(),
            ts_broker: bo.updated_ts,
            fill: None,
            legs: None,
            position_qty: None,
            reason: None,
            source: "rest_reconcile",
            raw: bo.raw.clone(),
            order: bo,
        })
    }

    /// Starts a reconcile: the start gate, then journaled-order lookups, recent orders and
    /// positions (section 11). It makes no broker call itself; `reconcile_step` runs it one
    /// call per tick. A request during one in progress marks it to run again after it.
    fn reconcile(&mut self, startup: bool) -> Result<(), String> {
        if let Some(rc) = self.reconcile.as_mut() {
            rc.again = true;
            return Ok(());
        }
        if startup {
            self.status("{}")?;
        }
        let from_ts = self.j.last_ts.saturating_sub(60_000_000_000);
        self.reconcile = Some(Reconcile {
            steps: VecDeque::from([Step::Gate]),
            from_ts,
            checked: 0,
            announce: startup,
            startup,
            again: false,
        });
        Ok(())
    }

    /// Runs the next call of the reconcile in progress, skipping lookups whose order no
    /// longer needs one. A refused token makes no call and keeps the step for a later tick.
    fn reconcile_step(&mut self) -> Result<Turn, String> {
        loop {
            let Some(rc) = self.reconcile.as_mut() else {
                return Ok(Turn::Waiting);
            };
            let Some(step) = rc.steps.pop_front() else {
                self.finish_reconcile()?;
                return Ok(Turn::Waiting);
            };
            let critical = rc.startup;
            // A lookup queued for an order that has since settled (a stream event, say) is
            // skipped: its answer could only be older than what the gateway holds.
            if let Step::Order(cid) = &step
                && !self
                    .orders
                    .get(cid)
                    .is_some_and(|s| matches!(s.phase, Phase::Done | Phase::Absent) && s.open())
            {
                continue;
            }
            if !self.budget.at(self.rate_now()).take(critical) {
                self.reconcile_front(step);
                return Ok(Turn::Waiting);
            }
            let turn = self.run_step(step)?;
            if self
                .reconcile
                .as_ref()
                .is_some_and(|rc| rc.steps.is_empty())
            {
                self.finish_reconcile()?;
            }
            return Ok(turn);
        }
    }

    fn reconcile_front(&mut self, step: Step) {
        if let Some(rc) = self.reconcile.as_mut() {
            rc.steps.push_front(step);
        }
    }

    /// Makes one reconcile call (its token already taken). A 429 holds the budget back.
    fn run_step(&mut self, step: Step) -> Result<Turn, String> {
        match step {
            Step::Gate => {
                let gate = match self.b.account() {
                    Ok(r) if r.value.status != "ACTIVE" => Some("account not active"),
                    Ok(r) if r.value.blocked => Some("account blocked"),
                    Ok(r) if r.value.options_trading_level < self.cfg.profile.min_options_level => {
                        Some("options level below the profile minimum")
                    }
                    Ok(_) => None,
                    Err(e) => {
                        self.reconcile_failed(&e);
                        Some("account query failed")
                    }
                };
                if let Some(why) = gate {
                    // Only a reconcile that opens the gate waits on it. A reconnect's reconcile
                    // with the gate already open is dropped, as before reconciles ran in calls:
                    // retrying it would claim every poll while the account call keeps failing,
                    // and the lookups, clears and degraded listing that settle orders would
                    // never run. The next reconnect starts a new one.
                    let required =
                        !self.gate_ok || self.reconcile.as_ref().is_some_and(|rc| rc.startup);
                    if !required {
                        let again = self.reconcile.take().is_some_and(|rc| rc.again);
                        if again {
                            self.reconcile(false)?;
                        }
                        return Ok(Turn::Gated);
                    }
                    let announce = self.reconcile.as_mut().is_some_and(|rc| {
                        rc.steps.push_front(Step::Gate);
                        std::mem::take(&mut rc.announce)
                    });
                    if announce {
                        self.status(&Obj::new().s("gate", why).done())?;
                    }
                    return Ok(Turn::Gated);
                }
                self.expand_reconcile()?;
                Ok(Turn::Called)
            }
            Step::Order(cid) => {
                if let Some(rc) = self.reconcile.as_mut() {
                    rc.checked += 1;
                }
                match self.b.order(&OrderRef::Client(cid)) {
                    Ok(r) => {
                        if let Some(bo) = r.value {
                            self.reconcile_order(bo)?;
                        }
                    }
                    Err(e) => self.reconcile_failed(&e),
                }
                Ok(Turn::Called)
            }
            Step::Recent => {
                let from_ts = self.reconcile.as_ref().map_or(0, |rc| rc.from_ts);
                match self.b.orders_since(from_ts) {
                    Ok(r) => {
                        for bo in r.value {
                            if let Some(rc) = self.reconcile.as_mut() {
                                rc.checked += 1;
                            }
                            self.reconcile_order(bo)?;
                        }
                    }
                    Err(e) => self.reconcile_failed(&e),
                }
                Ok(Turn::Called)
            }
            Step::Positions => {
                let r = match self.b.positions() {
                    Ok(r) => r,
                    Err(e) => {
                        self.reconcile_failed(&e);
                        return Ok(Turn::Called);
                    }
                };
                // Positions outside the allowlist: options are the account's own business,
                // and so are allowlisted stocks.
                let equities = &self.cfg.profile.equities;
                let theirs: Vec<(String, String)> = r
                    .value
                    .iter()
                    .filter(|p| occ(&p.symbol).is_none() && !equities.contains(&p.symbol))
                    .map(|p| {
                        let f = Obj::new()
                            .s("symbol", &p.symbol)
                            .n("qty", p.qty)
                            .raw("broker_raw", &r.raw)
                            .done();
                        (format!("position:{}", p.symbol), f)
                    })
                    .collect();
                for (key, f) in theirs {
                    self.foreign(&key, &f)?;
                }
                Ok(Turn::Called)
            }
        }
    }

    /// The gate passed: the journaled unresolved states are settled as they were, and the
    /// open orders are queued for a lookup each, then the recent listing and positions.
    fn expand_reconcile(&mut self) -> Result<(), String> {
        let mut cids: Vec<String> = self
            .orders
            .iter()
            .filter(|(_, s)| s.open())
            .map(|(c, _)| c.clone())
            .collect();
        cids.sort();
        let mut steps: VecDeque<Step> = VecDeque::new();
        for cid in cids {
            let Some(st) = self.orders.get_mut(&cid) else {
                continue;
            };
            let (client, rid) = (st.client.clone(), st.request_id.clone());
            match st.phase {
                Phase::Admitted => {
                    st.phase = Phase::Done;
                    "not_sent".clone_into(&mut st.status);
                    let o = not_sent("not_ready", &Obj::new().s("interrupted", "admitted").done());
                    self.outcome_event(&client, &rid, Some(&cid), o)?;
                }
                Phase::Sent => {
                    st.phase = Phase::Unknown;
                    let o = outcome("unknown")
                        .s("reason", "connection")
                        .s("client_order_id", &cid)
                        .done();
                    self.outcome_event(&client, &rid, Some(&cid), o)?;
                    self.pending.push(Pending {
                        cid,
                        start: self.rate_now(),
                        next: 0,
                        last: None,
                    });
                }
                Phase::Unknown => {
                    if !self.pending.iter().any(|p| p.cid == cid) {
                        let start = self.rate_now();
                        self.pending.push(Pending {
                            cid,
                            start,
                            next: 0,
                            last: None,
                        });
                    }
                }
                // Found now or not: only a lookup's answer settles an order, so Done and
                // Absent are both looked up.
                Phase::Done | Phase::Absent => steps.push_back(Step::Order(cid)),
                // Cleared orders are not open, so they are never listed here.
                Phase::Cleared => {}
            }
        }
        steps.push_back(Step::Recent);
        steps.push_back(Step::Positions);
        if let Some(rc) = self.reconcile.as_mut() {
            rc.steps.extend(steps);
        }
        Ok(())
    }

    /// Ends the reconcile: the gate opens and the resync is journaled. A reconcile requested
    /// meanwhile starts next.
    fn finish_reconcile(&mut self) -> Result<(), String> {
        let Some(rc) = self.reconcile.take() else {
            return Ok(());
        };
        self.gate_ok = true;
        let resync = Obj::new()
            .n("from_ts", rc.from_ts)
            .n("to_ts", self.ts())
            .n("orders_checked", rc.checked)
            .done();
        self.status(&Obj::new().raw("resync", &resync).done())?;
        if rc.again {
            self.reconcile(false)?;
        }
        Ok(())
    }

    /// A failed reconcile call; a 429 holds the budget back.
    fn reconcile_failed(&mut self, e: &BrokerError) {
        if e.http_status == 429 {
            self.budget.at(self.rate_now()).block(e.retry_after_ms);
        }
    }

    /// The open-orders check of a clear: the order's seen state and the listing's size.
    /// `None` when the budget refused the call, which sends nothing.
    fn open_check(&mut self, cid: &str, critical: bool) -> Option<(Seen, usize)> {
        if !self.budget.at(self.rate_now()).take(critical) {
            return None;
        }
        Some(match self.b.open_orders() {
            Ok(r) => {
                let seen = match r.value.iter().find(|o| o.client_order_id == cid) {
                    Some(o) => Seen::Shows(o.status.clone()),
                    None if listing_full(r.value.len()) => Seen::Unreliable("listing_full".into()),
                    None => Seen::Absent,
                };
                (seen, r.value.len())
            }
            Err(e) => (self.failed(&e), 0),
        })
    }

    /// The by-id check of a clear: the seen state, the raw answer when it is a 404, and the
    /// order the lookup found. `None` when the budget refused the call, which sends nothing.
    fn by_id_check(
        &mut self,
        cid: &str,
        critical: bool,
    ) -> Option<(Seen, String, Option<BrokerOrder>)> {
        if !self.budget.at(self.rate_now()).take(critical) {
            return None;
        }
        Some(match self.b.order(&OrderRef::Client(cid.to_owned())) {
            Ok(r) => match r.value {
                Some(o) => (Seen::Shows(o.status.clone()), String::from("null"), Some(o)),
                None => (Seen::Absent, r.raw, None),
            },
            Err(e) => (self.failed(&e), String::from("null"), None),
        })
    }

    /// A failed broker check; a 429 holds the budget back as it does for any query.
    fn failed(&mut self, e: &BrokerError) -> Seen {
        if e.http_status == 429 {
            self.budget.at(self.rate_now()).block(e.retry_after_ms);
        }
        Seen::Failed(e.code.clone())
    }

    /// An operator's `clear_order`: the `ok` or `not_cleared` outcome. A refused clear is
    /// journaled as `clear_refused`, whatever the refusal.
    fn clear_request(
        &mut self,
        requester: &str,
        cid: &str,
        reason: &str,
    ) -> Result<String, String> {
        match self.clear(requester, cid, reason)? {
            Ok(()) => Ok(outcome("ok").s("client_order_id", cid).done()),
            Err(r) => {
                self.refuse(requester, cid, reason, &r)?;
                Ok(not_cleared(r.reason, &r.detail))
            }
        }
    }

    /// Journals a clear request that was refused, with the requester's name and the
    /// reason it was given. Visible to the same clients as a `cleared` event.
    fn refuse(
        &mut self,
        requester: &str,
        cid: &str,
        request_reason: &str,
        r: &Refused,
    ) -> Result<(), String> {
        let mut body = Obj::new()
            .s("client_order_id", cid)
            .s("requester", requester)
            .s("request_reason", request_reason)
            .s("reason", r.reason)
            .raw("detail", &r.detail);
        if let Some(broker) = &r.broker {
            body = body.raw("broker", broker);
        }
        let aud = if cid.is_empty() {
            Aud::Client(requester.to_owned())
        } else {
            Aud::Prefix(cid.to_owned())
        };
        self.emit(self.ts(), "clear_refused", &body.done(), aud)
    }

    /// Clears an unresolved order that the broker does not show (section 5), for an
    /// operator's `clear_order`, critical traffic: both checks run now. Emits `cleared` and
    /// returns `Ok(Ok(()))`, or returns why the clear was refused.
    fn clear(
        &mut self,
        requester: &str,
        cid: &str,
        reason: &str,
    ) -> Result<Result<(), Refused>, String> {
        let Some(st) = self.orders.get(cid) else {
            return Ok(Err(Refused::plain("unknown_order")));
        };
        if !st.open() {
            return Ok(Err(Refused::plain("not_unresolved")));
        }
        // Both checks always run, so a shown order is reported as shown even when the other
        // check fails.
        let (open, listed) = self
            .open_check(cid, true)
            .unwrap_or((Seen::Failed("throttled".into()), 0));
        let (by_id, raw, found) = self.by_id_check(cid, true).unwrap_or((
            Seen::Failed("throttled".into()),
            String::from("null"),
            None,
        ));
        self.settle(
            requester,
            cid,
            reason,
            Checks {
                open,
                listed,
                by_id,
                raw,
                found,
            },
        )
    }

    /// Decides a clear on its checks, and clears the order when neither shows it. Emits
    /// `cleared`, or returns the refusal.
    fn settle(
        &mut self,
        requester: &str,
        cid: &str,
        reason: &str,
        c: Checks,
    ) -> Result<Result<(), Refused>, String> {
        // An order the lookup found settles from its state, as a found lookup settles it,
        // whatever the verdict: a terminal state stops it counting, a working one stays open.
        if let Some(bo) = c.found {
            self.reconcile_order(bo)?;
        }
        if let Err(refusal) = clear_verdict(&c.open, &c.by_id) {
            let (why, detail) = refusal_parts(&refusal);
            return Ok(Err(Refused {
                reason: why,
                detail,
                broker: Some(broker_checks(&c.open, c.listed, &c.by_id, &c.raw)),
            }));
        }
        if let Some(st) = self.orders.get_mut(cid) {
            st.phase = Phase::Cleared;
        }
        self.pending.retain(|p| p.cid != cid);
        let broker = Obj::new()
            .raw(
                "open_orders",
                &Obj::new()
                    .s("result", "absent")
                    .n("listed", c.listed)
                    .done(),
            )
            .raw(
                "by_id",
                &Obj::new()
                    .s("result", "absent")
                    .raw("broker_raw", &c.raw)
                    .done(),
            )
            .done();
        let body = Obj::new()
            .s("client_order_id", cid)
            .s("requester", requester)
            .s("reason", reason)
            .raw("broker", &broker)
            .done();
        self.emit(self.ts(), "cleared", &body, Aud::Prefix(cid.to_owned()))?;
        Ok(Ok(()))
    }

    /// Clears one unresolved day or cls order whose session has passed, when the broker
    /// shows nothing: one order per clear turn, round-robin, on the order's clear backoff.
    /// Its two checks run on separate polls: the open listing first, kept as `clear_half`,
    /// then the by-id lookup on a later clear turn. Each check takes a non-critical token;
    /// a refused token sends nothing, returns `false` and leaves the backoff alone. A
    /// refused automatic clear leaves the order alone and is not journaled, and backs off
    /// like a failed lookup. Returns whether it made a broker call.
    fn clear_day_orders(&mut self, at: u64) -> Result<bool, String> {
        let now = self.ts();
        let mut cids: Vec<String> = self
            .orders
            .iter()
            .filter(|(_, s)| {
                matches!(s.phase, Phase::Unknown | Phase::Absent)
                    && session_order_expired(s.tif.as_deref(), s.submitted, now)
            })
            .map(|(c, _)| c.clone())
            .collect();
        cids.sort();
        self.clear_retry
            .retain(|c, _| cids.binary_search(c).is_ok());
        if let Some(half) = self.clear_half.take()
            && cids.binary_search(&half.cid).is_ok()
        {
            return self.clear_by_id(half, at);
        }
        let retry = &self.clear_retry;
        let Some(cid) = self.round_robin(&cids, |c| retry.get(c).is_none_or(|r| r.due <= at))
        else {
            return Ok(false);
        };
        let Some((open, listed)) = self.open_check(&cid, false) else {
            return Ok(false);
        };
        self.cursor = Some(cid.clone());
        // A listing that already refuses the clear refuses it whatever the by-id lookup
        // says, so the lookup is not sent.
        if clear_verdict(&open, &Seen::Absent).is_err() {
            backoff(&mut self.clear_retry, &cid, at, self.cfg.timing.poll);
        } else {
            self.clear_half = Some(ClearHalf { cid, open, listed });
        }
        Ok(true)
    }

    /// The by-id half of an automatic clear, decided with the listing kept from an earlier
    /// poll. A refused token keeps the half for the next clear turn.
    fn clear_by_id(&mut self, half: ClearHalf, at: u64) -> Result<bool, String> {
        let Some((by_id, raw, found)) = self.by_id_check(&half.cid, false) else {
            self.clear_half = Some(half);
            return Ok(false);
        };
        let cid = half.cid.clone();
        self.cursor = Some(cid.clone());
        let checks = Checks {
            open: half.open,
            listed: half.listed,
            by_id,
            raw,
            found,
        };
        match self.settle(GATEWAY, &cid, "day_order_after_session", checks)? {
            Ok(()) => {
                self.clear_retry.remove(&cid);
            }
            Err(_) => backoff(&mut self.clear_retry, &cid, at, self.cfg.timing.poll),
        }
        Ok(true)
    }

    /// The first id after the cursor, in sorted order and wrapping, that `due` accepts.
    fn round_robin(&self, cids: &[String], due: impl Fn(&str) -> bool) -> Option<String> {
        let start = self
            .cursor
            .as_ref()
            .and_then(|c| cids.iter().position(|x| x > c))
            .unwrap_or(0);
        (0..cids.len())
            .map(|k| &cids[(start + k) % cids.len()])
            .find(|c| due(c))
            .cloned()
    }

    /// Makes at most one lookup of a due pending order. Returns whether it made one.
    fn resolve(&mut self, now: u64) -> Result<bool, String> {
        let offsets = self.cfg.timing.resolve.clone();
        let mut i = 0;
        while i < self.pending.len() {
            let k = self.pending[i].next;
            let mut due = self.pending[i].start.saturating_add(ns(offsets[k]));
            // Under a session the market clock can jump (overnight, a sparse step):
            // the lookups keep their spacing after the last one, so a jump doesn't
            // run them all at once and write the order off within one step.
            if self.b.session_clocked()
                && let (Some(last), Some(prev)) = (self.pending[i].last, k.checked_sub(1))
            {
                due = due.max(last.saturating_add(ns(offsets[k].saturating_sub(offsets[prev]))));
            }
            if due > now {
                i += 1;
                continue;
            }
            // Refused by our own budget: no lookup was made, so the schedule does
            // not move and the order is looked up on a later tick.
            if !self.budget.at(self.rate_now()).take(true) {
                i += 1;
                continue;
            }
            let cid = self.pending[i].cid.clone();
            let lookup = match self.b.order(&OrderRef::Client(cid.clone())) {
                Ok(r) => r.value.map_or(Lookup::NotFound, Lookup::Found),
                Err(e) => {
                    if e.http_status == 429 {
                        self.budget.at(self.rate_now()).block(e.retry_after_ms);
                    }
                    Lookup::Failed
                }
            };
            let last = k + 1 >= offsets.len();
            match lookup {
                Lookup::Found(bo) => {
                    self.pending.remove(i);
                    self.resolution(&cid, "found", offsets[k])?;
                    self.reconcile_order(bo)?;
                }
                Lookup::NotFound if last => {
                    self.pending.remove(i);
                    if let Some(st) = self.orders.get_mut(&cid) {
                        st.phase = Phase::Absent;
                    }
                    self.resolution(&cid, "absent_after", offsets[k])?;
                }
                Lookup::Failed if last => {
                    // Stays Unknown: still open and blocking. The poll looks it up
                    // again (`recheck_unresolved`), and a stream event, a reconcile or
                    // a restart can settle it too.
                    self.pending.remove(i);
                    self.resolution(&cid, "unresolved", offsets[k])?;
                }
                Lookup::NotFound | Lookup::Failed => {
                    self.pending[i].next += 1;
                    self.pending[i].last = Some(now);
                }
            }
            // One blocking lookup per tick: the others are due and run on later ticks.
            return Ok(true);
        }
        Ok(false)
    }

    /// Looks up one unresolved order by client ID per poll, round-robin: each absent
    /// order, and each unknown order that is not scheduled (its last lookup failed), so it
    /// settles without waiting for a stream reconnect. Found, in any state: it is
    /// reconciled from its state and settles, or stays open when the state is not
    /// terminal. A 404 or any other error never clears it: it stays open and counted
    /// until a stream event or a later lookup finds it. A failed lookup is retried on the
    /// next poll, then after a doubling interval capped at `RECHECK_CAP`. Each lookup
    /// takes a non-critical token first; a refused token skips the order for this poll.
    /// Returns whether a lookup was made.
    fn recheck_unresolved(&mut self, now: u64) -> Result<bool, String> {
        let mut cids: Vec<String> = self
            .orders
            .iter()
            .filter(|(c, s)| match s.phase {
                Phase::Absent => true,
                Phase::Unknown => !self.pending.iter().any(|p| p.cid == **c),
                _ => false,
            })
            .map(|(c, _)| c.clone())
            .collect();
        cids.sort();
        // An order that settled has no backoff left: its next failure starts over.
        self.rechecks.retain(|c, _| cids.binary_search(c).is_ok());
        let rechecks = &self.rechecks;
        let Some(cid) = self.round_robin(&cids, |c| rechecks.get(c).is_none_or(|r| r.due <= now))
        else {
            return Ok(false);
        };
        if !self.budget.at(self.rate_now()).take(false) {
            return Ok(false);
        }
        self.cursor = Some(cid.clone());
        match self.b.order(&OrderRef::Client(cid.clone())) {
            Ok(r) => match r.value {
                Some(bo) => {
                    self.rechecks.remove(&cid);
                    self.reconcile_order(bo)?;
                }
                None => backoff(&mut self.rechecks, &cid, now, self.cfg.timing.poll),
            },
            Err(e) => {
                if e.http_status == 429 {
                    self.budget.at(self.rate_now()).block(e.retry_after_ms);
                }
                backoff(&mut self.rechecks, &cid, now, self.cfg.timing.poll);
            }
        }
        Ok(true)
    }

    /// One pass of the session's periodic work. At most one broker call happens per tick,
    /// except a sentinel kill, which makes its own (section 9). A call holds the thread, so
    /// the rest of the tick's work waits for a later tick.
    fn tick(&mut self) -> Result<(), String> {
        self.drain()?;
        // Liveness runs on real time; the polls, their backoffs and the timers of
        // unresolved orders on the rate clock (the market clock under a session).
        let wall = Instant::now();
        let now = self.rate_now();
        if let Some((since, _)) = self.stream_down
            && !self.degraded
            && wall.duration_since(since) > self.cfg.timing.stream_grace
        {
            self.degraded = true;
            self.sync_state()?;
        }
        let mut called = false;
        // Under a session the sentinel is also checked on real time, so a paused
        // session can still be killed.
        if self.b.session_clocked()
            && wall.duration_since(self.last_kill_check) >= self.cfg.timing.poll
        {
            self.last_kill_check = wall;
            let halted = matches!(self.killed, Some((KillLevel::HaltAll, _)));
            if !halted && self.cfg.state.join("KILL").exists() {
                self.kill(KillLevel::HaltAll, "sentinel", "operator", "sentinel")?;
                called = true;
            }
        }
        if !called && now.saturating_sub(self.last_poll) >= ns(self.cfg.timing.poll) {
            self.last_poll = now;
            // The kill sentinel is checked before any lookup, so a lookup never delays it. A kill
            // made in this tick takes the tick's call, so the poll waits.
            let halted = matches!(self.killed, Some((KillLevel::HaltAll, _)));
            if !halted && self.cfg.state.join("KILL").exists() {
                self.kill(KillLevel::HaltAll, "sentinel", "operator", "sentinel")?;
                called = true;
            } else if !self.gate_ok {
                // The gate is closed: the reconcile's next call, and nothing else this tick.
                self.reconcile_step()?;
                called = true;
            } else if self.reconcile.is_some() && self.reconcile_step()? != Turn::Waiting {
                called = true;
            } else {
                // The recheck and the automatic clear take turns to go first, so neither
                // starves the other; the other runs when the first has nothing to do.
                let clear_first = self.clear_turn;
                self.clear_turn = !self.clear_turn;
                called = if clear_first {
                    self.clear_day_orders(now)?
                } else {
                    self.recheck_unresolved(now)?
                };
                if !called {
                    called = if clear_first {
                        self.recheck_unresolved(now)?
                    } else {
                        self.clear_day_orders(now)?
                    };
                }
                // Degraded: the listing reconciles what the stream missed. It runs only when no
                // call was made this tick, and a refused token or a 429 is handled as any query.
                if !called && self.degraded && self.budget.at(self.rate_now()).take(false) {
                    called = true;
                    match self.b.open_orders() {
                        Ok(r) => {
                            for bo in r.value {
                                self.reconcile_order(bo)?;
                            }
                        }
                        Err(e) => {
                            if e.http_status == 429 {
                                self.budget.at(self.rate_now()).block(e.retry_after_ms);
                            }
                        }
                    }
                }
            }
        }
        if !called {
            called = self.resolve(now)?;
        }
        let due: Vec<String> = self
            .cod_timers
            .iter()
            .filter(|(_, t)| *t <= wall)
            .map(|(n, _)| n.clone())
            .collect();
        self.cod_timers.retain(|(_, t)| *t > wall);
        for name in due {
            let Some(spec) = self.cfg.clients.iter().find(|c| c.name == name).cloned() else {
                continue;
            };
            if self
                .conns
                .iter()
                .any(|c| self.cfg.clients[c.client].name == name)
            {
                continue;
            }
            let mut targets: Vec<(String, String)> = self
                .orders
                .iter()
                .filter(|(c, s)| c.starts_with(&spec.prefix) && s.open())
                .filter_map(|(c, s)| s.broker_id.clone().map(|b| (c.clone(), b)))
                .collect();
            targets.sort();
            for (cid, bid) in targets {
                if !self.cod_work.iter().any(|(_, c, _)| *c == cid) {
                    self.cod_work.push_back((name.clone(), cid, bid));
                }
            }
        }
        if !called {
            self.cod_step()?;
        }
        Ok(())
    }

    /// Cancels the next queued cancel-on-disconnect target, in queue order: one cancel per
    /// call. A client that has reconnected loses its queued targets, and a target whose order
    /// has settled is dropped without a call. Returns whether it made a broker call.
    fn cod_step(&mut self) -> Result<bool, String> {
        while let Some((name, cid, bid)) = self.cod_work.pop_front() {
            if self
                .conns
                .iter()
                .any(|c| self.cfg.clients[c.client].name == name)
            {
                self.cod_work.retain(|(n, _, _)| *n != name);
                continue;
            }
            if !self.orders.get(&cid).is_some_and(OrderState::open) {
                continue;
            }
            self.do_cancel(&name, "cancel_on_disconnect", &cid, &bid)?;
            return Ok(true);
        }
        Ok(false)
    }
}

fn line_aud(v: &Value) -> Aud {
    let s = |k: &str| v[k].as_str().unwrap_or("").to_string();
    match v["kind"].as_str().unwrap_or("") {
        "submit" | "cancel" | "client" => Aud::Client(s("client")),
        "order" | "resolution" => Aud::Prefix(s("client_order_id")),
        _ => Aud::All,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ClearHalf, OrderState, OrderUpdate, Pending, Phase, Refusal, Seen, Session, clear_verdict,
        close_check, listing_full, session_order_expired, wall_ns,
    };
    use crate::broker::{
        AccountInfo, Broker, BrokerError, BrokerOrder, CancelResult, Capabilities, FlattenResult,
        MarketClock, OrderRef, Position, Query, Reply, SessionDay, SubmitResult, Update,
    };
    use crate::intent::Intent;
    use crate::journal::Journal;
    use crate::protocol::KillLevel;
    use crate::safety::{Profile, days_from_civil, et_ns};
    use crate::server::{ClientSpec, Config, Role, Timing};
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;
    use std::time::Instant;

    /// A broker that answers from a script and records each call by name. Its open listing
    /// is `open`; with `open_429` set, the listing fails with a 429 and that retry-after.
    struct Stub {
        calls: Rc<RefCell<Vec<&'static str>>>,
        open_429: Option<u64>,
        /// The account answers ACTIVE with the options level the default profile needs.
        account_ok: bool,
        /// Every by-id lookup fails with a 429 and this retry-after.
        order_429: Option<u64>,
        /// What the open listing returns.
        listing: Vec<BrokerOrder>,
        /// What a by-id lookup finds; any other client id is a 404.
        found: Vec<BrokerOrder>,
        /// What the next drain delivers.
        updates: Vec<Update>,
    }

    fn listed(cid: &str) -> BrokerOrder {
        BrokerOrder {
            client_order_id: cid.into(),
            broker_order_id: format!("b-{cid}"),
            status: "new".into(),
            filled_qty: 0,
            avg_price: None,
            legs: None,
            updated_ts: 0,
            raw: "{}".into(),
        }
    }

    fn unsupported() -> BrokerError {
        BrokerError::local("unsupported", "stub")
    }

    impl Broker for Stub {
        fn kind(&self) -> &'static str {
            "sim"
        }
        fn capabilities(&self) -> Capabilities {
            Capabilities {
                combo: true,
                max_legs: 4,
                client_order_id_max: 128,
                idempotency: "client_order_id",
            }
        }
        fn pins(&self) -> String {
            "{}".into()
        }
        fn now(&self) -> Option<u64> {
            None
        }
        fn submit(&mut self, _: &Intent) -> SubmitResult {
            SubmitResult::Unknown {
                reason: "timeout",
                retry_after_ms: None,
            }
        }
        fn cancel(&mut self, _: &str) -> CancelResult {
            self.calls.borrow_mut().push("cancel");
            CancelResult::Unknown {
                reason: "timeout",
                retry_after_ms: None,
            }
        }
        fn cancel_all(&mut self) -> CancelResult {
            self.calls.borrow_mut().push("cancel_all");
            CancelResult::Unknown {
                reason: "timeout",
                retry_after_ms: None,
            }
        }
        fn flatten(&mut self) -> FlattenResult {
            FlattenResult::Unknown { reason: "timeout" }
        }
        fn order(&mut self, r: &OrderRef) -> Query<Option<BrokerOrder>> {
            self.calls.borrow_mut().push("order");
            if let Some(ms) = self.order_429 {
                return Err(BrokerError {
                    http_status: 429,
                    code: "http_429".into(),
                    message: String::new(),
                    raw: String::new(),
                    retry_after_ms: Some(ms),
                });
            }
            let value = match r {
                OrderRef::Client(cid) => self
                    .found
                    .iter()
                    .find(|o| o.client_order_id == *cid)
                    .cloned(),
                OrderRef::Broker(_) => None,
            };
            Ok(Reply {
                value,
                raw: "{}".into(),
            })
        }
        fn open_orders(&mut self) -> Query<Vec<BrokerOrder>> {
            self.calls.borrow_mut().push("open_orders");
            if let Some(ms) = self.open_429 {
                return Err(BrokerError {
                    http_status: 429,
                    code: "http_429".into(),
                    message: String::new(),
                    raw: String::new(),
                    retry_after_ms: Some(ms),
                });
            }
            Ok(Reply {
                value: self.listing.clone(),
                raw: "[]".into(),
            })
        }
        fn orders_since(&mut self, _: u64) -> Query<Vec<BrokerOrder>> {
            self.calls.borrow_mut().push("orders_since");
            Err(unsupported())
        }
        fn account(&mut self) -> Query<AccountInfo> {
            self.calls.borrow_mut().push("account");
            if !self.account_ok {
                return Err(unsupported());
            }
            Ok(Reply {
                value: AccountInfo {
                    status: "ACTIVE".into(),
                    cash: "0".into(),
                    buying_power: "0".into(),
                    options_trading_level: 3,
                    blocked: false,
                },
                raw: "{}".into(),
            })
        }
        fn positions(&mut self) -> Query<Vec<Position>> {
            self.calls.borrow_mut().push("positions");
            Err(unsupported())
        }
        fn activities(
            &mut self,
            _: &[String],
            _: Option<&str>,
            _: Option<&str>,
        ) -> Query<(Vec<String>, Option<String>)> {
            Err(unsupported())
        }
        fn clock(&mut self) -> Query<MarketClock> {
            Err(unsupported())
        }
        fn calendar(&mut self, _: &str, _: &str) -> Query<Vec<SessionDay>> {
            Err(unsupported())
        }
        fn poll_updates(&mut self, out: &mut Vec<Update>) {
            out.append(&mut self.updates);
        }
    }

    /// A session on a fresh state root, with its calls recorded in the returned log.
    fn session(
        name: &str,
        open_429: Option<u64>,
    ) -> (Session<Stub>, Rc<RefCell<Vec<&'static str>>>) {
        let root = std::env::temp_dir().join(format!("og-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let (journal, _) = Journal::open(&root, "acct").unwrap();
        let cfg = Config {
            account: "acct".into(),
            environment: "paper".into(),
            listen: "127.0.0.1:0".parse().unwrap(),
            listen_any: false,
            state: root,
            forbid_state: Vec::new(),
            clients: Vec::new(),
            profile: Profile::default(),
            timing: Timing::default(),
            stall_after_admitted: false,
        };
        let calls = Rc::new(RefCell::new(Vec::new()));
        let stub = Stub {
            calls: Rc::clone(&calls),
            open_429,
            account_ok: false,
            order_429: None,
            listing: Vec::new(),
            found: Vec::new(),
            updates: Vec::new(),
        };
        let mut s = Session::new(cfg, stub, journal, &[], Arc::new(AtomicU64::new(0)));
        s.gate_ok = true;
        (s, calls)
    }

    /// Makes the next poll due and runs one tick.
    fn poll(s: &mut Session<Stub>) {
        s.last_poll = s.rate_now() - 60_000_000_000;
        s.tick().unwrap();
    }

    /// An unresolved order, not scheduled for a resolve lookup, with its time in force.
    fn unresolved(s: &mut Session<Stub>, cid: &str, tif: &str, submitted: Option<u64>) {
        let mut o = OrderState::admitted(
            "acct".into(),
            "req".into(),
            None,
            Vec::new(),
            Some(tif.into()),
            submitted,
        );
        o.phase = Phase::Unknown;
        s.orders.insert(cid.into(), o);
    }

    /// Submitted two New York trading days ago: a day order past its session.
    fn two_days_ago() -> Option<u64> {
        Some(wall_ns() - 2 * 86_400_000_000_000)
    }

    #[test]
    fn a_day_order_past_its_session_is_cleared_while_many_orders_are_due() {
        // Thirty unresolved good-til-canceled orders are due for a lookup on every poll. The
        // day order must still be cleared: the recheck and the clear take turns.
        let (mut s, _) = session("clear-turns", None);
        for i in 0..30 {
            unresolved(&mut s, &format!("wse-gtc-{i:02}"), "gtc", None);
        }
        unresolved(&mut s, "wse-day", "day", two_days_ago());
        for _ in 0..4 {
            poll(&mut s);
        }
        assert_eq!(s.orders["wse-day"].phase, Phase::Cleared);
    }

    #[test]
    fn a_clear_checks_its_listing_and_its_lookup_on_separate_polls() {
        // Each clear turn makes one call: the listing first, the by-id lookup on the next.
        let (mut s, calls) = session("clear-split", None);
        unresolved(&mut s, "wse-day", "day", two_days_ago());
        assert_eq!(s.clear_day_orders(s.rate_now()), Ok(true));
        assert_eq!(*calls.borrow(), vec!["open_orders"]);
        assert_eq!(s.orders["wse-day"].phase, Phase::Unknown);
        assert_eq!(s.clear_day_orders(s.rate_now()), Ok(true));
        assert_eq!(*calls.borrow(), vec!["open_orders", "order"]);
        assert_eq!(s.orders["wse-day"].phase, Phase::Cleared);
    }

    #[test]
    fn a_429_on_the_degraded_listing_blocks_further_polls() {
        let (mut s, calls) = session("degraded-429", Some(60_000));
        s.degraded = true;
        poll(&mut s);
        assert_eq!(*calls.borrow(), vec!["open_orders"]);
        assert!(s.budget.blocked());
        // Within the retry-after time no poll sends anything, whatever is due.
        poll(&mut s);
        poll(&mut s);
        assert_eq!(*calls.borrow(), vec!["open_orders"]);
    }

    #[test]
    fn a_tick_makes_at_most_one_broker_call() {
        // Degraded, with three unresolved orders due: one lookup, and no listing or resolve.
        let (mut s, calls) = session("one-call", None);
        s.degraded = true;
        for i in 0..3 {
            unresolved(&mut s, &format!("wse-gtc-{i}"), "gtc", None);
        }
        poll(&mut s);
        assert_eq!(calls.borrow().len(), 1);
    }

    #[test]
    fn a_throttled_clear_sends_nothing_and_does_not_back_off() {
        let (mut s, calls) = session("throttled-clear", None);
        unresolved(&mut s, "wse-day", "day", two_days_ago());
        let t = s.rate_now();
        s.budget.at(t).block(Some(60_000));
        assert_eq!(s.clear_day_orders(s.rate_now()), Ok(false));
        assert!(calls.borrow().is_empty());
        assert!(s.clear_retry.is_empty());
        // A kept listing whose by-id lookup is refused waits for the next turn, unbacked.
        s.clear_half = Some(ClearHalf {
            cid: "wse-day".into(),
            open: Seen::Absent,
            listed: 0,
        });
        assert_eq!(s.clear_day_orders(s.rate_now()), Ok(false));
        assert!(s.clear_half.is_some());
        assert!(s.clear_retry.is_empty());
        assert!(calls.borrow().is_empty());
    }

    fn shows(s: &str) -> Seen {
        Seen::Shows(s.into())
    }

    fn failed(s: &str) -> Seen {
        Seen::Failed(s.into())
    }

    #[test]
    fn a_clear_needs_both_checks_to_answer_and_neither_to_show_the_order() {
        assert_eq!(clear_verdict(&Seen::Absent, &Seen::Absent), Ok(()));
        assert_eq!(
            clear_verdict(&shows("new"), &Seen::Absent),
            Err(Refusal::Shows {
                check: "open_orders",
                status: "new".into()
            })
        );
        assert_eq!(
            clear_verdict(&Seen::Absent, &shows("filled")),
            Err(Refusal::Shows {
                check: "by_id",
                status: "filled".into()
            })
        );
        assert_eq!(
            clear_verdict(&failed("broker_error"), &Seen::Absent),
            Err(Refusal::Unreachable {
                check: "open_orders",
                code: "broker_error".into()
            })
        );
        assert_eq!(
            clear_verdict(&Seen::Absent, &failed("throttled")),
            Err(Refusal::Unreachable {
                check: "by_id",
                code: "throttled".into()
            })
        );
    }

    #[test]
    fn a_shown_order_refuses_even_when_the_other_check_fails() {
        assert_eq!(
            clear_verdict(&failed("broker_error"), &shows("accepted")),
            Err(Refusal::Shows {
                check: "by_id",
                status: "accepted".into()
            })
        );
        assert_eq!(
            clear_verdict(&shows("new"), &failed("broker_error")),
            Err(Refusal::Shows {
                check: "open_orders",
                status: "new".into()
            })
        );
    }

    #[test]
    fn only_a_day_or_cls_order_past_its_new_york_date_is_auto_cleared() {
        let d = days_from_civil(2026, 10, 16);
        let at = |d: i64, min: i64| et_ns(d, min);
        let sub = Some(at(d, 10 * 60));
        // Later the same New York day, even after the close: not yet.
        assert!(!session_order_expired(Some("day"), sub, at(d, 23 * 60)));
        assert!(!session_order_expired(Some("cls"), sub, at(d, 23 * 60)));
        // The next New York date: past its session.
        assert!(session_order_expired(Some("day"), sub, at(d + 1, 0)));
        assert!(session_order_expired(Some("cls"), sub, at(d + 1, 0)));
        // A good-til-canceled order or an unknown time-in-force never is, however old it is.
        assert!(!session_order_expired(Some("gtc"), sub, at(d + 5, 0)));
        assert!(!session_order_expired(None, sub, at(d + 5, 0)));
        // A submit time that was never journaled is never expired.
        assert!(!session_order_expired(Some("day"), None, at(d + 5, 0)));
        assert!(!session_order_expired(Some("cls"), None, at(d + 5, 0)));
    }

    #[test]
    fn a_full_open_orders_listing_may_hide_orders() {
        assert!(!listing_full(0));
        assert!(!listing_full(499));
        assert!(listing_full(500));
        assert!(listing_full(501));
    }

    #[test]
    fn a_full_listing_is_unreliable_and_refuses_the_clear_unless_a_check_shows_the_order() {
        let full = Seen::Unreliable("listing_full".into());
        assert_eq!(
            clear_verdict(&full, &Seen::Absent),
            Err(Refusal::Unreachable {
                check: "open_orders",
                code: "listing_full".into()
            })
        );
        assert_eq!(
            clear_verdict(&full, &shows("new")),
            Err(Refusal::Shows {
                check: "by_id",
                status: "new".into()
            })
        );
    }

    #[test]
    fn a_close_is_allowed_up_to_the_held_side_only() {
        // A sell closes a long; a buy closes a short.
        assert_eq!(close_check(Some(10), 10, false), Ok(()));
        assert_eq!(
            close_check(Some(10), 11, false),
            Err("close_exceeds_position")
        );
        assert_eq!(close_check(Some(-3), 3, true), Ok(()));
        assert_eq!(
            close_check(Some(-3), 4, true),
            Err("close_exceeds_position")
        );
        // A sell against a short, or a buy against a long, opens instead of closing.
        assert_eq!(
            close_check(Some(-3), 1, false),
            Err("close_exceeds_position")
        );
        assert_eq!(close_check(Some(3), 1, true), Err("close_exceeds_position"));
        // Flat is never closable; an unreadable position is never closable.
        assert_eq!(
            close_check(Some(0), 1, false),
            Err("close_exceeds_position")
        );
        assert_eq!(close_check(None, 1, false), Err("position_unknown"));
        assert_eq!(close_check(None, 1, true), Err("position_unknown"));
    }

    /// A gateway-held order that is `Done` with `status` and `tif`, and its broker id.
    fn done(s: &mut Session<Stub>, cid: &str, status: &str, tif: &str) {
        let mut o = done_order(status, Some(tif));
        o.broker_id = Some(format!("b-{cid}"));
        s.orders.insert(cid.into(), o);
    }

    fn done_order(status: &str, tif: Option<&str>) -> OrderState {
        let tif = tif.map(String::from);
        let mut o = OrderState::admitted(
            "acct".into(),
            "req".into(),
            None,
            Vec::new(),
            tif,
            Some(wall_ns()),
        );
        o.phase = Phase::Done;
        o.status = status.into();
        o
    }

    #[test]
    fn a_closed_gate_tick_makes_at_most_one_broker_call_and_skips_resolve() {
        let (mut s, calls) = session("gate-one", None);
        s.gate_ok = false;
        s.b.account_ok = true;
        s.reconcile(false).unwrap();
        for i in 0..3 {
            done(&mut s, &format!("wse-{i}"), "new", "day");
        }
        // An unknown order whose lookup is due: the gate-closed tick must not resolve it.
        unresolved(&mut s, "wse-pending", "day", None);
        s.pending.push(Pending {
            cid: "wse-pending".into(),
            start: s.rate_now() - 3_600_000_000_000,
            next: 0,
            last: None,
        });
        poll(&mut s);
        assert_eq!(*calls.borrow(), vec!["account"]);
        let mut before = calls.borrow().len();
        for _ in 0..8 {
            poll(&mut s);
            let now = calls.borrow().len();
            assert!(now - before <= 1);
            before = now;
        }
        assert!(s.gate_ok);
    }

    #[test]
    fn a_reconcile_with_the_gate_open_makes_one_call_per_tick() {
        // A stream-up reconcile queues its lookups; none runs as a burst.
        let (mut s, calls) = session("stream-up", None);
        s.b.account_ok = true;
        for i in 0..3 {
            done(&mut s, &format!("wse-{i}"), "new", "day");
        }
        s.reconcile(false).unwrap();
        let mut before = 0;
        for _ in 0..6 {
            poll(&mut s);
            let now = calls.borrow().len();
            assert!(now - before <= 1);
            before = now;
        }
        assert_eq!(calls.borrow().len(), 6);
    }

    fn filled(cid: &str) -> BrokerOrder {
        BrokerOrder {
            status: "filled".into(),
            filled_qty: 1,
            avg_price: Some("1.00".into()),
            ..listed(cid)
        }
    }

    #[test]
    fn a_reconnect_with_a_failing_account_call_still_settles_an_unknown_order() {
        // The gate is open, and every account call fails. A stream reconnect's reconcile is
        // dropped at its gate, so the poll's lookup still runs and settles the order.
        let (mut s, calls) = session("reconnect-gate", None);
        s.b.account_ok = false;
        unresolved(&mut s, "wse-u", "gtc", None);
        s.b.found = vec![filled("wse-u")];
        s.b.updates.push(Update::StreamUp);
        poll(&mut s);
        assert_eq!(*calls.borrow(), vec!["account"]);
        assert!(s.reconcile.is_none());
        assert!(s.gate_ok);
        // Another reconnect fails its gate the same way; the lookup runs after it.
        s.b.updates.push(Update::StreamUp);
        poll(&mut s);
        poll(&mut s);
        assert_eq!(*calls.borrow(), vec!["account", "account", "order"]);
        let st = &s.orders["wse-u"];
        assert_eq!(st.phase, Phase::Done);
        assert_eq!(st.status, "filled");
    }

    #[test]
    fn a_startup_reconcile_keeps_retrying_a_failing_gate() {
        let (mut s, calls) = session("startup-gate", None);
        s.gate_ok = false;
        s.reconcile(true).unwrap();
        unresolved(&mut s, "wse-u", "gtc", None);
        for _ in 0..3 {
            poll(&mut s);
        }
        assert_eq!(*calls.borrow(), vec!["account"; 3]);
        assert!(s.reconcile.is_some());
        assert!(!s.gate_ok);
    }

    #[test]
    fn a_stale_lookup_never_reopens_a_filled_order() {
        let (mut s, calls) = session("stale-lookup", None);
        s.b.account_ok = true;
        done(&mut s, "wse-f", "new", "gtc");
        s.b.found = vec![listed("wse-f")];
        s.reconcile(false).unwrap();
        poll(&mut s);
        assert_eq!(*calls.borrow(), vec!["account"]);
        // The stream fills the order before its queued lookup runs.
        let bo = filled("wse-f");
        s.on_order(OrderUpdate {
            event: "fill".into(),
            ts_broker: 0,
            fill: None,
            legs: None,
            position_qty: None,
            reason: None,
            source: "stream",
            raw: bo.raw.clone(),
            order: bo,
        })
        .unwrap();
        // The queued lookup is skipped: the next call is the recent listing.
        poll(&mut s);
        assert_eq!(*calls.borrow(), vec!["account", "orders_since"]);
        // A lookup that answers "new" anyway, from any path, leaves it filled.
        s.reconcile_order(listed("wse-f")).unwrap();
        let st = &s.orders["wse-f"];
        assert_eq!(st.status, "filled");
        assert_eq!(st.filled, 1);
        assert!(!st.open());
    }

    #[test]
    fn a_429_on_a_reconcile_call_blocks_further_polls() {
        let (mut s, calls) = session("reconcile-429", None);
        s.gate_ok = false;
        s.b.account_ok = true;
        s.b.order_429 = Some(60_000);
        s.reconcile(false).unwrap();
        done(&mut s, "wse-0", "new", "day");
        poll(&mut s);
        poll(&mut s);
        assert_eq!(*calls.borrow(), vec!["account", "order"]);
        assert!(s.budget.blocked());
        // Within the retry-after time no reconcile call is sent, whatever is queued.
        poll(&mut s);
        poll(&mut s);
        assert_eq!(*calls.borrow(), vec!["account", "order"]);
        assert!(!s.gate_ok);
    }

    #[test]
    fn a_sentinel_kill_makes_its_calls_and_no_poll_call() {
        let (mut s, calls) = session("sentinel", None);
        s.gate_ok = true;
        for i in 0..3 {
            done(&mut s, &format!("wse-{i}"), "new", "day");
        }
        s.b.listing = (0..3).map(|i| listed(&format!("wse-{i}"))).collect();
        // An absent order whose lookup a poll would make.
        unresolved(&mut s, "wse-absent", "gtc", None);
        s.orders.get_mut("wse-absent").unwrap().phase = Phase::Absent;
        std::fs::write(s.cfg.state.join("KILL"), "").unwrap();
        poll(&mut s);
        assert_eq!(
            *calls.borrow(),
            vec!["cancel_all", "open_orders", "cancel", "cancel", "cancel"]
        );
        assert!(matches!(s.killed, Some((KillLevel::HaltAll, _))));
    }

    #[test]
    fn cancel_on_disconnect_cancels_one_target_per_tick() {
        let (mut s, calls) = session("cod", None);
        s.gate_ok = true;
        s.cfg.clients.push(ClientSpec {
            name: "alice".into(),
            role: Role::Trade,
            prefix: "wse-".into(),
            token: "token".into(),
        });
        for i in 0..5 {
            done(&mut s, &format!("wse-{i}"), "new", "day");
        }
        s.cod_timers.push(("alice".into(), Instant::now()));
        let mut left = Vec::new();
        for _ in 0..5 {
            s.last_poll = s.rate_now();
            s.tick().unwrap();
            left.push(s.cod_work.len());
        }
        assert_eq!(left, vec![4, 3, 2, 1, 0]);
        assert_eq!(*calls.borrow(), vec!["cancel"; 5]);
    }

    #[test]
    fn done_for_day_ends_a_day_order_and_no_other() {
        assert!(!done_order("done_for_day", Some("day")).open());
        assert!(done_order("done_for_day", Some("gtc")).open());
        assert!(done_order("done_for_day", Some("cls")).open());
        assert!(done_order("done_for_day", None).open());
        assert!(!done_order("filled", Some("gtc")).open());
    }

    #[test]
    fn a_stopped_or_suspended_order_stays_open() {
        assert!(done_order("stopped", Some("day")).open());
        assert!(done_order("suspended", Some("day")).open());
    }

    #[test]
    fn a_gtc_order_in_done_for_day_blocks_a_close_on_its_leg_and_a_day_order_does_not() {
        let (mut s, _) = session("dfd-leg", None);
        s.gate_ok = true;
        s.cfg.profile.equities.push("ABC".into());
        let mut o = done_order("done_for_day", Some("gtc"));
        o.symbols = vec!["ABC".into()];
        s.orders.insert("wse-open".into(), o);
        let close = close_intent("wse-close", "ABC");
        let refused = s.refusal("alice", "wse-", &close).unwrap();
        assert!(refused.contains("leg_pending"), "{refused}");
        s.orders.get_mut("wse-open").unwrap().tif = Some("day".into());
        assert_eq!(s.refusal("alice", "wse-", &close), None);
    }

    fn close_intent(cid: &str, symbol: &str) -> Intent {
        let v: serde_json::Value = serde_json::from_str(&format!(
            r#"{{"client_order_id":"{cid}","shape":"single","instrument":{{"class":"equity","symbol":"{symbol}"}},"side":"sell","effect":"close","qty":1,"type":"limit","limit_price":"10.00","time_in_force":"day"}}"#
        ))
        .unwrap();
        Intent::parse(v).unwrap_or_else(|_| panic!("close intent"))
    }
}
