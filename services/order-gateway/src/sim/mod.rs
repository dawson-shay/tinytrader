//! `SimBroker`: a deterministic simulated broker behind the same `Broker` seam.
//! Scripted mode replays `FakeBroker` scenarios; market mode fills against DBN quotes in
//! client-clocked market time. Its cash, positions and activities are broker state.
mod expiry;
mod fill;
mod market;
mod queue;
mod rules;
mod script;

use crate::broker::{
    AccountInfo, Broker, BrokerError, BrokerOrder, CancelResult, Capabilities, Closing,
    FlattenResult, MarketClock, OrderRef, OrderUpdate, Position, Query, Reply, SessionDay,
    SubmitResult, Update, terminal,
};
use crate::intent::{Intent, fmt_cents, hex};
use crate::protocol::{Obj, arr, quote};
use crate::safety::{date_string, et_day, et_ns, hhmm, parse_date};
use crate::session::wall_ns;
pub use market::Feed;
use market::{QuoteSource, Rec};
use script::{OnCancel, OnSubmit, Rule, Script};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

/// The simulator's model version, in every pin.
pub const MODEL: &str = "tinytrader-sim/7";

/// Nanodollars per cent.
const CENT: i64 = 10_000_000;

#[derive(Clone)]
pub(crate) struct SimLeg {
    symbol: String,
    ratio: u64,
    buy: bool,
    open: bool,
    filled: u64,
    cost: i128,
}

#[derive(Clone)]
pub(crate) struct SimOrder {
    id: String,
    cid: String,
    combo: bool,
    legs: Vec<SimLeg>,
    qty: u64,
    limit_cents: i64,
    filled: u64,
    cost: i128,
    status: String,
    rule: Rule,
    t_act: u64,
    seq: u64,
    active: bool,
    updated: u64,
    /// Legs last filled at zero for want of a bid (`no_bid_combo_leg = "zero"`).
    zero_legs: Vec<String>,
    /// Its legs' queues, once it rests under the queue model.
    queue: Option<Vec<queue::LegQueue>>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Quote {
    bid: Option<i64>,
    ask: Option<i64>,
    bid_sz: u64,
    ask_sz: u64,
    used_bid: u64,
    used_ask: u64,
    /// When it was applied, ns.
    ts: u64,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum Action {
    Activate(usize),
    Cancel(usize),
    Expire(usize),
}

pub(crate) struct Market {
    src: QuoteSource,
    book: HashMap<u32, Quote>,
    /// Instruments a quote record other than `tcbbo` has given a book.
    quoted: std::collections::HashSet<u32>,
    /// The ET day last checked for expired contracts.
    day: i64,
    /// Trade prints of the instant being applied, for the queue model.
    prints: Vec<(u32, i64, u64)>,
    /// Each instrument id's symbol, the reverse of `ids`.
    names: HashMap<u32, String>,
    /// Fed and clocked by a session in process: the market time, shared with the feed.
    fed: Option<std::sync::Arc<std::sync::atomic::AtomicU64>>,
    ids: HashMap<String, u32>,
    actions: Vec<(u64, u64, Action)>,
    n: u64,
}

/// The simulated broker.
pub struct SimBroker {
    s: Script,
    script_sha: Option<String>,
    now: u64,
    market: Option<Market>,
    orders: Vec<SimOrder>,
    submits: u64,
    cancels: u64,
    execs: u64,
    cash: i128,
    pos: BTreeMap<String, (i64, i128)>,
    activities: Vec<String>,
    /// Non-trade activities so far, for their ids.
    acts: u64,
    /// Reports waiting out the report latency: `(release time, update)`, in order.
    delayed: std::collections::VecDeque<(u64, Update)>,
    /// Under `report_latency_applies_to = "all"`: the account as each delayed report
    /// left it, by release time, and the account as of the last released one.
    views: std::collections::VecDeque<(u64, View)>,
    visible: View,
    /// The latency jitter's generator state (SplitMix64), from the profile's seed.
    rng: u64,
    /// The next session close at which options are settled, once known.
    settle_at: Option<u64>,
    /// The next time the venue's liquidation rule runs, once known.
    liquidate_at: Option<u64>,
    updates: Vec<Update>,
    streamed: u64,
    down: Option<Instant>,
    dropped: bool,
}

/// Decimal text of a nanodollar amount, at least two places.
pub(crate) fn fmt_nanos(v: i128) -> String {
    let sign = if v < 0 { "-" } else { "" };
    let a = v.unsigned_abs();
    let frac = format!("{:09}", a % 1_000_000_000);
    let frac = frac.trim_end_matches('0');
    format!("{sign}{}.{frac:0<2}", a / 1_000_000_000)
}

fn avg(cost: i128, n: u64) -> Option<String> {
    (n > 0).then(|| fmt_nanos(cost / i128::from(n)))
}

fn error(http: u16, code: &str, message: &str) -> BrokerError {
    BrokerError {
        http_status: http,
        code: code.into(),
        message: message.into(),
        raw: Obj::new().s("code", code).s("message", message).done(),
        retry_after_ms: None,
    }
}

impl SimBroker {
    /// Scripted mode from script JSON text.
    ///
    /// # Errors
    /// An invalid script.
    pub fn scripted(script: &str) -> Result<Self, String> {
        let mut s = Self::new(script)?;
        s.now = s.s.clock_ts.unwrap_or(0);
        Ok(s)
    }

    /// Market mode: quotes from a DBN file or `tcp:HOST:PORT`; the script supplies the
    /// profile, account and calendar (`{}` for defaults).
    ///
    /// # Errors
    /// An invalid script or an unreadable quote source.
    pub fn market(script: &str, quotes: &str) -> Result<Self, String> {
        let mut s = Self::new(script)?;
        let mut src = QuoteSource::open(quotes)?;
        let first = src.peek_ts()?.unwrap_or(0);
        s.market = Some(Market {
            src,
            book: HashMap::new(),
            quoted: std::collections::HashSet::new(),
            day: 0,
            prints: Vec::new(),
            names: HashMap::new(),
            fed: None,
            ids: HashMap::new(),
            actions: Vec::new(),
            n: 0,
        });
        s.run_to(first)?;
        Ok(s)
    }

    /// Market mode fed in process by the session that clocks it: records arrive through
    /// the returned [`Feed`], and market time moves only with `advance`. `pin` says what
    /// feeds it (the session's sources and their hashes), for `pins`.
    ///
    /// # Errors
    /// An invalid script.
    pub fn fed(script: &str, pin: &str) -> Result<(Self, Feed), String> {
        let mut s = Self::new(script)?;
        let floor = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let (src, feed) = QuoteSource::feed(pin.to_owned(), floor.clone());
        s.market = Some(Market {
            src,
            book: HashMap::new(),
            quoted: std::collections::HashSet::new(),
            day: 0,
            prints: Vec::new(),
            names: HashMap::new(),
            fed: Some(floor),
            ids: HashMap::new(),
            actions: Vec::new(),
            n: 0,
        });
        Ok((s, feed))
    }

    /// When a submit or cancel received at market time `t` takes effect: after the
    /// strategy's declared latency and then the venue's.
    fn effective(&mut self, key: &str) -> u64 {
        let ms = self.s.strategy_latency_ms.saturating_add(self.s.latency_ms);
        let jitter = self.jitter(key);
        self.now
            .saturating_add(ms.saturating_mul(1_000_000))
            .saturating_add(jitter)
    }

    /// The profile's random latency jitter for `key` (the venue's admission number of
    /// the submit or cancel), ns: uniform over `latency_jitter_ms`, from the seed and
    /// the key alone, so it doesn't depend on the order requests arrive in. Zero
    /// without jitter.
    fn jitter(&mut self, key: &str) -> u64 {
        let Some((lo, hi)) = self.s.jitter_ms else {
            return 0;
        };
        // FNV-1a of the key, mixed into the seed; then SplitMix64's finaliser.
        let h = key.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |h, b| {
            (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
        });
        let mut z = (self.rng ^ h).wrapping_add(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        let (lo, hi) = (lo * 1_000_000, hi * 1_000_000);
        lo + z % (hi - lo + 1)
    }

    /// Queues an update for its client: at once, or after the profile's report
    /// latency in market mode.
    pub(super) fn report(&mut self, update: Update) {
        let delay = self.s.report_latency_ms.saturating_mul(1_000_000);
        if (delay > 0 || self.down.is_some()) && self.market.is_some() {
            let at = self.now.saturating_add(delay);
            self.delayed.push_back((at, update));
            if self.s.report_latency_all && delay > 0 {
                let v = self.snapshot();
                self.views.push_back((at, v));
            }
        } else {
            self.updates.push(update);
        }
    }

    fn new(script: &str) -> Result<Self, String> {
        let s = Script::parse(script)?;
        let mut sim = Self {
            cash: i128::from(s.cash_cents) * i128::from(CENT),
            script_sha: Some(hex(&Sha256::digest(script.as_bytes()))),
            s,
            now: 0,
            market: None,
            orders: Vec::new(),
            submits: 0,
            cancels: 0,
            execs: 0,
            pos: BTreeMap::new(),
            activities: Vec::new(),
            acts: 0,
            delayed: std::collections::VecDeque::new(),
            views: std::collections::VecDeque::new(),
            visible: View::default(),
            rng: 0,
            settle_at: None,
            liquidate_at: None,
            updates: Vec::new(),
            streamed: 0,
            down: None,
            dropped: false,
        };
        sim.rng = sim.s.seed;
        let preload: Vec<_> = sim.s.orders.drain(..).collect();
        for p in preload {
            let leg = SimLeg {
                symbol: p.symbol,
                ratio: 1,
                buy: p.buy,
                open: true,
                filled: 0,
                cost: 0,
            };
            sim.create(
                p.client_order_id,
                false,
                vec![leg],
                p.qty,
                p.limit_cents,
                Rule::default(),
            );
            if let Some(o) = sim.orders.last_mut() {
                o.status = p.status;
            }
        }
        Ok(sim)
    }

    fn ts(&self) -> u64 {
        if self.s.clock_ts.is_some() || self.market.is_some() {
            self.now
        } else {
            wall_ns()
        }
    }

    fn days(&self, start: i64, end: i64) -> Option<Vec<SessionDay>> {
        let list = self.s.calendar.as_ref()?;
        if !self.s.weekdays {
            let inside =
                |d: &&SessionDay| parse_date(&d.date).is_some_and(|x| (start..=end).contains(&x));
            return Some(list.iter().filter(inside).cloned().collect());
        }
        let weekday = |d: i64| (1..=5).contains(&(d + 4).rem_euclid(7));
        Some(
            (start..=end.min(start + 400))
                .filter(|d| weekday(*d))
                .map(|d| SessionDay {
                    date: date_string(d),
                    open: "09:30".into(),
                    close: "16:00".into(),
                })
                .collect(),
        )
    }

    /// Open and close (ns) of the session on ET day `day`.
    pub(crate) fn session(&self, day: i64) -> Option<(u64, u64)> {
        let d = self.days(day, day)?.into_iter().next()?;
        Some((et_ns(day, hhmm(&d.open)?), et_ns(day, hhmm(&d.close)?)))
    }

    fn create(
        &mut self,
        cid: String,
        combo: bool,
        legs: Vec<SimLeg>,
        qty: u64,
        limit_cents: i64,
        rule: Rule,
    ) -> usize {
        let n = self.orders.len() + 1;
        self.orders.push(SimOrder {
            id: format!("00000000-0000-4000-8000-{n:012x}"),
            cid,
            combo,
            legs,
            qty,
            limit_cents,
            filled: 0,
            cost: 0,
            status: "new".into(),
            rule,
            t_act: self.now,
            seq: u64::try_from(n).unwrap_or(0),
            active: self.market.is_none(),
            updated: self.ts(),
            queue: None,
            zero_legs: Vec::new(),
        });
        n - 1
    }

    fn order_raw(o: &SimOrder) -> String {
        let side = |l: &SimLeg| if l.buy { "buy" } else { "sell" };
        let intent =
            |l: &SimLeg| format!("{}_to_{}", side(l), if l.open { "open" } else { "close" });
        let mut r = Obj::new()
            .s("id", &o.id)
            .s("client_order_id", &o.cid)
            .s("status", &o.status)
            .s("qty", &o.qty.to_string())
            .s("filled_qty", &o.filled.to_string())
            .opt_s("filled_avg_price", avg(o.cost, o.filled).as_deref())
            .s("order_class", if o.combo { "mleg" } else { "simple" })
            .s("limit_price", &fmt_cents(o.limit_cents));
        if o.combo {
            r = r.raw(
                "legs",
                &arr(o.legs.iter().map(|l| {
                    Obj::new()
                        .s("symbol", &l.symbol)
                        .s("side", side(l))
                        .s("ratio_qty", &l.ratio.to_string())
                        .s("position_intent", &intent(l))
                        .s("filled_qty", &l.filled.to_string())
                        .opt_s("filled_avg_price", avg(l.cost, l.filled).as_deref())
                        .done()
                })),
            );
        } else {
            let l = &o.legs[0];
            r = r
                .s("symbol", &l.symbol)
                .s("side", side(l))
                .s("position_intent", &intent(l))
                .raw("legs", "null");
        }
        if !o.zero_legs.is_empty() {
            let list: Vec<String> = o.zero_legs.iter().map(|s| format!("{s:?}")).collect();
            r = r.raw("no_bid_zero_legs", &format!("[{}]", list.join(",")));
        }
        if let Some(q) = &o.queue {
            r = r.raw("queue", &Self::queue_raw(q, &o.legs));
        }
        r.n("updated_at_ns", o.updated).done()
    }

    fn broker_order(&self, i: usize) -> BrokerOrder {
        let o = &self.orders[i];
        let legs = o.combo.then(|| {
            arr(o.legs.iter().map(|l| {
                Obj::new()
                    .s("symbol", &l.symbol)
                    .n("filled_qty", l.filled)
                    .opt_s("avg_price", avg(l.cost, l.filled).as_deref())
                    .done()
            }))
        });
        BrokerOrder {
            client_order_id: o.cid.clone(),
            broker_order_id: o.id.clone(),
            status: o.status.clone(),
            filled_qty: o.filled,
            avg_price: avg(o.cost, o.filled),
            legs,
            updated_ts: o.updated,
            raw: Self::order_raw(o),
        }
    }

    /// Queues one stream update, honouring a scripted stream drop.
    fn push(
        &mut self,
        i: usize,
        event: &str,
        fill: Option<String>,
        legs: Option<String>,
        position: Option<i64>,
    ) {
        if self.down.is_some() {
            return;
        }
        let order = self.broker_order(i);
        let ts = self.ts();
        let raw = Obj::new()
            .s("event", event)
            .n("timestamp_ns", ts)
            .raw("order", &order.raw)
            .done();
        self.report(Update::Order(Box::new(OrderUpdate {
            order,
            event: event.into(),
            ts_broker: ts,
            fill,
            legs,
            position_qty: position,
            reason: None,
            source: "sim",
            raw,
        })));
        self.streamed += 1;
        if let Some((n, _)) = self.s.drop_after
            && !self.dropped
            && self.streamed >= n
        {
            self.dropped = true;
            self.down = Some(Instant::now());
            self.updates.push(Update::StreamDown);
        }
    }

    fn set_status(&mut self, i: usize, status: &str) {
        status.clone_into(&mut self.orders[i].status);
        self.orders[i].updated = self.ts();
        self.push(i, status, None, None, None);
    }

    fn find(&self, t: &OrderRef) -> Option<usize> {
        self.orders.iter().position(|o| match t {
            OrderRef::Client(c) => o.cid == *c,
            OrderRef::Broker(b) => o.id == *b,
        })
    }

    fn open(&self, i: usize) -> bool {
        !terminal(&self.orders[i].status)
    }

    /// Script fill prices (nanodollars): the opposing quote, else the limit for a single.
    fn script_prices(&self, i: usize) -> Vec<i64> {
        let o = &self.orders[i];
        o.legs
            .iter()
            .map(|l| {
                let q = self.s.quotes.get(&l.symbol).copied().unwrap_or_default();
                let side = if l.buy { q.1 } else { q.0 };
                side.map_or(if o.combo { 0 } else { o.limit_cents }, |c| c) * CENT
            })
            .collect()
    }

    fn schedule(&mut self, t: u64, a: Action) {
        if let Some(m) = &mut self.market {
            m.n += 1;
            m.actions.push((t, m.n, a));
        }
    }

    fn cancel_one(&mut self, i: usize) -> CancelResult {
        self.cancels += 1;
        if !self.open(i) {
            return CancelResult::Rejected(error(422, "42210000", "order is not cancelable"));
        }
        match self.orders[i].rule.cancel {
            OnCancel::Ignore => {}
            OnCancel::Reject => {
                return CancelResult::Rejected(error(422, "42210000", "order is not cancelable"));
            }
            OnCancel::Ok if self.market.is_some() => {
                let key = format!("cancel-{}", self.cancels);
                let t = self.effective(&key);
                self.schedule(t, Action::Cancel(i));
            }
            OnCancel::Ok => self.set_status(i, "canceled"),
        }
        CancelResult::Accepted("null".into())
    }

    #[expect(
        clippy::unnecessary_wraps,
        reason = "the simulator answers every query"
    )]
    fn reply<T>(value: T, raw: String) -> Query<T> {
        Ok(Reply { value, raw })
    }
}

impl Broker for SimBroker {
    fn kind(&self) -> &'static str {
        "sim"
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            combo: true,
            // Until the simulator models futures (margin, hours, settlement).
            futures: false,
            max_legs: 4,
            client_order_id_max: 128,
            idempotency: "broker_client_id",
        }
    }

    fn pins(&self) -> String {
        let clock = match (&self.market, self.s.clock_ts) {
            (Some(_), _) => "client_clocked_market",
            (None, Some(_)) => "fixed",
            (None, None) => "wall",
        };
        let calendar = match (&self.s.calendar, self.s.weekdays) {
            (None, _) => "none",
            (Some(_), true) => "weekdays-0930-1600",
            (Some(_), false) => "script",
        };
        Obj::new()
            .s("model", MODEL)
            .s(
                "mode",
                if self.market.is_some() {
                    "market"
                } else {
                    "scripted"
                },
            )
            .s("clock", clock)
            .raw(
                "quotes",
                self.market.as_ref().map_or("null", |m| m.src.pin.as_str()),
            )
            .n("latency_ms", self.s.latency_ms)
            .n("strategy_latency_ms", self.s.strategy_latency_ms)
            .n("report_latency_ms", self.s.report_latency_ms)
            .s(
                "report_latency_applies_to",
                if self.s.report_latency_all {
                    "all"
                } else {
                    "stream"
                },
            )
            .raw(
                "latency_jitter_ms",
                &self
                    .s
                    .jitter_ms
                    .map_or("null".into(), |(a, b)| format!("[{a},{b}]")),
            )
            .n("seed", self.s.seed)
            .s("fee_per_contract", &fmt_cents(self.s.fee_cents))
            .s("fee_per_share", &fmt_nanos(i128::from(self.s.fee_share)))
            .raw("expiry", &self.expiry_pin())
            .raw("venue_rules", &self.rules_pin())
            .raw(
                "queue",
                &self.s.queue.map_or("null".into(), |q| {
                    Obj::new()
                        .s(
                            "ahead",
                            &format!("{}.{:03}", q.ahead_permille / 1000, q.ahead_permille % 1000),
                        )
                        .s(
                            "fill_on_print",
                            if q.through_only {
                                "through"
                            } else {
                                "at_or_through"
                            },
                        )
                        .s("combo", if q.combo_implied { "implied" } else { "touch" })
                        .n("combo_window_s", q.window / 1_000_000_000)
                        .done()
                }),
            )
            .s(
                "no_bid_combo_leg",
                if self.s.no_bid_zero { "zero" } else { "refuse" },
            )
            .s(
                "fill_on",
                if self.s.next_quote {
                    "next_quote"
                } else {
                    "book"
                },
            )
            .s(
                "size_cap",
                if self.s.size_cap { "displayed" } else { "none" },
            )
            .s("calendar", calendar)
            .opt_s("script_sha256", self.script_sha.as_deref())
            .done()
    }

    fn now(&self) -> Option<u64> {
        Some(self.ts())
    }

    fn submit(&mut self, intent: &Intent) -> SubmitResult {
        self.submits += 1;
        let rule = self.s.rule_for(self.submits, &intent.client_order_id);
        if self.orders.iter().any(|o| o.cid == intent.client_order_id) {
            return SubmitResult::Rejected(error(
                422,
                "40010001",
                "client_order_id must be unique",
            ));
        }
        match &rule.submit {
            OnSubmit::Reject(http, code, msg) => {
                return SubmitResult::Rejected(error(*http, code, msg));
            }
            OnSubmit::ErrorBeforeSend => {
                return SubmitResult::Unknown {
                    reason: "connection",
                    retry_after_ms: None,
                };
            }
            OnSubmit::Http429(retry) => {
                return SubmitResult::Unknown {
                    reason: "http_429_after_send",
                    retry_after_ms: *retry,
                };
            }
            OnSubmit::Stall => loop {
                std::thread::sleep(Duration::from_hours(1));
            },
            _ => {}
        }
        if let Some(m) = &self.market {
            let today = et_day(self.now);
            if intent.legs.iter().any(|l| self.expired(&l.symbol, today)) {
                return SubmitResult::Rejected(error(
                    422,
                    "sim_expired_instrument",
                    "an option leg has expired",
                ));
            }
            if !intent.legs.iter().all(|l| m.ids.contains_key(&l.symbol)) {
                return SubmitResult::Rejected(error(
                    422,
                    "sim_unknown_instrument",
                    "no definition seen",
                ));
            }
            if !self
                .session(et_day(self.now))
                .is_some_and(|(o, c)| (o..c).contains(&self.now))
            {
                return SubmitResult::Rejected(error(
                    422,
                    "sim_market_closed",
                    "outside the session",
                ));
            }
            if let Some((code, message)) = self.refuse(intent) {
                return SubmitResult::Rejected(error(403, code, &message));
            }
        }
        let legs = intent
            .legs
            .iter()
            .map(|l| SimLeg {
                symbol: l.symbol.clone(),
                ratio: u64::from(l.ratio),
                buy: l.buy,
                open: l.open,
                filled: 0,
                cost: 0,
            })
            .collect();
        let lost = rule.submit == OnSubmit::TimeoutAfterAccept;
        let submit = rule.submit.clone();
        let i = self.create(
            intent.client_order_id.clone(),
            intent.combo,
            legs,
            u64::from(intent.qty),
            // A market order against quotes takes any price (a sell's at least
            // a cent); scripted, it fills at its reference price.
            if intent.market && self.market.is_some() {
                if !intent.combo && !intent.legs[0].buy {
                    1
                } else {
                    i64::MAX / CENT
                }
            } else {
                intent.limit_cents
            },
            rule,
        );
        let accepted = self.broker_order(i);
        if !lost {
            self.push(i, "new", None, None, None);
        }
        if self.market.is_some() {
            let t = self.effective(&format!("order-{}", self.submits));
            self.orders[i].t_act = t;
            self.schedule(t, Action::Activate(i));
        } else {
            match submit {
                OnSubmit::Fill => self.execute(i, u64::from(intent.qty), &self.script_prices(i)),
                OnSubmit::Partial(n) => self.execute(i, n, &self.script_prices(i)),
                _ => {}
            }
        }
        if lost {
            return SubmitResult::Unknown {
                reason: "timeout",
                retry_after_ms: None,
            };
        }
        SubmitResult::Accepted(accepted)
    }

    /// Cancels every open order, then closes each position at once: at its
    /// touch (bid to sell, ask to buy) in market mode, else at the scripted
    /// quote's mid, else at its average cost. Each close is an order with a
    /// fill, as Alpaca's liquidation produces.
    fn flatten(&mut self) -> FlattenResult {
        let _ = self.cancel_all();
        // Scripted positions join the book first so their closes net to zero.
        for (symbol, qty) in std::mem::take(&mut self.s.positions) {
            self.pos.entry(symbol).or_insert((0, 0)).0 += qty;
        }
        let held: Vec<(String, i64, i128)> = self
            .pos
            .iter()
            .filter(|(_, p)| p.0 != 0)
            .map(|(s, p)| (s.clone(), p.0, p.1))
            .collect();
        let mut closings = Vec::new();
        for (symbol, qty, cost) in held {
            let sell = qty > 0;
            let touch = self.market.as_ref().and_then(|m| {
                let q = m.book.get(m.ids.get(&symbol)?)?;
                if sell { q.bid } else { q.ask }
            });
            let mid = self.s.quotes.get(&symbol).and_then(|q| match q {
                (Some(b), Some(a)) => Some((b + a) / 2 * CENT),
                (Some(x), None) | (None, Some(x)) => Some(x * CENT),
                _ => None,
            });
            let avg = i64::try_from(cost / i128::from(qty)).unwrap_or(0).abs();
            let px = touch.or(mid).unwrap_or(avg);
            let cid = format!("flatten-{}-{symbol}", self.orders.len() + 1);
            let legs = vec![SimLeg {
                symbol: symbol.clone(),
                ratio: 1,
                buy: !sell,
                open: false,
                filled: 0,
                cost: 0,
            }];
            let i = self.create(cid, false, legs, qty.unsigned_abs(), 0, Rule::default());
            self.push(i, "new", None, None, None);
            self.execute(i, qty.unsigned_abs(), &[px]);
            closings.push(Closing {
                symbol,
                status: 200,
                raw: Self::order_raw(&self.orders[i]),
            });
        }
        let raw = arr(closings.iter().map(|c| {
            Obj::new()
                .s("symbol", &c.symbol)
                .n("status", c.status)
                .raw("body", &c.raw)
                .done()
        }));
        FlattenResult::Sent(closings, raw)
    }

    fn cancel(&mut self, broker_order_id: &str) -> CancelResult {
        match self.find(&OrderRef::Broker(broker_order_id.into())) {
            Some(i) => self.cancel_one(i),
            None => CancelResult::Rejected(error(404, "40410000", "order not found")),
        }
    }

    fn cancel_all(&mut self) -> CancelResult {
        for i in 0..self.orders.len() {
            if self.open(i) {
                let _ = self.cancel_one(i);
            }
        }
        CancelResult::Accepted("[]".into())
    }

    fn order(&mut self, target: &OrderRef) -> Query<Option<BrokerOrder>> {
        // A scripted fill on query happens to the live state, before any view.
        if let Some(i) = self.find(target)
            && self.orders[i].rule.fill_on_query
            && self.open(i)
        {
            let units = self.orders[i].qty - self.orders[i].filled;
            self.execute(i, units, &self.script_prices(i));
        }
        let saved = self.enter_view();
        let r = self.order_now(target);
        self.leave_view(saved);
        r
    }

    fn open_orders(&mut self) -> Query<Vec<BrokerOrder>> {
        let saved = self.enter_view();
        let r = self.open_orders_now();
        self.leave_view(saved);
        r
    }

    fn orders_since(&mut self, after_ts: u64) -> Query<Vec<BrokerOrder>> {
        let saved = self.enter_view();
        let r = self.orders_since_now(after_ts);
        self.leave_view(saved);
        r
    }

    fn account(&mut self) -> Query<AccountInfo> {
        let saved = self.enter_view();
        let r = self.account_now();
        self.leave_view(saved);
        r
    }

    fn positions(&mut self) -> Query<Vec<Position>> {
        let saved = self.enter_view();
        let r = self.positions_now();
        self.leave_view(saved);
        r
    }

    fn activities(
        &mut self,
        types: &[String],
        _after: Option<&str>,
        page_token: Option<&str>,
    ) -> Query<(Vec<String>, Option<String>)> {
        let saved = self.enter_view();
        let r = self.activities_now(types, _after, page_token);
        self.leave_view(saved);
        r
    }

    fn clock(&mut self) -> Query<MarketClock> {
        let ts = self.ts();
        let today = et_day(ts);
        let sessions: Vec<(u64, u64)> = (today..today + 10)
            .filter_map(|d| self.session(d))
            .collect();
        let is_open = sessions.iter().any(|(o, c)| (*o..*c).contains(&ts));
        let next_open = sessions.iter().map(|s| s.0).find(|o| *o > ts).unwrap_or(0);
        let next_close = sessions.iter().map(|s| s.1).find(|c| *c > ts).unwrap_or(0);
        let raw = Obj::new()
            .n("timestamp_ns", ts)
            .b("is_open", is_open)
            .n("next_open_ns", next_open)
            .n("next_close_ns", next_close)
            .done();
        Self::reply(
            MarketClock {
                ts,
                is_open,
                next_open,
                next_close,
            },
            raw,
        )
    }

    fn calendar(&mut self, start: &str, end: &str) -> Query<Vec<SessionDay>> {
        let (Some(a), Some(b)) = (parse_date(start), parse_date(end)) else {
            return Err(error(422, "40010000", "invalid dates"));
        };
        let Some(days) = self.days(a, b) else {
            return Err(error(404, "sim_no_calendar", "no calendar configured"));
        };
        let raw = arr(days.iter().map(|d| {
            Obj::new()
                .s("date", &d.date)
                .s("open", &d.open)
                .s("close", &d.close)
                .done()
        }));
        Self::reply(days, raw)
    }

    fn poll_updates(&mut self, out: &mut Vec<Update>) {
        let down_for = Duration::from_millis(self.s.drop_after.map_or(0, |d| d.1));
        if self.down.is_some_and(|t| t.elapsed() >= down_for) {
            self.down = None;
            self.updates.push(Update::StreamUp);
        }
        // Reports whose latency has passed by the market time, in time order; while
        // the stream is down they wait.
        while self.down.is_none() && self.delayed.front().is_some_and(|(t, _)| *t <= self.now) {
            if let Some((_, u)) = self.delayed.pop_front() {
                self.updates.push(u);
            }
        }
        out.append(&mut self.updates);
    }

    fn session_clocked(&self) -> bool {
        self.market.as_ref().is_some_and(|m| m.fed.is_some())
    }

    fn advance(&mut self, to_ts: u64) -> Result<u64, BrokerError> {
        if self.market.is_none() && self.s.clock_ts.is_none() {
            return Err(BrokerError::local(
                "unsupported",
                "advance needs a client-clocked simulator",
            ));
        }
        self.run_to(to_ts)
            .map_err(|e| error(500, "sim_quote_source", &e))?;
        Ok(self.now)
    }
}

impl SimBroker {
    /// Applies quote records and order actions up to `to` in the section 10 order.
    fn run_to(&mut self, to: u64) -> Result<(), String> {
        // A fed simulator's clock starts at zero: rules run from its first day.
        let from = if self.now == 0 { to } else { self.now };
        if self.settle_at.is_none() && self.market.is_some() && self.s.expiry.settle {
            self.settle_at = self.next_close(et_day(from));
        }
        if self.liquidate_at.is_none() && self.market.is_some() {
            self.liquidate_at = self.next_liquidation(et_day(from));
        }
        while let Some(m) = &mut self.market {
            // The venue's liquidation time, as the close below: after everything at or
            // before it.
            if let Some(c) = self.liquidate_at.filter(|c| *c <= to) {
                let rec = m.src.peek_ts()?.filter(|t| *t <= c);
                let act = m.actions.iter().any(|a| a.0 <= c);
                if rec.is_none() && !act {
                    self.now = self.now.max(c);
                    self.liquidate(c);
                    self.liquidate_at = self.next_liquidation(et_day(c) + 1);
                    continue;
                }
            }
            let Some(m) = &mut self.market else { break };
            // A session's close settles that day's expiries: after every record and
            // order action at or before it, before anything after it.
            if let Some(c) = self.settle_at.filter(|c| *c <= to) {
                let rec = m.src.peek_ts()?.filter(|t| *t <= c);
                let act = m.actions.iter().any(|a| a.0 <= c);
                if rec.is_none() && !act {
                    self.now = self.now.max(c);
                    self.settle_day(et_day(c), c)?;
                    self.settle_at = self.next_close(et_day(c) + 1);
                    continue;
                }
            }
            let rec = m.src.peek_ts()?.filter(|t| *t <= to);
            let act = m
                .actions
                .iter()
                .enumerate()
                .filter(|(_, a)| a.0 <= to)
                .min_by_key(|(_, a)| (a.0, a.1))
                .map(|(k, a)| (k, a.0));
            match (rec, act) {
                (_, Some((k, t))) if rec.is_none_or(|r| t < r) => {
                    let (_, _, a) = m.actions.remove(k);
                    self.roll_day(t);
                    self.now = self.now.max(t);
                    self.act(a);
                }
                (Some(_), _) => {
                    // Every record of one timestamp applies before orders are evaluated;
                    // the last quote of an instrument in that instant is its book. A
                    // combo never fills on some legs' new quotes and others' old ones,
                    // nor on a quote replaced within the same instant.
                    let Some((t, r)) = m.src.next() else { break };
                    self.roll_day(t);
                    self.now = self.now.max(t);
                    let mut touched = Vec::new();
                    let mut next = Some(r);
                    while let Some(r) = next.take() {
                        touched.extend(self.apply(r));
                        if let Some(m) = &mut self.market
                            && m.src.peek_ts()? == Some(t)
                        {
                            next = m.src.next().map(|(_, r)| r);
                        }
                    }
                    self.evaluate_touched(&touched);
                    // Then the instant's trade prints, against resting orders' queues.
                    let prints = self.market.as_mut().map(|m| std::mem::take(&mut m.prints));
                    for (iid, px, size) in prints.unwrap_or_default() {
                        self.on_print(iid, px, size);
                    }
                }
                _ => break,
            }
        }
        self.roll_day(to);
        self.now = self.now.max(to);
        if let Some(floor) = self.market.as_ref().and_then(|m| m.fed.as_ref()) {
            floor.store(self.now, std::sync::atomic::Ordering::SeqCst);
        }
        Ok(())
    }

    /// The close of the first session on or after ET day `day`, within a fortnight.
    fn next_close(&self, day: i64) -> Option<u64> {
        (day..day + 14)
            .filter_map(|d| self.session(d))
            .map(|(_, c)| c)
            .find(|c| *c >= self.now)
    }

    /// At the first market time of a new ET day, before anything of that day is
    /// applied: every quote is cleared, so nothing fills on yesterday's book before
    /// today's first quote; and contracts past their expiry are forgotten, as their
    /// ids may name other contracts by now. Mappings stay: they arrive the evening
    /// before.
    fn roll_day(&mut self, t: u64) {
        let today = et_day(t);
        let Some(m) = &mut self.market else { return };
        if m.day >= today {
            return;
        }
        if m.day != 0 {
            m.book.clear();
            m.quoted.clear();
        }
        m.day = today;
        let symbols: Vec<String> = m.ids.keys().cloned().collect();
        let gone: Vec<String> = symbols
            .into_iter()
            .filter(|s| self.expired(s, today))
            .collect();
        let Some(m) = &mut self.market else { return };
        for s in gone {
            if let Some(i) = m.ids.remove(&s) {
                m.names.remove(&i);
                m.book.remove(&i);
                m.quoted.remove(&i);
            }
        }
    }

    /// The expiry settings, for `pins`.
    fn expiry_pin(&self) -> String {
        let e = &self.s.expiry;
        let styles = e
            .styles
            .iter()
            .fold(Obj::new(), |o, (k, v)| o.s(k, &format!("{v:?}")))
            .done();
        let divs = e
            .dividends_sha
            .iter()
            .fold(Obj::new(), |o, (k, v)| o.s(k, v))
            .done();
        Obj::new()
            .b("settle", e.settle)
            .s(
                "model",
                "settle at the session close: cash at intrinsic, physical at the strike",
            )
            .raw("styles", &styles)
            .opt_s("settlement_file_sha256", e.prices_sha.as_deref())
            .b("settle_from_quote", e.from_quote)
            .s("exercise_threshold", &fmt_nanos(i128::from(e.threshold)))
            .b("early_assignment", e.early)
            .raw("dividends_sha256", &divs)
            .opt_s(
                "put_assignment_extrinsic",
                e.put_extrinsic.map(|p| fmt_nanos(i128::from(p))).as_deref(),
            )
            .n("quote_max_age_s", e.max_age / 1_000_000_000)
            .done()
    }

    /// Applies one record to the book; a quote returns its instrument id.
    fn apply(&mut self, r: Rec) -> Option<u32> {
        let now = self.now;
        let m = self.market.as_mut()?;
        match r {
            Rec::Def { iid, symbol } => {
                // OPRA assigns instrument ids per day: an id can name another contract
                // tomorrow, and a contract another id. Whatever an id meant before is
                // forgotten, its quote included, so no order fills against another
                // contract's book.
                if let Some(was) = m.names.insert(iid, symbol.clone()).filter(|w| *w != symbol) {
                    // The id named another contract: forget that one and its quote.
                    m.ids.remove(&was);
                    m.book.remove(&iid);
                    m.quoted.remove(&iid);
                }
                if let Some(old) = m.ids.insert(symbol, iid).filter(|old| *old != iid) {
                    // The contract had another id: that id's quote isn't its own now.
                    m.names.remove(&old);
                    m.book.remove(&old);
                    m.quoted.remove(&old);
                }
                None
            }
            Rec::Quote {
                iid,
                bid,
                ask,
                bid_sz,
                ask_sz,
                print,
            } => {
                let trade = print.is_some();
                // A print counts only inside its own pre-trade quote: outside it are
                // mostly complex-order legs, intermarket sweeps, auctions and late
                // reports, which a resting simple order wouldn't have traded with.
                if let (Some((px, size)), Some(b), Some(a)) = (print, bid, ask)
                    && px != dbn::UNDEF_PRICE
                    && b <= px
                    && px <= a
                {
                    m.prints.push((iid, px, size));
                }
                // A tcbbo quote is the book only where no quote record gives one: beside
                // cmbp-1 it would put back a quote a trade may have consumed.
                if trade && m.quoted.contains(&iid) {
                    return None;
                }
                if !trade {
                    m.quoted.insert(iid);
                }
                m.book.insert(
                    iid,
                    Quote {
                        bid,
                        ask,
                        bid_sz,
                        ask_sz,
                        used_bid: 0,
                        used_ask: 0,
                        ts: now,
                    },
                );
                Some(iid)
            }
        }
    }

    /// Evaluates the working orders with a leg on any of `iids`, in (`t_act`, `seq`) order.
    fn evaluate_touched(&mut self, iids: &[u32]) {
        let Some(m) = &self.market else { return };
        let ids = &m.ids;
        let mut touched: Vec<usize> = (0..self.orders.len())
            .filter(|i| {
                self.orders[*i]
                    .legs
                    .iter()
                    .any(|l| ids.get(&l.symbol).is_some_and(|id| iids.contains(id)))
            })
            .collect();
        touched.sort_by_key(|i| (self.orders[*i].t_act, self.orders[*i].seq));
        for i in touched {
            self.evaluate(i);
        }
    }

    fn act(&mut self, a: Action) {
        match a {
            Action::Activate(i) if self.open(i) => {
                self.orders[i].active = true;
                if let Some((_, close)) = self.session(et_day(self.now)) {
                    self.schedule(close.max(self.now), Action::Expire(i));
                }
                // fill_on = "next_quote": wait for the first quote after activation, as
                // a bar's quotes are the past by the time an order reaches the venue.
                if !self.s.next_quote {
                    self.evaluate(i);
                }
            }
            Action::Cancel(i) if self.open(i) => self.set_status(i, "canceled"),
            Action::Expire(i) if self.open(i) => self.set_status(i, "expired"),
            _ => {}
        }
    }
}

impl SimBroker {
    fn order_now(&mut self, target: &OrderRef) -> Query<Option<BrokerOrder>> {
        let Some(i) = self.find(target) else {
            return Self::reply(None, error(404, "40410000", "order not found").raw);
        };
        let o = self.broker_order(i);
        let raw = o.raw.clone();
        Self::reply(Some(o), raw)
    }

    fn open_orders_now(&mut self) -> Query<Vec<BrokerOrder>> {
        let list: Vec<_> = (0..self.orders.len())
            .filter(|i| self.open(*i))
            .map(|i| self.broker_order(i))
            .collect();
        let raw = arr(list.iter().map(|o| o.raw.clone()));
        Self::reply(list, raw)
    }

    fn orders_since_now(&mut self, after_ts: u64) -> Query<Vec<BrokerOrder>> {
        let list: Vec<_> = (0..self.orders.len())
            .filter(|i| self.orders[*i].updated >= after_ts)
            .map(|i| self.broker_order(i))
            .collect();
        let raw = arr(list.iter().map(|o| o.raw.clone()));
        Self::reply(list, raw)
    }

    fn account_now(&mut self) -> Query<AccountInfo> {
        let cash = fmt_nanos(self.cash);
        let power = if self.s.venue.defined_risk {
            fmt_nanos(self.buying_power())
        } else {
            cash.clone()
        };
        let raw = Obj::new()
            .s("status", &self.s.status)
            .n("options_trading_level", self.s.level)
            .s("cash", &cash)
            .s("buying_power", &power)
            .b("trading_blocked", self.s.trading_blocked)
            .raw(
                "sim",
                &Obj::new()
                    .n("submits", self.submits)
                    .n("cancels", self.cancels)
                    .done(),
            )
            .done();
        let info = AccountInfo {
            status: self.s.status.clone(),
            cash: cash.clone(),
            buying_power: power,
            options_trading_level: self.s.level,
            blocked: self.s.trading_blocked,
        };
        Self::reply(info, raw)
    }

    fn positions_now(&mut self) -> Query<Vec<Position>> {
        let mut list: Vec<Position> = self
            .pos
            .iter()
            .filter(|(_, p)| p.0 != 0)
            .map(|(s, p)| Position {
                symbol: s.clone(),
                qty: p.0,
                avg_entry_price: fmt_nanos((p.1 / i128::from(p.0)).abs()),
            })
            .collect();
        list.extend(self.s.positions.iter().map(|(s, q)| Position {
            symbol: s.clone(),
            qty: *q,
            avg_entry_price: "0.00".into(),
        }));
        let raw = arr(list.iter().map(|p| {
            Obj::new()
                .s("symbol", &p.symbol)
                .s("qty", &p.qty.to_string())
                .s("avg_entry_price", &p.avg_entry_price)
                .done()
        }));
        Self::reply(list, raw)
    }

    fn activities_now(
        &mut self,
        types: &[String],
        _after: Option<&str>,
        page_token: Option<&str>,
    ) -> Query<(Vec<String>, Option<String>)> {
        let start: usize = page_token.and_then(|t| t.parse().ok()).unwrap_or(0);
        let wanted = |a: &&String| {
            types.is_empty()
                || types
                    .iter()
                    .any(|t| a.contains(&format!("\"activity_type\":{}", quote(t))))
        };
        let all: Vec<String> = self.activities.iter().filter(wanted).cloned().collect();
        let page: Vec<String> = all.iter().skip(start).take(100).cloned().collect();
        let next = (start + page.len() < all.len()).then(|| (start + page.len()).to_string());
        let raw = arr(page.clone());
        Self::reply((page, next), raw)
    }
}

/// The account as a report left it: what `report_latency_applies_to = "all"` serves
/// queries from until the report is released. Orders settled by then can't have
/// changed since, so only the open ones are kept.
#[derive(Clone, Default)]
struct View {
    orders: usize,
    open: Vec<(usize, SimOrder)>,
    cash: i128,
    pos: BTreeMap<String, (i64, i128)>,
    activities: usize,
}

/// The live state a view displaced, to put back.
struct Displaced {
    tail: Vec<SimOrder>,
    open: Vec<(usize, SimOrder)>,
    cash: i128,
    pos: BTreeMap<String, (i64, i128)>,
    activities: Vec<String>,
}

impl SimBroker {
    fn snapshot(&self) -> View {
        View {
            orders: self.orders.len(),
            open: (0..self.orders.len())
                .filter(|i| self.open(*i))
                .map(|i| (i, self.orders[i].clone()))
                .collect(),
            cash: self.cash,
            pos: self.pos.clone(),
            activities: self.activities.len(),
        }
    }

    /// Serves queries from the account as of the last released report, while
    /// reports are still delayed under `report_latency_applies_to = "all"`.
    fn enter_view(&mut self) -> Option<Displaced> {
        while self.views.front().is_some_and(|(t, _)| *t <= self.now) {
            if let Some((_, v)) = self.views.pop_front() {
                self.visible = v;
            }
        }
        if self.views.is_empty() {
            return None;
        }
        let v = self.visible.clone();
        let tail = self.orders.split_off(v.orders.min(self.orders.len()));
        let open = v
            .open
            .into_iter()
            .map(|(i, o)| (i, std::mem::replace(&mut self.orders[i], o)))
            .collect();
        Some(Displaced {
            tail,
            open,
            cash: std::mem::replace(&mut self.cash, v.cash),
            pos: std::mem::replace(&mut self.pos, v.pos),
            activities: self
                .activities
                .split_off(v.activities.min(self.activities.len())),
        })
    }

    fn leave_view(&mut self, saved: Option<Displaced>) {
        let Some(d) = saved else { return };
        for (i, o) in d.open {
            self.orders[i] = o;
        }
        self.orders.extend(d.tail);
        self.cash = d.cash;
        self.pos = d.pos;
        self.activities.extend(d.activities);
    }
}

#[cfg(test)]
mod tests {
    use super::SimBroker;

    fn draws(script: &str) -> Vec<u64> {
        let (mut sim, _feed) = SimBroker::fed(script, "{}").unwrap();
        (0..200).map(|k| sim.jitter(&format!("s-{k}"))).collect()
    }

    /// Jitter is uniform within its bounds, the same for the same seed, and
    /// different for another; none without jitter configured.
    #[test]
    fn jitter_is_seeded_and_bounded() {
        let a = draws(r#"{"latency_jitter_ms":{"min":2,"max":20},"seed":7}"#);
        assert_eq!(
            a,
            draws(r#"{"latency_jitter_ms":{"min":2,"max":20},"seed":7}"#)
        );
        assert_ne!(
            a,
            draws(r#"{"latency_jitter_ms":{"min":2,"max":20},"seed":8}"#)
        );
        assert!(a.iter().all(|j| (2_000_000..=20_000_000).contains(j)));
        assert!(a.iter().any(|j| *j < 6_000_000) && a.iter().any(|j| *j > 16_000_000));
        assert!(draws("{}").iter().all(|j| *j == 0));
    }
}
