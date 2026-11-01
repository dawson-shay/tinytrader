//! Futures in the simulator: the CME equity index outrights the gateway knows
//! (`crate::intent::future`).
//!
//! - **Hours.** Globex, in New York time: from 18:00 to 17:00 the next day, with a halt
//!   from 17:00 to 18:00, closed from Friday 17:00 to Sunday 18:00. A session from 18:00
//!   belongs to the next day (its trade date), which must be a trading day of the venue's
//!   calendar: the calendar's holidays close futures too. CME's own early halts on
//!   holidays aren't modelled.
//! - **Day orders** expire at the trade date's 17:00.
//! - **Cash** moves by fees and realized profit and loss only: a fill doesn't pay its
//!   notional. Opening adds to the position at its price; reducing it realizes
//!   (price − average) × contracts × the multiplier.
//! - **Margin.** With `futures_margin = {"ES": "20000.00"}` (initial margin per
//!   contract), an opening order is refused (`sim_insufficient_margin`) unless the
//!   cash covers the margin of the root's held and working contracts with it, besides
//!   what the venue's options rules hold. Buying power is net of the margin held.
//! - **Caps.** An opening order is refused (`sim_future_position`) when the root's held
//!   and working contracts with it would exceed `futures_max_position` (10 by default),
//!   counted gross: a long in one month and a short in another both count. A limit order
//!   must be within `futures_price_band` (0.05 by default) of the last fresh mid
//!   (`sim_price_band`; with no fresh quote, `sim_no_quote`).
//! - **Expiry.** A contract trades until 09:30 on its third Friday, when what is held
//!   settles in cash at the final settlement price: the settlement file's `AM-SOQ` for
//!   the future's root, else for its index (SPX for ES and MES, NDX for NQ and MNQ, RUT
//!   for RTY and M2K), else (with `expiry.from_quote`) its own last fresh mid. Each
//!   settlement is a `SETTLE` activity; working orders on the contract expire.
use super::{CENT, SimBroker, fmt_nanos};
use crate::broker::Update;
use crate::intent::{Intent, future, future_expiry, future_index, future_multiplier};
use crate::protocol::Obj;
use crate::safety::{date_string, et_day, et_ns};

/// The daily halt and the reopening, minutes after midnight New York.
const HALT: i64 = 17 * 60;
const REOPEN: i64 = 18 * 60;
/// When an expiring contract stops trading and settles.
const SETTLE: i64 = 9 * 60 + 30;

/// Whether `symbol` is a known futures outright.
pub(super) fn is_future(symbol: &str) -> bool {
    future(symbol).is_some()
}

/// A futures symbol's root.
fn root(symbol: &str) -> Option<&str> {
    future(symbol).map(|(r, _, _)| r)
}

/// Dollars per point of a futures symbol; 1 for anything else.
pub(super) fn multiplier(symbol: &str) -> i128 {
    root(symbol)
        .and_then(future_multiplier)
        .map_or(1, i128::from)
}

/// Minutes after midnight New York of `t`.
fn minutes(t: u64) -> i64 {
    let midnight = et_ns(et_day(t), 0);
    i64::try_from(t.saturating_sub(midnight) / 60_000_000_000).unwrap_or(0)
}

impl SimBroker {
    /// The trade date (ET day) of market time `t`: from 18:00, the next day's.
    pub(super) fn trade_date(&self, t: u64) -> i64 {
        let d = et_day(t);
        if minutes(t) >= REOPEN { d + 1 } else { d }
    }

    /// Whether futures trade at `t`.
    pub(super) fn futures_open(&self, t: u64) -> bool {
        !(HALT..REOPEN).contains(&minutes(t)) && self.session(self.trade_date(t)).is_some()
    }

    /// When a futures day order sent at `t` expires: its trade date's 17:00, or the
    /// next trading day's when sent while closed.
    pub(super) fn futures_close(&self, t: u64) -> Option<u64> {
        let from = self.trade_date(t);
        (from..from + 14)
            .filter(|d| self.session(*d).is_some())
            .map(|d| et_ns(d, HALT))
            .find(|c| *c >= t)
    }

    /// Whether a futures contract has stopped trading by ET day `day`, at the market
    /// time: after 09:30 on its expiry day, or any day after.
    pub(super) fn future_expired(&self, symbol: &str, day: i64) -> bool {
        future_expiry(symbol, day)
            .is_some_and(|e| e < day || (e == day && self.now >= et_ns(e, SETTLE)))
    }

    /// Contracts of `root` held and working (opening orders' remaining quantity), and
    /// `extra` more: gross.
    fn future_contracts(&self, root_of: &str, extra: u64) -> u64 {
        let held: u64 = self
            .pos
            .iter()
            .filter(|(s, _)| root(s) == Some(root_of))
            .map(|(_, p)| p.0.unsigned_abs())
            .sum();
        let working: u64 = self
            .orders
            .iter()
            .enumerate()
            .filter(|(i, o)| {
                self.open(*i)
                    && o.legs
                        .iter()
                        .any(|l| l.open && root(&l.symbol) == Some(root_of))
            })
            .map(|(_, o)| o.qty - o.filled)
            .sum();
        held + working + extra
    }

    /// Initial margin held for futures, nanodollars, with `extra` contracts of
    /// `root_of` more.
    pub(super) fn futures_margin_held(&self, extra: Option<(&str, u64)>) -> i128 {
        self.s
            .futures_margin
            .iter()
            .map(|(r, m)| {
                let more = extra.filter(|(e, _)| e == r).map_or(0, |(_, n)| n);
                m * i128::from(self.future_contracts(r, more))
            })
            .sum()
    }

    /// The futures checks of a submit, if `intent` is a future: its hours, the price
    /// band, and for an opening the position cap and the margin.
    pub(super) fn refuse_future(&self, intent: &Intent) -> Option<(&'static str, String)> {
        if !intent.is_future() {
            return None;
        }
        let leg = &intent.legs[0];
        let symbol = &leg.symbol;
        let root_of = root(symbol)?;
        if !self.futures_open(self.now) {
            return Some(("sim_market_closed", "futures are halted or closed".into()));
        }
        if !intent.market {
            let Some(mid) = self
                .fresh(symbol, self.now)
                .and_then(|q| q.bid.zip(q.ask))
                .map(|(b, a)| (i128::from(b) + i128::from(a)) / 2)
            else {
                return Some(("sim_no_quote", format!("no fresh quote for {symbol}")));
            };
            let limit = i128::from(intent.limit_cents) * i128::from(CENT);
            let band = mid * i128::from(self.s.futures_band_permille) / 1000;
            if (limit - mid).abs() > band {
                return Some((
                    "sim_price_band",
                    format!(
                        "limit {} is more than {} from the mid {}",
                        fmt_nanos(limit),
                        fmt_nanos(band),
                        fmt_nanos(mid)
                    ),
                ));
            }
        }
        if !leg.open {
            return None;
        }
        let qty = u64::from(intent.qty);
        let after = self.future_contracts(root_of, qty);
        if after > self.s.futures_max_position {
            return Some((
                "sim_future_position",
                format!(
                    "{after} {root_of} contracts held and working, over {}",
                    self.s.futures_max_position
                ),
            ));
        }
        if self.s.futures_margin.contains_key(root_of) {
            let others = if self.s.venue.defined_risk {
                self.cash - self.buying_power_ex_futures()
            } else {
                0
            };
            let need = self.futures_margin_held(Some((root_of, qty)));
            if self.cash - others < need {
                return Some((
                    "sim_insufficient_margin",
                    format!(
                        "initial margin {} for {after} {root_of} contracts, with {} available",
                        fmt_nanos(need),
                        fmt_nanos(self.cash - others)
                    ),
                ));
            }
        }
        None
    }

    /// Cash a futures fill moves, besides its fee: the profit or loss it realizes,
    /// from the position before it (`held`, its total cost) and `delta` contracts at
    /// `px`.
    pub(super) fn realized(symbol: &str, held: (i64, i128), delta: i64, px: i64) -> i128 {
        let (qty, cost) = held;
        if qty == 0 || qty.signum() == delta.signum() {
            return 0;
        }
        let closed = i128::from(delta.unsigned_abs().min(qty.unsigned_abs()));
        let avg = cost / i128::from(qty);
        (i128::from(px) - avg) * closed * i128::from(qty.signum()) * multiplier(symbol)
    }

    /// The next futures settlement due: 09:30 on the expiry day of a held contract.
    pub(super) fn next_future_settlement(&self) -> Option<u64> {
        let day = et_day(self.now.max(1));
        self.pos
            .iter()
            .filter(|(s, p)| p.0 != 0 && is_future(s))
            .filter_map(|(s, _)| future_expiry(s, day))
            .map(|e| et_ns(e, SETTLE))
            .min()
    }

    /// Settles the futures expiring by `ts` (09:30 of their expiry day): each held
    /// contract in cash at its final settlement price; working orders on it expire.
    pub(super) fn settle_futures(&mut self, ts: u64) -> Result<(), String> {
        let day = et_day(ts);
        let due: Vec<(String, i64, i128)> = self
            .pos
            .iter()
            .filter(|(s, p)| {
                p.0 != 0 && future_expiry(s, day).is_some_and(|e| et_ns(e, SETTLE) <= ts)
            })
            .map(|(s, p)| (s.clone(), p.0, p.1))
            .collect();
        for (symbol, qty, cost) in due {
            let price = self.final_settlement(&symbol, ts)?;
            let pnl = Self::realized(&symbol, (qty, cost), -qty, price);
            self.cash += pnl;
            self.pos.remove(&symbol);
            for i in 0..self.orders.len() {
                if self.open(i) && self.orders[i].legs.iter().any(|l| l.symbol == symbol) {
                    self.set_status(i, "expired");
                }
            }
            self.acts += 1;
            let raw = Obj::new()
                .s("activity_type", "SETTLE")
                .s("id", &format!("sim-act-{:06}", self.acts))
                .s("symbol", &symbol)
                .s("qty", &qty.to_string())
                .s("price", &fmt_nanos(i128::from(price)))
                .s("net_amount", &fmt_nanos(pnl))
                .s("date", &date_string(day))
                .n("transaction_time_ns", ts)
                .done();
            self.activities.push(raw.clone());
            self.report(Update::Activity {
                activity_type: "SETTLE".into(),
                raw,
            });
        }
        Ok(())
    }

    /// A contract's final settlement price, nanodollars.
    fn final_settlement(&self, symbol: &str, ts: u64) -> Result<i64, String> {
        let root_of = root(symbol).unwrap_or_default();
        let date = date_string(et_day(ts));
        let e = &self.s.expiry;
        for r in [Some(root_of), future_index(root_of)].into_iter().flatten() {
            if let Some(v) = e
                .prices
                .get(&(r.to_owned(), date.clone(), "AM-SOQ".to_owned()))
            {
                return Ok(*v);
            }
        }
        if e.from_quote
            && let Some((b, a)) = self.fresh(symbol, ts).and_then(|q| q.bid.zip(q.ask))
        {
            return Ok((b + a) / 2);
        }
        Err(format!(
            "no final settlement price for {symbol} on {date}: add an AM-SOQ row for \
             {root_of}{} to the settlement file, or quote it within expiry.quote_max_age_s \
             of 09:30",
            future_index(root_of).map_or(String::new(), |i| format!(" or {i}"))
        ))
    }
}
