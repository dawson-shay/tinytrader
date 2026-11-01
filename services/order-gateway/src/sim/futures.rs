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
//! - **Margin.** `futures_margin = {"ES": "20000.00"}` is the initial margin per
//!   contract, required for every root traded (an opening without it is refused,
//!   `sim_no_margin`). An opening is refused (`sim_insufficient_margin`) unless cash
//!   and the futures' unrealized profit or loss cover the margin of the root's held
//!   and working contracts with it, besides what the venue's options rules hold.
//!   Buying power is net of the margin held. The account reports its net liquidation
//!   value as `equity`.
//! - **Caps.** An opening order is refused (`sim_future_position`) when the root's
//!   contracts at stake with it would exceed `futures_max_position` (10 by default):
//!   for each contract the larger position its working orders could leave (all buys
//!   filled, or all sells), summed over the root's contracts, so a long in one month
//!   and a short in another both count, and a sale that flips a long counts for the
//!   short it leaves. A limit order
//!   must be within `futures_price_band` (0.05 by default) of the last fresh mid
//!   (`sim_price_band`); an opening with no fresh quote is refused (`sim_no_quote`), a
//!   close isn't.
//! - **Expiry.** A contract trades until 09:30 on its third Friday (the trading day
//!   before, when that Friday is a holiday of the calendar), when what is held
//!   settles in cash at the final settlement price: the settlement file's `AM-SOQ` for
//!   the future's root, else for its index (SPX for ES and MES, NDX for NQ and MNQ, RUT
//!   for RTY and M2K; YM has none here), else (with `expiry.from_quote`) its own last
//!   fresh mid. Each
//!   settlement is a `SETTLE` activity; working orders on the contract expire.
use super::{CENT, SimBroker, fmt_nanos};
use crate::broker::Update;
use crate::intent::{Intent, future, future_expiry, future_index, future_multiplier};
use crate::protocol::Obj;
use crate::safety::{date_string, et_day, et_minutes, et_ns};

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

/// Minutes after midnight New York of `t`, on the local clock.
fn minutes(t: u64) -> i64 {
    et_minutes(t)
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
        self.future_expiry_day(symbol, day)
            .is_some_and(|e| e < day || (e == day && self.now >= et_ns(e, SETTLE)))
    }

    /// A contract's expiry day: its third Friday, or the trading day before it when
    /// the calendar has no session that Friday (a holiday).
    pub(super) fn future_expiry_day(&self, symbol: &str, day: i64) -> Option<i64> {
        let friday = future_expiry(symbol, day)?;
        Some(
            (friday - 7..=friday)
                .rev()
                .find(|d| self.session(*d).is_some())
                .unwrap_or(friday),
        )
    }

    /// The mid of `symbol`'s fresh quote, or else of its last one, and whether it's
    /// fresh; `None` with no quote at all.
    fn mid(&self, symbol: &str) -> Option<(i128, bool)> {
        let two = |q: super::Quote| {
            q.bid
                .zip(q.ask)
                .map(|(b, a)| (i128::from(b) + i128::from(a)) / 2)
        };
        if let Some(m) = self.fresh(symbol, self.now).and_then(two) {
            return Some((m, true));
        }
        let m = self.market.as_ref()?;
        let q = m.ids.get(symbol).and_then(|id| m.book.get(id)).copied()?;
        two(q).map(|m| (m, false))
    }

    /// Unrealized profit or loss of the futures held, at their fresh mids, or their
    /// last known ones when quotes go stale (none known: nothing), nanodollars.
    pub(super) fn futures_unrealized(&self) -> i128 {
        self.pos
            .iter()
            .filter(|(s, p)| p.0 != 0 && is_future(s))
            .filter_map(|(s, p)| {
                let (mid, _) = self.mid(s)?;
                Some(mid * i128::from(p.0) * multiplier(s) - p.1 * multiplier(s))
            })
            .sum()
    }

    /// Net liquidation value, nanodollars: cash, plus each position at its fresh mid
    /// (an option's at 100 shares, a future's as what it would realize). A position
    /// without one is valued at its last known mid (a future), or at its entry price
    /// (an option or shares, or a future never quoted); those are named in the second
    /// value.
    pub(super) fn net_liquidation(&self) -> (i128, Vec<String>) {
        let mut stale = Vec::new();
        let mut marked = 0_i128;
        for (s, p) in self.pos.iter().filter(|(_, p)| p.0 != 0) {
            let mid = self.mid(s);
            if !mid.is_some_and(|(_, fresh)| fresh) {
                stale.push(s.clone());
            }
            if is_future(s) {
                continue;
            }
            let shares = if crate::intent::occ(s).is_some() {
                100
            } else {
                1
            };
            let fresh = mid.filter(|(_, f)| *f).map(|(m, _)| m);
            marked += fresh.map_or(p.1, |m| m * i128::from(p.0)) * shares;
        }
        (self.cash + marked + self.futures_unrealized(), stale)
    }

    /// Contracts of `root_of` at stake, with `extra` (a symbol and signed contracts)
    /// working too: for each contract, the larger position the working orders could
    /// leave, all buys filled or all sells; summed over the root's contracts. A sale
    /// that flattens and flips a long counts for the short it leaves.
    fn future_contracts(&self, root_of: &str, extra: Option<(&str, i64)>) -> u64 {
        let mut by: std::collections::BTreeMap<&str, (i64, i64, i64)> =
            std::collections::BTreeMap::new();
        for (s, p) in &self.pos {
            if root(s) == Some(root_of) {
                by.entry(s.as_str()).or_default().0 += p.0;
            }
        }
        for (i, o) in self.orders.iter().enumerate() {
            if !self.open(i) {
                continue;
            }
            let left = i64::try_from(o.qty - o.filled).unwrap_or(0);
            for l in &o.legs {
                if root(&l.symbol) == Some(root_of) {
                    let e = by.entry(l.symbol.as_str()).or_default();
                    if l.buy {
                        e.1 += left;
                    } else {
                        e.2 += left;
                    }
                }
            }
        }
        if let Some((s, q)) = extra.filter(|(s, _)| root(s) == Some(root_of)) {
            let e = by.entry(s).or_default();
            if q > 0 {
                e.1 += q;
            } else {
                e.2 -= q;
            }
        }
        by.values()
            .map(|(held, buys, sells)| {
                (held + buys)
                    .unsigned_abs()
                    .max((held - sells).unsigned_abs())
            })
            .sum()
    }

    /// Initial margin held for futures, nanodollars, with `extra` (a symbol and
    /// signed contracts) working too.
    pub(super) fn futures_margin_held(&self, extra: Option<(&str, i64)>) -> i128 {
        self.s
            .futures_margin
            .iter()
            .map(|(r, m)| m * i128::from(self.future_contracts(r, extra)))
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
            let mid = self
                .fresh(symbol, self.now)
                .and_then(|q| q.bid.zip(q.ask))
                .map(|(b, a)| (i128::from(b) + i128::from(a)) / 2);
            // A close is never kept from closing for want of a quote: it's banded
            // only against a fresh one.
            let Some(mid) = mid else {
                if !leg.open {
                    return None;
                }
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
        let signed = i64::from(intent.qty) * if leg.buy { 1 } else { -1 };
        let after = self.future_contracts(root_of, Some((symbol, signed)));
        if after > self.s.futures_max_position {
            return Some((
                "sim_future_position",
                format!(
                    "{after} {root_of} contracts held and working, over {}",
                    self.s.futures_max_position
                ),
            ));
        }
        if !self.s.futures_margin.contains_key(root_of) {
            return Some((
                "sim_no_margin",
                format!("no futures_margin for {root_of} in the profile"),
            ));
        }
        let others = if self.s.venue.defined_risk {
            self.cash - self.buying_power_ex_futures()
        } else {
            0
        };
        // Cash with the futures' unrealized profit or loss, less the options' hold.
        let available = self.cash + self.futures_unrealized() - others;
        let need = self.futures_margin_held(Some((symbol, signed)));
        if available < need {
            return Some((
                "sim_insufficient_margin",
                format!(
                    "initial margin {} for {after} {root_of} contracts, with {} available",
                    fmt_nanos(need),
                    fmt_nanos(available)
                ),
            ));
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
        let held = self
            .pos
            .iter()
            .filter(|(s, p)| p.0 != 0 && is_future(s))
            .map(|(s, _)| s.as_str());
        let working = self
            .orders
            .iter()
            .enumerate()
            .filter(|(i, _)| self.open(*i))
            .flat_map(|(_, o)| o.legs.iter().map(|l| l.symbol.as_str()))
            .filter(|s| is_future(s));
        held.chain(working)
            .filter_map(|s| self.future_expiry_day(s, day))
            .map(|e| et_ns(e, SETTLE))
            .filter(|t| *t >= self.now)
            .min()
    }

    /// Settles the futures expiring by `ts` (09:30 of their expiry day): each held
    /// contract in cash at its final settlement price; working orders on it expire.
    pub(super) fn settle_futures(&mut self, ts: u64) -> Result<(), String> {
        let day = et_day(ts);
        let due_at = |me: &Self, s: &str| {
            me.future_expiry_day(s, day)
                .is_some_and(|e| et_ns(e, SETTLE) <= ts)
        };
        let due: Vec<(String, i64, i128)> = self
            .pos
            .iter()
            .filter(|(s, p)| p.0 != 0 && is_future(s) && due_at(self, s))
            .map(|(s, p)| (s.clone(), p.0, p.1))
            .collect();
        // Working orders on an expiring contract expire, held or not.
        for i in 0..self.orders.len() {
            if self.open(i)
                && self.orders[i]
                    .legs
                    .iter()
                    .any(|l| is_future(&l.symbol) && due_at(self, &l.symbol))
            {
                self.set_status(i, "expired");
            }
        }
        for (symbol, qty, cost) in due {
            let price = self.final_settlement(&symbol, ts)?;
            let pnl = Self::realized(&symbol, (qty, cost), -qty, price);
            self.cash += pnl;
            self.pos.remove(&symbol);
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

#[cfg(test)]
mod tests {
    use super::SimBroker;

    const PT: i64 = 1_000_000_000;

    /// What a fill realizes, from the position before it.
    #[test]
    fn realized_profit_and_loss() {
        let long2 = (2, 2 * 7743 * i128::from(PT));
        // Adding to a position, or opening, realizes nothing.
        assert_eq!(SimBroker::realized("ESZ6", long2, 1, 7750 * PT), 0);
        assert_eq!(SimBroker::realized("ESZ6", (0, 0), -1, 7750 * PT), 0);
        // Selling one of two at 7753: 10 points × 50.
        assert_eq!(
            SimBroker::realized("ESZ6", long2, -1, 7753 * PT),
            500 * i128::from(PT)
        );
        // Selling three: only the two held realize.
        assert_eq!(
            SimBroker::realized("ESZ6", long2, -3, 7753 * PT),
            1000 * i128::from(PT)
        );
        // A short covered higher loses; MES and NQ have their own multipliers.
        let short1 = (-1, -7743 * i128::from(PT));
        assert_eq!(
            SimBroker::realized("ESZ6", short1, 1, 7753 * PT),
            -500 * i128::from(PT)
        );
        assert_eq!(
            SimBroker::realized("MESZ6", short1, 1, 7753 * PT),
            -50 * i128::from(PT)
        );
        assert_eq!(
            SimBroker::realized("NQZ6", short1, 1, 7753 * PT),
            -200 * i128::from(PT)
        );
    }
}
