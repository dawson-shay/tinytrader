//! Fill model: touch arithmetic for singles and combos, optional displayed-size caps,
//! per-leg skew, positions, cash, fees and FILL activities.
use super::{CENT, SimBroker, avg, fmt_nanos};
use crate::broker::terminal;
use crate::protocol::{Obj, arr};

impl SimBroker {
    /// Evaluates one working order against the current book (market mode).
    #[expect(clippy::many_single_char_names, reason = "touch arithmetic")]
    pub(super) fn evaluate(&mut self, i: usize) {
        let o = &self.orders[i];
        let Some(m) = &self.market else { return };
        if !o.active || terminal(&o.status) {
            return;
        }
        // An expired contract has no book: nothing fills against it.
        if o.legs
            .iter()
            .any(|l| self.expired(&l.symbol, crate::safety::et_day(self.now)))
        {
            return;
        }
        let (mut net, mut prices, mut avail) = (0_i128, Vec::new(), u64::MAX);
        let mut zeroed = Vec::new();
        for l in &o.legs {
            let Some(q) = m.ids.get(&l.symbol).and_then(|id| m.book.get(id)) else {
                return;
            };
            // fill_on = "next_quote": every leg on a quote after activation, never one
            // leg's new quote beside another's from before.
            if self.s.next_quote && q.ts <= o.t_act {
                return;
            }
            if matches!((q.bid, q.ask), (Some(b), Some(a)) if b > a) {
                return; // crossed: unusable until a non-crossed quote
            }
            let (px, size, used) = if l.buy {
                (q.ask, q.ask_sz, q.used_ask)
            } else {
                (q.bid, q.bid_sz, q.used_bid)
            };
            // no_bid_combo_leg = "zero": a combo's sold leg offered but not bid
            // (worthless, as far-out options near expiry are) goes for nothing.
            let zero = px.is_none() && !l.buy && o.combo && self.s.no_bid_zero && q.ask.is_some();
            let Some(px) = px.or(zero.then_some(0)) else {
                return;
            };
            let sign = if l.buy { 1 } else { -1 };
            net += sign * i128::from(l.ratio) * i128::from(px);
            prices.push(px);
            if zero {
                zeroed.push(l.symbol.clone());
            } else {
                avail = avail.min(size.saturating_sub(used) / l.ratio);
            }
        }
        let limit = i128::from(o.limit_cents) * i128::from(CENT);
        // A single sell is fillable when bid >= L, i.e. -bid <= -L.
        let limit = if !o.combo && !o.legs[0].buy {
            -limit
        } else {
            limit
        };
        if net > limit {
            // Resting: under the queue model, it joins the queue at its price.
            self.join_queue(i);
            return;
        }
        let remaining = o.qty - o.filled;
        let units = if self.s.size_cap {
            avail.min(remaining)
        } else {
            remaining
        };
        if units == 0 {
            return;
        }
        if self.s.size_cap {
            let legs: Vec<(String, bool, u64)> = o
                .legs
                .iter()
                .map(|l| (l.symbol.clone(), l.buy, l.ratio))
                .collect();
            if let Some(m) = &mut self.market {
                for (sym, buy, ratio) in legs {
                    if let Some(q) = m.ids.get(&sym).and_then(|id| m.book.get_mut(id)) {
                        let used = if buy {
                            &mut q.used_ask
                        } else {
                            &mut q.used_bid
                        };
                        *used += units * ratio;
                    }
                }
            }
        }
        self.orders[i].zero_legs = zeroed;
        self.execute(i, units, &prices);
    }

    pub(super) fn book_position(&mut self, symbol: &str, delta: i64, px: i64) -> i64 {
        let p = self.pos.entry(symbol.to_owned()).or_insert((0, 0));
        let next = p.0 + delta;
        p.1 = if p.0 == 0 || p.0.signum() == delta.signum() {
            p.1 + i128::from(delta) * i128::from(px)
        } else if next == 0 || next.signum() == p.0.signum() {
            p.1 * i128::from(next) / i128::from(p.0)
        } else {
            i128::from(next) * i128::from(px)
        };
        p.0 = next;
        next
    }

    /// Executes `units` parent units at per-leg prices (nanodollars per share).
    pub(super) fn execute(&mut self, i: usize, units: u64, prices: &[i64]) {
        let ts = self.ts();
        let units = units.min(self.orders[i].qty - self.orders[i].filled);
        if units == 0 {
            return;
        }
        // Options pay the per-contract fee; stock pays the per-share fee.
        let option_fee = i128::from(self.s.fee_cents) * i128::from(CENT);
        let share_fee = i128::from(self.s.fee_share);
        let mut fees = 0_i128;
        let (mut net, mut legs, mut position) = (0_i128, Vec::new(), None);
        for (k, px) in prices.iter().copied().enumerate() {
            let (symbol, ratio, buy) = {
                let l = &self.orders[i].legs[k];
                (l.symbol.clone(), l.ratio, l.buy)
            };
            let skip = self.orders[i].rule.skew.remove(&symbol).unwrap_or(0);
            let n = (units * ratio).saturating_sub(skip);
            let sign: i64 = if buy { 1 } else { -1 };
            net += i128::from(sign) * i128::from(ratio) * i128::from(px);
            let qty = i64::try_from(n).unwrap_or(0);
            let before = self.pos.get(&symbol).copied().unwrap_or((0, 0));
            position = Some(self.book_position(&symbol, sign * qty, px));
            if super::futures::is_future(&symbol) {
                // A future pays no notional: cash moves by what it realizes, and its fee.
                let fee = i128::from(self.s.fee_future) * i128::from(n);
                fees += fee;
                self.cash += Self::realized(&symbol, before, sign * qty, px) - fee;
            } else {
                // An option contract is 100 shares; a stock leg is one.
                let multiplier = if crate::intent::occ(&symbol).is_some() {
                    100
                } else {
                    1
                };
                let fee = if multiplier == 100 {
                    option_fee
                } else {
                    share_fee
                } * i128::from(n);
                fees += fee;
                self.cash -= i128::from(sign * qty) * i128::from(px) * multiplier + fee;
            }
            let l = &mut self.orders[i].legs[k];
            l.filled += n;
            l.cost += i128::from(n) * i128::from(px);
            legs.push(
                Obj::new()
                    .s("symbol", &symbol)
                    .n("filled_qty", l.filled)
                    .opt_s("avg_price", avg(l.cost, l.filled).as_deref())
                    .raw(
                        "fill",
                        &Obj::new()
                            .n("qty", n)
                            .s("price", &fmt_nanos(i128::from(px)))
                            .done(),
                    )
                    .done(),
            );
        }
        let o = &mut self.orders[i];
        let parent = if o.combo { net } else { i128::from(prices[0]) };
        o.filled += units;
        o.cost += i128::from(units) * parent;
        o.status = if o.filled == o.qty {
            "filled"
        } else {
            "partially_filled"
        }
        .into();
        o.updated = ts;
        let (combo, oid, done) = (o.combo, o.id.clone(), o.filled == o.qty);
        self.execs += 1;
        let exec = format!("sim-exec-{:06}", self.execs);
        let fill = Obj::new()
            .s("execution_id", &exec)
            .n("qty", units)
            .s("price", &fmt_nanos(parent))
            .s("fees", &fmt_nanos(fees))
            .done();
        self.activities.push(
            Obj::new()
                .s("activity_type", "FILL")
                .s("id", &exec)
                .s("order_id", &oid)
                .s("qty", &units.to_string())
                .s("price", &fmt_nanos(parent))
                .n("transaction_time_ns", ts)
                .done(),
        );
        let event = if done { "fill" } else { "partial_fill" };
        let (legs, position) = if combo {
            (Some(arr(legs)), None)
        } else {
            (None, position)
        };
        self.push(i, event, Some(fill), legs, position);
    }
}
