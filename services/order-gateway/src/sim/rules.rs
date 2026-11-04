//! A venue's own rules, from its profile (`venue_rules`), as a broker applies them:
//!
//! - **No new same-day openings after a time** (`no_same_day_opening_after`, such as
//!   `"15:30"`, Alpaca's): an order opening an option that expires today is
//!   refused after it (`sim_same_day_opening`).
//! - **Same-day short options liquidated at a time** (`liquidate_same_day_shorts_at`,
//!   such as `"15:45"`, Alpaca's for accounts that can't cover exercise or
//!   assignment): every short option position in a physically settled root on its
//!   last trading day is bought back at its ask, if fresh (within the expiry
//!   quote age), up to the ask's size, as a broker order: the account's clients see
//!   it as a `foreign` order and its fill as a `FILL` activity. Working orders on
//!   those contracts are canceled first. What isn't bought back settles at the
//!   close. The fill is at the ask when the rule runs, without the venue's latency
//!   or `fill_on`.
//! - **Times move with an early close**: a rule's time keeps its distance from the
//!   close, so 15:30 and 15:45 on a 16:00 day are 12:30 and 12:45 on a 13:00 day.
//! - **Buying power** (`buying_power = "defined_risk"`): an opening order is refused
//!   (`sim_insufficient_buying_power`), never resized, when cash, less the order's
//!   premium, less the worst loss at expiry of each expiry's option positions with
//!   the order filled, would be negative. A position whose loss has no bound (more
//!   short calls than long) is refused outright. Shares are bought with cash. It
//!   approximates a broker's options buying power for defined-risk spreads, and
//!   does not model margin on naked positions. Working orders count as if filled:
//!   their legs in the positions, their debits as paid. A short option covered by a
//!   long of the same root, type and strike that expires later (a calendar) is paired
//!   with it and the pair needs nothing beyond its debit, as the gateway's
//!   `--allow-calendars` sizes it: each long covers at most its own quantity, and a
//!   European root is never paired. The requirement is the lower of the two ways,
//!   paired and by expiry alone, so pairing never raises it.
use super::{CENT, SimBroker};
use crate::intent::Intent;
use crate::safety::{et_day, et_ns, hhmm};
use std::collections::BTreeMap;

/// Option legs `(call, strike, signed quantity)` by root and expiry.
type Groups = BTreeMap<(String, i64), Vec<(bool, i64, i64)>>;

/// Option symbols `(expiry, symbol)` by root, type (call) and strike.
type Lines<'a> = BTreeMap<(&'a str, bool, i64), Vec<(i64, &'a str)>>;

/// The rules of a profile.
#[derive(Clone, Debug, Default)]
pub(crate) struct VenueRules {
    /// Minutes after midnight, ET.
    pub(crate) no_same_day_opening_after: Option<i64>,
    pub(crate) liquidate_at: Option<i64>,
    pub(crate) defined_risk: bool,
}

impl VenueRules {
    pub(crate) fn parse_time(s: &str) -> Option<i64> {
        hhmm(s)
    }
}

/// The worst payoff at expiry (nanodollars, ≤ 0 is a loss) of option positions in
/// one expiry, or `None` when it has no bound.
fn worst(legs: &[(bool, i64, i64)]) -> Option<i128> {
    // Calls whose net quantity is short lose without bound as the price rises.
    let calls: i64 = legs.iter().filter(|l| l.0).map(|l| l.2).sum();
    if calls < 0 {
        return None;
    }
    let payoff = |s: i64| -> i128 {
        legs.iter()
            .map(|(call, k, q)| {
                let v = if *call {
                    (s - k).max(0)
                } else {
                    (k - s).max(0)
                };
                i128::from(*q) * 100 * i128::from(v)
            })
            .sum()
    };
    // Piecewise linear between strikes: the worst is at zero or a strike.
    let mut points: Vec<i64> = legs.iter().map(|l| l.1).collect();
    points.push(0);
    points.iter().map(|s| payoff(*s)).min().or(Some(0))
}

/// The worst loss at expiry of option positions (symbol, signed quantity), summed over
/// expiries; the root of a position with no bound, as an error.
fn requirement(all: &BTreeMap<&str, i64>) -> Result<i128, String> {
    let mut groups: Groups = BTreeMap::new();
    for (symbol, &q) in all {
        if q == 0 {
            continue;
        }
        if let (Some((root, call, strike)), Some((_, exp))) =
            (super::expiry::contract(symbol), crate::intent::occ(symbol))
        {
            groups
                .entry((root.to_owned(), exp))
                .or_default()
                .push((call, strike, q));
        }
    }
    let mut required = 0_i128;
    for ((root, _), legs) in &groups {
        required += (-worst(legs).ok_or_else(|| root.clone())?).max(0);
    }
    Ok(required)
}

/// The positions with each calendar taken out: a short covered by longs of the same root,
/// type and strike that expire later, each long covering at most its own quantity. Such a
/// pair loses at most what was paid for it (an American long can be exercised to meet the
/// short), so it needs nothing more. Shorts are covered latest first, as they have the
/// fewest longs after them. European roots are left as they are.
fn paired(mut all: BTreeMap<&str, i64>) -> BTreeMap<&str, i64> {
    let mut lines: Lines = BTreeMap::new();
    for (&symbol, _) in all.iter().filter(|(_, q)| **q != 0) {
        if let (Some((root, call, strike)), Some((_, exp))) =
            (super::expiry::contract(symbol), crate::intent::occ(symbol))
            && !crate::intent::EUROPEAN_ROOTS.contains(&root)
        {
            lines
                .entry((root, call, strike))
                .or_default()
                .push((exp, symbol));
        }
    }
    for mut line in lines.into_values() {
        line.sort_unstable();
        for i in (0..line.len()).rev() {
            for j in i + 1..line.len() {
                let (short, long) = (all[line[i].1], all[line[j].1]);
                if short >= 0 {
                    break;
                }
                if long <= 0 || line[j].0 <= line[i].0 {
                    continue;
                }
                let n = (-short).min(long);
                *all.entry(line[i].1).or_default() += n;
                *all.entry(line[j].1).or_default() -= n;
            }
        }
    }
    all
}

impl SimBroker {
    /// Why the venue's rules refuse `intent` now, if they do: a code and a message.
    pub(super) fn refuse(&self, intent: &Intent) -> Option<(&'static str, String)> {
        let r = &self.s.venue;
        let today = et_day(self.now);
        if let Some(after) = r.no_same_day_opening_after {
            let late = self.rule_time(today, after).is_some_and(|t| self.now >= t);
            // Its last trading day: the session before expiry for an AM-settled root.
            let same_day = intent
                .legs
                .iter()
                .any(|l| l.open && self.last_trading_day(&l.symbol) == Some(today));
            if late && same_day {
                return Some((
                    "sim_same_day_opening",
                    "options expiring today can't be opened this late".into(),
                ));
            }
        }
        // A future is held to its margin (`futures`), not to options buying power.
        if r.defined_risk && !intent.is_future() && intent.legs.iter().any(|l| l.open) {
            return self.short_of_buying_power(intent);
        }
        None
    }

    /// The defined-risk buying power check of an opening order.
    fn short_of_buying_power(&self, intent: &Intent) -> Option<(&'static str, String)> {
        let qty = i64::from(intent.qty);
        // The order's premium: a debit is paid, a credit received.
        let signed = if intent.combo || intent.legs[0].buy {
            intent.limit_cents
        } else {
            -intent.limit_cents
        };
        let multiplier = if intent
            .legs
            .iter()
            .all(|l| l.class == crate::intent::Class::Equity)
        {
            1
        } else {
            100
        };
        // Its premium, and the working orders' debits still to pay (a credit not yet
        // received counts for nothing).
        let premium = i128::from(signed) * i128::from(CENT) * i128::from(qty) * multiplier
            + self.working_debits();
        // Option positions with the working orders and this one filled.
        let mut extra = self.working_legs();
        extra.extend(intent.legs.iter().map(|l| {
            let sign = if l.buy { 1 } else { -1 };
            (l.symbol.clone(), sign * qty * i64::from(l.ratio))
        }));
        let required = match self.required(&extra) {
            Ok(r) => r,
            Err(root) => {
                return Some((
                    "sim_insufficient_buying_power",
                    format!("{root}: a position with no bound on its loss"),
                ));
            }
        };
        // The futures' initial margin is held from the same cash, and their unrealized
        // loss counts against it (a gain doesn't pay for options).
        let left = self.cash + self.futures_unrealized().min(0)
            - premium
            - required
            - self.futures_margin_held(None);
        (left < 0).then(|| {
            (
                "sim_insufficient_buying_power",
                format!("needs {} more", super::fmt_nanos(-left)),
            )
        })
    }

    /// The worst loss at expiry of the option positions, with `extra` added, summed
    /// over expiries; the root of a position with no bound, as an error.
    fn required(&self, extra: &[(String, i64)]) -> Result<i128, String> {
        let mut all: BTreeMap<&str, i64> = BTreeMap::new();
        for (s, q) in self
            .pos
            .iter()
            .map(|(s, p)| (s.as_str(), p.0))
            .chain(extra.iter().map(|(s, q)| (s.as_str(), *q)))
        {
            *all.entry(s).or_default() += q;
        }
        let by_expiry = requirement(&all);
        match (by_expiry, requirement(&paired(all))) {
            (Ok(a), Ok(b)) => Ok(a.min(b)),
            (Ok(r), Err(_)) | (Err(_), Ok(r)) => Ok(r),
            (Err(root), Err(_)) => Err(root),
        }
    }

    /// Cash less the worst expiry loss of the option positions held.
    pub(super) fn buying_power(&self) -> i128 {
        (self.buying_power_ex_futures() + self.futures_unrealized()
            - self.futures_margin_held(None))
        .max(0)
    }

    /// Buying power before the futures margin.
    pub(super) fn buying_power_ex_futures(&self) -> i128 {
        self.required(&self.working_legs())
            .map_or(0, |r| (self.cash - self.working_debits() - r).max(0))
    }

    /// Working orders' unfilled opening legs, signed, as positions they would add. A
    /// closing leg would lower the requirement only once it fills, so it counts
    /// for nothing until then.
    fn working_legs(&self) -> Vec<(String, i64)> {
        let mut out = Vec::new();
        for (i, o) in self.orders.iter().enumerate() {
            if !self.open(i) {
                continue;
            }
            let left = i64::try_from(o.qty - o.filled).unwrap_or(0);
            for l in &o.legs {
                if !l.open || super::futures::is_future(&l.symbol) {
                    continue;
                }
                let sign = if l.buy { 1 } else { -1 };
                out.push((
                    l.symbol.clone(),
                    sign * left * i64::try_from(l.ratio).unwrap_or(1),
                ));
            }
        }
        out
    }

    /// The debits working orders would still pay, nanodollars.
    fn working_debits(&self) -> i128 {
        self.orders
            .iter()
            .enumerate()
            .filter(|(i, o)| {
                self.open(*i) && !o.legs.iter().any(|l| super::futures::is_future(&l.symbol))
            })
            .map(|(_, o)| {
                let left = i128::from(o.qty - o.filled);
                let debit = if o.combo || o.legs[0].buy {
                    o.limit_cents
                } else {
                    0
                };
                let multiplier = if o
                    .legs
                    .iter()
                    .all(|l| crate::intent::occ(&l.symbol).is_none())
                {
                    1
                } else {
                    100
                };
                i128::from(debit.max(0)) * i128::from(CENT) * left * multiplier
            })
            .sum()
    }

    /// A rule's time on ET day `day`: as far before that session's close as before a
    /// 16:00 close, so an early close (13:00) moves 15:45 to 12:45.
    fn rule_time(&self, day: i64, minutes: i64) -> Option<u64> {
        let (_, close) = self.session(day)?;
        let regular = et_ns(day, 16 * 60);
        let before = regular.checked_sub(et_ns(day, minutes))?;
        close.checked_sub(before)
    }

    /// The rules, for `pins`.
    pub(super) fn rules_pin(&self) -> String {
        let r = &self.s.venue;
        let t = |m: Option<i64>| m.map(|m| format!("{:02}:{:02}", m / 60, m % 60));
        crate::protocol::Obj::new()
            .opt_s(
                "no_same_day_opening_after",
                t(r.no_same_day_opening_after).as_deref(),
            )
            .opt_s("liquidate_same_day_shorts_at", t(r.liquidate_at).as_deref())
            .s(
                "buying_power",
                if r.defined_risk {
                    "defined_risk"
                } else {
                    "cash"
                },
            )
            .done()
    }

    /// The next time the liquidation rule runs, on or after ET day `day`.
    pub(super) fn next_liquidation(&self, day: i64) -> Option<u64> {
        let at = self.s.venue.liquidate_at?;
        (day..day + 14)
            .filter_map(|d| self.rule_time(d, at))
            .find(|t| *t >= self.now)
    }

    /// Buys back every short option expiring today in a physically settled root, at
    /// its ask, as broker orders; working orders on them are canceled first.
    pub(super) fn liquidate(&mut self, ts: u64) {
        let today = et_day(ts);
        let shorts: Vec<(String, i64)> = self
            .pos
            .iter()
            .filter(|(s, p)| {
                // Its last trading day, and a root that delivers shares.
                p.0 < 0
                    && self.last_trading_day(s) == Some(today)
                    && crate::intent::occ(s).is_some_and(|(root, _)| self.physical(root))
            })
            .map(|(s, p)| (s.clone(), -p.0))
            .collect();
        for (symbol, qty) in shorts {
            // A fresh ask only, and no more than it shows: what isn't bought back
            // settles at the close.
            let Some(q) = self.fresh(&symbol, ts) else {
                continue;
            };
            let Some(ask) = q.ask else { continue };
            // No size shown, nothing bought back: it settles at the close.
            if q.ask_sz == 0 {
                continue;
            }
            let qty = qty.min(i64::try_from(q.ask_sz).unwrap_or(qty));
            let working: Vec<usize> = (0..self.orders.len())
                .filter(|i| {
                    self.open(*i) && self.orders[*i].legs.iter().any(|l| l.symbol == symbol)
                })
                .collect();
            for i in working {
                self.set_status(i, "canceled");
            }
            let n = self.orders.len() + 1;
            let leg = super::SimLeg {
                symbol: symbol.clone(),
                ratio: 1,
                buy: true,
                open: false,
                filled: 0,
                cost: 0,
            };
            let i = self.create(
                format!("sim-liquidation-{n}"),
                false,
                vec![leg],
                u64::try_from(qty).unwrap_or(0),
                0,
                super::script::Rule::default(),
            );
            self.orders[i].active = true;
            self.push(i, "new", None, None, None);
            // The broker's own order: the gateway reports it as `foreign` and its fill
            // as `foreign_fill`, as it does Alpaca's.
            self.execute(i, u64::try_from(qty).unwrap_or(0), &[ask]);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{paired, requirement, worst};
    use std::collections::BTreeMap;

    const D: i64 = 1_000_000_000;

    #[test]
    fn the_worst_loss_of_spreads_and_condors() {
        // A put credit spread 570/565: worst −5 × 100.
        assert_eq!(
            worst(&[(false, 570 * D, -1), (false, 565 * D, 1)]),
            Some(-500 * i128::from(D))
        );
        // An iron condor 565/570/590/595: worst −5 × 100 on either side.
        let condor = [
            (false, 565 * D, 1),
            (false, 570 * D, -1),
            (true, 590 * D, -1),
            (true, 595 * D, 1),
        ];
        assert_eq!(worst(&condor), Some(-500 * i128::from(D)));
        // A naked short call has no bound; a long call can't lose at expiry.
        assert_eq!(worst(&[(true, 590 * D, -1)]), None);
        assert_eq!(worst(&[(true, 590 * D, 1)]), Some(0));
    }

    /// The requirement of positions, the lower of paired and by expiry alone, as
    /// `SimBroker::required` takes it.
    fn need(pos: &[(&'static str, i64)]) -> Result<i128, String> {
        let all: BTreeMap<&str, i64> = pos.iter().copied().collect();
        match (requirement(&all), requirement(&paired(all))) {
            (Ok(a), Ok(b)) => Ok(a.min(b)),
            (Ok(r), Err(_)) | (Err(_), Ok(r)) => Ok(r),
            (Err(root), Err(_)) => Err(root),
        }
    }

    #[test]
    fn a_calendar_needs_only_its_debit() {
        // A double calendar: 570 puts and 580 calls, sold on 5 Oct and bought on 12 Oct.
        let double = [
            ("SPY261005P00570000", -2),
            ("SPY261012P00570000", 2),
            ("SPY261005C00580000", -2),
            ("SPY261012C00580000", 2),
        ];
        assert_eq!(need(&double), Ok(0));
        // By expiry alone the front call is naked: no bound.
        let all: BTreeMap<&str, i64> = double.iter().copied().collect();
        assert_eq!(requirement(&all), Err("SPY".into()));
        // One back for two fronts leaves a naked call.
        assert_eq!(
            need(&[("SPY261005C00580000", -2), ("SPY261012C00580000", 1)]),
            Err("SPY".into())
        );
        // A later long covers one of two shorts, not both.
        assert_eq!(
            need(&[
                ("SPY261005C00580000", -1),
                ("SPY261012C00580000", -1),
                ("SPY261019C00580000", 1),
            ]),
            Err("SPY".into())
        );
        // A diagonal is not paired: the front put is held to its strike.
        assert_eq!(
            need(&[("SPY261005P00570000", -1), ("SPY261012P00565000", 1)]),
            Ok(570 * 100 * i128::from(D))
        );
        // Nor is a European root.
        assert_eq!(
            need(&[("SPXW261016C05800000", -1), ("SPXW261023C05800000", 1)]),
            Err("SPXW".into())
        );
    }

    #[test]
    fn pairing_never_raises_the_requirement() {
        // A 570/565 put debit spread on 12 Oct, and a 570 put sold on 5 Oct. Paired, the
        // 12 Oct 570 put covers the front and leaves the 565 sold alone: 565 x 100. By
        // expiry the front put needs 570 x 100 and the spread nothing; the lower is taken.
        let pos = [
            ("SPY261005P00570000", -1),
            ("SPY261012P00570000", 1),
            ("SPY261012P00565000", -1),
        ];
        let all: BTreeMap<&str, i64> = pos.iter().copied().collect();
        assert_eq!(requirement(&all), Ok(570 * 100 * i128::from(D)));
        assert_eq!(need(&pos), Ok(565 * 100 * i128::from(D)));
        // A bear call spread on 12 Oct beside a front short call: pairing the front with
        // the spread's long leaves the 575 naked, so the other way is kept.
        let calls = [
            ("SPY261005C00580000", -1),
            ("SPY261005C00575000", 1),
            ("SPY261012C00580000", 1),
            ("SPY261012C00575000", -1),
        ];
        let all: BTreeMap<&str, i64> = calls.iter().copied().collect();
        assert_eq!(requirement(&all), Ok(5 * 100 * i128::from(D)));
        assert_eq!(need(&calls), Ok(5 * 100 * i128::from(D)));
    }

    /// A rule's time keeps its distance from the close: on a 13:00 early close,
    /// 15:45 is 12:45.
    #[test]
    fn rule_times_move_with_an_early_close() {
        let script = r#"{"calendar":[{"date":"2026-11-27","open":"09:30","close":"13:00"},{"date":"2026-11-30","open":"09:30","close":"16:00"}]}"#;
        let (sim, _feed) = crate::sim::SimBroker::fed(script, "{}").unwrap();
        let day = crate::safety::parse_date("2026-11-27").unwrap();
        assert_eq!(
            sim.rule_time(day, 15 * 60 + 45),
            Some(crate::safety::et_ns(day, 12 * 60 + 45))
        );
        let monday = crate::safety::parse_date("2026-11-30").unwrap();
        assert_eq!(
            sim.rule_time(monday, 15 * 60 + 45),
            Some(crate::safety::et_ns(monday, 15 * 60 + 45))
        );
    }
}
