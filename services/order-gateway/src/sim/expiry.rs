//! Option expiry in market mode: at the session close of each day, options
//! expiring that day are settled as a broker would, as account activities
//! (`OPEXP`, `OPEXC`, `OPASN`), and, where the profile asks for it, short options
//! are assigned early.
//!
//! - **Cash-settled** roots (index options) pay their intrinsic value in cash
//!   against the settlement value: the PM close (`cash-pm`, SPXW and the like) or
//!   the AM special opening quotation (`cash-am`, SPX monthlies).
//! - **Physically settled** roots (SPY and other stocks and ETFs) exercise long and
//!   assign short options in the money by at least the exercise threshold ($0.01,
//!   as the OCC's), into the underlying at the strike, against the underlying's
//!   price at the close.
//! - A settlement price comes from the profile's settlement file (root, date, kind,
//!   value; pinned by SHA-256), or, where the profile allows, the venue's own quote
//!   of the underlying at the close (its mid). A price that can't be had fails the
//!   run: never a guess.
//! - **Early assignment** (opt-in, `early_assignment`): at the close of the day
//!   before an ex-dividend date, a short call in the money whose extrinsic value
//!   (its bid less its intrinsic value) is below the dividend is assigned; and, when
//!   `put_assignment_extrinsic` is set, at any close a short put in the money whose
//!   extrinsic value is below it. Pin risk and the holder's choice not to exercise
//!   are not modelled.
//! - **Last trading day.** An AM-settled root (`cash-am`) stops trading the session
//!   before its expiry date, whose morning gives its settlement value: from then on
//!   it can't be ordered or filled, its book is gone, and the venue's same-day rules
//!   treat that session as its last. Every other option trades through its expiry
//!   date's close.
//! - **Fresh prices only.** A quote stands in for a settlement price, or gives an
//!   option's bid for early assignment, only if it is from within `quote_max_age_s`
//!   (60 s) of the close.
//! - **All or nothing.** A day's settlements and assignments are worked out first,
//!   every price included; only then are positions, cash and shares changed and the
//!   activities sent. A missing price changes nothing.
use super::{SimBroker, fmt_nanos};
use crate::broker::Update;
use crate::protocol::Obj;
use crate::safety::{date_string, et_day};
use std::collections::{BTreeMap, HashMap};

/// How a root settles.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Style {
    CashPm,
    CashAm,
    Physical,
}

impl Style {
    pub(crate) fn parse(s: &str) -> Option<Self> {
        match s {
            "cash-pm" => Some(Self::CashPm),
            "cash-am" => Some(Self::CashAm),
            "physical" => Some(Self::Physical),
            _ => None,
        }
    }

    fn kind(self) -> &'static str {
        match self {
            Self::CashAm => "AM-SOQ",
            Self::CashPm | Self::Physical => "PM-close",
        }
    }
}

/// The index roots that settle in cash unless the profile says otherwise.
fn default_style(root: &str) -> Style {
    match root {
        // VIX and its weeklies settle on a Wednesday morning's SOQ.
        "SPX" | "NDX" | "RUT" | "VIX" | "VIXW" | "DJX" => Style::CashAm,
        "SPXW" | "XSP" | "NDXP" | "RUTW" | "MRUT" | "XND" => Style::CashPm,
        _ => Style::Physical,
    }
}

/// The expiry settings of a profile.
#[derive(Clone, Debug, Default)]
pub(crate) struct Expiry {
    /// Whether options are settled at all (on by default).
    pub(crate) settle: bool,
    /// Per-root overrides of [`default_style`].
    pub(crate) styles: BTreeMap<String, Style>,
    /// `(root, date, kind)` → value, nanodollars.
    pub(crate) prices: HashMap<(String, String, String), i64>,
    /// The settlement file's SHA-256, for the pin.
    pub(crate) prices_sha: Option<String>,
    /// Whether a missing price may come from the venue's quote of the underlying.
    pub(crate) from_quote: bool,
    /// ITM by at least this much exercises and assigns, nanodollars.
    pub(crate) threshold: i64,
    /// Root → ex-date → dividend, nanodollars per share.
    pub(crate) dividends: BTreeMap<String, BTreeMap<String, i64>>,
    pub(crate) dividends_sha: BTreeMap<String, String>,
    pub(crate) early: bool,
    /// A short put's extrinsic value below which it is assigned early, nanodollars;
    /// `None`, no early assignment of puts.
    pub(crate) put_extrinsic: Option<i64>,
    /// How old a quote may be at the close to stand in for a price, ns.
    pub(crate) max_age: u64,
}

/// Root, call or put, and strike (nanodollars) of a compact OCC symbol.
pub(crate) fn contract(symbol: &str) -> Option<(&str, bool, i64)> {
    let (root, _) = crate::intent::occ(symbol)?;
    let tail = &symbol[symbol.len() - 9..];
    let call = tail.starts_with('C');
    let strike: i64 = tail[1..].parse().ok()?;
    Some((root, call, strike * 1_000_000))
}

/// A decimal dollar amount as nanodollars: `5812.34`, `0.005`.
pub(crate) fn nanos(s: &str) -> Option<i64> {
    let s = s.trim();
    let (neg, s) = s.strip_prefix('-').map_or((false, s), |r| (true, r));
    let (whole, frac) = s.split_once('.').unwrap_or((s, ""));
    if frac.len() > 9 || whole.is_empty() && frac.is_empty() {
        return None;
    }
    let w: i64 = if whole.is_empty() {
        0
    } else {
        whole.parse().ok()?
    };
    let f: i64 = if frac.is_empty() {
        0
    } else {
        format!("{frac:0<9}").parse().ok()?
    };
    let v = w.checked_mul(1_000_000_000)?.checked_add(f)?;
    Some(if neg { -v } else { v })
}

/// The rows of a CSV with a header, as maps from column to value.
pub(crate) fn csv(text: &str) -> Vec<HashMap<String, String>> {
    let mut lines = text.lines().filter(|l| !l.trim().is_empty());
    let Some(head) = lines.next() else {
        return Vec::new();
    };
    let cols: Vec<String> = head.split(',').map(|c| c.trim().to_lowercase()).collect();
    lines
        .map(|l| {
            cols.iter()
                .cloned()
                .zip(l.split(',').map(|v| v.trim().to_owned()))
                .collect()
        })
        .collect()
}

/// What happens to one option position.
struct Settled {
    symbol: String,
    qty: i64,
    kind: &'static str,
    price: i64,
    /// Shares delivered: `(root, call, strike)`; `None` when it pays cash.
    deliver: Option<(String, bool, i64)>,
    /// Cash paid (+) or taken (−), nanodollars, for a cash settlement.
    cash: i128,
}

impl SimBroker {
    /// The settlement price of `root` on ET day `day`, by its style, at `close`.
    fn settlement(&self, root: &str, day: i64, style: Style, close: u64) -> Result<i64, String> {
        let e = &self.s.expiry;
        let date = date_string(day);
        if let Some(v) = e
            .prices
            .get(&(root.to_owned(), date.clone(), style.kind().to_owned()))
        {
            return Ok(*v);
        }
        if e.from_quote
            && style == Style::Physical
            && let Some((b, a)) = self.fresh(root, close).and_then(|q| q.bid.zip(q.ask))
        {
            return Ok((b + a) / 2);
        }
        Err(format!(
            "no {} settlement price for {root} on {date}: add it to the settlement file{} \
             (or set expiry.settle = false)",
            style.kind(),
            if style == Style::Physical {
                ", or quote the underlying in this venue within expiry.quote_max_age_s of the close"
            } else {
                ""
            }
        ))
    }

    /// Whether `root`'s options settle into shares.
    pub(super) fn physical(&self, root: &str) -> bool {
        self.style(root) == Style::Physical
    }

    /// The venue's quote of `symbol`, if it is from within the profile's age of `at`.
    pub(super) fn fresh(&self, symbol: &str, at: u64) -> Option<super::Quote> {
        let m = self.market.as_ref()?;
        let q = m.ids.get(symbol).and_then(|id| m.book.get(id))?;
        (q.ts + self.s.expiry.max_age >= at).then_some(*q)
    }

    /// The style `root` settles by.
    fn style(&self, root: &str) -> Style {
        self.s
            .expiry
            .styles
            .get(root)
            .copied()
            .unwrap_or_else(|| default_style(root))
    }

    /// The last ET day an option trades: the session before its expiry date for an
    /// AM-settled root, else its expiry date. `None` for anything not an option.
    pub(crate) fn last_trading_day(&self, symbol: &str) -> Option<i64> {
        let (root, expiry) = crate::intent::occ(symbol)?;
        if self.style(root) != Style::CashAm {
            return Some(expiry);
        }
        Some(
            (expiry - 14..expiry)
                .rev()
                .find(|d| self.session(*d).is_some())
                .unwrap_or(expiry - 1),
        )
    }

    /// Whether `symbol` is an option past its last trading day on ET day `day`.
    pub(crate) fn expired(&self, symbol: &str, day: i64) -> bool {
        if super::futures::is_future(symbol) {
            return self.future_expired(symbol, day);
        }
        self.last_trading_day(symbol).is_some_and(|last| last < day)
    }

    /// Settles every option position expiring on ET day `day` (or before, missed),
    /// then assigns early where the profile asks: all worked out first, then
    /// applied together. `ts` is the session's close.
    pub(super) fn settle_day(&mut self, day: i64, ts: u64) -> Result<(), String> {
        let mut plan = Vec::new();
        for (symbol, p) in &self.pos {
            let qty = p.0;
            let Some((root, call, strike)) = contract(symbol).filter(|_| qty != 0) else {
                continue;
            };
            let Some((_, expiry)) = crate::intent::occ(symbol).filter(|(_, e)| *e <= day) else {
                continue;
            };
            let style = self.style(root);
            let s = self.settlement(root, expiry, style, ts)?;
            let intrinsic = if call { s - strike } else { strike - s }.max(0);
            // Cash pays any intrinsic value; delivery needs the threshold.
            let floor = if style == Style::Physical {
                self.s.expiry.threshold.max(1)
            } else {
                1
            };
            let kind = if intrinsic < floor {
                "OPEXP"
            } else if qty > 0 {
                "OPEXC"
            } else {
                "OPASN"
            };
            let physical = style == Style::Physical && kind != "OPEXP";
            plan.push(Settled {
                symbol: symbol.clone(),
                qty,
                kind,
                price: s,
                deliver: physical.then(|| (root.to_owned(), call, strike)),
                cash: if style == Style::Physical {
                    0
                } else {
                    i128::from(qty) * 100 * i128::from(intrinsic)
                },
            });
        }
        if self.s.expiry.early {
            plan.extend(self.plan_early(day, ts)?);
        }
        for s in &plan {
            self.pos.remove(&s.symbol);
            if let Some((root, call, strike)) = &s.deliver {
                self.deliver(root, *call, *strike, s.qty);
            }
            self.cash += s.cash;
        }
        for s in &plan {
            self.activity(s, ts);
        }
        Ok(())
    }

    /// Short options to assign before expiry at the close of ET day `day`: a price
    /// is looked up only where a rule applies (a dividend tomorrow; the put rule).
    fn plan_early(&self, day: i64, ts: u64) -> Result<Vec<Settled>, String> {
        let next = (day + 1..day + 8).find(|d| self.session(*d).is_some());
        let mut out = Vec::new();
        for (symbol, p) in &self.pos {
            let qty = p.0;
            let Some((root, call, strike)) = contract(symbol).filter(|_| qty < 0) else {
                continue;
            };
            // Settled today anyway, or European: no early exercise.
            let expiring = crate::intent::occ(symbol).is_some_and(|(_, e)| e <= day);
            if expiring || self.style(root) != Style::Physical {
                continue;
            }
            let threshold = if call {
                let dividend = next.and_then(|d| {
                    self.s
                        .expiry
                        .dividends
                        .get(root)
                        .and_then(|m| m.get(&date_string(d)))
                        .copied()
                });
                let Some(div) = dividend else { continue };
                div
            } else {
                let Some(t) = self.s.expiry.put_extrinsic else {
                    continue;
                };
                t
            };
            // Its bid at the close; without a fresh one, it can't be judged.
            let Some(bid) = self.fresh(symbol, ts).and_then(|q| q.bid) else {
                continue;
            };
            let s = self.settlement(root, day, Style::Physical, ts)?;
            let intrinsic = if call { s - strike } else { strike - s };
            if intrinsic > 0 && bid - intrinsic < threshold {
                out.push(Settled {
                    symbol: symbol.clone(),
                    qty,
                    kind: "OPASN",
                    price: s,
                    deliver: Some((root.to_owned(), call, strike)),
                    cash: 0,
                });
            }
        }
        Ok(out)
    }

    /// Exercise or assignment of `qty` contracts (signed: long +, short −) into
    /// 100 shares each of `root` at the strike.
    fn deliver(&mut self, root: &str, call: bool, strike: i64, qty: i64) {
        // A long call or a short put buys shares; a long put or a short call sells.
        let shares = if call { 100 * qty } else { -100 * qty };
        self.book_position(root, shares, strike);
        self.cash -= i128::from(shares) * i128::from(strike);
    }

    fn activity(&mut self, s: &Settled, ts: u64) {
        self.acts += 1;
        let raw = Obj::new()
            .s("activity_type", s.kind)
            .s("id", &format!("sim-act-{:06}", self.acts))
            .s("symbol", &s.symbol)
            .s("qty", &s.qty.to_string())
            .s("price", &fmt_nanos(i128::from(s.price)))
            .s("date", &date_string(et_day(ts)))
            .n("transaction_time_ns", ts)
            .done();
        self.activities.push(raw.clone());
        let update = Update::Activity {
            activity_type: s.kind.to_owned(),
            raw,
        };
        // While the stream is down, held back and sent when it returns.
        // While the stream is down it waits in the report queue, in time order.
        self.report(update);
    }
}

#[cfg(test)]
mod tests {
    use super::{contract, csv, nanos};
    use crate::broker::{Broker, Update};
    use crate::sim::{Feed, SimBroker};
    use dbn::{CbboMsg, RecordHeader, RecordRef, SType, Schema, SymbolMappingMsg, rtype};

    const S: u64 = 1_000_000_000;
    /// 2026-10-05 10:00 New York (a Monday), and that day's 16:00 close.
    const T0: u64 = 1_791_208_800_000_000_000;
    const CLOSE: u64 = 1_791_230_400_000_000_000;
    const D: i64 = 1_000_000_000;

    fn push(feed: &Feed, iid: u32, raw: &str, bid: i64, ask: i64, ts: u64) {
        let m = SymbolMappingMsg::new(iid, ts, SType::Parent, "X", SType::RawSymbol, raw, 0, 0)
            .unwrap();
        feed.push(ts, RecordRef::from(&m)).unwrap();
        let mut q = CbboMsg {
            hd: RecordHeader::new::<CbboMsg>(rtype::CBBO_1M, 30, iid, u64::MAX),
            ts_recv: ts,
            ..CbboMsg::default_for_schema(Schema::Cbbo1M)
        };
        q.levels[0].bid_px = bid;
        q.levels[0].ask_px = ask;
        feed.push(ts, RecordRef::from(&q)).unwrap();
    }

    fn sim(script: &str) -> (SimBroker, Feed) {
        SimBroker::fed(script, "{}").unwrap()
    }

    fn kinds(sim: &mut SimBroker) -> Vec<(String, String)> {
        let mut out = Vec::new();
        sim.poll_updates(&mut out);
        out.into_iter()
            .filter_map(|u| match u {
                Update::Activity { activity_type, raw } => {
                    let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
                    Some((activity_type, v["symbol"].as_str().unwrap().to_owned()))
                }
                _ => None,
            })
            .collect()
    }

    #[test]
    fn contracts_prices_and_files_parse() {
        assert_eq!(
            contract("SPY260717C00749000"),
            Some(("SPY", true, 749_000_000_000))
        );
        assert_eq!(
            contract("SPXW261030P05812500"),
            Some(("SPXW", false, 5_812_500_000_000))
        );
        assert_eq!(nanos("5812.34"), Some(5_812_340_000_000));
        assert_eq!(nanos("0.005"), Some(5_000_000));
        assert_eq!(nanos("-1.5"), Some(-1_500_000_000));
        assert_eq!(nanos("x"), None);
        let rows = csv("root,date,kind,value\nSPXW,2026-10-30,PM-close,5812.34\n");
        assert_eq!(rows[0]["value"], "5812.34");
    }

    /// Physical settlement against the venue's quote of the underlying: a long call
    /// in the money is exercised into shares at the strike, a short call is
    /// assigned, a put out of the money expires, and a contract a cent short of the
    /// threshold expires too.
    #[test]
    fn physical_options_exercise_and_assign_at_the_close() {
        let (mut sim, feed) = sim("{}");
        push(
            &feed,
            9,
            "SPY",
            589_990_000_000,
            590_010_000_000,
            CLOSE - 30 * S,
        );
        sim.pos.insert("SPY261005C00580000".into(), (2, 0));
        sim.pos.insert("SPY261005C00585000".into(), (-1, 0));
        sim.pos.insert("SPY261005P00570000".into(), (-3, 0));
        sim.pos.insert("SPY261005C00590000".into(), (1, 0)); // at the money: expires
        let cash = sim.cash;
        sim.advance(CLOSE + S).unwrap();
        let mut k = kinds(&mut sim);
        k.sort();
        assert_eq!(
            k,
            [
                ("OPASN".into(), "SPY261005C00585000".into()),
                ("OPEXC".into(), "SPY261005C00580000".into()),
                ("OPEXP".into(), "SPY261005C00590000".into()),
                ("OPEXP".into(), "SPY261005P00570000".into()),
            ]
        );
        // +200 shares at 580, −100 at 585: 100 shares, 58,500 less cash.
        assert_eq!(sim.pos.get("SPY").map(|p| p.0), Some(100));
        assert_eq!(sim.cash - cash, i128::from(-200 * 580 * D + 100 * 585 * D));
        assert!(sim.pos.keys().all(|s| !s.starts_with("SPY26")));
    }

    /// Cash settlement from the settlement file: SPXW at the PM close, intrinsic in
    /// cash, no shares.
    #[test]
    fn index_options_settle_in_cash_from_the_settlement_file() {
        let dir = std::env::temp_dir().join(format!("sim-settle-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("settle.csv");
        std::fs::write(
            &file,
            "root,date,kind,value\nSPXW,2026-10-05,PM-close,5812.50\n",
        )
        .unwrap();
        let script = format!(
            r#"{{"expiry":{{"settlement_file":{:?}}}}}"#,
            file.display().to_string()
        );
        let (mut sim, _feed) = sim(&script);
        sim.pos.insert("SPXW261005C05800000".into(), (1, 0));
        sim.pos.insert("SPXW261005P05800000".into(), (-1, 0));
        let cash = sim.cash;
        sim.advance(CLOSE + S).unwrap();
        assert_eq!(sim.cash - cash, i128::from(100 * 12_500_000_000_i64)); // 12.50 × 100
        assert!(sim.pos.is_empty());
        let pins: serde_json::Value = serde_json::from_str(&sim.pins()).unwrap();
        assert_eq!(
            pins["expiry"]["settlement_file_sha256"]
                .as_str()
                .unwrap()
                .len(),
            64
        );
    }

    /// No price for a settlement fails the run, never a guess.
    #[test]
    fn a_missing_settlement_price_fails() {
        let (mut sim, _feed) = sim(r#"{"expiry":{"settle_from_quote":false}}"#);
        sim.pos.insert("SPY261005C00580000".into(), (1, 0));
        let e = sim.advance(CLOSE + S).unwrap_err();
        assert!(
            e.message
                .contains("no PM-close settlement price for SPY on 2026-10-05"),
            "{}",
            e.message
        );
    }

    /// Early assignment: a short call whose extrinsic value is below tomorrow's
    /// dividend is assigned at today's close; one with more time value isn't.
    #[test]
    fn short_calls_are_assigned_before_an_ex_dividend_date() {
        let dir = std::env::temp_dir().join(format!("sim-div-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("spy.csv");
        std::fs::write(&file, "ex_date,dividend_usd\n2026-10-06,1.80\n").unwrap();
        let script = format!(
            r#"{{"expiry":{{"early_assignment":true,"dividends":{{"SPY":{:?}}}}}}}"#,
            file.display().to_string()
        );
        let (mut sim, feed) = sim(&script);
        push(
            &feed,
            9,
            "SPY",
            599_990_000_000,
            600_010_000_000,
            CLOSE - 30 * S,
        );
        // Intrinsic 20: one bid 21.00 (extrinsic 1.00 < 1.80), one bid 23.00 (3.00).
        push(
            &feed,
            1,
            "SPY261016C00580000",
            21_000_000_000,
            21_100_000_000,
            CLOSE - 30 * S,
        );
        push(
            &feed,
            2,
            "SPY261120C00580000",
            23_000_000_000,
            23_100_000_000,
            CLOSE - 30 * S,
        );
        sim.pos.insert("SPY261016C00580000".into(), (-1, 0));
        sim.pos.insert("SPY261120C00580000".into(), (-1, 0));
        sim.advance(CLOSE + S).unwrap();
        assert_eq!(
            kinds(&mut sim),
            [("OPASN".to_owned(), "SPY261016C00580000".to_owned())]
        );
        assert_eq!(sim.pos.get("SPY").map(|p| p.0), Some(-100));
        assert_eq!(sim.pos.get("SPY261120C00580000").map(|p| p.0), Some(-1));
    }

    /// A quote from 10:00 is no settlement price at 16:00, and a price that can't be
    /// had changes nothing: no position removed, no cash moved, no activity sent.
    #[test]
    fn a_stale_quote_settles_nothing() {
        let (mut sim, feed) = sim("{}");
        push(&feed, 9, "SPY", 589_990_000_000, 590_010_000_000, T0);
        sim.pos.insert("SPY261005C00580000".into(), (1, 0));
        sim.pos.insert("SPY261005P00570000".into(), (-1, 0));
        let cash = sim.cash;
        let e = sim.advance(CLOSE + S).unwrap_err();
        assert!(e.message.contains("quote_max_age_s"), "{}", e.message);
        assert_eq!(sim.pos.len(), 2);
        assert_eq!(sim.cash, cash);
        assert!(kinds(&mut sim).is_empty());
    }

    /// An AM-settled root's last trading day is the session before its expiry date:
    /// on that Friday its monthly can't be ordered, VIX weeklies settle AM too.
    #[test]
    fn am_roots_stop_trading_the_session_before_expiry() {
        let (sim, _feed) = sim("{}");
        let friday = crate::safety::et_day(T0) + 4; // 2026-10-09
        let thursday = friday - 1;
        assert_eq!(sim.last_trading_day("SPX261009C05800000"), Some(thursday));
        assert!(sim.expired("SPX261009C05800000", friday));
        assert!(!sim.expired("SPXW261009C05800000", friday));
        assert!(!sim.expired("SPY261009C00580000", friday));
        assert_eq!(super::default_style("VIXW"), super::Style::CashAm);
    }
}
