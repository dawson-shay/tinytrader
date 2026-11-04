//! The v1 order intent: protocol validation, canonical form and `intent_sha256`.
use crate::protocol::{Fields, Invalid, Obj, arr, invalid};
use crate::safety::days_from_civil;
use serde_json::Value;
use sha2::{Digest, Sha256};

/// What a leg trades.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Class {
    /// An option contract (compact OCC symbol).
    Option,
    /// Shares (a ticker; single orders only).
    Equity,
    /// A futures contract (an outright such as `ESZ6`; single orders only).
    Future,
}

impl Class {
    fn name(self) -> &'static str {
        match self {
            Self::Option => "option",
            Self::Equity => "equity",
            Self::Future => "future",
        }
    }
}

/// One leg; a single order is one leg with ratio 1.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Leg {
    /// Compact OCC symbol, a stock's ticker, or a future's outright symbol.
    pub symbol: String,
    /// What it trades.
    pub class: Class,
    /// Ratio per parent unit (1 for a single).
    pub ratio: u32,
    /// `side: buy`.
    pub buy: bool,
    /// `effect: open`.
    pub open: bool,
}

/// A validated day intent of one allowlisted shape: a limit order, or a
/// market order carrying the client's reference price for the caps.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Intent {
    /// The only logical order key.
    pub client_order_id: String,
    /// `shape: combo`.
    pub combo: bool,
    /// One leg for a single, 2 to 4 for a combo.
    pub legs: Vec<Leg>,
    /// Contracts (parent units for a combo).
    pub qty: u32,
    /// Limit in cents; a combo is signed (+ debit, − credit). For a market
    /// order, its `ref_price`: what the client expects it to cost (the
    /// natural price when sent), which the notional cap is checked against.
    /// It doesn't bound the fill.
    pub limit_cents: i64,
    /// `type: market`: sent with no price; fills at whatever the market gives.
    pub market: bool,
    /// `time_in_force: cls`: the closing auction (limit-on-close), stock singles only.
    pub on_close: bool,
}

pub(crate) fn valid_id(id: &str) -> bool {
    (1..=128).contains(&id.len())
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b':' | b'-'))
}

/// Root and expiry (days since the epoch) of a compact OCC symbol.
pub(crate) fn occ(symbol: &str) -> Option<(&str, i64)> {
    let b = symbol.as_bytes();
    let root = b.len().checked_sub(15).filter(|n| (1..=6).contains(n))?;
    let digits = |r: std::ops::Range<usize>| b[r].iter().all(u8::is_ascii_digit);
    if !b[..root].iter().all(u8::is_ascii_uppercase)
        || !digits(root..root + 6)
        || !matches!(b[root + 6], b'C' | b'P')
        || !digits(root + 7..b.len())
    {
        return None;
    }
    let num = |i: usize| i64::from(b[i] - b'0') * 10 + i64::from(b[i + 1] - b'0');
    let (y, m, d) = (2000 + num(root), num(root + 2), num(root + 4));
    let days = days_from_civil(y, u32::try_from(m).ok()?, u32::try_from(d).ok()?);
    let back = crate::safety::civil_from_days(days);
    (back == (y, u32::try_from(m).ok()?, u32::try_from(d).ok()?)).then_some((&symbol[..root], days))
}

/// The parts of an option that its maximum loss needs.
pub(crate) struct Contract<'a> {
    pub root: &'a str,
    /// Days since the epoch.
    pub expiry: i64,
    pub put: bool,
    /// Whole cents; `None` when the strike has a fraction of a cent.
    pub strike_cents: Option<i64>,
}

/// The contract of a valid compact OCC symbol.
pub(crate) fn contract(symbol: &str) -> Option<Contract<'_>> {
    let (root, expiry) = occ(symbol)?;
    let mills: i64 = symbol[root.len() + 7..].parse().ok()?;
    Some(Contract {
        root,
        expiry,
        put: symbol.as_bytes()[root.len() + 6] == b'P',
        strike_cents: (mills % 10 == 0).then_some(mills / 10),
    })
}

/// Parses a decimal price with at most two decimal places into cents.
pub(crate) fn parse_cents(s: &str) -> Option<i64> {
    let (neg, body) = s.strip_prefix('-').map_or((false, s), |b| (true, b));
    let (int, frac) = body.split_once('.').unwrap_or((body, "00"));
    let ok = |p: &str| p.bytes().all(|b| b.is_ascii_digit());
    if int.is_empty() || int.len() > 9 || frac.is_empty() || frac.len() > 2 || !ok(int) || !ok(frac)
    {
        return None;
    }
    let cents = int.parse::<i64>().ok()? * 100
        + frac.parse::<i64>().ok()? * if frac.len() == 1 { 10 } else { 1 };
    Some(if neg { -cents } else { cents })
}

/// Formats cents with exactly two decimal places.
pub(crate) fn fmt_cents(c: i64) -> String {
    let sign = if c < 0 { "-" } else { "" };
    let a = c.unsigned_abs();
    format!("{sign}{}.{:02}", a / 100, a % 100)
}

fn gcd(a: u32, b: u32) -> u32 {
    if b == 0 { a } else { gcd(b, a % b) }
}

/// A stock ticker: 1 to 5 capitals, with an optional class suffix (`BRK.B`).
pub(crate) fn ticker(symbol: &str) -> bool {
    let (base, class) = symbol.split_once('.').unwrap_or((symbol, "A"));
    (1..=5).contains(&base.len())
        && base.bytes().all(|b| b.is_ascii_uppercase())
        && class.len() == 1
        && class.bytes().all(|b| b.is_ascii_uppercase())
}

/// The futures roots the gateway knows, with their multipliers (dollars per point):
/// the CME equity index futures, which expire quarterly (March, June, September,
/// December) on the third Friday.
const FUTURES: [(&str, u64); 7] = [
    ("ES", 50),
    ("MES", 5),
    ("NQ", 20),
    ("MNQ", 2),
    ("RTY", 50),
    ("M2K", 5),
    ("YM", 5),
];

/// A known futures root's multiplier.
#[must_use]
pub fn future_multiplier(root: &str) -> Option<u64> {
    FUTURES.iter().find(|(r, _)| *r == root).map(|(_, m)| *m)
}

/// The root, month (1 to 12) and year digits of an outright futures symbol of a known
/// root: the root, a quarterly month code (H, M, U, Z) and one or two digits of year,
/// as CME lists them (`ESZ6`, `MESH27`). Spreads (`ESZ6-ESH7`) are not outrights.
#[must_use]
pub fn future(symbol: &str) -> Option<(&str, u32, u32)> {
    // ASCII letters and digits only, so every slice below is on a char boundary.
    if !(3..=8).contains(&symbol.len())
        || !symbol
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
    {
        return None;
    }
    let digits = symbol.bytes().rev().take_while(u8::is_ascii_digit).count();
    if !(1..=2).contains(&digits) || symbol.len() < digits + 2 {
        return None;
    }
    let (head, year) = symbol.split_at(symbol.len() - digits);
    let (root, code) = head.split_at(head.len() - 1);
    let month = match code {
        "H" => 3,
        "M" => 6,
        "U" => 9,
        "Z" => 12,
        _ => return None,
    };
    future_multiplier(root)?;
    Some((root, month, year.parse().ok()?))
}

/// A futures symbol's expiry (days since the epoch): the third Friday of its month, in
/// the year whose last digits match, from the year before `today`'s (days since the
/// epoch) on. CME lists equity index futures up to a few years out, and a contract
/// just expired is still named in the days after it.
#[must_use]
pub fn future_expiry(symbol: &str, today: i64) -> Option<i64> {
    let (_, month, digits) = future(symbol)?;
    let width = symbol.bytes().rev().take_while(u8::is_ascii_digit).count();
    let modulus = if width == 2 { 100 } else { 10 };
    let (this_year, _, _) = crate::safety::civil_from_days(today);
    let year = (this_year - 1..this_year - 1 + modulus)
        .find(|y| y.rem_euclid(modulus) == i64::from(digits))?;
    // Days since the epoch are 0 on a Thursday: the month's first Friday, then two
    // weeks on.
    let first = days_from_civil(year, month, 1);
    let to_friday = (1 - first).rem_euclid(7);
    Some(first + to_friday + 14)
}

/// The index whose opening quotation a futures root settles to, where it has one.
#[must_use]
pub fn future_index(root: &str) -> Option<&'static str> {
    match root {
        "ES" | "MES" => Some("SPX"),
        "NQ" | "MNQ" => Some("NDX"),
        "RTY" | "M2K" => Some("RUT"),
        _ => None,
    }
}

/// `(symbol, class)`.
fn instrument(f: &mut Fields) -> Result<(String, Class), Invalid> {
    let mut i = Fields::new(f.req("instrument")?, &f.at("instrument"))?;
    let class = match i.string("class")?.as_str() {
        "option" => Class::Option,
        "equity" => Class::Equity,
        "future" => Class::Future,
        _ => return Err(invalid(&i.at("class"), "option, equity or future")),
    };
    let symbol = i.string("symbol")?;
    let valid = match class {
        Class::Equity => ticker(&symbol),
        Class::Option => occ(&symbol).is_some(),
        Class::Future => future(&symbol).is_some(),
    };
    if !valid {
        let want = match class {
            Class::Equity => "a stock ticker such as AAPL or BRK.B",
            Class::Option => "compact OCC symbol with a valid date",
            Class::Future => "an outright of a known futures root, such as ESZ6",
        };
        return Err(invalid(&i.at("symbol"), want));
    }
    i.finish()?;
    Ok((symbol, class))
}

fn side_effect(f: &mut Fields) -> Result<(bool, bool), Invalid> {
    let buy = match f.string("side")?.as_str() {
        "buy" => true,
        "sell" => false,
        _ => return Err(invalid(&f.at("side"), "buy or sell")),
    };
    let open = match f.string("effect")?.as_str() {
        "open" => true,
        "close" => false,
        _ => return Err(invalid(&f.at("effect"), "open or close")),
    };
    Ok((buy, open))
}

impl Intent {
    /// Protocol validation (section 6); the path prefix is `intent`.
    pub(crate) fn parse(v: Value) -> Result<Self, Invalid> {
        let mut f = Fields::new(v, "intent")?;
        let client_order_id = f.string("client_order_id")?;
        if !valid_id(&client_order_id) {
            return Err(invalid(
                &f.at("client_order_id"),
                "1 to 128 of [A-Za-z0-9._:-]",
            ));
        }
        let combo = match f.string("shape")?.as_str() {
            "single" => false,
            "combo" => true,
            _ => return Err(invalid(&f.at("shape"), "single or combo")),
        };
        let legs = if combo {
            let path = f.at("legs");
            let raw = f.array("legs")?;
            if !(2..=4).contains(&raw.len()) {
                return Err(invalid(&path, "2 to 4 legs"));
            }
            let mut legs = Vec::new();
            for (n, v) in raw.into_iter().enumerate() {
                let mut l = Fields::new(v, &format!("{path}[{n}]"))?;
                let (symbol, class) = instrument(&mut l)?;
                if class != Class::Option {
                    return Err(invalid(&format!("{path}[{n}]"), "combos are options only"));
                }
                let ratio = u32::try_from(l.int("ratio", 1, 10_000)?).unwrap_or(1);
                let (buy, open) = side_effect(&mut l)?;
                l.finish()?;
                if legs.iter().any(|x: &Leg| x.symbol == symbol) {
                    return Err(invalid(&path, "leg symbols must be distinct"));
                }
                legs.push(Leg {
                    symbol,
                    class,
                    ratio,
                    buy,
                    open,
                });
            }
            if legs.iter().fold(0, |g, l| gcd(g, l.ratio)) != 1 {
                return Err(invalid(&path, "leg ratios must have gcd 1"));
            }
            legs
        } else {
            let (symbol, class) = instrument(&mut f)?;
            let (buy, open) = side_effect(&mut f)?;
            vec![Leg {
                symbol,
                class,
                ratio: 1,
                buy,
                open,
            }]
        };
        let qty = u32::try_from(f.int("qty", 1, 1_000_000)?).unwrap_or(1);
        let market = match f.string("type")?.as_str() {
            "limit" => false,
            "market" => true,
            _ => return Err(invalid(&f.at("type"), "limit or market")),
        };
        // A market order names no limit, but a reference price for the caps.
        let price_field = if market { "ref_price" } else { "limit_price" };
        let price_path = f.at(price_field);
        let limit_cents = parse_cents(&f.string(price_field)?)
            .filter(|c| if combo { *c != 0 } else { *c > 0 })
            .ok_or_else(|| {
                invalid(
                    &price_path,
                    "decimal, at most 2 places; single > 0, combo != 0",
                )
            })?;
        let on_close = match f.string("time_in_force")?.as_str() {
            "day" => false,
            "cls" if !combo && legs[0].class == Class::Equity && !market => true,
            _ => {
                return Err(invalid(
                    &f.at("time_in_force"),
                    "day, or cls for a stock single limit",
                ));
            }
        };
        f.finish()?;
        Ok(Self {
            client_order_id,
            combo,
            legs,
            qty,
            limit_cents,
            market,
            on_close,
        })
    }

    /// Whether this is a stock order (a single equity leg).
    #[must_use]
    pub fn is_equity(&self) -> bool {
        !self.combo && self.legs[0].class == Class::Equity
    }

    /// Whether this is a futures order (a single futures leg).
    #[must_use]
    pub fn is_future(&self) -> bool {
        !self.combo && self.legs[0].class == Class::Future
    }

    /// Dollars per unit of price: 100 for an option contract, 1 for a share, the root's
    /// for a future (50 for ES).
    #[must_use]
    pub fn multiplier(&self) -> u64 {
        if self.is_equity() {
            1
        } else if self.is_future() {
            future(&self.legs[0].symbol)
                .and_then(|(root, _, _)| future_multiplier(root))
                .unwrap_or(1)
        } else {
            100
        }
    }

    /// Whether any leg opens a position.
    #[must_use]
    pub fn has_open(&self) -> bool {
        self.legs.iter().any(|l| l.open)
    }

    /// The canonical intent: fixed field order, no whitespace, two-place prices.
    #[must_use]
    pub fn canonical(&self) -> String {
        let instrument = |l: &Leg| {
            Obj::new()
                .s("class", l.class.name())
                .s("symbol", &l.symbol)
                .done()
        };
        let side = |l: &Leg| if l.buy { "buy" } else { "sell" };
        let effect = |l: &Leg| if l.open { "open" } else { "close" };
        let mut o = Obj::new().s("client_order_id", &self.client_order_id);
        if self.combo {
            let legs = arr(self.legs.iter().map(|l| {
                Obj::new()
                    .raw("instrument", &instrument(l))
                    .n("ratio", l.ratio)
                    .s("side", side(l))
                    .s("effect", effect(l))
                    .done()
            }));
            o = o.s("shape", "combo").raw("legs", &legs);
        } else {
            let l = &self.legs[0];
            o = o
                .s("shape", "single")
                .raw("instrument", &instrument(l))
                .s("side", side(l))
                .s("effect", effect(l));
        }
        o.n("qty", self.qty)
            .s("type", if self.market { "market" } else { "limit" })
            .s(
                if self.market {
                    "ref_price"
                } else {
                    "limit_price"
                },
                &fmt_cents(self.limit_cents),
            )
            .s("time_in_force", if self.on_close { "cls" } else { "day" })
            .done()
    }

    /// Lowercase hex SHA-256 of [`Intent::canonical`].
    #[must_use]
    pub fn sha256(&self) -> String {
        hex(&Sha256::digest(self.canonical().as_bytes()))
    }
}

/// What an opening intent can lose, and what it costs to open.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MaxLoss {
    /// The most the intent can lose, in cents, for its whole quantity.
    pub cents: u64,
    /// The debit the buyer pays, in cents, for its whole quantity. `None` for a credit,
    /// a short leg, or any intent with no debit.
    pub premium_cents: Option<u64>,
}

/// The most an opening intent can lose, or the reason it cannot be bounded. A long option or
/// share loses its premium; a naked short call or share is `max_loss_unbounded`; a naked
/// short put loses its strike less the credit. A combo of options with one underlying and
/// one expiry loses its net debit less the least payoff at expiry (see [`combo_loss`]).
/// A combo with a closing leg is `max_loss_unknown`, and so is a combo across expiries other
/// than a two-leg calendar or diagonal (see [`calendar_loss`]). A partial close is not an
/// intent property; the caller passes the open quantity that remains, since the loss is
/// linear in quantity and zero once nothing remains open. This is [`max_loss_with`] with the
/// profile's `--allow-calendars` off.
pub fn max_loss(intent: &Intent) -> Result<MaxLoss, &'static str> {
    max_loss_with(intent, false)
}

/// [`max_loss`] under a profile: with `allow_calendars` (`--allow-calendars`), a combo across
/// expiries that [`calendar_loss`] does not bound is also sized as a covered calendar (see
/// [`covered_calendar_loss`]): a double calendar, or calendars in ratio.
pub fn max_loss_with(intent: &Intent, allow_calendars: bool) -> Result<MaxLoss, &'static str> {
    let limit = i128::from(intent.limit_cents);
    let (per_unit, debit) = if intent.combo {
        (combo_loss(intent, allow_calendars)?, intent.limit_cents > 0)
    } else {
        (single_loss(&intent.legs[0], limit)?, intent.legs[0].buy)
    };
    let mult = i128::from(intent.multiplier());
    let qty = i128::from(intent.qty);
    // Nothing left open (a full close of the opening quantity) has no loss.
    let cents = if intent.qty == 0 {
        0
    } else {
        u64::try_from(per_unit * qty * mult)
            .ok()
            .filter(|v| *v > 0)
            .ok_or("max_loss_unknown")?
    };
    let premium_cents = debit
        .then(|| u64::try_from(limit * qty * mult).ok())
        .flatten();
    Ok(MaxLoss {
        cents,
        premium_cents,
    })
}

/// The loss per unit of a single opening leg, in cents of price (signed).
fn single_loss(leg: &Leg, limit: i128) -> Result<i128, &'static str> {
    if !leg.open {
        return Err("max_loss_unknown");
    }
    // A future's risk is capped by its contract count, not a loss bound.
    if leg.class == Class::Future {
        return Err("max_loss_future");
    }
    if leg.buy {
        return Ok(limit);
    }
    if leg.class == Class::Equity {
        return Err("max_loss_unbounded");
    }
    match contract(&leg.symbol) {
        Some(c) if c.put => Ok(strike_of(&c)? - limit),
        Some(_) => Err("max_loss_unbounded"),
        None => Err("max_loss_unknown"),
    }
}

/// Index roots whose options are European: they exercise only at expiry, so their value before
/// expiry is discounted and can sit below the intrinsic payoff. SPXW is the weekly SPX root,
/// VIXW the weekly VIX root, NDXP the PM-settled NDX root and RUTW the weekly RUT root. The list
/// must match the contract specs: an unlisted root is treated as American. The payoff-floor
/// bound of [`combo_loss`] and the calendar bound of [`calendar_loss`] depend on this.
pub(crate) const EUROPEAN_ROOTS: [&str; 14] = [
    "SPX", "SPXW", "XSP", "NDX", "NDXP", "XND", "RUT", "RUTW", "MRUT", "DJX", "XEO", "VIX", "VIXW",
    "SPIKE",
];

/// The loss per parent unit of a combo, in cents of price (signed). The payoff at expiry is
/// piecewise linear with breakpoints at the strikes and at zero, so its least value is at
/// one of those points, provided it does not keep falling above the highest strike. The
/// limit is signed (+ debit, - credit), so the loss is the limit less that least payoff:
/// a $1.00 credit against a least payoff of -$5.00 loses $4.00.
///
/// Each leg pays max(S - K, 0) for a call and max(K - S, 0) for a put, per unit of its
/// ratio and signed by its side (+ bought, - sold). The slope above the highest strike is
/// the sum of the signed call ratios; if it is negative the loss is unbounded.
///
/// A combo with a closing leg is unknown. The intent carries one net limit for every leg
/// and no price per leg, so the closing legs' share of it cannot be read. Nothing is
/// assumed about that split, so the opening legs are not sized alone. Legs with different
/// expiries are a calendar or diagonal (see [`calendar_loss`]).
///
/// The bound at the least payoff is sound only if the position is worth at least that payoff
/// before expiry. An American option always is (it can be exercised). A European one is not:
/// its value is discounted, so a long position with a payoff floor above zero can be worth less
/// than the floor before expiry. For a European root such a floor is not used, and the combo is
/// `max_loss_unknown`. A floor at zero is the debit, which holds for any option, and a negative
/// least payoff (a short-side bound) is no less than its discounted value, so both stay.
fn combo_loss(intent: &Intent, allow_calendars: bool) -> Result<i128, &'static str> {
    if intent.legs.iter().any(|l| !l.open) {
        return Err("max_loss_unknown");
    }
    let cs = intent
        .legs
        .iter()
        .map(|l| contract(&l.symbol).ok_or("max_loss_unknown"))
        .collect::<Result<Vec<_>, _>>()?;
    if cs.iter().any(|c| c.root != cs[0].root) {
        return Err("max_loss_unknown");
    }
    if cs.iter().any(|c| c.expiry != cs[0].expiry) {
        let pair = calendar_loss(intent, &cs);
        return if pair.is_err() && allow_calendars {
            covered_calendar_loss(intent, &cs)
        } else {
            pair
        };
    }
    // Per leg: its strike in cents, whether it is a put, and its signed ratio.
    let parts = intent
        .legs
        .iter()
        .zip(&cs)
        .map(|(l, c)| {
            let w = i128::from(l.ratio);
            Ok((strike_of(c)?, c.put, if l.buy { w } else { -w }))
        })
        .collect::<Result<Vec<_>, &'static str>>()?;
    let slope_above: i128 = parts.iter().filter(|p| !p.1).map(|p| p.2).sum();
    if slope_above < 0 {
        return Err("max_loss_unbounded");
    }
    let payoff = |s: i128| -> i128 {
        parts
            .iter()
            .map(|&(k, put, w)| w * if put { (k - s).max(0) } else { (s - k).max(0) })
            .sum()
    };
    // Zero bounds the underlying below; every strike is a breakpoint.
    let least = parts
        .iter()
        .map(|p| p.0)
        .chain([0])
        .map(payoff)
        .min()
        .unwrap_or(0);
    if least > 0 && EUROPEAN_ROOTS.contains(&cs[0].root) {
        return Err("max_loss_unknown");
    }
    Ok(i128::from(intent.limit_cents) - least)
}

/// The loss per unit of a calendar or diagonal: one short and one long leg, one-to-one, of
/// one type, with the short expiring first. A long at least as favourable as the short (a
/// call at or below its strike, a put at or above) covers it, and the loss is the debit.
/// A less favourable long (a call above the short strike, a put below it) leaves the gap
/// between the strikes open, so the loss is the debit plus that gap. A short expiring after
/// its long is unknown, as is anything with more legs or ratios other than one (but see
/// [`covered_calendar_loss`], under `--allow-calendars`).
///
/// Every calendar or diagonal on a European root, call or put, is unknown whatever its strikes.
/// The bound assumes that when the short expires the long is worth at least the short's payoff,
/// which holds for an American long (it can be exercised) but not for a European one, which
/// can only be held. A European put is worth its strike discounted, not its intrinsic value.
/// A deep in-the-money European call is worth less than its intrinsic value once the dividends
/// paid before its expiry outweigh the interest on the strike. And VIX and VIXW expiries each
/// settle on a different VIX futures month, so the long is not even on the same underlying
/// value as the short: a later VIX call can be worth far less than an expiring one.
fn calendar_loss(intent: &Intent, cs: &[Contract]) -> Result<i128, &'static str> {
    let (a, b) = match (intent.legs.as_slice(), cs) {
        ([a, b], [_, _]) => (a, b),
        _ => return Err("max_loss_unknown"),
    };
    if a.ratio != 1 || b.ratio != 1 || a.buy == b.buy {
        return Err("max_loss_unknown");
    }
    let (si, li) = if a.buy { (1, 0) } else { (0, 1) };
    let (short, long) = (&cs[si], &cs[li]);
    if short.put != long.put || short.expiry > long.expiry {
        return Err("max_loss_unknown");
    }
    if EUROPEAN_ROOTS.contains(&short.root) {
        return Err("max_loss_unknown");
    }
    let (ks, kl) = (strike_of(short)?, strike_of(long)?);
    // Positive when the long's strike is less favourable than the short's.
    let gap = if short.put { ks - kl } else { kl - ks };
    Ok(i128::from(intent.limit_cents) + gap.max(0))
}

/// The loss per unit of a covered calendar, under `--allow-calendars`: every short leg is
/// covered by long legs of the same root, type and strike that expire strictly later, with at
/// least its ratio between them, and no long's ratio is counted twice. Every long covers some
/// earlier short, so no leg is left outside a calendar; a long may have more ratio than the
/// shorts it covers. A double calendar (a put and a call calendar at two strikes) and a
/// calendar sold 2 by 1 against two later longs are such combos; a diagonal (strikes that
/// differ) of more than two legs is not.
///
/// When a short expires, an American long of the same strike expiring later is worth at least
/// the short's payoff (it can be exercised), so each pair is worth at least zero and the combo
/// can lose no more than its debit. Only a debit is bounded this way: a credit or a zero debit
/// of this shape is `max_loss_unknown` (it would claim no loss at all). A European root (see
/// [`calendar_loss`] for why) is `max_loss_unknown`.
///
/// Coverage is checked as a matching. A short can be covered by any long expiring after it, so
/// the shorts expiring on or after any short's expiry need at least as much long ratio expiring
/// strictly after that date; checked at every short's expiry, that is enough for a matching that
/// counts each long once.
fn covered_calendar_loss(intent: &Intent, cs: &[Contract]) -> Result<i128, &'static str> {
    if intent.limit_cents <= 0 || EUROPEAN_ROOTS.contains(&cs[0].root) {
        return Err("max_loss_unknown");
    }
    // Per type and strike: each leg's expiry and signed ratio (+ bought, - sold).
    let mut groups: std::collections::BTreeMap<(bool, i128), Vec<(i64, i64)>> =
        std::collections::BTreeMap::new();
    for (l, c) in intent.legs.iter().zip(cs) {
        let w = i64::from(l.ratio);
        groups
            .entry((c.put, strike_of(c)?))
            .or_default()
            .push((c.expiry, if l.buy { w } else { -w }));
    }
    for legs in groups.values() {
        for &(e, q) in legs {
            let paired = legs.iter().any(|&(e2, q2)| {
                if q < 0 {
                    q2 > 0 && e2 > e
                } else {
                    q2 < 0 && e2 < e
                }
            });
            if !paired {
                return Err("max_loss_unknown");
            }
        }
        for &(e, _) in legs.iter().filter(|l| l.1 < 0) {
            let short: i64 = legs
                .iter()
                .filter(|l| l.1 < 0 && l.0 >= e)
                .map(|l| -l.1)
                .sum();
            let long: i64 = legs
                .iter()
                .filter(|l| l.1 > 0 && l.0 > e)
                .map(|l| l.1)
                .sum();
            if short > long {
                return Err("max_loss_unknown");
            }
        }
    }
    Ok(i128::from(intent.limit_cents))
}

fn strike_of(c: &Contract) -> Result<i128, &'static str> {
    c.strike_cents.map(i128::from).ok_or("max_loss_unknown")
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::new(), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

#[cfg(test)]
mod tests {
    use super::{Class, Intent, Leg, MaxLoss, max_loss, max_loss_with};

    fn max_loss_cents(o: &Intent) -> Result<u64, &'static str> {
        max_loss(o).map(|m| m.cents)
    }

    const C575: &str = "SPY261005C00575000";
    const C580: &str = "SPY261005C00580000";
    const C585: &str = "SPY261005C00585000";
    const C580_NEXT: &str = "SPY261012C00580000";
    const P565: &str = "SPY261005P00565000";
    const P570: &str = "SPY261005P00570000";
    const P570_ODD: &str = "SPY261005P00570005";
    const P580: &str = "SPY261005P00580000";
    const C570: &str = "SPY261005C00570000";
    const C585_NEXT: &str = "SPY261012C00585000";

    fn leg(symbol: &str, buy: bool, open: bool) -> Leg {
        Leg {
            symbol: symbol.into(),
            class: Class::Option,
            ratio: 1,
            buy,
            open,
        }
    }

    fn ratio_leg(symbol: &str, buy: bool, ratio: u32) -> Leg {
        Leg {
            ratio,
            ..leg(symbol, buy, true)
        }
    }

    fn share(buy: bool, open: bool) -> Leg {
        Leg {
            symbol: "AAPL".into(),
            class: Class::Equity,
            ratio: 1,
            buy,
            open,
        }
    }

    /// A limit intent: `limit` in cents, signed for a combo (+ debit, - credit).
    fn order(legs: Vec<Leg>, limit: i64, qty: u32) -> Intent {
        Intent {
            client_order_id: "wse-t".into(),
            combo: legs.len() > 1,
            legs,
            qty,
            limit_cents: limit,
            market: false,
            on_close: false,
        }
    }

    #[test]
    fn a_single_long_is_its_premium() {
        // $1.25 a contract, 10 contracts: $1,250, as before.
        let o = order(vec![leg(C580, true, true)], 125, 10);
        assert_eq!(max_loss_cents(&o), Ok(125 * 10 * 100));
        let s = order(vec![share(true, true)], 22_250, 12);
        assert_eq!(max_loss_cents(&s), Ok(22_250 * 12));
        assert_eq!(max_loss(&o).unwrap().premium_cents, Some(125 * 10 * 100));
    }

    #[test]
    fn a_credit_spread_loses_its_width_less_the_credit() {
        // Sell 570 put, buy 565 put, $1.00 credit: $5 wide, so $4 a share at risk.
        let o = order(
            vec![leg(P570, false, true), leg(P565, true, true)],
            -100,
            10,
        );
        assert_eq!(max_loss_cents(&o), Ok(400 * 10 * 100));
        // A call credit spread: sell 580, buy 585 for $1.00.
        let c = order(vec![leg(C580, false, true), leg(C585, true, true)], -100, 1);
        assert_eq!(max_loss_cents(&c), Ok(400 * 100));
        // A credit is received, not paid: no premium.
        assert_eq!(max_loss(&o).unwrap().premium_cents, None);
    }

    #[test]
    fn a_debit_vertical_loses_its_debit() {
        // A call debit spread: buy 580, sell 585 for $1.50.
        let call = order(vec![leg(C580, true, true), leg(C585, false, true)], 150, 3);
        assert_eq!(max_loss_cents(&call), Ok(150 * 3 * 100));
        assert_eq!(max_loss(&call).unwrap().premium_cents, Some(150 * 3 * 100));
        // A put debit spread: buy 570, sell 565 for $2.00.
        let put = order(vec![leg(P570, true, true), leg(P565, false, true)], 200, 1);
        assert_eq!(max_loss_cents(&put), Ok(200 * 100));
    }

    #[test]
    fn a_debit_with_the_long_inside_loses_the_width_too() {
        // Buy 585, sell 580 calls for a $1.00 debit: the short leg is in the money first.
        let o = order(vec![leg(C585, true, true), leg(C580, false, true)], 100, 1);
        assert_eq!(max_loss_cents(&o), Ok((500 + 100) * 100));
    }

    #[test]
    fn a_credit_worth_more_than_the_width_is_unknown() {
        let o = order(vec![leg(P570, false, true), leg(P565, true, true)], -600, 1);
        assert_eq!(max_loss_cents(&o), Err("max_loss_unknown"));
    }

    #[test]
    fn an_iron_condor_loses_its_wider_wing_less_the_credit() {
        // Puts 565/570 (5 wide), calls 575/585 (10 wide), $2.00 credit.
        let o = order(
            vec![
                leg(P565, true, true),
                leg(P570, false, true),
                leg(C575, false, true),
                leg(C585, true, true),
            ],
            -200,
            1,
        );
        assert_eq!(max_loss_cents(&o), Ok((1000 - 200) * 100));
        // The same wings for a $2.00 debit: the payoff falls to -$10 above 585 (short 575,
        // long 585 calls), so the loss is the wider wing plus the debit: $12.
        let debit = order(
            vec![
                leg(P565, true, true),
                leg(P570, false, true),
                leg(C575, false, true),
                leg(C585, true, true),
            ],
            200,
            1,
        );
        assert_eq!(max_loss_cents(&debit), Ok((1000 + 200) * 100));
    }

    #[test]
    fn a_naked_short_call_or_share_is_unbounded() {
        let call = order(vec![leg(C580, false, true)], 125, 1);
        assert_eq!(max_loss_cents(&call), Err("max_loss_unbounded"));
        let short = order(vec![share(false, true)], 22_250, 1);
        assert_eq!(max_loss_cents(&short), Err("max_loss_unbounded"));
    }

    #[test]
    fn a_naked_short_put_is_its_strike_less_the_credit() {
        // Sell the 570 put for $1.00: $569 a share at risk.
        let o = order(vec![leg(P570, false, true)], 100, 2);
        assert_eq!(max_loss_cents(&o), Ok((57_000 - 100) * 2 * 100));
        assert_eq!(max_loss(&o).unwrap().premium_cents, None);
    }

    #[test]
    fn a_butterfly_with_a_debit_loses_the_debit() {
        // Buy 575, sell two 580, buy 585 calls for $1.00. The payoff is 0 at and below 575
        // and at and above 585, and peaks at $5 at 580, so its least payoff is 0.
        // Loss: $1.00 a share, $100 a contract.
        let fly = order(
            vec![
                leg(C575, true, true),
                ratio_leg(C580, false, 2),
                leg(C585, true, true),
            ],
            100,
            1,
        );
        assert_eq!(max_loss_cents(&fly), Ok(100 * 100));
        assert_eq!(max_loss(&fly).unwrap().premium_cents, Some(100 * 100));
    }

    #[test]
    fn a_butterfly_with_a_credit_loses_its_width_less_the_credit() {
        // Sell 575 and 585 calls, buy two 580 calls, for a $1.00 credit. At 580 the payoff
        // is -$5 (the short 575 is $5 in the money), and it is 0 at 575 and at 585. So the
        // least payoff is -$5: loss $5 - $1 = $4 a share, $400 a contract.
        let fly = order(
            vec![
                leg(C575, false, true),
                ratio_leg(C580, true, 2),
                leg(C585, false, true),
            ],
            -100,
            1,
        );
        assert_eq!(max_loss_cents(&fly), Ok(400 * 100));
        assert_eq!(max_loss(&fly).unwrap().premium_cents, None);
    }

    #[test]
    fn a_debit_condor_loses_its_debit() {
        // Buy the 570 put and the 575 call, sell the 565 put and the 585 call, for $2.00.
        // The payoff is between 0 and $10 and touches 0 on [570, 575], so the loss is the
        // $2.00 debit: $200 a contract.
        let condor = order(
            vec![
                leg(P570, true, true),
                leg(P565, false, true),
                leg(C575, true, true),
                leg(C585, false, true),
            ],
            200,
            1,
        );
        assert_eq!(max_loss_cents(&condor), Ok(200 * 100));
        assert_eq!(max_loss(&condor).unwrap().premium_cents, Some(200 * 100));
    }

    #[test]
    fn a_put_ratio_spread_is_bounded_and_a_call_ratio_spread_is_not() {
        // Buy one 580 put, sell two 570 puts, for a $1.00 credit. The payoff is 580 - 2 x 570
        // = -$560 at zero, and it rises to 0 at 580, so the least payoff is -$560 a share.
        // Loss: $560 - $1 = $559 a share, $55,900 a contract.
        let put = order(
            vec![leg(P580, true, true), ratio_leg(P570, false, 2)],
            -100,
            1,
        );
        assert_eq!(max_loss_cents(&put), Ok(559 * 100 * 100));
        // Buy one 575 call, sell two 585 calls, for a $1.00 debit: above 585 the payoff
        // falls by $1 for each dollar of the underlying, so the loss is unbounded.
        let call = order(
            vec![leg(C575, true, true), ratio_leg(C585, false, 2)],
            100,
            1,
        );
        assert_eq!(max_loss_cents(&call), Err("max_loss_unbounded"));
    }

    #[test]
    fn a_long_combo_with_a_payoff_floor_loses_less_than_its_debit() {
        // Buy the 580 put and the 570 call for $11.00. Between 570 and 580 the payoff is a
        // constant $10, so the least payoff is $10 and the loss is $1: $100 a contract.
        let o = order(vec![leg(P580, true, true), leg(C570, true, true)], 1100, 1);
        assert_eq!(max_loss_cents(&o), Ok(100 * 100));
    }

    #[test]
    fn a_long_combo_with_a_floor_is_unknown_on_a_european_root() {
        // The same shape as the SPY test above, on the European SPX, SPXW and XSP roots: the
        // floor is not used, so no bound is claimed. SPY keeps its bound of $1.
        for root in ["SPX", "SPXW", "XSP"] {
            let put = format!("{root}261005P00580000");
            let call = format!("{root}261005C00570000");
            let o = order(vec![leg(&put, true, true), leg(&call, true, true)], 1100, 1);
            assert_eq!(max_loss_cents(&o), Err("max_loss_unknown"), "{root}");
        }
        let spy = order(vec![leg(P580, true, true), leg(C570, true, true)], 1100, 1);
        assert_eq!(max_loss_cents(&spy), Ok(100 * 100));
    }

    #[test]
    fn a_covered_calendar_loses_its_debit() {
        // Sell the 580 call and buy the 580 call expiring a week later, for a $1.50 debit, two
        // contracts. The long covers the short at the same strike, so the loss is the debit:
        // $1.50 x 100 x 2 = $300.
        let cal = order(
            vec![leg(C580, false, true), leg(C580_NEXT, true, true)],
            150,
            2,
        );
        assert_eq!(max_loss_cents(&cal), Ok(150 * 2 * 100));
        assert_eq!(max_loss(&cal).unwrap().premium_cents, Some(150 * 2 * 100));
    }

    #[test]
    fn a_diagonal_loses_its_debit_plus_the_strike_gap() {
        // Sell the 580 call and buy the 585 call expiring a week later, for a $1.50 debit. The long
        // is the less favourable strike, so the 585 - 580 = $5 gap is open: the loss is
        // $1.50 + $5.00 = $6.50 a share, $650 a contract.
        let diag = order(
            vec![leg(C580, false, true), leg(C585_NEXT, true, true)],
            150,
            1,
        );
        assert_eq!(max_loss_cents(&diag), Ok((150 + 500) * 100));
    }

    #[test]
    fn a_european_put_calendar_with_a_lower_long_strike_is_unknown() {
        // Sell the 580 put expiring 16 Oct and buy the 570 put expiring 23 Oct, for a $1.00
        // debit. On SPXW the long put is worth its discounted strike at a zero underlying, so
        // the loss exceeds the $11.00 debit-plus-gap bound: no bound is claimed.
        let cal = order(
            vec![
                leg("SPXW261016P00580000", false, true),
                leg("SPXW261023P00570000", true, true),
            ],
            100,
            1,
        );
        assert_eq!(max_loss_cents(&cal), Err("max_loss_unknown"));
        // The same on the other European roots, and with the long strike at or above the short.
        for root in [
            "SPX", "XSP", "NDX", "NDXP", "XND", "RUT", "RUTW", "MRUT", "DJX", "XEO", "VIX", "VIXW",
            "SPIKE",
        ] {
            let cal = order(
                vec![
                    leg(&format!("{root}261016P00580000"), false, true),
                    leg(&format!("{root}261023P00570000"), true, true),
                ],
                100,
                1,
            );
            assert_eq!(max_loss_cents(&cal), Err("max_loss_unknown"), "{root}");
        }
        let covered = order(
            vec![
                leg("SPXW261016P00570000", false, true),
                leg("SPXW261023P00580000", true, true),
            ],
            100,
            1,
        );
        assert_eq!(max_loss_cents(&covered), Err("max_loss_unknown"));
    }

    #[test]
    fn a_put_calendar_with_a_lower_long_strike_keeps_its_bound_on_spy() {
        // The same put calendar on SPY is American, so the bound stands: $1.00 debit plus the
        // $10 gap, $11.00 a contract.
        let cal = order(
            vec![
                leg("SPY261016P00580000", false, true),
                leg("SPY261023P00570000", true, true),
            ],
            100,
            1,
        );
        assert_eq!(max_loss_cents(&cal), Ok(1100 * 100));
    }

    #[test]
    fn a_european_call_calendar_or_diagonal_is_unknown() {
        // Sell the VIX 20 call expiring 21 Oct and buy the 20 call expiring 18 Nov, for a $1.00
        // debit. Each expiry settles on its own VIX futures month, so the long need not be
        // worth the short's payoff: no bound is claimed.
        let vix = order(
            vec![
                leg("VIX261021C00020000", false, true),
                leg("VIX261118C00020000", true, true),
            ],
            100,
            1,
        );
        assert_eq!(max_loss_cents(&vix), Err("max_loss_unknown"));
        // The same for a call calendar and a call diagonal on every other European root: a deep
        // in-the-money European call can be worth less than its intrinsic value.
        for root in [
            "SPX", "SPXW", "XSP", "NDX", "NDXP", "XND", "RUT", "RUTW", "MRUT", "DJX", "XEO",
            "VIXW", "SPIKE",
        ] {
            for long_strike in ["00580000", "00585000"] {
                let cal = order(
                    vec![
                        leg(&format!("{root}261016C00580000"), false, true),
                        leg(&format!("{root}261023C{long_strike}"), true, true),
                    ],
                    100,
                    1,
                );
                assert_eq!(
                    max_loss_cents(&cal),
                    Err("max_loss_unknown"),
                    "{root} {long_strike}"
                );
            }
        }
    }

    #[test]
    fn a_spy_call_calendar_keeps_its_bound() {
        // Sell the 580 call expiring 5 Oct and buy the 580 call expiring 12 Oct, for a $1.00
        // debit. SPY is American, so the long covers the short: $100 a contract.
        let cal = order(
            vec![leg(C580, false, true), leg(C580_NEXT, true, true)],
            100,
            1,
        );
        assert_eq!(max_loss_cents(&cal), Ok(100 * 100));
    }

    /// The loss with `--allow-calendars` on.
    fn with_calendars(o: &Intent) -> Result<u64, &'static str> {
        max_loss_with(o, true).map(|m| m.cents)
    }

    const P570_NEXT: &str = "SPY261012P00570000";
    const P565_NEXT: &str = "SPY261012P00565000";
    const C580_LATER: &str = "SPY261019C00580000";

    /// Sell the 5 Oct 570 put and 580 call, buy the 12 Oct 570 put and 580 call.
    fn double_calendar(limit: i64, qty: u32) -> Intent {
        order(
            vec![
                leg(P570, false, true),
                leg(P570_NEXT, true, true),
                leg(C580, false, true),
                leg(C580_NEXT, true, true),
            ],
            limit,
            qty,
        )
    }

    #[test]
    fn a_double_calendar_loses_its_debit_with_calendars_allowed() {
        // A $2.40 debit, two units: each short is covered by the later long of its type and
        // strike, so the loss is the debit, $2.40 x 100 x 2 = $480.
        let o = double_calendar(240, 2);
        assert_eq!(with_calendars(&o), Ok(240 * 100 * 2));
        assert_eq!(
            max_loss_with(&o, true).unwrap().premium_cents,
            Some(240 * 100 * 2)
        );
        // The legs in another order are the same combo.
        let mut shuffled = o.clone();
        shuffled.legs.reverse();
        assert_eq!(with_calendars(&shuffled), Ok(240 * 100 * 2));
    }

    #[test]
    fn a_double_calendar_is_unknown_with_calendars_off() {
        let o = double_calendar(240, 2);
        assert_eq!(max_loss_cents(&o), Err("max_loss_unknown"));
        assert_eq!(max_loss_with(&o, false), max_loss(&o));
    }

    #[test]
    fn a_single_calendar_keeps_its_debit_with_calendars_allowed() {
        let cal = order(
            vec![leg(C580, false, true), leg(C580_NEXT, true, true)],
            150,
            2,
        );
        assert_eq!(with_calendars(&cal), Ok(150 * 2 * 100));
        assert_eq!(with_calendars(&cal), max_loss_cents(&cal));
    }

    #[test]
    fn a_diagonal_is_not_a_covered_calendar() {
        // A double diagonal: the backs are at other strikes, so neither short is covered.
        let o = order(
            vec![
                leg(P570, false, true),
                leg(P565_NEXT, true, true),
                leg(C580, false, true),
                leg(C585_NEXT, true, true),
            ],
            240,
            1,
        );
        assert_eq!(with_calendars(&o), Err("max_loss_unknown"));
        // A two-leg diagonal keeps the bound it has without the flag: debit plus the gap.
        let diag = order(
            vec![leg(C580, false, true), leg(C585_NEXT, true, true)],
            150,
            1,
        );
        assert_eq!(with_calendars(&diag), Ok((150 + 500) * 100));
    }

    #[test]
    fn a_short_covered_only_in_its_own_expiry_is_unknown() {
        // The 580 calls are a calendar, but the 570 put sold on 12 Oct has only a 565 put of the
        // same expiry beside it: a vertical, not a later long of its strike.
        let o = order(
            vec![
                leg(C580, false, true),
                leg(C580_NEXT, true, true),
                leg(P570_NEXT, false, true),
                leg(P565_NEXT, true, true),
            ],
            100,
            1,
        );
        assert_eq!(with_calendars(&o), Err("max_loss_unknown"));
        // A long of the same expiry never covers: the cover must expire strictly later.
        // (Validation refuses the repeated symbol; the rule refuses the shape anyway.)
        let same = order(
            vec![
                leg(C580, false, true),
                leg(C580_NEXT, true, true),
                leg(P570_NEXT, false, true),
                leg(P570_NEXT, true, true),
            ],
            100,
            1,
        );
        assert_eq!(with_calendars(&same), Err("max_loss_unknown"));
    }

    #[test]
    fn a_back_with_less_quantity_is_unknown() {
        // Sell two front 580 calls against one back: one is uncovered.
        let o = order(
            vec![ratio_leg(C580, false, 2), ratio_leg(C580_NEXT, true, 1)],
            50,
            1,
        );
        assert_eq!(with_calendars(&o), Err("max_loss_unknown"));
        // The same on the put side of a double calendar.
        let d = order(
            vec![
                ratio_leg(P570, false, 2),
                ratio_leg(P570_NEXT, true, 1),
                ratio_leg(C580, false, 2),
                ratio_leg(C580_NEXT, true, 3),
            ],
            100,
            1,
        );
        assert_eq!(with_calendars(&d), Err("max_loss_unknown"));
    }

    #[test]
    fn calendars_in_ratio_are_covered_without_double_counting() {
        // Sell one front 580 call and buy two backs: covered, with a long to spare.
        let more = order(
            vec![ratio_leg(C580, false, 1), ratio_leg(C580_NEXT, true, 2)],
            300,
            3,
        );
        assert_eq!(with_calendars(&more), Ok(300 * 100 * 3));
        // Sell two front 580 calls, buy one on 12 Oct and one on 19 Oct: each covers one.
        let spread = order(
            vec![
                ratio_leg(C580, false, 2),
                ratio_leg(C580_NEXT, true, 1),
                ratio_leg(C580_LATER, true, 1),
            ],
            200,
            1,
        );
        assert_eq!(with_calendars(&spread), Ok(200 * 100));
        // Sell the 5 Oct and the 12 Oct 580 calls against one 19 Oct: the long can cover only
        // one of them.
        let twice = order(
            vec![
                leg(C580, false, true),
                leg(C580_NEXT, false, true),
                leg(C580_LATER, true, true),
            ],
            100,
            1,
        );
        assert_eq!(with_calendars(&twice), Err("max_loss_unknown"));
        // With two of the 19 Oct, both are covered.
        let both = order(
            vec![
                ratio_leg(C580, false, 1),
                ratio_leg(C580_NEXT, false, 1),
                ratio_leg(C580_LATER, true, 2),
            ],
            100,
            1,
        );
        assert_eq!(with_calendars(&both), Ok(100 * 100));
    }

    #[test]
    fn a_covered_calendar_needs_a_debit_and_every_leg_in_it() {
        // A credit or no debit claims no loss, so it is not bounded this way.
        assert_eq!(
            with_calendars(&double_calendar(-50, 1)),
            Err("max_loss_unknown")
        );
        assert_eq!(
            with_calendars(&double_calendar(0, 1)),
            Err("max_loss_unknown")
        );
        // A long that covers no short is not part of a calendar.
        let stray = order(
            vec![
                ratio_leg(C580, false, 1),
                ratio_leg(C580_NEXT, true, 1),
                ratio_leg(P570_NEXT, true, 1),
            ],
            200,
            1,
        );
        assert_eq!(with_calendars(&stray), Err("max_loss_unknown"));
        // A closing leg is never sized.
        let mut closing = double_calendar(240, 1);
        closing.legs[1].open = false;
        assert_eq!(with_calendars(&closing), Err("max_loss_unknown"));
    }

    #[test]
    fn a_european_double_calendar_is_unknown() {
        let o = order(
            vec![
                leg("SPXW261016P05700000", false, true),
                leg("SPXW261023P05700000", true, true),
                leg("SPXW261016C05800000", false, true),
                leg("SPXW261023C05800000", true, true),
            ],
            900,
            1,
        );
        assert_eq!(with_calendars(&o), Err("max_loss_unknown"));
    }

    #[test]
    fn a_short_expiring_after_its_long_is_unknown() {
        // Buy the 580 call and sell the 580 call expiring a week later: the short is not covered
        // by a later long, so no bound is claimed.
        let back = order(
            vec![leg(C580, true, true), leg(C580_NEXT, false, true)],
            150,
            1,
        );
        assert_eq!(max_loss_cents(&back), Err("max_loss_unknown"));
    }

    #[test]
    fn a_roll_is_unknown_because_its_limit_is_not_split() {
        // Close the 580 call and open the 585 call: one net limit prices both legs, and the
        // intent gives no price for each, so the opening leg's loss cannot be sized alone.
        let roll = order(vec![leg(C580, false, false), leg(C585, true, true)], 100, 1);
        assert_eq!(max_loss_cents(&roll), Err("max_loss_unknown"));
        let roll_credit = order(
            vec![leg(C580, false, false), leg(C585, true, true)],
            -100,
            1,
        );
        assert_eq!(max_loss_cents(&roll_credit), Err("max_loss_unknown"));
    }

    #[test]
    fn a_shape_that_is_not_classified_is_unknown() {
        // A calendar: the legs expire on different dates.
        let cal = order(
            vec![leg(C580, true, true), leg(C580_NEXT, false, true)],
            100,
            1,
        );
        assert_eq!(max_loss_cents(&cal), Err("max_loss_unknown"));
        // A strike with a fraction of a cent, and a symbol that is not an option.
        let odd = order(vec![leg(P570_ODD, false, true)], 100, 1);
        assert_eq!(max_loss_cents(&odd), Err("max_loss_unknown"));
        let bad = order(vec![leg("SPY261005X00580000", false, true)], 100, 1);
        assert_eq!(max_loss_cents(&bad), Err("max_loss_unknown"));
    }

    #[test]
    fn a_long_combo_of_options_loses_its_debit() {
        // A long straddle: buy the 580 call and the 570 put for $10.00.
        let o = order(vec![leg(C580, true, true), leg(P570, true, true)], 1000, 1);
        assert_eq!(max_loss_cents(&o), Ok(1000 * 100));
    }

    #[test]
    fn a_partial_close_leaves_the_remaining_open_quantity() {
        // Long 10 calls at $1.25, 4 closed: the remaining 6 are what can still be lost.
        let full = order(vec![leg(C580, true, true)], 125, 10);
        let rest = order(vec![leg(C580, true, true)], 125, 6);
        assert_eq!(max_loss_cents(&full), Ok(125 * 10 * 100));
        assert_eq!(max_loss_cents(&rest), Ok(125 * 6 * 100));
        assert_eq!(max_loss(&rest).unwrap().premium_cents, Some(125 * 6 * 100));
    }

    #[test]
    fn a_full_close_leaves_no_loss() {
        // Nothing remains open, so the loss and the premium are zero: a long option paid
        // nothing more, and a credit spread has no loss left.
        let long = order(vec![leg(C580, true, true)], 125, 0);
        assert_eq!(
            max_loss(&long),
            Ok(MaxLoss {
                cents: 0,
                premium_cents: Some(0)
            })
        );
        let spread = order(vec![leg(P570, false, true), leg(P565, true, true)], -100, 0);
        assert_eq!(max_loss_cents(&spread), Ok(0));
    }

    #[test]
    fn a_closing_leg_is_never_netted_against_an_opening_leg() {
        // A close beside an open is one limit for both, so neither leg is sized alone.
        // The close does not offset the open put, and the open does not offset the close.
        let credit = order(
            vec![leg(P570, false, true), leg(P565, true, false)],
            -100,
            1,
        );
        assert_eq!(max_loss_cents(&credit), Err("max_loss_unknown"));
        let debit = order(vec![leg(C580, true, true), leg(C585, false, false)], 150, 1);
        assert_eq!(max_loss_cents(&debit), Err("max_loss_unknown"));
        // A closing leg alone is never an opening intent, so it has no loss to bound.
        let close = order(vec![leg(C580, false, false)], 125, 1);
        assert_eq!(max_loss_cents(&close), Err("max_loss_unknown"));
    }

    #[test]
    fn a_naked_call_beside_a_long_put_is_unknown_not_a_vertical() {
        // A call sold with a long call is a vertical, not a naked call.
        let covered = order(vec![leg(C580, false, true), leg(C585, true, true)], -100, 1);
        assert_eq!(max_loss_cents(&covered), Ok(400 * 100));
        // A naked short call next to a long put is unbounded above the call strike.
        let mixed = order(vec![leg(C580, false, true), leg(P570, true, true)], -100, 1);
        assert_eq!(max_loss_cents(&mixed), Err("max_loss_unbounded"));
    }

    #[test]
    fn futures_symbols() {
        use super::{future, future_multiplier};
        assert_eq!(future("ESZ6"), Some(("ES", 12, 6)));
        assert_eq!(future("MESH27"), Some(("MES", 3, 27)));
        assert_eq!(future("M2KU6"), Some(("M2K", 9, 6)));
        // Not quarterly, a spread, an unknown root, no year.
        for bad in [
            "",
            "6",
            "Z6",
            "ESZ",
            "ESF6",
            "ESZ6-ESH7",
            "XXZ6",
            "ESZ123",
            "Eé6",
            "ÉSZ6",
            "ESé6",
            "esz6",
            "ES Z6",
            "ESZ6\n",
            "MESZ6MESZ6",
            "\u{0}SZ6",
        ] {
            assert_eq!(future(bad), None, "{bad}");
        }
        assert_eq!(future_multiplier("ES"), Some(50));
        assert_eq!(future_multiplier("MNQ"), Some(2));
    }

    #[test]
    fn a_future_is_a_single_and_its_risk_is_its_contracts() {
        let v = |class: &str, symbol: &str, side: &str| {
            serde_json::json!({"client_order_id":"f-1","shape":"single",
                "instrument":{"class":class,"symbol":symbol},"side":side,"effect":"open",
                "qty":2,"type":"limit","limit_price":"7743.25","time_in_force":"day"})
        };
        let o = Intent::parse(v("future", "ESZ6", "sell")).unwrap();
        assert_eq!(o.legs[0].class, Class::Future);
        assert!(o.is_future() && !o.is_equity());
        assert_eq!(o.multiplier(), 50);
        assert_eq!(max_loss_cents(&o), Err("max_loss_future"));
        assert!(
            o.canonical()
                .contains(r#""class":"future","symbol":"ESZ6""#)
        );
        assert!(Intent::parse(v("future", "AAPL", "buy")).is_err());
        assert!(Intent::parse(v("future", "ESZ6-ESH7", "buy")).is_err());
        let combo = serde_json::json!({"client_order_id":"f-2","shape":"combo","legs":[
            {"instrument":{"class":"future","symbol":"ESZ6"},"ratio":1,"side":"buy","effect":"open"},
            {"instrument":{"class":"future","symbol":"ESH7"},"ratio":1,"side":"sell","effect":"open"}],
            "qty":1,"type":"limit","limit_price":"10.00","time_in_force":"day"});
        assert!(Intent::parse(combo).is_err(), "futures combos are refused");
    }

    #[test]
    fn futures_expire_on_the_third_friday() {
        use super::future_expiry;
        use crate::safety::days_from_civil;
        let oct30 = days_from_civil(2026, 10, 30);
        assert_eq!(
            future_expiry("ESZ6", oct30),
            Some(days_from_civil(2026, 12, 18))
        );
        assert_eq!(
            future_expiry("ESH7", oct30),
            Some(days_from_civil(2027, 3, 19))
        );
        // Four years out, and one just expired (named in the new year).
        assert_eq!(
            future_expiry("ESH0", oct30),
            Some(days_from_civil(2030, 3, 15))
        );
        let jan2 = days_from_civil(2027, 1, 2);
        assert_eq!(
            future_expiry("ESZ6", jan2),
            Some(days_from_civil(2026, 12, 18))
        );
        assert_eq!(
            future_expiry("MESH27", oct30),
            Some(days_from_civil(2027, 3, 19))
        );
    }
}
