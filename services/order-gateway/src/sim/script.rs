//! Scripted simulator input: FakeBroker-style per-submit rules, account fields,
//! preloaded broker state, the clock/calendar and the model profile.
use crate::broker::SessionDay;
use crate::intent::parse_cents;
use crate::protocol::{Fields, Invalid, invalid};
use crate::safety::{hhmm, parse_date};
use serde_json::Value;
use std::collections::BTreeMap;

/// Programmed submit outcome.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) enum OnSubmit {
    #[default]
    Accept,
    Fill,
    Partial(u64),
    Reject(u16, String, String),
    ErrorBeforeSend,
    TimeoutAfterAccept,
    Http429(Option<u64>),
    Stall,
}

/// Programmed cancel outcome.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum OnCancel {
    #[default]
    Ok,
    Ignore,
    Reject,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct Rule {
    pub(crate) submit: OnSubmit,
    pub(crate) cancel: OnCancel,
    pub(crate) fill_on_query: bool,
    pub(crate) skew: BTreeMap<String, u64>,
}

pub(crate) enum Match {
    Nth(u64),
    Id(String),
}

pub(crate) struct Preload {
    pub(crate) client_order_id: String,
    pub(crate) symbol: String,
    pub(crate) buy: bool,
    pub(crate) qty: u64,
    pub(crate) limit_cents: i64,
    pub(crate) status: String,
}

/// The parsed script; every field has a default, so `{}` is a valid script.
pub(crate) struct Script {
    pub(crate) clock_ts: Option<u64>,
    /// `None`: no calendar (session checks fail closed).
    pub(crate) calendar: Option<Vec<SessionDay>>,
    pub(crate) weekdays: bool,
    pub(crate) status: String,
    pub(crate) level: u32,
    pub(crate) cash_cents: i64,
    pub(crate) trading_blocked: bool,
    pub(crate) positions: BTreeMap<String, i64>,
    pub(crate) orders: Vec<Preload>,
    pub(crate) quotes: BTreeMap<String, (Option<i64>, Option<i64>)>,
    pub(crate) default: Rule,
    pub(crate) rules: Vec<(Match, Rule)>,
    pub(crate) drop_after: Option<(u64, u64)>,
    pub(crate) fee_cents: i64,
    /// Per share of stock, nanodollars.
    pub(crate) fee_share: i64,
    /// Per futures contract per side, nanodollars.
    pub(crate) fee_future: i64,
    /// Initial margin per contract by futures root, nanodollars.
    pub(crate) futures_margin: BTreeMap<String, i128>,
    /// Most contracts of a futures root held and working at once, gross.
    pub(crate) futures_max_position: u64,
    /// How far a futures limit may be from the mid, in thousandths of it.
    pub(crate) futures_band_permille: u64,
    pub(crate) expiry: super::expiry::Expiry,
    pub(crate) venue: super::rules::VenueRules,
    pub(crate) latency_ms: u64,
    pub(crate) strategy_latency_ms: u64,
    /// Reports reach the client this long after they happen.
    pub(crate) report_latency_ms: u64,
    /// The report latency delays queries' answers too, not only the stream.
    pub(crate) report_latency_all: bool,
    /// Random latency added to each order's activation and each cancel: uniform
    /// over `[min, max]` ms.
    pub(crate) jitter_ms: Option<(u64, u64)>,
    /// The seed of the jitter (a session derives it from its own and the venue's).
    pub(crate) seed: u64,
    pub(crate) size_cap: bool,
    /// The queue model, if on.
    pub(crate) queue: Option<super::queue::QueueModel>,
    /// Fill only on quotes after an order's activation, never the book standing then.
    pub(crate) next_quote: bool,
    /// A combo's sold leg with an ask and no bid is priced at zero.
    pub(crate) no_bid_zero: bool,
}

fn price(v: &Value, path: &str) -> Result<i64, Invalid> {
    v.as_str()
        .and_then(parse_cents)
        .ok_or_else(|| invalid(path, "decimal price"))
}

#[expect(clippy::many_single_char_names, reason = "flat rule grammar")]
fn rule(v: Value, path: &str) -> Result<(Option<Match>, Rule), Invalid> {
    let mut f = Fields::new(v, path)?;
    let m = match (f.opt_int("nth")?, f.opt_string("client_order_id")?) {
        (Some(n), None) => Some(Match::Nth(n)),
        (None, Some(id)) => Some(Match::Id(id)),
        (None, None) => None,
        _ => return Err(invalid(path, "nth or client_order_id, not both")),
    };
    let mut r = Rule::default();
    if let Some(s) = f.take("submit") {
        let p = f.at("submit");
        r.submit = match s {
            Value::String(s) => match s.as_str() {
                "accept" => OnSubmit::Accept,
                "fill" => OnSubmit::Fill,
                "error_before_send" => OnSubmit::ErrorBeforeSend,
                "timeout_after_accept" => OnSubmit::TimeoutAfterAccept,
                "stall" => OnSubmit::Stall,
                _ => return Err(invalid(&p, "unknown submit outcome")),
            },
            obj => {
                let mut o = Fields::new(obj, &p)?;
                let out = if let Some(n) = o.opt_int("partial")? {
                    OnSubmit::Partial(n)
                } else if let Some(rej) = o.take("reject") {
                    let mut j = Fields::new(rej, &o.at("reject"))?;
                    let http = u16::try_from(j.int("http", 400, 599)?).unwrap_or(422);
                    let code = j.int("code", 0, u64::MAX)?.to_string();
                    let out = OnSubmit::Reject(http, code, j.string("message")?);
                    j.finish()?;
                    out
                } else if let Some(h) = o.take("http_429") {
                    let mut j = Fields::new(h, &o.at("http_429"))?;
                    let mut retry = None;
                    if let Some(Value::Object(hdr)) = j.take("headers") {
                        for (k, v) in hdr {
                            let secs = v.as_str().and_then(|s| s.parse::<u64>().ok());
                            if k.eq_ignore_ascii_case("retry-after") {
                                retry = secs.map(|s| s * 1000);
                            }
                        }
                    }
                    j.finish()?;
                    OnSubmit::Http429(retry)
                } else {
                    return Err(invalid(&p, "partial, reject or http_429"));
                };
                o.finish()?;
                out
            }
        };
    }
    if let Some(c) = f.opt_string("cancel")? {
        r.cancel = match c.as_str() {
            "ok" => OnCancel::Ok,
            "ignore" => OnCancel::Ignore,
            "reject" => OnCancel::Reject,
            _ => return Err(invalid(&f.at("cancel"), "ok, ignore or reject")),
        };
    }
    r.fill_on_query = f.opt_bool("fill_on_query")?.unwrap_or(false);
    if let Some(Value::Object(skew)) = f.take("skew") {
        for (k, v) in skew {
            r.skew
                .insert(k, v.as_u64().ok_or_else(|| invalid(path, "skew counts"))?);
        }
    }
    f.finish()?;
    Ok((m, r))
}

impl Script {
    pub(crate) fn parse(text: &str) -> Result<Self, String> {
        let v: Value = serde_json::from_str(text).map_err(|e| format!("sim script: {e}"))?;
        Self::from_value(v).map_err(|e| format!("sim script {}: {}", e.path, e.message))
    }

    #[expect(clippy::many_single_char_names, reason = "flat script grammar")]
    #[expect(clippy::too_many_lines, reason = "flat script grammar")]
    fn from_value(v: Value) -> Result<Self, Invalid> {
        let mut f = Fields::new(v, "")?;
        let mut s = Self {
            clock_ts: f.opt_int("clock_ts")?,
            calendar: Some(Vec::new()),
            weekdays: true,
            status: "ACTIVE".into(),
            level: 3,
            cash_cents: 200_000,
            trading_blocked: false,
            positions: BTreeMap::new(),
            orders: Vec::new(),
            quotes: BTreeMap::new(),
            default: Rule::default(),
            rules: Vec::new(),
            drop_after: None,
            fee_cents: 2,
            fee_share: 0,
            fee_future: 0,
            futures_margin: BTreeMap::new(),
            futures_max_position: 10,
            futures_band_permille: 50,
            venue: super::rules::VenueRules::default(),
            expiry: super::expiry::Expiry {
                settle: true,
                from_quote: true,
                threshold: 10_000_000,
                put_extrinsic: None,
                max_age: 60_000_000_000,
                ..Default::default()
            },
            latency_ms: f.opt_int("latency_ms")?.unwrap_or(5),
            strategy_latency_ms: f.opt_int("strategy_latency_ms")?.unwrap_or(0),
            report_latency_ms: f.opt_int("report_latency_ms")?.unwrap_or(0),
            report_latency_all: match f.opt_string("report_latency_applies_to")?.as_deref() {
                None | Some("stream") => false,
                Some("all") => true,
                Some(_) => {
                    return Err(invalid("report_latency_applies_to", "stream or all"));
                }
            },
            jitter_ms: None,
            seed: f.opt_int("seed")?.unwrap_or(0),
            size_cap: false,
            next_quote: false,
            no_bid_zero: false,
            queue: None,
        };
        if let Some(j) = f.take("latency_jitter_ms") {
            let mut j = Fields::new(j, "latency_jitter_ms")?;
            let (lo, hi) = (j.int("min", 0, 3_600_000)?, j.int("max", 0, 3_600_000)?);
            j.finish()?;
            if hi < lo {
                return Err(invalid("latency_jitter_ms", "min no more than max"));
            }
            s.jitter_ms = Some((lo, hi));
        }
        for (k, v) in [
            ("latency_ms", s.latency_ms),
            ("strategy_latency_ms", s.strategy_latency_ms),
            ("report_latency_ms", s.report_latency_ms),
        ] {
            if v > 3_600_000 {
                return Err(invalid(k, "at most 3600000 (an hour)"));
            }
        }
        match f.map_entry("calendar") {
            None => {}
            Some(Value::Null) => s.calendar = None,
            Some(Value::Array(days)) => {
                s.weekdays = false;
                for d in days {
                    let mut df = Fields::new(d, "calendar")?;
                    let day = SessionDay {
                        date: df.string("date")?,
                        open: df.string("open")?,
                        close: df.string("close")?,
                    };
                    if parse_date(&day.date).is_none()
                        || hhmm(&day.open).is_none()
                        || hhmm(&day.close).is_none()
                    {
                        return Err(invalid("calendar", "date YYYY-MM-DD, open/close HH:MM"));
                    }
                    df.finish()?;
                    s.calendar.get_or_insert_with(Vec::new).push(day);
                }
            }
            Some(_) => return Err(invalid("calendar", "null or a list of days")),
        }
        if let Some(a) = f.take("account") {
            let mut a = Fields::new(a, "account")?;
            s.status = a.opt_string("status")?.unwrap_or(s.status);
            s.level = u32::try_from(a.opt_int("options_trading_level")?.unwrap_or(3)).unwrap_or(0);
            if let Some(c) = a.take("cash") {
                s.cash_cents = price(&c, "account.cash")?;
            }
            s.trading_blocked = a.opt_bool("blocked")?.unwrap_or(false);
            a.finish()?;
        }
        if let Some(Value::Object(p)) = f.take("positions") {
            for (k, v) in p {
                s.positions.insert(
                    k,
                    v.as_i64().ok_or_else(|| invalid("positions", "integers"))?,
                );
            }
        }
        if let Some(Value::Array(orders)) = f.take("orders") {
            for o in orders {
                let mut o = Fields::new(o, "orders")?;
                s.orders.push(Preload {
                    client_order_id: o.string("client_order_id")?,
                    symbol: o.string("symbol")?,
                    buy: o.string("side")? == "buy",
                    qty: o.int("qty", 1, 1_000_000)?,
                    limit_cents: price(&o.req("limit_price")?, "orders.limit_price")?,
                    status: o.opt_string("status")?.unwrap_or_else(|| "new".into()),
                });
                o.finish()?;
            }
        }
        if let Some(Value::Object(q)) = f.take("quotes") {
            for (k, v) in q {
                let side = |n: &str| v.get(n).map(|p| price(p, "quotes")).transpose();
                s.quotes.insert(k.clone(), (side("bid")?, side("ask")?));
            }
        }
        if let Some(d) = f.take("default") {
            s.default = rule(d, "default")?.1;
        }
        if let Some(Value::Array(rules)) = f.take("rules") {
            for (i, r) in rules.into_iter().enumerate() {
                let path = format!("rules[{i}]");
                let (m, r) = rule(r, &path)?;
                s.rules.push((
                    m.ok_or_else(|| invalid(&path, "nth or client_order_id"))?,
                    r,
                ));
            }
        }
        if let Some(st) = f.take("stream") {
            let mut st = Fields::new(st, "stream")?;
            s.drop_after = Some((
                st.int("drop_after_events", 0, u64::MAX)?,
                st.opt_int("down_ms")?.unwrap_or(200),
            ));
            st.finish()?;
        }
        if let Some(fee) = f.take("fee_per_contract") {
            s.fee_cents = price(&fee, "fee_per_contract")?;
        }
        if let Some(v) = f.take("fee_per_share") {
            s.fee_share = v
                .as_str()
                .and_then(super::expiry::nanos)
                .filter(|n| *n >= 0)
                .ok_or_else(|| invalid("fee_per_share", "a decimal dollar amount"))?;
        }
        let dollars = |v: &Value, k: &str| {
            v.as_str()
                .and_then(super::expiry::nanos)
                .filter(|n| *n >= 0)
                .ok_or_else(|| invalid(k, "a decimal dollar amount"))
        };
        if let Some(v) = f.take("fee_per_future") {
            s.fee_future = dollars(&v, "fee_per_future")?;
        }
        if let Some(Value::Object(m)) = f.take("futures_margin") {
            for (root, v) in m {
                if crate::intent::future_multiplier(&root).is_none() {
                    return Err(invalid("futures_margin", "keys are known futures roots"));
                }
                let n = dollars(&v, &format!("futures_margin.{root}"))?;
                s.futures_margin.insert(root, i128::from(n));
            }
        }
        if let Some(n) = f.opt_int("futures_max_position")? {
            s.futures_max_position = n;
        }
        if let Some(v) = f.take("futures_price_band") {
            let n = dollars(&v, "futures_price_band")?;
            if n % 1_000_000 != 0 || n == 0 {
                return Err(invalid(
                    "futures_price_band",
                    "a fraction above 0 of at most three places, such as 0.05",
                ));
            }
            s.futures_band_permille = u64::try_from(n / 1_000_000).unwrap_or(50);
        }
        if let Some(e) = f.take("expiry") {
            parse_expiry(&mut s.expiry, e)?;
        }
        if let Some(v) = f.take("venue_rules") {
            let mut r = Fields::new(v, "venue_rules")?;
            let time = |s: Option<String>, k: &str| {
                s.map(|s| {
                    super::rules::VenueRules::parse_time(&s)
                        .ok_or_else(|| invalid(k, "HH:MM, New York"))
                })
                .transpose()
            };
            s.venue.no_same_day_opening_after = time(
                r.opt_string("no_same_day_opening_after")?,
                "venue_rules.no_same_day_opening_after",
            )?;
            s.venue.liquidate_at = time(
                r.opt_string("liquidate_same_day_shorts_at")?,
                "venue_rules.liquidate_same_day_shorts_at",
            )?;
            s.venue.defined_risk = match r.opt_string("buying_power")?.as_deref() {
                None | Some("cash") => false,
                Some("defined_risk") => true,
                Some(_) => return Err(invalid("venue_rules.buying_power", "cash or defined_risk")),
            };
            r.finish()?;
        }
        if let Some(q) = f.take("queue") {
            let mut q = Fields::new(q, "queue")?;
            let ahead = q.opt_string("ahead")?.unwrap_or_else(|| "1".into());
            let permille = super::expiry::nanos(&ahead)
                .filter(|n| *n >= 0 && n % 1_000_000 == 0)
                .map(|n| u64::try_from(n / 1_000_000).unwrap_or(0))
                .ok_or_else(|| {
                    invalid(
                        "queue.ahead",
                        "a decimal of at most three places, such as 1 or 0.5",
                    )
                })?;
            let through_only = match q.opt_string("fill_on_print")?.as_deref() {
                None | Some("at_or_through") => false,
                Some("through") => true,
                Some(_) => return Err(invalid("queue.fill_on_print", "at_or_through or through")),
            };
            let combo_implied = match q.opt_string("combo")?.as_deref() {
                None | Some("touch") => false,
                Some("implied") => true,
                Some(_) => return Err(invalid("queue.combo", "touch or implied")),
            };
            let window_s = q.opt_int("combo_window_s")?.unwrap_or(60);
            if window_s > 3600 {
                return Err(invalid("queue.combo_window_s", "at most 3600"));
            }
            q.finish()?;
            s.queue = Some(super::queue::QueueModel {
                ahead_permille: permille,
                through_only,
                combo_implied,
                window: window_s * 1_000_000_000,
            });
        }
        s.no_bid_zero = match f.opt_string("no_bid_combo_leg")?.as_deref() {
            None | Some("refuse") => false,
            Some("zero") => true,
            Some(_) => return Err(invalid("no_bid_combo_leg", "refuse or zero")),
        };
        s.next_quote = match f.opt_string("fill_on")?.as_deref() {
            None | Some("book") => false,
            Some("next_quote") => true,
            Some(_) => return Err(invalid("fill_on", "book or next_quote")),
        };
        s.size_cap = match f.opt_string("size_cap")?.as_deref() {
            None | Some("none") => false,
            Some("displayed") => true,
            Some(_) => return Err(invalid("size_cap", "none or displayed")),
        };
        f.finish()?;
        Ok(s)
    }

    /// The rule for the `nth` submit of `cid`: an id rule wins over an nth rule.
    pub(crate) fn rule_for(&self, nth: u64, cid: &str) -> Rule {
        let by_id = self
            .rules
            .iter()
            .find(|(m, _)| matches!(m, Match::Id(i) if i == cid));
        let by_nth = || {
            self.rules
                .iter()
                .find(|(m, _)| matches!(m, Match::Nth(n) if *n == nth))
        };
        by_id
            .or_else(by_nth)
            .map_or_else(|| self.default.clone(), |(_, r)| r.clone())
    }
}

/// A file named in the profile: its text and SHA-256.
fn read_pinned(path: &str, key: &str) -> Result<(String, String), Invalid> {
    use sha2::{Digest, Sha256};
    let bytes = std::fs::read(path).map_err(|e| invalid(key, &format!("{path}: {e}")))?;
    let sha = crate::intent::hex(&Sha256::digest(&bytes));
    let text = String::from_utf8(bytes).map_err(|_| invalid(key, "UTF-8 text"))?;
    Ok((text, sha))
}

/// The profile's `expiry` object.
fn parse_expiry(e: &mut super::expiry::Expiry, v: Value) -> Result<(), Invalid> {
    use super::expiry::{Style, csv, nanos};
    let mut f = Fields::new(v, "expiry")?;
    if let Some(Value::Object(m)) = f.take("styles") {
        for (root, style) in m {
            let st = style
                .as_str()
                .and_then(Style::parse)
                .ok_or_else(|| invalid("expiry.styles", "cash-pm, cash-am or physical"))?;
            e.styles.insert(root, st);
        }
    }
    if let Some(path) = f.opt_string("settlement_file")? {
        let (text, sha) = read_pinned(&path, "expiry.settlement_file")?;
        // Strict: every row has a root, a date, a kind of AM-SOQ or PM-close and a
        // value; a row repeated for its root, date and kind is refused.
        let bad = |why: &str| invalid("expiry.settlement_file", why);
        for (n, row) in csv(&text).into_iter().enumerate() {
            let get = |k: &str| row.get(k).filter(|v| !v.is_empty()).cloned();
            let (Some(root), Some(date), Some(kind), Some(value)) =
                (get("root"), get("date"), get("kind"), get("value"))
            else {
                return Err(bad(&format!("row {}: root, date, kind and value", n + 2)));
            };
            if parse_date(&date).is_none() || !matches!(kind.as_str(), "AM-SOQ" | "PM-close") {
                return Err(bad(&format!(
                    "row {}: a YYYY-MM-DD date and kind AM-SOQ or PM-close",
                    n + 2
                )));
            }
            let value =
                nanos(&value).ok_or_else(|| bad(&format!("row {}: a decimal value", n + 2)))?;
            if e.prices
                .insert((root.clone(), date.clone(), kind.clone()), value)
                .is_some()
            {
                return Err(bad(&format!("{root} {date} {kind} is given twice")));
            }
        }
        e.prices_sha = Some(sha);
    }
    if let Some(b) = f.take("settle") {
        e.settle = b
            .as_bool()
            .ok_or_else(|| invalid("expiry.settle", "true or false"))?;
    }
    if let Some(b) = f.take("settle_from_quote") {
        e.from_quote = b
            .as_bool()
            .ok_or_else(|| invalid("expiry.settle_from_quote", "true or false"))?;
    }
    let dollars = |v: &Value, k: &str| {
        v.as_str()
            .and_then(nanos)
            .filter(|n| *n >= 0)
            .ok_or_else(|| invalid(k, "a decimal dollar amount"))
    };
    if let Some(v) = f.take("exercise_threshold") {
        e.threshold = dollars(&v, "expiry.exercise_threshold")?;
    }
    if let Some(v) = f.take("put_assignment_extrinsic") {
        e.put_extrinsic = Some(dollars(&v, "expiry.put_assignment_extrinsic")?);
    }
    if let Some(v) = f.opt_int("quote_max_age_s")? {
        e.max_age = v.saturating_mul(1_000_000_000);
    }
    if let Some(b) = f.take("early_assignment") {
        e.early = b
            .as_bool()
            .ok_or_else(|| invalid("expiry.early_assignment", "true or false"))?;
    }
    if let Some(Value::Object(m)) = f.take("dividends") {
        for (root, path) in m {
            let path = path
                .as_str()
                .ok_or_else(|| invalid("expiry.dividends", "root: path"))?;
            let (text, sha) = read_pinned(path, "expiry.dividends")?;
            let mut by_date = BTreeMap::new();
            for row in csv(&text) {
                let (Some(d), Some(a)) = (row.get("ex_date"), row.get("dividend_usd")) else {
                    return Err(invalid(
                        "expiry.dividends",
                        "ex_date and dividend_usd columns",
                    ));
                };
                if parse_date(d).is_none() {
                    return Err(invalid("expiry.dividends", "ex_date as YYYY-MM-DD"));
                }
                let amount = nanos(a).ok_or_else(|| invalid("expiry.dividends", "dividend_usd"))?;
                if by_date.insert(d.clone(), amount).is_some() {
                    return Err(invalid(
                        "expiry.dividends",
                        &format!("{root} {d} is given twice"),
                    ));
                }
            }
            e.dividends.insert(root.clone(), by_date);
            e.dividends_sha.insert(root, sha);
        }
    }
    f.finish()
}
