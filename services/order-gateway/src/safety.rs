//! Mechanical safety: caps profile, request budget, civil/ET time. None of this
//! is portfolio or strategy state.
use crate::intent::fmt_cents;
use crate::protocol::Obj;

const NS: i64 = 1_000_000_000;

/// Operator profile: allowlist and per-account caps (section 8).
#[derive(Clone, Debug)]
pub struct Profile {
    /// Allowed option roots; none by default, so a deployment names its own
    /// (`--underlyings`).
    pub underlyings: Vec<String>,
    /// Clients whose option orders may be on any underlying (a manual
    /// trading client); every other check and cap still applies to them.
    pub any_underlying_clients: Vec<String>,
    /// Allowed stocks (single orders, `day` or `cls`); none by default.
    pub equities: Vec<String>,
    /// Whether a stock may be sold to open (a short sale).
    pub allow_short: bool,
    /// Maximum combo legs.
    pub max_legs: usize,
    /// Working orders, including unknown outcomes.
    pub max_open: usize,
    /// |limit| × qty × the multiplier (100 an option contract, 1 a share), in cents.
    pub max_notional_cents: i64,
    /// Admitted opening submits in any 60 s. Closes are not counted.
    pub submits_per_min: usize,
    /// Admitted opening submits per UTC day. Closes are not counted.
    pub submits_per_day: usize,
    /// Admitted close submits in any 60 s, counted apart from the opens, so a
    /// buggy loop cannot flood the broker with closes.
    pub closes_per_min: usize,
    /// Consecutive broker rejects that trip `halt_opens`.
    pub max_rejects: u32,
    /// Requests per minute for this account process.
    pub budget_per_min: u32,
    /// Minimum `options_trading_level` for the start gate.
    pub min_options_level: u32,
}

impl Default for Profile {
    fn default() -> Self {
        Self {
            underlyings: Vec::new(),
            any_underlying_clients: Vec::new(),
            equities: Vec::new(),
            allow_short: false,
            max_legs: 4,
            max_open: 4,
            max_notional_cents: 250_000,
            submits_per_min: 60,
            submits_per_day: 500,
            closes_per_min: 30,
            max_rejects: 5,
            budget_per_min: 150,
            min_options_level: 3,
        }
    }
}

impl Profile {
    pub(crate) fn json(&self) -> String {
        let u = crate::protocol::arr(self.underlyings.iter().map(|s| crate::protocol::quote(s)));
        let e = crate::protocol::arr(self.equities.iter().map(|s| crate::protocol::quote(s)));
        let mut o = Obj::new().raw("underlyings", &u);
        if !self.any_underlying_clients.is_empty() {
            let a = crate::protocol::arr(
                self.any_underlying_clients
                    .iter()
                    .map(|s| crate::protocol::quote(s)),
            );
            o = o.raw("any_underlying_clients", &a);
        }
        o.raw("equities", &e)
            .s("allow_short", if self.allow_short { "yes" } else { "no" })
            .n("max_legs", self.max_legs)
            .n("max_open", self.max_open)
            .s("max_notional", &fmt_cents(self.max_notional_cents))
            .n("submits_per_min", self.submits_per_min)
            .n("submits_per_day", self.submits_per_day)
            .n("max_rejects", self.max_rejects)
            .n("budget_per_min", self.budget_per_min)
            .n("min_options_level", self.min_options_level)
            .done()
    }
}

/// One token bucket per account with critical and poll priorities; polls keep a 20%
/// reserve for critical requests and are shed first. Its times are ns on the
/// gateway's rate clock (`at`): monotonic live, the market time under a session.
pub(crate) struct Budget {
    per_min: f64,
    tokens: f64,
    last: u64,
    now: u64,
    pub(crate) blocked_until: Option<u64>,
}

impl Budget {
    pub(crate) fn new(per_min: u32) -> Self {
        Self {
            per_min: f64::from(per_min),
            tokens: f64::from(per_min),
            last: 0,
            now: 0,
            blocked_until: None,
        }
    }
    /// The rate clock's time, ns: every use goes through it.
    pub(crate) fn at(&mut self, now: u64) -> &mut Self {
        if self.last == 0 {
            self.last = now;
        }
        self.now = now.max(self.now);
        self
    }
    #[allow(clippy::cast_precision_loss)]
    fn refill(&mut self) {
        let gained = self.now.saturating_sub(self.last) as f64 / 1e9 * self.per_min / 60.0;
        self.tokens = (self.tokens + gained).min(self.per_min);
        self.last = self.now;
    }
    pub(crate) fn blocked(&mut self) -> bool {
        let now = self.now;
        self.blocked_until = self.blocked_until.filter(|t| *t > now);
        self.blocked_until.is_some()
    }
    /// Takes one token; `false` means throttled before any send.
    pub(crate) fn take(&mut self, critical: bool) -> bool {
        self.refill();
        let floor = if critical {
            1.0
        } else {
            1.0 + self.per_min * 0.2
        };
        if self.blocked() || self.tokens < floor {
            return false;
        }
        self.tokens -= 1.0;
        true
    }
    /// Takes one critical token for a close. A 429 backoff holds opens, not
    /// closes: this ignores `blocked`, but still needs a token above zero, so the
    /// bucket bounds closes as it bounds everything else.
    pub(crate) fn take_close(&mut self) -> bool {
        self.refill();
        if self.tokens < 1.0 {
            return false;
        }
        self.tokens -= 1.0;
        true
    }
    /// Charges reconciliation, which is never shed (the start gate depends on it).
    pub(crate) fn charge(&mut self) {
        self.refill();
        self.tokens = (self.tokens - 1.0).max(0.0);
    }
    /// After a 429: `blocked_until = max(Retry-After, reset, backoff)`.
    pub(crate) fn block(&mut self, ms: Option<u64>) {
        let until = self
            .now
            .saturating_add(ms.unwrap_or(0).max(1000).saturating_mul(1_000_000));
        self.blocked_until = Some(self.blocked_until.map_or(until, |b| b.max(until)));
    }
}

/// Days since 1970-01-01 of a proleptic Gregorian date.
pub(crate) fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (i64::from(m) + 9) % 12;
    let doy = (153 * mp + 2) / 5 + i64::from(d) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

pub(crate) fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = u32::try_from(doy - (153 * mp + 2) / 5 + 1).unwrap_or(1);
    let m = u32::try_from(if mp < 10 { mp + 3 } else { mp - 9 }).unwrap_or(1);
    (
        if m <= 2 {
            yoe + era * 400 + 1
        } else {
            yoe + era * 400
        },
        m,
        d,
    )
}

pub(crate) fn date_string(days: i64) -> String {
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02}")
}

#[expect(clippy::many_single_char_names, reason = "civil date arithmetic")]
pub(crate) fn parse_date(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    if b.len() != 10 || b[4] != b'-' || b[7] != b'-' {
        return None;
    }
    let (y, m, d) = (
        s[..4].parse().ok()?,
        s[5..7].parse().ok()?,
        s[8..].parse().ok()?,
    );
    let days = days_from_civil(y, m, d);
    (civil_from_days(days) == (y, m, d)).then_some(days)
}

/// Minutes after midnight of an `HH:MM` string.
pub(crate) fn hhmm(s: &str) -> Option<i64> {
    let (h, m) = s.split_once(':')?;
    let (h, m): (i64, i64) = (h.parse().ok()?, m.parse().ok()?);
    (s.len() == 5 && h < 24 && m < 60).then_some(h * 60 + m)
}

/// `America/New_York` UTC offset in seconds at a UTC instant (US rules since 2007).
fn et_offset(utc_secs: i64) -> i64 {
    let (y, _, _) = civil_from_days(utc_secs.div_euclid(86_400));
    let sunday_on_or_after = |d: i64| d + (7 - (d + 4).rem_euclid(7)) % 7;
    let start = (sunday_on_or_after(days_from_civil(y, 3, 1)) + 7) * 86_400 + 7 * 3600;
    let end = sunday_on_or_after(days_from_civil(y, 11, 1)) * 86_400 + 6 * 3600;
    if (start..end).contains(&utc_secs) {
        -4 * 3600
    } else {
        -5 * 3600
    }
}

fn ns_secs(ts: u64) -> i64 {
    i64::try_from(ts / 1_000_000_000).unwrap_or(i64::MAX)
}

/// The `America/New_York` calendar day (days since the epoch) at `ts` (ns).
pub(crate) fn et_day(ts: u64) -> i64 {
    let s = ns_secs(ts);
    (s + et_offset(s)).div_euclid(86_400)
}

/// UTC nanoseconds of `minutes` after local midnight on ET day `day`.
pub(crate) fn et_ns(day: i64, minutes: i64) -> u64 {
    let local = day * 86_400 + minutes * 60;
    u64::try_from((local - et_offset(local + 5 * 3600)) * NS).unwrap_or(0)
}

#[cfg(test)]
mod budget_tests {
    use super::Budget;

    const MIN: u64 = 60_000_000_000;

    /// A huge Retry-After blocks (saturating), never wraps around to unblocked.
    #[test]
    fn a_huge_retry_after_blocks() {
        let mut b = Budget::new(60);
        b.at(5 * MIN).block(Some(u64::MAX / 2));
        assert!(b.at(6 * MIN).blocked());
        assert!(!b.at(6 * MIN).take(true));
    }

    /// The bucket refills on its clock's time: emptied, it has nothing at the same
    /// time and refills a minute later.
    #[test]
    fn the_bucket_refills_on_its_clock() {
        let mut b = Budget::new(3);
        for _ in 0..3 {
            assert!(b.at(MIN).take_close());
        }
        assert!(!b.at(MIN).take_close());
        assert!(b.at(2 * MIN).take_close());
    }

    /// A 429's block lasts its time on the clock.
    #[test]
    fn a_block_lasts_its_time_on_the_clock() {
        let mut b = Budget::new(60);
        b.at(MIN).block(Some(30_000));
        assert!(b.at(MIN + MIN / 4).blocked());
        assert!(!b.at(2 * MIN).blocked());
    }
}
