//! Alpaca SIP (consolidated) quotes and trades as DBN `CMBP-1`, with trade
//! conditions as `tinytrader.trade-conditions/1` records and quote conditions
//! as `tinytrader.quote-conditions/1` records ([`crate::quote_conditions`]). For history
//! (`alpaca-history`, decision 0014); the conventions are IEX's ([`crate::iex`])
//! where they apply:
//!
//! - A quote is an `M` record with side `N`, `price` undefined and `size` 0.
//!   Its header's publisher is the consolidated feed ([`PUBLISHER`]); the
//!   book is in `levels[0]`, each side naming the exchange quoting it
//!   (`bid_pb`, `ask_pb`).
//! - A trade is a `T` record with side `N`, its own price and size; its
//!   header's publisher is the exchange that executed it, and it carries the
//!   consolidated book as it stood before the trade.
//! - Exchanges are Alpaca's one-letter codes, each a publisher id
//!   `40100 + (letter - 'A')`; `?` (a venue the SIP doesn't name) is
//!   [`UNKNOWN_EXCHANGE`]; any other code is refused, not guessed.
//! - Prices, timestamps, the round-lot rule for quote sizes, `F_LAST` and the
//!   order of a day (a trade before a quote with the same time) are as for IEX.

use dbn::{
    Cmbp1Msg, ConsolidatedBidAskPair, FlagSet, InstrumentDefMsg, RecordHeader, UNDEF_PRICE,
    UNDEF_TIMESTAMP, rtype,
};
use serde::Deserialize;
use serde_json::value::RawValue;
use tinytrader_dbn::dbn;
use tinytrader_dbn::{TradeConditions, TradeUpdate};

use crate::iex::{ABSENT, Side, price, quote_lot, side, timestamp};
use crate::quote_conditions::QuoteConditions;

/// The dataset name.
pub const DATASET: &str = "ALPACA.SIP";
/// Publisher id of the consolidated feed itself (a quote's header). Not a
/// Databento allocation.
pub const PUBLISHER: u16 = 40002;
/// Publisher ids of the exchanges start here: `A` is 40100, `Z` 40125.
pub const EXCHANGE_BASE: u16 = 40100;
/// Publisher id for Alpaca's `?`: a venue the SIP itself doesn't name (seen
/// on some 2021 trades). Kept as "unknown venue", not guessed as an exchange
/// and not a reason to refuse the day.
pub const UNKNOWN_EXCHANGE: u16 = 40099;

/// An exchange's publisher id from Alpaca's one-letter code.
///
/// # Errors
/// Anything but one capital letter or `?` ([`UNKNOWN_EXCHANGE`]).
pub fn exchange(code: &str) -> Result<u16, String> {
    match code.as_bytes() {
        [c @ b'A'..=b'Z'] => Ok(EXCHANGE_BASE + u16::from(c - b'A')),
        [b'?'] => Ok(UNKNOWN_EXCHANGE),
        _ => Err(format!("unknown exchange code {code:?}")),
    }
}

/// A stable instrument id for a symbol: FNV-1a of `ALPACA.SIP/<symbol>`,
/// avoiding the values DBN reserves.
#[must_use]
pub fn instrument_id(symbol: &str) -> u32 {
    let mut h: u32 = 0x811c_9dc5;
    for b in format!("{DATASET}/{symbol}").bytes() {
        h ^= u32::from(b);
        h = h.wrapping_mul(0x0100_0193);
    }
    match h {
        0 => 1,
        u32::MAX => u32::MAX - 1,
        h => h,
    }
}

/// A SIP quote row from Alpaca's history.
#[derive(Debug, Deserialize)]
pub struct RawQuote<'a> {
    #[serde(borrow)]
    pub t: &'a str,
    #[serde(borrow)]
    pub bp: &'a RawValue,
    pub bs: u64,
    #[serde(default)]
    pub bx: String,
    #[serde(borrow)]
    pub ap: &'a RawValue,
    #[serde(rename = "as")]
    pub as_: u64,
    #[serde(default)]
    pub ax: String,
    /// Quote conditions, as sent.
    #[serde(default)]
    pub c: Vec<String>,
    /// Tape (`A`, `B` or `C`).
    #[serde(default)]
    pub z: String,
}

/// A SIP trade row from Alpaca's history.
#[derive(Debug, Deserialize)]
pub struct RawTrade<'a> {
    #[serde(borrow)]
    pub t: &'a str,
    pub x: String,
    #[serde(borrow)]
    pub p: &'a RawValue,
    pub s: u64,
    pub i: u64,
    #[serde(default)]
    pub c: Vec<String>,
    #[serde(default)]
    pub z: String,
    #[serde(default)]
    pub u: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Quote {
    pub ts: u64,
    pub bid: Side,
    pub ask: Side,
    /// Quoting exchanges' publisher ids; 0 for an absent side.
    pub bid_pb: u16,
    pub ask_pb: u16,
    /// Condition codes in source order (the first `count`), one byte each.
    pub codes: [u8; 4],
    pub count: u8,
    /// `b'A'`, `b'B'` or `b'C'`; 0 when Alpaca sent none (or another).
    pub tape: u8,
    /// The conditions or tape Alpaca sent can't be held (more than four codes,
    /// a code that isn't one printable ASCII byte, no tape): the quote is kept,
    /// and its conditions record says so.
    pub unparsed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Trade {
    pub ts: u64,
    pub px: i64,
    pub sz: u32,
    pub id: u64,
    pub exchange: u16,
    pub codes: Vec<String>,
    pub tape: u8,
    pub update: TradeUpdate,
}

/// A side's exchange, or 0 for an absent side.
fn side_exchange(s: Side, code: &str) -> Result<u16, String> {
    if s == ABSENT { Ok(0) } else { exchange(code) }
}

impl RawQuote<'_> {
    /// # Errors
    /// Refuses rows the mapping can't represent.
    pub fn parse(&self) -> Result<Quote, String> {
        let ts = timestamp(self.t)?;
        let lot = quote_lot(ts);
        let (bid, ask) = (side(self.bp, self.bs, lot)?, side(self.ap, self.as_, lot)?);
        // Conditions that don't fit never refuse the quote: they're marked.
        let tape = self
            .z
            .bytes()
            .next()
            .filter(|t| (b'A'..=b'C').contains(t) && self.z.len() == 1);
        let ((codes, count), unparsed) = match (TradeConditions::codes_from(&self.c), tape) {
            (Ok(c), Some(_)) => (c, false),
            _ => (([0; 4], 0), true),
        };
        Ok(Quote {
            ts,
            bid,
            ask,
            bid_pb: side_exchange(bid, &self.bx)?,
            ask_pb: side_exchange(ask, &self.ax)?,
            codes,
            count,
            tape: tape.unwrap_or(0),
            unparsed,
        })
    }
}

impl RawTrade<'_> {
    /// # Errors
    /// Refuses rows the mapping can't represent.
    pub fn parse(&self) -> Result<Trade, String> {
        let update = match self.u.as_deref() {
            None => TradeUpdate::None,
            Some("canceled") => TradeUpdate::Canceled,
            Some("incorrect") => TradeUpdate::Incorrect,
            Some("corrected") => TradeUpdate::Corrected,
            Some(other) => return Err(format!("unknown trade update {other:?}")),
        };
        Ok(Trade {
            ts: timestamp(self.t)?,
            px: price(self.p.get())?,
            sz: u32::try_from(self.s).map_err(|_| format!("size {} doesn't fit", self.s))?,
            id: self.i,
            exchange: exchange(&self.x)?,
            codes: self.c.clone(),
            tape: self.z.bytes().next().unwrap_or(0),
            update,
        })
    }
}

/// A row of one day, in tape order.
pub enum Row {
    Quote(Quote),
    Trade(Trade),
}

impl Row {
    /// The row's event time.
    #[must_use]
    pub fn ts(&self) -> u64 {
        match self {
            Self::Quote(q) => q.ts,
            Self::Trade(t) => t.ts,
        }
    }
}

/// Merge a day's trades and quotes (each already in time order) into tape
/// order: by time, a trade before a quote with the same time.
///
/// # Errors
/// Refuses a lane that isn't in time order.
pub fn merge(trades: Vec<Trade>, quotes: Vec<Quote>) -> Result<Vec<Row>, String> {
    for (name, ordered) in [
        ("trades", trades.windows(2).all(|w| w[0].ts <= w[1].ts)),
        ("quotes", quotes.windows(2).all(|w| w[0].ts <= w[1].ts)),
    ] {
        if !ordered {
            return Err(format!("{name} are not in time order"));
        }
    }
    let mut out = Vec::with_capacity(trades.len() + quotes.len());
    let (mut t, mut q) = (trades.into_iter().peekable(), quotes.into_iter().peekable());
    loop {
        let take_trade = match (t.peek(), q.peek()) {
            (Some(a), Some(b)) => a.ts <= b.ts,
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => break,
        };
        out.push(if take_trade {
            Row::Trade(t.next().expect("peeked"))
        } else {
            Row::Quote(q.next().expect("peeked"))
        });
    }
    Ok(out)
}

fn level(q: &Quote) -> ConsolidatedBidAskPair {
    ConsolidatedBidAskPair {
        bid_px: q.bid.px,
        ask_px: q.ask.px,
        bid_sz: q.bid.sz,
        ask_sz: q.ask.sz,
        bid_pb: q.bid_pb,
        ask_pb: q.ask_pb,
        ..Default::default()
    }
}

const EMPTY: Quote = Quote {
    ts: 0,
    bid: ABSENT,
    ask: ABSENT,
    bid_pb: 0,
    ask_pb: 0,
    codes: [0; 4],
    count: 0,
    tape: 0,
    unparsed: false,
};

/// Turns one symbol-day's rows, in tape order, into records one at a time:
/// each with its conditions record (the nth trade conditions for the nth
/// trade, the nth quote conditions for the nth quote). `F_LAST` depends on
/// the next row; the caller sets it ([`encode_day`] does).
pub struct Encoder {
    instrument: u32,
    book: Quote,
    trades: u32,
    quotes: u32,
}

/// A row's conditions record.
pub enum Conditions {
    Trade(TradeConditions),
    Quote(QuoteConditions),
}

impl Encoder {
    #[must_use]
    pub fn new(symbol: &str) -> Self {
        Self {
            instrument: instrument_id(symbol),
            book: EMPTY,
            trades: 0,
            quotes: 0,
        }
    }

    /// The next row's record (`F_LAST` unset) and its conditions.
    ///
    /// # Errors
    /// Refuses conditions the layouts can't hold.
    pub fn push(&mut self, row: &Row) -> Result<(Cmbp1Msg, Conditions), String> {
        let (publisher, ts, action, px, sz, lv, cond) = match row {
            Row::Quote(q) => {
                self.book = *q;
                let c = QuoteConditions {
                    publisher_id: PUBLISHER,
                    instrument_id: self.instrument,
                    ts_event: q.ts,
                    codes: q.codes,
                    count: q.count,
                    tape: q.tape,
                    quote_seq: self.quotes,
                    unparsed: q.unparsed,
                };
                c.encode().map_err(|e| format!("quote at {}: {e}", q.ts))?;
                self.quotes = self.quotes.wrapping_add(1);
                (
                    PUBLISHER,
                    q.ts,
                    b'M',
                    UNDEF_PRICE,
                    0,
                    level(q),
                    Conditions::Quote(c),
                )
            }
            // Ties go to trades, so the book here is the one before the trade.
            Row::Trade(t) => {
                let (codes, count) = TradeConditions::codes_from(&t.codes)
                    .map_err(|e| format!("trade {} at {}: {e:?}", t.id, t.ts))?;
                let c = TradeConditions {
                    publisher_id: t.exchange,
                    instrument_id: self.instrument,
                    ts_event: t.ts,
                    price: t.px,
                    trade_id: t.id,
                    size: t.sz,
                    codes,
                    count,
                    tape: t.tape,
                    update: t.update,
                    trade_seq: self.trades,
                };
                c.encode()
                    .map_err(|e| format!("trade {} at {}: {e:?}", t.id, t.ts))?;
                self.trades = self.trades.wrapping_add(1);
                (
                    t.exchange,
                    t.ts,
                    b'T',
                    t.px,
                    t.sz,
                    level(&self.book),
                    Conditions::Trade(c),
                )
            }
        };
        let mut rec = Cmbp1Msg::default_for_schema(dbn::Schema::Cmbp1);
        rec.hd = RecordHeader::new::<Cmbp1Msg>(rtype::CMBP_1, publisher, self.instrument, ts);
        rec.price = px;
        rec.size = sz;
        rec.action = action as std::ffi::c_char;
        rec.side = b'N' as std::ffi::c_char;
        rec.flags = FlagSet::empty();
        rec.ts_recv = UNDEF_TIMESTAMP;
        rec.ts_in_delta = 0;
        rec.levels = [lv];
        Ok((rec, cond))
    }
}

/// A day encoded whole: its records, trade conditions and quote conditions.
pub type Day = (Vec<Cmbp1Msg>, Vec<TradeConditions>, Vec<QuoteConditions>);

/// Encode a day from history: its CMBP-1 records, their trade conditions
/// (the nth for the nth trade) and quote conditions (the nth for the nth
/// quote). Rows from history have no receive time.
///
/// # Errors
/// Refuses conditions the layouts can't hold.
pub fn encode_day(symbol: &str, rows: &[Row]) -> Result<Day, String> {
    let mut enc = Encoder::new(symbol);
    let mut records = Vec::with_capacity(rows.len());
    let (mut trades, mut quotes) = (Vec::new(), Vec::new());
    for (i, row) in rows.iter().enumerate() {
        let (mut rec, cond) = enc.push(row)?;
        if rows.get(i + 1).is_none_or(|next| next.ts() != row.ts()) {
            rec.flags = FlagSet::empty().set_last();
        }
        match cond {
            Conditions::Trade(c) => trades.push(c),
            Conditions::Quote(c) => quotes.push(c),
        }
        records.push(rec);
    }
    Ok((records, trades, quotes))
}

/// The day's instrument definition: a US equity in USD, one-cent tick.
#[must_use]
pub fn definition(symbol: &str) -> InstrumentDefMsg {
    let mut d = InstrumentDefMsg {
        hd: RecordHeader::new::<InstrumentDefMsg>(
            rtype::INSTRUMENT_DEF,
            PUBLISHER,
            instrument_id(symbol),
            UNDEF_TIMESTAMP,
        ),
        min_price_increment: 10_000_000,
        raw_symbol: dbn::record::str_to_c_chars(symbol).unwrap_or([0; 71]),
        security_update_action: b'A' as std::ffi::c_char,
        user_defined_instrument: b'N' as std::ffi::c_char,
        ..Default::default()
    };
    d.asset = dbn::record::str_to_c_chars(symbol).unwrap_or_default();
    d.currency = dbn::record::str_to_c_chars("USD").unwrap_or_default();
    d.security_type = dbn::record::str_to_c_chars("EQUITY").unwrap_or_default();
    d
}

#[cfg(test)]
mod tests {
    use super::*;

    fn q(ts: u64, bid: i64, bx: u16) -> Quote {
        Quote {
            ts,
            bid: Side { px: bid, sz: 100 },
            ask: ABSENT,
            bid_pb: bx,
            ask_pb: 0,
            codes: [b'R', 0, 0, 0],
            count: 1,
            tape: b'B',
            unparsed: false,
        }
    }
    fn t(ts: u64, x: u16) -> Trade {
        Trade {
            ts,
            px: 5,
            sz: 10,
            id: ts,
            exchange: x,
            codes: vec!["@".into()],
            tape: b'B',
            update: TradeUpdate::None,
        }
    }

    #[test]
    fn exchanges_map_to_fixed_publishers() {
        assert_eq!(exchange("A"), Ok(40100));
        assert_eq!(exchange("V"), Ok(40121));
        assert_eq!(exchange("?"), Ok(UNKNOWN_EXCHANGE));
        assert!(exchange("").is_err() && exchange("NY").is_err() && exchange("a").is_err());
    }

    #[test]
    fn parses_rows_with_their_exchanges() {
        let row: RawQuote = serde_json::from_str(
            r#"{"t":"2026-10-01T13:30:00.5Z","bp":774.5,"bs":2,"bx":"Q","ap":0,"as":0,"ax":" ","c":["R"],"z":"B"}"#,
        )
        .unwrap();
        let p = row.parse().unwrap();
        assert_eq!((p.bid.sz, p.bid_pb, p.ask, p.ask_pb), (2, 40116, ABSENT, 0));
        let tr: RawTrade = serde_json::from_str(
            r#"{"t":"2026-10-01T13:30:00.5Z","x":"V","p":774.51,"s":100,"i":7,"c":["@","I"],"z":"B"}"#,
        )
        .unwrap();
        assert_eq!(tr.parse().unwrap().exchange, 40121);
    }

    #[test]
    fn trades_carry_the_prior_book_and_their_exchange() {
        let rows = merge(vec![t(2, 40121)], vec![q(1, 7, 40116), q(2, 9, 40116)]).unwrap();
        let (recs, conds, qconds) = encode_day("SPY", &rows).unwrap();
        assert_eq!(recs.len(), 3);
        let trade = &recs[1];
        assert_eq!(trade.action as u8, b'T');
        assert_eq!(trade.hd.publisher_id, 40121);
        // The book before the trade: the quote at time 1, not the one at 2.
        assert_eq!(trade.levels[0].bid_px, 7);
        assert_eq!(trade.levels[0].bid_pb, 40116);
        assert_eq!(recs[2].hd.publisher_id, PUBLISHER);
        // F_LAST only where the next record's time differs.
        assert!(recs[0].flags.is_last() && !recs[1].flags.is_last() && recs[2].flags.is_last());
        assert_eq!(
            (conds.len(), conds[0].trade_seq, conds[0].publisher_id),
            (1, 0, 40121)
        );
        // One quote conditions record a quote, in order, with its codes and tape.
        let q: Vec<(u32, u64, &[u8], u8)> = qconds
            .iter()
            .map(|c| (c.quote_seq, c.ts_event, c.codes(), c.tape))
            .collect();
        assert_eq!(q, [(0, 1, &b"R"[..], b'B'), (1, 2, &b"R"[..], b'B')]);
    }

    #[test]
    fn quote_conditions_are_parsed_and_marked_when_they_dont_fit() {
        let row: RawQuote = serde_json::from_str(
            r#"{"t":"2026-10-01T13:30:00.5Z","bp":774.5,"bs":2,"bx":"Q","ap":774.6,"as":1,"ax":"P","c":["R","Y"],"z":"B"}"#,
        )
        .unwrap();
        let p = row.parse().unwrap();
        assert_eq!(
            (&p.codes[..usize::from(p.count)], p.tape),
            (&b"RY"[..], b'B')
        );
        let bad: RawQuote = serde_json::from_str(
            r#"{"t":"2026-10-01T13:30:00.5Z","bp":774.5,"bs":2,"bx":"Q","ap":0,"as":0,"c":["RR"],"z":"B"}"#,
        )
        .unwrap();
        let five: RawQuote = serde_json::from_str(
            r#"{"t":"2026-10-01T13:30:00.5Z","bp":774.5,"bs":2,"bx":"Q","ap":0,"as":0,"c":["A","B","C","D","E"],"z":"B"}"#,
        )
        .unwrap();
        let multi: RawQuote = serde_json::from_str(
            r#"{"t":"2026-10-01T13:30:00.5Z","bp":774.5,"bs":2,"bx":"Q","ap":0,"as":0,"c":["\u00e9"],"z":"B"}"#,
        )
        .unwrap();
        let none: RawQuote = serde_json::from_str(
            r#"{"t":"2026-10-01T13:30:00.5Z","bp":774.5,"bs":2,"bx":"Q","ap":0,"as":0}"#,
        )
        .unwrap();
        // Each still a quote, its conditions record marked unparsed, never refused.
        for (raw, tape) in [(bad, b'B'), (five, b'B'), (multi, b'B'), (none, 0)] {
            let q = raw.parse().unwrap();
            assert!(q.unparsed && q.count == 0 && q.tape == tape, "{q:?}");
            let (_, c) = Encoder::new("SPY").push(&Row::Quote(q)).unwrap();
            let Conditions::Quote(c) = c else {
                panic!("a quote")
            };
            let back = QuoteConditions::decode(&c.encode().unwrap()).unwrap();
            assert!(back.unparsed && back.codes().is_empty());
        }
    }
}
