//! `tinytrader.quote-conditions/1`: the conditions of one quote.
//!
//! Standard DBN has no field for quote condition codes. This record carries
//! them beside a quote record (an `M` record of the day's main file), keeping
//! the venue's codes exactly as sent, as `tinytrader.trade-conditions/1` does
//! for trades. The nth record belongs to the nth quote of the main file.
//!
//! | Offset | Type | Field |
//! |---:|---|---|
//! | 0 | u8 | length in 4-byte words: 8 |
//! | 1 | u8 | rtype `0xD5` |
//! | 2 | u16 | publisher id of the joined quote |
//! | 4 | u32 | instrument id of the joined quote |
//! | 8 | u64 | `ts_event` of the joined quote |
//! | 16 | [u8; 4] | condition codes, one printable ASCII byte each, in source order, NUL after `count` |
//! | 20 | u8 | tape: `A`, `B` or `C` (0 allowed only when unparsed) |
//! | 21 | u8 | number of codes, 0 to 4 |
//! | 22 | u8 | layout revision: 1 |
//! | 23 | u8 | flags: bit 0 *unparsed*, the venue's conditions or tape couldn't be held (no codes are kept); other bits 0 |
//! | 24 | u32 | ordinal of the joined quote among the quotes of its stream |
//! | 28 | u32 | reserved, 0 |
//!
//! All integers are little-endian. The rtype isn't allocated by DBN (nor
//! used by Hyphae, whose extensions are `0xD1` to `0xD4`), so a stock DBN
//! reader skips the record by its length.

use tinytrader_dbn::dbn::UNDEF_TIMESTAMP;
use tinytrader_dbn::trade_conditions::Invalid;

/// The record type. Not allocated by DBN.
pub const RTYPE: u8 = 0xD5;
/// Record size in bytes.
pub const LEN: usize = 32;
/// Layout revision, at byte 22.
pub const REVISION: u8 = 1;
/// Most condition codes one record holds.
pub const MAX_CODES: usize = 4;
/// Flag: the quote's conditions as sent couldn't be held.
pub const UNPARSED: u8 = 1;
/// The record's name and version.
pub const NAME: &str = "tinytrader.quote-conditions/1";

/// One quote's conditions, as fields.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QuoteConditions {
    pub publisher_id: u16,
    pub instrument_id: u32,
    pub ts_event: u64,
    /// Condition codes in source order; only the first `count` are used.
    pub codes: [u8; MAX_CODES],
    pub count: u8,
    /// `b'A'`, `b'B'` or `b'C'`.
    pub tape: u8,
    /// Ordinal of the joined quote among its stream's quotes.
    pub quote_seq: u32,
    /// The quote's conditions or tape couldn't be held: `count` is 0.
    pub unparsed: bool,
}

impl QuoteConditions {
    /// The codes in use.
    #[must_use]
    pub fn codes(&self) -> &[u8] {
        &self.codes[..usize::from(self.count).min(MAX_CODES)]
    }

    fn check(&self) -> Result<(), Invalid> {
        let n = usize::from(self.count);
        if n > MAX_CODES || !self.codes[..n].iter().all(|b| (0x20..=0x7E).contains(b)) {
            return Err(Invalid::Codes);
        }
        if self.codes[n..].iter().any(|b| *b != 0) {
            return Err(Invalid::Padding);
        }
        if self.unparsed && n > 0 {
            return Err(Invalid::Codes);
        }
        if !((b'A'..=b'C').contains(&self.tape) || self.unparsed && self.tape == 0) {
            return Err(Invalid::Tape);
        }
        if self.ts_event == UNDEF_TIMESTAMP
            || self.publisher_id == 0
            || self.publisher_id == u16::MAX
            || self.instrument_id == 0
            || self.instrument_id == u32::MAX
        {
            return Err(Invalid::Value);
        }
        Ok(())
    }

    /// The record's bytes.
    ///
    /// # Errors
    /// Refuses fields the layout can't represent.
    pub fn encode(&self) -> Result<[u8; LEN], Invalid> {
        self.check()?;
        let mut b = [0u8; LEN];
        b[0] = (LEN / 4) as u8;
        b[1] = RTYPE;
        b[2..4].copy_from_slice(&self.publisher_id.to_le_bytes());
        b[4..8].copy_from_slice(&self.instrument_id.to_le_bytes());
        b[8..16].copy_from_slice(&self.ts_event.to_le_bytes());
        b[16..20].copy_from_slice(&self.codes);
        b[20] = self.tape;
        b[21] = self.count;
        b[22] = REVISION;
        b[23] = if self.unparsed { UNPARSED } else { 0 };
        b[24..28].copy_from_slice(&self.quote_seq.to_le_bytes());
        Ok(b)
    }

    /// Read a record. Only bytes that re-encode identically are accepted.
    ///
    /// # Errors
    /// Refuses anything that isn't a well-formed revision 1 record.
    pub fn decode(b: &[u8]) -> Result<Self, Invalid> {
        let b: &[u8; LEN] = b.try_into().map_err(|_| Invalid::Framing)?;
        if usize::from(b[0]) * 4 != LEN || b[1] != RTYPE {
            return Err(Invalid::Framing);
        }
        if b[22] != REVISION {
            return Err(Invalid::Revision);
        }
        if b[23] & !UNPARSED != 0 || b[28..32].iter().any(|x| *x != 0) {
            return Err(Invalid::Padding);
        }
        let u32_at = |i: usize| u32::from_le_bytes([b[i], b[i + 1], b[i + 2], b[i + 3]]);
        let mut ts = [0; 8];
        ts.copy_from_slice(&b[8..16]);
        let rec = Self {
            publisher_id: u16::from_le_bytes([b[2], b[3]]),
            instrument_id: u32_at(4),
            ts_event: u64::from_le_bytes(ts),
            codes: [b[16], b[17], b[18], b[19]],
            tape: b[20],
            count: b[21],
            quote_seq: u32_at(24),
            unparsed: b[23] & UNPARSED != 0,
        };
        rec.check()?;
        Ok(rec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tinytrader_dbn::TradeConditions;

    fn sample() -> QuoteConditions {
        let (codes, count) = TradeConditions::codes_from(&["R"]).unwrap();
        QuoteConditions {
            publisher_id: 40002,
            instrument_id: 425_464_287,
            ts_event: 1_791_207_000_000_000_000,
            codes,
            count,
            tape: b'B',
            quote_seq: 9,
            unparsed: false,
        }
    }

    #[test]
    fn round_trips_and_frames_as_dbn() {
        let q = sample();
        let b = q.encode().unwrap();
        assert_eq!((b[0], b[1], b.len()), (8, 0xD5, 32));
        assert_eq!(QuoteConditions::decode(&b), Ok(q));
        assert_eq!(QuoteConditions::decode(&b).unwrap().codes(), b"R");
    }

    #[test]
    fn refuses_what_the_layout_cant_hold() {
        let mut q = sample();
        q.tape = b'Z';
        assert_eq!(q.encode(), Err(Invalid::Tape));
        let mut q = sample();
        q.count = 5;
        assert_eq!(q.encode(), Err(Invalid::Codes));
        let mut b = sample().encode().unwrap();
        b[30] = 1;
        assert_eq!(QuoteConditions::decode(&b), Err(Invalid::Padding));
        let mut b = sample().encode().unwrap();
        b[23] = 2;
        assert_eq!(QuoteConditions::decode(&b), Err(Invalid::Padding));
        // Unparsed: no codes, tape may be absent.
        let u = QuoteConditions {
            codes: [0; 4],
            count: 0,
            tape: 0,
            unparsed: true,
            ..sample()
        };
        assert_eq!(QuoteConditions::decode(&u.encode().unwrap()), Ok(u));
        assert_eq!(
            QuoteConditions {
                tape: 0,
                ..sample()
            }
            .encode(),
            Err(Invalid::Tape)
        );
        let mut b = sample().encode().unwrap();
        b[22] = 2;
        assert_eq!(QuoteConditions::decode(&b), Err(Invalid::Revision));
    }
}
