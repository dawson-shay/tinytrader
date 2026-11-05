//! Alpaca market data as DBN, shared by the relay (live IEX) and
//! `alpaca-history` (SIP and IEX history): IEX as `MBP-1`, SIP as `CMBP-1`,
//! each with trade conditions as `tinytrader.trade-conditions/1` records (and
//! SIP quote conditions as `tinytrader.quote-conditions/1`).

pub mod iex;
pub mod quote_conditions;
pub mod sip;
