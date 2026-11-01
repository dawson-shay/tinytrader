//! Futures in a simulator fed in process and clocked by a session: fills that move
//! cash by what they realize, margin, caps, Globex hours, and the quarterly
//! settlement. Synthetic quotes, made here.
mod support;
use dbn::{BboMsg, RecordHeader, RecordRef, SType, Schema, SymbolMappingMsg, rtype};
use order_gateway::{Clock, Feed, Gateway, SimBroker, start};
use serde_json::Value;
use std::time::Duration;
use support::*;

const S: u64 = 1_000_000_000;
const H: u64 = 3600 * S;
const WAIT: Duration = Duration::from_secs(5);
const ES: &str = "ESZ6";

/// A price in hundredths of a point, as nanodollars.
fn px(hundredths: i64) -> i64 {
    hundredths * 10_000_000
}

/// ESZ6 as instrument 20, as a Globex file's mapping names it.
fn mapping(feed: &Feed) {
    let m = SymbolMappingMsg::new(
        20,
        T0 - S,
        SType::Parent,
        "ES.FUT",
        SType::RawSymbol,
        ES,
        0,
        0,
    )
    .unwrap();
    feed.push(T0 - S, RecordRef::from(&m)).unwrap();
}

/// A one-minute top of book for ESZ6, prices in hundredths of a point.
fn bbo(feed: &Feed, ts: u64, bid: i64, ask: i64) {
    let mut q = BboMsg {
        hd: RecordHeader::new::<BboMsg>(rtype::BBO_1M, 1, 20, ts),
        ts_recv: ts,
        ..BboMsg::default_for_schema(Schema::Bbo1M)
    };
    q.levels[0].bid_px = px(bid);
    q.levels[0].ask_px = px(ask);
    q.levels[0].bid_sz = 50;
    q.levels[0].ask_sz = 50;
    feed.push(ts, RecordRef::from(&q)).unwrap();
}

fn gateway(name: &str, script: &str) -> (Gateway, Feed, Clock) {
    let (sim, feed) = SimBroker::fed(script, r#"{"source":"session","run":"test"}"#).unwrap();
    let mut cfg = config(root(name));
    cfg.profile.futures = vec!["ES".into()];
    cfg.profile.max_future_qty = 5;
    cfg.profile.max_open = 10;
    let gw = start(cfg, sim).unwrap();
    let clock = gw.clock();
    (gw, feed, clock)
}

fn es(cid: &str, side: &str, effect: &str, qty: u32, price: &str) -> String {
    format!(
        r#"{{"client_order_id":"{cid}","shape":"single","instrument":{{"class":"future","symbol":"{ES}"}},"side":"{side}","effect":"{effect}","qty":{qty},"type":"limit","limit_price":"{price}","time_in_force":"day"}}"#
    )
}

fn fill(c: &mut Conn, cid: &str) -> Value {
    c.until(|v| v["client_order_id"] == cid && v["event"] == "fill")
        .pop()
        .unwrap()
}

fn account(c: &mut Conn, rid: &str) -> Value {
    c.req("account", rid, "").0["account"].clone()
}

/// A fill pays no notional: cash moves by the fee, then by what closing realizes.
#[test]
fn cash_moves_by_what_a_future_realizes() {
    let (gw, feed, clock) = gateway(
        "fut-cash",
        r#"{"account":{"cash":"100000.00"},"fee_per_future":"1.25","expiry":{"settle":false}}"#,
    );
    let mut c = Conn::open(&gw, "reactor", None);
    mapping(&feed);
    bbo(&feed, T0, 774_325, 774_350);
    clock.advance("reactor", 0, T0, WAIT).unwrap();
    c.submit("r1", &es("wse-long", "buy", "open", 1, "7744.00"));
    clock.advance("reactor", 1, T0 + S, WAIT).unwrap();
    assert_eq!(fill(&mut c, "wse-long")["fill"]["price"], "7743.50");
    assert_eq!(account(&mut c, "r2")["cash"], "99998.75");
    bbo(&feed, T0 + 60 * S, 775_000, 775_025);
    clock.advance("reactor", 0, T0 + 60 * S, WAIT).unwrap();
    c.submit("r3", &es("wse-flat", "sell", "close", 1, "7749.00"));
    clock.advance("reactor", 1, T0 + 61 * S, WAIT).unwrap();
    assert_eq!(fill(&mut c, "wse-flat")["fill"]["price"], "7750.00");
    // (7750.00 − 7743.50) × 50 = 325.00, less two fees.
    assert_eq!(account(&mut c, "r4")["cash"], "100322.50");
    gw.stop().unwrap();
}

/// Initial margin holds cash: a second contract the cash can't margin is refused,
/// and buying power is net of the margin held.
#[test]
fn initial_margin_is_held_and_enforced() {
    let (gw, feed, clock) = gateway(
        "fut-margin",
        r#"{"account":{"cash":"30000.00"},"futures_margin":{"ES":"20000.00"},"expiry":{"settle":false}}"#,
    );
    let mut c = Conn::open(&gw, "reactor", None);
    mapping(&feed);
    bbo(&feed, T0, 774_325, 774_350);
    clock.advance("reactor", 0, T0, WAIT).unwrap();
    c.submit("r1", &es("wse-m1", "sell", "open", 1, "7743.00"));
    clock.advance("reactor", 1, T0 + S, WAIT).unwrap();
    fill(&mut c, "wse-m1");
    assert_eq!(account(&mut c, "r2")["buying_power"], "10000.00");
    let r = c.submit("r3", &es("wse-m2", "sell", "open", 1, "7743.00"));
    assert_eq!(r["code"], "sim_insufficient_margin", "{r}");
    // Closing needs no margin.
    let r = c.submit("r4", &es("wse-m3", "buy", "close", 1, "7744.00"));
    assert_eq!(r["outcome"], "accepted", "{r}");
    gw.stop().unwrap();
}

/// The gross position cap counts held and working contracts; a limit far from the
/// mid is refused.
#[test]
fn position_cap_and_price_band() {
    let (gw, feed, clock) = gateway(
        "fut-caps",
        r#"{"futures_max_position":2,"expiry":{"settle":false}}"#,
    );
    let mut c = Conn::open(&gw, "reactor", None);
    mapping(&feed);
    bbo(&feed, T0, 774_325, 774_350);
    clock.advance("reactor", 0, T0, WAIT).unwrap();
    // Two resting (below the bid), then a third would make three.
    assert_eq!(
        c.submit("r1", &es("wse-c1", "buy", "open", 2, "7700.00"))["outcome"],
        "accepted"
    );
    let r = c.submit("r2", &es("wse-c2", "sell", "open", 1, "7800.00"));
    assert_eq!(r["code"], "sim_future_position", "{r}");
    let r = c.submit("r3", &es("wse-c3", "buy", "open", 1, "8200.00"));
    assert_eq!(r["code"], "sim_price_band", "{r}");
    gw.stop().unwrap();
}

/// Globex hours: open after the 16:00 equity close; a day order expires at 17:00;
/// closed in the halt; a book stays across midnight.
#[test]
fn globex_hours() {
    let (gw, feed, clock) = gateway("fut-hours", r#"{"expiry":{"settle":false}}"#);
    let mut c = Conn::open(&gw, "reactor", None);
    mapping(&feed);
    let t1630 = T0 + 6 * H + 30 * 60 * S;
    bbo(&feed, t1630, 774_325, 774_350);
    clock.advance("reactor", 0, t1630, WAIT).unwrap();
    assert_eq!(
        c.submit("r1", &es("wse-h1", "buy", "open", 1, "7700.00"))["outcome"],
        "accepted"
    );
    let a = clock.advance("reactor", 1, T0 + 7 * H + S, WAIT).unwrap();
    let seen = c.until(|v| v["seq"].as_u64() == Some(a.last_seq));
    let expired = seen
        .iter()
        .find(|v| v["client_order_id"] == "wse-h1" && v["event"] == "expired")
        .unwrap_or_else(|| panic!("{seen:?}"));
    assert_eq!(expired["ts_broker"], T0 + 7 * H);
    let r = c.submit("r2", &es("wse-h2", "buy", "open", 1, "7744.00"));
    assert_eq!(r["code"], "sim_market_closed", "{r}");
    // 23:59:40, then past midnight with no new quote: it fills on the book, which
    // the day's roll keeps (the quote is still fresh for the price band).
    let t2359 = T0 + 13 * H + 59 * 60 * S + 40 * S;
    bbo(&feed, t2359, 774_325, 774_350);
    clock.advance("reactor", 1, t2359, WAIT).unwrap();
    clock.advance("reactor", 0, t2359 + 30 * S, WAIT).unwrap();
    c.submit("r3", &es("wse-h3", "buy", "open", 1, "7744.00"));
    clock.advance("reactor", 1, t2359 + 31 * S, WAIT).unwrap();
    assert_eq!(fill(&mut c, "wse-h3")["fill"]["price"], "7743.50");
    gw.stop().unwrap();
}

/// A contract held to its third Friday settles at 09:30 at the index's opening
/// quotation (from the settlement file, through ES → SPX), and is then expired.
#[test]
fn a_held_contract_settles_at_the_opening_quotation() {
    let dir = root("fut-settle-file");
    let file = dir.join("settlements.csv");
    std::fs::write(
        &file,
        "root,date,kind,value\nSPX,2026-12-18,AM-SOQ,7800.00\n",
    )
    .unwrap();
    let script = format!(
        r#"{{"account":{{"cash":"100000.00"}},"expiry":{{"settlement_file":{:?}}}}}"#,
        file.display().to_string()
    );
    let (gw, feed, clock) = gateway("fut-settle", &script);
    let mut c = Conn::open(&gw, "reactor", None);
    mapping(&feed);
    bbo(&feed, T0, 774_325, 774_350);
    clock.advance("reactor", 0, T0, WAIT).unwrap();
    c.submit("r1", &es("wse-s1", "sell", "open", 1, "7743.00"));
    clock.advance("reactor", 1, T0 + S, WAIT).unwrap();
    fill(&mut c, "wse-s1");
    // 2026-12-18, 10:00 New York: 74 days on, and an hour more for the end of
    // daylight saving time.
    let dec18 = T0 + 74 * 24 * H + H;
    clock.advance("reactor", 0, dec18, WAIT).unwrap();
    let s = c
        .until(|v| v["kind"] == "activity" && v["activity_type"] == "SETTLE")
        .pop()
        .unwrap();
    assert_eq!(s["broker_raw"]["price"], "7800.00", "{s}");
    // Short at 7743.25, settled at 7800.00: (7743.25 − 7800.00) × 50.
    assert_eq!(s["broker_raw"]["net_amount"], "-2837.50", "{s}");
    assert_eq!(s["broker_raw"]["transaction_time_ns"], dec18 - 30 * 60 * S);
    assert_eq!(account(&mut c, "r2")["cash"], "97162.50");
    let (p, _) = c.req("positions", "r3", "");
    assert_eq!(p["positions"], serde_json::json!([]), "{p}");
    let r = c.submit("r4", &es("wse-s2", "buy", "open", 1, "7800.00"));
    assert_eq!(r["code"], "sim_expired_instrument", "{r}");
    gw.stop().unwrap();
}
