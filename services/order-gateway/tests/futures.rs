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
    map(feed, T0 - S, 20, ES);
}

/// `symbol` as instrument `iid`, from `ts`, under its root's parent (`ES.FUT`).
fn map(feed: &Feed, ts: u64, iid: u32, symbol: &str) {
    let root = symbol[..symbol.len() - 2].to_owned();
    let m = SymbolMappingMsg::new(
        iid,
        ts,
        SType::Parent,
        &format!("{root}.FUT"),
        SType::RawSymbol,
        symbol,
        0,
        0,
    )
    .unwrap();
    feed.push(ts, RecordRef::from(&m)).unwrap();
}

/// A one-minute top of book for ESZ6, prices in hundredths of a point.
fn bbo(feed: &Feed, ts: u64, bid: i64, ask: i64) {
    quote(feed, 20, ts, bid, ask);
}

/// A one-minute top of book for instrument `iid`.
fn quote(feed: &Feed, iid: u32, ts: u64, bid: i64, ask: i64) {
    let mut q = BboMsg {
        hd: RecordHeader::new::<BboMsg>(rtype::BBO_1M, 1, iid, ts),
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
    cfg.profile.futures = vec!["ES".into(), "MES".into(), "NQ".into()];
    cfg.profile.max_future_qty = 5;
    cfg.profile.max_open = 10;
    let gw = start(cfg, sim).unwrap();
    let clock = gw.clock();
    (gw, feed, clock)
}

fn es(cid: &str, side: &str, effect: &str, qty: u32, price: &str) -> String {
    order(ES, cid, side, effect, qty, price)
}

fn order(symbol: &str, cid: &str, side: &str, effect: &str, qty: u32, price: &str) -> String {
    format!(
        r#"{{"client_order_id":"{cid}","shape":"single","instrument":{{"class":"future","symbol":"{symbol}"}},"side":"{side}","effect":"{effect}","qty":{qty},"type":"limit","limit_price":"{price}","time_in_force":"day"}}"#
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
        r#"{"account":{"cash":"100000.00"},"fee_per_future":"1.25","futures_margin":{"ES":"1.00"},"expiry":{"settle":false}}"#,
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
    // Cash less the margin, with the short's unrealized −6.25 (filled at the 7743.25
    // bid, marked at the 7743.375 mid).
    assert_eq!(account(&mut c, "r2")["buying_power"], "9993.75");
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
        r#"{"futures_max_position":2,"futures_margin":{"ES":"1.00"},"expiry":{"settle":false}}"#,
    );
    let mut c = Conn::open(&gw, "reactor", None);
    mapping(&feed);
    bbo(&feed, T0, 774_325, 774_350);
    clock.advance("reactor", 0, T0, WAIT).unwrap();
    // Two resting buys (below the bid); a third buy could make a long of three.
    assert_eq!(
        c.submit("r1", &es("wse-c1", "buy", "open", 2, "7700.00"))["outcome"],
        "accepted"
    );
    let r = c.submit("r2", &es("wse-c2", "buy", "open", 1, "7700.00"));
    assert_eq!(r["code"], "sim_future_position", "{r}");
    let r = c.submit("r3", &es("wse-c3", "buy", "open", 1, "8200.00"));
    assert_eq!(r["code"], "sim_price_band", "{r}");
    gw.stop().unwrap();
}

/// Globex hours: open after the 16:00 equity close; a day order expires at 17:00;
/// closed in the halt; a book stays across midnight.
#[test]
fn globex_hours() {
    let (gw, feed, clock) = gateway(
        "fut-hours",
        r#"{"futures_margin":{"ES":"1.00"},"expiry":{"settle":false}}"#,
    );
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
        r#"{{"account":{{"cash":"100000.00"}},"futures_margin":{{"ES":"1.00"}},"expiry":{{"settlement_file":{:?}}}}}"#,
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

/// A partial close realizes on what it closes and keeps the average; a sale past
/// flat realizes the whole long and starts a short at its own price; a close is
/// allowed with the position at its cap.
#[test]
fn partial_close_flip_and_a_close_at_the_cap() {
    let (gw, feed, clock) = gateway(
        "fut-flip",
        r#"{"account":{"cash":"100000.00"},"futures_max_position":2,"futures_margin":{"ES":"1.00"},"expiry":{"settle":false}}"#,
    );
    let mut c = Conn::open(&gw, "reactor", None);
    mapping(&feed);
    let mut t = T0;
    let mut step = |c: &mut Conn, bid: i64, ask: i64, intent: String, cid: &str| {
        t += 60 * S;
        bbo(&feed, t, bid, ask);
        clock.advance("reactor", 0, t, WAIT).unwrap();
        let r = c.submit(&format!("r-{cid}"), &intent);
        assert_eq!(r["outcome"], "accepted", "{r}");
        clock.advance("reactor", 1, t + S, WAIT).unwrap();
        fill(c, cid)
    };
    // Long 2 at 7743.50.
    step(
        &mut c,
        774_325,
        774_350,
        es("wse-f1", "buy", "open", 2, "7744.00"),
        "wse-f1",
    );
    // Held 2, the cap: a close is still allowed. Sell 1 at 7753.50: +500.00.
    step(
        &mut c,
        775_350,
        775_375,
        es("wse-f2", "sell", "close", 1, "7753.00"),
        "wse-f2",
    );
    assert_eq!(account(&mut c, "a1")["cash"], "100500.00");
    let (p, _) = c.req("positions", "p1", "");
    assert_eq!(p["positions"][0]["avg_entry_price"], "7743.50", "{p}");
    // Sell 2 at 7733.50: the last long realizes −500.00; a short of 1 starts there.
    step(
        &mut c,
        773_350,
        773_375,
        es("wse-f3", "sell", "open", 2, "7733.00"),
        "wse-f3",
    );
    assert_eq!(account(&mut c, "a2")["cash"], "100000.00");
    let (p, _) = c.req("positions", "p2", "");
    assert_eq!(p["positions"][0]["qty"], -1, "{p}");
    assert_eq!(p["positions"][0]["avg_entry_price"], "7733.50", "{p}");
    gw.stop().unwrap();
}

/// Globex opens Sunday at 18:00 on the local clock, also on the Sundays daylight
/// saving time ends (2026-11-01) and starts (2027-03-14).
#[test]
fn sunday_opens_at_18_00_local_on_dst_sundays_too() {
    // (a closed probe, an open probe, the contract), New York time: 17:59 and 18:00
    // on 2026-10-04; 17:30 and 18:05 EST on 2026-11-01; 17:30 and 18:30 EDT on
    // 2027-03-14.
    let cases: [(u64, u64, &str); 3] = [
        (1_791_151_140_000_000_000, 1_791_151_200_000_000_000, "ESZ6"),
        (1_793_572_200_000_000_000, 1_793_574_300_000_000_000, "ESZ6"),
        (1_805_059_800_000_000_000, 1_805_063_400_000_000_000, "ESM7"),
    ];
    for (k, (closed, open, symbol)) in cases.into_iter().enumerate() {
        let (gw, feed, clock) = gateway(
            &format!("fut-sunday-{k}"),
            r#"{"futures_margin":{"ES":"1.00"},"expiry":{"settle":false}}"#,
        );
        let mut c = Conn::open(&gw, "reactor", None);
        map(&feed, closed - 60 * S, 20, symbol);
        quote(&feed, 20, closed, 774_325, 774_350);
        clock.advance("reactor", 0, closed, WAIT).unwrap();
        let r = c.submit(
            "r1",
            &order(symbol, "wse-sun-1", "buy", "open", 1, "7700.00"),
        );
        assert_eq!(r["code"], "sim_market_closed", "case {k}: {r}");
        quote(&feed, 20, open, 774_325, 774_350);
        clock.advance("reactor", 1, open, WAIT).unwrap();
        let r = c.submit(
            "r2",
            &order(symbol, "wse-sun-2", "buy", "open", 1, "7700.00"),
        );
        assert_eq!(r["outcome"], "accepted", "case {k}: {r}");
        gw.stop().unwrap();
    }
}

/// Each root's multiplier: MES realizes 5 a point; NQ settles at 20 a point through
/// the NDX opening quotation.
#[test]
fn multipliers_and_the_ndx_settlement() {
    let dir = root("fut-nq-file");
    let file = dir.join("settlements.csv");
    std::fs::write(
        &file,
        "root,date,kind,value\nNDX,2026-12-18,AM-SOQ,21100.00\n",
    )
    .unwrap();
    let script = format!(
        r#"{{"account":{{"cash":"100000.00"}},"futures_margin":{{"MES":"1.00","NQ":"1.00"}},"expiry":{{"settlement_file":{:?}}}}}"#,
        file.display().to_string()
    );
    let (gw, feed, clock) = gateway("fut-mult", &script);
    let mut c = Conn::open(&gw, "reactor", None);
    map(&feed, T0 - S, 30, "MESZ6");
    map(&feed, T0 - S, 31, "NQZ6");
    quote(&feed, 30, T0, 774_325, 774_350);
    quote(&feed, 31, T0, 2_100_000, 2_100_025);
    clock.advance("reactor", 0, T0, WAIT).unwrap();
    c.submit(
        "r1",
        &order("MESZ6", "wse-mes", "buy", "open", 1, "7744.00"),
    );
    c.submit("r2", &order("NQZ6", "wse-nq", "buy", "open", 1, "21001.00"));
    clock.advance("reactor", 2, T0 + S, WAIT).unwrap();
    fill(&mut c, "wse-mes");
    quote(&feed, 30, T0 + 60 * S, 775_350, 775_375);
    clock.advance("reactor", 0, T0 + 60 * S, WAIT).unwrap();
    c.submit(
        "r3",
        &order("MESZ6", "wse-mes-x", "sell", "close", 1, "7753.00"),
    );
    clock.advance("reactor", 1, T0 + 61 * S, WAIT).unwrap();
    fill(&mut c, "wse-mes-x");
    // MES: (7753.50 − 7743.50) × 5 = 50.00.
    assert_eq!(account(&mut c, "a1")["cash"], "100050.00");
    // NQ held to 2026-12-18 09:30: (21100.00 − 21000.25) × 20 = 1995.00.
    clock
        .advance("reactor", 0, T0 + 74 * 24 * H + H, WAIT)
        .unwrap();
    let s = c
        .until(|v| v["kind"] == "activity" && v["activity_type"] == "SETTLE")
        .pop()
        .unwrap();
    assert_eq!(s["broker_raw"]["symbol"], "NQZ6", "{s}");
    assert_eq!(s["broker_raw"]["net_amount"], "1995.00", "{s}");
    gw.stop().unwrap();
}

/// A root without a margin in the profile can't be opened; the account reports
/// its net liquidation value, with the futures' unrealized profit.
#[test]
fn margin_is_required_and_equity_marks_the_position() {
    let (gw, feed, clock) = gateway(
        "fut-nlv",
        r#"{"account":{"cash":"100000.00"},"futures_margin":{"ES":"20000.00"},"expiry":{"settle":false}}"#,
    );
    let mut c = Conn::open(&gw, "reactor", None);
    mapping(&feed);
    map(&feed, T0 - S, 30, "MESZ6");
    bbo(&feed, T0, 774_325, 774_350);
    quote(&feed, 30, T0, 774_325, 774_350);
    clock.advance("reactor", 0, T0, WAIT).unwrap();
    let r = c.submit(
        "r1",
        &order("MESZ6", "wse-mes", "buy", "open", 1, "7744.00"),
    );
    assert_eq!(r["code"], "sim_no_margin", "{r}");
    c.submit("r2", &es("wse-es", "buy", "open", 1, "7744.00"));
    clock.advance("reactor", 2, T0 + S, WAIT).unwrap();
    fill(&mut c, "wse-es");
    // Bought at 7743.50; the mid rises to 7753.375: +493.75 unrealized.
    bbo(&feed, T0 + 30 * S, 775_325, 775_350);
    clock.advance("reactor", 0, T0 + 30 * S, WAIT).unwrap();
    let (a, _) = c.req("account", "a1", "");
    assert_eq!(a["broker_raw"]["equity"], "100493.75", "{a}");
    assert_eq!(a["account"]["buying_power"], "80493.75", "{a}");
    gw.stop().unwrap();
}

/// A close needs no fresh quote; an opening does.
#[test]
fn a_close_needs_no_fresh_quote() {
    let (gw, feed, clock) = gateway(
        "fut-stale",
        r#"{"futures_margin":{"ES":"1.00"},"expiry":{"settle":false}}"#,
    );
    let mut c = Conn::open(&gw, "reactor", None);
    mapping(&feed);
    bbo(&feed, T0, 774_325, 774_350);
    clock.advance("reactor", 0, T0, WAIT).unwrap();
    c.submit("r1", &es("wse-q1", "buy", "open", 1, "7744.00"));
    clock.advance("reactor", 1, T0 + S, WAIT).unwrap();
    fill(&mut c, "wse-q1");
    // Ten minutes with no quote.
    clock.advance("reactor", 0, T0 + 600 * S, WAIT).unwrap();
    let r = c.submit("r2", &es("wse-q2", "buy", "open", 1, "7744.00"));
    assert_eq!(r["code"], "sim_no_quote", "{r}");
    let r = c.submit("r3", &es("wse-q3", "sell", "close", 1, "7700.00"));
    assert_eq!(r["outcome"], "accepted", "{r}");
    gw.stop().unwrap();
}

/// At 09:30 on the expiry day, a working order on the contract expires even with
/// nothing held.
#[test]
fn working_orders_expire_at_the_settlement() {
    let (gw, feed, clock) = gateway(
        "fut-expire-working",
        r#"{"futures_margin":{"ES":"1.00"},"expiry":{"settle":false}}"#,
    );
    let mut c = Conn::open(&gw, "reactor", None);
    mapping(&feed);
    // 2026-12-18, 09:00 New York: a resting buy.
    let dec18_9 = T0 + 74 * 24 * H;
    bbo(&feed, dec18_9, 774_325, 774_350);
    clock.advance("reactor", 0, dec18_9, WAIT).unwrap();
    let r = c.submit("r1", &es("wse-w1", "buy", "open", 1, "7700.00"));
    assert_eq!(r["outcome"], "accepted", "{r}");
    let a = clock.advance("reactor", 1, dec18_9 + H, WAIT).unwrap();
    let seen = c.until(|v| v["seq"].as_u64() == Some(a.last_seq));
    let expired = seen
        .iter()
        .find(|v| v["client_order_id"] == "wse-w1" && v["event"] == "expired")
        .unwrap_or_else(|| panic!("{seen:?}"));
    assert_eq!(expired["ts_broker"], dec18_9 + 30 * 60 * S);
    gw.stop().unwrap();
}

/// A scripted simulator (no market data) can't run the futures checks: it refuses
/// futures.
#[test]
fn scripted_mode_refuses_futures() {
    let mut cfg = config(root("fut-scripted"));
    cfg.profile.futures = vec!["ES".into()];
    let gw = scripted(cfg, "{}");
    let mut c = Conn::open(&gw, "reactor", None);
    let r = c.submit("r1", &es("wse-x1", "buy", "open", 1, "7744.00"));
    assert_eq!(r["code"], "sim_futures_need_quotes", "{r}");
    gw.stop().unwrap();
}
