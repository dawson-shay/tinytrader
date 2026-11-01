//! A simulator fed in process and clocked by a session (decision 0022): records pushed
//! through a `Feed`, symbols from symbol mappings, `cbbo-1m` and `bbo-1m` books, and a
//! `Clock` that advances only once the client's order messages have all been handled.
mod support;
use dbn::{
    BboMsg, CbboMsg, Cmbp1Msg, RecordHeader, RecordRef, SType, Schema, SymbolMappingMsg, rtype,
};
use order_gateway::{Clock, Feed, Gateway, SimBroker, start};
use serde_json::Value;
use std::time::Duration;
use support::*;

const S: u64 = 1_000_000_000;
const WAIT: Duration = Duration::from_secs(5);

fn px(cents: i64) -> i64 {
    cents * 10_000_000
}

/// The options by OPRA raw symbol (root padded to six), and SPY's shares, as a batch
/// file's symbol mappings give them.
fn mappings(feed: &Feed) {
    let opra = |s: &str| format!("{:<6}{}", &s[..3], &s[3..]);
    for (iid, raw) in [
        (1, opra(CALL)),
        (2, opra(PUT_A)),
        (3, opra(PUT_B)),
        (9, "SPY".into()),
    ] {
        let m = SymbolMappingMsg::new(
            iid,
            T0 - S,
            SType::Parent,
            "SPY.OPT",
            SType::RawSymbol,
            &raw,
            0,
            0,
        )
        .unwrap();
        feed.push(T0 - S, RecordRef::from(&m)).unwrap();
    }
}

/// A one-minute consolidated quote, stamped at the interval's end.
fn cbbo(feed: &Feed, iid: u32, ts: u64, bid: i64, ask: i64) {
    let mut q = CbboMsg {
        hd: RecordHeader::new::<CbboMsg>(rtype::CBBO_1M, 30, iid, u64::MAX),
        ts_recv: ts,
        ..CbboMsg::default_for_schema(Schema::Cbbo1M)
    };
    q.levels[0].bid_px = px(bid);
    q.levels[0].ask_px = px(ask);
    q.levels[0].bid_sz = 10;
    q.levels[0].ask_sz = 10;
    feed.push(ts, RecordRef::from(&q)).unwrap();
}

/// A `tcbbo` record: a trade of `size` at `price` cents, with the quote before it.
fn print(feed: &Feed, iid: u32, ts: u64, (bid, ask): (i64, i64), price: i64, size: u32) {
    let mut q = Cmbp1Msg {
        hd: RecordHeader::new::<Cmbp1Msg>(rtype::TCBBO, 30, iid, ts),
        ts_recv: ts,
        price: px(price),
        size,
        ..Cmbp1Msg::default_for_schema(Schema::Tcbbo)
    };
    q.levels[0].bid_px = px(bid);
    q.levels[0].ask_px = px(ask);
    q.levels[0].bid_sz = 10;
    q.levels[0].ask_sz = 10;
    feed.push(ts, RecordRef::from(&q)).unwrap();
}

fn bbo(feed: &Feed, iid: u32, ts: u64, bid: i64, ask: i64) {
    let mut q = BboMsg {
        hd: RecordHeader::new::<BboMsg>(rtype::BBO_1M, 2, iid, ts),
        ts_recv: ts,
        ..BboMsg::default_for_schema(Schema::Bbo1M)
    };
    q.levels[0].bid_px = px(bid);
    q.levels[0].ask_px = px(ask);
    q.levels[0].bid_sz = 300;
    q.levels[0].ask_sz = 300;
    feed.push(ts, RecordRef::from(&q)).unwrap();
}

fn gateway(name: &str, script: &str) -> (Gateway, Feed, Clock) {
    gateway_capped(name, script, None)
}

fn gateway_capped(name: &str, script: &str, cap_dollars: Option<i64>) -> (Gateway, Feed, Clock) {
    let (sim, feed) = SimBroker::fed(script, r#"{"source":"session","run":"test"}"#).unwrap();
    let mut cfg = config(root(name));
    cfg.profile.equities = vec!["SPY".into()];
    if let Some(c) = cap_dollars {
        cfg.profile.max_notional_cents = c * 100;
    }
    let gw = start(cfg, sim).unwrap();
    let clock = gw.clock();
    (gw, feed, clock)
}

fn stock(cid: &str, side: &str, qty: u32, price: &str) -> String {
    format!(
        r#"{{"client_order_id":"{cid}","shape":"single","instrument":{{"class":"equity","symbol":"SPY"}},"side":"{side}","effect":"open","qty":{qty},"type":"limit","limit_price":"{price}","time_in_force":"day"}}"#
    )
}

fn fill(c: &mut Conn, cid: &str) -> Value {
    c.until(|v| v["kind"] == "order" && v["client_order_id"] == cid && v["event"] == "fill")
        .pop()
        .unwrap()
}

/// An options combo and a share order fill on the minute after they're sent, each at
/// its touch, from `cbbo-1m` and `bbo-1m` books keyed by symbol mappings.
#[test]
fn options_and_shares_fill_from_minute_quotes() {
    let (gw, feed, clock) = gateway("fed-fill", "{}");
    let mut c = Conn::open(&gw, "reactor", None);
    let welcome = serde_json::from_str::<Value>(&c.lines[0]).unwrap();
    let pins = &welcome["pins"]["broker"];
    assert_eq!(pins["model"], "tinytrader-sim/7");
    assert_eq!(pins["quotes"]["source"], "session");
    mappings(&feed);
    // At first neither fills: the spread's natural is 0.92 - 0.53 = 0.39 credit, short
    // of the 0.40 asked, and the shares' ask is above the limit.
    cbbo(&feed, 2, T0, 92, 94);
    cbbo(&feed, 3, T0, 51, 53);
    bbo(&feed, 9, T0, 58_011, 58_012);
    assert_eq!(clock.advance("reactor", 0, T0, WAIT).unwrap().market_ts, T0);
    assert_eq!(
        c.submit("r1", &credit_spread("wse-cs", 2))["outcome"],
        "accepted"
    );
    assert_eq!(
        c.submit("r2", &stock("wse-sh", "buy", 4, "580.10"))["outcome"],
        "accepted"
    );
    // The next minute the shares fill at the new ask; the spread is unchanged. A minute
    // later its natural is 0.94 - 0.52 = 0.42 and it fills there: both legs' quotes of
    // that instant apply before it is evaluated, never 0.94 against the old 0.53.
    cbbo(&feed, 2, T0 + 60 * S, 92, 94);
    cbbo(&feed, 3, T0 + 60 * S, 51, 53);
    bbo(&feed, 9, T0 + 60 * S, 58_006, 58_007);
    clock.advance("reactor", 2, T0 + 60 * S, WAIT).unwrap();
    let f = fill(&mut c, "wse-sh");
    assert_eq!(f["fill"]["price"], "580.07");
    assert_eq!(f["ts_broker"], T0 + 60 * S);
    cbbo(&feed, 2, T0 + 120 * S, 94, 96);
    cbbo(&feed, 3, T0 + 120 * S, 50, 52);
    clock.advance("reactor", 2, T0 + 120 * S, WAIT).unwrap();
    let f = fill(&mut c, "wse-cs");
    assert_eq!(
        (f["fill"]["price"].as_str(), f["filled_qty"].as_u64()),
        (Some("-0.42"), Some(2))
    );
    assert_eq!(f["ts_broker"], T0 + 120 * S);
    gw.stop().unwrap();
}

/// An order sent just before the clock moves is always handled first: the clock waits
/// for the requests the session says were sent, however the threads interleave.
#[test]
fn the_clock_waits_for_the_requests_sent() {
    let mut seen = Vec::new();
    for round in 0..10 {
        let (gw, feed, clock) = gateway(&format!("fed-race-{round}"), "{}");
        let mut c = Conn::open(&gw, "reactor", None);
        mappings(&feed);
        cbbo(&feed, 1, T0, 120, 130);
        clock.advance("reactor", 0, T0, WAIT).unwrap();
        // Sent, not awaited: the clock may well reach the gateway first.
        let body = format!(r#"{{"request_id":"r1","intent":{}}}"#, buy("wse-race"));
        c.c.send("submit", &body).unwrap();
        cbbo(&feed, 1, T0 + 60 * S, 121, 124);
        clock.advance("reactor", 1, T0 + 60 * S, WAIT).unwrap();
        let f = fill(&mut c, "wse-race");
        seen.push((f["fill"]["price"].clone(), f["ts_broker"].clone()));
        gw.stop().unwrap();
    }
    assert!(seen.iter().all(|s| *s == seen[0]), "{seen:?}");
    assert_eq!(seen[0].0, "1.24");
}

#[test]
fn a_clock_waiting_for_requests_never_sent_says_so() {
    let (gw, feed, clock) = gateway("fed-wait", "{}");
    let _c = Conn::open(&gw, "reactor", None);
    mappings(&feed);
    let e = clock
        .advance("reactor", 3, T0, Duration::from_millis(200))
        .unwrap_err();
    assert!(e.contains("waited for 3 order messages, 0 arrived"), "{e}");
    let e = clock.advance("nobody", 0, T0, WAIT).unwrap_err();
    assert!(e.contains("no client nobody"), "{e}");
    gw.stop().unwrap();
}

/// The strategy's declared latency delays activation: a quote good for one second is
/// missed by a strategy two seconds slow.
#[test]
fn strategy_latency_delays_activation() {
    let run = |name: &str, script: &str| {
        let (gw, feed, clock) = gateway(name, script);
        let mut c = Conn::open(&gw, "reactor", None);
        mappings(&feed);
        cbbo(&feed, 1, T0, 120, 130);
        clock.advance("reactor", 0, T0, WAIT).unwrap();
        c.submit("r1", &buy("wse-lat"));
        cbbo(&feed, 1, T0 + S, 120, 124);
        cbbo(&feed, 1, T0 + 2 * S, 120, 130);
        clock.advance("reactor", 1, T0 + 10 * S, WAIT).unwrap();
        let (r, _) = c.req("order", "r3", r#""client_order_id":"wse-lat""#);
        gw.stop().unwrap();
        r["order"]["status"].as_str().unwrap().to_owned()
    };
    assert_eq!(run("fed-lat-0", "{}"), "filled");
    assert_ne!(
        run("fed-lat-2s", r#"{"strategy_latency_ms":2000}"#),
        "filled"
    );
}

/// A wait that times out is dropped: it never holds up the advances behind it, and
/// never runs late when the order it waited for arrives after all.
#[test]
fn a_timed_out_clock_never_wedges_or_runs_late() {
    let (gw, feed, clock) = gateway("fed-wedge", "{}");
    let mut c = Conn::open(&gw, "reactor", None);
    mappings(&feed);
    cbbo(&feed, 1, T0, 120, 130);
    clock.advance("reactor", 0, T0, WAIT).unwrap();
    let late = T0 + 60 * S;
    cbbo(&feed, 1, late, 121, 124);
    assert!(
        clock
            .advance("reactor", 1, late, Duration::from_millis(100))
            .is_err()
    );
    // Not wedged: an advance that needs nothing more runs at once.
    assert_eq!(
        clock.advance("reactor", 0, T0 + S, WAIT).unwrap().market_ts,
        T0 + S
    );
    // The order the dropped wait wanted arrives now; the dropped advance to `late`
    // doesn't run, so the order rests on the book of T0 + 1 s.
    c.submit("r1", &buy("wse-late"));
    std::thread::sleep(Duration::from_millis(100));
    let (r, _) = c.req("order", "r2", r#""client_order_id":"wse-late""#);
    assert_ne!(r["order"]["status"], "filled", "{r}");
    gw.stop().unwrap();
}

/// Only order messages count: queries sent before an order don't release the clock
/// before the order is handled.
#[test]
fn queries_never_count_as_order_messages() {
    let (gw, feed, clock) = gateway("fed-queries", "{}");
    let mut c = Conn::open(&gw, "reactor", None);
    mappings(&feed);
    cbbo(&feed, 1, T0, 120, 130);
    clock.advance("reactor", 0, T0, WAIT).unwrap();
    c.c.send("open_orders", r#"{"request_id":"q1"}"#).unwrap();
    c.c.send("positions", r#"{"request_id":"q2"}"#).unwrap();
    let body = format!(r#"{{"request_id":"r1","intent":{}}}"#, buy("wse-q"));
    c.c.send("submit", &body).unwrap();
    cbbo(&feed, 1, T0 + 60 * S, 121, 124);
    clock.advance("reactor", 1, T0 + 60 * S, WAIT).unwrap();
    assert_eq!(fill(&mut c, "wse-q")["ts_broker"], T0 + 60 * S);
    gw.stop().unwrap();
}

#[test]
fn a_client_cannot_move_a_session_clock() {
    let (gw, feed, clock) = gateway("fed-own-advance", "{}");
    let mut c = Conn::open(&gw, "reactor", None);
    mappings(&feed);
    clock.advance("reactor", 0, T0, WAIT).unwrap();
    let (r, _) = c.req("advance", "a1", &format!(r#""to_ts":{}"#, T0 + S));
    assert_eq!(r["code"], "clocked_by_session", "{r}");
    gw.stop().unwrap();
}

#[test]
fn a_record_going_back_is_refused() {
    let (gw, feed, _clock) = gateway("fed-back", "{}");
    cbbo(&feed, 1, T0 + S, 120, 130);
    let q = CbboMsg {
        hd: RecordHeader::new::<CbboMsg>(rtype::CBBO_1M, 30, 1, u64::MAX),
        ts_recv: T0,
        ..CbboMsg::default_for_schema(Schema::Cbbo1M)
    };
    let e = feed.push(T0, RecordRef::from(&q)).unwrap_err();
    assert!(e.contains("after one at"), "{e}");
    gw.stop().unwrap();
}

/// Three updates in one instant, two of them for one contract: the order sees only
/// the instant's last quote of each, never the replaced one.
#[test]
fn an_instant_is_one_book_with_the_last_quote_of_each() {
    let (gw, feed, clock) = gateway("fed-instant", "{}");
    let mut c = Conn::open(&gw, "reactor", None);
    mappings(&feed);
    cbbo(&feed, 2, T0, 92, 94);
    cbbo(&feed, 3, T0, 51, 53);
    clock.advance("reactor", 0, T0, WAIT).unwrap();
    c.submit("r1", &credit_spread("wse-cs", 1));
    // A good enough with the old B, then B, then A replaced by a worse quote.
    cbbo(&feed, 2, T0 + 60 * S, 99, 101);
    cbbo(&feed, 3, T0 + 60 * S, 51, 53);
    cbbo(&feed, 2, T0 + 60 * S, 92, 94);
    clock.advance("reactor", 1, T0 + 60 * S, WAIT).unwrap();
    let (r, _) = c.req("order", "r2", r#""client_order_id":"wse-cs""#);
    assert_ne!(r["order"]["status"], "filled", "{r}");
    gw.stop().unwrap();
}

/// A tcbbo quote (the one before a trade) doesn't replace a book that quote records
/// give; alone, it is the book.
#[test]
fn tcbbo_is_a_book_only_where_nothing_else_is() {
    let tcbbo = |feed: &Feed, iid: u32, ts: u64, bid: i64, ask: i64| {
        let mut q = Cmbp1Msg {
            hd: RecordHeader::new::<Cmbp1Msg>(rtype::TCBBO, 30, iid, ts),
            ts_recv: ts,
            ..Cmbp1Msg::default_for_schema(Schema::Tcbbo)
        };
        q.levels[0].bid_px = px(bid);
        q.levels[0].ask_px = px(ask);
        q.levels[0].bid_sz = 10;
        q.levels[0].ask_sz = 10;
        feed.push(ts, RecordRef::from(&q)).unwrap();
    };
    let (gw, feed, clock) = gateway("fed-tcbbo", "{}");
    let mut c = Conn::open(&gw, "reactor", None);
    mappings(&feed);
    cbbo(&feed, 1, T0, 120, 130);
    tcbbo(&feed, 2, T0, 50, 52);
    clock.advance("reactor", 0, T0, WAIT).unwrap();
    c.submit("r1", &buy("wse-q"));
    c.submit("r2", &single("wse-t", PUT_A, "buy", "open", 1, "0.51"));
    tcbbo(&feed, 1, T0 + S, 119, 121);
    tcbbo(&feed, 2, T0 + S, 49, 51);
    clock.advance("reactor", 2, T0 + 2 * S, WAIT).unwrap();
    assert_eq!(fill(&mut c, "wse-t")["fill"]["price"], "0.51");
    let (r, _) = c.req("order", "r3", r#""client_order_id":"wse-q""#);
    assert_ne!(r["order"]["status"], "filled", "{r}");
    gw.stop().unwrap();
}

/// The clock's answer names the account's last event, so a strategy can read its
/// reports up to it before acting on the step: here, the fill.
#[test]
fn an_advance_names_the_last_event_it_journaled() {
    let (gw, feed, clock) = gateway("fed-seq", "{}");
    let mut c = Conn::open(&gw, "reactor", None);
    mappings(&feed);
    cbbo(&feed, 1, T0, 120, 130);
    clock.advance("reactor", 0, T0, WAIT).unwrap();
    c.submit("r1", &buy("wse-seq"));
    cbbo(&feed, 1, T0 + 60 * S, 121, 124);
    let a = clock.advance("reactor", 1, T0 + 60 * S, WAIT).unwrap();
    let f = fill(&mut c, "wse-seq");
    assert_eq!(f["seq"], a.last_seq);
    gw.stop().unwrap();
}

/// A record for a time the simulator has already passed is refused, and a refused
/// push doesn't move the feed's notion of the last time.
#[test]
fn a_record_behind_the_clock_is_refused() {
    let (gw, feed, clock) = gateway("fed-floor", "{}");
    let _c = Conn::open(&gw, "reactor", None);
    mappings(&feed);
    cbbo(&feed, 1, T0, 120, 130);
    clock.advance("reactor", 0, T0 + S, WAIT).unwrap();
    let q = CbboMsg {
        hd: RecordHeader::new::<CbboMsg>(rtype::CBBO_1M, 30, 1, u64::MAX),
        ts_recv: T0 + S / 2,
        ..CbboMsg::default_for_schema(Schema::Cbbo1M)
    };
    let e = feed.push(T0 + S / 2, RecordRef::from(&q)).unwrap_err();
    assert!(e.contains("the simulator is at"), "{e}");
    cbbo(&feed, 1, T0 + 2 * S, 120, 130);
    gw.stop().unwrap();
}

/// The bug a year-long run found: a position held past its expiry, its contract's
/// instrument id reused the next day by a deep in-the-money LEAP quoted near $707,
/// and a close on the expired contract filled against the LEAP's book. An expired
/// contract is refused and has no book; a reused id forgets the contract it was;
/// a contract no longer mapped has no stale quote.
#[test]
fn an_expired_contract_never_fills_against_a_reused_id() {
    let day = 86_400 * S;
    let (gw, feed, clock) = gateway_capped("fed-expired", "{}", Some(100_000));
    let mut c = Conn::open(&gw, "reactor", None);
    mappings(&feed); // CALL (expires on T0's day) as 1, PUT_A as 2, …
    let map = |iid: u32, ts: u64, raw: &str| {
        let m = SymbolMappingMsg::new(
            iid,
            ts,
            SType::Parent,
            "SPY.OPT",
            SType::RawSymbol,
            raw,
            0,
            0,
        )
        .unwrap();
        feed.push(ts, RecordRef::from(&m)).unwrap();
    };
    // A weekly expiring later, as 4.
    map(4, T0 - S / 2, "SPY   261009P00570000");
    // SPY itself, so the call held through its expiry settles at the close.
    bbo(&feed, 9, T0, 57_000, 57_002);
    cbbo(&feed, 1, T0, 120, 125);
    cbbo(&feed, 4, T0, 80, 82);
    clock.advance("reactor", 0, T0, WAIT).unwrap();
    assert_eq!(c.submit("r1", &buy("wse-open"))["outcome"], "accepted");
    clock.advance("reactor", 1, T0 + 60 * S, WAIT).unwrap();
    assert_eq!(fill(&mut c, "wse-open")["fill"]["price"], "1.25");
    // SPY just before the close, to settle the call that expires today.
    bbo(&feed, 9, T0 + 6 * 3_600 * S - 30 * S, 57_000, 57_002);
    // The next day: id 1 is a LEAP now, and id 4 another weekly.
    let t1 = T0 + day;
    map(1, t1 - 60 * S, "SPY   270115C00060000");
    map(4, t1 - 60 * S, "SPY   261009P00560000");
    cbbo(&feed, 1, t1, 70_700, 70_731);
    cbbo(&feed, 4, t1, 40, 42);
    clock.advance("reactor", 1, t1, WAIT).unwrap();
    // Settled at its close (exercised against SPY's quote), nothing is left to
    // close; an order on it is refused as expired, never priced at the LEAP's ask.
    let r = c.submit("r2", &single("wse-again", CALL, "buy", "open", 1, "707.31"));
    assert_eq!(r["outcome"], "rejected", "{r}");
    assert_eq!(r["code"], "sim_expired_instrument", "{r}");
    // A contract not mapped today is unknown, never priced from yesterday's quote.
    let r = c.submit(
        "r3",
        &single("wse-weekly", "SPY261009P00570000", "buy", "open", 1, "1.00"),
    );
    assert_eq!(r["code"], "sim_unknown_instrument", "{r}");
    // The LEAP itself trades on its own, new book.
    let r = c.submit(
        "r4",
        &single("wse-leap", "SPY270115C00060000", "buy", "open", 1, "707.31"),
    );
    assert_eq!(r["outcome"], "accepted", "{r}");
    clock.advance("reactor", 2, t1 + 60 * S, WAIT).unwrap();
    assert_eq!(fill(&mut c, "wse-leap")["fill"]["price"], "707.31");
    gw.stop().unwrap();
}

/// End to end: a long call held to its expiry is exercised at the close against
/// the venue's quote of the underlying, as an `activity` event the client sees,
/// and the position becomes 100 shares at the strike.
#[test]
fn a_call_held_to_expiry_is_exercised_into_shares() {
    let (gw, feed, clock) = gateway("fed-exercise", "{}");
    let mut c = Conn::open(&gw, "reactor", None);
    mappings(&feed); // CALL is SPY 2026-10-05 580 call, as 1; SPY shares as 9
    cbbo(&feed, 1, T0, 1_000, 1_002);
    bbo(&feed, 9, T0, 58_999, 59_001);
    clock.advance("reactor", 0, T0, WAIT).unwrap();
    c.submit("r1", &single("wse-call", CALL, "buy", "open", 1, "10.02"));
    clock.advance("reactor", 1, T0 + 60 * S, WAIT).unwrap();
    assert_eq!(fill(&mut c, "wse-call")["fill"]["price"], "10.02");
    // The close: SPY at 590.00, the call 10.00 in the money; a quote within the
    // close's minute settles it.
    let close = T0 + 6 * 3_600 * S;
    bbo(&feed, 9, close - 30 * S, 58_999, 59_001);
    clock.advance("reactor", 1, close + S, WAIT).unwrap();
    let a = c.until(|v| v["kind"] == "activity").pop().unwrap();
    assert_eq!(a["activity_type"], "OPEXC", "{a}");
    assert_eq!(a["broker_raw"]["symbol"], CALL);
    assert_eq!(a["broker_raw"]["price"], "590.00");
    let (r, _) = c.req("positions", "r2", "");
    let pos = r["positions"].as_array().unwrap();
    assert_eq!(pos.len(), 1, "{r}");
    assert_eq!(
        (pos[0]["symbol"].as_str(), pos[0]["qty"].as_i64()),
        (Some("SPY"), Some(100))
    );
    assert_eq!(pos[0]["avg_entry_price"], "580.00");
    gw.stop().unwrap();
}

/// Every quote is cleared at a new day: an order at the next day's open can't
/// fill on yesterday's book, only on that day's first quote.
#[test]
fn an_order_at_the_open_never_fills_on_yesterdays_book() {
    let day = 86_400 * S;
    let (gw, feed, clock) = gateway("fed-overnight", "{}");
    let mut c = Conn::open(&gw, "reactor", None);
    mappings(&feed);
    let weekly = "SPY261009P00570000";
    let map = |iid: u32, ts: u64| {
        let m = SymbolMappingMsg::new(
            iid,
            ts,
            SType::Parent,
            "SPY.OPT",
            SType::RawSymbol,
            "SPY   261009P00570000",
            0,
            0,
        )
        .unwrap();
        feed.push(ts, RecordRef::from(&m)).unwrap();
    };
    map(4, T0 - S / 2);
    cbbo(&feed, 4, T0 + 6 * 3_600 * S - 60 * S, 120, 125); // 15:59, ask 1.25
    clock
        .advance("reactor", 0, T0 + 6 * 3_600 * S - 30 * S, WAIT)
        .unwrap();
    // The next day at 09:30:30, before its first bar.
    let open = T0 + day - 30 * 60 * S + 30 * S;
    map(4, T0 + day - 14 * 3_600 * S);
    clock.advance("reactor", 0, open, WAIT).unwrap();
    c.submit("r1", &single("wse-open", weekly, "buy", "open", 1, "1.30"));
    clock.advance("reactor", 1, open + 10 * S, WAIT).unwrap();
    let (r, _) = c.req("order", "r2", r#""client_order_id":"wse-open""#);
    assert_ne!(
        r["order"]["status"], "filled",
        "filled on yesterday's book: {r}"
    );
    cbbo(&feed, 4, open + 30 * S, 126, 129); // 09:31, ask 1.29
    clock.advance("reactor", 1, open + 31 * S, WAIT).unwrap();
    assert_eq!(fill(&mut c, "wse-open")["fill"]["price"], "1.29");
    gw.stop().unwrap();
}

/// fill_on = "next_quote": a marketable order waits for the first quote after it
/// activates, never filling on the bar its strategy decided on; the default fills
/// on the book standing at activation.
#[test]
fn next_quote_fills_on_the_bar_after_the_decision() {
    let run = |name: &str, script: &str| {
        let (gw, feed, clock) = gateway(name, script);
        let mut c = Conn::open(&gw, "reactor", None);
        mappings(&feed);
        cbbo(&feed, 1, T0, 120, 125);
        clock.advance("reactor", 0, T0, WAIT).unwrap();
        c.submit("r1", &single("wse-nq", CALL, "buy", "open", 1, "1.30"));
        cbbo(&feed, 1, T0 + 60 * S, 126, 128);
        clock.advance("reactor", 1, T0 + 60 * S, WAIT).unwrap();
        let f = fill(&mut c, "wse-nq");
        gw.stop().unwrap();
        (
            f["fill"]["price"].as_str().unwrap().to_owned(),
            f["ts_broker"].as_u64().unwrap(),
        )
    };
    assert_eq!(run("fed-book", "{}"), ("1.25".into(), T0 + 5_000_000));
    assert_eq!(
        run("fed-next", r#"{"fill_on":"next_quote"}"#),
        ("1.28".into(), T0 + 60 * S)
    );
}

/// Orders on an expired contract are refused, and however many there are, they
/// never halt opens: the next valid order is accepted.
#[test]
fn expired_rejects_never_halt_opens() {
    let day = 86_400 * S;
    let (gw, feed, clock) = gateway("fed-rejects", "{}");
    let mut c = Conn::open(&gw, "reactor", None);
    mappings(&feed);
    let t1 = T0 + day;
    cbbo(&feed, 3, t1, 50, 52);
    clock.advance("reactor", 0, t1, WAIT).unwrap();
    for k in 0..8 {
        let r = c.submit(
            &format!("x{k}"),
            &single(&format!("wse-x{k}"), CALL, "buy", "open", 1, "1.00"),
        );
        assert_eq!(r["code"], "sim_expired_instrument", "{r}");
    }
    let r = c.submit(
        "ok",
        &single("wse-ok", "SPY261009P00570000", "buy", "open", 1, "1.00"),
    );
    assert_ne!(r["reason"], "kill", "{r}");
    assert_ne!(r["code"], "killed", "{r}");
    gw.stop().unwrap();
}

/// A contract still trades on its expiry day, to the close: a fill at 15:59.
#[test]
fn a_contract_trades_on_its_expiry_day() {
    let (gw, feed, clock) = gateway("fed-0dte", "{}");
    let mut c = Conn::open(&gw, "reactor", None);
    mappings(&feed);
    let t = T0 + 6 * 3_600 * S - 60 * S; // 15:59 on CALL's expiry day
    cbbo(&feed, 1, t - 60 * S, 5, 7);
    clock.advance("reactor", 0, t - 60 * S, WAIT).unwrap();
    c.submit("r1", &single("wse-0dte", CALL, "buy", "open", 1, "0.07"));
    cbbo(&feed, 1, t, 4, 6);
    clock.advance("reactor", 1, t, WAIT).unwrap();
    assert_eq!(fill(&mut c, "wse-0dte")["fill"]["price"], "0.07");
    gw.stop().unwrap();
}

const H: u64 = 3_600 * S;

/// Same-day openings are refused after the venue's time; other expiries, and the
/// same contract before it, are not.
#[test]
fn same_day_openings_are_refused_late_in_the_day() {
    let (gw, feed, clock) = gateway(
        "fed-1530",
        r#"{"venue_rules":{"no_same_day_opening_after":"15:30"},"expiry":{"settle":false}}"#,
    );
    let mut c = Conn::open(&gw, "reactor", None);
    mappings(&feed);
    let weekly = "SPY261009P00570000";
    let m = SymbolMappingMsg::new(
        4,
        T0 - S / 2,
        SType::Parent,
        "SPY.OPT",
        SType::RawSymbol,
        "SPY   261009P00570000",
        0,
        0,
    )
    .unwrap();
    feed.push(T0 - S / 2, RecordRef::from(&m)).unwrap();
    cbbo(&feed, 1, T0, 120, 125);
    cbbo(&feed, 4, T0, 80, 82);
    clock
        .advance("reactor", 0, T0 + 5 * H + 29 * 60 * S, WAIT)
        .unwrap(); // 15:29
    assert_eq!(
        c.submit("r1", &single("wse-early", CALL, "buy", "open", 1, "1.00"))["outcome"],
        "accepted"
    );
    clock
        .advance("reactor", 1, T0 + 5 * H + 31 * 60 * S, WAIT)
        .unwrap(); // 15:31
    let r = c.submit("r2", &single("wse-late", CALL, "buy", "open", 1, "1.00"));
    assert_eq!(r["code"], "sim_same_day_opening", "{r}");
    assert_eq!(
        c.submit(
            "r3",
            &single("wse-weekly", weekly, "buy", "open", 1, "0.70")
        )["outcome"],
        "accepted"
    );
    gw.stop().unwrap();
}

/// At the venue's liquidation time, a short option expiring today is bought back
/// at its ask, as a broker order the client sees with its fill; the long wing
/// stays, as Alpaca left it.
#[test]
fn same_day_shorts_are_liquidated_at_the_venues_time() {
    let (gw, feed, clock) = gateway(
        "fed-1545",
        r#"{"venue_rules":{"liquidate_same_day_shorts_at":"15:45"},"expiry":{"settle":false}}"#,
    );
    let mut c = Conn::open(&gw, "reactor", None);
    mappings(&feed);
    cbbo(&feed, 2, T0, 92, 94);
    cbbo(&feed, 3, T0, 50, 52);
    clock.advance("reactor", 0, T0, WAIT).unwrap();
    c.submit("r1", &credit_spread("wse-cs", 1));
    cbbo(&feed, 2, T0 + 60 * S, 95, 97);
    clock.advance("reactor", 1, T0 + 60 * S, WAIT).unwrap();
    assert_eq!(fill(&mut c, "wse-cs")["fill"]["price"], "-0.40");
    cbbo(&feed, 2, T0 + 5 * H + 44 * 60 * S, 30, 33);
    clock
        .advance("reactor", 1, T0 + 5 * H + 46 * 60 * S, WAIT)
        .unwrap();
    // The broker's order arrives as `foreign`; its fill as a FILL activity.
    let f = c
        .until(|v| v["kind"] == "activity" && v["activity_type"] == "FILL")
        .pop()
        .unwrap();
    assert_eq!(f["broker_raw"]["price"], "0.33", "{f}");
    assert_eq!(f["broker_raw"]["symbol"], PUT_A);
    assert_eq!(
        f["broker_raw"]["transaction_time_ns"],
        T0 + 5 * H + 45 * 60 * S
    );
    let (r, _) = c.req("positions", "r2", "");
    let syms: Vec<(&str, i64)> = r["positions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| (p["symbol"].as_str().unwrap(), p["qty"].as_i64().unwrap()))
        .collect();
    assert_eq!(syms, [(PUT_B, 1)], "{r}");
    gw.stop().unwrap();
}

/// Working orders count against buying power: two spreads sent in one step on
/// cash for one, the second is refused.
#[test]
fn working_orders_count_against_buying_power() {
    let (gw, feed, clock) = gateway(
        "fed-bp-working",
        r#"{"account":{"cash":"300.00"},"venue_rules":{"buying_power":"defined_risk"},"expiry":{"settle":false},"fill_on":"next_quote"}"#,
    );
    let mut c = Conn::open(&gw, "reactor", None);
    mappings(&feed);
    cbbo(&feed, 2, T0, 92, 94);
    cbbo(&feed, 3, T0, 50, 52);
    clock.advance("reactor", 0, T0, WAIT).unwrap();
    assert_eq!(
        c.submit("r1", &credit_spread("wse-one", 1))["outcome"],
        "accepted"
    );
    let r = c.submit("r2", &credit_spread("wse-two", 1));
    assert_eq!(r["code"], "sim_insufficient_buying_power", "{r}");
    gw.stop().unwrap();
}

/// A resting order that closes doesn't free buying power before it fills.
#[test]
fn a_resting_close_frees_nothing() {
    let (gw, feed, clock) = gateway(
        "fed-bp-close",
        r#"{"account":{"cash":"300.00"},"venue_rules":{"buying_power":"defined_risk"},"expiry":{"settle":false}}"#,
    );
    let mut c = Conn::open(&gw, "reactor", None);
    mappings(&feed);
    cbbo(&feed, 2, T0, 92, 94);
    cbbo(&feed, 3, T0, 50, 52);
    clock.advance("reactor", 0, T0, WAIT).unwrap();
    c.submit("r1", &credit_spread("wse-open", 1));
    clock.advance("reactor", 1, T0 + 60 * S, WAIT).unwrap();
    fill(&mut c, "wse-open");
    let (a, _) = c.req("account", "r2", "");
    let before = a["account"]["buying_power"].clone();
    // A buy-to-close of the short put at 0.01, which won't fill.
    c.submit("r3", &single("wse-close", PUT_A, "buy", "close", 1, "0.01"));
    clock.advance("reactor", 2, T0 + 120 * S, WAIT).unwrap();
    // Its own debit (0.01 × 100) is held as if paid; the risk it would remove isn't
    // released until it fills.
    let (a, _) = c.req("account", "r4", "");
    assert_eq!(before, "139.96");
    assert_eq!(a["account"]["buying_power"], "138.96", "{a}");
    gw.stop().unwrap();
}

/// Defined-risk buying power: a spread whose worst loss cash can't cover is
/// refused, never resized; the account reports what's left.
#[test]
fn buying_power_refuses_what_cash_cant_cover() {
    let (gw, feed, clock) = gateway(
        "fed-bp",
        r#"{"account":{"cash":"150.00"},"venue_rules":{"buying_power":"defined_risk"},"expiry":{"settle":false}}"#,
    );
    let mut c = Conn::open(&gw, "reactor", None);
    mappings(&feed);
    cbbo(&feed, 2, T0, 92, 94);
    cbbo(&feed, 3, T0, 50, 52);
    clock.advance("reactor", 0, T0, WAIT).unwrap();
    // 570/568 for 0.40 credit: worst loss 2.00 × 100; cash 150 + 40 can't cover 200.
    let r = c.submit("r1", &credit_spread("wse-big", 1));
    assert_eq!(r["code"], "sim_insufficient_buying_power", "{r}");
    let (a, _) = c.req("account", "r2", "");
    assert_eq!(a["account"]["buying_power"], "150.00", "{a}");
    gw.stop().unwrap();

    let (gw, feed, clock) = gateway(
        "fed-bp-ok",
        r#"{"account":{"cash":"500.00"},"venue_rules":{"buying_power":"defined_risk"},"expiry":{"settle":false}}"#,
    );
    let mut c = Conn::open(&gw, "reactor", None);
    mappings(&feed);
    cbbo(&feed, 2, T0, 92, 94);
    cbbo(&feed, 3, T0, 50, 52);
    clock.advance("reactor", 0, T0, WAIT).unwrap();
    assert_eq!(
        c.submit("r1", &credit_spread("wse-ok", 1))["outcome"],
        "accepted"
    );
    cbbo(&feed, 2, T0 + 60 * S, 95, 97);
    clock.advance("reactor", 1, T0 + 60 * S, WAIT).unwrap();
    fill(&mut c, "wse-ok");
    // Cash 500 + 40 credit − 0.04 fees − 200 at risk.
    let (a, _) = c.req("account", "r2", "");
    assert_eq!(a["account"]["buying_power"], "339.96", "{a}");
    gw.stop().unwrap();
}

/// fill_on = "next_quote" for a combo: one leg's new quote isn't enough; every leg
/// must have quoted since the order activated.
#[test]
fn next_quote_waits_for_every_leg_of_a_combo() {
    let (gw, feed, clock) = gateway(
        "fed-nq-combo",
        r#"{"fill_on":"next_quote","expiry":{"settle":false}}"#,
    );
    let mut c = Conn::open(&gw, "reactor", None);
    mappings(&feed);
    let call2 = "SPY261005C00585000";
    let m = SymbolMappingMsg::new(
        5,
        T0 - S / 2,
        SType::Parent,
        "SPY.OPT",
        SType::RawSymbol,
        "SPY   261005C00585000",
        0,
        0,
    )
    .unwrap();
    feed.push(T0 - S / 2, RecordRef::from(&m)).unwrap();
    let all = |t: u64| {
        cbbo(&feed, 2, t, 92, 94); // PUT_A 570
        cbbo(&feed, 3, t, 50, 52); // PUT_B 568
        cbbo(&feed, 1, t, 120, 122); // CALL 580
        cbbo(&feed, 5, t, 60, 62); // 585
    };
    all(T0);
    clock.advance("reactor", 0, T0, WAIT).unwrap();
    // An iron condor for a credit of at least 0.90: natural 0.92 − 0.52 + 1.20 − 0.62 = 0.98.
    let condor = combo(
        "wse-ic",
        &[
            (PUT_A, 1, "sell", "open"),
            (PUT_B, 1, "buy", "open"),
            (CALL, 1, "sell", "open"),
            (call2, 1, "buy", "open"),
        ],
        1,
        "-0.90",
    );
    assert_eq!(c.submit("r1", &condor)["outcome"], "accepted");
    // A minute later only the short put quotes: no fill on the other legs' old quotes.
    cbbo(&feed, 2, T0 + 60 * S, 92, 94);
    clock.advance("reactor", 1, T0 + 60 * S, WAIT).unwrap();
    let (r, _) = c.req("order", "r2", r#""client_order_id":"wse-ic""#);
    assert_ne!(r["order"]["status"], "filled", "{r}");
    // Then every leg: it fills on those quotes.
    all(T0 + 120 * S);
    clock.advance("reactor", 1, T0 + 120 * S, WAIT).unwrap();
    let f = fill(&mut c, "wse-ic");
    assert_eq!(
        (f["fill"]["price"].as_str(), f["ts_broker"].as_u64()),
        (Some("-0.98"), Some(T0 + 120 * S))
    );
    gw.stop().unwrap();
}

/// Under report latency, queries answer at once by default (as a broker's REST
/// runs ahead of its stream); with `report_latency_applies_to = "all"` they show
/// the account as of the last released report.
#[test]
fn queries_in_the_report_latency_window() {
    for (applies, shown) in [("stream", true), ("all", false)] {
        let (gw, feed, clock) = gateway(
            &format!("fed-latency-query-{applies}"),
            &format!(
                r#"{{"report_latency_ms":2000,"report_latency_applies_to":"{applies}","expiry":{{"settle":false}}}}"#
            ),
        );
        let mut c = Conn::open(&gw, "reactor", None);
        mappings(&feed);
        cbbo(&feed, 1, T0, 120, 130);
        clock.advance("reactor", 0, T0, WAIT).unwrap();
        c.submit("r1", &buy("wse-q"));
        cbbo(&feed, 1, T0 + 60 * S, 121, 124);
        clock.advance("reactor", 1, T0 + 61 * S, WAIT).unwrap();
        // Filled at 10:01, reported at 10:01:02: at 10:01:01 the fill is in the window.
        let (p, _) = c.req("positions", "r2", "");
        let held = p["positions"].as_array().is_some_and(|a| !a.is_empty());
        assert_eq!(held, shown, "{applies}: {p}");
        clock.advance("reactor", 0, T0 + 62 * S, WAIT).unwrap();
        let (p, _) = c.req("positions", "r3", "");
        assert!(
            p["positions"].as_array().is_some_and(|a| !a.is_empty()),
            "{applies}: {p}"
        );
        gw.stop().unwrap();
    }
}

/// report_latency_ms: a fill happens at its time but reaches the client only once
/// the market time has passed that time plus the latency; its stamp is the fill's.
#[test]
fn reports_arrive_after_the_report_latency() {
    let (gw, feed, clock) = gateway(
        "fed-report-latency",
        r#"{"report_latency_ms":2000,"expiry":{"settle":false}}"#,
    );
    let mut c = Conn::open(&gw, "reactor", None);
    mappings(&feed);
    cbbo(&feed, 1, T0, 120, 130);
    clock.advance("reactor", 0, T0, WAIT).unwrap();
    c.submit("r1", &buy("wse-late"));
    cbbo(&feed, 1, T0 + 60 * S, 121, 124);
    let a = clock.advance("reactor", 1, T0 + 61 * S, WAIT).unwrap();
    // Every event up to the advance's last: no fill among them yet.
    let seen = c.until(|v| v["seq"].as_u64() == Some(a.last_seq));
    assert!(!seen.iter().any(|v| v["event"] == "fill"), "{seen:?}");
    clock.advance("reactor", 1, T0 + 62 * S, WAIT).unwrap();
    let f = fill(&mut c, "wse-late");
    assert_eq!(f["ts_broker"], T0 + 60 * S);
    assert_eq!(f["fill"]["price"], "1.24");
    gw.stop().unwrap();
}

/// The queue model: a buy resting at the bid fills once prints at its price have
/// traded the size shown ahead of it, at its limit.
#[test]
fn a_resting_order_fills_once_the_queue_ahead_has_traded() {
    let (gw, feed, clock) = gateway("fed-queue-at", r#"{"queue":{},"expiry":{"settle":false}}"#);
    let mut c = Conn::open(&gw, "reactor", None);
    mappings(&feed);
    cbbo(&feed, 3, T0, 50, 52);
    clock.advance("reactor", 0, T0, WAIT).unwrap();
    c.submit("r1", &single("wse-qat", PUT_B, "buy", "open", 1, "0.50"));
    // Ten shown at the bid ahead of it: six trade, then six more.
    print(&feed, 3, T0 + 60 * S, (50, 52), 50, 6);
    let a = clock.advance("reactor", 1, T0 + 60 * S, WAIT).unwrap();
    let seen = c.until(|v| v["seq"].as_u64() == Some(a.last_seq));
    assert!(!seen.iter().any(|v| v["event"] == "fill"), "{seen:?}");
    print(&feed, 3, T0 + 61 * S, (50, 52), 50, 6);
    clock.advance("reactor", 0, T0 + 61 * S, WAIT).unwrap();
    let f = fill(&mut c, "wse-qat");
    assert_eq!(f["fill"]["price"], "0.50", "{f}");
    assert_eq!(f["broker_raw"]["order"]["queue"][0]["ahead"], 0, "{f}");
    gw.stop().unwrap();
}

/// A print fills no more than it leaves past the queue; the rest stays queued.
#[test]
fn a_print_fills_only_its_size_past_the_queue() {
    let (gw, feed, clock) = gateway(
        "fed-queue-partial",
        r#"{"queue":{"ahead":"0.5"},"expiry":{"settle":false}}"#,
    );
    let mut c = Conn::open(&gw, "reactor", None);
    mappings(&feed);
    cbbo(&feed, 3, T0, 50, 52);
    clock.advance("reactor", 0, T0, WAIT).unwrap();
    c.submit("r1", &single("wse-qpart", PUT_B, "buy", "open", 5, "0.50"));
    // Five ahead: a print of seven leaves two.
    print(&feed, 3, T0 + 60 * S, (50, 52), 50, 7);
    clock.advance("reactor", 1, T0 + 60 * S, WAIT).unwrap();
    let f = c
        .until(|v| v["client_order_id"] == "wse-qpart" && v["event"] == "partial_fill")
        .pop()
        .unwrap();
    assert_eq!(f["filled_qty"], 2, "{f}");
    print(&feed, 3, T0 + 61 * S, (50, 52), 50, 3);
    clock.advance("reactor", 0, T0 + 61 * S, WAIT).unwrap();
    let f = fill(&mut c, "wse-qpart");
    assert_eq!(f["filled_qty"], 5, "{f}");
    gw.stop().unwrap();
}

/// With `fill_on_print = "through"`, prints at the price never fill it, nor does
/// one outside its own quote; one through it, inside its quote, does.
#[test]
fn through_only_needs_a_print_through_the_price() {
    let (gw, feed, clock) = gateway(
        "fed-queue-through",
        r#"{"queue":{"ahead":"0.5","fill_on_print":"through"},"expiry":{"settle":false}}"#,
    );
    let mut c = Conn::open(&gw, "reactor", None);
    mappings(&feed);
    cbbo(&feed, 3, T0, 50, 52);
    clock.advance("reactor", 0, T0, WAIT).unwrap();
    c.submit("r1", &single("wse-qth", PUT_B, "buy", "open", 1, "0.50"));
    print(&feed, 3, T0 + 60 * S, (50, 52), 50, 500);
    // Below its own quote: a complex order's leg, say.
    print(&feed, 3, T0 + 61 * S, (50, 52), 45, 500);
    let a = clock.advance("reactor", 1, T0 + 61 * S, WAIT).unwrap();
    let seen = c.until(|v| v["seq"].as_u64() == Some(a.last_seq));
    assert!(!seen.iter().any(|v| v["event"] == "fill"), "{seen:?}");
    print(&feed, 3, T0 + 62 * S, (49, 52), 49, 1);
    clock.advance("reactor", 0, T0 + 62 * S, WAIT).unwrap();
    assert_eq!(fill(&mut c, "wse-qth")["fill"]["price"], "0.50");
    gw.stop().unwrap();
}

/// A resting combo fills when every leg has cleared at its passive touch within
/// the window, at its implied prices, never worse than its limit; legs clearing further
/// apart don't fill it.
#[test]
fn a_combo_fills_when_its_legs_clear_within_the_window() {
    let (gw, feed, clock) = gateway(
        "fed-queue-combo",
        r#"{"queue":{},"expiry":{"settle":false}}"#,
    );
    let mut c = Conn::open(&gw, "reactor", None);
    mappings(&feed);
    cbbo(&feed, 2, T0, 92, 94);
    cbbo(&feed, 3, T0, 50, 52);
    clock.advance("reactor", 0, T0, WAIT).unwrap();
    // 0.46 credit, beyond the touch's 0.40: it rests. Its legs queue at the PUT_A
    // ask (0.94) and the PUT_B bid (0.50).
    let order = combo(
        "wse-qcb",
        &[(PUT_A, 1, "sell", "open"), (PUT_B, 1, "buy", "open")],
        1,
        "-0.46",
    );
    c.submit("r1", &order);
    print(&feed, 2, T0 + 60 * S, (92, 96), 95, 1);
    clock.advance("reactor", 1, T0 + 60 * S, WAIT).unwrap();
    // 61 s later: outside the window.
    print(&feed, 3, T0 + 121 * S, (48, 52), 48, 1);
    let a = clock.advance("reactor", 0, T0 + 121 * S, WAIT).unwrap();
    let seen = c.until(|v| v["seq"].as_u64() == Some(a.last_seq));
    assert!(!seen.iter().any(|v| v["event"] == "fill"), "{seen:?}");
    print(&feed, 2, T0 + 179 * S, (92, 96), 95, 1);
    clock.advance("reactor", 0, T0 + 179 * S, WAIT).unwrap();
    let f = fill(&mut c, "wse-qcb");
    let q = &f["broker_raw"]["order"]["queue"];
    // Implied 0.9558 and 0.4958, rounded against it to 0.95 and 0.50 (0.45),
    // then the last leg back to 0.49: the 0.46 limit.
    assert_eq!(q[0]["price"], "0.95", "{f}");
    assert_eq!(q[1]["price"], "0.49", "{f}");
    assert_eq!(q[0]["queue_price"], "0.94", "{f}");
    gw.stop().unwrap();
}

/// A combo leg's size from prints leaves the window print by print: 1-lots
/// trickling in over minutes never add up to fill the other leg's unit later.
#[test]
fn old_prints_leave_the_combo_window_one_by_one() {
    let (gw, feed, clock) = gateway(
        "fed-queue-roll",
        r#"{"queue":{},"expiry":{"settle":false}}"#,
    );
    let mut c = Conn::open(&gw, "reactor", None);
    mappings(&feed);
    cbbo(&feed, 2, T0, 92, 94);
    cbbo(&feed, 3, T0, 50, 52);
    clock.advance("reactor", 0, T0, WAIT).unwrap();
    let order = combo(
        "wse-qroll",
        &[(PUT_A, 1, "sell", "open"), (PUT_B, 1, "buy", "open")],
        2,
        "-0.46",
    );
    c.submit("r1", &order);
    let mut sent = 1;
    // One PUT_A contract through its ask every 50 s for five minutes.
    for k in 1..=6 {
        print(&feed, 2, T0 + k * 50 * S, (92, 96), 95, 1);
        clock
            .advance("reactor", sent, T0 + k * 50 * S, WAIT)
            .unwrap();
        sent = 0;
    }
    // Two PUT_B: only one PUT_A grant (at 300 s) is still within 60 s.
    print(&feed, 3, T0 + 320 * S, (48, 52), 48, 2);
    clock.advance("reactor", 0, T0 + 320 * S, WAIT).unwrap();
    let f = c
        .until(|v| v["client_order_id"] == "wse-qroll" && v["event"] == "partial_fill")
        .pop()
        .unwrap();
    assert_eq!(f["filled_qty"], 1, "{f}");
    gw.stop().unwrap();
}

/// A combo whose sold leg has an ask but no bid: refused by default; under
/// no_bid_combo_leg = "zero" that leg goes for nothing.
#[test]
fn a_sold_leg_without_a_bid() {
    for (setting, fills) in [("refuse", false), ("zero", true)] {
        let (gw, feed, clock) = gateway(
            &format!("fed-no-bid-{setting}"),
            &format!(r#"{{"no_bid_combo_leg":"{setting}","expiry":{{"settle":false}}}}"#),
        );
        let mut c = Conn::open(&gw, "reactor", None);
        mappings(&feed);
        cbbo(&feed, 2, T0, 1, 2);
        // PUT_B: offered at 0.01, no bid.
        let mut q = CbboMsg {
            hd: RecordHeader::new::<CbboMsg>(rtype::CBBO_1M, 30, 3, u64::MAX),
            ts_recv: T0,
            ..CbboMsg::default_for_schema(Schema::Cbbo1M)
        };
        q.levels[0].ask_px = px(1);
        q.levels[0].ask_sz = 9000;
        feed.push(T0, RecordRef::from(&q)).unwrap();
        clock.advance("reactor", 0, T0, WAIT).unwrap();
        // A put spread: buy PUT_A, sell PUT_B.
        let spread = combo(
            "wse-nobid",
            &[(PUT_A, 1, "buy", "open"), (PUT_B, 1, "sell", "open")],
            1,
            "1.01",
        );
        assert_eq!(c.submit("r1", &spread)["outcome"], "accepted");
        clock.advance("reactor", 1, T0 + 60 * S, WAIT).unwrap();
        if fills {
            // PUT_A at its 0.02 ask, PUT_B given away.
            let f = fill(&mut c, "wse-nobid");
            assert_eq!(f["fill"]["price"], "0.02");
            assert_eq!(
                f["broker_raw"]["order"]["no_bid_zero_legs"][0], PUT_B,
                "{f}"
            );
        } else {
            let (r, _) = c.req("order", "r2", r#""client_order_id":"wse-nobid""#);
            assert_ne!(r["order"]["status"], "filled", "{setting}: {r}");
        }
        gw.stop().unwrap();
    }
}

/// The rate limits run on the session's clock: a third open within a market
/// minute is refused, and accepted a market minute later, though only
/// milliseconds have passed.
#[test]
fn rate_limits_count_market_minutes() {
    let (sim, feed) = SimBroker::fed(
        r#"{"expiry":{"settle":false}}"#,
        r#"{"source":"session","run":"test"}"#,
    )
    .unwrap();
    let mut cfg = config(root("fed-rate-market"));
    cfg.profile.submits_per_min = 2;
    let gw = start(cfg, sim).unwrap();
    let clock = gw.clock();
    let mut c = Conn::open(&gw, "reactor", None);
    mappings(&feed);
    cbbo(&feed, 3, T0, 50, 52);
    clock.advance("reactor", 0, T0, WAIT).unwrap();
    let rest = |cid: &str| single(cid, PUT_B, "buy", "open", 1, "0.01");
    assert_eq!(c.submit("r1", &rest("wse-rate-1"))["outcome"], "accepted");
    assert_eq!(c.submit("r2", &rest("wse-rate-2"))["outcome"], "accepted");
    let r = c.submit("r3", &rest("wse-rate-3"));
    assert_eq!(r["detail"]["cap"], "submits_per_minute", "{r}");
    clock.advance("reactor", 3, T0 + 61 * S, WAIT).unwrap();
    assert_eq!(c.submit("r4", &rest("wse-rate-4"))["outcome"], "accepted");
    gw.stop().unwrap();
}
