//! `AlpacaPaper` through the real gateway against a loopback fake Alpaca (REST and
//! `trade_updates`), offline.
mod fake_alpaca;
mod support;

use fake_alpaca::{Act, Fake, FakeOrder, KEY, SECRET};
use order_gateway::trading::{Credentials, Endpoints};
use order_gateway::{AlpacaOptions, AlpacaPaper, Gateway, Profile, start};
use serde_json::Value;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use support::{CALL, Conn, PUT_A, PUT_B, buy, config, credit_spread, journal, root, single};

struct Rig {
    fake: Fake,
    gw: Gateway,
    state: PathBuf,
}

fn rig(name: &str) -> Rig {
    rig_with(name, |_| {})
}

/// A rig whose profile is the default with `max_open` 10, then `profile` applied.
fn rig_with(name: &str, profile: impl FnOnce(&mut Profile)) -> Rig {
    boot(Fake::start(), root(name), 1, profile)
}

/// Starts a gateway on `state` against `fake`, once the stream has listened `listening` times.
fn boot(fake: Fake, state: PathBuf, listening: usize, profile: impl FnOnce(&mut Profile)) -> Rig {
    boot_kept(fake, state, listening, profile, None)
}

/// As [`boot`], keeping an ended foreign order's fill record for `keep`.
fn boot_kept(
    fake: Fake,
    state: PathBuf,
    listening: usize,
    profile: impl FnOnce(&mut Profile),
    keep: Option<Duration>,
) -> Rig {
    let creds = Credentials::from_env_text(&format!(
        "export APCA_API_KEY_ID={KEY}\nAPCA_API_SECRET_KEY=\"{SECRET}\"\n"
    ))
    .unwrap();
    let opts = AlpacaOptions {
        timeout: Duration::from_secs(2),
        reconnect_min: Duration::from_millis(50),
        reconnect_max: Duration::from_millis(200),
    };
    let broker = AlpacaPaper::new(Endpoints::loopback(fake.addr).unwrap(), creds, opts);
    let mut cfg = config(state.clone());
    cfg.account = "alpaca-paper-test".into();
    cfg.profile.max_open = 10;
    if let Some(k) = keep {
        cfg.timing.foreign_keep = k;
    }
    profile(&mut cfg.profile);
    let gw = start(cfg, broker).unwrap();
    fake.wait_listening(listening);
    Rig { fake, gw, state }
}

/// Stops the gateway and starts another on the same state root and fake broker.
fn restart(r: Rig, listening: usize) -> Rig {
    let Rig { fake, gw, state } = r;
    gw.stop().unwrap();
    boot(fake, state, listening, |_| {})
}

/// A client that has seen the startup resync and the resync after the first `listening`.
fn ready(gw: &Gateway, name: &str) -> Conn {
    let mut c = Conn::open(gw, name, Some(0));
    let n = std::cell::Cell::new(0);
    c.until(|v| {
        if v["kind"] == "status" && v.get("resync").is_some() {
            n.set(n.get() + 1);
        }
        n.get() == 2
    });
    c
}

fn posts(fake: &Fake) -> Vec<String> {
    let st = fake.lock();
    st.log
        .iter()
        .filter(|(m, p, _)| m == "POST" && p == "/v2/orders")
        .map(|(_, _, b)| b.clone())
        .collect()
}

#[test]
fn exact_bodies_and_every_stream_event() {
    let r = rig("alpaca-events");
    let mut c = ready(&r.gw, "reactor");
    let resp = c.submit("r1", &single("wse-1", CALL, "buy", "open", 2, "1.25"));
    assert_eq!(resp["outcome"], "accepted", "{resp}");
    assert_eq!(resp["status"], "accepted");
    let line = c.lines.last().unwrap();
    assert!(
        line.contains(r#""x_unknown_field":{"kept":1.50}"#),
        "{line}"
    );
    let resp = c.submit("r2", &credit_spread("wse-2", 3));
    assert_eq!(resp["outcome"], "accepted", "{resp}");
    assert_eq!(
        posts(&r.fake),
        [
            r#"{"symbol":"SPY261005C00580000","qty":"2","side":"buy","type":"limit","limit_price":"1.25","time_in_force":"day","client_order_id":"wse-1","position_intent":"buy_to_open"}"#,
            r#"{"order_class":"mleg","qty":"3","type":"limit","time_in_force":"day","limit_price":"-0.40","client_order_id":"wse-2","legs":[{"symbol":"SPY261005P00570000","ratio_qty":"1","side":"sell","position_intent":"sell_to_open"},{"symbol":"SPY261005P00568000","ratio_qty":"1","side":"buy","position_intent":"buy_to_open"}]}"#,
        ]
    );
    for (event, status) in [
        ("pending_new", "pending_new"),
        ("accepted", "accepted"),
        ("new", "new"),
    ] {
        r.fake.event("wse-1", event, status);
        let v = c.order_event("wse-1", event);
        assert_eq!(v["status"], status);
        assert_eq!(v["source"], "stream");
        assert_eq!(v["ts_broker"], 1_791_208_801_123_456_789_u64);
    }
    r.fake.fill("wse-1", 1, "1.24");
    let v = c.order_event("wse-1", "partial_fill");
    assert_eq!(v["fill"]["qty"], 1);
    assert_eq!(v["fill"]["price"], "1.24");
    assert!(
        v["fill"]["execution_id"]
            .as_str()
            .unwrap()
            .starts_with("e000")
    );
    assert_eq!(
        (v["filled_qty"].as_u64(), v["position_qty"].as_i64()),
        (Some(1), Some(1))
    );
    r.fake.fill("wse-1", 1, "1.23");
    let v = c.order_event("wse-1", "fill");
    assert_eq!(
        (v["status"].as_str(), v["filled_qty"].as_u64()),
        (Some("filled"), Some(2))
    );
    assert_eq!(v["broker_raw"]["order"]["filled_avg_price"], "1.23");

    // MLEG: per-leg executions from `data.legs`, the net price at the top.
    r.fake.mleg_fill("wse-2", 1, "-0.41", &["0.93", "0.52"]);
    let v = c.order_event("wse-2", "partial_fill");
    assert_eq!(v["fill"]["price"], "-0.41");
    assert_eq!(v["fill"]["execution_id"], Value::Null);
    let legs = v["legs"].as_array().unwrap();
    assert_eq!(legs[0]["symbol"], PUT_A);
    assert_eq!(legs[0]["filled_qty"], 1);
    assert_eq!(legs[0]["fill"]["price"], "0.93");
    assert_eq!(legs[1]["symbol"], PUT_B);
    assert_eq!(legs[1]["fill"]["qty"], 1);
    assert!(legs[1]["fill"]["execution_id"].is_string());
    assert_eq!(v["broker_raw"]["position_qtys"][PUT_A], "-1");
    r.fake.mleg_fill("wse-2", 2, "-0.40", &["0.92", "0.52"]);
    let v = c.order_event("wse-2", "fill");
    assert_eq!(v["legs"][0]["filled_qty"], 3);

    let terminal = [
        ("canceled", "canceled", "canceled"),
        ("expired", "expired", "expired"),
        ("rejected", "rejected", "rejected"),
        ("replaced", "replaced", "replaced"),
        ("done_for_day", "done_for_day", "done_for_day"),
        ("order_cancel_rejected", "new", "cancel_rejected"),
        ("pending_replace", "pending_replace", "other"),
    ];
    for (n, (event, status, normalized)) in terminal.iter().enumerate() {
        let cid = format!("wse-t{n}");
        let resp = c.submit(&format!("t{n}"), &buy(&cid));
        assert_eq!(resp["outcome"], "accepted", "{resp}");
        r.fake.event(&cid, event, status);
        let v = c.order_event(&cid, normalized);
        assert_eq!(
            (v["broker_event"].as_str(), v["status"].as_str()),
            (Some(*event), Some(*status))
        );
    }
}

#[test]
fn broker_refusals_map_status_and_code() {
    let r = rig("alpaca-rejects");
    let mut c = ready(&r.gw, "reactor");
    let bp = include_str!("fixtures/alpaca/error_buying_power.json");
    r.fake.reply("POST", "/v2/orders", 403, bp);
    let v = c.submit("r1", &buy("wse-a"));
    assert_eq!(
        (v["outcome"].as_str(), v["http_status"].as_u64()),
        (Some("rejected"), Some(403))
    );
    assert_eq!(
        (v["code"].as_str(), v["message"].as_str()),
        (Some("40310000"), Some("insufficient buying power"))
    );
    assert_eq!(v["broker_raw"]["code"], 40_310_000);
    let lvl = include_str!("fixtures/alpaca/error_options_level.json");
    r.fake.reply("POST", "/v2/orders", 403, lvl);
    let v = c.submit("r2", &buy("wse-b"));
    assert!(v["message"].as_str().unwrap().contains("not eligible"));
    // An id the broker already holds (placed outside the gateway): Alpaca's 422.
    let mut st = r.fake.lock();
    let dashboard = FakeOrder {
        id: "00000000-0000-4000-8000-0000000000aa".into(),
        cid: "wse-c".into(),
        single: None,
        legs: Vec::new(),
        qty: 1,
        filled: 0,
        avg: None,
        limit: "1.00".into(),
        status: "canceled".into(),
        tif: String::new(),
    };
    st.orders.push(dashboard);
    drop(st);
    let v = c.submit("r3", &buy("wse-c"));
    assert_eq!(
        (v["http_status"].as_u64(), v["code"].as_str()),
        (Some(422), Some("40010001"))
    );
    // A gateway-known id is never sent again.
    let v = c.submit("r4", &buy("wse-c"));
    assert_eq!(v["outcome"], "duplicate");
    assert_eq!(posts(&r.fake).len(), 3);
    r.fake
        .reply("POST", "/v2/orders", 502, "<html>bad gateway</html>");
    let v = c.submit("r5", &buy("wse-d"));
    assert_eq!(
        (v["outcome"].as_str(), v["reason"].as_str()),
        (Some("unknown"), Some("http_5xx"))
    );
}

#[test]
fn lost_response_resolves_by_lookup_without_resend() {
    let r = rig("alpaca-lost");
    let mut c = ready(&r.gw, "reactor");
    r.fake.next("POST", "/v2/orders", Act::Lost);
    let v = c.submit("r1", &buy("wse-lost"));
    assert_eq!(
        (v["outcome"].as_str(), v["reason"].as_str()),
        (Some("unknown"), Some("connection"))
    );
    let res = c.event("resolution", |v| v["client_order_id"] == "wse-lost");
    assert_eq!(res["outcome"], "found");
    let v = c.order_event("wse-lost", "accepted");
    assert_eq!(v["source"], "rest_reconcile");
    assert_eq!(posts(&r.fake).len(), 1);
    assert!(r.fake.count("GET", "/v2/orders:by_client_order_id") >= 1);
}

#[test]
fn a_failed_lookup_leaves_the_order_unknown_not_absent() {
    let r = rig("alpaca-lookup-error");
    let mut c = ready(&r.gw, "reactor");
    // The broker is failing, which is not the same as saying the order is missing:
    // every lookup gets a 500, so the order is unresolved, not absent. The submit's
    // reply is a 502 and the broker never stores the order, so no lookup can find it.
    // That keeps the test about failed lookups and nothing else.
    for _ in 0..3 {
        r.fake.next(
            "GET",
            "/v2/orders:by_client_order_id",
            Act::Respond(500, Vec::new(), "{}".into()),
        );
    }
    r.fake
        .reply("POST", "/v2/orders", 502, "<html>bad gateway</html>");
    let v = c.submit("r1", &buy("wse-err"));
    assert_eq!(
        (v["outcome"].as_str(), v["reason"].as_str()),
        (Some("unknown"), Some("http_5xx"))
    );
    let res = c.event("resolution", |v| v["client_order_id"] == "wse-err");
    assert_eq!(
        (res["outcome"].as_str(), res["window_ms"].as_u64()),
        (Some("unresolved"), Some(80))
    );
    assert!(r.fake.count("GET", "/v2/orders:by_client_order_id") >= 3);
    // Still unknown and open. The order reaches the broker later, and its stream
    // event settles it.
    r.fake.appear(&format!(
        r#"{{"client_order_id":"wse-err","symbol":"{CALL}","qty":"1","side":"buy","limit_price":"1.25","position_intent":"buy_to_open"}}"#
    ));
    r.fake.event("wse-err", "new", "new");
    let o = c.order_event("wse-err", "new");
    assert_eq!(o["source"], "stream");
    assert_eq!(posts(&r.fake).len(), 1, "never resent");
}

/// A submit whose POST gets a 502 and never reaches the fake, so every lookup is a
/// 404: the order ends up absent, not unknown.
fn absent_submit(r: &Rig, c: &mut Conn, cid: &str) {
    r.fake
        .reply("POST", "/v2/orders", 502, "<html>bad gateway</html>");
    let v = c.submit("r-abs", &buy(cid));
    assert_eq!(v["outcome"], "unknown");
    let res = c.event("resolution", |v| v["client_order_id"] == cid);
    assert_eq!(res["outcome"], "absent_after");
}

#[test]
fn an_absent_order_still_counts_toward_max_open() {
    let r = rig_with("alpaca-absent-cap", |p| p.max_open = 1);
    let mut c = ready(&r.gw, "reactor");
    absent_submit(&r, &mut c, "wse-abs");
    let (q, _) = c.req("open_orders", "q1", "");
    assert_eq!(q["unresolved"], 1);
    let v = c.submit("r2", &buy("wse-next"));
    assert_eq!(
        (v["reason"].as_str(), v["detail"]["cap"].as_str()),
        (Some("cap"), Some("open_orders"))
    );
    // Nothing refused was sent, and a close on its leg waits for the absent order.
    assert_eq!(posts(&r.fake).len(), 1);
    let v = c.submit("r3", &single("wse-close", CALL, "sell", "close", 1, "1.00"));
    assert_eq!(v["reason"], "leg_pending");
    assert_eq!(v["detail"]["symbols"][0], CALL);
}

#[test]
fn a_stream_event_settles_an_absent_order() {
    let r = rig_with("alpaca-absent-settle", |p| p.max_open = 1);
    let mut c = ready(&r.gw, "reactor");
    absent_submit(&r, &mut c, "wse-abs");
    let v = c.submit("r2", &buy("wse-next"));
    assert_eq!(v["detail"]["cap"], "open_orders");
    // The order turns up on the stream and is cancelled: it no longer counts.
    r.fake.appear(&format!(
        r#"{{"client_order_id":"wse-abs","symbol":"{CALL}","qty":"1","side":"buy","limit_price":"1.25","position_intent":"buy_to_open"}}"#
    ));
    r.fake.event("wse-abs", "canceled", "canceled");
    c.order_event("wse-abs", "canceled");
    let (q, _) = c.req("open_orders", "q1", "");
    assert_eq!(q["unresolved"], 0);
    assert_eq!(c.submit("r3", &buy("wse-next"))["outcome"], "accepted");
}

/// Polls `open_orders` until nothing is unresolved, with no stream event sent.
fn wait_resolved(c: &mut Conn) -> Value {
    let t = Instant::now();
    let mut n = 0;
    loop {
        n += 1;
        let (q, _) = c.req("open_orders", &format!("w{n}"), "");
        if q["unresolved"] == 0 {
            return q;
        }
        assert!(
            t.elapsed() < Duration::from_secs(5),
            "still unresolved: {q}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[test]
fn a_periodic_check_settles_an_absent_order_the_broker_now_shows() {
    let r = rig_with("alpaca-absent-periodic", |p| p.max_open = 1);
    let mut c = ready(&r.gw, "reactor");
    absent_submit(&r, &mut c, "wse-pc");
    r.fake.appear(&format!(
        r#"{{"client_order_id":"wse-pc","symbol":"{CALL}","qty":"1","side":"buy","limit_price":"1.25","position_intent":"buy_to_open"}}"#
    ));
    // No stream event: the poll's lookup finds it. It is working, so it still counts.
    wait_resolved(&mut c);
    assert_eq!(
        c.submit("r2", &buy("wse-next"))["detail"]["cap"],
        "open_orders"
    );
}

#[test]
fn a_404_never_clears_an_absent_order_however_many_checks_return_it() {
    let r = rig_with("alpaca-absent-404", |p| p.max_open = 1);
    let mut c = ready(&r.gw, "reactor");
    absent_submit(&r, &mut c, "wse-404");
    // Let the poll's lookup run several times, each one a 404 from the broker.
    let before = r.fake.count("GET", "/v2/orders:by_client_order_id");
    let t = Instant::now();
    while r.fake.count("GET", "/v2/orders:by_client_order_id") < before + 5 {
        assert!(
            t.elapsed() < Duration::from_secs(5),
            "the poll never looked again"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    // Still counted: it uses the open slot, and a close on its leg waits for it.
    let (q, _) = c.req("open_orders", "q1", "");
    assert_eq!(q["unresolved"], 1);
    let v = c.submit("r2", &buy("wse-next"));
    assert_eq!(v["detail"]["cap"], "open_orders");
    let v = c.submit(
        "r3",
        &single("wse-404-close", CALL, "sell", "close", 1, "1.00"),
    );
    assert_eq!(v["reason"], "leg_pending");
}

/// A submit whose POST gets a 502 and whose every lookup fails with a 500: the order is
/// unknown after its last lookup, and nothing is scheduled for it any more.
fn unknown_submit_with_failed_lookups(r: &Rig, c: &mut Conn, cid: &str) {
    for _ in 0..3 {
        r.fake.next(
            "GET",
            "/v2/orders:by_client_order_id",
            Act::Respond(500, Vec::new(), "{}".into()),
        );
    }
    r.fake
        .reply("POST", "/v2/orders", 502, "<html>bad gateway</html>");
    let v = c.submit("r-unk", &buy(cid));
    assert_eq!(v["outcome"], "unknown");
    let res = c.event("resolution", |v| v["client_order_id"] == cid);
    assert_eq!(res["outcome"], "unresolved");
}

/// The broker now has the order, in `status` with `filled` executed, with no stream event.
fn broker_shows(r: &Rig, cid: &str, status: &str, filled: u64) {
    r.fake.appear(&format!(
        r#"{{"client_order_id":"{cid}","symbol":"{CALL}","qty":"1","side":"buy","limit_price":"1.25","position_intent":"buy_to_open"}}"#
    ));
    let mut st = r.fake.lock();
    let o = st.orders.iter_mut().find(|o| o.cid == cid).unwrap();
    o.status = status.into();
    o.filled = filled;
}

/// An unknown order whose lookups all failed settles from the broker's terminal state
/// found by the poll: it stops counting, and its leg closes.
fn unknown_order_settles_on_a_terminal_state(status: &str, filled: u64) {
    let name = format!("alpaca-unk-{status}");
    let r = rig_with(&name, |p| p.max_open = 1);
    let mut c = ready(&r.gw, "reactor");
    r.fake.hold(&[(CALL, 1)]);
    unknown_submit_with_failed_lookups(&r, &mut c, "wse-unk");
    assert_eq!(
        c.submit("r2", &buy("wse-next"))["detail"]["cap"],
        "open_orders"
    );
    let v = c.submit(
        "r3",
        &single("wse-u-close", CALL, "sell", "close", 1, "1.00"),
    );
    assert_eq!(v["reason"], "leg_pending");
    // Only the poll's lookup can settle it: no stream event is sent.
    broker_shows(&r, "wse-unk", status, filled);
    let o = c.order_event("wse-unk", normalized(status));
    assert_eq!(o["source"], "rest_reconcile");
    assert_eq!(o["status"], status);
    let (q, _) = c.req("open_orders", "q1", "");
    assert_eq!(q["unresolved"], 0);
    // Its slot is free for an open on another leg, and its leg accepts a close.
    let v = c.submit("r4", &single("wse-put", PUT_A, "buy", "open", 1, "1.00"));
    assert_eq!(v["outcome"], "accepted", "{v}");
    let v = c.submit(
        "r5",
        &single("wse-u-close2", CALL, "sell", "close", 1, "1.00"),
    );
    assert_eq!(v["outcome"], "accepted", "{v}");
}

/// The normalized `order` event a reconciled status is journaled as.
fn normalized(status: &str) -> &'static str {
    match status {
        "filled" => "fill",
        "canceled" => "canceled",
        "new" => "new",
        other => panic!("no event for {other}"),
    }
}

#[test]
fn an_unknown_order_the_broker_shows_filled_settles_and_frees_its_slot() {
    unknown_order_settles_on_a_terminal_state("filled", 1);
}

#[test]
fn an_unknown_order_the_broker_shows_canceled_settles_and_frees_its_slot() {
    unknown_order_settles_on_a_terminal_state("canceled", 0);
}

#[test]
fn a_404_alone_leaves_an_unknown_order_open_and_counted() {
    let r = rig_with("alpaca-unk-404", |p| p.max_open = 1);
    let mut c = ready(&r.gw, "reactor");
    unknown_submit_with_failed_lookups(&r, &mut c, "wse-u404");
    // The poll looks it up again, and the broker answers 404 each time.
    let before = r.fake.count("GET", "/v2/orders:by_client_order_id");
    let t = Instant::now();
    while r.fake.count("GET", "/v2/orders:by_client_order_id") < before + 5 {
        assert!(
            t.elapsed() < Duration::from_secs(5),
            "the poll never looked again"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    let (q, _) = c.req("open_orders", "q1", "");
    assert_eq!(q["unresolved"], 1);
    assert_eq!(
        c.submit("r2", &buy("wse-next"))["detail"]["cap"],
        "open_orders"
    );
    let v = c.submit(
        "r3",
        &single("wse-u404-close", CALL, "sell", "close", 1, "1.00"),
    );
    assert_eq!(v["reason"], "leg_pending");
}

#[test]
fn a_non_terminal_status_makes_an_unknown_order_an_ordinary_open_order() {
    let r = rig_with("alpaca-unk-new", |p| p.max_open = 1);
    let mut c = ready(&r.gw, "reactor");
    unknown_submit_with_failed_lookups(&r, &mut c, "wse-unew");
    broker_shows(&r, "wse-unew", "new", 0);
    let o = c.order_event("wse-unew", "new");
    assert_eq!(o["source"], "rest_reconcile");
    // Open, not unresolved: it counts toward the cap and blocks its leg's close.
    let (q, _) = c.req("open_orders", "q1", "");
    assert_eq!(q["unresolved"], 0);
    assert_eq!(
        c.submit("r2", &buy("wse-next"))["detail"]["cap"],
        "open_orders"
    );
    let v = c.submit(
        "r3",
        &single("wse-un-close", CALL, "sell", "close", 1, "1.00"),
    );
    assert_eq!(v["reason"], "leg_pending");
}

#[test]
fn a_close_waits_for_pending_orders_on_its_leg_and_opens_do_not() {
    let r = rig("alpaca-leg-guard");
    let mut c = ready(&r.gw, "reactor");
    r.fake.hold(&[(PUT_A, 1)]);
    absent_submit(&r, &mut c, "wse-p1");
    // The close is on the pending order's leg: refused whole, and not sent.
    let v = c.submit("r2", &single("wse-p2", CALL, "sell", "close", 1, "1.00"));
    assert_eq!(v["reason"], "leg_pending");
    assert_eq!(v["detail"]["symbols"][0], CALL);
    // A close on a leg with no pending order is not held by the guard.
    let v = c.submit("r3", &single("wse-p3", PUT_A, "sell", "close", 1, "1.00"));
    assert_eq!(v["outcome"], "accepted");
    // An open on the pending leg is not held by the guard either.
    assert_eq!(c.submit("r4", &buy("wse-p4"))["outcome"], "accepted");
    assert_eq!(posts(&r.fake).len(), 3, "the refused close was never sent");
}

#[test]
fn a_close_on_a_clear_leg_passes_the_day_cap_but_not_a_working_order_on_its_leg() {
    let r = rig_with("alpaca-leg-day", |p| p.submits_per_day = 1);
    let mut c = ready(&r.gw, "reactor");
    r.fake.hold(&[(PUT_A, 1)]);
    assert_eq!(c.submit("d1", &buy("wse-d1"))["outcome"], "accepted");
    // The day's one open is used up, but PUT_A has no pending order.
    let v = c.submit("d2", &single("wse-d2", PUT_A, "sell", "close", 1, "1.00"));
    assert_eq!(v["outcome"], "accepted");
    // The open on CALL is still working, so a close on CALL waits for it.
    let v = c.submit("d3", &single("wse-d3", CALL, "sell", "close", 1, "1.00"));
    assert_eq!(v["reason"], "leg_pending");
}

#[test]
fn rate_limits_back_off_and_never_resend() {
    let r = rig("alpaca-429");
    let mut c = ready(&r.gw, "reactor");
    let reset = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 1;
    let headers = vec![
        ("Retry-After".into(), "1".into()),
        ("X-RateLimit-Limit".into(), "200".into()),
        ("X-RateLimit-Remaining".into(), "0".into()),
        ("X-RateLimit-Reset".into(), reset.to_string()),
    ];
    let body = include_str!("fixtures/alpaca/error_rate_limit.json").to_owned();
    r.fake.next(
        "GET",
        "/v2/positions",
        Act::Respond(429, headers.clone(), body.clone()),
    );
    let (v, _) = c.req("positions", "q1", "");
    assert_eq!(
        (v["code"].as_str(), v["detail"]["http_status"].as_u64()),
        (Some("broker_error"), Some(429))
    );
    let (v, _) = c.req("positions", "q2", "");
    assert_eq!(v["code"], "throttled");
    std::thread::sleep(Duration::from_millis(2100));
    r.fake
        .next("POST", "/v2/orders", Act::Respond(429, headers, body));
    let v = c.submit("r1", &buy("wse-r1"));
    assert_eq!(
        (v["outcome"].as_str(), v["reason"].as_str()),
        (Some("unknown"), Some("http_429_after_send"))
    );
    let v = c.submit("r2", &buy("wse-r2"));
    assert_eq!(
        (v["outcome"].as_str(), v["reason"].as_str()),
        (Some("not_sent"), Some("throttled"))
    );
    assert_eq!(posts(&r.fake).len(), 1);
}

#[test]
fn stream_loss_degrades_refuses_and_resyncs_on_reconnect() {
    let r = rig("alpaca-stream");
    let mut c = ready(&r.gw, "reactor");
    assert_eq!(c.submit("r1", &buy("wse-s"))["outcome"], "accepted");
    {
        let mut st = r.fake.lock();
        st.auth_ok = false;
        st.ws_drop = true;
    }
    let v = c.event("status", |v| v["state"] == "degraded");
    assert!(v["broker_stream"]["down"]["since"].is_u64(), "{v}");
    let v = c.submit("r2", &buy("wse-s2"));
    assert_eq!(
        (v["outcome"].as_str(), v["reason"].as_str()),
        (Some("not_sent"), Some("not_ready"))
    );
    let refused = r.fake.lock().ws_auth.len();
    let until = Instant::now() + Duration::from_secs(5);
    while r.fake.lock().ws_auth.len() < refused + 2 {
        assert!(Instant::now() < until, "no reconnect attempts");
        std::thread::sleep(Duration::from_millis(10));
    }
    {
        // Filled while the stream was down: only REST resync can report it.
        let mut st = r.fake.lock();
        let o = st.orders.iter_mut().find(|o| o.cid == "wse-s").unwrap();
        (o.filled, o.avg, o.status) = (1, Some("1.20".into()), "filled".into());
        st.auth_ok = true;
    }
    let v = c.order_event("wse-s", "fill");
    assert_eq!(
        (v["source"].as_str(), v["filled_qty"].as_u64()),
        (Some("rest_reconcile"), Some(1))
    );
    assert!(v.get("fill").is_none(), "no fabricated execution");
    let v = c.event("status", |v| v.get("resync").is_some());
    assert_eq!(
        (v["state"].as_str(), v["broker_stream"].as_str()),
        (Some("ready"), Some("up"))
    );
    assert_eq!(c.submit("r3", &buy("wse-s3"))["outcome"], "accepted");
}

#[test]
fn queries_cancel_and_kill_pass_through_without_credentials_in_output() {
    let r = rig("alpaca-queries");
    let mut c = ready(&r.gw, "reactor");
    let mut ops = ready(&r.gw, "ops");
    let (v, _) = c.req("account", "q1", "");
    assert_eq!(v["account"]["options_trading_level"], 3);
    assert_eq!(v["broker_raw"]["account_number"], "PA0000000000");
    assert!(v["ts_gateway"].is_u64());
    let (v, _) = c.req("positions", "q2", "");
    assert_eq!(v["positions"][0]["qty"], -2);
    let (v, _) = c.req("clock", "q3", "");
    assert_eq!(v["clock"]["ts"], support::T0);
    assert_eq!(v["clock"]["is_open"], true);
    let (v, _) = c.req(
        "calendar",
        "q4",
        r#""start":"2026-10-05","end":"2026-10-05""#,
    );
    assert_eq!(v["days"][0]["close"], "16:00");
    let (v, _) = c.req("activities", "q5", r#""types":["FILL","OPEXP"]"#);
    assert_eq!(v["items"].as_array().unwrap().len(), 2);
    assert_eq!(v["items"][1]["activity_type"], "OPEXP");
    assert_eq!(v["next_page_token"], Value::Null);
    assert!(
        r.fake
            .lock()
            .log
            .iter()
            .any(|(_, p, _)| p.contains("activity_types=FILL%2COPEXP"))
    );

    assert_eq!(c.submit("r1", &buy("wse-q1"))["outcome"], "accepted");
    let (v, _) = c.req("order", "q6", r#""client_order_id":"wse-q1""#);
    let bid = v["order"]["broker_order_id"].as_str().unwrap().to_owned();
    assert!(v["order"]["intent_sha256"].is_string());
    let (v, _) = c.req("order", "q7", &format!(r#""broker_order_id":"{bid}""#));
    assert_eq!(v["order"]["client_order_id"], "wse-q1");
    let (v, _) = c.req("order", "q8", r#""client_order_id":"wse-none""#);
    assert_eq!(v["order"], Value::Null);
    let (v, _) = c.req("open_orders", "q9", "");
    assert_eq!(v["orders"][0]["client_order_id"], "wse-q1");
    let (v, _) = c.req("cancel", "c1", r#""client_order_id":"wse-q1""#);
    assert_eq!(v["outcome"], "accepted", "{v}");
    assert!(r.fake.count("DELETE", &format!("/v2/orders/{bid}")) == 1);
    r.fake.event("wse-q1", "canceled", "canceled");
    c.order_event("wse-q1", "canceled");
    let (v, _) = c.req("cancel", "c2", r#""client_order_id":"wse-q1""#);
    assert_eq!(
        (v["outcome"].as_str(), v["http_status"].as_u64()),
        (Some("rejected"), Some(422))
    );

    assert_eq!(c.submit("r2", &buy("wse-q2"))["outcome"], "accepted");
    let (v, _) = ops.req("kill", "k1", r#""level":"halt_all","reason":"test""#);
    assert_eq!(v["state"], "killed");
    assert_eq!(r.fake.count("DELETE", "/v2/orders"), 3);
    assert_eq!(r.fake.lock().orders[1].status, "canceled");

    assert!(r.fake.lock().keys_seen.iter().all(|k| k == KEY));
    let mut text = journal(&r.state).join("\n");
    for conn in [&c, &ops] {
        text += &conn.lines.join("\n");
    }
    assert!(!text.contains(KEY) && !text.contains(SECRET));
}

#[test]
fn cli_refuses_unsafe_or_incomplete_credentials_without_printing_values() {
    use std::os::unix::fs::PermissionsExt;
    let dir = root("alpaca-cli");
    let run = |extra: &[&str]| {
        let state = dir.join("state");
        let mut args = vec!["--account", "a", "--listen", "127.0.0.1:0"];
        args.extend([
            "--state",
            state.to_str().unwrap(),
            "--broker",
            "alpaca-paper",
        ]);
        args.extend(extra);
        let out = std::process::Command::new(env!("CARGO_BIN_EXE_order-gateway"))
            .args(&args)
            .output()
            .unwrap();
        (
            out.status.code(),
            String::from_utf8_lossy(&out.stderr).into_owned(),
        )
    };
    let (code, err) = run(&[]);
    assert_eq!(code, Some(2), "{err}");
    assert!(err.contains("--credentials-file"));
    let file = dir.join("alpaca-paper-9.env");
    std::fs::write(
        &file,
        format!("ALPACA_API_KEY_ID={KEY}\nALPACA_API_SECRET_KEY={SECRET}\n"),
    )
    .unwrap();
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644)).unwrap();
    let path = file.to_str().unwrap();
    let (code, err) = run(&["--credentials-file", path]);
    assert_eq!(code, Some(2));
    assert!(err.contains("mode-600") && !err.contains(SECRET), "{err}");
    std::fs::write(&file, format!("ALPACA_API_KEY_ID={KEY}\n")).unwrap();
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).unwrap();
    let (code, err) = run(&["--credentials-file", path]);
    assert_eq!(code, Some(2));
    assert!(
        err.contains("APCA_API_SECRET_KEY") && !err.contains(KEY),
        "{err}"
    );
    let (code, _) = run(&["--credentials-file", path, "--sim-script", path]);
    assert_eq!(code, Some(2));
}

#[test]
fn flatten_sends_alpacas_liquidate_all_once() {
    let r = rig("alpaca-flatten");
    let mut c = ready(&r.gw, "reactor");
    let mut ops = ready(&r.gw, "ops");
    assert_eq!(
        c.req("flatten", "f0", r#""reason":"test""#).0["code"],
        "forbidden"
    );
    let (v, _) = ops.req("flatten", "f1", r#""reason":"test""#);
    assert_eq!(
        (v["outcome"].as_str(), v["state"].as_str()),
        (Some("ok"), Some("killed"))
    );
    assert_eq!(v["positions"][0]["symbol"], "SPY261016P00560000");
    assert_eq!(v["positions"][0]["status"], 200);
    assert_eq!(v["positions"][0]["broker_raw"]["type"], "market");
    let liquidations: Vec<String> = r
        .fake
        .lock()
        .log
        .iter()
        .filter(|(m, p, _)| m == "DELETE" && p.starts_with("/v2/positions"))
        .map(|(_, p, _)| p.clone())
        .collect();
    assert_eq!(liquidations, ["/v2/positions?cancel_orders=true"]);
    // Opens are refused afterwards.
    assert_eq!(c.submit("r1", &support::buy("wse-1"))["reason"], "killed");
}

fn stock_close(cid: &str, symbol: &str, qty: u32) -> String {
    format!(
        r#"{{"client_order_id":"{cid}","shape":"single","instrument":{{"class":"equity","symbol":"{symbol}"}},"side":"sell","effect":"close","qty":{qty},"type":"limit","limit_price":"220.00","time_in_force":"day"}}"#
    )
}

#[test]
fn a_close_is_sent_only_up_to_the_position_the_broker_holds() {
    let r = rig_with("alpaca-close-held", |p| {
        p.equities = vec!["AAPL".into(), "KO".into(), "BRK.B".into()];
    });
    let mut c = ready(&r.gw, "reactor");
    r.fake.hold(&[("AAPL", 10), ("KO", 10)]);
    // A close equal to the held long is sent.
    let v = c.submit("h1", &stock_close("wse-h1", "AAPL", 10));
    assert_eq!(v["outcome"], "accepted");
    assert_eq!(posts(&r.fake).len(), 1);
    // A close larger than the held long is refused whole, and never sent.
    let v = c.submit("h2", &stock_close("wse-h2", "KO", 11));
    assert_eq!(
        (
            v["reason"].as_str(),
            v["detail"]["symbol"].as_str(),
            v["detail"]["requested_qty"].as_u64(),
            v["detail"]["held_qty"].as_i64(),
        ),
        (
            Some("close_exceeds_position"),
            Some("KO"),
            Some(11),
            Some(10)
        )
    );
    assert_eq!(posts(&r.fake).len(), 1, "the oversized close was not sent");
    // A close on a flat symbol is refused as well.
    let v = c.submit("h3", &stock_close("wse-h3", "BRK.B", 1));
    assert_eq!(
        (v["reason"].as_str(), v["detail"]["held_qty"].as_i64()),
        (Some("close_exceeds_position"), Some(0))
    );
    // A position that cannot be read refuses the close (fail closed), and it is not sent.
    r.fake.next(
        "GET",
        "/v2/positions",
        Act::Respond(500, Vec::new(), r#"{"message":"unavailable"}"#.into()),
    );
    let v = c.submit("h4", &stock_close("wse-h4", "KO", 1));
    assert_eq!(v["reason"], "position_unknown");
    assert_eq!(v["detail"]["symbol"], "KO");
    assert_eq!(posts(&r.fake).len(), 1, "the unreadable close was not sent");
}

#[test]
fn an_option_close_is_checked_against_the_contracts_held_on_its_leg() {
    let r = rig("alpaca-close-option");
    let mut c = ready(&r.gw, "reactor");
    r.fake.hold(&[(CALL, 1)]);
    let v = c.submit("o1", &single("wse-o1", CALL, "sell", "close", 2, "1.00"));
    assert_eq!(
        (
            v["reason"].as_str(),
            v["detail"]["requested_qty"].as_u64(),
            v["detail"]["held_qty"].as_i64(),
        ),
        (Some("close_exceeds_position"), Some(2), Some(1))
    );
    assert_eq!(posts(&r.fake).len(), 0, "the oversized close was not sent");
    let v = c.submit("o2", &single("wse-o2", CALL, "sell", "close", 1, "1.00"));
    assert_eq!(v["outcome"], "accepted");
    assert_eq!(posts(&r.fake).len(), 1);
}

/// The operator's clear: an admin client, a reason, and the broker's two answers.
fn clear_req(ops: &mut Conn, rid: &str, cid: &str, reason: &str) -> Value {
    ops.req(
        "clear_order",
        rid,
        &format!(r#""client_order_id":"{cid}","reason":"{reason}""#),
    )
    .0
}

#[test]
fn a_manual_clear_is_refused_while_the_broker_shows_the_order() {
    let r = rig_with("alpaca-clear-shown", |p| p.max_open = 1);
    let mut c = ready(&r.gw, "reactor");
    let mut ops = ready(&r.gw, "ops");
    absent_submit(&r, &mut c, "wse-shown");
    // The broker turns up the order. The poll's lookup settles it as an ordinary
    // working order, and the operator's clear is refused on what the broker shows.
    r.fake.appear(&format!(
        r#"{{"client_order_id":"wse-shown","symbol":"{CALL}","qty":"1","side":"buy","limit_price":"1.25","position_intent":"buy_to_open"}}"#
    ));
    wait_resolved(&mut c);
    let v = clear_req(&mut ops, "k1", "wse-shown", "operator check");
    assert_eq!(
        (v["outcome"].as_str(), v["reason"].as_str()),
        (Some("not_cleared"), Some("broker_shows_order")),
        "{v}"
    );
    assert_eq!(
        (
            v["detail"]["check"].as_str(),
            v["detail"]["status"].as_str()
        ),
        (Some("open_orders"), Some("accepted")),
        "{v}"
    );
    assert_eq!(c.req("open_orders", "q1", "").0["unresolved"], 0);
}

#[test]
fn a_manual_clear_is_refused_as_unreachable_when_a_broker_check_fails() {
    let r = rig("alpaca-clear-down");
    let mut c = ready(&r.gw, "reactor");
    let mut ops = ready(&r.gw, "ops");
    absent_submit(&r, &mut c, "wse-down");
    // The open-orders check fails; the by-id check says 404. One failed check is enough.
    r.fake.next(
        "GET",
        "/v2/orders?status=open",
        Act::Respond(500, Vec::new(), "{}".into()),
    );
    let v = clear_req(&mut ops, "k1", "wse-down", "operator check");
    assert_eq!(
        (v["reason"].as_str(), v["detail"]["check"].as_str()),
        (Some("broker_unreachable"), Some("open_orders")),
        "{v}"
    );
    // Nothing was cleared: the order still counts as unresolved.
    assert_eq!(c.req("open_orders", "q1", "").0["unresolved"], 1);
}

#[test]
fn a_manual_clear_is_accepted_when_both_checks_show_nothing_and_is_journaled() {
    let r = rig("alpaca-clear-ok");
    let mut c = ready(&r.gw, "reactor");
    let mut ops = ready(&r.gw, "ops");
    absent_submit(&r, &mut c, "wse-clr");
    let v = clear_req(&mut ops, "k1", "wse-clr", "stuck after the 502");
    assert_eq!(
        (v["outcome"].as_str(), v["client_order_id"].as_str()),
        (Some("ok"), Some("wse-clr")),
        "{v}"
    );
    let ev = ops.event("cleared", |v| v["client_order_id"] == "wse-clr");
    assert_eq!(
        (ev["requester"].as_str(), ev["reason"].as_str()),
        (Some("ops"), Some("stuck after the 502")),
        "{ev}"
    );
    assert_eq!(ev["broker"]["open_orders"]["result"], "absent");
    assert_eq!(ev["broker"]["by_id"]["result"], "absent");
    assert_eq!(c.req("open_orders", "q1", "").0["unresolved"], 0);
    let lines = journal(&r.state);
    let cleared: Vec<&String> = lines
        .iter()
        .filter(|l| l.contains(r#""kind":"cleared""#))
        .collect();
    assert_eq!(cleared.len(), 1, "{lines:?}");
    assert!(
        cleared[0].contains(r#""requester":"ops""#),
        "{}",
        cleared[0]
    );
}

#[test]
fn only_an_admin_clears_and_a_clear_needs_a_reason() {
    let r = rig("alpaca-clear-roles");
    let mut c = ready(&r.gw, "reactor");
    let mut watch = ready(&r.gw, "watch");
    let mut ops = ready(&r.gw, "ops");
    absent_submit(&r, &mut c, "wse-role");
    for who in [&mut c, &mut watch] {
        let (v, _) = who.req(
            "clear_order",
            "x1",
            r#""client_order_id":"wse-role","reason":"try""#,
        );
        assert_eq!(
            (v["outcome"].as_str(), v["code"].as_str()),
            (Some("error"), Some("forbidden")),
            "{v}"
        );
    }
    let v = clear_req(&mut ops, "x2", "wse-role", "   ");
    assert_eq!(v["code"], "invalid_request", "{v}");
    let v = clear_req(&mut ops, "x3", "wse-none", "test");
    assert_eq!(
        (v["outcome"].as_str(), v["reason"].as_str()),
        (Some("not_cleared"), Some("unknown_order")),
        "{v}"
    );
    // Still unresolved after every refusal.
    assert_eq!(c.req("open_orders", "q1", "").0["unresolved"], 1);
}

#[test]
fn a_cleared_order_does_not_count_toward_max_open_and_its_leg_closes() {
    let r = rig_with("alpaca-clear-cap", |p| p.max_open = 1);
    let mut c = ready(&r.gw, "reactor");
    let mut ops = ready(&r.gw, "ops");
    r.fake.hold(&[(CALL, 1)]);
    absent_submit(&r, &mut c, "wse-cap");
    assert_eq!(
        c.submit("r2", &buy("wse-next"))["detail"]["cap"],
        "open_orders"
    );
    assert_eq!(
        clear_req(&mut ops, "k1", "wse-cap", "test")["outcome"],
        "ok"
    );
    // The free slot takes an open on another leg; the cleared leg has no pending order,
    // so its close goes (the position is held).
    let v = c.submit("r3", &single("wse-put", PUT_A, "buy", "open", 1, "1.00"));
    assert_eq!(v["outcome"], "accepted", "{v}");
    let v = c.submit(
        "r4",
        &single("wse-cap-close", CALL, "sell", "close", 1, "1.00"),
    );
    assert_eq!(v["outcome"], "accepted", "{v}");
}

#[test]
fn a_cleared_id_stays_reserved_and_stays_cleared_across_a_restart() {
    let r = rig("alpaca-clear-restart");
    let mut c = ready(&r.gw, "reactor");
    let mut ops = ready(&r.gw, "ops");
    absent_submit(&r, &mut c, "wse-rs");
    assert_eq!(clear_req(&mut ops, "k1", "wse-rs", "test")["outcome"], "ok");
    let r = restart(r, 2);
    let mut c = ready(&r.gw, "reactor");
    assert_eq!(c.req("open_orders", "q1", "").0["unresolved"], 0);
    // The id is reserved: the same intent is a duplicate, and nothing is sent.
    let before = posts(&r.fake).len();
    assert_eq!(c.submit("r2", &buy("wse-rs"))["outcome"], "duplicate");
    assert_eq!(posts(&r.fake).len(), before);
    let cleared = journal(&r.state)
        .iter()
        .filter(|l| l.contains(r#""kind":"cleared""#))
        .count();
    assert_eq!(cleared, 1);
}

/// Alpaca trades no futures: with ES allowed, a futures order (open or close) is
/// refused before anything is sent. The welcome lists the futures roots, and a
/// held ES position isn't foreign at reconcile while an NQ one (not allowed) is.
#[test]
fn alpaca_refuses_futures_and_reconciles_allowed_roots() {
    let fake = Fake::start();
    fake.hold(&[("ESZ6", 1), ("NQZ6", -1)]);
    let r = boot(fake, root("alpaca-futures"), 1, |p| {
        p.futures = vec!["ES".into()];
        p.max_future_qty = 2;
    });
    let mut c = ready(&r.gw, "reactor");
    let welcome: Value = serde_json::from_str(&c.lines[0]).unwrap();
    assert_eq!(welcome["limits"]["futures"][0], "ES", "{welcome}");
    let foreign: Vec<String> = c
        .lines
        .iter()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .filter(|v| v["kind"] == "foreign")
        .filter_map(|v| v["symbol"].as_str().map(String::from))
        .collect();
    assert_eq!(foreign, vec!["NQZ6".to_owned()]);
    for (rid, effect) in [("r1", "open"), ("r2", "close")] {
        let intent = format!(
            r#"{{"client_order_id":"wse-es-{effect}","shape":"single","instrument":{{"class":"future","symbol":"ESZ6"}},"side":"sell","effect":"{effect}","qty":1,"type":"limit","limit_price":"7743.25","time_in_force":"day"}}"#
        );
        let v = c.submit(rid, &intent);
        assert_eq!(v["outcome"], "not_sent", "{v}");
        assert_eq!(v["detail"]["class"], "future", "{v}");
    }
    assert!(posts(&r.fake).is_empty());
    r.gw.stop().unwrap();
}

/// The journal's events of `kind`, parsed.
fn journaled(state: &std::path::Path, kind: &str) -> Vec<Value> {
    journal(state)
        .iter()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .filter(|v| v["kind"] == kind)
        .collect()
}

/// Waits until the journal holds at least `n` events of `kind`.
fn wait_journaled(state: &std::path::Path, kind: &str, n: usize) -> Vec<Value> {
    let until = Instant::now() + Duration::from_secs(10);
    loop {
        let got = journaled(state, kind);
        if got.len() >= n {
            return got;
        }
        assert!(Instant::now() < until, "{kind}: {} of {n}", got.len());
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Waits for a reconcile after a restart: `n` resync statuses in the journal.
fn wait_resyncs(state: &std::path::Path, n: usize) {
    let until = Instant::now() + Duration::from_secs(10);
    while journaled(state, "status")
        .iter()
        .filter(|v| v.get("resync").is_some())
        .count()
        < n
    {
        assert!(Instant::now() < until, "no resync");
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn resyncs(state: &std::path::Path) -> usize {
    journaled(state, "status")
        .iter()
        .filter(|v| v.get("resync").is_some())
        .count()
}

/// A broker order of its own (a liquidation): `qty` of `symbol`, bought.
fn broker_buy(fake: &Fake, cid: &str, symbol: &str, qty: u64) {
    fake.appear(&format!(
        r#"{{"client_order_id":"{cid}","symbol":"{symbol}","qty":"{qty}","side":"buy","type":"market","time_in_force":"day"}}"#
    ));
}

/// A liquidation by the broker, new → partial → fill on the stream: one `foreign`
/// event and a `foreign_fill` per execution. A restart's REST reconcile then sees
/// the same fills and reports none again.
#[test]
fn a_broker_liquidation_reports_each_fill_once() {
    let r = rig("foreign-fills-stream");
    let _c = ready(&r.gw, "reactor");
    broker_buy(&r.fake, "liq-1", PUT_A, 2);
    r.fake.event("liq-1", "new", "new");
    r.fake.fill("liq-1", 1, "0.33");
    r.fake.fill("liq-1", 1, "0.35");
    let ff = wait_journaled(&r.state, "foreign_fill", 2);
    assert_eq!(journaled(&r.state, "foreign").len(), 1);
    for (f, (price, cum)) in ff.iter().zip([("0.33", 1), ("0.35", 2)]) {
        assert_eq!(f["symbol"], PUT_A, "{f}");
        assert_eq!(f["side"], "buy", "{f}");
        assert_eq!(f["qty"], 1, "{f}");
        assert_eq!(f["price"], price, "{f}");
        assert_eq!(f["price_basis"], "fill", "{f}");
        assert_eq!(f["filled_qty"], cum, "{f}");
        assert_eq!(f["source"], "stream", "{f}");
        assert!(f["execution_id"].as_str().is_some(), "{f}");
    }
    let before = resyncs(&r.state);
    let r = restart(r, 2);
    wait_resyncs(&r.state, before + 1);
    let _c = ready(&r.gw, "reactor");
    assert_eq!(
        journaled(&r.state, "foreign_fill").len(),
        2,
        "no fill twice"
    );
    r.gw.stop().unwrap();
}

/// A restart between the partial and the fill: the REST reconcile reports nothing
/// already reported; the stream's last fill is reported, once.
#[test]
fn a_restart_mid_way_reports_only_the_rest() {
    let r = rig("foreign-fills-restart");
    let _c = ready(&r.gw, "reactor");
    broker_buy(&r.fake, "liq-2", PUT_A, 2);
    r.fake.event("liq-2", "new", "new");
    r.fake.fill("liq-2", 1, "0.33");
    wait_journaled(&r.state, "foreign_fill", 1);
    let before = resyncs(&r.state);
    let r = restart(r, 2);
    // Both reconciles (startup, and once the stream listens) before the stream fills.
    wait_resyncs(&r.state, before + 2);
    assert_eq!(journaled(&r.state, "foreign_fill").len(), 1);
    r.fake.fill("liq-2", 1, "0.35");
    let ff = wait_journaled(&r.state, "foreign_fill", 2);
    assert_eq!(
        (ff[1]["qty"].as_u64(), ff[1]["price"].as_str()),
        (Some(1), Some("0.35"))
    );
    r.gw.stop().unwrap();
}

/// A fill the REST reconcile sees first is reported from it (priced from the average);
/// the stream's copy of it isn't reported again, and the next fill is, once.
#[test]
fn rest_first_then_the_stream_reports_no_fill_twice() {
    let r = rig("foreign-fills-rest-first");
    let _c = ready(&r.gw, "reactor");
    broker_buy(&r.fake, "liq-3", PUT_A, 2);
    {
        let mut st = r.fake.lock();
        let o = st.orders.iter_mut().find(|o| o.cid == "liq-3").unwrap();
        o.filled = 1;
        o.avg = Some("0.33".into());
        o.status = "partially_filled".into();
    }
    let before = resyncs(&r.state);
    let r = restart(r, 2);
    // Both reconciles (startup, and once the stream listens) before the stream fills.
    wait_resyncs(&r.state, before + 2);
    let ff = wait_journaled(&r.state, "foreign_fill", 1);
    assert_eq!(ff[0]["source"], "rest_reconcile", "{}", ff[0]);
    assert_eq!(ff[0]["price_basis"], "avg_delta", "{}", ff[0]);
    assert_eq!(ff[0]["price"], "0.33", "{}", ff[0]);
    // The stream then fills the second contract at 0.35 (average 0.34).
    r.fake.fill("liq-3", 1, "0.35");
    let ff = wait_journaled(&r.state, "foreign_fill", 2);
    // Whichever path sees it first (the fake's REST state changes before its frame
    // is pushed) reports it, once.
    assert_eq!(ff[1]["qty"], 1, "{}", ff[1]);
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(journaled(&r.state, "foreign_fill").len(), 2);
    r.gw.stop().unwrap();
}

/// Two broker orders filled at the same price and quantity are both reported.
#[test]
fn two_orders_alike_are_both_reported() {
    let r = rig("foreign-fills-alike");
    let _c = ready(&r.gw, "reactor");
    broker_buy(&r.fake, "liq-4", PUT_A, 1);
    broker_buy(&r.fake, "liq-5", PUT_A, 1);
    r.fake.fill("liq-4", 1, "0.33");
    r.fake.fill("liq-5", 1, "0.33");
    let ff = wait_journaled(&r.state, "foreign_fill", 2);
    assert_ne!(ff[0]["broker_order_id"], ff[1]["broker_order_id"]);
    r.gw.stop().unwrap();
}

/// An mleg broker order whose stream shows one leg filled, then a REST reconcile
/// showing both: each leg is reported once.
#[test]
fn an_mleg_broker_order_reports_each_leg_once() {
    let r = rig("foreign-fills-mleg");
    let _c = ready(&r.gw, "reactor");
    r.fake.appear(&format!(
        r#"{{"client_order_id":"liq-6","order_class":"mleg","qty":"1","type":"market","time_in_force":"day","legs":[{{"symbol":"{PUT_A}","ratio_qty":"1","side":"buy","position_intent":"buy_to_close"}},{{"symbol":"{PUT_B}","ratio_qty":"1","side":"sell","position_intent":"sell_to_close"}}]}}"#
    ));
    {
        let mut st = r.fake.lock();
        let o = st.orders.iter_mut().find(|o| o.cid == "liq-6").unwrap();
        o.legs[0].filled = 1;
        o.legs[0].avg = Some("0.33".into());
    }
    r.fake.event("liq-6", "partial_fill", "partially_filled");
    let ff = wait_journaled(&r.state, "foreign_fill", 1);
    assert_eq!(
        (ff[0]["symbol"].as_str(), ff[0]["side"].as_str()),
        (Some(PUT_A), Some("buy"))
    );
    {
        let mut st = r.fake.lock();
        let o = st.orders.iter_mut().find(|o| o.cid == "liq-6").unwrap();
        o.legs[1].filled = 1;
        o.legs[1].avg = Some("0.12".into());
        o.filled = 1;
        o.status = "filled".into();
    }
    let before = resyncs(&r.state);
    let r = restart(r, 2);
    wait_resyncs(&r.state, before + 1);
    let ff = wait_journaled(&r.state, "foreign_fill", 2);
    assert_eq!(ff.len(), 2, "{ff:?}");
    assert_eq!(
        (ff[1]["symbol"].as_str(), ff[1]["price"].as_str()),
        (Some(PUT_B), Some("0.12"))
    );
    r.gw.stop().unwrap();
}

/// Shorter than a day, for the tests of ended orders' records.
const KEEP: Duration = Duration::from_millis(200);

/// A GTC broker order through `done_for_day` stays live: its record is kept past the
/// keep period, and its next fill is reported, once.
#[test]
fn a_gtc_order_through_done_for_day_keeps_its_record() {
    let r = boot_kept(
        Fake::start(),
        root("foreign-fills-gtc"),
        1,
        |_| {},
        Some(KEEP),
    );
    let _c = ready(&r.gw, "reactor");
    r.fake.appear(&format!(
        r#"{{"client_order_id":"gtc-1","symbol":"{PUT_A}","qty":"2","side":"buy","type":"limit","limit_price":"0.30","time_in_force":"gtc"}}"#
    ));
    r.fake.fill("gtc-1", 1, "0.30");
    wait_journaled(&r.state, "foreign_fill", 1);
    r.fake.event("gtc-1", "done_for_day", "done_for_day");
    std::thread::sleep(KEEP * 2);
    // Another broker order's update prunes what's due.
    broker_buy(&r.fake, "liq-7", PUT_B, 1);
    r.fake.fill("liq-7", 1, "0.12");
    wait_journaled(&r.state, "foreign_fill", 2);
    assert!(
        journaled(&r.state, "foreign_done")
            .iter()
            .all(|v| v["broker_order_id"]
                != journaled(&r.state, "foreign_fill")[0]["broker_order_id"])
    );
    r.fake.fill("gtc-1", 1, "0.29");
    let ff = wait_journaled(&r.state, "foreign_fill", 3);
    let gtc: Vec<_> = ff
        .iter()
        .filter(|v| v["client_order_id"] == "gtc-1")
        .collect();
    assert_eq!(gtc.len(), 2, "{ff:?}");
    assert_eq!(
        (gtc[1]["qty"].as_u64(), gtc[1]["price"].as_str()),
        (Some(1), Some("0.29"))
    );
    r.gw.stop().unwrap();
}

/// More fills than the execution ids kept: each reported once, and none again
/// after a restart.
#[test]
fn seventy_fills_are_each_reported_once() {
    let r = rig("foreign-fills-seventy");
    let _c = ready(&r.gw, "reactor");
    broker_buy(&r.fake, "liq-8", PUT_A, 70);
    for _ in 0..70 {
        r.fake.fill("liq-8", 1, "0.33");
    }
    wait_journaled(&r.state, "foreign_fill", 70);
    let before = resyncs(&r.state);
    let r = restart(r, 2);
    wait_resyncs(&r.state, before + 2);
    assert_eq!(journaled(&r.state, "foreign_fill").len(), 70);
    r.gw.stop().unwrap();
}

/// A stream execution whose order shows nothing more filled is said
/// (`foreign_fill_lag`); the stream's copy of a fill REST reported first isn't.
#[test]
fn lag_is_said_only_when_it_is_one() {
    let r = rig("foreign-fills-lag");
    let _c = ready(&r.gw, "reactor");
    broker_buy(&r.fake, "liq-9", PUT_A, 2);
    r.fake.event("liq-9", "new", "new");
    wait_journaled(&r.state, "foreign", 1);
    r.fake.fill_lagging("liq-9", 1, "0.33");
    let lag = wait_journaled(&r.state, "foreign_fill_lag", 1);
    assert_eq!(lag[0]["reason"], "cumulative_lags", "{}", lag[0]);
    assert_eq!(journaled(&r.state, "foreign_fill").len(), 0);
    // REST first (a restart), then the stream's copy: no lag.
    broker_buy(&r.fake, "liq-10", PUT_B, 1);
    {
        let mut st = r.fake.lock();
        let o = st.orders.iter_mut().find(|o| o.cid == "liq-10").unwrap();
        o.filled = 1;
        o.avg = Some("0.12".into());
        o.status = "filled".into();
    }
    let before = resyncs(&r.state);
    let r = restart(r, 2);
    wait_resyncs(&r.state, before + 2);
    wait_journaled(&r.state, "foreign_fill", 1);
    {
        let mut st = r.fake.lock();
        let o = st.orders.iter_mut().find(|o| o.cid == "liq-10").unwrap();
        o.filled = 0;
    }
    r.fake.fill("liq-10", 1, "0.12");
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(
        journaled(&r.state, "foreign_fill_lag").len(),
        1,
        "only the true lag"
    );
    assert_eq!(journaled(&r.state, "foreign_fill").len(), 1);
    r.gw.stop().unwrap();
}

/// Two legs filled under one execution id are both reported.
#[test]
fn legs_sharing_an_execution_id_are_both_reported() {
    let r = rig("foreign-fills-shared-exec");
    let _c = ready(&r.gw, "reactor");
    r.fake.appear(&format!(
        r#"{{"client_order_id":"liq-11","order_class":"mleg","qty":"1","type":"market","time_in_force":"day","legs":[{{"symbol":"{PUT_A}","ratio_qty":"1","side":"buy","position_intent":"buy_to_close"}},{{"symbol":"{PUT_B}","ratio_qty":"1","side":"sell","position_intent":"sell_to_close"}}]}}"#
    ));
    r.fake
        .mleg_fill_one_exec("liq-11", 1, "0.21", &["0.33", "0.12"]);
    let ff = wait_journaled(&r.state, "foreign_fill", 2);
    let syms: Vec<_> = ff.iter().map(|v| v["symbol"].as_str().unwrap()).collect();
    assert_eq!(syms, vec![PUT_A, PUT_B]);
    r.gw.stop().unwrap();
}

/// A restart after longer than the keep period: the records rebuilt from the
/// journal are kept, so a REST relisting of an ended order reports nothing again.
#[test]
fn a_long_restart_reports_nothing_again() {
    let r = boot_kept(
        Fake::start(),
        root("foreign-fills-long"),
        1,
        |_| {},
        Some(KEEP),
    );
    let _c = ready(&r.gw, "reactor");
    broker_buy(&r.fake, "liq-12", PUT_A, 1);
    r.fake.fill("liq-12", 1, "0.33");
    wait_journaled(&r.state, "foreign_fill", 1);
    wait_journaled(&r.state, "foreign_done", 1);
    let Rig { fake, gw, state } = r;
    gw.stop().unwrap();
    std::thread::sleep(KEEP * 3);
    let before = resyncs(&state);
    let r = boot_kept(fake, state, 2, |_| {}, Some(KEEP));
    wait_resyncs(&r.state, before + 2);
    // Another order's update would prune what's due.
    broker_buy(&r.fake, "liq-13", PUT_B, 1);
    r.fake.fill("liq-13", 1, "0.12");
    wait_journaled(&r.state, "foreign_fill", 2);
    let before = resyncs(&r.state);
    let r = restart(r, 3);
    wait_resyncs(&r.state, before + 2);
    let ff = journaled(&r.state, "foreign_fill");
    assert_eq!(
        ff.iter()
            .filter(|v| v["client_order_id"] == "liq-12")
            .count(),
        1,
        "{ff:?}"
    );
    r.gw.stop().unwrap();
}
