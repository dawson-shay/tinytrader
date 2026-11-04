//! Safety and session rules: persistent kill switch, caps, stream drop resync,
//! `cancel_on_disconnect`, resume and slow consumers.
mod support;
use std::time::Duration;
use support::*;

fn clocked() -> String {
    format!(r#"{{"clock_ts":{T0}}}"#)
}

/// The clock, and options held long: closes are checked against the position held.
fn clocked_holding() -> String {
    format!(
        r#"{{"clock_ts":{T0},"positions":{{"{CALL}":10,"{PUT_A}":10,"{PUT_B}":10,"{FAR}":10}}}}"#
    )
}

#[test]
fn kill_levels_persist_across_restart_until_unkill() {
    let state = root("kill");
    let gw = scripted(config(state.clone()), &clocked_holding());
    let mut c = Conn::open(&gw, "reactor", None);
    c.submit("r1", &buy("wse-1"));
    let mut ops = Conn::open(&gw, "ops", None);
    let (r, _) = ops.req("kill", "k1", r#""level":"halt_opens","reason":"test""#);
    assert_eq!(r["state"], "killed");
    assert_eq!(c.submit("r2", &buy("wse-2"))["reason"], "killed");
    // The resting buy on CALL is working, so the close goes on PUT_A.
    let close = single("wse-3", PUT_A, "sell", "close", 1, "1.00");
    assert_eq!(c.submit("r3", &close)["outcome"], "accepted");
    ops.req("kill", "k2", r#""level":"halt_all","reason":"test""#);
    // halt_all cancels every open order.
    c.order_event("wse-1", "canceled");
    c.order_event("wse-3", "canceled");
    let close = single("wse-4", CALL, "sell", "close", 1, "1.00");
    assert_eq!(c.submit("r4", &close)["reason"], "killed");
    gw.stop().unwrap();

    let gw = scripted(config(state.clone()), &clocked_holding());
    let mut c = Conn::open(&gw, "reactor", None);
    assert_eq!(
        c.submit("r5", &close.replace("wse-4", "wse-5"))["reason"],
        "killed"
    );
    let mut ops = Conn::open(&gw, "ops", None);
    assert_eq!(
        ops.req("unkill", "u1", r#""reason":"done""#).0["state"],
        "ready"
    );
    assert_eq!(c.submit("r6", &buy("wse-6"))["outcome"], "accepted");
    gw.stop().unwrap();

    std::fs::write(state.join("KILL"), b"").unwrap();
    let gw = scripted(config(state.clone()), &clocked_holding());
    let mut c = Conn::open(&gw, "reactor", None);
    let st = c.event("status", |v| v.get("kill").is_some());
    assert_eq!(
        (st["kill"]["level"].as_str(), st["kill"]["reason"].as_str()),
        (Some("halt_all"), Some("sentinel"))
    );
    gw.stop().unwrap();
}

#[test]
fn each_cap_refuses_before_any_send() {
    let mut cfg = config(root("caps"));
    cfg.profile.max_open = 2;
    cfg.profile.max_legs = 2;
    cfg.profile.submits_per_min = 3;
    let gw = scripted(cfg, &clocked());
    let mut c = Conn::open(&gw, "reactor", None);
    let cap = |r: serde_json::Value| {
        (
            r["reason"].as_str().map(String::from),
            r["detail"]["cap"].as_str().map(String::from),
        )
    };
    let want = |name: &str| (Some("cap".to_string()), Some(name.to_string()));
    let three = combo(
        "wse-l",
        &[
            (PUT_A, 1, "buy", "open"),
            (PUT_B, 1, "sell", "open"),
            (CALL, 1, "buy", "open"),
        ],
        1,
        "0.10",
    );
    assert_eq!(cap(c.submit("l", &three)), want("legs"));
    assert_eq!(
        cap(c.submit("n", &single("wse-n", CALL, "buy", "open", 10, "2.51"))),
        want("notional")
    );
    // A market opening is refused whatever its reference price, before the notional cap.
    let market = |s: String| {
        s.replace(
            r#""type":"limit","limit_price""#,
            r#""type":"market","ref_price""#,
        )
    };
    let r = c.submit(
        "nm",
        &market(single("wse-nm", CALL, "buy", "open", 10, "2.51")),
    );
    assert_eq!(r["reason"], "market_opening_refused", "{r}");
    c.submit("a1", &buy("wse-a1"));
    c.submit("a2", &buy("wse-a2"));
    assert_eq!(cap(c.submit("o", &buy("wse-o"))), want("open_orders"));
    c.req("cancel", "c1", r#""client_order_id":"wse-a1""#);
    c.submit("a3", &buy("wse-a3"));
    c.req("cancel", "c2", r#""client_order_id":"wse-a2""#);
    assert_eq!(
        cap(c.submit("m", &buy("wse-m"))),
        want("submits_per_minute")
    );
    gw.stop().unwrap();

    let mut cfg = config(root("caps-day"));
    cfg.profile.submits_per_day = 1;
    let gw = scripted(cfg, &clocked());
    let mut c = Conn::open(&gw, "reactor", None);
    c.submit("d1", &buy("wse-d1"));
    assert_eq!(cap(c.submit("d2", &buy("wse-d2"))), want("submits_per_day"));
    gw.stop().unwrap();
}

/// The `cap` name of a refusal, or `None`.
fn cap_name(r: &serde_json::Value) -> Option<String> {
    (r["reason"] == "cap")
        .then(|| r["detail"]["cap"].as_str().map(String::from))
        .flatten()
}

fn close(cid: &str) -> String {
    close_on(cid, CALL)
}

fn close_on(cid: &str, symbol: &str) -> String {
    single(cid, symbol, "sell", "close", 1, "1.00")
}

/// A fourth option, for a close on a leg with no order of its own.
const FAR: &str = "SPY261005C00590000";
const CALL_585: &str = "SPY261005C00585000";
const PUT_C: &str = "SPY261005P00565000";
/// A 575 call, and the 580 call of the next week's expiry, 2026-10-12 (a calendar with the 580
/// call).
const CALL_575: &str = "SPY261005C00575000";
const CALL_580_NEXT: &str = "SPY261012C00580000";

#[test]
fn a_close_is_not_counted_toward_the_daily_cap() {
    let mut cfg = config(root("caps-day-close"));
    cfg.profile.submits_per_day = 1;
    cfg.profile.max_open = 10;
    let gw = scripted(cfg, &clocked_holding());
    let mut c = Conn::open(&gw, "reactor", None);
    // Each close is on its own leg: a working close or open on a leg refuses a close there.
    assert_eq!(c.submit("c1", &close("wse-c1"))["outcome"], "accepted");
    assert_eq!(
        c.submit("c2", &close_on("wse-c2", PUT_A))["outcome"],
        "accepted"
    );
    assert_eq!(
        c.submit("o1", &buy("wse-o1"))["outcome"],
        "accepted",
        "closes used none of the day's opens"
    );
    assert_eq!(
        cap_name(&c.submit("o2", &buy("wse-o2"))),
        Some("submits_per_day".into())
    );
    assert_eq!(
        c.submit("c3", &close_on("wse-c3", FAR))["outcome"],
        "accepted",
        "a close passes the day cap the opens have used up"
    );
    gw.stop().unwrap();
}

#[test]
fn closes_per_minute_refuses_closes_and_never_opens() {
    let mut cfg = config(root("closes-per-min"));
    cfg.profile.closes_per_min = 2;
    cfg.profile.max_open = 10;
    let gw = scripted(cfg, &clocked_holding());
    let mut c = Conn::open(&gw, "reactor", None);
    assert_eq!(c.submit("c1", &close("wse-c1"))["outcome"], "accepted");
    assert_eq!(
        c.submit("c2", &close_on("wse-c2", PUT_A))["outcome"],
        "accepted"
    );
    assert_eq!(
        cap_name(&c.submit("c3", &close_on("wse-c3", PUT_B))),
        Some("closes_per_minute".into())
    );
    assert_eq!(
        c.submit("o1", &buy("wse-o1"))["outcome"],
        "accepted",
        "the closes budget is not the opens budget"
    );
    assert_eq!(sends(&mut c, "s1"), 3, "a refused close is never sent");
    gw.stop().unwrap();
}

#[test]
fn restart_counts_only_opens_toward_the_day() {
    let state = root("day-restart");
    let day_cap = |n: usize| {
        let mut cfg = config(state.clone());
        cfg.profile.submits_per_day = n;
        cfg.profile.max_open = 10;
        cfg
    };
    let gw = scripted(day_cap(1), &clocked_holding());
    let mut c = Conn::open(&gw, "reactor", None);
    assert_eq!(c.submit("c1", &close("wse-c1"))["outcome"], "accepted");
    gw.stop().unwrap();

    let gw = scripted(day_cap(1), &clocked_holding());
    let mut c = Conn::open(&gw, "reactor", None);
    assert_eq!(
        c.submit("o1", &buy("wse-o1"))["outcome"],
        "accepted",
        "the day count rebuilt from the journal has no close in it"
    );
    gw.stop().unwrap();

    let gw = scripted(day_cap(1), &clocked_holding());
    let mut c = Conn::open(&gw, "reactor", None);
    assert_eq!(
        cap_name(&c.submit("o2", &buy("wse-o2"))),
        Some("submits_per_day".into()),
        "an open journaled today still counts"
    );
    gw.stop().unwrap();
}

#[test]
fn a_429_backoff_holds_opens_but_not_closes() {
    let rule =
        r#"{"client_order_id":"wse-429","submit":{"http_429":{"headers":{"Retry-After":"1"}}}}"#;
    let script = format!(r#"{{"clock_ts":{T0},"positions":{{"{PUT_A}":1}},"rules":[{rule}]}}"#);
    let gw = scripted(config(root("backoff-close")), &script);
    let mut c = Conn::open(&gw, "reactor", None);
    let r = c.submit("r1", &buy("wse-429"));
    assert_eq!(r["reason"], "http_429_after_send");
    assert_eq!(
        c.submit("r2", &buy("wse-open"))["reason"],
        "throttled",
        "the backoff holds opens"
    );
    assert_eq!(
        c.submit("r3", &close_on("wse-close", PUT_A))["outcome"],
        "accepted",
        "and does not hold closes"
    );
    gw.stop().unwrap();
}

#[test]
fn stream_drop_degrades_then_resyncs_to_ready() {
    let script = format!(
        r#"{{"clock_ts":{T0},"stream":{{"drop_after_events":1,"down_ms":400}},"rules":[{{"nth":2,"submit":"fill"}}]}}"#
    );
    let gw = scripted(config(root("stream")), &script);
    let mut c = Conn::open(&gw, "reactor", None);
    c.submit("r1", &buy("wse-1"));
    c.order_event("wse-1", "new");
    let st = c.event("status", |v| v["state"] == "degraded");
    assert!(st["broker_stream"]["down"]["since"].is_u64());
    assert_eq!(c.submit("r2", &buy("wse-2"))["reason"], "not_ready");
    let (r, _) = c.req("cancel", "r3", r#""client_order_id":"wse-1""#);
    assert_eq!(
        r["outcome"], "accepted",
        "cancels are allowed while degraded"
    );
    // The cancel happened while the stream was down; the 1 Hz poll or resync reports it.
    // The stream can also deliver it first once it is back, so either source is correct.
    let o = c.order_event("wse-1", "canceled");
    assert!(
        matches!(o["source"].as_str(), Some("rest_reconcile" | "sim")),
        "{o}"
    );
    let st = c.event("status", |v| v.get("resync").is_some());
    assert_eq!(st["state"], "ready");
    assert_eq!(c.submit("r4", &buy("wse-4"))["outcome"], "accepted");
    gw.stop().unwrap();
}

#[test]
fn cancel_on_disconnect_is_opt_in() {
    let gw = scripted(config(root("cod")), &clocked());
    let mut watch = Conn::open(&gw, "watch", None);
    let mut keep = Conn::open(&gw, "other", None);
    keep.submit("o1", &buy("oth-1"));
    drop(keep);
    let mut c = Conn::with(&gw, "reactor", None, true);
    c.submit("r1", &buy("wse-1"));
    drop(c);
    let ev = watch.order_event("wse-1", "canceled");
    assert_eq!(ev["status"], "canceled");
    std::thread::sleep(Duration::from_millis(200));
    let (q, _) = watch.req("order", "w1", r#""client_order_id":"oth-1""#);
    assert_eq!(
        q["order"]["status"], "new",
        "no automatic cancel without opt-in"
    );
    let cancels: Vec<_> = watch
        .lines
        .iter()
        .filter(|l| l.contains("cancel_on_disconnect"))
        .collect();
    assert_eq!(cancels.len(), 2, "sent + outcome: {cancels:?}");
    gw.stop().unwrap();
}

#[test]
fn reconnect_resumes_an_identical_suffix() {
    let gw = scripted(
        config(root("resume")),
        &format!(r#"{{"clock_ts":{T0},"default":{{"submit":"fill"}}}}"#),
    );
    let mut watch = Conn::open(&gw, "watch", Some(0));
    let mut c = Conn::open(&gw, "reactor", None);
    c.submit("r1", &buy("wse-1"));
    c.order_event("wse-1", "fill");
    let mut early = Conn::open(&gw, "ops", Some(0));
    let cut = early.order_event("wse-1", "new")["seq"].as_u64().unwrap();
    drop(early);
    c.submit("r2", &buy("wse-2"));
    c.order_event("wse-2", "fill");
    let last = watch.order_event("wse-2", "fill")["seq"].as_u64().unwrap();
    let mut back = Conn::open(&gw, "ops", Some(cut));
    let resumed = back.until(|v| v["seq"] == last);
    let reference: Vec<String> = watch
        .lines
        .iter()
        .filter(|l| {
            let v: serde_json::Value = serde_json::from_str(l).unwrap();
            v["seq"].as_u64().is_some_and(|s| s > cut && s <= last)
        })
        .cloned()
        .collect();
    assert_eq!(back.lines[1..].to_vec(), reference);
    assert_eq!(resumed.len(), reference.len());
    gw.stop().unwrap();
}

#[test]
fn slow_consumer_is_closed_and_recovers_by_resume() {
    let mut cfg = config(root("slow"));
    cfg.timing.queue_bytes = 8000;
    let gw = scripted(
        cfg,
        &format!(r#"{{"clock_ts":{T0},"default":{{"submit":"fill"}}}}"#),
    );
    let mut c = Conn::open(&gw, "reactor", None);
    for n in 0..10 {
        c.submit(&format!("r{n}"), &buy(&format!("wse-{n}")));
        c.order_event(&format!("wse-{n}"), "fill");
    }
    let mut slow =
        order_gateway::client::Client::connect(gw.addr(), "watch", TOKEN, Some(0), false).unwrap();
    let first = slow.line().unwrap().unwrap();
    assert!(first.contains("slow_consumer"), "{first}");
    assert_eq!(slow.line().unwrap(), None, "closed after slow_consumer");
    // A bounded resume from a recent seq fits the queue.
    let recent = c
        .lines
        .iter()
        .rev()
        .find_map(|l| serde_json::from_str::<serde_json::Value>(l).ok()?["seq"].as_u64());
    let mut c2 = Conn::open(&gw, "watch", recent.map(|s| s - 2));
    assert_eq!(c2.next()["seq"], recent.unwrap() - 1);
    gw.stop().unwrap();
}

#[test]
fn flatten_closes_every_position_and_halts_opens() {
    let script = format!(
        r#"{{"clock_ts":{T0},"positions":{{"{CALL}":2,"{PUT_A}":-1}},"quotes":{{"{CALL}":{{"bid":"1.20","ask":"1.30"}}}}}}"#
    );
    let gw = scripted(config(root("flatten")), &script);
    let mut c = Conn::open(&gw, "reactor", None);
    assert_eq!(c.submit("r1", &buy("wse-1"))["outcome"], "accepted");
    // Only an admin may flatten.
    assert_eq!(
        c.req("flatten", "f0", r#""reason":"test""#).0["code"],
        "forbidden"
    );
    let mut ops = Conn::open(&gw, "ops", None);
    let (r, _) = ops.req("flatten", "f1", r#""reason":"test""#);
    assert_eq!(
        (r["outcome"].as_str(), r["state"].as_str()),
        (Some("ok"), Some("killed"))
    );
    let mut closed: Vec<String> = r["positions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["symbol"].as_str().unwrap().to_owned())
        .collect();
    closed.sort();
    assert_eq!(closed, [CALL, PUT_A]);
    // The flatten events follow its response, so the next request reads past them.
    let (p, skipped) = ops.req("positions", "p1", "");
    let ev = skipped
        .iter()
        .find(|v| v["kind"] == "flatten" && v["phase"] == "outcome")
        .expect("a flatten outcome event")
        .clone();
    assert_eq!(p["positions"].as_array().unwrap().len(), 0, "{p}");
    // The resting open order was cancelled; opens stay refused, closes pass.
    c.order_event("wse-1", "canceled");
    assert_eq!(c.submit("r2", &buy("wse-2"))["reason"], "killed");
    // Closes are not stopped by halt_opens, but nothing is held now: the close is refused.
    let close = single("wse-3", CALL, "sell", "close", 1, "1.00");
    let r3 = c.submit("r3", &close);
    assert_eq!(
        (r3["reason"].as_str(), r3["detail"]["held_qty"].as_i64()),
        (Some("close_exceeds_position"), Some(0))
    );
    assert_eq!(ev["outcome"]["outcome"], "ok");
    gw.stop().unwrap();
}

#[test]
fn an_any_underlying_client_skips_only_the_allowlist() {
    let mut cfg = config(root("any-underlying"));
    cfg.profile.any_underlying_clients = vec!["other".into()];
    let gw = scripted(cfg, &clocked());
    let sap = |cid: &str, qty: u32, price: &str| {
        single(cid, "SAP261016C00250000", "buy", "open", qty, price)
    };
    // Another trading client on the same account keeps the allowlist.
    let mut c = Conn::open(&gw, "reactor", None);
    let r = c.submit("r1", &sap("wse-1", 1, "1.00"));
    assert_eq!(
        (r["reason"].as_str(), r["detail"]["underlying"].as_str()),
        (Some("unsupported"), Some("SAP"))
    );
    let mut o = Conn::open(&gw, "other", None);
    assert_eq!(
        o.submit("o1", &sap("oth-1", 1, "1.00"))["outcome"],
        "accepted"
    );
    // Caps still apply.
    assert_eq!(
        o.submit("o3", &sap("oth-3", 1, "26.00"))["detail"]["cap"],
        "notional"
    );
    gw.stop().unwrap();
}

#[test]
fn expiry_day_opens_go_to_the_broker() {
    // No gateway cutoff: the broker's own expiry-day rules decide.
    let gw = scripted(
        config(root("expiry-day")),
        &format!(r#"{{"clock_ts":{LATE}}}"#),
    );
    let mut c = Conn::open(&gw, "reactor", None);
    assert_eq!(c.submit("r1", &buy("wse-1"))["outcome"], "accepted");
    gw.stop().unwrap();
}

#[test]
fn closing_orders_pass_the_notional_and_open_order_caps() {
    let mut cfg = config(root("caps-close"));
    cfg.profile.max_open = 1;
    let gw = scripted(cfg, &clocked_holding());
    let mut c = Conn::open(&gw, "reactor", None);
    assert_eq!(c.submit("a1", &buy("wse-a1"))["outcome"], "accepted");
    // Past the notional cap ($2,500) and with the one open order working on CALL: still sent.
    let r = c.submit("x1", &single("wse-x1", PUT_A, "sell", "close", 10, "2.51"));
    assert_eq!(r["outcome"], "accepted", "{r}");
    // Opening is still held to both.
    assert_eq!(
        c.submit("o1", &buy("wse-o1"))["detail"]["cap"],
        "open_orders"
    );
    gw.stop().unwrap();
}

/// A $5-wide put credit spread (sell 570, buy 565) for $1.00.
fn wide_credit_spread(cid: &str, qty: u32) -> String {
    combo(
        cid,
        &[(PUT_A, 1, "sell", "open"), (PUT_C, 1, "buy", "open")],
        qty,
        "-1.00",
    )
}

#[test]
fn an_opening_credit_spread_is_held_to_its_maximum_loss() {
    // $5 wide at a $1.00 credit: $4 a share at risk. The $1,000 cap admits 2 units
    // (a $800 maximum loss) where the premium check admitted 10 ($1,000 of credit).
    let mut cfg = config(root("max-loss-spread"));
    cfg.profile.max_notional_cents = 100_000;
    let gw = scripted(cfg, &clocked_holding());
    let mut c = Conn::open(&gw, "reactor", None);
    assert_eq!(
        cap_name(&c.submit("w10", &wide_credit_spread("wse-w10", 10))),
        Some("notional".into())
    );
    assert_eq!(
        c.submit("w3", &wide_credit_spread("wse-w3", 3))["detail"]["cap"],
        "notional"
    );
    assert_eq!(
        c.submit("w2", &wide_credit_spread("wse-w2", 2))["outcome"],
        "accepted"
    );
    gw.stop().unwrap();
}

#[test]
fn a_naked_short_call_is_refused_as_unbounded() {
    let gw = scripted(config(root("max-loss-naked")), &clocked_holding());
    let mut c = Conn::open(&gw, "reactor", None);
    let r = c.submit("n1", &single("wse-n1", CALL, "sell", "open", 1, "1.25"));
    assert_eq!(
        (r["reason"].as_str(), r["detail"]["symbols"][0].as_str()),
        (Some("max_loss_unbounded"), Some(CALL))
    );
    gw.stop().unwrap();
}

#[test]
fn a_butterfly_is_held_to_its_bounded_loss() {
    // Buy 575, sell two 580, buy 585 calls for $1.00: a loss of $1.00 a share, $100 a
    // contract. The $1,000 cap admits 10 (exactly $1,000) and refuses 11 ($1,100).
    let mut cfg = config(root("max-loss-fly"));
    cfg.profile.max_notional_cents = 100_000;
    let gw = scripted(cfg, &clocked_holding());
    let mut c = Conn::open(&gw, "reactor", None);
    let fly = |cid: &str, qty: u32| {
        combo(
            cid,
            &[
                (CALL_575, 1, "buy", "open"),
                (CALL_585, 2, "sell", "open"),
                (FAR, 1, "buy", "open"),
            ],
            qty,
            "1.00",
        )
    };
    assert_eq!(
        cap_name(&c.submit("f11", &fly("wse-f11", 11))),
        Some("notional".into())
    );
    assert_eq!(c.submit("f10", &fly("wse-f10", 10))["outcome"], "accepted");
    gw.stop().unwrap();
}

#[test]
fn an_unbounded_ratio_spread_is_refused_as_unbounded() {
    // Buy one 575 call, sell two 585 calls: above 585 the payoff falls $1 a dollar.
    let gw = scripted(config(root("max-loss-ratio")), &clocked_holding());
    let mut c = Conn::open(&gw, "reactor", None);
    let r = c.submit(
        "r1",
        &combo(
            "wse-r1",
            &[(CALL_575, 1, "buy", "open"), (CALL_585, 2, "sell", "open")],
            1,
            "1.00",
        ),
    );
    assert_eq!(r["reason"], "max_loss_unbounded", "{r}");
    gw.stop().unwrap();
}

#[test]
fn a_roll_and_a_calendar_are_refused_as_unknown() {
    // A roll closes the held 580 call and opens the 585: one limit prices both legs.
    let gw = scripted(config(root("max-loss-unknown")), &clocked_holding());
    let mut c = Conn::open(&gw, "reactor", None);
    let roll = combo(
        "wse-roll",
        &[(CALL, 1, "sell", "close"), (CALL_585, 1, "buy", "open")],
        1,
        "1.00",
    );
    let r = c.submit("roll", &roll);
    assert_eq!(r["reason"], "max_loss_unknown", "{r}");
    // A calendar: the 580 calls of two expiries.
    let cal = combo(
        "wse-cal",
        &[(CALL, 1, "buy", "open"), (CALL_580_NEXT, 1, "sell", "open")],
        1,
        "1.00",
    );
    let r = c.submit("cal", &cal);
    assert_eq!(r["reason"], "max_loss_unknown", "{r}");
    gw.stop().unwrap();
}

#[test]
fn a_spy_call_calendar_is_admitted_at_its_debit() {
    // Sell the 261005 580 call and buy the 261012 580 call for a $1.00 debit: SPY is American,
    // so the long covers the short and the loss is the debit, $100 a contract. A $100 cap
    // admits one contract and refuses two.
    let mut cfg = config(root("max-loss-spy-cal"));
    cfg.profile.max_notional_cents = 10_000;
    let gw = scripted(cfg, &clocked_holding());
    let mut c = Conn::open(&gw, "reactor", None);
    let cal = |cid: &str, qty: u32| {
        combo(
            cid,
            &[(CALL, 1, "sell", "open"), (CALL_580_NEXT, 1, "buy", "open")],
            qty,
            "1.00",
        )
    };
    assert_eq!(
        cap_name(&c.submit("c2", &cal("wse-c2", 2))),
        Some("notional".into())
    );
    let r = c.submit("c1", &cal("wse-c1", 1));
    assert_eq!(r["outcome"], "accepted", "{r}");
    gw.stop().unwrap();
}

#[test]
fn a_vix_call_calendar_is_refused_as_unknown() {
    // Sell the 21 Oct VIX 20 call and buy the 18 Nov VIX 20 call: each expiry settles on its
    // own VIX futures month, so the long need not cover the short.
    let mut cfg = config(root("max-loss-vix-cal"));
    cfg.profile.underlyings.push("VIX".into());
    let gw = scripted(cfg, &clocked_holding());
    let mut c = Conn::open(&gw, "reactor", None);
    let cal = combo(
        "wse-vix",
        &[
            ("VIX261021C00020000", 1, "sell", "open"),
            ("VIX261118C00020000", 1, "buy", "open"),
        ],
        1,
        "1.00",
    );
    let r = c.submit("vix", &cal);
    assert_eq!(r["reason"], "max_loss_unknown", "{r}");
    gw.stop().unwrap();
}

/// A double calendar: sell the 5 Oct 570 put and 580 call, buy the 12 Oct 570 put and 580
/// call.
fn double_calendar(cid: &str, qty: u32, price: &str) -> String {
    combo(
        cid,
        &[
            ("SPY261005P00570000", 1, "sell", "open"),
            ("SPY261012P00570000", 1, "buy", "open"),
            (CALL, 1, "sell", "open"),
            (CALL_580_NEXT, 1, "buy", "open"),
        ],
        qty,
        price,
    )
}

#[test]
fn a_double_calendar_is_refused_without_allow_calendars() {
    let gw = scripted(config(root("max-loss-double-cal-off")), &clocked_holding());
    let mut c = Conn::open(&gw, "reactor", None);
    let welcome: serde_json::Value = serde_json::from_str(&c.lines[0]).unwrap();
    assert!(
        welcome["limits"].get("allow_calendars").is_none(),
        "{welcome}"
    );
    let r = c.submit("d1", &double_calendar("wse-d1", 1, "2.40"));
    assert_eq!(r["reason"], "max_loss_unknown", "{r}");
    gw.stop().unwrap();
}

#[test]
fn a_double_calendar_is_admitted_at_its_debit_with_allow_calendars() {
    // A $2.40 debit is $240 a unit: a $500 cap admits two units and refuses three. A diagonal
    // of four legs is still unknown.
    let mut cfg = config(root("max-loss-double-cal"));
    cfg.profile.allow_calendars = true;
    cfg.profile.max_notional_cents = 50_000;
    let gw = scripted(cfg, &clocked_holding());
    let mut c = Conn::open(&gw, "reactor", None);
    let welcome: serde_json::Value = serde_json::from_str(&c.lines[0]).unwrap();
    assert_eq!(welcome["limits"]["allow_calendars"], "yes", "{welcome}");
    assert_eq!(
        cap_name(&c.submit("d3", &double_calendar("wse-d3", 3, "2.40"))),
        Some("notional".into())
    );
    let r = c.submit("d2", &double_calendar("wse-d2", 2, "2.40"));
    assert_eq!(r["outcome"], "accepted", "{r}");
    let diag = combo(
        "wse-dd",
        &[
            ("SPY261005P00570000", 1, "sell", "open"),
            ("SPY261012P00565000", 1, "buy", "open"),
            (CALL, 1, "sell", "open"),
            ("SPY261012C00585000", 1, "buy", "open"),
        ],
        1,
        "1.00",
    );
    let r = c.submit("dd", &diag);
    assert_eq!(r["reason"], "max_loss_unknown", "{r}");
    gw.stop().unwrap();
}

#[test]
fn a_close_is_not_held_to_the_maximum_loss() {
    // Selling a held call to close is a naked short's shape; a close is never bounded.
    let mut cfg = config(root("max-loss-close"));
    cfg.profile.max_notional_cents = 1;
    let gw = scripted(cfg, &clocked_holding());
    let mut c = Conn::open(&gw, "reactor", None);
    let r = c.submit("cc", &single("wse-cc", CALL, "sell", "close", 10, "2.51"));
    assert_eq!(r["outcome"], "accepted", "{r}");
    gw.stop().unwrap();
}

/// The market form of a single limit intent: `type` and `ref_price` replace the limit.
fn market_of(s: String) -> String {
    s.replace(
        r#""type":"limit","limit_price""#,
        r#""type":"market","ref_price""#,
    )
}

#[test]
fn a_market_opening_is_refused_and_never_sent() {
    // 10 calls at a $1.25 reference price is $1,250, under the cap, but a market order
    // fills at whatever the market gives: it is refused, and nothing is sent.
    let gw = scripted(config(root("market-open")), &clocked_holding());
    let mut c = Conn::open(&gw, "reactor", None);
    let before = sends(&mut c, "s0");
    let r = c.submit(
        "m1",
        &market_of(single("wse-m1", CALL, "buy", "open", 10, "1.25")),
    );
    assert_eq!(
        (r["outcome"].as_str(), r["reason"].as_str()),
        (Some("not_sent"), Some("market_opening_refused")),
        "{r}"
    );
    assert_eq!(sends(&mut c, "s1"), before);
    gw.stop().unwrap();
}

#[test]
fn a_market_short_sale_of_stock_is_refused_even_when_shorts_are_allowed() {
    let mut cfg = config(root("market-short"));
    cfg.profile.equities = vec!["INTC".into()];
    cfg.profile.allow_short = true;
    let gw = scripted(cfg, &clocked());
    let mut c = Conn::open(&gw, "reactor", None);
    let before = sends(&mut c, "s0");
    let intent = r#"{"client_order_id":"wse-ms","shape":"single","instrument":{"class":"equity","symbol":"INTC"},"side":"sell","effect":"open","qty":10,"type":"market","ref_price":"27.30","time_in_force":"day"}"#;
    let r = c.submit("ms", intent);
    assert_eq!(r["reason"], "market_opening_refused", "{r}");
    assert_eq!(sends(&mut c, "s1"), before);
    gw.stop().unwrap();
}

#[test]
fn a_market_close_is_accepted_and_not_checked_against_the_cap() {
    // A cap of one cent refuses any opening, so the close is accepted for its cap's sake.
    let mut cfg = config(root("market-close"));
    cfg.profile.max_notional_cents = 1;
    let gw = scripted(cfg, &clocked_holding());
    let mut c = Conn::open(&gw, "reactor", None);
    let r = c.submit(
        "mc",
        &market_of(single("wse-mc", CALL, "sell", "close", 10, "2.51")),
    );
    assert_eq!(r["outcome"], "accepted", "{r}");
    gw.stop().unwrap();
}

#[test]
fn a_limit_opening_is_still_checked_against_the_notional_cap() {
    // The limit form of the market opening above: 10 at $2.51 is $2,510, over the cap.
    let gw = scripted(config(root("market-limit")), &clocked_holding());
    let mut c = Conn::open(&gw, "reactor", None);
    let r = c.submit("ln", &single("wse-ln", CALL, "buy", "open", 10, "2.51"));
    assert_eq!(
        (r["reason"].as_str(), r["detail"]["cap"].as_str()),
        (Some("cap"), Some("notional")),
        "{r}"
    );
    assert_eq!(c.submit("lo", &buy("wse-lo"))["outcome"], "accepted");
    gw.stop().unwrap();
}

/// One day in nanoseconds: the next New York trading date after `T0` (a Monday, EDT).
const DAY: u64 = 86_400_000_000_000;

fn cleared_for(state: &std::path::Path, cid: &str) -> usize {
    let needle = format!(r#""client_order_id":"{cid}""#);
    journal(state)
        .iter()
        .filter(|l| l.contains(r#""kind":"cleared""#) && l.contains(&needle))
        .count()
}

#[test]
fn a_day_order_left_unresolved_is_cleared_once_its_session_has_passed() {
    let state = root("day-clear");
    // The first submit never reaches the broker, so every lookup is a 404.
    let script = format!(
        r#"{{"clock_ts":{T0},"positions":{{"{CALL}":1}},"rules":[{{"nth":1,"submit":"error_before_send"}}]}}"#
    );
    let gw = scripted(config(state.clone()), &script);
    let mut c = Conn::open(&gw, "reactor", None);
    assert_eq!(c.submit("r1", &buy("wse-day"))["outcome"], "unknown");
    c.event("resolution", |v| {
        v["client_order_id"] == "wse-day" && v["outcome"] == "absent_after"
    });
    // Same session: still counted, and not cleared.
    assert_eq!(c.req("open_orders", "q0", "").0["unresolved"], 1);
    assert_eq!(cleared_for(&state, "wse-day"), 0);
    // The New York date moves on, and the broker still has nothing.
    c.req("advance", "a1", &format!(r#""to_ts":{}"#, T0 + DAY));
    let ev = c.event("cleared", |v| v["client_order_id"] == "wse-day");
    assert_eq!(
        (ev["requester"].as_str(), ev["reason"].as_str()),
        (Some("gateway"), Some("day_order_after_session")),
        "{ev}"
    );
    assert_eq!(c.req("open_orders", "q1", "").0["unresolved"], 0);
    // Its leg can close, as the position is held.
    let v = c.submit(
        "r2",
        &single("wse-day-close", CALL, "sell", "close", 1, "1.00"),
    );
    assert_eq!(v["outcome"], "accepted", "{v}");
    gw.stop().unwrap();
    assert_eq!(cleared_for(&state, "wse-day"), 1);
}

/// A closing-auction (`cls`) stock limit buy on KO, which the test profile allows.
fn closing_auction_buy(cid: &str) -> String {
    format!(
        r#"{{"client_order_id":"{cid}","shape":"single","instrument":{{"class":"equity","symbol":"KO"}},"side":"buy","effect":"open","qty":1,"type":"limit","limit_price":"60.00","time_in_force":"cls"}}"#
    )
}

fn config_with_ko(state: std::path::PathBuf) -> order_gateway::Config {
    let mut cfg = config(state);
    cfg.profile.equities = vec!["KO".into()];
    cfg
}

#[test]
fn a_closing_auction_order_left_unresolved_is_cleared_once_its_session_has_passed() {
    let state = root("cls-clear");
    // The first submit never reaches the broker, so every lookup is a 404.
    let script =
        format!(r#"{{"clock_ts":{T0},"rules":[{{"nth":1,"submit":"error_before_send"}}]}}"#);
    let gw = scripted(config_with_ko(state.clone()), &script);
    let mut c = Conn::open(&gw, "reactor", None);
    assert_eq!(
        c.submit("r1", &closing_auction_buy("wse-cls"))["outcome"],
        "unknown"
    );
    c.event("resolution", |v| {
        v["client_order_id"] == "wse-cls" && v["outcome"] == "absent_after"
    });
    // Same session: still counted, and not cleared.
    assert_eq!(c.req("open_orders", "q0", "").0["unresolved"], 1);
    assert_eq!(cleared_for(&state, "wse-cls"), 0);
    // The New York date moves on, and the broker still has nothing.
    c.req("advance", "a1", &format!(r#""to_ts":{}"#, T0 + DAY));
    let ev = c.event("cleared", |v| v["client_order_id"] == "wse-cls");
    assert_eq!(
        (ev["requester"].as_str(), ev["reason"].as_str()),
        (Some("gateway"), Some("day_order_after_session")),
        "{ev}"
    );
    assert_eq!(c.req("open_orders", "q1", "").0["unresolved"], 0);
    gw.stop().unwrap();
    assert_eq!(cleared_for(&state, "wse-cls"), 1);
}

#[test]
fn a_day_and_a_closing_auction_order_from_a_prior_session_are_each_cleared() {
    let state = root("cls-and-day");
    // Two submits never reach the broker: a day option and a closing-auction stock.
    let script = format!(
        r#"{{"clock_ts":{T0},"rules":[{{"nth":1,"submit":"error_before_send"}},{{"nth":2,"submit":"error_before_send"}}]}}"#
    );
    let gw = scripted(config_with_ko(state.clone()), &script);
    let mut c = Conn::open(&gw, "reactor", None);
    assert_eq!(c.submit("r1", &buy("wse-d"))["outcome"], "unknown");
    // Each resolution is awaited before the next request: a request reads and discards
    // the events that arrive before its response, so a later wait would never see them.
    c.event("resolution", |v| {
        v["client_order_id"] == "wse-d" && v["outcome"] == "absent_after"
    });
    assert_eq!(
        c.submit("r2", &closing_auction_buy("wse-cls"))["outcome"],
        "unknown"
    );
    c.event("resolution", |v| {
        v["client_order_id"] == "wse-cls" && v["outcome"] == "absent_after"
    });
    c.req("advance", "a1", &format!(r#""to_ts":{}"#, T0 + DAY));
    // Both are past their session. One automatic clear runs per poll, so they clear on
    // successive polls. Waiting on the unresolved count, not on each event, since an event
    // read by one wait is not seen by the next.
    let mut q = c.req("open_orders", "q1", "").0;
    for n in 0..100 {
        if q["unresolved"] == 0 {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
        q = c.req("open_orders", &format!("q{}", n + 2), "").0;
    }
    assert_eq!(q["unresolved"], 0, "{q}");
    gw.stop().unwrap();
    assert_eq!(cleared_for(&state, "wse-d"), 1);
    assert_eq!(cleared_for(&state, "wse-cls"), 1);
}

#[test]
fn a_day_order_the_broker_still_shows_is_left_alone() {
    let state = root("day-shown");
    let first =
        format!(r#"{{"clock_ts":{T0},"rules":[{{"nth":1,"submit":"error_before_send"}}]}}"#);
    let gw = scripted(config(state.clone()), &first);
    let mut c = Conn::open(&gw, "reactor", None);
    assert_eq!(c.submit("r1", &buy("wse-live"))["outcome"], "unknown");
    c.event("resolution", |v| v["client_order_id"] == "wse-live");
    gw.stop().unwrap();
    // After a restart the next session has begun, and the broker holds the order.
    let next = format!(
        r#"{{"clock_ts":{},"orders":[{{"client_order_id":"wse-live","symbol":"{CALL}","side":"buy","qty":1,"limit_price":"1.25"}}]}}"#,
        T0 + DAY
    );
    let gw = scripted(config(state.clone()), &next);
    let mut c = Conn::open(&gw, "reactor", Some(0));
    c.event("order", |v| v["client_order_id"] == "wse-live");
    let (q, _) = c.req("open_orders", "q1", "");
    assert_eq!(q["unresolved"], 0, "{q}");
    assert_eq!(q["orders"][0]["client_order_id"], "wse-live", "{q}");
    gw.stop().unwrap();
    assert_eq!(cleared_for(&state, "wse-live"), 0);
}

/// The journaled `clear_refused` lines for one client order id.
fn refused_for(state: &std::path::Path, cid: &str) -> Vec<serde_json::Value> {
    let needle = format!(r#""client_order_id":"{cid}""#);
    journal(state)
        .iter()
        .filter(|l| l.contains(r#""kind":"clear_refused""#) && l.contains(&needle))
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

#[test]
fn a_clear_is_for_admins_and_refuses_what_the_broker_shows() {
    let state = root("clear-sim");
    let gw = scripted(config(state.clone()), &format!(r#"{{"clock_ts":{T0}}}"#));
    let mut c = Conn::open(&gw, "reactor", None);
    let mut ops = Conn::open(&gw, "ops", None);
    assert_eq!(c.submit("r1", &buy("wse-ok"))["outcome"], "accepted");
    let (v, _) = c.req(
        "clear_order",
        "x1",
        r#""client_order_id":"wse-ok","reason":"try""#,
    );
    assert_eq!(v["code"], "forbidden", "{v}");
    // The order is working at the broker, so the clear names the status it shows.
    let (v, _) = ops.req(
        "clear_order",
        "x2",
        r#""client_order_id":"wse-ok","reason":"test""#,
    );
    assert_eq!(
        (
            v["reason"].as_str(),
            v["detail"]["check"].as_str(),
            v["detail"]["status"].as_str()
        ),
        (Some("broker_shows_order"), Some("open_orders"), Some("new")),
        "{v}"
    );
    let (v, _) = ops.req(
        "clear_order",
        "x3",
        r#""client_order_id":"wse-none","reason":"test""#,
    );
    assert_eq!(v["reason"], "unknown_order", "{v}");
    let (v, _) = ops.req(
        "clear_order",
        "x4",
        r#""client_order_id":"wse-ok","reason":" ""#,
    );
    assert_eq!(v["code"], "invalid_request", "{v}");
    gw.stop().unwrap();
    // Every request is journaled, refused or not, with who asked and why.
    let refused = refused_for(&state, "wse-ok");
    let shape: Vec<(&str, &str, &str)> = refused
        .iter()
        .map(|r| {
            (
                r["requester"].as_str().unwrap(),
                r["reason"].as_str().unwrap(),
                r["request_reason"].as_str().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        shape,
        [
            ("reactor", "forbidden", "try"),
            ("ops", "broker_shows_order", "test"),
            ("ops", "invalid_request", " "),
        ],
        "{refused:?}"
    );
    assert_eq!(refused[0]["detail"], serde_json::Value::Null);
    assert_eq!(refused[1]["detail"]["check"], "open_orders");
    assert_eq!(refused[1]["broker"]["open_orders"]["result"], "shows");
    assert_eq!(refused[1]["broker"]["open_orders"]["status"], "new");
    assert_eq!(refused[2]["detail"]["path"], "reason");
    let unknown = refused_for(&state, "wse-none");
    assert_eq!(unknown.len(), 1, "{unknown:?}");
    assert_eq!(unknown[0]["reason"], "unknown_order");
}

#[test]
fn a_full_open_orders_listing_stops_both_the_automatic_and_the_operator_clear() {
    let state = root("listing-full");
    // Two submits never reach the broker: a day option and a closing-auction stock.
    let first = format!(
        r#"{{"clock_ts":{T0},"rules":[{{"nth":1,"submit":"error_before_send"}},{{"nth":2,"submit":"error_before_send"}}]}}"#
    );
    let gw = scripted(config_with_ko(state.clone()), &first);
    let mut c = Conn::open(&gw, "reactor", None);
    assert_eq!(c.submit("r1", &buy("wse-day"))["outcome"], "unknown");
    // Each resolution is awaited before the next request: a request reads and discards
    // the events that arrive before its response, so a later wait would never see them.
    c.event("resolution", |v| {
        v["client_order_id"] == "wse-day" && v["outcome"] == "absent_after"
    });
    assert_eq!(
        c.submit("r2", &closing_auction_buy("wse-cls"))["outcome"],
        "unknown"
    );
    c.event("resolution", |v| {
        v["client_order_id"] == "wse-cls" && v["outcome"] == "absent_after"
    });
    gw.stop().unwrap();
    // The next New York date. The broker's open listing is full: 500 other orders, so
    // the listing may hide either order.
    let others = (0..500)
        .map(|i| {
            format!(
                r#"{{"client_order_id":"other-{i}","symbol":"{CALL}","side":"buy","qty":1,"limit_price":"1.25"}}"#
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    let next = format!(r#"{{"clock_ts":{},"orders":[{others}]}}"#, T0 + DAY);
    let gw = scripted(config_with_ko(state.clone()), &next);
    let mut ops = Conn::open(&gw, "ops", None);
    // The operator's clear is refused: the listing cannot rule the order out.
    let (v, _) = ops.req(
        "clear_order",
        "x1",
        r#""client_order_id":"wse-day","reason":"test""#,
    );
    assert_eq!(
        (
            v["reason"].as_str(),
            v["detail"]["check"].as_str(),
            v["detail"]["code"].as_str()
        ),
        (
            Some("broker_unreachable"),
            Some("open_orders"),
            Some("listing_full")
        ),
        "{v}"
    );
    // Let several polls run: the automatic clear must leave both orders alone.
    std::thread::sleep(Duration::from_millis(300));
    gw.stop().unwrap();
    assert_eq!(cleared_for(&state, "wse-day"), 0);
    assert_eq!(cleared_for(&state, "wse-cls"), 0);
    let refused = refused_for(&state, "wse-day");
    assert_eq!(refused.len(), 1, "{refused:?}");
    assert_eq!(refused[0]["broker"]["open_orders"]["result"], "unreliable");
    let automatic = journal(&state)
        .iter()
        .filter(|l| {
            l.contains(r#""kind":"clear_refused""#) && l.contains(r#""requester":"gateway""#)
        })
        .count();
    assert_eq!(automatic, 0);
    // A restart reads the refusals and changes no state: both orders stay unresolved.
    let gw = scripted(config_with_ko(state.clone()), &next);
    let mut c = Conn::open(&gw, "reactor", None);
    let (q, _) = c.req("open_orders", "q9", "");
    assert_eq!(q["unresolved"], 2, "{q}");
    gw.stop().unwrap();
}
