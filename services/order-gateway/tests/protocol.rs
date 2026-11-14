//! Protocol conformance: a golden NDJSON transcript, framing and handshake errors,
//! strict request validation, roles and event visibility, loopback-only binding.
mod support;
use order_gateway::{SimBroker, check_paths, check_state_under, start};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;
use support::*;

const GOLDEN: &str = "tests/golden/scripted-session.ndjson";

/// One fixed-clock session in Reactor's two shapes; every byte the client receives.
#[test]
fn golden_scripted_transcript() {
    let script = format!(
        r#"{{"clock_ts":{T0},"quotes":{{"{CALL}":{{"bid":"1.20","ask":"1.24"}},"{PUT_A}":{{"bid":"0.93","ask":"0.95"}},"{PUT_B}":{{"bid":"0.50","ask":"0.52"}}}},"rules":[{{"nth":1,"submit":"fill"}},{{"nth":2,"submit":{{"partial":1}}}}]}}"#
    );
    let gw = scripted(config(root("golden")), &script);
    let mut c = Conn::open(&gw, "reactor", Some(0));
    c.submit("r-17", &buy("wse-20261005-0001"));
    c.order_event("wse-20261005-0001", "fill");
    c.submit("r-18", &credit_spread("wse-20261005-0002", 3));
    c.order_event("wse-20261005-0002", "partial_fill");
    c.req("cancel", "r-19", r#""client_order_id":"wse-20261005-0002""#);
    c.order_event("wse-20261005-0002", "canceled");
    c.submit(
        "r-20",
        &single("wse-20261005-0003", CALL, "buy", "open", 1, "1.255"),
    );
    c.submit("r-21", &buy("wse-20261005-0001"));
    c.req("order", "r-22", r#""client_order_id":"wse-20261005-0002""#);
    c.req("replace", "r-23", "");
    gw.stop().unwrap();
    let got = c.lines.join("\n") + "\n";
    if std::env::var_os("GATEWAY_GOLDEN_WRITE").is_some() {
        std::fs::write(GOLDEN, &got).unwrap();
    }
    let want = std::fs::read_to_string(GOLDEN).unwrap();
    assert_eq!(
        got, want,
        "transcript drifted; rerun with GATEWAY_GOLDEN_WRITE=1 after review"
    );
}

fn raw(gw: &order_gateway::Gateway, lines: &[&[u8]]) -> String {
    let mut s = TcpStream::connect(gw.addr()).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    for l in lines {
        s.write_all(l).unwrap();
    }
    let mut out = String::new();
    let _ = s.read_to_string(&mut out);
    out
}

fn hello() -> Vec<u8> {
    format!("{{\"schema\":\"tinytrader.gateway/1\",\"type\":\"hello\",\"client\":\"reactor\",\"token\":\"{TOKEN}\"}}\n").into_bytes()
}

#[test]
fn handshake_and_framing_errors_close_the_connection() {
    let mut cfg = config(root("framing"));
    cfg.timing.hello = Duration::from_millis(200);
    cfg.timing.idle = Duration::from_millis(400);
    let gw = scripted(cfg, "{}");
    let code = |out: String| -> String {
        let last = out.lines().last().unwrap_or_default().to_string();
        let v: serde_json::Value = serde_json::from_str(&last).unwrap();
        assert_eq!(v["type"], "error", "{out}");
        v["code"].as_str().unwrap().to_string()
    };
    assert_eq!(
        code(raw(
            &gw,
            &[b"{\"schema\":\"tinytrader.gateway/1\",\"type\":\"clock\",\"request_id\":\"a\"}\n"]
        )),
        "hello_required"
    );
    assert_eq!(code(raw(&gw, &[b"not json\n"])), "bad_json");
    assert_eq!(
        code(raw(
            &gw,
            &[b"{\"schema\":\"tinytrader.gateway/2\",\"type\":\"hello\"}\n"]
        )),
        "schema_unsupported"
    );
    let bad = format!(
        "{{\"schema\":\"tinytrader.gateway/1\",\"type\":\"hello\",\"client\":\"reactor\",\"token\":\"{}\"}}\n",
        "cd".repeat(32)
    );
    assert_eq!(code(raw(&gw, &[bad.as_bytes()])), "unauthorized");
    let stranger = format!(
        "{{\"schema\":\"tinytrader.gateway/1\",\"type\":\"hello\",\"client\":\"nobody\",\"token\":\"{TOKEN}\"}}\n"
    );
    assert_eq!(code(raw(&gw, &[stranger.as_bytes()])), "unauthorized");
    assert_eq!(code(raw(&gw, &[])), "hello_timeout");
    assert_eq!(code(raw(&gw, &[&hello()])), "idle_timeout");
    let long = vec![b'x'; 70_000];
    assert_eq!(code(raw(&gw, &[&hello(), &long])), "line_too_long");
    gw.stop().unwrap();
}

#[test]
fn request_errors_keep_the_connection_open() {
    let gw = scripted(config(root("requests")), &format!(r#"{{"clock_ts":{T0}}}"#));
    let mut c = Conn::open(&gw, "reactor", None);
    let (r, _) = c.req("clock", "dup", "");
    assert_eq!(r["outcome"], "ok");
    assert_eq!(r["clock"]["ts"], T0);
    assert_eq!(c.req("clock", "dup", "").0["code"], "duplicate_request_id");
    let (r, _) = c.req("clock", "x1", r#""extra":1"#);
    assert_eq!(
        (r["code"].as_str(), r["detail"]["path"].as_str()),
        (Some("invalid_request"), Some("extra"))
    );
    let (r, _) = c.req(
        "order",
        "x2",
        r#""client_order_id":"a","broker_order_id":"b""#,
    );
    assert_eq!(r["code"], "invalid_request");
    let cases = [
        (
            single("wse-1", CALL, "buy", "open", 0, "1.00"),
            "intent.qty",
        ),
        (
            single("wse-1", CALL, "buy", "open", 1, "0.00"),
            "intent.limit_price",
        ),
        (
            single("wse-1", "SPY261305C00580000", "buy", "open", 1, "1.00"),
            "intent.instrument.symbol",
        ),
        (
            single("wse-1", CALL, "hold", "open", 1, "1.00"),
            "intent.side",
        ),
        (
            combo(
                "wse-1",
                &[(PUT_A, 2, "buy", "open"), (PUT_B, 4, "sell", "open")],
                1,
                "0.10",
            ),
            "intent.legs",
        ),
        (
            combo(
                "wse-1",
                &[(PUT_A, 1, "buy", "open"), (PUT_A, 1, "sell", "open")],
                1,
                "0.10",
            ),
            "intent.legs",
        ),
        (
            combo(
                "wse-1",
                &[(PUT_A, 1, "buy", "open"), (PUT_B, 1, "sell", "open")],
                1,
                "0.00",
            ),
            "intent.limit_price",
        ),
        (buy("wse 1"), "intent.client_order_id"),
        (
            buy("wse-1").replace(r#""type":"limit""#, r#""type":"stop""#),
            "intent.type",
        ),
        // A market order names a reference price for the caps, not a limit.
        (
            buy("wse-1").replace(r#""type":"limit""#, r#""type":"market""#),
            "intent.ref_price",
        ),
        (
            buy("wse-1").replace(r#""day""#, r#""day","extra":true"#),
            "intent.extra",
        ),
    ];
    for (n, (intent, path)) in cases.iter().enumerate() {
        let r = c.submit(&format!("v{n}"), intent);
        assert_eq!(
            (r["reason"].as_str(), r["detail"]["path"].as_str()),
            (Some("validation"), Some(*path)),
            "{intent}"
        );
    }
    assert_eq!(c.submit("p1", &buy("oth-1"))["reason"], "prefix");
    let r = c.submit(
        "u1",
        &single("wse-2", "QQQ261005C00480000", "buy", "open", 1, "1.00"),
    );
    assert_eq!(
        (r["reason"].as_str(), r["detail"]["underlying"].as_str()),
        (Some("unsupported"), Some("QQQ"))
    );
    assert_eq!(
        c.req("advance", "a1", &format!(r#""to_ts":{}"#, T0 + 5)).0["market_ts"],
        T0 + 5
    );
    gw.stop().unwrap();
}

#[test]
fn roles_and_event_visibility() {
    let gw = scripted(config(root("roles")), &format!(r#"{{"clock_ts":{T0}}}"#));
    let mut watch = Conn::open(&gw, "watch", None);
    let mut other = Conn::open(&gw, "other", None);
    let mut c = Conn::open(&gw, "reactor", None);
    assert_eq!(watch.submit("w1", &buy("wse-w"))["code"], "forbidden");
    assert_eq!(
        watch
            .req("kill", "w2", r#""level":"halt_all","reason":"x""#)
            .0["code"],
        "forbidden"
    );
    assert_eq!(
        c.req("kill", "k1", r#""level":"halt_all","reason":"x""#).0["code"],
        "forbidden"
    );
    assert_eq!(
        c.req("unkill", "k2", r#""reason":"x""#).0["code"],
        "forbidden"
    );
    c.submit("r1", &buy("wse-1"));
    c.order_event("wse-1", "new");
    assert_eq!(
        other.req("cancel", "o1", r#""client_order_id":"wse-1""#).0["code"],
        "forbidden"
    );
    other.submit("o2", &buy("oth-1"));
    // The observer sees both clients' orders; a trade client sees only its own prefix.
    watch.order_event("wse-1", "new");
    watch.order_event("oth-1", "new");
    let mut ops = Conn::open(&gw, "ops", None);
    assert_eq!(
        ops.req("cancel", "a1", r#""client_order_id":"oth-1""#).0["outcome"],
        "accepted"
    );
    other.order_event("oth-1", "canceled");
    assert!(
        other.lines.iter().all(|l| !l.contains("wse-1")),
        "{:?}",
        other.lines
    );
    assert!(c.lines.iter().all(|l| !l.contains("oth-1")));
    gw.stop().unwrap();
}

#[test]
fn listen_any_is_the_only_way_to_bind_outside_loopback() {
    let mut refused = config(root("listen-any-refused"));
    refused.listen = "0.0.0.0:0".parse().unwrap();
    let err = start(refused, SimBroker::scripted("{}").unwrap())
        .err()
        .expect("a non-loopback listen without --listen-any is refused");
    assert!(err.contains("loopback"), "{err}");

    let mut allowed = config(root("listen-any-allowed"));
    allowed.listen = "0.0.0.0:0".parse().unwrap();
    allowed.listen_any = true;
    assert!(start(allowed, SimBroker::scripted("{}").unwrap()).is_ok());
}

#[test]
fn loopback_only_and_state_root_refusals() {
    let tmp = std::path::Path::new("/var/lib/gateway-test/state");
    assert!(
        check_paths("0.0.0.0:7601".parse().unwrap(), tmp)
            .unwrap_err()
            .contains("loopback")
    );
    assert!(check_paths("[::]:7601".parse().unwrap(), tmp).is_err());
    assert!(
        check_paths(
            "127.0.0.1:7601".parse().unwrap(),
            std::path::Path::new("/tmp/gw")
        )
        .unwrap_err()
        .contains("/tmp")
    );
    let bulk = std::path::PathBuf::from("/srv/bulk");
    assert!(
        check_state_under(
            std::path::Path::new("/srv/bulk/gw"),
            std::slice::from_ref(&bulk)
        )
        .unwrap_err()
        .contains("/srv/bulk")
    );
    assert!(check_state_under(std::path::Path::new("/srv/state/gw"), &[bulk]).is_ok());
    assert!(check_paths("[::1]:7601".parse().unwrap(), tmp).is_ok());
    let r = root("bind");
    let tok = r.join("t.token");
    std::fs::write(&tok, TOKEN).unwrap();
    std::fs::set_permissions(&tok, std::os::unix::fs::PermissionsExt::from_mode(0o600)).unwrap();
    let run = |listen: &str| {
        std::process::Command::new(env!("CARGO_BIN_EXE_order-gateway"))
            .args(["--account", "a", "--broker", "sim", "--listen", listen])
            .arg("--state")
            .arg(r.join("state"))
            .arg("--client")
            .arg(format!("reactor,trade,wse-,{}", tok.display()))
            .output()
            .unwrap()
    };
    let out = run("0.0.0.0:0");
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("loopback"));
    std::fs::set_permissions(&tok, std::os::unix::fs::PermissionsExt::from_mode(0o644)).unwrap();
    let out = run("127.0.0.1:0");
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("mode 600"));
    assert!(!String::from_utf8_lossy(&out.stderr).contains(TOKEN));
}
