//! A backtest session end to end, with a toy strategy written here: one day of
//! minute quotes from a cache job, served over the relay protocol, one simulated
//! venue, lockstep on the control connection, a timer, and a fill. The same
//! configuration twice gives the same audit hash.
use dbn::decode::{DbnDecoder, DecodeRecordRef};
use dbn::encode::EncodeRecord;
use dbn::encode::dbn::Encoder;
use dbn::{CbboMsg, MappingInterval, Metadata, RecordHeader, SType, Schema, SymbolMapping, rtype};
use order_gateway::client::Client;
use serde_json::{Value, json};
use session::Session;
use sha2::{Digest, Sha256};
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};

const DAY: u64 = 86_400_000_000_000;
const D27: u64 = 1_793_059_200_000_000_000; // 2026-10-27T00:00Z, a Tuesday
const MIN: u64 = 60_000_000_000;
/// 10:00 New York.
const T0: u64 = D27 + 14 * 3_600_000_000_000;
const CALL: &str = "SPY   261027C00580000";

fn root(name: &str) -> PathBuf {
    let p = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("session-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn sha(b: &[u8]) -> String {
    Sha256::digest(b)
        .iter()
        .map(|x| format!("{x:02x}"))
        .collect()
}

/// A cache job of one day: the call quoted every minute from 10:00, its ask 1.30
/// until 10:02, then 1.24; `extra` more contracts mapped, never quoted.
fn job(dir: &Path, extra: u32) {
    let date = time::Date::from_julian_day(i32::try_from(D27 / DAY).unwrap() + 2_440_588).unwrap();
    let meta = Metadata::builder()
        .dataset("OPRA.PILLAR")
        .schema(Some(Schema::Cbbo1M))
        .start(D27)
        .stype_in(Some(SType::Parent))
        .stype_out(SType::InstrumentId)
        .symbols(vec!["SPY.OPT".into()])
        .mappings(
            std::iter::once((CALL.to_owned(), 11))
                .chain((0..extra).map(|k| (format!("SPY   261027P{:08}", 100_000 + k), 1000 + k)))
                .map(|(raw, iid)| SymbolMapping {
                    raw_symbol: raw,
                    intervals: vec![MappingInterval {
                        start_date: date,
                        end_date: date.next_day().unwrap(),
                        symbol: iid.to_string(),
                    }],
                })
                .collect(),
        )
        .build();
    let mut out = Vec::new();
    {
        let mut e = Encoder::with_zstd(&mut out, &meta).unwrap();
        for m in 0..6 {
            let mut q = CbboMsg {
                hd: RecordHeader::new::<CbboMsg>(rtype::CBBO_1M, 30, 11, u64::MAX),
                ts_recv: T0 + m * MIN,
                ..CbboMsg::default_for_schema(Schema::Cbbo1M)
            };
            q.levels[0].bid_px = 1_200_000_000;
            q.levels[0].ask_px = if m < 2 { 1_300_000_000 } else { 1_240_000_000 };
            q.levels[0].bid_sz = 10;
            q.levels[0].ask_sz = 10;
            e.encode_record(&q).unwrap();
        }
    }
    let name = "opra-pillar-20261027.cbbo-1m.dbn.zst";
    std::fs::write(dir.join(name), &out).unwrap();
    std::fs::write(
        dir.join("metadata.json"),
        r#"{"query":{"dataset":"OPRA.PILLAR","schema":"cbbo-1m","symbols":["SPY.OPT"],"stype_in":"parent"}}"#,
    )
    .unwrap();
    std::fs::write(
        dir.join("manifest.json"),
        format!(
            r#"{{"files":[{{"filename":"{name}","hash":"sha256:{}"}}]}}"#,
            sha(&out)
        ),
    )
    .unwrap();
    std::fs::write(dir.join("COMPLETE"), "").unwrap();
}

fn config(job: &Path, timeout: u64) -> String {
    format!(
        r#"
days = ["2026-10-27"]
ready_timeout_s = {timeout}

[[sources]]
name = "opra"
holdings = "job:{}"
provider = "databento"
dataset = "OPRA.PILLAR"
symbol = "SPY.OPT"
schema = "cbbo-1m"

[[venues]]
name = "options"
quotes = ["opra"]
underlyings = ["SPY"]
[venues.sim]
fee_per_contract = "0.65"
# These tests are about lockstep; the toy's call isn't settled at expiry.
[venues.sim.expiry]
settle = false
"#,
        job.display()
    )
}

fn send(w: &mut TcpStream, v: &Value) {
    w.write_all(format!("{v}\n").as_bytes()).unwrap();
}

fn recv(r: &mut BufReader<TcpStream>) -> Value {
    let mut line = String::new();
    r.read_line(&mut line).unwrap();
    serde_json::from_str(&line).unwrap()
}

/// What the toy strategy saw.
#[derive(Debug, PartialEq)]
struct Seen {
    steps: Vec<(u64, u64, Vec<String>)>,
    fills: Vec<(String, u64)>,
    end: Value,
}

/// How the toy misbehaves, if it does.
#[derive(Clone, Copy, PartialEq)]
enum Fault {
    None,
    /// Stops answering at this step.
    Stall(u64),
    /// Sets a timer after the last day.
    FarTimer,
    /// Never reads the data stream.
    NoRead,
    /// Says it sent no orders after sending one.
    Undercount,
}

/// Buys one call at 1.25 on the 10:00 step, sets a timer for 10:02:30, and answers
/// every step, misbehaving as `fault` says.
fn toy(control: std::net::SocketAddr, fault: Fault) -> Seen {
    let stall_at = match fault {
        Fault::Stall(n) => Some(n),
        _ => None,
    };
    let mut ctl = TcpStream::connect(control).unwrap();
    let mut cr = BufReader::new(ctl.try_clone().unwrap());
    send(
        &mut ctl,
        &json!({"type": "hello", "protocol": "tinytrader.session/1", "strategy": "toy", "revision": "test"}),
    );
    let w = recv(&mut cr);
    assert_eq!(w["type"], "welcome", "{w}");
    let mut data = TcpStream::connect(w["data"].as_str().unwrap()).unwrap();
    data.write_all(b"{\"protocol\":\"tinytrader.relay/1\",\"client\":\"toy\"}\n")
        .unwrap();
    let mut dec = DbnDecoder::new(BufReader::new(data)).unwrap();
    let o = &w["orders"]["options"];
    let mut gw = Client::connect(
        o["addr"].as_str().unwrap().parse().unwrap(),
        o["client"].as_str().unwrap(),
        o["token"].as_str().unwrap(),
        None,
        false,
    )
    .unwrap();
    let welcome: Value = serde_json::from_str(&gw.line().unwrap().unwrap()).unwrap();
    assert_eq!(welcome["type"], "welcome");
    send(&mut ctl, &json!({"type": "ready", "as_of": 0, "sent": {}}));
    let (mut seen, mut last_seq, mut sent) = (
        Seen {
            steps: Vec::new(),
            fills: Vec::new(),
            end: Value::Null,
        },
        0,
        0,
    );
    loop {
        let step = recv(&mut cr);
        if step["type"] == "end" {
            seen.end = step;
            return seen;
        }
        let as_of = step["as_of"].as_u64().unwrap();
        if fault == Fault::NoRead {
            seen.end = recv(&mut cr);
            return seen;
        }
        for _ in 0..step["records"].as_u64().unwrap() {
            let rec = dec.decode_record_ref().unwrap().unwrap();
            assert!(rec.get::<CbboMsg>().is_some() || rec.header().rtype == rtype::SYMBOL_MAPPING);
        }
        // Every report of the step, before acting on it.
        let want = step["reports"]["options"].as_u64().unwrap();
        while last_seq < want {
            let v: Value = serde_json::from_str(&gw.line().unwrap().unwrap()).unwrap();
            if let Some(s) = v["seq"].as_u64() {
                last_seq = s;
            }
            if v["event"] == "fill" {
                seen.fills.push((
                    v["fill"]["price"].as_str().unwrap().to_owned(),
                    v["ts_broker"].as_u64().unwrap(),
                ));
            }
        }
        let timers: Vec<String> = step["timers"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t.as_str().unwrap().to_owned())
            .collect();
        let n = step["step"].as_u64().unwrap();
        seen.steps.push((n, as_of, timers));
        if stall_at == Some(n) {
            // Never answers; the session gives up.
            let end = recv(&mut cr);
            seen.end = end;
            return seen;
        }
        let mut new = Vec::new();
        if as_of == T0 {
            let intent = r#"{"client_order_id":"s-1","shape":"single","instrument":{"class":"option","symbol":"SPY261027C00580000"},"side":"buy","effect":"open","qty":1,"type":"limit","limit_price":"1.25","time_in_force":"day"}"#;
            gw.send(
                "submit",
                &format!(r#"{{"request_id":"r1","intent":{intent}}}"#),
            )
            .unwrap();
            if fault == Fault::Undercount {
                // Handled before the ready that leaves it out, so the next advance sees it.
                loop {
                    let v: Value = serde_json::from_str(&gw.line().unwrap().unwrap()).unwrap();
                    if v["type"] == "response" {
                        break;
                    }
                }
            } else {
                sent += 1;
            }
            new.push(json!({"at": T0 + 2 * MIN + MIN / 2, "id": "check"}));
            if fault == Fault::FarTimer {
                new.push(json!({"at": D27 + 2 * DAY, "id": "far"}));
            }
        }
        send(
            &mut ctl,
            &json!({"type": "ready", "as_of": as_of, "sent": {"options": sent}, "timers": new}),
        );
    }
}

fn run(name: &str, timeout: u64, fault: Fault) -> (session::Outcome, Seen, PathBuf) {
    run_with(name, timeout, fault, 0)
}

fn run_with(
    name: &str,
    timeout: u64,
    fault: Fault,
    extra: u32,
) -> (session::Outcome, Seen, PathBuf) {
    let base = root(name);
    let j = base.join("job");
    std::fs::create_dir_all(&j).unwrap();
    job(&j, extra);
    let dir = base.join("run");
    let s = Session::start(&config(&j, timeout), &dir, [127, 0, 0, 1].into()).unwrap();
    let control = s.ready().control;
    assert!(s.ready().line().starts_with("READY session="));
    let strategy = std::thread::spawn(move || toy(control, fault));
    let outcome = s.run().unwrap();
    (outcome, strategy.join().unwrap(), dir)
}

#[test]
fn a_session_runs_a_strategy_in_lockstep() {
    let (o, seen, dir) = run("lockstep", 30, Fault::None);
    assert_eq!(o.status, "complete", "{o:?}");
    assert_eq!(seen.end["status"], "complete");
    assert_eq!(seen.end["audit"], o.audit);
    // The day's symbol mappings at midnight, a step per minute, and the timer's own
    // step between 10:02 and 10:03.
    let times: Vec<u64> = seen.steps.iter().map(|s| s.1).collect();
    assert_eq!(
        times,
        [
            D27,
            T0,
            T0 + MIN,
            T0 + 2 * MIN,
            T0 + 2 * MIN + MIN / 2,
            T0 + 3 * MIN,
            T0 + 4 * MIN,
            T0 + 5 * MIN
        ]
    );
    assert_eq!(seen.steps[4].2, ["check"]);
    // Sent at 10:00, it rests until the ask comes down at 10:02, and the strategy
    // has the fill before it acts on that step.
    assert_eq!(seen.fills, [("1.24".to_owned(), T0 + 2 * MIN)]);
    assert_eq!((o.steps, o.records), (8, 7));
    let rec: Value =
        serde_json::from_str(&std::fs::read_to_string(dir.join("run.json")).unwrap()).unwrap();
    assert_eq!(rec["status"], "complete");
    assert_eq!(rec["audit"], o.audit);
    assert_eq!(rec["strategy"]["name"], "toy");
    assert_eq!(rec["sources"][0]["days"][0]["files"][0]["verified"], true);
    assert_eq!(rec["venues"][0]["sent"], 1);
    assert_eq!(rec["venues"][0]["model"], "tinytrader-sim/8");
    assert_eq!(rec["seed"], 0);
    // Each venue draws from its own seed, derived from the run's and its name.
    assert!(rec["venues"][0]["script"]["seed"].as_u64().is_some());
    // The end names each venue's last event after the final advance.
    assert!(seen.end["reports"]["options"].as_u64().unwrap() > 0);
}

#[test]
fn the_same_run_twice_gives_the_same_audit_hash() {
    let (a, sa, _) = run("twice-a", 30, Fault::None);
    let (b, sb, _) = run("twice-b", 30, Fault::None);
    assert_eq!(a.audit, b.audit);
    assert_eq!(sa, sb);
}

#[test]
fn a_strategy_that_stops_answering_leaves_the_run_incomplete() {
    let (o, seen, dir) = run("stall", 1, Fault::Stall(3));
    assert_eq!(o.status, "incomplete");
    assert!(
        o.reason.as_deref().unwrap_or_default().contains("control"),
        "{o:?}"
    );
    assert_eq!(seen.end["status"], "incomplete");
    let rec: Value =
        serde_json::from_str(&std::fs::read_to_string(dir.join("run.json")).unwrap()).unwrap();
    assert_eq!(rec["status"], "incomplete");
}

/// A step far larger than a socket's buffer (30,000 mappings at the day's start,
/// about 5 MB) flows: the step goes first, then its records as the strategy reads.
#[test]
fn a_step_larger_than_a_socket_buffer_flows() {
    let (o, seen, _) = run_with("big-step", 30, Fault::None, 30_000);
    assert_eq!(o.status, "complete", "{o:?}");
    assert_eq!(o.records, 6 + 30_001);
    assert_eq!(seen.fills.len(), 1);
}

fn reason(o: &session::Outcome) -> &str {
    o.reason.as_deref().unwrap_or_default()
}

#[test]
fn a_timer_after_the_last_day_ends_the_run() {
    let (o, _, _) = run("far-timer", 30, Fault::FarTimer);
    assert_eq!(o.status, "incomplete");
    assert!(reason(&o).contains("after the last day"), "{o:?}");
}

/// A strategy that stops reading its data ends the run at the write timeout, never
/// blocks it.
#[test]
fn a_strategy_that_stops_reading_leaves_the_run_incomplete() {
    let (o, _, _) = run_with("no-read", 1, Fault::NoRead, 30_000);
    assert_eq!(o.status, "incomplete");
    assert!(reason(&o).contains("data"), "{o:?}");
}

/// An order the strategy sent but didn't count is caught, not stamped on whichever
/// side of the clock it happened to land.
#[test]
fn an_order_sent_but_not_counted_ends_the_run() {
    let (o, _, _) = run("undercount", 5, Fault::Undercount);
    assert_eq!(o.status, "incomplete");
    assert!(
        reason(&o).contains("order messages handled, but the strategy said 0"),
        "{o:?}"
    );
}

/// The session's market calendar reaches its venues: a run day the calendar
/// doesn't list (a holiday) has no session, so the toy's order is refused as
/// outside it, and the record pins the calendar.
#[test]
fn the_calendar_closes_a_holiday() {
    let base = root("holiday");
    let j = base.join("job");
    std::fs::create_dir_all(&j).unwrap();
    job(&j, 0);
    let cal = base.join("calendar.json");
    std::fs::write(
        &cal,
        r#"{"days":[{"date":"2026-10-26","open":"09:30","close":"16:00"},{"date":"2026-10-28","open":"09:30","close":"16:00"}]}"#,
    )
    .unwrap();
    let config = config(&j, 30).replacen(
        "days = [\"2026-10-27\"]",
        &format!(
            "days = [\"2026-10-27\"]\ncalendar = {:?}",
            cal.display().to_string()
        ),
        1,
    );
    let dir = base.join("run");
    let s = Session::start(&config, &dir, [127, 0, 0, 1].into()).unwrap();
    let control = s.ready().control;
    let strategy = std::thread::spawn(move || toy(control, Fault::None));
    let o = s.run().unwrap();
    let seen = strategy.join().unwrap();
    assert_eq!(o.status, "complete", "{o:?}");
    assert!(seen.fills.is_empty());
    let journal =
        std::fs::read_to_string(dir.join("venues/options/journal/2026-10-27.jsonl")).unwrap();
    assert!(journal.contains("sim_market_closed"), "{journal}");
    let rec: Value =
        serde_json::from_str(&std::fs::read_to_string(dir.join("run.json")).unwrap()).unwrap();
    assert_eq!(rec["calendar_sha256"].as_str().unwrap().len(), 64);
    assert_eq!(
        rec["venues"][0]["script"]["calendar"][0]["date"],
        "2026-10-26"
    );
}
