//! `tt` against a fake console on loopback: price, quote, approve,
//! submitting, submitted. No network beyond it.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use axum::Json;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use clap::Parser;
use serde_json::{Value, json};

use super::*;

/// A price with more digits than cents: what's confirmed must be exactly it.
const COST: f64 = 12.345_678_901_234_567;

#[derive(Default)]
struct Fake {
    /// The price of each `/price` in turn (the last repeats).
    costs: Vec<f64>,
    prices: usize,
    /// Each approval's body and its order token.
    approves: Vec<(Value, Option<String>)>,
    submits: usize,
    /// The gate refuses approvals: 403, or 401 without this token.
    forbid: bool,
    token: Option<&'static str>,
    /// The first approval finds the price expired.
    expire_first: bool,
    jobs: Value,
    /// The jobs list is empty for this many requests (a job just submitted).
    hide: usize,
    listed: usize,
    /// The quotes bought.
    done: Vec<String>,
}

type Shared = Arc<Mutex<Fake>>;

fn quote(id: usize, cost: f64, state: &str) -> Value {
    json!({ "quote": format!("q-t-{id}"), "state": state, "priced": 1, "of": 2,
        "dataset": "OPRA.PILLAR", "schema": "cbbo-1m", "symbol": "SPY.OPT", "start": "2026-10-01", "end": "2026-10-02",
        "rule": false, "again": [], "cost_usd": cost, "needs_approval": cost != 0.0, "expires_at": now() + 3600,
        "requests": [{ "kind": "whole", "first_day": "2026-10-01", "last_day": "2026-10-02", "cost_usd": cost }],
        "notes": ["Databento marks 1 day degraded: 2026-10-01", "2026-10-02: not bought, it overlaps a", "2026-10-02: not bought, it overlaps b"],
        "submitted": if state == "submitted" { json!(1) } else { Value::Null }, "jobs": ["J1"] })
}

fn gate(code: StatusCode, why: &str) -> Response {
    (
        code,
        [("x-write-gate", "refused")],
        Json(json!({ "error": why, "gate": true })),
    )
        .into_response()
}

async fn price(State(f): State<Shared>) -> Response {
    let mut f = f.lock().unwrap();
    f.prices += 1;
    (StatusCode::ACCEPTED, Json(json!({ "quote": format!("q-t-{}", f.prices), "state": "pricing", "priced": 0, "of": 2 }))).into_response()
}

async fn one(State(f): State<Shared>, Path(id): Path<String>) -> Json<Value> {
    let f = f.lock().unwrap();
    let n: usize = id.trim_start_matches("q-t-").parse().unwrap();
    let bought = f.done.contains(&id) || !f.jobs.is_null();
    let cost = f.costs[(n - 1).min(f.costs.len() - 1)];
    Json(quote(n, cost, if bought { "submitted" } else { "ready" }))
}

async fn approve(
    State(f): State<Shared>,
    Path(id): Path<String>,
    h: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let mut f = f.lock().unwrap();
    let token = h
        .get("x-order-token")
        .map(|v| v.to_str().unwrap().to_owned());
    f.approves.push((body.clone(), token.clone()));
    if f.forbid {
        return gate(
            StatusCode::FORBIDDEN,
            "writes are not allowed for this login",
        );
    }
    if f.token.is_some_and(|t| token.as_deref() != Some(t)) {
        return gate(
            StatusCode::UNAUTHORIZED,
            "this write needs the console's order token",
        );
    }
    if f.expire_first && f.approves.len() == 1 {
        return (
            StatusCode::CONFLICT,
            Json(json!({ "error": "that price has expired; price the request again" })),
        )
            .into_response();
    }
    f.done.push(id);
    Json(json!({ "submitted": [{ "job": "J1", "first_day": "2026-10-01", "last_day": "2026-10-02", "cost_usd": body["confirm_usd"] }], "cost_usd": body["confirm_usd"] })).into_response()
}

async fn submit(State(f): State<Shared>, Json(body): Json<Value>) -> Json<Value> {
    let mut f = f.lock().unwrap();
    f.submits += 1;
    f.done.push(text(&body["quote"]));
    Json(json!({ "submitted": [{ "job": "J1" }], "cost_usd": 0.0 }))
}

/// A fake console with these prices, changed by `with` before it serves.
async fn serve(costs: &[f64], with: impl FnOnce(&mut Fake)) -> (String, Shared) {
    let (addr, f) = listen(costs, with).await;
    (format!("http://{addr}"), f)
}

async fn listen(costs: &[f64], with: impl FnOnce(&mut Fake)) -> (std::net::SocketAddr, Shared) {
    let mut f = Fake {
        costs: costs.to_vec(),
        ..Fake::default()
    };
    with(&mut f);
    let f = Arc::new(Mutex::new(f));
    let app = axum::Router::new()
        .route("/api/jobs/price", post(price))
        .route(
            "/api/jobs/quotes",
            get(|State(f): State<Shared>| async move {
                Json(json!({ "quotes": [quote(1, f.lock().unwrap().costs[0], "ready")] }))
            }),
        )
        .route("/api/jobs/quotes/{id}", get(one))
        .route("/api/jobs/submit", post(submit))
        .route(
            "/api/jobs/jobs",
            get(|State(f): State<Shared>| async move {
                let mut f = f.lock().unwrap();
                f.listed += 1;
                Json(if f.listed <= f.hide {
                    json!({ "jobs": [] })
                } else {
                    f.jobs.clone()
                })
            }),
        )
        .route("/api/job-quotes/{id}/approve", post(approve))
        .with_state(f.clone());
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    (addr, f)
}

/// A person at a terminal (or not), with their answers.
#[derive(Default)]
struct Script {
    tty: bool,
    answers: VecDeque<&'static str>,
    said: Vec<String>,
    asked: Vec<String>,
    secrets: usize,
}

impl Term for Script {
    fn tty(&self) -> bool {
        self.tty
    }
    fn say(&mut self, line: &str) {
        self.said.push(line.to_owned());
    }
    fn ask(&mut self, prompt: &str) -> Option<String> {
        self.asked.push(prompt.to_owned());
        self.answers.pop_front().map(str::to_owned)
    }
    fn input(&mut self) -> Result<String, String> {
        Ok(String::new())
    }
    fn secret(&mut self, _: &str) -> Option<String> {
        self.secrets += 1;
        Some("made-up-token".into())
    }
}

fn person(answers: &[&'static str]) -> Script {
    Script {
        tty: true,
        answers: answers.iter().copied().collect(),
        ..Script::default()
    }
}

async fn tt(base: &str, args: &str, term: &mut Script) -> (u8, String) {
    at(base, args, term, false).await
}

/// `tt`, its stdout a terminal or not.
async fn at(base: &str, args: &str, term: &mut Script, out_tty: bool) -> (u8, String) {
    through(base, reqwest::Client::new(), args, term, out_tty).await
}

/// `tt` with this HTTP client.
async fn through(
    base: &str,
    http: reqwest::Client,
    args: &str,
    term: &mut Script,
    out_tty: bool,
) -> (u8, String) {
    let cli = Cli::try_parse_from(std::iter::once("tt").chain(args.split_whitespace())).unwrap();
    let c = Console {
        base: base.into(),
        http,
        token: Mutex::new(None),
        poll: Duration::from_millis(5),
    };
    let mut out = Vec::new();
    let code = exit(cli, &c, term, &mut out, out_tty).await;
    (code, String::from_utf8(out).unwrap())
}

const FETCH: &str = "fetch databento --dataset OPRA.PILLAR --schema cbbo-1m --symbols SPY.OPT --start 2026-10-01 --end 2026-10-02 --hours 09:30-16:01";

fn said(t: &Script, part: &str) -> bool {
    t.said.iter().any(|l| l.contains(part))
}

#[tokio::test]
async fn a_paid_fetch_confirms_exactly_the_quoted_price() {
    let (base, f) = serve(&[COST], |_| {}).await;
    let mut t = person(&["y"]);
    let (code, out) = tt(&base, FETCH, &mut t).await;
    assert_eq!(code, 0, "{:?}", t.said);
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(
        (&v["quote"], &v["submitted"][0]["job"]),
        (&json!("q-t-1"), &json!("J1"))
    );
    let body = f.lock().unwrap().approves[0].0.clone();
    assert_eq!(
        body["confirm_usd"].as_f64().map(f64::to_bits),
        Some(COST.to_bits())
    );
    assert!(body.get("approved_by").is_none(), "{body}");
    let ask = "Buy OPRA.PILLAR cbbo-1m SPY.OPT, 2026-10-01 → 2026-10-02, 1 requests, for ≈$12.35? (expires ";
    assert!(
        t.asked[0].starts_with(ask) && t.asked[0].ends_with(" ET) [y/N]"),
        "{:?}",
        t.asked
    );
    for line in [
        "pricing: 0 of 2",
        "degraded: 2026-10-01",
        "2 notes on what was skipped",
    ] {
        assert!(said(&t, line), "{line}: {:?}", t.said);
    }
}

#[tokio::test]
async fn only_y_or_yes_buys() {
    for (answer, code) in [
        ("n", 1),
        ("", 1),
        ("sure", 1),
        ("yes please", 1),
        ("YES", 0),
    ] {
        let (base, f) = serve(&[COST], |_| {}).await;
        let (got, _) = tt(&base, FETCH, &mut person(&[answer])).await;
        let approves = f.lock().unwrap().approves.len();
        assert_eq!(
            (got, approves),
            (code, usize::from(code == 0)),
            "{answer:?}"
        );
    }
}

#[tokio::test]
async fn without_a_terminal_a_paid_purchase_is_refused_and_a_free_one_goes_ahead() {
    let (base, f) = serve(&[COST], |_| {}).await;
    let mut t = Script::default();
    let (code, out) = tt(&base, FETCH, &mut t).await;
    assert_eq!(code, 3);
    assert!(said(&t, "needs a person at a terminal"), "{:?}", t.said);
    // The quote is the answer, for whoever approves it.
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(
        (&v["quote"], &v["cost_usd"]),
        (&json!("q-t-1"), &json!(COST))
    );
    assert!(t.asked.is_empty() && f.lock().unwrap().approves.is_empty());
    let (base, f) = serve(&[0.0], |_| {}).await;
    assert_eq!(tt(&base, FETCH, &mut Script::default()).await.0, 0);
    let f = f.lock().unwrap();
    assert_eq!((f.submits, f.approves.len()), (1, 0));
}

#[tokio::test]
async fn the_gates_403_says_why_and_is_never_retried() {
    let (base, f) = serve(&[COST], |f| f.forbid = true).await;
    let mut t = person(&["y"]);
    assert_eq!(tt(&base, "approve q-t-1", &mut t).await.0, 3);
    let why = format!("writes are not allowed for this login: {PROXY_ONLY}");
    assert!(said(&t, &why), "{:?}", t.said);
    assert_eq!(f.lock().unwrap().approves.len(), 1);
}

#[tokio::test]
async fn the_order_token_is_asked_for_once_and_sent_as_a_header() {
    let (base, f) = serve(&[COST], |f| f.token = Some("made-up-token")).await;
    let mut t = person(&["y"]);
    let (code, out) = tt(&base, "approve q-t-1", &mut t).await;
    assert_eq!(
        (code, t.secrets, out.contains("made-up-token")),
        (0, 1, false)
    );
    let tokens: Vec<_> = f
        .lock()
        .unwrap()
        .approves
        .iter()
        .map(|a| a.1.clone())
        .collect();
    assert_eq!(tokens, [None, Some("made-up-token".into())]);
}

#[tokio::test]
async fn an_expired_price_is_priced_again_and_asked_again() {
    let (base, f) = serve(&[5.0, 6.5], |f| f.expire_first = true).await;
    let mut t = person(&["y", "y", "y"]);
    assert_eq!(tt(&base, FETCH, &mut t).await.0, 0);
    assert!(said(&t, "that price has expired"));
    let a = &t.asked;
    assert!(
        a[0].contains("for $5.00?")
            && a[1] == "Price it again (free)? [y/N]"
            && a[2].contains("for $6.50?"),
        "{a:?}"
    );
    {
        let f = f.lock().unwrap();
        assert_eq!(
            (f.prices, f.approves[1].0["confirm_usd"].as_f64()),
            (2, Some(6.5))
        );
    }
    // Not priced again, nor bought, unasked.
    let (base, f) = serve(&[5.0, 6.5], |f| f.expire_first = true).await;
    assert_eq!(tt(&base, FETCH, &mut person(&["y", "n"])).await.0, 3);
    let f = f.lock().unwrap();
    assert_eq!((f.prices, f.approves.len()), (1, 1));
}

#[tokio::test]
async fn quotes_are_a_table_at_a_terminal_and_json_elsewhere() {
    let (base, _) = serve(&[COST], |_| {}).await;
    let (code, out) = tt(&base, "quotes", &mut Script::default()).await;
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!((code, &v["quotes"][0]["quote"]), (0, &json!("q-t-1")));
    let (_, out) = at(&base, "quotes --format json", &mut Script::default(), true).await;
    assert!(serde_json::from_str::<Value>(&out).is_ok(), "{out}");
    let (_, out) = at(&base, "quotes", &mut Script::default(), true).await;
    let lines: Vec<&str> = out.lines().collect();
    assert!(
        lines[0].starts_with("QUOTE")
            && lines[1].contains("≈$12.35")
            && lines[1].contains("min left"),
        "{out}"
    );
}

#[tokio::test]
async fn following_jobs_ends_with_each_outcome_and_fails_if_any_failed() {
    let jobs = json!({ "jobs": [
        { "id": "J1", "state": "done", "held": { "how": "filed", "volume": "M1" } },
        { "id": "J2", "state": "done", "transfer": { "state": "failed", "error": "the store refused it" } },
    ] });
    let (base, _) = serve(&[COST], |f| f.jobs = jobs).await;
    let (code, out) = tt(&base, "jobs --follow J1 J2", &mut Script::default()).await;
    let v: Value = serde_json::from_str(&out).unwrap();
    let got: Vec<_> = v["jobs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|j| (text(&j["id"]), text(&j["outcome"])))
        .collect();
    assert_eq!(code, 1);
    assert_eq!(
        got,
        [
            ("J2".into(), "failed".into()),
            ("J1".into(), "filed".into())
        ]
    );
    // A quote's jobs, by its ID.
    let (code, _) = tt(&base, "jobs --follow q-t-1", &mut Script::default()).await;
    assert_eq!(code, 0);
}

#[tokio::test]
async fn api_passes_through_with_the_same_guards() {
    let (base, _) = serve(&[COST], |f| f.forbid = true).await;
    let (code, out) = tt(
        &base,
        "api GET /api/jobs/quotes/q-t-1",
        &mut Script::default(),
    )
    .await;
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!((code, &v["cost_usd"]), (0, &json!(COST)));
    // The gate's 403 on another write is still said, and exits 3.
    let mut t = Script::default();
    let (code, _) = tt(&base, "api POST /api/jobs/price", &mut t).await;
    assert!(code == 0, "{:?}", t.said);
    let mut t = Script::default();
    assert_eq!(tt(&base, "api GET /elsewhere", &mut t).await.0, 2);
    assert!(t.said[0].starts_with("tt: "), "{:?}", t.said);
}

#[tokio::test]
async fn api_never_approves_declines_or_submits() {
    let (base, f) = serve(&[0.0], |_| {}).await;
    for path in [
        "/api/job-quotes/q-t-1/approve",
        "/api/job-quotes/q-t-1/decline",
        "/API/Job-Quotes/q-t-1/Approve/",
        "/api//job-quotes//q-t-1/approve",
        "/api/jobs/../job-quotes/q-t-1/approve",
        "/api/jobs/%2e%2e/job-quotes/q-t-1/approve",
        "/api/jobs/%252e%252e/job-quotes/q-t-1/approve",
        "/api/job%2Dquotes/q-t-1/approve",
        "/api/job-quotes%2Fq-t-1%2Fapprove",
        "/api/jobs/submit",
        "/api/jobs/submit/",
        "/api/jobs/./submit?x=1",
        "/api/jobs/sub%6Dit",
        "/api/jobs/SUBMIT",
        "/api/queue/../jobs/submit",
        "/api/jobs\\submit",
    ] {
        let mut t = Script::default();
        let (code, out) = tt(&base, &format!("api POST {path}"), &mut t).await;
        assert!(
            code == 3
                && out.is_empty()
                && said(&t, "`tt approve QUOTE`")
                && said(&t, "`tt decline QUOTE`"),
            "{path}: {code} {:?}",
            t.said
        );
    }
    {
        let f = f.lock().unwrap();
        assert_eq!((f.approves.len(), f.submits, f.prices), (0, 0, 0));
    }
    // Reads of the same places, and other writes, go through.
    for (args, want) in [
        ("api GET /api/jobs/quotes/q-t-1", 0),
        ("api GET /api/jobs/jobs", 0),
        ("api POST /api/jobs/price", 0),
    ] {
        let mut t = Script::default();
        assert_eq!(
            tt(&base, args, &mut t).await.0,
            want,
            "{args}: {:?}",
            t.said
        );
    }
    assert!(!buys("/api/jobs/submitted") && !buys("/api/jobs/quotes/q-1"));
}

#[tokio::test]
async fn the_order_token_goes_only_over_https_or_to_this_machine() {
    for (base, ok) in [
        ("https://console.example", true),
        ("http://127.0.0.1:9", true),
        ("http://[::1]:9", true),
        ("http://LocalHost:9", true),
        ("http://console.example", false),
        ("http://127.0.0.1.example", false),
        ("ftp://127.0.0.1", false),
    ] {
        assert_eq!(private(base), ok, "{base}");
    }
    // A console asking for the token over plain http elsewhere isn't given it.
    let (addr, f) = listen(&[COST], |f| f.token = Some("made-up-token")).await;
    let http = reqwest::Client::builder()
        .resolve("console.example", addr)
        .build()
        .unwrap();
    let base = format!("http://console.example:{}", addr.port());
    let mut t = person(&["y"]);
    let (code, _) = through(&base, http, "approve q-t-1", &mut t, false).await;
    assert!(code == 3 && said(&t, UNSEALED), "{code} {:?}", t.said);
    assert_eq!(t.secrets, 0);
    let tokens: Vec<_> = f
        .lock()
        .unwrap()
        .approves
        .iter()
        .map(|a| a.1.clone())
        .collect();
    assert_eq!(tokens, [None]);
}

#[tokio::test]
async fn following_a_job_that_isnt_listed_fails_after_a_grace() {
    let jobs = json!({ "jobs": [{ "id": "J1", "state": "done", "held": { "how": "filed", "volume": "M1" } }] });
    let (base, _) = serve(&[COST], |f| f.jobs = jobs.clone()).await;
    let mut t = Script::default();
    let (code, out) = tt(&base, "jobs --follow J1 J9", &mut t).await;
    assert!(
        code == 1 && out.is_empty() && said(&t, "no such job: J9"),
        "{code} {:?}",
        t.said
    );
    // One just submitted, not listed at first, is waited for.
    let (base, f) = serve(&[COST], |f| (f.jobs, f.hide) = (jobs, 2)).await;
    let mut t = Script::default();
    assert_eq!(
        tt(&base, "jobs --follow J1", &mut t).await.0,
        0,
        "{:?}",
        t.said
    );
    assert!(f.lock().unwrap().listed > 2);
}

/// `tt store ls`: the store's `/files`, as a table or JSON; a refusal is
/// exit 1.
#[tokio::test]
async fn store_ls_lists_the_store_s_files() {
    let app = axum::Router::new().route(
        "/files",
        get(|q: axum::extract::Query<std::collections::HashMap<String, String>>| async move {
            if q.get("pattern").map(String::as_str) == Some("none") {
                return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({ "error": "the store is still scanning" })));
            }
            (StatusCode::OK, Json(json!({ "files": [{ "name": "a/Databento/x/2026-10-02.dbn.zst", "volume": "a", "path": "Databento/x/2026-10-02.dbn.zst", "bytes": 42 }], "truncated": false })))
        }),
    );
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", l.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    let ls = |pattern: &str| StoreCommand::Ls {
        pattern: pattern.into(),
        limit: None,
    };
    let mut out = Vec::new();
    assert_eq!(store(&base, &ls("*"), Format::Table, &mut out).await, 0);
    let text = String::from_utf8(out).unwrap();
    assert!(
        text.contains("a/Databento/x/2026-10-02.dbn.zst  42"),
        "{text}"
    );
    let mut out = Vec::new();
    assert_eq!(store(&base, &ls("*"), Format::Json, &mut out).await, 0);
    let v: Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(v["files"][0]["bytes"], 42);
    assert_eq!(
        store(&base, &ls("none"), Format::Json, &mut Vec::new()).await,
        1
    );
}
