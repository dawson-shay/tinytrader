//! The purchase paths (decision 0019) against a fake Databento (job list,
//! job details, cost, submit, with delays and failures) and a fake store
//! (`/catalog`, `/coverage`, `/files`) on loopback. Nothing here reaches the
//! network or a real disk outside a temporary folder.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;

use axum::extract::RawQuery;
use databento::historical::batch::{BatchFileDesc, BatchJobShort, ListJobsParams};
use databento::historical::metadata::{
    DatasetCondition, DatasetConditionDetail, DatasetRange, GetDatasetConditionParams,
};

use super::*;
use crate::api::Fut;

const DS: &str = "OPRA.PILLAR";
const SCHEMA: &str = "cmbp-1";
const DAY: &str = "2026-10-02";
const C1: &str = "SPY   261002C00728000";
const C2: &str = "SPY   261002P00728000";
const HOUR: i64 = 3_600_000_000_000;

fn date(s: &str) -> time::Date {
    time::Date::parse(s, time::macros::format_description!("[year]-[month]-[day]")).unwrap()
}

fn iso(ns: i64) -> String {
    let t = had::at(ns);
    format!(
        "{}T{:02}:{:02}:{:02}.{:09}Z",
        t.date(),
        t.hour(),
        t.minute(),
        t.second(),
        t.nanosecond()
    )
}

/// A job as Databento's job details describe it.
fn job(id: &str, symbols: Value, stype_in: &str, w: had::Window, state: &str) -> BatchJob {
    let now = had::ns(time::OffsetDateTime::now_utc());
    serde_json::from_value(json!({
        "id": id, "user_id": null, "cost_usd": 0.0, "dataset": DS, "symbols": symbols,
        "stype_in": stype_in, "stype_out": "instrument_id", "schema": SCHEMA,
        "start": iso(w.0), "end": iso(w.1), "limit": null, "encoding": "dbn", "compression": "zstd",
        "pretty_px": false, "pretty_ts": false, "map_symbols": false, "split_symbols": false,
        "split_duration": "day", "split_size": null, "delivery": "download", "record_count": null,
        "billed_size": null, "actual_size": null, "package_size": null, "state": state,
        "ts_received": iso(now), "ts_queued": null, "ts_process_start": null,
        "ts_process_done": null, "ts_expiration": null, "progress": null,
    }))
    .unwrap()
}

#[derive(Default)]
struct FakeState {
    jobs: Vec<BatchJob>,
    /// Jobs Databento made but doesn't list yet.
    hidden: HashSet<String>,
    unreadable: HashSet<String>,
    list_fails: bool,
    price: f64,
    /// Each price waits (up to 300 ms) until this many have been asked
    /// for: two purchases checked at once both get this far.
    cost_gate: usize,
    costs_asked: usize,
    /// Databento makes the job but the answer is lost.
    submit_lost: bool,
    /// Databento doesn't answer a submission naming this symbol at all.
    hang_on: Option<String>,
    /// The submission (counting from 0) that hangs (`true`) or is refused.
    stop_at: Option<(usize, bool)>,
    /// Each job's files, as Databento lists them.
    files: HashMap<String, Vec<BatchFileDesc>>,
    /// Listing these jobs' files fails with this HTTP status and message.
    files_fail: HashMap<String, (Option<u16>, String)>,
    costed: Vec<(Vec<String>, had::Window)>,
    /// Errors the next prices answer with, in turn.
    cost_errors: std::collections::VecDeque<String>,
    /// Prices the next requests get, in turn, before `price`.
    prices: std::collections::VecDeque<f64>,
    submitted: Vec<SubmitJobParams>,
    listed_with: Vec<ListJobsParams>,
    /// Day conditions, for every dataset; none: the call fails.
    conditions: Option<Vec<DatasetConditionDetail>>,
    conditions_asked: Vec<String>,
}

#[derive(Default)]
struct Fake {
    s: Mutex<FakeState>,
    next: AtomicUsize,
}

impl Fake {
    fn add(&self, j: BatchJob) {
        lock(&self.s).jobs.push(j);
    }
    fn submits(&self) -> usize {
        lock(&self.s).submitted.len()
    }
}

fn not_faked<'a, T: Send + 'a>() -> Fut<'a, T> {
    Box::pin(async { Err("not faked".to_owned()) })
}

impl api::Databento for Fake {
    fn files_checked<'a>(&'a self, id: &'a str) -> api::Checked<'a, Vec<BatchFileDesc>> {
        Box::pin(async move {
            if let Some((status, message)) = lock(&self.s).files_fail.get(id).cloned() {
                return Err(api::CallError { status, message });
            }
            self.files(id).await.map_err(api::CallError::from)
        })
    }
    fn list_jobs<'a>(&'a self, p: &'a ListJobsParams) -> Fut<'a, Vec<BatchJobShort>> {
        Box::pin(async move {
            let mut s = lock(&self.s);
            s.listed_with.push(p.clone());
            if s.list_fails {
                return Err("503 Service Unavailable".into());
            }
            // As Databento: without states, expired jobs aren't listed.
            let states = p
                .states
                .clone()
                .unwrap_or_else(|| vec![JobState::Queued, JobState::Processing, JobState::Done]);
            Ok(s.jobs
                .iter()
                .filter(|j| !s.hidden.contains(&j.id) && states.contains(&j.state))
                .map(|j| BatchJobShort {
                    id: j.id.clone(),
                    state: j.state,
                    ts_received: j.ts_received,
                })
                .collect())
        })
    }
    fn job<'a>(&'a self, id: &'a str) -> Fut<'a, BatchJob> {
        Box::pin(async move {
            let s = lock(&self.s);
            if s.unreadable.contains(id) {
                return Err("timed out".into());
            }
            s.jobs
                .iter()
                .find(|j| j.id == id)
                .cloned()
                .ok_or_else(|| "no such job".into())
        })
    }
    fn files<'a>(&'a self, id: &'a str) -> Fut<'a, Vec<BatchFileDesc>> {
        Box::pin(async move {
            lock(&self.s)
                .files
                .get(id)
                .cloned()
                .ok_or_else(|| "no such job".to_owned())
        })
    }
    fn submit<'a>(&'a self, p: &'a SubmitJobParams) -> Fut<'a, BatchJob> {
        Box::pin(async move {
            let hang = lock(&self.s).hang_on.clone();
            if hang.is_some_and(|h| p.symbols.to_api_string().contains(&h)) {
                tokio::time::sleep(Duration::from_secs(3600)).await;
            }
            let (n, stop) = {
                let s = lock(&self.s);
                (s.submitted.len(), s.stop_at)
            };
            match stop {
                Some((at, true)) if at == n => tokio::time::sleep(Duration::from_secs(3600)).await,
                Some((at, false)) if at == n => {
                    return Err("API error: 429 Too Many Requests: at most 20 a minute".into());
                }
                _ => {}
            }
            let id = format!("OPRA-NEW{}", self.next.fetch_add(1, Ordering::Relaxed));
            let mut j = job(
                &id,
                json!(p.symbols.to_api_string()),
                p.stype_in.as_str(),
                (
                    had::ns(p.date_time_range.start),
                    had::ns(p.date_time_range.end),
                ),
                "queued",
            );
            j.dataset.clone_from(&p.dataset);
            let mut s = lock(&self.s);
            s.submitted.push(p.clone());
            s.jobs.push(j.clone());
            if s.submit_lost {
                s.hidden.insert(id);
                return Err("connection reset".into());
            }
            Ok(j)
        })
    }
    fn cost<'a>(&'a self, p: &'a GetCostParams) -> Fut<'a, (f64, u64)> {
        Box::pin(async move {
            let gate = {
                let mut s = lock(&self.s);
                s.costs_asked += 1;
                s.cost_gate
            };
            for _ in 0..60 {
                if lock(&self.s).costs_asked >= gate {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            let mut s = lock(&self.s);
            let w = (
                had::ns(p.date_time_range.start),
                had::ns(p.date_time_range.end),
            );
            let symbols = p
                .symbols
                .to_api_string()
                .split(',')
                .map(str::to_owned)
                .collect();
            s.costed.push((symbols, w));
            match s.cost_errors.pop_front() {
                Some(e) => Err(e),
                None => {
                    let p = s.prices.pop_front().unwrap_or(s.price);
                    Ok((p, 1000))
                }
            }
        })
    }
    fn datasets(&self) -> Fut<'_, Vec<String>> {
        Box::pin(async { Ok(vec![DS.to_owned(), "XNAS.ITCH".to_owned()]) })
    }
    fn schemas<'a>(&'a self, _: &'a str) -> Fut<'a, Vec<Schema>> {
        not_faked()
    }
    fn range<'a>(&'a self, _: &'a str) -> Fut<'a, DatasetRange> {
        not_faked()
    }
    fn conditions<'a>(
        &'a self,
        p: &'a GetDatasetConditionParams,
    ) -> Fut<'a, Vec<DatasetConditionDetail>> {
        Box::pin(async move {
            let mut s = lock(&self.s);
            s.conditions_asked.push(p.dataset.clone());
            match &s.conditions {
                Some(c) => Ok(c.clone()),
                None => Err("not faked".to_owned()),
            }
        })
    }
}

/// What the fake store answers.
struct StoreState {
    catalog: (u16, Value),
    coverage: (u16, Value),
    coverage_asked: Vec<String>,
    catalog_asked: Vec<String>,
    files_asked: Vec<String>,
    definitions: Vec<u8>,
    /// What `POST /conditions/databento` answers, and what it was sent
    /// (with its authorization).
    conditions_reply: (u16, Value),
    conditions_posted: Vec<(Option<String>, Value)>,
    /// What `/health` says, as a capture service would.
    health: &'static str,
    /// `/batches/databento`'s batches.
    batches: Value,
    /// Answers 503 to this many `/catalog` reads first (a rescan).
    catalog_503s: u32,
    /// And to this many `/incoming` reads; then this.
    incoming_503s: u32,
    incoming: Value,
    /// Files uploaded to incoming, and each filing asked for (path and query).
    uploaded: Vec<String>,
    incorporated: Vec<String>,
    /// Filing fails with this, when set.
    incorporate_error: Option<String>,
    /// Incorporations answered 503 (the store's filing queue full) before one is
    /// taken.
    incorporate_busy: u32,
    incorporate_delay_ms: u64,
    /// Downloads (`/dl/...`) answered 500 before one is served.
    dl_fail: u32,
}

/// A definition day (zstd DBN) listing these contracts, all expiring on `DAY`.
fn definitions(symbols: &[&str]) -> Vec<u8> {
    use dbn::encode::EncodeRecord as _;
    let meta = dbn::Metadata::builder()
        .dataset(DS)
        .schema(Some(dbn::Schema::Definition))
        .start(0)
        .stype_in(Some(dbn::SType::RawSymbol))
        .stype_out(dbn::SType::InstrumentId)
        .build();
    let expiration = u64::try_from(whole_day().0 + 20 * HOUR).unwrap();
    let mut out = Vec::new();
    {
        let mut e = dbn::encode::dbn::Encoder::with_zstd(&mut out, &meta).unwrap();
        for (i, s) in symbols.iter().enumerate() {
            e.encode_record(&dbn::InstrumentDefMsg {
                hd: dbn::RecordHeader::new::<dbn::InstrumentDefMsg>(
                    dbn::rtype::INSTRUMENT_DEF,
                    1,
                    u32::try_from(i).unwrap() + 1,
                    0,
                ),
                raw_symbol: dbn::record::str_to_c_chars(s).unwrap(),
                expiration,
                strike_price: 728_000_000_000,
                ..Default::default()
            })
            .unwrap();
        }
    }
    out
}

fn ready_store() -> StoreState {
    StoreState {
        catalog: (200, json!({ "roots": [], "missing_volumes": [] })),
        coverage: (200, json!({ "days": [], "missing_volumes": [] })),
        coverage_asked: Vec::new(),
        catalog_asked: Vec::new(),
        files_asked: Vec::new(),
        definitions: definitions(&[C1, C2]),
        conditions_reply: (200, json!({ "changed": [] })),
        conditions_posted: Vec::new(),
        health: "idle",
        batches: json!({}),
        catalog_503s: 0,
        incoming_503s: 0,
        incoming: json!({ "batches": [] }),
        uploaded: Vec::new(),
        incorporated: Vec::new(),
        incorporate_error: None,
        incorporate_busy: 0,
        incorporate_delay_ms: 0,
        dl_fail: 0,
    }
}

type StoreHandle = Arc<Mutex<StoreState>>;

fn answer((code, v): (u16, Value)) -> Reply {
    (StatusCode::from_u16(code).unwrap(), Json(v))
}

async fn serve_store(state: StoreHandle) -> String {
    let app = Router::new()
        .route(
            "/catalog",
            get(|AxState(s): AxState<StoreHandle>, RawQuery(q): RawQuery| async move {
                let mut s = lock(&s);
                s.catalog_asked.push(q.unwrap_or_default());
                if s.catalog_503s > 0 {
                    s.catalog_503s -= 1;
                    return answer((503, json!({ "error": "rescanning" })));
                }
                answer(s.catalog.clone())
            }),
        )
        .route(
            "/coverage",
            get(|AxState(s): AxState<StoreHandle>, RawQuery(q): RawQuery| async move {
                let mut s = lock(&s);
                s.coverage_asked.push(q.unwrap_or_default());
                answer(s.coverage.clone())
            }),
        )
        .route(
            "/files",
            get(|AxState(s): AxState<StoreHandle>, RawQuery(q): RawQuery| async move {
                lock(&s).files_asked.push(q.unwrap_or_default());
                Json(json!({ "files": [{ "volume": "v1", "path": format!("{DS}/SPY.OPT/definition/{DAY}.dbn.zst") }] }))
            }),
        )
        .route(
            "/conditions/databento",
            post(
                |AxState(s): AxState<StoreHandle>, headers: axum::http::HeaderMap, Json(v): Json<Value>| async move {
                    let auth = headers.get("authorization").and_then(|a| a.to_str().ok()).map(str::to_owned);
                    let mut s = lock(&s);
                    s.conditions_posted.push((auth, v));
                    answer(s.conditions_reply.clone())
                },
            ),
        )
        .route(
            "/batches/databento",
            get(|AxState(s): AxState<StoreHandle>| async move {
                Json(json!({ "provider": "databento", "batches": lock(&s).batches.clone() }))
            }),
        )
        .route(
            "/incoming",
            get(|AxState(s): AxState<StoreHandle>| async move {
                let mut s = lock(&s);
                if s.incoming_503s > 0 {
                    s.incoming_503s -= 1;
                    return answer((503, json!({ "error": "rescanning" })));
                }
                answer((200, s.incoming.clone()))
            }),
        )
        .route(
            "/incoming/databento/{job}/{file}",
            axum::routing::put(
                |AxState(s): AxState<StoreHandle>, UrlPath((job, file)): UrlPath<(String, String)>| async move {
                    lock(&s).uploaded.push(format!("{job}/{file}"));
                    Json(json!({ "ok": true }))
                },
            ),
        )
        .route(
            "/incorporate/databento/{job}",
            post(
                |AxState(s): AxState<StoreHandle>, UrlPath(job): UrlPath<String>, RawQuery(q): RawQuery| async move {
                    let delay = lock(&s).incorporate_delay_ms;
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                    let mut s = lock(&s);
                    if s.incorporate_busy > 0 {
                        s.incorporate_busy -= 1;
                        return answer((503, json!({ "error": "busy: the store's filing queue is full; try again" })));
                    }
                    s.incorporated.push(format!("{job}?{}", q.unwrap_or_default()));
                    match s.incorporate_error.clone() {
                        Some(e) => answer((500, json!({ "error": e }))),
                        None => answer((200, json!({ "filed": job }))),
                    }
                },
            ),
        )
        .route(
            "/dl/{file}",
            get(|AxState(s): AxState<StoreHandle>| async move {
                let mut s = lock(&s);
                if s.dl_fail > 0 {
                    s.dl_fail -= 1;
                    return (StatusCode::INTERNAL_SERVER_ERROR, "down");
                }
                (StatusCode::OK, "dbn bytes")
            }),
        )
        .route(
            "/health",
            get(|AxState(s): AxState<StoreHandle>| async move { Json(json!({ "state": lock(&s).health })) }),
        )
        .route(
            "/files/v1/{*path}",
            get(|AxState(s): AxState<StoreHandle>| async move { lock(&s).definitions.clone() }),
        )
        .with_state(state);
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(l, app).await });
    format!("http://{addr}")
}

struct Rig {
    jobs: Shared,
    fake: Arc<Fake>,
    store: StoreHandle,
    dir: PathBuf,
}

impl Drop for Rig {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

async fn rig(name: &str) -> Rig {
    rig_with(name, |_| {}).await
}

/// A rig whose service is changed by `setup` before it starts.
async fn rig_with(name: &str, setup: impl FnOnce(&mut Jobs)) -> Rig {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let dir = std::env::temp_dir().join(format!("tinytrader-jobs-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let store = Arc::new(Mutex::new(ready_store()));
    let url = serve_store(store.clone()).await;
    let fake = Arc::new(Fake::default());
    let ledger = ledger::Ledger::open(&dir.join("submissions.jsonl")).unwrap();
    // The fake store is plain http: no certificates, so none are loaded
    // from the system (a build sandbox has none).
    let http = reqwest::Client::builder()
        .tls_certs_only(Vec::<reqwest::Certificate>::new())
        .build()
        .unwrap();
    let mut jobs = Jobs::new(fake.clone(), "key".into(), &url, None, Some(ledger), http);
    // Short limits on Databento's answers, to keep the tests fast.
    jobs.db = Arc::new(api::Timed {
        inner: fake.clone(),
        read: Duration::from_secs(2),
        cost: Duration::from_secs(2),
        submit: Duration::from_millis(300),
    });
    // No floor of free space for the cache: the test machine's disk is its own.
    jobs.cache_min_free = 0;
    jobs.cache_min_free_pct = 0;
    setup(&mut jobs);
    let jobs = Arc::new(jobs);
    Rig {
        jobs,
        fake,
        store,
        dir,
    }
}

/// Fail unless an answer fits this service's committed OpenAPI document.
fn fits(method: &str, path: &str, code: StatusCode, v: &Value) {
    tinytrader_openapi::assert_conforms("databento-jobs", method, path, code.as_str(), v);
}

/// The approval token of the rigs that take paid purchases.
const APPROVE: &str = "approve-test";

impl Rig {
    /// Price a request: `/price` with these fields over the defaults (SPY's
    /// whole chain on `DAY`).
    async fn price(&self, fields: Value) -> (StatusCode, Value) {
        let mut v = json!({ "dataset": DS.to_lowercase(), "schema": SCHEMA, "symbol": "spy.opt", "start": DAY, "end": DAY });
        for (k, x) in fields.as_object().unwrap() {
            v[k] = x.clone();
        }
        let (code, Json(v)) = price(
            AxState(self.jobs.clone()),
            Json(serde_json::from_value(v).unwrap()),
        )
        .await;
        fits("post", "/price", code, &v);
        (code, v)
    }

    /// SPY's contracts on `DAY`, by the (empty) rule: bought by contract.
    async fn price_contracts(&self, again: Option<Value>) -> (StatusCode, Value) {
        self.price(json!({ "rule": {}, "again": again })).await
    }

    /// The quote for a whole symbol over these days ("" when it can't be priced).
    async fn quote(&self, symbol: &str, start: &str, end: &str) -> String {
        let (_, v) = self
            .price(json!({ "symbol": symbol, "start": start, "end": end }))
            .await;
        v["quote"].as_str().unwrap_or_default().to_owned()
    }

    async fn submit(&self, quote: &str, again: Option<Value>) -> (StatusCode, Value) {
        self.submit_as(quote, again, None, None, None).await
    }

    /// Submit with an approval: the bearer token, who, and the confirmed price.
    async fn submit_as(
        &self,
        quote: &str,
        again: Option<Value>,
        bearer: Option<&str>,
        by: Option<&str>,
        confirm_usd: Option<f64>,
    ) -> (StatusCode, Value) {
        let mut headers = axum::http::HeaderMap::new();
        if let Some(b) = bearer {
            headers.insert("authorization", format!("Bearer {b}").parse().unwrap());
        }
        let (code, Json(v)) = submit(
            AxState(self.jobs.clone()),
            headers,
            Json(Submit {
                quote: quote.into(),
                confirm_usd,
                again,
                approved_by: by.map(Into::into),
                origin: None,
            }),
        )
        .await;
        fits("post", "/submit", code, &v);
        (code, v)
    }

    fn write(&self, request: &str, state: &str, job: Option<&str>, at: f64) {
        lock(&self.jobs.ledger)
            .as_mut()
            .unwrap()
            .append(ledger::Entry {
                at,
                request: request.into(),
                state: state.into(),
                job: job.map(Into::into),
                cost_usd: Some(0.0),
                ..Default::default()
            })
            .unwrap();
    }

    fn entries(&self) -> Vec<Value> {
        let text = std::fs::read_to_string(self.dir.join("submissions.jsonl")).unwrap_or_default();
        text.lines()
            .map(|l| serde_json::from_str::<Value>(l).unwrap())
            .collect()
    }

    fn states(&self) -> Vec<String> {
        self.entries()
            .iter()
            .map(|e| e["state"].as_str().unwrap().to_owned())
            .collect()
    }

    /// Hold every price until `more` more have been asked for (or 300 ms).
    fn gate(&self, more: usize) {
        let mut s = lock(&self.fake.s);
        s.cost_gate = s.costs_asked + more;
    }
}

fn whole_day() -> had::Window {
    had::day_window(date(DAY))
}

fn fp(symbols: &[&str]) -> String {
    let s: Vec<String> = symbols.iter().map(|x| (*x).to_owned()).collect();
    ledger::fingerprint(DS, SCHEMA, &s, DAY, "2026-10-03")
}

/// The contract-list requests a quote prices: each one's window and count.
fn requests(v: &Value) -> Vec<(Value, u64)> {
    rows(v)
        .iter()
        .filter(|r| r["kind"] == "contracts")
        .map(|r| (r["window"].clone(), r["contracts"].as_u64().unwrap()))
        .collect()
}

/// The whole-symbol requests a quote prices: each one's first and last day.
fn wholes(v: &Value) -> Vec<(String, String)> {
    rows(v)
        .iter()
        .filter(|r| r["kind"] == "whole")
        .map(|r| {
            (
                r["first_day"].as_str().unwrap().to_owned(),
                r["last_day"].as_str().unwrap().to_owned(),
            )
        })
        .collect()
}

fn rows(v: &Value) -> Vec<Value> {
    v["requests"].as_array().cloned().unwrap_or_default()
}

#[tokio::test]
async fn two_concurrent_submits_buy_once() {
    let r = rig("concurrent").await;
    // The same request twice, then two that overlap by a day.
    let (a, b) = (
        r.quote("SPY.OPT", DAY, DAY).await,
        r.quote("SPY.OPT", DAY, DAY).await,
    );
    // Both are checked before either is sent, unless one waits its turn.
    r.gate(2);
    let ((ca, _), (cb, _)) = tokio::join!(r.submit(&a, None), r.submit(&b, None));
    assert_eq!(r.fake.submits(), 1, "{ca} {cb}");
    let (c, d) = (
        r.quote("QQQ.OPT", "2026-10-01", DAY).await,
        r.quote("QQQ.OPT", DAY, "2026-10-05").await,
    );
    r.gate(2);
    let ((cc, _), (cd, _)) = tokio::join!(r.submit(&c, None), r.submit(&d, None));
    assert_eq!(r.fake.submits(), 2, "{cc} {cd}");
    assert!([cc, cd].contains(&StatusCode::CONFLICT));
}

#[tokio::test]
async fn a_submission_lost_after_databento_accepted_it_is_never_bought_again() {
    let r = rig("lost").await;
    lock(&r.fake.s).submit_lost = true;
    let (code, _) = r.submit(&r.quote("SPY.OPT", DAY, DAY).await, None).await;
    assert_eq!(code, StatusCode::BAD_GATEWAY);
    // It doesn't show on Databento's list yet: still never bought again.
    lock(&r.fake.s).submit_lost = false;
    let (code, v) = r.submit(&r.quote("SPY.OPT", DAY, DAY).await, None).await;
    assert_eq!(code, StatusCode::CONFLICT, "{v}");
    refresh(&r.jobs).await.unwrap();
    assert_eq!(r.states(), ["sending", "uncertain"]);
    // It shows: reconciled to its job, and still never bought again.
    lock(&r.fake.s).hidden.clear();
    refresh(&r.jobs).await.unwrap();
    assert_eq!(r.states(), ["sending", "uncertain", "submitted"]);
    let (code, _) = r.submit(&r.quote("SPY.OPT", DAY, DAY).await, None).await;
    assert_eq!(code, StatusCode::CONFLICT);
    assert_eq!(r.fake.submits(), 1);
    // The list asked for is complete: every state, all history.
    let asked = lock(&r.fake.s).listed_with.clone();
    assert!(asked.iter().all(|p| {
        p.since.is_some()
            && p.states
                .as_ref()
                .is_some_and(|s| s.contains(&JobState::Expired))
    }));
}

#[tokio::test]
async fn an_old_unresolved_submission_is_never_written_off() {
    let r = rig("old").await;
    r.write(&fp(&["SPY.OPT"]), "uncertain", None, 1.0);
    refresh(&r.jobs).await.unwrap();
    assert_eq!(r.states(), ["uncertain"]);
    let (code, _) = r.submit(&r.quote("SPY.OPT", DAY, DAY).await, None).await;
    assert_eq!(code, StatusCode::CONFLICT);
    // Until the operator says it was never made.
    let (code, Json(v)) = resolve(
        AxState(r.jobs.clone()),
        Json(Resolve {
            request: fp(&["SPY.OPT"]),
            at: 1.0,
            job: None,
            note: "not in Databento's portal".into(),
        }),
    )
    .await;
    assert_eq!(code, StatusCode::OK, "{v}");
    let (code, v) = r.submit(&r.quote("SPY.OPT", DAY, DAY).await, None).await;
    assert_eq!(code, StatusCode::OK, "{v}");
}

#[tokio::test]
async fn multi_symbol_all_symbols_contract_list_and_instrument_id_jobs_block() {
    for (n, (symbols, stype, left)) in [
        (json!("SPY.OPT,QQQ.OPT"), "parent", 0),
        (json!(["QQQ.OPT", "SPY.OPT"]), "parent", 0),
        (json!("ALL_SYMBOLS"), "parent", 0),
        (json!(C1), "raw_symbol", 1),
        (json!([123, 456]), "instrument_id", 0),
    ]
    .into_iter()
    .enumerate()
    {
        let r = rig(&format!("symbols-{n}")).await;
        r.fake
            .add(job("OPRA-OLD", symbols.clone(), stype, whole_day(), "done"));
        // The whole chain, or by contract: never the parent, only what's left.
        for asked in [json!({}), json!({ "rule": {} })] {
            let (code, v) = r.price(asked).await;
            assert_eq!(code, StatusCode::OK, "{v}");
            assert!(wholes(&v).is_empty(), "{symbols}: {v}");
            let left_now: u64 = requests(&v).iter().map(|(_, n)| n).sum();
            assert_eq!(left_now, left, "{symbols}: {v}");
        }
        assert_eq!(r.fake.submits(), 0);
    }
}

#[tokio::test]
async fn ledger_purchases_listed_or_not_are_subtracted() {
    let r = rig("ledger").await;
    // Bought here, its job expired and gone from the list.
    r.write(&fp(&[C1]), "submitted", Some("OPRA-GONE"), 1.0);
    let (_, v) = r.price_contracts(None).await;
    assert_eq!(requests(&v), [(json!([DAY, "2026-10-03"]), 1)]);
    // And one whose outcome isn't known.
    r.write(&fp(&[C2]), "uncertain", None, 2.0);
    let (_, v) = r.price_contracts(None).await;
    assert!(rows(&v).is_empty(), "{v}");
}

#[tokio::test]
async fn buying_again_names_exactly_the_expired_jobs() {
    let r = rig("again").await;
    r.write(&fp(&["SPY.OPT"]), "submitted", Some("OPRA-GONE"), 1.0);
    let (code, v) = r.submit(&r.quote("SPY.OPT", DAY, DAY).await, None).await;
    assert_eq!(code, StatusCode::CONFLICT, "{v}");
    assert!(v["error"].as_str().unwrap().contains("OPRA-GONE"), "{v}");
    for (again, code) in [
        (json!(true), StatusCode::BAD_REQUEST),
        (json!(["OPRA-OTHER"]), StatusCode::BAD_REQUEST),
        (json!(["OPRA-GONE", "OPRA-OTHER"]), StatusCode::BAD_REQUEST),
    ] {
        assert_eq!(r.price(json!({ "again": again })).await.0, code, "{again}");
    }
    assert_eq!(r.fake.submits(), 0);
    let (_, v) = r.price(json!({ "again": ["OPRA-GONE"] })).await;
    assert_eq!(wholes(&v), [(DAY.to_owned(), DAY.to_owned())], "{v}");
    let (code, v) = r.submit(v["quote"].as_str().unwrap(), None).await;
    assert_eq!(code, StatusCode::OK, "{v}");
    // A job still on the list is never bought again, named or not.
    let id = v["submitted"][0]["job"].as_str().unwrap();
    assert_eq!(
        r.price(json!({ "again": [id] })).await.0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(r.fake.submits(), 1);
}

#[tokio::test]
async fn a_store_that_cant_say_what_it_holds_refuses_every_purchase() {
    let ready = || {
        (
            200,
            json!({ "days": [], "roots": [], "missing_volumes": [] }),
        )
    };
    for (n, (catalog, coverage)) in [
        ((503, json!({ "error": "first scan" })), ready()),
        (
            (200, json!({ "roots": [], "missing_volumes": ["M2-B"] })),
            ready(),
        ),
        (ready(), (200, json!({ "days": [] }))),
        (ready(), (503, json!({}))),
    ]
    .into_iter()
    .enumerate()
    {
        let r = rig(&format!("store-{n}")).await;
        // Priced while the store could say, submitted once it can't.
        let q = r.quote("SPY.OPT", DAY, DAY).await;
        {
            let mut s = lock(&r.store);
            s.catalog = catalog;
            s.coverage = coverage;
        }
        let (code, v) = r.submit(&q, None).await;
        assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE, "{n}: {v}");
        assert_eq!(r.price(json!({})).await.0, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(r.fake.submits(), 0);
    }
    // Nor while Databento's job list can't be read.
    let r = rig("store-list").await;
    let q = r.quote("SPY.OPT", DAY, DAY).await;
    lock(&r.fake.s).list_fails = true;
    let (code, _) = r.submit(&q, None).await;
    assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(r.price(json!({})).await.0, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(r.fake.submits(), 0);
}

#[tokio::test]
async fn a_catalog_day_coverage_doesnt_explain_is_refused() {
    let dataset = |provider: &str| json!({ "provider": provider, "filed": true, "id": DS, "symbol": "SPY.OPT", "schemas": { SCHEMA: { "days": { DAY: {} } } } });
    // Another provider's holdings don't count for Databento's.
    let r = rig("unexplained").await;
    lock(&r.store).catalog.1 =
        json!({ "roots": [{ "datasets": [dataset("alpaca")] }], "missing_volumes": [] });
    let (code, v) = r.submit(&r.quote("SPY.OPT", DAY, DAY).await, None).await;
    assert_eq!(code, StatusCode::OK, "{v}");
    let r = rig("unexplained-2").await;
    lock(&r.store).catalog.1 =
        json!({ "roots": [{ "datasets": [dataset("databento")] }], "missing_volumes": [] });
    let (code, v) = r.submit(&r.quote("SPY.OPT", DAY, DAY).await, None).await;
    assert_eq!(code, StatusCode::CONFLICT, "{v}");
    let (_, v) = r.price_contracts(None).await;
    assert!(rows(&v).is_empty(), "{v}");
    let s = lock(&r.store);
    assert!(!s.coverage_asked.is_empty() && !s.files_asked.is_empty());
    assert!(
        s.coverage_asked
            .iter()
            .chain(&s.files_asked)
            .all(|q| q.contains("provider=databento"))
    );
}

#[tokio::test]
async fn an_unread_job_during_reconcile_resolves_nothing() {
    let r = rig("unread").await;
    let now = tinytrader_observe::unix_seconds();
    r.write(&fp(&["SPY.OPT"]), "uncertain", None, now);
    r.fake.add(job(
        "OPRA-MATCH",
        json!("SPY.OPT"),
        "parent",
        whole_day(),
        "queued",
    ));
    r.fake.add(job(
        "OPRA-SLOW",
        json!("QQQ.OPT"),
        "parent",
        whole_day(),
        "queued",
    ));
    lock(&r.fake.s).unreadable.insert("OPRA-SLOW".into());
    refresh(&r.jobs).await.unwrap();
    assert_eq!(r.states(), ["uncertain"]);
    lock(&r.fake.s).unreadable.clear();
    refresh(&r.jobs).await.unwrap();
    assert_eq!(r.states(), ["uncertain", "submitted"]);
}

#[tokio::test]
async fn the_missing_hours_of_a_partial_day_are_bought_as_their_own_request() {
    let r = rig("hours").await;
    let day = whole_day();
    lock(&r.store).coverage.1 = json!({ "days": [
        { "day": DAY, "kind": "partial", "filed": true, "volumes": ["v1"], "coverage": { "kind": "contracts", "stype": "raw_symbol", "symbols": [C1], "window": [day.0 + 13 * HOUR, day.1] } },
    ], "missing_volumes": [] });
    // The whole chain, partly held: by contract, the hours each still needs.
    let (code, v) = r.price(json!({})).await;
    assert_eq!(code, StatusCode::OK, "{v}");
    assert!(wholes(&v).is_empty(), "{v}");
    assert_eq!(
        requests(&v),
        [
            (json!([DAY, "2026-10-02T13:00:00.000000000Z"]), 1),
            (json!([DAY, "2026-10-03"]), 1)
        ]
    );
    let hours = (vec![C1.to_owned()], (day.0, day.0 + 13 * HOUR));
    assert!(lock(&r.fake.s).costed.contains(&hours));
    let (code, v) = r.submit(v["quote"].as_str().unwrap(), None).await;
    assert_eq!(code, StatusCode::OK, "{v}");
    assert_eq!(r.fake.submits(), 2);
    // Both now pending: nothing left to buy.
    let (_, v) = r.price_contracts(None).await;
    assert!(rows(&v).is_empty(), "{v}");
}

#[tokio::test]
async fn a_price_that_isnt_a_number_of_dollars_is_refused() {
    for price in [f64::NAN, -1.0] {
        let r = paid("nan", price).await;
        // Never free: it needs the approval, and approved, it is still refused.
        let (code, v) = r.submit(&r.quote("SPY.OPT", DAY, DAY).await, None).await;
        assert_eq!(code, StatusCode::FORBIDDEN, "{v}");
        let (_, v) = r.price_contracts(None).await;
        assert_eq!(v["needs_approval"], true, "{v}");
        let (code, v) = r
            .submit_as(
                v["quote"].as_str().unwrap(),
                None,
                Some(APPROVE),
                Some("me"),
                Some(price),
            )
            .await;
        assert_ne!(code, StatusCode::OK, "{v}");
        assert_eq!(r.fake.submits(), 0);
    }
}

#[tokio::test]
async fn names_are_canonical_and_datasets_known() {
    let r = rig("names").await;
    let (code, v) = r.submit(&r.quote("spy.opt", DAY, DAY).await, None).await;
    assert_eq!(code, StatusCode::OK, "{v}");
    let sent = lock(&r.fake.s).submitted[0].clone();
    assert_eq!(
        (sent.dataset.as_str(), sent.symbols.to_api_string()),
        (DS, "SPY.OPT".into())
    );
    let (code, _) = r.price(json!({ "dataset": "OPRA.NOPE" })).await;
    assert_eq!(code, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn a_hung_submission_is_uncertain_and_frees_the_lock() {
    let r = rig("hung").await;
    // Databento never answers for SPY: while it hangs, a QQQ purchase still
    // goes through once the SPY submission gives up.
    let (spy, qqq) = (
        r.quote("SPY.OPT", DAY, DAY).await,
        r.quote("QQQ.OPT", DAY, DAY).await,
    );
    lock(&r.fake.s).hang_on = Some("SPY".into());
    let other = async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        r.submit(&qqq, None).await
    };
    let both = async { tokio::join!(r.submit(&spy, None), other) };
    let ((first, why), (second, v)) = tokio::time::timeout(Duration::from_secs(5), both)
        .await
        .expect("a hung submission must not hold the purchase lock");
    assert_eq!(first, StatusCode::BAD_GATEWAY, "{why}");
    let why = why["error"].as_str().unwrap();
    assert!(
        why.contains("no answer from Databento") && why.contains("outcome is unknown"),
        "{why}"
    );
    assert_eq!(second, StatusCode::OK, "{v}");
    // The SPY one is never bought again while its outcome isn't known.
    let (code, v) = r.submit(&r.quote("SPY.OPT", DAY, DAY).await, None).await;
    assert_eq!(code, StatusCode::CONFLICT);
    assert!(
        v["error"].as_str().unwrap().contains("nothing to buy"),
        "{v}"
    );
    assert_eq!(r.states(), ["sending", "uncertain", "sending", "submitted"]);
}

#[tokio::test]
async fn reconcile_matches_jobs_from_when_the_submission_was_sent() {
    let r = rig("sent-at").await;
    let now = tinytrader_observe::unix_seconds();
    r.write(&fp(&["SPY.OPT"]), "sending", None, now - 1000.0);
    r.write(&fp(&["SPY.OPT"]), "uncertain", None, now);
    let mut j = job(
        "OPRA-MATCH",
        json!("SPY.OPT"),
        "parent",
        whole_day(),
        "queued",
    );
    j.ts_received -= time::Duration::seconds(990);
    r.fake.add(j);
    refresh(&r.jobs).await.unwrap();
    assert_eq!(r.states(), ["sending", "uncertain", "submitted"]);
}

#[tokio::test]
async fn reconcile_waits_while_a_purchase_is_in_flight() {
    let r = rig("in-flight").await;
    let now = tinytrader_observe::unix_seconds();
    r.write(&fp(&["SPY.OPT"]), "sending", None, now);
    r.fake.add(job(
        "OPRA-MATCH",
        json!("SPY.OPT"),
        "parent",
        whole_day(),
        "queued",
    ));
    {
        let _purchase = r.jobs.purchase.lock().await;
        refresh(&r.jobs).await.unwrap();
        assert_eq!(r.states(), ["sending"]);
    }
    refresh(&r.jobs).await.unwrap();
    assert_eq!(r.states(), ["sending", "submitted"]);
}

#[tokio::test]
async fn an_old_automatic_write_off_blocks_until_matched() {
    let r = rig("written-off").await;
    let now = tinytrader_observe::unix_seconds();
    r.write(&fp(&["SPY.OPT"]), "sending", None, now - 900.0);
    let mut off = ledger::Entry {
        at: now - 100.0,
        request: fp(&["SPY.OPT"]),
        state: "not-submitted".into(),
        note: Some("no such job appeared in Databento's job list".into()),
        ..Default::default()
    };
    lock(&r.jobs.ledger)
        .as_mut()
        .unwrap()
        .append(off.clone())
        .unwrap();
    let (code, _) = r.submit(&r.quote("SPY.OPT", DAY, DAY).await, None).await;
    assert_eq!(code, StatusCode::CONFLICT);
    // An operator's write-off doesn't block.
    off.note = Some("resolved by the operator: checked".into());
    lock(&r.jobs.ledger).as_mut().unwrap().append(off).unwrap();
    let (code, v) = r.submit(&r.quote("SPY.OPT", DAY, DAY).await, None).await;
    assert_eq!(code, StatusCode::OK, "{v}");
}

#[tokio::test]
async fn an_old_automatic_write_off_is_matched_by_reconcile() {
    let r = rig("written-off-matched").await;
    let now = tinytrader_observe::unix_seconds();
    r.write(&fp(&["SPY.OPT"]), "sending", None, now - 900.0);
    lock(&r.jobs.ledger)
        .as_mut()
        .unwrap()
        .append(ledger::Entry {
            at: now - 100.0,
            request: fp(&["SPY.OPT"]),
            state: "not-submitted".into(),
            note: Some("no such job appeared in Databento's job list".into()),
            ..Default::default()
        })
        .unwrap();
    // Its job, received soon after it was sent, has expired since.
    let mut j = job(
        "OPRA-MATCH",
        json!("SPY.OPT"),
        "parent",
        whole_day(),
        "expired",
    );
    j.ts_received -= time::Duration::seconds(890);
    r.fake.add(j);
    refresh(&r.jobs).await.unwrap();
    assert_eq!(r.states(), ["sending", "not-submitted", "submitted"]);
    let l = lock(&r.jobs.ledger);
    assert!(l.as_ref().unwrap().submitted().contains("OPRA-MATCH"));
}

fn resolution(request: &str, at: f64, job: Option<&str>) -> Json<Resolve> {
    Json(Resolve {
        request: request.into(),
        at,
        job: job.map(Into::into),
        note: "checked the portal".into(),
    })
}

#[tokio::test]
async fn unresolved_submissions_are_listed_with_what_resolve_needs() {
    let r = rig("unresolved-list").await;
    let now = tinytrader_observe::unix_seconds();
    let open = fp(&["SPY.OPT"]);
    let done = fp(&["QQQ.OPT"]);
    r.write(&open, "sending", None, now - 60.0);
    r.write(&open, "uncertain", None, now - 50.0);
    r.write(&done, "sending", None, now - 40.0);
    r.write(&done, "submitted", Some("OPRA-DONE"), now - 30.0);
    let (code, Json(v)) = ledger_unresolved(AxState(r.jobs.clone())).await;
    assert_eq!(code, StatusCode::OK, "{v}");
    let list = v["unresolved"].as_array().unwrap();
    assert_eq!(list.len(), 1, "{v}");
    assert_eq!(list[0]["request"], json!(open));
    assert_eq!(list[0]["state"], "uncertain");
    assert_eq!(list[0]["at"], json!(now - 50.0));
    assert_eq!(list[0]["sent_at"], json!(now - 60.0));
}

#[tokio::test]
async fn never_made_only_after_half_an_hour_and_a_full_read_since() {
    let r = rig("resolve-none").await;
    let now = tinytrader_observe::unix_seconds();
    let req = fp(&["SPY.OPT"]);
    r.write(&req, "sending", None, now - 60.0);
    r.write(&req, "uncertain", None, now - 50.0);
    refresh(&r.jobs).await.unwrap();
    let (code, Json(v)) =
        resolve(AxState(r.jobs.clone()), resolution(&req, now - 50.0, None)).await;
    assert_eq!(code, StatusCode::CONFLICT, "{v}");
    assert!(v["error"].as_str().unwrap().contains("only after"), "{v}");
    // Old enough, but no full read since then.
    let r = rig("resolve-none-2").await;
    r.write(&req, "sending", None, now - 3600.0);
    r.write(&req, "uncertain", None, now - 3500.0);
    let (code, _) = resolve(
        AxState(r.jobs.clone()),
        resolution(&req, now - 3500.0, None),
    )
    .await;
    assert_eq!(code, StatusCode::CONFLICT);
    // A read that couldn't see every job doesn't count.
    r.fake.add(job(
        "OPRA-SLOW",
        json!("QQQ.OPT"),
        "parent",
        whole_day(),
        "queued",
    ));
    lock(&r.fake.s).unreadable.insert("OPRA-SLOW".into());
    refresh(&r.jobs).await.unwrap();
    let (code, _) = resolve(
        AxState(r.jobs.clone()),
        resolution(&req, now - 3500.0, None),
    )
    .await;
    assert_eq!(code, StatusCode::CONFLICT);
    lock(&r.fake.s).unreadable.clear();
    refresh(&r.jobs).await.unwrap();
    let (code, Json(v)) = resolve(
        AxState(r.jobs.clone()),
        resolution(&req, now - 3500.0, None),
    )
    .await;
    assert_eq!(code, StatusCode::OK, "{v}");
}

#[tokio::test]
async fn resolving_to_a_job_checks_the_job() {
    let r = rig("resolve-job").await;
    let now = tinytrader_observe::unix_seconds();
    let req = fp(&["SPY.OPT"]);
    r.write(&req, "sending", None, now);
    r.write(&fp(&["QQQ.OPT"]), "submitted", Some("OPRA-TAKEN"), now);
    let mut other = job("OPRA-DEFS", json!("SPY.OPT"), "parent", whole_day(), "done");
    other.schema = Schema::Definition;
    r.fake.add(other);
    r.fake.add(job(
        "OPRA-TAKEN",
        json!("SPY.OPT"),
        "parent",
        whole_day(),
        "done",
    ));
    r.fake.add(job(
        "OPRA-MINE",
        json!("SPY.OPT"),
        "parent",
        whole_day(),
        "done",
    ));
    for id in ["OPRA-NONE", "OPRA-DEFS", "OPRA-TAKEN"] {
        let (code, Json(v)) =
            resolve(AxState(r.jobs.clone()), resolution(&req, now, Some(id))).await;
        assert_eq!(code, StatusCode::CONFLICT, "{id}: {v}");
    }
    let (code, Json(v)) = resolve(
        AxState(r.jobs.clone()),
        resolution(&req, now, Some("OPRA-MINE")),
    )
    .await;
    assert_eq!(code, StatusCode::OK, "{v}");
}

#[tokio::test]
async fn buying_missing_contracts_again_names_exactly_the_expired_jobs() {
    let r = rig("again-missing").await;
    r.write(&fp(&[C1]), "submitted", Some("OPRA-GONE"), 1.0);
    let (_, v) = r.price_contracts(None).await;
    assert_eq!(requests(&v), [(json!([DAY, "2026-10-03"]), 1)]);
    assert!(v["notes"].to_string().contains("OPRA-GONE"), "{v}");
    assert_eq!(
        r.price_contracts(Some(json!(true))).await.0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        r.price_contracts(Some(json!(["OPRA-OTHER"]))).await.0,
        StatusCode::BAD_REQUEST
    );
    let (code, v) = r.price_contracts(Some(json!(["OPRA-GONE"]))).await;
    assert_eq!(code, StatusCode::OK, "{v}");
    assert_eq!(requests(&v), [(json!([DAY, "2026-10-03"]), 2)]);
    assert_eq!(v["again"], json!(["OPRA-GONE"]));
    // A submission that names `again` names exactly what the quote did.
    let quote = v["quote"].as_str().unwrap().to_owned();
    let (code, _) = r.submit(&quote, Some(json!(["OPRA-OTHER"]))).await;
    assert_eq!(code, StatusCode::CONFLICT);
    let (_, v) = r.price_contracts(Some(json!(["OPRA-GONE"]))).await;
    let (code, v) = r
        .submit(v["quote"].as_str().unwrap(), Some(json!(["OPRA-GONE"])))
        .await;
    assert_eq!(code, StatusCode::OK, "{v}");
    assert_eq!(r.fake.submits(), 1);
}

#[tokio::test]
async fn a_purchase_rechecks_against_a_fresh_scan_and_a_price_doesnt() {
    let r = rig("fresh").await;
    let fresh = |r: &Rig| {
        let s = lock(&r.store);
        let f = |v: &[String]| v.iter().map(|q| q.contains("fresh=1")).collect::<Vec<_>>();
        (f(&s.catalog_asked), f(&s.coverage_asked))
    };
    let clear = |r: &Rig| {
        let mut s = lock(&r.store);
        s.catalog_asked.clear();
        s.coverage_asked.clear();
    };
    for asked in [json!({ "rule": {} }), json!({ "symbol": "QQQ.OPT" })] {
        clear(&r);
        let (_, v) = r.price(asked).await;
        assert_eq!(fresh(&r), (vec![false], vec![false]));
        clear(&r);
        let (code, v) = r.submit(v["quote"].as_str().unwrap(), None).await;
        assert_eq!(code, StatusCode::OK, "{v}");
        assert_eq!(fresh(&r), (vec![true], vec![true]));
    }
}

#[tokio::test]
async fn a_paid_purchase_needs_the_consoles_approval() {
    let r = rig_with("paid", |j| j.approve_token = Some(APPROVE.into())).await;
    lock(&r.fake.s).price = 2.5;
    let q = || r.quote("SPY.OPT", DAY, DAY);
    let (_, v) = r.price(json!({})).await;
    assert_eq!(
        (
            v["needs_approval"].clone(),
            v["valid_seconds"].as_u64().unwrap() > 3500
        ),
        (json!(true), true)
    );
    for (bearer, by, confirm, code) in [
        (
            None,
            Some("me@example.com"),
            Some(2.5),
            StatusCode::FORBIDDEN,
        ),
        (
            Some("guess"),
            Some("me@example.com"),
            Some(2.5),
            StatusCode::FORBIDDEN,
        ),
        (Some(APPROVE), None, Some(2.5), StatusCode::FORBIDDEN),
        (Some(APPROVE), Some(" "), Some(2.5), StatusCode::FORBIDDEN),
        (
            Some(APPROVE),
            Some("me@example.com"),
            None,
            StatusCode::PRECONDITION_REQUIRED,
        ),
        (
            Some(APPROVE),
            Some("me@example.com"),
            Some(2.49),
            StatusCode::PRECONDITION_REQUIRED,
        ),
    ] {
        let (got, v) = r.submit_as(&q().await, None, bearer, by, confirm).await;
        assert_eq!(got, code, "{bearer:?} {by:?} {confirm:?}: {v}");
    }
    assert!(r.entries().is_empty() && r.fake.submits() == 0);
    let quote = q().await;
    let (code, v) = r
        .submit_as(
            &quote,
            None,
            Some(APPROVE),
            Some("me@example.com"),
            Some(2.5),
        )
        .await;
    assert_eq!(code, StatusCode::OK, "{v}");
    let e = r.entries();
    assert_eq!(r.states(), ["approved", "sending", "submitted"]);
    assert_eq!(
        (
            e[0]["by"].as_str(),
            e[0]["quote"].as_str(),
            e[0]["cost_usd"].as_f64()
        ),
        (Some("me@example.com"), Some(quote.as_str()), Some(2.5))
    );
    // Without an approval token, paid purchases are off altogether.
    let r = rig("paid-off").await;
    lock(&r.fake.s).price = 2.5;
    let (code, v) = r
        .submit_as(
            &r.quote("SPY.OPT", DAY, DAY).await,
            None,
            Some(APPROVE),
            Some("me"),
            Some(2.5),
        )
        .await;
    assert_eq!(code, StatusCode::FORBIDDEN);
    assert!(
        v["error"].as_str().unwrap().contains("JOBS_APPROVE_TOKEN"),
        "{v}"
    );
    // A free one needs none of it, and lives 15 minutes.
    let r = rig("free").await;
    let (_, v) = r.price(json!({})).await;
    assert!(v["valid_seconds"].as_u64().unwrap() <= 900);
    let at = v["expires_at"].as_u64().unwrap() as f64 - tinytrader_observe::unix_seconds();
    assert!((898.0..=901.0).contains(&at), "{v}");
    let (code, v) = r.submit(v["quote"].as_str().unwrap(), None).await;
    assert_eq!(code, StatusCode::OK, "{v}");
    assert_eq!(r.states(), ["sending", "submitted"]);
}

#[tokio::test]
async fn quotes_are_listed_in_full_and_declined_with_the_token() {
    let r = rig_with("quotes", |j| j.approve_token = Some(APPROVE.into())).await;
    lock(&r.fake.s).price = 1.25;
    let quote = r.quote("SPY.OPT", DAY, DAY).await;
    let (c, Json(list)) = quotes_list(AxState(r.jobs.clone())).await;
    fits("get", "/quotes", c, &list);
    let q = &list["quotes"][0];
    assert_eq!(
        (
            q["quote"].as_str(),
            q["dataset"].as_str(),
            q["symbol"].as_str(),
            q["cost_usd"].as_f64(),
            q["state"].as_str()
        ),
        (
            Some(quote.as_str()),
            Some(DS),
            Some("SPY.OPT"),
            Some(1.25),
            Some("ready")
        )
    );
    assert_eq!(q["requests"][0]["kind"], "whole");
    assert!(q["notes"].is_array() && q["again"].is_array() && q["billable_bytes"].is_u64());
    let del = |bearer: Option<&str>| {
        let mut h = axum::http::HeaderMap::new();
        if let Some(b) = bearer {
            h.insert("authorization", format!("Bearer {b}").parse().unwrap());
        }
        quote_delete(AxState(r.jobs.clone()), h, UrlPath(quote.clone()))
    };
    assert_eq!(del(None).await.0, StatusCode::FORBIDDEN);
    assert_eq!(del(Some("guess")).await.0, StatusCode::FORBIDDEN);
    assert_eq!(del(Some(APPROVE)).await.0, StatusCode::OK);
    let (code, _) = r
        .submit_as(&quote, None, Some(APPROVE), Some("me"), Some(1.25))
        .await;
    assert_eq!(code, StatusCode::CONFLICT);
}

#[tokio::test]
async fn contract_lists_and_session_hours() {
    let r = rig("lists").await;
    let rth = json!({ "start": "09:30", "end": "16:01", "tz": "America/New_York" });
    let (code, v) = r
        .price(json!({ "contracts": { DAY: [C1] }, "hours": rth }))
        .await;
    assert_eq!(code, StatusCode::OK, "{v}");
    // 2026-10-02 is in daylight saving: 13:30 to 20:01 UTC.
    assert_eq!(
        requests(&v),
        [(
            json!([
                "2026-10-02T13:30:00.000000000Z",
                "2026-10-02T20:01:00.000000000Z"
            ]),
            1
        )]
    );
    // Contracts not in the day's definitions, or a day without any, are refused.
    let (code, v) = r
        .price(json!({ "contracts": { DAY: [C1, "SPY   261002C00999000"] } }))
        .await;
    assert_eq!(code, StatusCode::BAD_REQUEST);
    assert!(
        v["error"]
            .as_str()
            .unwrap()
            .contains("SPY   261002C00999000"),
        "{v}"
    );
    let (code, _) = r
        .price(json!({ "start": "2026-10-01", "contracts": { "2026-10-01": [C1] } }))
        .await;
    assert_eq!(code, StatusCode::BAD_REQUEST);
    // What's held of those hours is subtracted: C1 held from 13:00 covers it.
    let day = whole_day();
    lock(&r.store).coverage.1 = json!({ "days": [
        { "day": DAY, "kind": "partial", "volumes": ["v1"], "coverage": { "kind": "contracts", "symbols": [C1], "window": [day.0 + 13 * HOUR, day.1] } },
    ], "missing_volumes": [] });
    let (_, v) = r
        .price(json!({ "contracts": { DAY: [C1, C2] }, "hours": rth }))
        .await;
    assert_eq!(
        requests(&v),
        [(
            json!([
                "2026-10-02T13:30:00.000000000Z",
                "2026-10-02T20:01:00.000000000Z"
            ]),
            1
        )]
    );
    assert_eq!(rows(&v)[0]["skipped_held"], 1);
}

#[tokio::test]
async fn untouched_days_are_bought_whole_in_runs() {
    let r = rig("runs").await;
    lock(&r.store).coverage.1 = json!({ "days": [
        { "day": DAY, "kind": "complete", "volumes": ["v1"] },
    ], "missing_volumes": [] });
    let (_, v) = r
        .price(json!({ "start": "2026-09-30", "end": "2026-10-05" }))
        .await;
    assert_eq!(
        wholes(&v),
        [
            ("2026-09-30".to_owned(), "2026-10-01".to_owned()),
            ("2026-10-03".to_owned(), "2026-10-05".to_owned())
        ],
        "{v}"
    );
    assert!(requests(&v).is_empty());
    // With hours, each day is its own request.
    let rth = json!({ "start": "09:30", "end": "16:01" });
    let (_, v) = r
        .price(json!({ "start": "2026-10-05", "end": "2026-10-06", "hours": rth }))
        .await;
    assert_eq!(wholes(&v).len(), 2, "{v}");
    let (code, v) = r.submit(v["quote"].as_str().unwrap(), None).await;
    assert_eq!(code, StatusCode::OK, "{v}");
    let sent = lock(&r.fake.s).submitted.clone();
    assert_eq!(sent[0].stype_in, SType::Parent);
    assert_eq!(
        had::ns(sent[0].date_time_range.start),
        had::day_window(date("2026-10-05")).0 + 13 * HOUR + 30 * 60_000_000_000
    );
}

#[tokio::test]
async fn a_day_partly_had_of_a_plain_symbol_isnt_bought() {
    let r = rig("plain").await;
    r.fake.add(job(
        "OPRA-X",
        json!("SPY"),
        "raw_symbol",
        whole_day(),
        "done",
    ));
    let (_, v) = r
        .price(json!({ "symbol": "SPY", "start": "2026-10-01", "end": "2026-10-03" }))
        .await;
    assert_eq!(
        wholes(&v),
        [
            ("2026-10-01".to_owned(), "2026-10-01".to_owned()),
            ("2026-10-03".to_owned(), "2026-10-03".to_owned())
        ],
        "{v}"
    );
    assert!(v["notes"].to_string().contains("OPRA-X"), "{v}");
}

#[tokio::test]
async fn pricing_is_tried_again_only_on_server_failures() {
    let r = rig("retry").await;
    let errors =
        |e: &[&str]| lock(&r.fake.s).cost_errors = e.iter().map(|x| (*x).to_owned()).collect();
    errors(&[
        "API error: 503 Service Unavailable",
        "no answer from Databento in 60 s",
    ]);
    let (code, v) = r.price(json!({})).await;
    assert_eq!(code, StatusCode::OK, "{v}");
    assert_eq!(lock(&r.fake.s).costed.len(), 3);
    errors(&["API error: 503 Service Unavailable"; 3]);
    assert_eq!(r.price(json!({})).await.0, StatusCode::BAD_GATEWAY);
    lock(&r.fake.s).costed.clear();
    errors(&["API error: 422 Unprocessable Entity bad symbols"]);
    assert_eq!(r.price(json!({})).await.0, StatusCode::BAD_GATEWAY);
    assert_eq!(lock(&r.fake.s).costed.len(), 1, "a 4xx isn't tried again");
}

#[tokio::test]
async fn a_long_price_is_answered_while_it_is_worked_out() {
    let r = rig_with("slow", |j| j.price_wait = Duration::ZERO).await;
    r.gate(100);
    let (code, v) = r.price(json!({ "start": "2026-10-01", "end": "2026-10-05", "hours": { "start": "09:30", "end": "16:01" } })).await;
    assert_eq!(code, StatusCode::ACCEPTED, "{v}");
    assert_eq!(v["state"], "pricing");
    let quote = v["quote"].as_str().unwrap().to_owned();
    // Not yet submittable.
    assert_eq!(r.submit(&quote, None).await.0, StatusCode::CONFLICT);
    let v = loop {
        let (_, Json(v)) = quote_get(AxState(r.jobs.clone()), UrlPath(quote.clone())).await;
        if v["state"] != "pricing" {
            break v;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(
        (v["state"].as_str(), v["priced"].as_u64(), v["of"].as_u64()),
        (Some("ready"), Some(5), Some(5))
    );
}

#[tokio::test]
async fn whole_days_tile_across_daylight_saving_changes() {
    let r = rig("dst").await;
    let all = json!({ "start": "00:00", "end": "24:00", "tz": "America/New_York" });
    for (start, end) in [("2026-03-07", "2026-03-09"), ("2026-10-31", "2026-11-02")] {
        let (code, v) = r
            .price(json!({ "start": start, "end": end, "hours": all }))
            .await;
        assert_eq!(code, StatusCode::OK, "{v}");
        let w: Vec<Value> = rows(&v).iter().map(|x| x["window"].clone()).collect();
        assert_eq!(w.len(), 3, "{v}");
        assert_eq!((&w[0][1], &w[1][1]), (&w[1][0], &w[2][0]), "{v}");
    }
}

#[test]
fn a_plan_that_would_buy_twice_is_caught() {
    let part = |w: had::Window, whole: bool, symbols: &[&str]| missing::Part {
        day: date(DAY),
        last: date(DAY),
        window: w,
        whole,
        symbols: symbols.iter().map(|x| (*x).to_owned()).collect(),
        stype: SType::RawSymbol,
        cost_usd: 0.0,
        bytes: 0,
        skipped_held: 0,
        skipped_pending: 0,
    };
    assert!(overlap(&[part((0, 10), false, &[C1]), part((10, 20), false, &[C1])]).is_none());
    assert!(overlap(&[part((0, 10), false, &[C1]), part((5, 20), false, &[C2])]).is_none());
    assert!(overlap(&[part((0, 10), false, &[C1]), part((5, 20), false, &[C2, C1])]).is_some());
    assert!(
        overlap(&[
            part((0, 10), true, &["SPY.OPT"]),
            part((9, 20), false, &[C2])
        ])
        .is_some()
    );
}

/// A rig that takes paid purchases, priced at `usd` each.
async fn paid(name: &str, usd: f64) -> Rig {
    let r = rig_with(name, |j| j.approve_token = Some(APPROVE.into())).await;
    lock(&r.fake.s).price = usd;
    r
}

#[tokio::test]
async fn an_unapproved_or_unconfirmed_submit_leaves_the_quote() {
    let r = paid("keep", 2.5).await;
    let q = r.quote("SPY.OPT", DAY, DAY).await;
    assert_eq!(r.submit(&q, None).await.0, StatusCode::FORBIDDEN);
    let me = Some("me@example.com");
    assert_eq!(
        r.submit_as(&q, None, Some(APPROVE), me, Some(2.4)).await.0,
        StatusCode::PRECONDITION_REQUIRED
    );
    let (code, v) = r.submit_as(&q, None, Some(APPROVE), me, Some(2.5)).await;
    assert_eq!(code, StatusCode::OK, "{v}");
    assert_eq!(r.fake.submits(), 1);
}

#[tokio::test]
async fn each_request_is_priced_as_dollars() {
    // Two days' requests at $1 and -$1: zero in all, but not free.
    let r = paid("parts", 0.0).await;
    lock(&r.fake.s).prices = [1.0, -1.0, 1.0, -1.0].into();
    let rth = json!({ "start": "09:30", "end": "16:01" });
    let (_, v) = r
        .price(json!({ "start": "2026-10-05", "end": "2026-10-06", "hours": rth }))
        .await;
    assert_eq!(v["needs_approval"], true, "{v}");
    let q = v["quote"].as_str().unwrap().to_owned();
    assert_eq!(r.submit(&q, None).await.0, StatusCode::FORBIDDEN);
    assert_ne!(
        r.submit_as(&q, None, Some(APPROVE), Some("me"), Some(0.0))
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(r.fake.submits(), 0);
}

#[tokio::test]
async fn quote_ids_differ_from_one_start_to_the_next() {
    let (a, b) = (rig("boot-a").await, rig("boot-b").await);
    let (qa, qb) = (
        a.quote("SPY.OPT", DAY, DAY).await,
        b.quote("SPY.OPT", DAY, DAY).await,
    );
    assert!(qa.starts_with("q-") && qa != qb, "{qa} {qb}");
}

#[tokio::test]
async fn a_price_that_takes_too_long_fails() {
    let r = rig_with("cap", |j| {
        j.price_wait = Duration::ZERO;
        j.pricing_cap = Duration::from_millis(30);
    })
    .await;
    r.gate(100);
    let (code, v) = r.price(json!({})).await;
    assert_eq!(code, StatusCode::ACCEPTED, "{v}");
    let quote = v["quote"].as_str().unwrap().to_owned();
    tokio::time::sleep(Duration::from_millis(400)).await;
    let (c, Json(v)) = quote_get(AxState(r.jobs.clone()), UrlPath(quote)).await;
    fits("get", "/quotes/{id}", c, &v);
    assert_eq!(v["state"], "failed", "{v}");
    assert!(v["error"].as_str().unwrap().contains("longer than"), "{v}");
}

#[tokio::test]
async fn two_approvals_of_one_quote_buy_once() {
    let r = paid("twice", 2.5).await;
    let q = r.quote("SPY.OPT", DAY, DAY).await;
    let me = Some("me@example.com");
    let ((a, _), (b, _)) = tokio::join!(
        r.submit_as(&q, None, Some(APPROVE), me, Some(2.5)),
        r.submit_as(&q, None, Some(APPROVE), me, Some(2.5))
    );
    let mut codes = [a, b];
    codes.sort();
    assert_eq!(codes, [StatusCode::OK, StatusCode::CONFLICT]);
    assert_eq!(r.fake.submits(), 1);
}

fn condition(day: &str, c: DatasetCondition, modified: Option<&str>) -> DatasetConditionDetail {
    DatasetConditionDetail {
        date: date(day),
        condition: c,
        last_modified_date: modified.map(date),
    }
}

/// A store holding SPY's chain filed on two days, and an Alpaca dataset, as
/// the store's catalog lists them.
fn holding() -> Value {
    json!({ "roots": [{ "name": "v1", "datasets": [
        { "provider": "databento", "id": DS, "symbol": "SPY.OPT", "filed": true, "sources": ["store: /v1/Databento/OPRA.PILLAR"],
          "schemas": { SCHEMA: { "days": { "2026-10-01": {}, DAY: {} } } } },
        { "provider": "alpaca", "id": "ALPACA.IEX", "symbol": "SPY", "schemas": { "bars": { "days": { DAY: {} } } } },
    ] }], "missing_volumes": [] })
}

#[tokio::test]
async fn day_conditions_are_sent_to_the_store_and_changes_told() {
    let r = rig_with("conditions", |j| j.store_token = Some("store-token".into())).await;
    lock(&r.store).catalog.1 = holding();
    lock(&r.fake.s).conditions = Some(vec![
        condition(
            "2026-10-01",
            DatasetCondition::Available,
            Some("2026-10-02"),
        ),
        condition(DAY, DatasetCondition::Available, Some("2026-10-20")),
        condition("2026-10-03", DatasetCondition::Pending, None),
    ]);
    // DAY was filed from a batch whose condition.json said degraded.
    lock(&r.store).coverage.1 = json!({ "days": [
        { "day": DAY, "volume": "v1", "kind": "complete", "filed": true, "size": 10,
          "condition": { "state": "degraded", "last_modified": "2026-10-04", "source": "batch OPRA-1", "checked_at": 1.0 } },
        { "day": "2026-10-01", "volume": "v1", "kind": "complete", "filed": true, "size": 10,
          "condition": { "state": "degraded", "last_modified": "2026-10-02", "source": "provider api", "checked_at": 1.0 } },
    ], "missing_volumes": [] });
    // As the store answers (PR #49): no last_modified in what changed.
    let folder = format!("/v1/Databento/{DS}/SPY.OPT/{SCHEMA}");
    lock(&r.store).conditions_reply.1 = json!({
        "changed": [
            { "dataset": DS, "day": DAY, "volume": "v1", "folder": folder, "from": "degraded", "to": "available" },
            { "dataset": DS, "day": "2026-10-01", "volume": "v1", "folder": folder, "from": "degraded", "to": "available" },
        ],
        "unchanged": 0,
        "not_held": [],
    });
    let done = conditions::sync(&r.jobs).await.unwrap();
    assert_eq!((done.rows, done.failed.len()), (2, 0), "{done:?}");
    assert_eq!(
        lock(&r.fake.s).conditions_asked,
        [DS],
        "only Databento's datasets"
    );
    let (auth, rows) = lock(&r.store).conditions_posted[0].clone();
    assert_eq!(auth.as_deref(), Some("Bearer store-token"));
    assert_eq!(
        rows,
        json!([
            { "dataset": DS, "day": "2026-10-01", "state": "available", "last_modified": "2026-10-02" },
            { "dataset": DS, "day": DAY, "state": "available", "last_modified": "2026-10-20" },
        ])
    );
    let told = &done.changed[0];
    assert!(
        told.contains("degraded → available")
            && told.contains("re-released 2026-10-20")
            && told.contains("downloading it again"),
        "{told}"
    );
    // A day whose condition didn't come from a batch has no such hint.
    let other = &done.changed[1];
    assert!(
        other.contains("2026-10-01")
            && other.contains("re-released 2026-10-02")
            && !other.contains("downloading"),
        "{other}"
    );
    // The condition's source was read from the store before it was replaced.
    assert!(
        lock(&r.store)
            .coverage_asked
            .iter()
            .any(|q| q.contains("provider=databento") && q.contains("symbol=SPY.OPT"))
    );
    assert_eq!(r.fake.submits(), 0, "nothing is bought");
}

#[tokio::test]
async fn a_failed_condition_check_is_reported_not_fatal() {
    let r = rig("conditions-fail").await;
    lock(&r.store).catalog.1 = holding();
    let done = conditions::sync(&r.jobs).await.unwrap();
    assert_eq!(done.failed.len(), 1, "{done:?}");
    lock(&r.store).catalog = (503, json!({}));
    let done = conditions::sync(&r.jobs).await.unwrap();
    assert!(done.failed[0].contains("503"), "{done:?}");
    // The store refusing them is reported too.
    lock(&r.store).catalog.1 = holding();
    lock(&r.store).catalog.0 = 200;
    lock(&r.fake.s).conditions = Some(vec![condition(
        DAY,
        DatasetCondition::Degraded,
        Some("2026-10-02"),
    )]);
    lock(&r.store).conditions_reply = (401, json!({ "error": "no token" }));
    let done = conditions::sync(&r.jobs).await.unwrap();
    assert!(done.failed[0].contains("401"), "{done:?}");
}

#[tokio::test]
async fn condition_checks_wait_while_capture_is_live() {
    let r = rig_with("conditions-yield", |j| {
        j.yield_to = vec![("relay".into(), j.store.clone())]
    })
    .await;
    lock(&r.store).catalog.1 = holding();
    lock(&r.fake.s).conditions = Some(vec![condition(DAY, DatasetCondition::Available, None)]);
    lock(&r.store).health = "ready";
    assert!(conditions::sync(&r.jobs).await.is_none());
    assert!(
        lock(&r.store).conditions_posted.is_empty() && lock(&r.fake.s).conditions_asked.is_empty()
    );
    lock(&r.store).health = "idle";
    assert_eq!(conditions::sync(&r.jobs).await.unwrap().rows, 1);
}

#[tokio::test]
async fn a_quote_marks_requests_on_days_that_arent_available() {
    let r = rig("quote-conditions").await;
    lock(&r.fake.s).conditions = Some(vec![
        condition("2026-10-01", DatasetCondition::Degraded, Some("2026-10-02")),
        condition(DAY, DatasetCondition::Available, Some("2026-10-03")),
    ]);
    let rth = json!({ "start": "09:30", "end": "16:01" });
    let (_, v) = r
        .price(json!({ "start": "2026-10-01", "end": DAY, "hours": rth }))
        .await;
    let rows = rows(&v);
    assert_eq!(
        (rows[0]["condition"].as_str(), rows[1].get("condition")),
        (Some("degraded"), None),
        "{v}"
    );
    assert!(v["notes"].to_string().contains("degraded"), "{v}");
}

#[tokio::test]
async fn a_day_with_no_condition_before_is_told_too() {
    let r = rig("conditions-new").await;
    lock(&r.store).catalog.1 = holding();
    lock(&r.fake.s).conditions = Some(vec![condition(
        DAY,
        DatasetCondition::Degraded,
        Some("2026-10-02"),
    )]);
    lock(&r.store).conditions_reply.1 = json!({
        "changed": [{ "dataset": DS, "day": DAY, "volume": "v1", "folder": "/v1", "from": null, "to": "degraded" }],
        "unchanged": 3,
        "not_held": [{ "dataset": DS, "day": "2026-10-05" }],
    });
    let done = conditions::sync(&r.jobs).await.unwrap();
    assert_eq!(
        done.changed,
        [format!(
            "{DS} {DAY}: none → degraded (on v1), re-released 2026-10-02"
        )]
    );
}

/// A done job of SPY contracts, followed (bought here), with one file at
/// the fake store, which has room.
fn contract_job(r: &Rig, id: &str) {
    r.fake.add(job(
        id,
        json!(format!("{C1},{C2}")),
        "raw_symbol",
        whole_day(),
        "done",
    ));
    let url = r.jobs.store.clone();
    lock(&r.fake.s).files.insert(
        id.into(),
        vec![BatchFileDesc {
            filename: format!("opra-pillar-{}.cmbp-1.dbn.zst", DAY.replace('-', "")),
            size: 9,
            hash: "sha256:00".into(),
            urls: [("https".to_owned(), format!("{url}/dl/x"))].into(),
        }],
    );
    lock(&r.store).catalog.1 = json!({ "roots": [{ "name": "v1", "writable": true,
        "volume": { "total_bytes": 1e12, "available_bytes": 5e11 }, "datasets": [] }],
        "thresholds": { "critical": 90 }, "missing_volumes": [] });
    lock(&r.jobs.follow).insert(id.into());
}

/// Until no download is queued or running.
async fn settled(r: &Rig) {
    for _ in 0..200 {
        if lock(&r.jobs.in_flight).is_empty() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("a download never finished");
}

#[test]
fn contract_lists_of_one_root_are_fileable() {
    let raw = |s: &str| job("OPRA-1", json!(s), "raw_symbol", whole_day(), "done");
    assert_eq!(unfileable(&raw(&format!("{C1},{C2}"))), None);
    assert_eq!(
        contract_parent(&raw(&format!("{C1},{C2}"))).as_deref(),
        Some("SPY.OPT")
    );
    for refused in [
        raw(&format!("{C1},QQQ   261002C00500000")),
        raw(&format!("{C1},SPY261002C00728000")),
        job(
            "OPRA-2",
            json!("SPY.OPT,QQQ.OPT"),
            "parent",
            whole_day(),
            "done",
        ),
    ] {
        assert!(unfileable(&refused).is_some(), "{:?}", refused.symbols);
    }
}

#[tokio::test]
async fn a_contract_list_job_is_filed_as_partial_days_of_its_parent() {
    let r = rig("file-contracts").await;
    contract_job(&r, "OPRA-C1");
    refresh(&r.jobs).await.unwrap();
    settled(&r).await;
    let s = lock(&r.store);
    assert_eq!(
        s.incorporated,
        ["OPRA-C1?parent=SPY.OPT"],
        "{:?}",
        lock(&r.jobs.transfers)
            .get("OPRA-C1")
            .map(|t| t.error.clone())
    );
    assert_eq!(
        s.uploaded.len(),
        2,
        "the file and the manifest: {:?}",
        s.uploaded
    );
}

#[tokio::test]
async fn a_job_is_downloaded_once_however_often_refreshed() {
    let r = rig("once").await;
    contract_job(&r, "OPRA-C1");
    lock(&r.store).incorporate_delay_ms = 150;
    for _ in 0..3 {
        refresh(&r.jobs).await.unwrap();
    }
    settled(&r).await;
    assert_eq!(lock(&r.store).incorporated.len(), 1);
    // Already filed (the store says so): nothing is fetched or filed again.
    lock(&r.store).batches = json!({ "OPRA-C1": { "volume": "v1", "how": "filed" } });
    let (code, _) = download(
        AxState(r.jobs.clone()),
        UrlPath("OPRA-C1".into()),
        Query(OriginQuery::default()),
    )
    .await;
    assert_eq!(code, StatusCode::ACCEPTED);
    settled(&r).await;
    let s = lock(&r.store);
    assert_eq!((s.incorporated.len(), s.uploaded.len()), (1, 2));
}

/// Decision 0033: a job bought by hand is downloaded and filed as soon as it's
/// done, while capture is live (the refresh is paused then); one bought on a
/// schedule isn't. An expired one is forgotten.
#[tokio::test]
async fn a_job_bought_by_hand_is_filed_during_capture() {
    let r = rig_with("bought-by-hand", |j| {
        j.yield_to = vec![("relay".into(), j.store.clone())];
    })
    .await;
    lock(&r.store).health = "ready";
    contract_job(&r, "OPRA-C1");
    contract_job(&r, "OPRA-C2");
    lock(&r.jobs.manual).insert("OPRA-C1".into());
    start_manual_downloads(&r.jobs).await;
    settled(&r).await;
    let filed = lock(&r.store).incorporated.clone();
    assert_eq!(filed.len(), 1, "{filed:?}");
    assert!(filed[0].starts_with("OPRA-C1"), "{filed:?}");
    r.fake.add(job(
        "OPRA-X1",
        json!("SPY.OPT"),
        "parent",
        whole_day(),
        "expired",
    ));
    lock(&r.jobs.manual).insert("OPRA-X1".into());
    start_manual_downloads(&r.jobs).await;
    assert!(!lock(&r.jobs.manual).contains("OPRA-X1"));
}

/// The ledger says which submissions were made by hand; older entries, with no
/// origin, weren't (they keep yielding).
#[test]
fn the_ledger_keeps_who_asked_for_a_submission() {
    let dir = std::env::temp_dir().join(format!(
        "tinytrader-jobs-origin-{}-{}",
        std::process::id(),
        tinytrader_observe::unix_seconds() as u64
    ));
    let mut l = ledger::Ledger::open(&dir.join("submissions.jsonl")).unwrap();
    for (job, origin) in [
        ("OPRA-M", Some("manual")),
        ("OPRA-S", Some("scheduled")),
        ("OPRA-O", None),
    ] {
        l.append(ledger::Entry {
            at: 1.0,
            request: job.into(),
            state: "submitted".into(),
            job: Some(job.into()),
            origin: origin.map(Into::into),
            ..ledger::Entry::default()
        })
        .unwrap();
    }
    // Read back, as at a restart.
    drop(l);
    let l = ledger::Ledger::open(&dir.join("submissions.jsonl")).unwrap();
    assert_eq!(l.submitted_by_hand(), HashSet::from(["OPRA-M".to_owned()]));
    assert_eq!(l.submitted().len(), 3);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn a_download_waits_while_the_store_rescans() {
    let r = rig("rescan").await;
    contract_job(&r, "OPRA-C1");
    lock(&r.store).catalog_503s = 3;
    refresh(&r.jobs).await.unwrap();
    settled(&r).await;
    assert_eq!(lock(&r.store).incorporated.len(), 1);
    // Not forever.
    let r = rig_with("rescan-long", |j| j.store_wait = Duration::from_millis(30)).await;
    contract_job(&r, "OPRA-C2");
    lock(&r.store).catalog_503s = 1000;
    refresh(&r.jobs).await.unwrap();
    settled(&r).await;
    let t = lock(&r.jobs.transfers).get("OPRA-C2").cloned().unwrap();
    assert!(t.error.unwrap().contains("503"));
}

/// A busy store (503) is waited for and asked again; it costs no try.
#[tokio::test]
async fn a_busy_store_is_waited_for_not_a_failed_filing() {
    let r = rig("busy-filing").await;
    contract_job(&r, "OPRA-C3");
    lock(&r.store).incorporate_busy = 3;
    refresh(&r.jobs).await.unwrap();
    settled(&r).await;
    assert_eq!(lock(&r.store).incorporated.len(), 1);
    let t = lock(&r.jobs.transfers).get("OPRA-C3").cloned().unwrap();
    assert_eq!(t.state, "filed", "{:?}", t.error);
    assert!(!lock(&r.jobs.tries).contains_key("OPRA-C3"));
}

#[tokio::test]
async fn a_failed_filing_is_tried_again_a_few_times() {
    let r = rig("retry-filing").await;
    contract_job(&r, "OPRA-C1");
    lock(&r.store).incorporate_error = Some("a day's file is missing".into());
    for _ in 0..8 {
        refresh(&r.jobs).await.unwrap();
        settled(&r).await;
        // Left in the store's incoming folder by the failed filing.
        lock(&r.store).batches = json!({ "OPRA-C1": { "volume": "v1", "how": "incoming" } });
    }
    assert_eq!(lock(&r.store).incorporated.len(), MAX_TRIES as usize);
    let view = jobs_view(&r.jobs);
    let row = view["jobs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|j| j["id"] == "OPRA-C1")
        .unwrap()
        .clone();
    assert_eq!(row["transfer"]["state"], "failed");
    assert!(
        row["transfer"]["error"]
            .as_str()
            .unwrap()
            .contains("a day's file is missing"),
        "{row}"
    );
}

#[tokio::test]
async fn a_submission_shows_its_progress_and_why_it_stopped() {
    let r = rig("progress").await;
    let days = json!({ "start": "2026-10-01", "end": "2026-10-05", "hours": { "start": "09:30", "end": "16:01" } });
    let (_, v) = r.price(days.clone()).await;
    let quote = v["quote"].as_str().unwrap().to_owned();
    assert_eq!(rows(&v).len(), 5);
    // The third hangs a while: meanwhile, it shows as submitting.
    lock(&r.fake.s).stop_at = Some((2, true));
    let sent = tokio::spawn({
        let jobs = r.jobs.clone();
        let quote = quote.clone();
        async move {
            submit(
                AxState(jobs),
                axum::http::HeaderMap::new(),
                Json(Submit {
                    quote,
                    confirm_usd: None,
                    again: None,
                    approved_by: None,
                    origin: None,
                }),
            )
            .await
        }
    });
    let mut seen = Value::Null;
    for _ in 0..200 {
        let (_, Json(v)) = quote_get(AxState(r.jobs.clone()), UrlPath(quote.clone())).await;
        seen = v;
        if seen["submitted"] == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(seen["submitted"], 2, "{seen}");
    assert_eq!(
        (seen["state"].as_str(), seen["of"].as_u64()),
        (Some("submitting"), Some(5))
    );
    assert_eq!(seen["jobs"].as_array().map(Vec::len), Some(2), "{seen}");
    let (code, _) = sent.await.unwrap();
    assert_eq!(code, StatusCode::BAD_GATEWAY);
    let (_, Json(v)) = quote_get(AxState(r.jobs.clone()), UrlPath(quote.clone())).await;
    assert_eq!(
        (
            v["state"].as_str(),
            v["submitted"].as_u64(),
            v["of"].as_u64()
        ),
        (Some("failed"), Some(2), Some(5)),
        "{v}"
    );
    assert!(
        v["error"]
            .as_str()
            .unwrap()
            .contains("no answer from Databento"),
        "{v}"
    );
    let told = r.jobs.events.since(0, 500).to_string();
    assert!(
        told.contains(&format!(
            "submission of {quote} stopped (502) after 2 of 5 requests"
        )),
        "{told}"
    );
    // The rest: the same request priced again quotes exactly what wasn't sent
    // (the third, its outcome unknown, is never bought again).
    lock(&r.fake.s).stop_at = None;
    let (_, v) = r.price(days).await;
    let left: Vec<String> = rows(&v)
        .iter()
        .map(|x| x["first_day"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(left, ["2026-10-04", "2026-10-05"], "{v}");
}

#[tokio::test]
async fn a_refused_submission_is_told() {
    let r = paid("told", 2.5).await;
    let quote = r.quote("SPY.OPT", DAY, DAY).await;
    assert_eq!(r.submit(&quote, None).await.0, StatusCode::FORBIDDEN);
    let told = r.jobs.events.since(0, 500).to_string();
    assert!(
        told.contains(&format!("submission of {quote} stopped (403)")) && told.contains("approval"),
        "{told}"
    );
    // A refusal from Databento midway (a 429) is told with how far it got.
    let r = rig("told-429").await;
    let (_, v) = r.price(json!({ "start": "2026-10-01", "end": "2026-10-02", "hours": { "start": "09:30", "end": "16:01" } })).await;
    lock(&r.fake.s).stop_at = Some((1, false));
    let quote = v["quote"].as_str().unwrap().to_owned();
    assert_eq!(r.submit(&quote, None).await.0, StatusCode::BAD_GATEWAY);
    let (c, Json(v)) = quote_get(AxState(r.jobs.clone()), UrlPath(quote)).await;
    fits("get", "/quotes/{id}", c, &v);
    assert_eq!(
        (v["state"].as_str(), v["submitted"].as_u64()),
        (Some("failed"), Some(1))
    );
    assert!(v["error"].as_str().unwrap().contains("429"), "{v}");
}

#[tokio::test]
async fn a_job_left_failed_is_tried_again_after_a_while() {
    let r = rig_with("rearm", |j| j.rearm_after = Duration::from_millis(50)).await;
    contract_job(&r, "OPRA-C1");
    lock(&r.store).incorporate_error = Some("a day's file is missing".into());
    for _ in 0..MAX_TRIES + 2 {
        refresh(&r.jobs).await.unwrap();
        settled(&r).await;
    }
    assert_eq!(lock(&r.store).incorporated.len(), MAX_TRIES as usize);
    tokio::time::sleep(Duration::from_millis(80)).await;
    lock(&r.store).incorporate_error = None;
    refresh(&r.jobs).await.unwrap();
    settled(&r).await;
    assert_eq!(lock(&r.store).incorporated.len(), MAX_TRIES as usize + 1);
}

#[tokio::test]
async fn a_job_near_its_expiry_and_not_filed_is_told() {
    let r = rig("expiring").await;
    let mut j = job(
        "OPRA-OLD",
        json!("SPY.OPT,QQQ.OPT"),
        "parent",
        whole_day(),
        "done",
    );
    j.ts_expiration = Some(time::OffsetDateTime::now_utc() + time::Duration::days(3));
    r.fake.add(j);
    refresh(&r.jobs).await.unwrap();
    refresh(&r.jobs).await.unwrap();
    let row = jobs_view(&r.jobs)["jobs"][0].clone();
    assert_eq!(row["expires_soon"], true, "{row}");
    let told = r.jobs.events.since(0, 500).to_string();
    assert_eq!(
        r.jobs.events.since(0, 500)["events"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["summary"]
                .as_str()
                .unwrap()
                .starts_with("OPRA-OLD isn't filed and expires"))
            .count(),
        1,
        "{told}"
    );
}

#[tokio::test]
async fn a_merge_waits_for_room_on_the_days_volume() {
    let r = rig_with("merge-room", |j| j.merge_wait = Duration::from_millis(30)).await;
    contract_job(&r, "OPRA-C1");
    // The day it merges into is 1 TB, on v1, which has room for 400 GB.
    lock(&r.store).coverage.1 = json!({ "days": [
        { "day": DAY, "kind": "partial", "volumes": ["v1"], "size": 1_000_000_000_000_u64, "coverage": { "kind": "contracts", "symbols": [C1] } },
    ], "missing_volumes": [] });
    refresh(&r.jobs).await.unwrap();
    settled(&r).await;
    let t = lock(&r.jobs.transfers).get("OPRA-C1").cloned().unwrap();
    assert!(
        t.error.unwrap().contains("v1 needs 1000.0 GB to merge"),
        "{}",
        t.state
    );
    assert!(lock(&r.store).uploaded.is_empty());
}

#[tokio::test]
async fn the_store_s_incoming_list_is_waited_for_too() {
    let r = rig("incoming").await;
    contract_job(&r, "OPRA-C1");
    // The day's file is uploaded already: once the store answers, it isn't sent again.
    let name = format!("opra-pillar-{}.cmbp-1.dbn.zst", DAY.replace('-', ""));
    lock(&r.store).incoming =
        json!({ "batches": [{ "batch": "OPRA-C1", "files": [{ "name": name, "bytes": 9 }] }] });
    lock(&r.store).incoming_503s = 2;
    refresh(&r.jobs).await.unwrap();
    settled(&r).await;
    let s = lock(&r.store);
    assert_eq!(
        (s.incorporated.len(), s.uploaded.len()),
        (1, 1),
        "only the manifest: {:?}",
        s.uploaded
    );
}

#[test]
fn startup_says_how_many_jobs_are_followed() {
    let dir =
        std::env::temp_dir().join(format!("tinytrader-jobs-{}-following", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mut l = ledger::Ledger::open(&dir.join("submissions.jsonl")).unwrap();
    for (n, job) in ["OPRA-1", "OPRA-2"].iter().enumerate() {
        l.append(ledger::Entry {
            at: 1.0,
            request: format!("r{n}"),
            state: "submitted".into(),
            job: Some((*job).into()),
            ..Default::default()
        })
        .unwrap();
    }
    assert!(following(&l).starts_with("following 2 jobs submitted here"));
    drop(l);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A store that answers each connection as `script` says, in turn: `drop`
/// (closed unanswered), `cut` (an answer cut off), or a status (with `body`
/// when 200). Counts the connections.
async fn flaky_store(script: &[&'static str], body: Vec<u8>) -> (String, Arc<AtomicUsize>) {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("http://{}", l.local_addr().unwrap());
    let seen = Arc::new(AtomicUsize::new(0));
    let (script, count) = (script.to_vec(), seen.clone());
    tokio::spawn(async move {
        loop {
            let (mut s, _) = l.accept().await.unwrap();
            let step = script
                .get(count.fetch_add(1, Ordering::SeqCst))
                .copied()
                .unwrap_or("200");
            let mut req = Vec::new();
            let mut buf = [0u8; 1024];
            while !req.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = s.read(&mut buf).await.unwrap();
                req.extend_from_slice(&buf[..n]);
            }
            let head = |code: &str, len: usize| {
                format!("HTTP/1.1 {code} X\r\ncontent-length: {len}\r\nconnection: close\r\n\r\n")
            };
            let out = match step {
                "drop" => Vec::new(),
                "cut" => [head("200", body.len() + 100).as_bytes(), &body[..1]].concat(),
                "200" => [head("200", body.len()).as_bytes(), &body].concat(),
                code => head(code, 0).into_bytes(),
            };
            let _ = s.write_all(&out).await;
        }
    });
    (addr, seen)
}

#[tokio::test]
async fn store_reads_are_tried_again_after_a_dropped_answer() {
    let ok = serde_json::to_vec(&json!({ "days": [], "missing_volumes": [] })).unwrap();
    let cases: [(&[&str], bool, usize); 4] = [
        (&["drop", "cut"], true, 3),
        (&["503", "drop"], true, 3),
        (&["503", "503", "503"], false, 3),
        (&["500"], false, 1),
    ];
    for (script, works, tries) in cases {
        let (url, seen) = flaky_store(script, ok.clone()).await;
        let r = rig_with("flaky", |j| j.store = url).await;
        let got = store_json(&r.jobs, "/coverage", &[], false).await;
        let n = seen.load(Ordering::SeqCst);
        assert_eq!((got.is_ok(), n), (works, tries), "{script:?}: {got:?}");
        let _ = std::fs::remove_dir_all(&r.dir);
    }
    // The definitions read and a definition day's file, the same way.
    let file = format!("{DS}/SPY.OPT/definition/{DAY}.dbn.zst");
    let listing = json!({ "files": [{ "volume": "v1", "path": file }] });
    let (url, seen) = flaky_store(&["drop"], serde_json::to_vec(&listing).unwrap()).await;
    let r = rig_with("flaky-defs", |j| j.store = url).await;
    let defs = super::definitions(&r.jobs, DS, "SPY.OPT").await.unwrap();
    assert_eq!((defs.len(), seen.load(Ordering::SeqCst)), (1, 2));
    let (url, seen) = flaky_store(&["cut"], definitions(&[C1])).await;
    let url = format!("{url}/files/v1/{file}");
    let day = store_get(&r.jobs, &url, missing::contracts).await;
    let n = seen.load(Ordering::SeqCst);
    assert_eq!((day.map(|c| c.len()), n), (Ok(1), 2));
    let _ = std::fs::remove_dir_all(&r.dir);
}

/// Downloads running now, the most at once, and each job's in the order
/// they started.
type Source = Arc<Mutex<(usize, usize, Vec<String>)>>;

/// Contract jobs whose files a source serves, each taking `ms` to answer.
async fn slow_jobs(r: &Rig, ids: &[&str], ms: u64) -> Source {
    let (addr, seen) = source(ms).await;
    for id in ids {
        contract_job(r, id);
        for f in lock(&r.fake.s).files.get_mut(*id).unwrap() {
            f.urls = [("https".to_owned(), format!("{addr}/dl/{id}"))].into();
        }
    }
    seen
}

/// A source of downloads, each taking `ms` to answer: its address.
async fn source(ms: u64) -> (String, Source) {
    let seen = Source::default();
    let app = Router::new()
        .route(
            "/dl/{job}",
            get(
                move |AxState(s): AxState<Source>, UrlPath(job): UrlPath<String>| async move {
                    {
                        let mut s = lock(&s);
                        s.0 += 1;
                        s.1 = s.1.max(s.0);
                        s.2.push(job);
                    }
                    tokio::time::sleep(Duration::from_millis(ms)).await;
                    lock(&s).0 -= 1;
                    "dbn bytes"
                },
            ),
        )
        .with_state(seen.clone());
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(l, app).await });
    (format!("http://{addr}"), seen)
}

const FIVE: [&str; 5] = ["OPRA-D1", "OPRA-D2", "OPRA-D3", "OPRA-D4", "OPRA-D5"];

#[tokio::test]
async fn four_downloads_run_at_once_and_a_fifth_waits() {
    let r = rig("four-at-once").await;
    // Each download takes a second: had the fifth not waited, five would
    // have run at once.
    let seen = slow_jobs(&r, &FIVE, 1_000).await;
    for id in FIVE.iter().chain(&FIVE) {
        // Each job asked for twice: run once.
        start_download(&r.jobs, (*id).to_owned(), false);
    }
    for _ in 0..1_000 {
        if lock(&r.jobs.in_flight).is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let (_, most, order) = lock(&seen).clone();
    assert_eq!((most, order.len()), (4, 5));
    assert_eq!(lock(&r.store).incorporated.len(), 5);
}

#[tokio::test]
async fn with_one_download_at_a_time_they_keep_their_order() {
    let r = rig_with("one-at-once", |j| {
        j.downloads = tokio::sync::Semaphore::new(1)
    })
    .await;
    let seen = slow_jobs(&r, &FIVE, 20).await;
    for id in FIVE {
        start_download(&r.jobs, id.to_owned(), false);
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    settled(&r).await;
    let (_, most, order) = lock(&seen).clone();
    assert_eq!((most, order), (1, FIVE.map(str::to_owned).to_vec()));
}

#[tokio::test]
async fn a_filing_in_progress_doesnt_hold_a_download_back() {
    let r = rig_with("filing-apart", |j| {
        j.downloads = tokio::sync::Semaphore::new(1)
    })
    .await;
    let seen = slow_jobs(&r, &FIVE[..2], 10).await;
    lock(&r.store).incorporate_delay_ms = 2_000;
    start_download(&r.jobs, FIVE[0].to_owned(), false);
    start_download(&r.jobs, FIVE[1].to_owned(), false);
    for _ in 0..100 {
        if lock(&seen).2.len() == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // The second downloaded while the first was still filing.
    assert_eq!(lock(&seen).2.len(), 2);
    assert!(lock(&r.store).incorporated.is_empty());
    assert_eq!(lock(&r.jobs.transfers)[FIVE[0]].state, "filing");
}

#[tokio::test]
async fn the_capture_live_pause_holds_every_download() {
    let r = rig("pause-all").await;
    let seen = slow_jobs(&r, &FIVE, 10).await;
    r.jobs.paused_until.store(1, Ordering::Relaxed);
    for id in FIVE {
        start_download(&r.jobs, id.to_owned(), false);
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(lock(&seen).2.is_empty());
    let t = lock(&r.jobs.transfers);
    let count = |state: &str| FIVE.iter().filter(|id| t[**id].state == state).count();
    // Four hold their turn, waiting out the capture; the fifth waits for a turn.
    assert_eq!(
        (count("waiting: capture is live"), count("waiting")),
        (4, 1)
    );
}

#[tokio::test]
async fn downloads_in_flight_hold_their_room() {
    let r = rig("room-in-flight").await;
    let seen = slow_jobs(&r, &FIVE[..2], 500).await;
    // Room for one 9-byte job, not two.
    lock(&r.store).catalog.1["roots"][0]["volume"]["available_bytes"] = json!(1e11 + 15.0);
    for id in &FIVE[..2] {
        start_download(&r.jobs, (*id).to_owned(), false);
    }
    let mut waited = false;
    for _ in 0..500 {
        waited |= lock(&r.jobs.transfers)
            .values()
            .any(|t| t.state.contains("downloads in flight hold the room"));
        if lock(&r.jobs.in_flight).is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(waited);
    assert_eq!((lock(&seen).1, lock(&r.store).incorporated.len()), (1, 2));
    assert!(lock(&r.jobs.reserved).is_empty(), "released once filed");
}

#[tokio::test]
async fn bytes_landed_count_once() {
    let r = rig("room-landed").await;
    let ((fast, seen), (slow, _)) = (source(10).await, source(3_000).await);
    // A: two 9-byte files, the second slow. B fits beside what A has left
    // to land; C, 12 bytes, doesn't.
    let file = |name: &str, size: u64, url: &str, id: &str| BatchFileDesc {
        filename: format!("opra-pillar-{name}.cmbp-1.dbn.zst"),
        size,
        hash: "sha256:00".into(),
        urls: [("https".to_owned(), format!("{url}/dl/{id}"))].into(),
    };
    for (id, files) in [
        (
            "OPRA-A",
            vec![
                file("20261002", 9, &fast, "A"),
                file("20261005", 9, &slow, "A"),
            ],
        ),
        ("OPRA-B", vec![file("20261002", 9, &fast, "B")]),
        ("OPRA-C", vec![file("20261002", 12, &fast, "C")]),
    ] {
        contract_job(&r, id);
        lock(&r.fake.s).files.insert(id.into(), files);
    }
    // 20 bytes free, A's first file in it already.
    lock(&r.store).catalog.1["roots"][0]["volume"]["available_bytes"] = json!(1e11 + 20.0);
    start_download(&r.jobs, "OPRA-A".into(), false);
    // Its first file landed: only the second is held.
    for _ in 0..300 {
        if lock(&r.jobs.reserved)
            .get("OPRA-A")
            .is_some_and(|a| a.0 == 9)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(lock(&r.jobs.reserved)["OPRA-A"].0, 9);
    start_download(&r.jobs, "OPRA-B".into(), false);
    start_download(&r.jobs, "OPRA-C".into(), false);
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(lock(&seen).2.contains(&"B".to_owned()), "B went ahead");
    assert!(!lock(&seen).2.contains(&"C".to_owned()));
    let t = lock(&r.jobs.transfers);
    assert!(
        t["OPRA-C"]
            .state
            .contains("downloads in flight hold the room"),
        "{}",
        t["OPRA-C"].state
    );
}

/// A done job of one file at the fake store's `/dl/x` ("dbn bytes"), listed
/// with `hash`, for the cache.
fn cache_job(r: &Rig, id: &str, hash: &str) {
    r.fake
        .add(job(id, json!("SPY.OPT"), "parent", whole_day(), "done"));
    let url = r.jobs.store.clone();
    lock(&r.fake.s).files.insert(
        id.into(),
        vec![BatchFileDesc {
            filename: format!("opra-pillar-{}.tcbbo.dbn.zst", DAY.replace('-', "")),
            size: 9,
            hash: hash.into(),
            urls: [("https".to_owned(), format!("{url}/dl/x"))].into(),
        }],
    );
}

/// Until the job's cache download has ended; its last state.
async fn cache_done(r: &Rig, id: &str) -> (String, Option<String>) {
    for _ in 0..300 {
        if let Some(t) = lock(&r.jobs.caching).get(id)
            && (t.state == "cached" || t.state == "failed")
        {
            return (t.state.clone(), t.error.clone());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("{id} never finished caching");
}

const DBN_BYTES_SHA256: &str =
    "sha256:a0352b911ea12767c932c228fc48328dc31e7380cb49d20141d2d79712883079";

#[tokio::test]
async fn a_done_job_is_cached_whole_and_checked_outside_the_store() {
    let folder = std::env::temp_dir().join(format!("tinytrader-jobs-cache-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&folder);
    let dir = folder.clone();
    let r = rig_with("cache", move |j| j.cache_dir = Some(dir)).await;
    cache_job(&r, "OPRA-K1", DBN_BYTES_SHA256);
    let (code, Json(v)) = cache(
        AxState(r.jobs.clone()),
        UrlPath("OPRA-K1".into()),
        Query(OriginQuery::default()),
    )
    .await;
    assert_eq!(code, StatusCode::ACCEPTED, "{v}");
    assert_eq!(cache_done(&r, "OPRA-K1").await, ("cached".into(), None));
    let file = folder.join("OPRA-K1").join(format!(
        "opra-pillar-{}.tcbbo.dbn.zst",
        DAY.replace('-', "")
    ));
    assert_eq!(std::fs::read(&file).unwrap(), b"dbn bytes");
    let manifest: Value =
        serde_json::from_slice(&std::fs::read(folder.join("OPRA-K1/manifest.json")).unwrap())
            .unwrap();
    assert_eq!(manifest["job_id"], "OPRA-K1");
    let complete: Value =
        serde_json::from_slice(&std::fs::read(folder.join("OPRA-K1/COMPLETE")).unwrap()).unwrap();
    assert_eq!(
        (complete["job_id"].as_str(), complete["files"].as_u64()),
        (Some("OPRA-K1"), Some(1))
    );
    // Nothing went to the store.
    assert!(lock(&r.store).uploaded.is_empty());
    // Again: the whole file is kept, not fetched again.
    let before = std::fs::metadata(&file).unwrap().modified().unwrap();
    let (code, _) = cache(
        AxState(r.jobs.clone()),
        UrlPath("OPRA-K1".into()),
        Query(OriginQuery::default()),
    )
    .await;
    assert_eq!(code, StatusCode::ACCEPTED);
    assert_eq!(cache_done(&r, "OPRA-K1").await.0, "cached");
    assert_eq!(
        std::fs::metadata(&file).unwrap().modified().unwrap(),
        before
    );
    // Deleted on request.
    let (code, _) = uncache(AxState(r.jobs.clone()), UrlPath("OPRA-K1".into())).await;
    assert_eq!(code, StatusCode::OK);
    assert!(!folder.join("OPRA-K1").exists());
    let (code, _) = uncache(AxState(r.jobs.clone()), UrlPath("OPRA-K1".into())).await;
    assert_eq!(code, StatusCode::NOT_FOUND);
    let _ = std::fs::remove_dir_all(&folder);
}

#[tokio::test]
async fn a_cached_file_that_fails_its_checksum_is_not_kept() {
    let folder =
        std::env::temp_dir().join(format!("tinytrader-jobs-cache-bad-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&folder);
    let dir = folder.clone();
    let r = rig_with("cache-bad", move |j| j.cache_dir = Some(dir)).await;
    cache_job(&r, "OPRA-K2", "sha256:00");
    let (code, _) = cache(
        AxState(r.jobs.clone()),
        UrlPath("OPRA-K2".into()),
        Query(OriginQuery::default()),
    )
    .await;
    assert_eq!(code, StatusCode::ACCEPTED);
    let (state, err) = cache_done(&r, "OPRA-K2").await;
    assert_eq!(state, "failed");
    assert!(err.unwrap().contains("SHA-256"));
    let left: Vec<_> = std::fs::read_dir(folder.join("OPRA-K2")).unwrap().collect();
    assert!(left.is_empty(), "{left:?}");
    let _ = std::fs::remove_dir_all(&folder);
}

fn cache_rows(v: &Value) -> Vec<(String, String, Option<u64>)> {
    v["cache"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| {
            (
                r["job"].as_str().unwrap().to_owned(),
                r["state"].as_str().unwrap().to_owned(),
                r["position"].as_u64(),
            )
        })
        .collect()
}

/// A scheduled request's origin (decision 0033).
fn sched() -> Query<OriginQuery> {
    Query(OriginQuery {
        origin: Some("scheduled".into()),
    })
}

/// Decision 0033: while capture is live, a cache request asked for by hand
/// (the default) downloads at once; the queue takes it before a scheduled one,
/// which waits. A download asked for by hand doesn't wait either. An unknown
/// origin is refused.
#[tokio::test]
async fn work_asked_for_by_hand_runs_through_capture() {
    let folder = cache_folder("manual-live");
    let dir = folder.clone();
    let r = rig_with("cache-manual-live", move |j| {
        j.cache_dir = Some(dir);
        j.yield_to = vec![("relay".into(), j.store.clone())];
    })
    .await;
    lock(&r.store).health = "ready";
    cache_job(&r, "OPRA-H1", DBN_BYTES_SHA256);
    cache_job(&r, "OPRA-H2", DBN_BYTES_SHA256);
    let (code, _) = cache(AxState(r.jobs.clone()), UrlPath("OPRA-H1".into()), sched()).await;
    assert_eq!(code, StatusCode::ACCEPTED);
    cache_state(&r, "OPRA-H1", "waiting").await;
    let (code, Json(v)) = cache(
        AxState(r.jobs.clone()),
        UrlPath("OPRA-H2".into()),
        Query(OriginQuery::default()),
    )
    .await;
    assert_eq!(
        (code, v["origin"].as_str()),
        (StatusCode::ACCEPTED, Some("manual")),
        "{v}"
    );
    // Cached while capture is still live; the scheduled one still waits.
    assert_eq!(cache_done(&r, "OPRA-H2").await, ("cached".into(), None));
    assert_ne!(lock(&r.jobs.caching)["OPRA-H1"].state, "cached");
    // A download asked for by hand doesn't wait for capture either.
    let (code, Json(v)) = download(
        AxState(r.jobs.clone()),
        UrlPath("OPRA-H2".into()),
        Query(OriginQuery::default()),
    )
    .await;
    assert_eq!(
        (code, v["origin"].as_str()),
        (StatusCode::ACCEPTED, Some("manual")),
        "{v}"
    );
    let (code, _) = cache(
        AxState(r.jobs.clone()),
        UrlPath("OPRA-H9".into()),
        Query(OriginQuery {
            origin: Some("whenever".into()),
        }),
    )
    .await;
    assert_eq!(code, StatusCode::BAD_REQUEST);
    let _ = std::fs::remove_dir_all(&folder);
}

/// A cache request made while capture is live is accepted, queued and shown,
/// and runs once capture ends: one at a time, in the order asked.
#[tokio::test]
async fn a_cache_request_during_capture_runs_when_capture_ends() {
    let folder =
        std::env::temp_dir().join(format!("tinytrader-jobs-cache-live-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&folder);
    let dir = folder.clone();
    let r = rig_with("cache-live", move |j| {
        j.cache_dir = Some(dir);
        j.yield_to = vec![("relay".into(), j.store.clone())];
    })
    .await;
    lock(&r.store).health = "ready";
    cache_job(&r, "OPRA-L1", DBN_BYTES_SHA256);
    cache_job(&r, "OPRA-L2", DBN_BYTES_SHA256);
    for id in ["OPRA-L1", "OPRA-L2"] {
        let (code, Json(v)) = cache(AxState(r.jobs.clone()), UrlPath(id.into()), sched()).await;
        assert_eq!(
            (code, v["state"].as_str()),
            (StatusCode::ACCEPTED, Some("queued")),
            "{v}"
        );
        assert!(folder.join(id).join("REQUESTED").exists());
    }
    // Asked again while queued: refused, not queued twice.
    let (code, _) = cache(AxState(r.jobs.clone()), UrlPath("OPRA-L2".into()), sched()).await;
    assert_eq!(code, StatusCode::CONFLICT);
    let mut rows = Vec::new();
    for _ in 0..300 {
        let (_, Json(v)) = cache_list(AxState(r.jobs.clone())).await;
        rows = cache_rows(&v);
        if rows.first().is_some_and(|r| r.1.starts_with("waiting")) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        rows,
        [
            (
                "OPRA-L1".to_owned(),
                "waiting: relay is live".to_owned(),
                None
            ),
            // Both wait their turns in the queue (decision 0033): neither
            // holds the worker while capture is live.
            (
                "OPRA-L2".to_owned(),
                "waiting: relay is live".to_owned(),
                None
            ),
        ]
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !folder.join("OPRA-L1/COMPLETE").exists(),
        "nothing while capture is live"
    );
    lock(&r.store).health = "idle";
    assert_eq!(cache_done(&r, "OPRA-L1").await, ("cached".into(), None));
    assert_eq!(cache_done(&r, "OPRA-L2").await, ("cached".into(), None));
    assert!(
        !folder.join("OPRA-L1/REQUESTED").exists(),
        "the request has ended"
    );
    let _ = std::fs::remove_dir_all(&folder);
}

/// Databento failing for a while (5xx) doesn't fail the request: it's tried
/// again until the file arrives.
#[tokio::test]
async fn a_cache_download_that_fails_for_a_while_is_retried() {
    let folder = std::env::temp_dir().join(format!(
        "tinytrader-jobs-cache-retry-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&folder);
    let dir = folder.clone();
    let r = rig_with("cache-retry", move |j| j.cache_dir = Some(dir)).await;
    lock(&r.store).dl_fail = 7;
    cache_job(&r, "OPRA-R1", DBN_BYTES_SHA256);
    let (code, _) = cache(
        AxState(r.jobs.clone()),
        UrlPath("OPRA-R1".into()),
        Query(OriginQuery::default()),
    )
    .await;
    assert_eq!(code, StatusCode::ACCEPTED);
    assert_eq!(cache_done(&r, "OPRA-R1").await, ("cached".into(), None));
    assert_eq!(lock(&r.store).dl_fail, 0);
    let _ = std::fs::remove_dir_all(&folder);
}

/// A request a restart interrupted (its marker, no `COMPLETE`) is taken up again
/// at startup; one that had finished isn't.
#[tokio::test]
async fn a_restart_resumes_unfinished_cache_requests() {
    let folder = std::env::temp_dir().join(format!(
        "tinytrader-jobs-cache-resume-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&folder);
    for (id, complete) in [("OPRA-S1", false), ("OPRA-S2", true)] {
        std::fs::create_dir_all(folder.join(id)).unwrap();
        std::fs::write(folder.join(id).join("REQUESTED"), r#"{"requested_at": 1}"#).unwrap();
        if complete {
            std::fs::write(folder.join(id).join("COMPLETE"), "{}").unwrap();
        }
    }
    let dir = folder.clone();
    let r = rig_with("cache-resume", move |j| j.cache_dir = Some(dir)).await;
    cache_job(&r, "OPRA-S1", DBN_BYTES_SHA256);
    resume_caching(&r.jobs);
    assert_eq!(cache_done(&r, "OPRA-S1").await, ("cached".into(), None));
    assert!(lock(&r.jobs.caching).get("OPRA-S2").is_none());
    assert!(!folder.join("OPRA-S2/REQUESTED").exists());
    let _ = std::fs::remove_dir_all(&folder);
}

/// A cache folder for a test, emptied.
fn cache_folder(name: &str) -> PathBuf {
    let f = std::env::temp_dir().join(format!(
        "tinytrader-jobs-cache-{name}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&f);
    f
}

/// Until `id`'s cache state starts with `prefix`.
async fn cache_state(r: &Rig, id: &str, prefix: &str) -> String {
    for _ in 0..500 {
        if let Some(t) = lock(&r.jobs.caching).get(id)
            && t.state.starts_with(prefix)
        {
            return t.state.clone();
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!(
        "{id} never reached {prefix}: {:?}",
        lock(&r.jobs.caching).get(id).map(|t| t.state.clone())
    );
}

/// Too little room on the cache's volume: the download waits, showing why, and
/// never starts; with room again it runs.
#[tokio::test]
async fn a_cache_download_waits_for_disk_rather_than_fill_it() {
    let folder = cache_folder("disk");
    let dir = folder.clone();
    let r = rig_with("cache-disk", move |j| {
        j.cache_dir = Some(dir);
    })
    .await;
    cache_job(&r, "OPRA-D1", DBN_BYTES_SHA256);
    set_floor(&r, u64::MAX / 4);
    let (code, _) = cache(
        AxState(r.jobs.clone()),
        UrlPath("OPRA-D1".into()),
        Query(OriginQuery::default()),
    )
    .await;
    assert_eq!(code, StatusCode::ACCEPTED);
    let state = cache_state(&r, "OPRA-D1", "waiting for disk").await;
    assert!(state.contains("keeping"), "{state}");
    assert!(
        !folder
            .join("OPRA-D1")
            .join(format!(
                "opra-pillar-{}.tcbbo.dbn.zst",
                DAY.replace('-', "")
            ))
            .exists()
    );
    set_floor(&r, 0);
    assert_eq!(cache_done(&r, "OPRA-D1").await, ("cached".into(), None));
    let _ = std::fs::remove_dir_all(&folder);
}

/// The floor of free space, changed under a running rig.
fn set_floor(r: &Rig, bytes: u64) {
    r.jobs
        .cache_floor_for_tests
        .store(bytes, std::sync::atomic::Ordering::SeqCst);
}

/// A job that can't be listed for now waits at the back of the queue: the one
/// behind it runs meanwhile. One that has expired fails at once, with why.
#[tokio::test]
async fn a_stalled_or_dead_cache_job_never_holds_up_the_rest() {
    let folder = cache_folder("round");
    let dir = folder.clone();
    let r = rig_with("cache-round", move |j| j.cache_dir = Some(dir)).await;
    // Done, but its files can't be listed yet: transient.
    r.fake.add(job(
        "OPRA-T1",
        json!("SPY.OPT"),
        "parent",
        whole_day(),
        "done",
    ));
    cache_job(&r, "OPRA-T2", DBN_BYTES_SHA256);
    for id in ["OPRA-T1", "OPRA-T2"] {
        let (code, _) = cache(
            AxState(r.jobs.clone()),
            UrlPath(id.into()),
            Query(OriginQuery::default()),
        )
        .await;
        assert_eq!(code, StatusCode::ACCEPTED);
    }
    assert_eq!(cache_done(&r, "OPRA-T2").await, ("cached".into(), None));
    assert!(
        cache_state(&r, "OPRA-T1", "retrying")
            .await
            .contains("list files")
    );
    // Its files appear: it's cached on a later turn.
    cache_job(&r, "OPRA-T1", DBN_BYTES_SHA256);
    assert_eq!(cache_done(&r, "OPRA-T1").await, ("cached".into(), None));
    // Expired once asked for: failed, with the reason, not retried.
    // Done when asked for, with no files listed yet, so it can't finish first.
    r.fake.add(job(
        "OPRA-T3",
        json!("SPY.OPT"),
        "parent",
        whole_day(),
        "done",
    ));
    let (code, _) = cache(
        AxState(r.jobs.clone()),
        UrlPath("OPRA-T3".into()),
        Query(OriginQuery::default()),
    )
    .await;
    assert_eq!(code, StatusCode::ACCEPTED);
    {
        let mut f = lock(&r.fake.s);
        f.files.remove("OPRA-T3");
        f.jobs.retain(|j| j.id != "OPRA-T3");
    }
    r.fake.add(job(
        "OPRA-T3",
        json!("SPY.OPT"),
        "parent",
        whole_day(),
        "expired",
    ));
    let (state, err) = cache_done(&r, "OPRA-T3").await;
    assert_eq!(state, "failed");
    assert!(err.unwrap().contains("expired"));
    let _ = std::fs::remove_dir_all(&folder);
}

/// Permanent or not is decided on the HTTP status, never the error's text: a
/// 404 fails the job, while a message that merely contains "404" and "429" (a
/// job id, a request id) with no status is tried again.
#[tokio::test]
async fn a_listing_error_is_judged_by_its_status_not_its_text() {
    let folder = cache_folder("status");
    let dir = folder.clone();
    let r = rig_with("cache-status", move |j| j.cache_dir = Some(dir)).await;
    cache_job(&r, "OPRA-20240401-X404", DBN_BYTES_SHA256);
    cache_job(&r, "OPRA-GONE", DBN_BYTES_SHA256);
    {
        let mut f = lock(&r.fake.s);
        f.files_fail.insert(
            "OPRA-20240401-X404".into(),
            (
                None,
                "req-4290400 failed: connection reset; job OPRA-20240401-X404".into(),
            ),
        );
        f.files_fail
            .insert("OPRA-GONE".into(), (Some(404), "not found".into()));
    }
    for id in ["OPRA-20240401-X404", "OPRA-GONE"] {
        let (code, _) = cache(
            AxState(r.jobs.clone()),
            UrlPath(id.into()),
            Query(OriginQuery::default()),
        )
        .await;
        assert_eq!(code, StatusCode::ACCEPTED);
    }
    let (state, err) = cache_done(&r, "OPRA-GONE").await;
    assert_eq!(state, "failed");
    assert!(err.unwrap().contains("404"));
    cache_state(&r, "OPRA-20240401-X404", "retrying").await;
    lock(&r.fake.s).files_fail.remove("OPRA-20240401-X404");
    assert_eq!(
        cache_done(&r, "OPRA-20240401-X404").await,
        ("cached".into(), None)
    );
    let _ = std::fs::remove_dir_all(&folder);
}

/// The queue's bound is real: with 256 waiting, the next is refused as busy,
/// its marker not left behind.
#[tokio::test]
async fn a_full_cache_queue_answers_busy() {
    let folder = cache_folder("full");
    let dir = folder.clone();
    let r = rig_with("cache-full", move |j| {
        j.cache_dir = Some(dir);
        j.yield_to = vec![("relay".into(), j.store.clone())];
    })
    .await;
    // Capture is live: the first request waits, the rest stay queued.
    lock(&r.store).health = "ready";
    let ids: Vec<String> = (0..=CACHE_QUEUE + 1)
        .map(|n| format!("OPRA-F{n}"))
        .collect();
    for id in &ids {
        cache_job(&r, id, DBN_BYTES_SHA256);
    }
    let (code, _) = cache(AxState(r.jobs.clone()), UrlPath(ids[0].clone()), sched()).await;
    assert_eq!(code, StatusCode::ACCEPTED);
    cache_state(&r, &ids[0], "waiting").await;
    for id in &ids[1..=CACHE_QUEUE] {
        let (code, _) = cache(AxState(r.jobs.clone()), UrlPath(id.clone()), sched()).await;
        assert_eq!(code, StatusCode::ACCEPTED, "{id}");
    }
    let last = &ids[CACHE_QUEUE + 1];
    let (code, Json(v)) = cache(AxState(r.jobs.clone()), UrlPath(last.clone()), sched()).await;
    assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE, "{v}");
    assert!(!folder.join(last).join("REQUESTED").exists());
    let (_, Json(v)) = cache_list(AxState(r.jobs.clone())).await;
    assert_eq!(v["queued"], CACHE_QUEUE);
    let _ = std::fs::remove_dir_all(&folder);
}

/// More interrupted requests than the queue holds: all are queued again, as
/// room comes, and all are cached.
#[tokio::test]
async fn a_restart_resumes_more_requests_than_the_queue_holds() {
    let folder = cache_folder("resume-many");
    let ids: Vec<String> = (0..CACHE_QUEUE + 40)
        .map(|n| format!("OPRA-M{n}"))
        .collect();
    for (n, id) in ids.iter().enumerate() {
        std::fs::create_dir_all(folder.join(id)).unwrap();
        std::fs::write(
            folder.join(id).join("REQUESTED"),
            format!(r#"{{"requested_at": {n}}}"#),
        )
        .unwrap();
    }
    let dir = folder.clone();
    let r = rig_with("cache-resume-many", move |j| j.cache_dir = Some(dir)).await;
    for id in &ids {
        cache_job(&r, id, DBN_BYTES_SHA256);
    }
    resume_caching(&r.jobs);
    for id in &ids {
        assert_eq!(cache_done(&r, id).await, ("cached".into(), None), "{id}");
    }
    assert!(
        ids.iter()
            .all(|id| !folder.join(id).join("REQUESTED").exists())
    );
    let _ = std::fs::remove_dir_all(&folder);
}

#[tokio::test]
async fn caching_needs_a_folder_a_done_job_and_a_job_id() {
    let r = rig("cache-refused").await;
    cache_job(&r, "OPRA-K3", DBN_BYTES_SHA256);
    let (code, _) = cache(
        AxState(r.jobs.clone()),
        UrlPath("OPRA-K3".into()),
        Query(OriginQuery::default()),
    )
    .await;
    assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
    let dir = std::env::temp_dir().join(format!("tinytrader-jobs-cache-r-{}", std::process::id()));
    let r = rig_with("cache-refused-2", move |j| j.cache_dir = Some(dir)).await;
    r.fake.add(job(
        "OPRA-K4",
        json!("SPY.OPT"),
        "parent",
        whole_day(),
        "processing",
    ));
    let (code, _) = cache(
        AxState(r.jobs.clone()),
        UrlPath("OPRA-K4".into()),
        Query(OriginQuery::default()),
    )
    .await;
    assert_eq!(code, StatusCode::CONFLICT);
    let (code, _) = cache(
        AxState(r.jobs.clone()),
        UrlPath("../etc".into()),
        Query(OriginQuery::default()),
    )
    .await;
    assert_eq!(code, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn a_free_purchase_may_ignore_what_is_held_if_asked() {
    let r = rig("free-ignore").await;
    // A store that can't answer at all, and a day held: not asked, refused.
    lock(&r.store).catalog = (503, json!({ "error": "rescanning" }));
    assert_eq!(r.price(json!({})).await.0, StatusCode::SERVICE_UNAVAILABLE);
    // Asked, and free: the whole day is priced and submitted, unasked.
    let (code, v) = r.price(json!({ "free_ignore_held": true })).await;
    assert_eq!(code, StatusCode::OK, "{v}");
    assert_eq!(v["cost_usd"], 0.0, "{v}");
    assert_eq!(v["requests"][0]["kind"], "whole", "{v}");
    let (code, v) = r.submit(v["quote"].as_str().unwrap(), None).await;
    assert_eq!(code, StatusCode::OK, "{v}");
    assert_eq!(r.fake.submits(), 1);
}

#[tokio::test]
async fn a_paid_purchase_never_ignores_what_is_held() {
    let r = rig("paid-ignore").await;
    lock(&r.fake.s).price = 2.5;
    let (code, v) = r.price(json!({ "free_ignore_held": true })).await;
    assert_eq!(code, StatusCode::CONFLICT, "{v}");
    assert!(
        v["error"]
            .as_str()
            .unwrap_or_default()
            .contains("only a free purchase"),
        "{v}"
    );
    assert_eq!(r.fake.submits(), 0);
}

#[tokio::test]
async fn a_free_purchase_ignoring_held_still_skips_a_whole_job_on_databentos_list() {
    // A whole-chain job of the day on Databento's list: nothing to buy.
    let r = rig("free-ignore-listed").await;
    lock(&r.store).catalog = (503, json!({ "error": "rescanning" }));
    r.fake.add(job(
        "OPRA-W1",
        json!("SPY.OPT"),
        "parent",
        whole_day(),
        "done",
    ));
    let (code, v) = r.price(json!({ "free_ignore_held": true })).await;
    assert_eq!(code, StatusCode::OK, "{v}");
    assert!(v["requests"].as_array().is_none_or(Vec::is_empty), "{v}");
    assert!(v.to_string().contains("OPRA-W1"), "{v}");
    // A job of some contracts only doesn't count: the day is bought whole.
    let r = rig("free-ignore-contracts").await;
    lock(&r.store).catalog = (503, json!({ "error": "rescanning" }));
    r.fake
        .add(job("OPRA-C9", json!(C1), "raw_symbol", whole_day(), "done"));
    let (code, v) = r.price(json!({ "free_ignore_held": true })).await;
    assert_eq!(code, StatusCode::OK, "{v}");
    assert_eq!(v["requests"][0]["kind"], "whole", "{v}");
}

#[tokio::test]
async fn a_price_only_quote_prices_without_the_store_and_cant_be_submitted() {
    let r = rig("price-only").await;
    // A store that can't answer anything, and a paid price.
    {
        let mut s = lock(&r.store);
        s.catalog = (503, json!({ "error": "rescanning" }));
        s.coverage = (503, json!({ "error": "rescanning" }));
    }
    lock(&r.fake.s).price = 2.5;
    let (code, v) = r.price(json!({ "price_only": true })).await;
    assert_eq!(code, StatusCode::OK, "{v}");
    assert_eq!(v["cost_usd"], 2.5, "{v}");
    // Marked as an estimate, with nothing to approve.
    assert_eq!(
        (v["price_only"].as_bool(), v["needs_approval"].as_bool()),
        (Some(true), Some(false)),
        "{v}"
    );
    assert_eq!(v["requests"][0]["kind"], "whole", "{v}");
    assert!(v.to_string().contains("price only"), "{v}");
    // Never submitted, approved or not.
    let q = v["quote"].as_str().unwrap();
    let (code, v) = r
        .submit_as(q, None, Some(APPROVE), Some("test"), Some(2.5))
        .await;
    assert_eq!(code, StatusCode::CONFLICT, "{v}");
    assert!(
        v["error"]
            .as_str()
            .unwrap_or_default()
            .contains("price-only"),
        "{v}"
    );
    assert_eq!(r.fake.submits(), 0);
}

#[tokio::test]
async fn price_only_takes_no_rule() {
    let r = rig("price-only-rule").await;
    let (code, v) = r
        .price(json!({ "price_only": true, "rule": { "dte_max": 21 } }))
        .await;
    assert_eq!(code, StatusCode::BAD_REQUEST, "{v}");
}

#[tokio::test]
async fn a_free_price_only_quote_cant_be_submitted_either() {
    let r = rig("price-only-free").await;
    let (code, v) = r.price(json!({ "price_only": true })).await;
    assert_eq!(code, StatusCode::OK, "{v}");
    assert_eq!(v["cost_usd"], 0.0, "{v}");
    let (code, v) = r.submit(v["quote"].as_str().unwrap(), None).await;
    assert_eq!(code, StatusCode::CONFLICT, "{v}");
    assert_eq!(r.fake.submits(), 0);
}
