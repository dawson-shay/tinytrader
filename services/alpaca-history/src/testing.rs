//! A fake Alpaca and a fake store, for tests: no network beyond loopback.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post, put};
use sha2::{Digest, Sha256};

use crate::fetch::{Alpaca, Pace};

pub const DAY: &str = "2026-10-01";

/// What a lane serves: `pages` pages of `rows` rows.
#[derive(Clone, Copy)]
pub struct Shape {
    pub pages: usize,
    pub rows: usize,
    /// Rows on equal times: two quotes a microsecond, and every other trade on
    /// a quote's time.
    pub ties: bool,
    /// The row (by index in its lane) whose conditions can't be held: a
    /// quote's `["RR"]` and no tape, a trade's five codes.
    pub bad: Option<usize>,
}

impl Shape {
    pub const fn new(pages: usize, rows: usize) -> Self {
        Self {
            pages,
            rows,
            ties: false,
            bad: None,
        }
    }
}

/// A request the fake answered: lane, page, status, day.
pub type Seen = (String, usize, u16, String);

#[derive(Default)]
struct Inner {
    shapes: HashMap<String, Shape>,
    /// Statuses to answer for (lane, page) before answering it.
    faults: HashMap<(String, usize), VecDeque<u16>>,
    /// (lane, page) never answered.
    hang: Option<(String, usize)>,
    log: Vec<Seen>,
    sessions: Vec<String>,
}

/// A fake Alpaca: SPY and QQQ (and any symbol) trades and quotes on any
/// session day, paged with tokens that need percent-encoding.
#[derive(Clone)]
pub struct FakeAlpaca {
    pub url: String,
    inner: Arc<Mutex<Inner>>,
    in_flight: Arc<AtomicUsize>,
    pub most_in_flight: Arc<AtomicUsize>,
    delay_ms: u64,
}

/// A page token with characters a URL must escape.
fn token(page: usize) -> String {
    format!("p+{page}/x==")
}
fn page_of(t: &str) -> Option<usize> {
    t.strip_prefix("p+")?.strip_suffix("/x==")?.parse().ok()
}

fn ts_text(day: &str, k: usize) -> String {
    // 13:30:00 plus k microseconds.
    let us = k as u64;
    let secs = 13 * 3600 + 30 * 60 + us / 1_000_000;
    format!(
        "{day}T{:02}:{:02}:{:02}.{:06}Z",
        secs / 3600,
        secs / 60 % 60,
        secs % 60,
        us % 1_000_000
    )
}

/// The kth quote's conditions: alternating one and two codes.
pub fn quote_codes(k: usize) -> &'static [u8] {
    if k.is_multiple_of(2) { b"R" } else { b"RY" }
}

/// Page `page` of a lane, as the fake serves it.
pub fn body(lane: &str, day: &str, page: usize, shape: Shape) -> String {
    let mut rows = Vec::with_capacity(shape.rows);
    for i in 0..shape.rows {
        let k = page * shape.rows + i;
        // Trades and quotes interleave: quotes at even microseconds, trades at
        // odd ones; with ties, quotes in pairs on one time and every other
        // trade on that time too.
        let (qt, tt) = if shape.ties {
            (k / 2 * 2, k / 2 * 2 + k % 2)
        } else {
            (2 * k, 2 * k + 1)
        };
        let bad = shape.bad == Some(k);
        rows.push(if lane == "trades" {
            let c = if bad { r#"["@","F","I","T","Z"]"# } else { r#"["@","I"]"# };
            format!(r#"{{"t":"{}","x":"V","p":774.51,"s":100,"i":{k},"c":{c},"z":"B"}}"#, ts_text(day, tt))
        } else {
            let c: Vec<String> = quote_codes(k).iter().map(|b| format!("\"{}\"", *b as char)).collect();
            let (c, z) = if bad { (r#""RR""#.to_owned(), String::new()) } else { (c.join(","), r#","z":"B""#.to_owned()) };
            // A bid that moves each quote, so which book a trade carries shows.
            format!(
                r#"{{"t":"{}","bp":{}.{:02},"bs":2,"bx":"Q","ap":900.5,"as":3,"ax":"P","c":[{c}]{z}}}"#,
                ts_text(day, qt),
                700 + k / 100,
                k % 100
            )
        });
    }
    let next = if page + 1 < shape.pages {
        format!("\"{}\"", token(page + 1))
    } else {
        "null".into()
    };
    format!(
        r#"{{"{lane}":[{}],"next_page_token":{next}}}"#,
        rows.join(",")
    )
}

impl FakeAlpaca {
    pub async fn start(trades: Shape, quotes: Shape) -> Self {
        Self::start_with(trades, quotes, 0).await
    }

    /// One that takes `delay_ms` to answer each page.
    pub async fn start_with(trades: Shape, quotes: Shape, delay_ms: u64) -> Self {
        let inner = Inner {
            shapes: HashMap::from([("trades".to_owned(), trades), ("quotes".to_owned(), quotes)]),
            sessions: vec![DAY.to_owned()],
            ..Inner::default()
        };
        let mut fake = Self {
            url: String::new(),
            inner: Arc::new(Mutex::new(inner)),
            in_flight: Arc::default(),
            most_in_flight: Arc::default(),
            delay_ms,
        };
        let app = axum::Router::new()
            .route("/v2/stocks/{symbol}/{lane}", get(answer))
            .route(
                "/v2/calendar",
                get(|State(f): State<FakeAlpaca>| async move {
                    let s = f.inner.lock().unwrap().sessions.clone();
                    axum::Json(s.iter().map(|d| serde_json::json!({ "date": d, "open": "09:30", "close": "16:00" })).collect::<Vec<_>>())
                }),
            )
            .with_state(fake.clone());
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        fake.url = format!("http://{}", l.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
        fake
    }

    /// Answer `statuses` for this page before serving it.
    pub fn fault(&self, lane: &str, page: usize, statuses: &[u16]) {
        self.inner
            .lock()
            .unwrap()
            .faults
            .entry((lane.to_owned(), page))
            .or_default()
            .extend(statuses);
    }
    /// Never answer this page.
    pub fn hang(&self, lane: &str, page: usize) {
        self.inner.lock().unwrap().hang = Some((lane.to_owned(), page));
    }
    pub fn sessions(&self, days: &[&str]) {
        self.inner.lock().unwrap().sessions = days.iter().map(|d| (*d).to_owned()).collect();
    }
    pub fn log(&self) -> Vec<Seen> {
        self.inner.lock().unwrap().log.clone()
    }
    /// Times a page was served (200).
    pub fn served(&self, lane: &str, page: usize) -> usize {
        self.log()
            .iter()
            .filter(|(l, p, s, _)| l == lane && *p == page && *s == 200)
            .count()
    }

    pub fn client(&self) -> Alpaca {
        Alpaca {
            http: reqwest::Client::new(),
            data: self.url.clone(),
            key_id: "k".into(),
            secret: "s".into(),
            pace: Pace::new(100_000, 6),
            hold: std::sync::atomic::AtomicBool::new(false),
            events: tinytrader_observe::Events::default(),
        }
    }
}

async fn answer(
    State(f): State<FakeAlpaca>,
    Path((_symbol, lane)): Path<(String, String)>,
    Query(q): Query<HashMap<String, String>>,
) -> (StatusCode, String) {
    let page = match q.get("page_token") {
        None => 0,
        // A token that didn't arrive as sent is refused, as Alpaca would.
        Some(t) => match page_of(t) {
            Some(p) => p,
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    format!(r#"{{"message":"invalid page token {t:?}"}}"#),
                );
            }
        },
    };
    let day = q
        .get("start")
        .and_then(|s| s.get(..10))
        .unwrap_or(DAY)
        .to_owned();
    let (fault, hang, shape) = {
        let mut i = f.inner.lock().unwrap();
        let fault = i
            .faults
            .get_mut(&(lane.clone(), page))
            .and_then(VecDeque::pop_front);
        let hang = i.hang.as_ref() == Some(&(lane.clone(), page));
        (fault, hang, i.shapes.get(&lane).copied())
    };
    if hang {
        std::future::pending::<()>().await;
    }
    let n = f.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
    f.most_in_flight.fetch_max(n, Ordering::SeqCst);
    if f.delay_ms > 0 {
        tokio::time::sleep(std::time::Duration::from_millis(f.delay_ms)).await;
    }
    f.in_flight.fetch_sub(1, Ordering::SeqCst);
    let status = fault.unwrap_or(200);
    f.inner
        .lock()
        .unwrap()
        .log
        .push((lane.clone(), page, status, day.clone()));
    if status != 200 {
        return (
            StatusCode::from_u16(status).unwrap(),
            r#"{"message":"scripted"}"#.into(),
        );
    }
    let Some(shape) = shape else {
        return (StatusCode::NOT_FOUND, "{}".into());
    };
    (StatusCode::OK, body(&lane, &day, page, shape))
}

#[derive(Default)]
struct StoreInner {
    filed: HashSet<String>,
    /// Batch → file name → (size, SHA-256) received.
    received: HashMap<String, HashMap<String, (u64, String)>>,
    /// Statuses to answer the next incorporations with.
    refuse: VecDeque<u16>,
}

/// A fake store: takes uploads (checking each one's size and SHA-256 as it
/// streams), files batches, and lists what it holds.
#[derive(Clone)]
pub struct FakeStore {
    pub url: String,
    inner: Arc<Mutex<StoreInner>>,
}

impl FakeStore {
    pub async fn start() -> Self {
        let mut s = Self {
            url: String::new(),
            inner: Arc::default(),
        };
        let app = axum::Router::new()
            .route(
                "/batches/alpaca",
                get(|State(s): State<FakeStore>| async move {
                    let filed = s.inner.lock().unwrap().filed.clone();
                    let m: serde_json::Map<String, serde_json::Value> = filed
                        .into_iter()
                        .map(|b| (b, serde_json::json!({ "how": "filed" })))
                        .collect();
                    axum::Json(serde_json::json!({ "batches": m }))
                }),
            )
            .route(
                "/incoming/alpaca/{id}/{name}",
                put(
                    |State(s): State<FakeStore>,
                     Path((id, name)): Path<(String, String)>,
                     Query(q): Query<HashMap<String, String>>,
                     body: axum::body::Body| async move {
                        use futures_util::StreamExt;
                        let mut stream = body.into_data_stream();
                        let (mut h, mut n) = (Sha256::new(), 0u64);
                        while let Some(chunk) = stream.next().await {
                            let chunk = chunk.unwrap();
                            n += chunk.len() as u64;
                            h.update(&chunk);
                        }
                        let sha = format!("{:x}", h.finalize());
                        if q.get("size") != Some(&n.to_string()) || q.get("sha256") != Some(&sha) {
                            return (
                                StatusCode::UNPROCESSABLE_ENTITY,
                                "size or sha256 differ".to_owned(),
                            );
                        }
                        s.inner
                            .lock()
                            .unwrap()
                            .received
                            .entry(id)
                            .or_default()
                            .insert(name, (n, sha));
                        (StatusCode::OK, "{}".to_owned())
                    },
                ),
            )
            .route(
                "/incorporate/alpaca/{id}",
                post(
                    |State(s): State<FakeStore>, Path(id): Path<String>| async move {
                        let mut i = s.inner.lock().unwrap();
                        if let Some(code) = i.refuse.pop_front() {
                            return (
                                StatusCode::from_u16(code).unwrap(),
                                r#"{"error":"scripted"}"#.to_owned(),
                            );
                        }
                        i.filed.insert(id);
                        (StatusCode::OK, r#"{"filed":true}"#.to_owned())
                    },
                ),
            )
            .with_state(s.clone());
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        s.url = format!("http://{}", l.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
        s
    }

    pub fn hold(&self, batches: &[String]) {
        self.inner
            .lock()
            .unwrap()
            .filed
            .extend(batches.iter().cloned());
    }
    pub fn filed(&self) -> HashSet<String> {
        self.inner.lock().unwrap().filed.clone()
    }
    pub fn received(&self, batch: &str) -> HashMap<String, (u64, String)> {
        self.inner
            .lock()
            .unwrap()
            .received
            .get(batch)
            .cloned()
            .unwrap_or_default()
    }
}

/// A fresh scratch folder.
pub fn scratch(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("ah-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}
