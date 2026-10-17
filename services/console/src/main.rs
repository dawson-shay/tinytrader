//! tinytrader console backend.
//!
//! Serves the console page and a small read-only API:
//!
//! - `GET /api/topology`: the deployment as drawn: hosts, services, stores,
//!   edges and panels.
//! - `GET /api/state`: the latest snapshot of every service: container state
//!   from the Docker Engine API, plus each service's `/health`, `/metrics`
//!   and optional `/recent` endpoints. With `?since=G&epoch=E` (the last
//!   answer's), only the service fields changed since (see `delta`).
//!
//! A background task refreshes the snapshot every two seconds, so any number
//! of viewers cost one round of polling. The console never writes to Docker
//! or to any service; it should reach Docker through a read-only proxy.
//!
//! **Discovery.** With Docker configured, every container in the project
//! appears in the graph. A container labelled `tinytrader.observe=<port>` is
//! asked for its `/describe` document, which supplies its role, upstream
//! sources, stores, event and series endpoints and suggested panels. Other
//! labels: `tinytrader.role` (a description for containers without
//! `/describe`), `tinytrader.label` and `tinytrader.host`. Without Docker, or for
//! services Docker doesn't run, a topology entry such as
//! `{ "id": "ticker", "observe": "http://127.0.0.1:9105" }` is described and
//! polled the same way.
//!
//! **Overrides.** The topology file is optional. When present, it is layered
//! over what was discovered: `name`, `focus` and `hosts` replace; `services`,
//! `external` and `stores` entries merge by `id`; `edges` are added; `panels`
//! for a service replace that service's suggested panels; `hide` lists ids to
//! leave out. Without Docker, the file alone describes the deployment.
//!
//! Configuration (environment):
//! - `CONSOLE_LISTEN` (or `TINYTRADER_LISTEN`): address to serve on (default `127.0.0.1:8080`)
//! - `CONSOLE_TOPOLOGY`: path to the optional topology file (default `topology.json`)
//! - `CONSOLE_DOCKER`: Docker Engine API base URL, e.g. `http://docker-proxy:2375` (optional)
//! - `CONSOLE_COMPOSE_PROJECT`: only show containers from this Compose project (optional)
//! - `CONSOLE_COLLECTIONS`: document collections shown in the sidebar, e.g.
//!   `Docs=/srv/docs;Research=/srv/research` (optional; see `collections.rs`)
//! - `CONSOLE_NOTIFY`: a URL to POST a notice to when a service goes down or
//!   comes back (ntfy-style: the body is the message, with `Title`, `Priority`
//!   and `Tags` headers), or `CONSOLE_NOTIFY_FILE` naming a file holding it.
//!   Optional. Down: a container exited, dead, restarting or gone, or a
//!   service reporting `failed` (`notify.rs`).
//! - `CONSOLE_STORE`: the store, e.g. `http://store:9120`, so
//!   the Storage widget can approve a new drive (decision 0007). Only that
//!   one call is passed through. `CONSOLE_STORE_TOKEN` (or `_FILE`): the
//!   store's write token, when the store requires one, sent with it.
//! - `CONSOLE_JOBS`: the Databento jobs service, e.g. `http://databento-jobs:9121`
//!   (optional). The one exception to read-only: `/api/jobs/...` is passed to
//!   it, so the Data page can price, submit and download jobs. The jobs
//!   service itself insists on an exact confirmation before any charge.
//! - `CONSOLE_QUEUE`: the job queue, e.g. `http://queue:9123` (optional;
//!   decision 0012). `/api/queue/...` is passed to it, so the Queue widget
//!   can add, edit and cancel jobs; the queue runs only its templates.
//! - `CONSOLE_GATEWAYS`: order gateways by account, e.g.
//!   `paper=gateway-paper:7701;live=gateway-live:7710`, and
//!   `CONSOLE_GATEWAY_TOKEN` (or `_FILE`), the operator's admin token for them
//!   (optional). `/api/accounts/{account}/flatten` and `.../resume` act through
//!   the account's gateway; an account without one (live) can't be flattened.
//!   The console says `hello` as `CONSOLE_GATEWAY_CLIENT` (default `ops`).
//!   With `CONSOLE_TRADE_TOKEN` (or `_FILE`), the console's trade client
//!   (`CONSOLE_TRADE_CLIENT`, default `console`; order ids start with
//!   `CONSOLE_TRADE_PREFIX`, default `tt-`) sends the Spread builder's orders: `GET /api/orders` (each
//!   account's gateway state and the console's open orders), `POST
//!   /api/orders/{account}` (`{order, confirm: account}`) and `POST
//!   /api/orders/{account}/{client_order_id}/cancel`.
//! - `CONSOLE_PAGE`: serve the page from this file, and its modules and
//!   libraries from beside it, read on each request, instead of the copies
//!   built in (optional; lets the page change without a rebuild).
//! - `CONSOLE_PROFILES`: where profile-mode recordings are kept
//!   (`/api/profiles`); by default `profiles/` beside `CONSOLE_REMINDERS`.
//! - `CONSOLE_PREFS`: a JSON file of the page's settings that follow the user
//!   between devices (`/api/prefs`); by default `prefs.json` beside
//!   `CONSOLE_REMINDERS`. `CONSOLE_PREFS_DEFAULTS`: a read-only JSON file of
//!   the same keys, used where the settings have none, such as curated views
//!   kept in a deployment's repository and written by
//!   `scripts/prefs-promote` (optional).
//! - `CONSOLE_REMINDERS`: a JSON file for the roadmap's reminders, which the
//!   console edits (`/api/reminders`); it may be a file in a git checkout,
//!   and is read again when it changes on disk. `CONSOLE_REMINDERS_SEED`: a
//!   Markdown roadmap to start it from when there is no file (optional).
//! - `CONSOLE_JOURNAL`: the trading journal's JSON file (`/api/journal`), by
//!   default `journal.json` beside `CONSOLE_REMINDERS`. Writes are refused
//!   while the file can't be read (it is left as it is).

mod collections;
mod delta;
mod gateways;
mod journal;
mod notify;
mod prefs;
mod profiles;
mod reminders;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axum::Router;
use axum::extract::State as AxumState;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use serde_json::json;
use tinytrader_fleet::{EventsError, Poller, PollerConfig};
use tinytrader_observe::{Observer, State};

/// Every file under `web/`, built in.
mod web {
    include!(concat!(env!("OUT_DIR"), "/web.rs"));
}

struct App {
    poller: Poller,
    /// The jobs service's base URL, and a client to reach it.
    jobs: Option<(String, reqwest::Client)>,
    /// The job queue's base URL, and a client to reach it.
    queue: Option<(String, reqwest::Client)>,
    /// The store's base URL, for approving drives.
    store: Option<String>,
    /// The store's write token, if it requires one.
    store_token: Option<String>,
    /// The roadmap's reminders, if configured.
    /// Settings that follow the user between devices (`/api/prefs`).
    prefs: Option<prefs::Prefs>,
    reminders: Option<reminders::Reminders>,
    /// The trading journal (`/api/journal`), if its file can be read.
    journal: Option<journal::Journal>,
    /// Profile-mode recordings uploaded by pages, if a directory is configured.
    profiles: Option<profiles::Profiles>,
    /// Order gateways by account, for Flatten and Resume.
    gateways: gateways::Gateways,
    polls: AtomicU64,
    /// Changes-only snapshots for pages that poll.
    delta: delta::Delta,
    collections: Vec<collections::Collection>,
}

#[tokio::main]
async fn main() {
    let listen = tinytrader_observe::listen_address("CONSOLE_LISTEN", "127.0.0.1:8080");
    let store_token = match tinytrader_observe::optional_secret("CONSOLE_STORE_TOKEN") {
        Ok(t) => t,
        Err(e) => {
            eprintln!("console: {e}");
            std::process::exit(2);
        }
    };
    let config = PollerConfig {
        topology: env("CONSOLE_TOPOLOGY", "topology.json"),
        docker: std::env::var("CONSOLE_DOCKER")
            .ok()
            .filter(|s| !s.is_empty()),
        project: std::env::var("CONSOLE_COMPOSE_PROJECT")
            .ok()
            .filter(|s| !s.is_empty()),
    };
    // The Docker proxy, if any, is this console's upstream.
    let proxy = config.docker.as_deref().and_then(|u| {
        u.split("//")
            .nth(1)?
            .split([':', '/'])
            .next()
            .map(str::to_owned)
    });
    let app = Arc::new(App {
        poller: Poller::new(config),
        polls: AtomicU64::new(0),
        delta: delta::Delta::new(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_millis() as u64),
        ),
        collections: collections::parse(&env("CONSOLE_COLLECTIONS", "")),
        store: std::env::var("CONSOLE_STORE")
            .ok()
            .filter(|s| !s.is_empty())
            .map(|s| s.trim_end_matches('/').to_owned()),
        store_token,
        jobs: std::env::var("CONSOLE_JOBS")
            .ok()
            .filter(|s| !s.is_empty())
            .map(|u| (u.trim_end_matches('/').to_owned(), reqwest::Client::new())),
        queue: std::env::var("CONSOLE_QUEUE")
            .ok()
            .filter(|s| !s.is_empty())
            .map(|u| (u.trim_end_matches('/').to_owned(), reqwest::Client::new())),
        gateways: gateways::Gateways::from_env(),
        // Beside the reminders unless named.
        profiles: std::env::var("CONSOLE_PROFILES")
            .ok()
            .filter(|s| !s.is_empty())
            .map(std::path::PathBuf::from)
            .or_else(|| {
                std::env::var("CONSOLE_REMINDERS")
                    .ok()
                    .filter(|s| !s.is_empty())
                    .and_then(|r| {
                        std::path::Path::new(&r)
                            .parent()
                            .map(|d| d.join("profiles"))
                    })
            })
            .map(profiles::Profiles::new),
        // Beside the reminders unless named.
        prefs: std::env::var("CONSOLE_PREFS")
            .ok()
            .filter(|s| !s.is_empty())
            .map(std::path::PathBuf::from)
            .or_else(|| {
                std::env::var("CONSOLE_REMINDERS")
                    .ok()
                    .filter(|s| !s.is_empty())
                    .and_then(|r| {
                        std::path::Path::new(&r)
                            .parent()
                            .map(|d| d.join("prefs.json"))
                    })
            })
            .map(|p| {
                let defaults = std::env::var("CONSOLE_PREFS_DEFAULTS")
                    .ok()
                    .filter(|s| !s.is_empty());
                prefs::Prefs::open(p, defaults.as_deref().map(std::path::Path::new))
            }),
        journal: std::env::var("CONSOLE_JOURNAL")
            .ok()
            .filter(|s| !s.is_empty())
            .map(std::path::PathBuf::from)
            .or_else(|| {
                journal::default_path(
                    std::env::var("CONSOLE_REMINDERS")
                        .ok()
                        .filter(|s| !s.is_empty())
                        .map(std::path::PathBuf::from)
                        .as_deref(),
                )
            })
            .and_then(|p| {
                journal::Journal::open(p)
                    .map_err(|e| eprintln!("console: journal off: {e}"))
                    .ok()
            }),
        reminders: std::env::var("CONSOLE_REMINDERS")
            .ok()
            .filter(|s| !s.is_empty())
            .and_then(|p| {
                let seed = std::env::var("CONSOLE_REMINDERS_SEED")
                    .ok()
                    .filter(|s| !s.is_empty());
                // An unreadable file is left for the operator to fix, and
                // reminders are off until then.
                reminders::Reminders::open(
                    std::path::PathBuf::from(p),
                    seed.as_deref().map(std::path::Path::new),
                )
                .map_err(|e| eprintln!("console: reminders off: {e}"))
                .ok()
            }),
    });

    let observer = Observer::new("console", env!("CARGO_PKG_VERSION"));
    observer.describe(json!({
        "role": "The operator console: polls every service and Docker every two seconds.",
        "schedule": [{ "name": "Polling every service and Docker", "kind": "interval", "every_seconds": 2, "when": "every 2 s" }],
        "upstream": proxy.map_or_else(Vec::new, |p| vec![json!({ "service": p, "kind": "read", "via": "container state" })]),
    }));
    {
        let a = app.clone();
        observer.set_metrics(move |w| {
            w.counter(
                "tinytrader_console_polls_total",
                "Snapshot refreshes since start.",
                &[],
                a.polls.load(Ordering::Relaxed) as f64,
            );
        });
    }

    tokio::spawn(poll_forever(app.clone(), observer.clone()));

    let router = Router::new()
        .route("/", get(page))
        .route("/docs", get(page))
        .route("/docs/", get(page))
        .route("/vendor/{file}", get(file))
        .route("/lib/{file}", get(file))
        .route("/widgets/{file}", get(file))
        .route("/api/topology", get(topology))
        .route("/api/state", get(state))
        .route("/api/events/{service}", get(events))
        .route("/api/bars/{service}", get(bars))
        .route("/api/stream/{service}/{stream}", get(stream))
        .route("/api/collections", get(list_collections))
        .route("/api/collections/{id}/{index}", get(collection_doc))
        .route("/api/jobs/{*rest}", get(jobs_proxy).post(jobs_proxy))
        .route(
            "/api/queue/{*rest}",
            get(queue_proxy).post(queue_proxy).patch(queue_proxy),
        )
        .route(
            "/api/store/volumes/approve",
            axum::routing::post(store_approve),
        )
        .route("/api/reminders", get(reminders_get).post(reminders_post))
        .route("/api/prefs", get(prefs_get))
        .route("/api/journal", get(journal_get).post(journal_post))
        .route(
            "/api/journal/{id}",
            axum::routing::put(journal_put).delete(journal_delete),
        )
        .route("/api/prefs/{key}", axum::routing::put(prefs_put))
        .route(
            "/api/profiles",
            get(profiles_list)
                .post(profiles_post)
                .layer(axum::extract::DefaultBodyLimit::max(
                    profiles::MAX_BYTES + 1024,
                )),
        )
        .route("/api/profiles/{id}", get(profiles_get))
        .route("/api/accounts/gateways", get(gateways_list))
        .route("/api/orders", get(orders_list))
        .route("/api/orders/{account}", axum::routing::post(order_submit))
        .route(
            "/api/orders/{account}/{id}/cancel",
            axum::routing::post(order_cancel),
        )
        .route(
            "/api/accounts/{account}/flatten",
            axum::routing::post(account_flatten),
        )
        .route(
            "/api/accounts/{account}/resume",
            axum::routing::post(account_resume),
        )
        // The console's writes (jobs, the queue, drive approval, reminders, flatten) can't be
        // forged by another page open in the browser.
        .layer(axum::middleware::from_fn(
            tinytrader_observe::guard::json_writes,
        ))
        .with_state(app)
        .merge(observer.router());
    let listener = match tokio::net::TcpListener::bind(&listen).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("console: cannot listen on {listen}: {e}");
            std::process::exit(2);
        }
    };
    eprintln!("console: serving http://{listen}");
    if let Err(e) = axum::serve(listener, router)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
    {
        eprintln!("console: server stopped: {e}");
    }
}

async fn poll_forever(app: Arc<App>, observer: Observer) {
    let mut tick = tokio::time::interval(Duration::from_secs(2));
    // Push notifications when a service goes down or comes back (CONSOLE_NOTIFY).
    let notify = match tinytrader_observe::secret("CONSOLE_NOTIFY") {
        Ok(u) if !u.is_empty() => Some(u),
        Ok(_) => None,
        // Unset is fine (no notices); a named file that can't be read is said.
        Err(e) => {
            if std::env::var_os("CONSOLE_NOTIFY_FILE").is_some() {
                eprintln!("console: {e}; no notices");
            }
            None
        }
    };
    let (mut watch, http) = (notify::Watch::default(), reqwest::Client::new());
    loop {
        tick.tick().await;
        let problem = app.poller.poll().await;
        app.polls.fetch_add(1, Ordering::Relaxed);
        if let Some(url) = &notify {
            for n in watch.observe(&app.poller.topology(), &app.poller.snapshot()) {
                if let Err(e) = notify::send(&http, url, &n).await {
                    observer.record_error(&e);
                }
            }
        }
        match problem {
            None => observer.set_state(State::Ready, "polling"),
            Some(p) => {
                observer.record_error(&p);
                observer.set_state(State::Degraded, &p);
            }
        }
    }
}

/// A file of the page, from the directory `CONSOLE_PAGE` names when set
/// (read on each request, so the page changes without a rebuild), or else
/// as built in. One source either way: neither falls back to the other.
fn web_file(path: &str) -> Option<std::borrow::Cow<'static, [u8]>> {
    match std::env::var("CONSOLE_PAGE") {
        Ok(page) => {
            let ok = path.split('/').all(|s| {
                !s.is_empty()
                    && !s.starts_with('.')
                    && s.chars()
                        .all(|c| c.is_ascii_alphanumeric() || "._-".contains(c))
            });
            let dir = std::path::Path::new(&page).parent()?;
            let file = if path == "index.html" {
                std::path::PathBuf::from(&page)
            } else {
                dir.join(path)
            };
            ok.then(|| std::fs::read(file).ok())
                .flatten()
                .map(std::borrow::Cow::Owned)
        }
        Err(_) => web::FILES
            .iter()
            .find(|(n, _)| *n == path)
            .map(|(_, b)| std::borrow::Cow::Borrowed(*b)),
    }
}

async fn file(uri: axum::http::Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    let Some(body) = web_file(path) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let kind = match path.rsplit('.').next() {
        Some("js") => "text/javascript; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("svg") => "image/svg+xml",
        Some("json") => "application/json",
        Some("woff2") => "font/woff2",
        _ => "application/octet-stream",
    };
    // Libraries carry their versions in their names; the page's own modules change.
    let cache = if path.starts_with("vendor/") {
        "public, max-age=86400"
    } else {
        "no-cache"
    };
    (
        [(header::CONTENT_TYPE, kind), (header::CACHE_CONTROL, cache)],
        body,
    )
        .into_response()
}

async fn page() -> Response {
    match web_file("index.html") {
        Some(body) => (
            [
                (header::CONTENT_TYPE, "text/html; charset=utf-8"),
                (header::CACHE_CONTROL, "no-cache"),
            ],
            body,
        )
            .into_response(),
        None => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "the console page is missing (CONSOLE_PAGE)",
        )
            .into_response(),
    }
}

async fn topology(AxumState(app): AxumState<Arc<App>>) -> Response {
    no_store(axum::Json(app.poller.topology_json()))
}

async fn state(
    AxumState(app): AxumState<Arc<App>>,
    axum::extract::Query(q): axum::extract::Query<HashMap<String, String>>,
) -> Response {
    let since = q.get("since").and_then(|v| v.parse().ok());
    let epoch = q.get("epoch").and_then(|v| v.parse().ok());
    no_store(axum::Json(app.delta.answer(
        app.poller.snapshot_json(),
        since,
        epoch,
    )))
}

/// A fleet lookup's failure as the API's error document: `404` for a stream
/// the service doesn't declare, `502` for one that doesn't answer.
fn fleet_error(e: &EventsError) -> Response {
    let code = match e {
        EventsError::Unreachable(_) => StatusCode::BAD_GATEWAY,
        EventsError::NotDeclared(_) | EventsError::NotServed(..) => StatusCode::NOT_FOUND,
    };
    (code, axum::Json(json!({ "error": e.to_string() }))).into_response()
}

/// Live tail of a service's events, forwarded from the service.
async fn events(
    AxumState(app): AxumState<Arc<App>>,
    axum::extract::Path(service): axum::extract::Path<String>,
    axum::extract::Query(q): axum::extract::Query<HashMap<String, String>>,
) -> Response {
    let after = q.get("after").and_then(|v| v.parse().ok()).unwrap_or(0);
    let limit = q.get("limit").and_then(|v| v.parse().ok()).unwrap_or(100);
    match app.poller.events_json(&service, after, limit).await {
        Ok(v) => no_store(axum::Json(v)),
        Err(e) => fleet_error(&e),
    }
}

/// One symbol's one-minute bars from a relay (`symbol=SPY&days=2`).
async fn bars(
    AxumState(app): AxumState<Arc<App>>,
    axum::extract::Path(service): axum::extract::Path<String>,
    axum::extract::RawQuery(query): axum::extract::RawQuery,
) -> Response {
    match app
        .poller
        .bars_json(&service, query.as_deref().unwrap_or(""))
        .await
    {
        Ok((code, v)) => no_store((
            StatusCode::from_u16(code).unwrap_or(StatusCode::BAD_GATEWAY),
            axum::Json(v),
        )),
        Err(e) => fleet_error(&e),
    }
}

/// A service's on-demand data (stock snapshots, option chains, bars).
async fn stream(
    AxumState(app): AxumState<Arc<App>>,
    axum::extract::Path((service, stream)): axum::extract::Path<(String, String)>,
    axum::extract::RawQuery(query): axum::extract::RawQuery,
) -> Response {
    match app
        .poller
        .stream_json(&service, &stream, query.as_deref().unwrap_or(""))
        .await
    {
        Ok((code, v)) => no_store((
            StatusCode::from_u16(code).unwrap_or(StatusCode::BAD_GATEWAY),
            axum::Json(v),
        )),
        Err(e) => fleet_error(&e),
    }
}

async fn list_collections(AxumState(app): AxumState<Arc<App>>) -> Response {
    let list: Vec<serde_json::Value> = app
        .collections
        .iter()
        .map(collections::Collection::index)
        .collect();
    no_store(axum::Json(list))
}

async fn collection_doc(
    AxumState(app): AxumState<Arc<App>>,
    axum::extract::Path((id, index)): axum::extract::Path<(String, usize)>,
) -> Response {
    app.collections
        .iter()
        .find(|c| c.id == id)
        .and_then(|c| c.render(index))
        .map_or_else(
            || (StatusCode::NOT_FOUND, axum::Json(json!({ "error": "no such document" }))).into_response(),
            |d| no_store(axum::Json(d)),
        )
}

async fn prefs_get(AxumState(app): AxumState<Arc<App>>) -> Response {
    match &app.prefs {
        Some(p) => no_store(axum::Json(p.get())),
        None => (StatusCode::NOT_FOUND, "no settings file configured").into_response(),
    }
}

/// Save one setting. `If-Match` names the version the page started from;
/// a stale save gets 409 with the current value, to merge and save again.
async fn prefs_put(
    AxumState(app): AxumState<Arc<App>>,
    axum::extract::Path(key): axum::extract::Path<String>,
    headers: axum::http::HeaderMap,
    axum::Json(value): axum::Json<serde_json::Value>,
) -> Response {
    let Some(p) = &app.prefs else {
        return (StatusCode::NOT_FOUND, "no settings file configured").into_response();
    };
    let based_on = headers
        .get(axum::http::header::IF_MATCH)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.trim_matches('"').to_owned());
    // A page from before versions (an old tab) can't overwrite: reload it.
    let Some(based_on) = based_on else {
        return (
            StatusCode::PRECONDITION_REQUIRED,
            axum::Json(json!({ "error": "save with If-Match (reload the page)" })),
        )
            .into_response();
    };
    match p.put(&key, value, Some(&based_on)) {
        Ok(()) => no_store(axum::Json(json!({ "ok": true }))),
        Err(prefs::Refused::Stale(current)) => (
            StatusCode::CONFLICT,
            axum::Json(json!({ "error": "changed since", "current": current })),
        )
            .into_response(),
        Err(prefs::Refused::Bad(e)) => {
            (StatusCode::BAD_REQUEST, axum::Json(json!({ "error": e }))).into_response()
        }
    }
}

/// The entries whose day is in `from..=to`, newest first.
async fn journal_get(
    AxumState(app): AxumState<Arc<App>>,
    axum::extract::Query(q): axum::extract::Query<HashMap<String, String>>,
) -> Response {
    let Some(j) = &app.journal else {
        return (StatusCode::NOT_FOUND, "no journal configured").into_response();
    };
    let entries = j.list(
        q.get("from").map(String::as_str),
        q.get("to").map(String::as_str),
    );
    no_store(axum::Json(json!({ "entries": entries })))
}

/// Journal writes answer with the entry, or the error.
fn journal_answer(r: Result<journal::Entry, journal::Refused>) -> Response {
    match r {
        Ok(e) => no_store(axum::Json(e)),
        Err(journal::Refused::NotFound) => (
            StatusCode::NOT_FOUND,
            axum::Json(json!({ "error": "no such entry" })),
        )
            .into_response(),
        Err(journal::Refused::Bad(e)) => {
            (StatusCode::BAD_REQUEST, axum::Json(json!({ "error": e }))).into_response()
        }
    }
}

async fn journal_post(
    AxumState(app): AxumState<Arc<App>>,
    axum::Json(body): axum::Json<serde_json::Value>,
) -> Response {
    match &app.journal {
        Some(j) => journal_answer(j.create(&body)),
        None => (StatusCode::NOT_FOUND, "no journal configured").into_response(),
    }
}

async fn journal_put(
    AxumState(app): AxumState<Arc<App>>,
    axum::extract::Path(id): axum::extract::Path<String>,
    axum::Json(body): axum::Json<serde_json::Value>,
) -> Response {
    match &app.journal {
        Some(j) => journal_answer(j.update(&id, &body)),
        None => (StatusCode::NOT_FOUND, "no journal configured").into_response(),
    }
}

async fn journal_delete(
    AxumState(app): AxumState<Arc<App>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Response {
    match &app.journal {
        Some(j) => match j.delete(&id) {
            Ok(()) => no_store(axum::Json(json!({ "ok": true }))),
            Err(e) => journal_answer(Err(e)),
        },
        None => (StatusCode::NOT_FOUND, "no journal configured").into_response(),
    }
}

async fn reminders_get(AxumState(app): AxumState<Arc<App>>) -> Response {
    match &app.reminders {
        Some(r) => no_store(axum::Json(r.get())),
        None => (StatusCode::NOT_FOUND, "no reminders configured").into_response(),
    }
}

async fn reminders_post(
    AxumState(app): AxumState<Arc<App>>,
    axum::Json(change): axum::Json<serde_json::Value>,
) -> Response {
    let Some(r) = &app.reminders else {
        return (StatusCode::NOT_FOUND, "no reminders configured").into_response();
    };
    match r.apply(&change) {
        Ok(doc) => no_store(axum::Json(doc)),
        Err(e) => (StatusCode::BAD_REQUEST, axum::Json(json!({ "error": e }))).into_response(),
    }
}

async fn profiles_list(AxumState(app): AxumState<Arc<App>>) -> Response {
    match &app.profiles {
        Some(p) => no_store(axum::Json(p.list())),
        None => (StatusCode::NOT_FOUND, "no profiles directory configured").into_response(),
    }
}

async fn profiles_post(AxumState(app): AxumState<Arc<App>>, body: axum::body::Bytes) -> Response {
    let Some(p) = &app.profiles else {
        return (StatusCode::NOT_FOUND, "no profiles directory configured").into_response();
    };
    match p.store(&body) {
        Ok(id) => no_store(axum::Json(json!({ "id": id }))),
        Err(e) => (StatusCode::BAD_REQUEST, axum::Json(json!({ "error": e }))).into_response(),
    }
}

async fn profiles_get(
    AxumState(app): AxumState<Arc<App>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Response {
    match app.profiles.as_ref().and_then(|p| p.get(&id)) {
        Some(b) => no_store(([(header::CONTENT_TYPE, "application/json")], b)),
        None => (StatusCode::NOT_FOUND, "no such recording").into_response(),
    }
}

/// Pass a request to the jobs service and its answer back.
async fn jobs_proxy(
    AxumState(app): AxumState<Arc<App>>,
    method: axum::http::Method,
    axum::extract::Path(rest): axum::extract::Path<String>,
    axum::extract::RawQuery(query): axum::extract::RawQuery,
    body: axum::body::Bytes,
) -> Response {
    pass(
        app.jobs.as_ref(),
        "jobs service",
        method,
        &rest,
        query,
        body,
    )
    .await
}

/// Pass a request to the job queue and its answer back.
async fn queue_proxy(
    AxumState(app): AxumState<Arc<App>>,
    method: axum::http::Method,
    axum::extract::Path(rest): axum::extract::Path<String>,
    axum::extract::RawQuery(query): axum::extract::RawQuery,
    body: axum::body::Bytes,
) -> Response {
    pass(app.queue.as_ref(), "job queue", method, &rest, query, body).await
}

async fn pass(
    to: Option<&(String, reqwest::Client)>,
    what: &str,
    method: axum::http::Method,
    rest: &str,
    query: Option<String>,
    body: axum::body::Bytes,
) -> Response {
    let Some((base, http)) = to else {
        return (
            StatusCode::NOT_FOUND,
            axum::Json(json!({ "error": format!("no {what} configured") })),
        )
            .into_response();
    };
    let url = match query {
        Some(q) => format!("{base}/{rest}?{q}"),
        None => format!("{base}/{rest}"),
    };
    let req = match method {
        axum::http::Method::POST | axum::http::Method::PATCH => http
            .request(method, url)
            .header(header::CONTENT_TYPE, "application/json")
            .body(body),
        _ => http.get(url),
    };
    match req.timeout(Duration::from_secs(120)).send().await {
        Ok(r) => {
            let code = StatusCode::from_u16(r.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
            let bytes = r.bytes().await.unwrap_or_default();
            no_store((code, [(header::CONTENT_TYPE, "application/json")], bytes))
        }
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            axum::Json(json!({ "error": format!("{what}: {e}") })),
        )
            .into_response(),
    }
}

/// Approve a drive for the store: the one store call the console passes on.
async fn store_approve(AxumState(app): AxumState<Arc<App>>, body: axum::body::Bytes) -> Response {
    let Some(base) = &app.store else {
        return (
            StatusCode::NOT_FOUND,
            axum::Json(json!({ "error": "no store configured (CONSOLE_STORE)" })),
        )
            .into_response();
    };
    let mut req = reqwest::Client::new().post(format!("{base}/volumes/approve"));
    if let Some(t) = &app.store_token {
        req = req.bearer_auth(t);
    }
    let sent = req
        .header(header::CONTENT_TYPE, "application/json")
        .body(body)
        .timeout(Duration::from_secs(30))
        .send()
        .await;
    match sent {
        Ok(r) => {
            let code = StatusCode::from_u16(r.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
            let bytes = r.bytes().await.unwrap_or_default();
            no_store((code, [(header::CONTENT_TYPE, "application/json")], bytes))
        }
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            axum::Json(json!({ "error": format!("store: {e}") })),
        )
            .into_response(),
    }
}

/// Which accounts have a gateway, and each gateway's state (from its welcome).
async fn gateways_list(AxumState(app): AxumState<Arc<App>>) -> Response {
    let mut out = Vec::new();
    for account in app.gateways.addrs.keys() {
        let state = match app
            .gateways
            .call(account, None, Duration::from_secs(5))
            .await
        {
            Ok((w, _)) => {
                json!({ "account": account, "state": w["state"], "environment": w["environment"] })
            }
            Err(e) => json!({ "account": account, "error": e }),
        };
        out.push(state);
    }
    no_store(axum::Json(json!({ "gateways": out })))
}

/// For the Spread builder: whether the console can trade, and each paper
/// account's gateway state with the console's own open orders (`tt-`).
async fn orders_list(AxumState(app): AxumState<Arc<App>>) -> Response {
    let can = app.gateways.can_trade();
    let mut out = Vec::new();
    for account in app.gateways.addrs.keys() {
        let who = if can {
            gateways::As::Trade
        } else {
            gateways::As::Ops
        };
        let req = can.then(|| json!({ "type": "open_orders" }));
        out.push(match app.gateways.call_as(who, account, req, Duration::from_secs(5)).await {
            Ok((w, r)) => {
                let mine: Vec<serde_json::Value> = r
                    .as_ref()
                    .and_then(|r| r["orders"].as_array())
                    .into_iter()
                    .flatten()
                    .filter(|o| o["client_order_id"].as_str().is_some_and(|i| i.starts_with(app.gateways.prefix.as_str())))
                    .map(|o| json!({ "client_order_id": o["client_order_id"], "status": o["status"], "filled_qty": o["filled_qty"], "avg_price": o["avg_price"], "legs": o["legs"], "limit_price": o["broker_raw"]["limit_price"], "qty": o["broker_raw"]["qty"], "submitted_at": o["broker_raw"]["submitted_at"] }))
                    .collect();
                json!({ "account": account, "state": w["state"], "environment": w["environment"], "broker": w["broker"], "orders": mine })
            }
            Err(e) => json!({ "account": account, "error": e }),
        });
    }
    no_store(axum::Json(
        json!({ "trading": can, "prefix": app.gateways.prefix, "accounts": out }),
    ))
}

/// Send one of the Spread builder's orders to a paper account's gateway as
/// the console's trade client. The body names the account again in
/// `confirm`; the id is made here. The gateway's answer is returned as is
/// (`accepted` is not a fill).
async fn order_submit(
    AxumState(app): AxumState<Arc<App>>,
    axum::extract::Path(account): axum::extract::Path<String>,
    body: axum::body::Bytes,
) -> Response {
    let ask: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
    if ask["confirm"].as_str() != Some(account.as_str()) {
        return (
            StatusCode::BAD_REQUEST,
            axum::Json(json!({ "error": "confirm the account" })),
        )
            .into_response();
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let id = format!(
        "{}{}-{:05}",
        app.gateways.prefix,
        now.as_secs(),
        now.subsec_nanos() % 100_000
    );
    if !app.gateways.addrs.contains_key(&account) {
        return no_gateway();
    }
    // A live account's order also says so, from the ticket's typed confirmation.
    if ask["live"] != true && is_live(&app, &account).await {
        return (
            StatusCode::BAD_REQUEST,
            axum::Json(json!({ "error": "a live account's order must be confirmed as live" })),
        )
            .into_response();
    }
    let intent = match gateways::intent(&ask["order"], &id) {
        Ok(i) => i,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, axum::Json(json!({ "error": e }))).into_response();
        }
    };
    trade_request(
        &app,
        &account,
        json!({ "type": "submit", "intent": intent }),
        30,
    )
    .await
}

/// Cancel one of the console's orders.
async fn order_cancel(
    AxumState(app): AxumState<Arc<App>>,
    axum::extract::Path((account, id)): axum::extract::Path<(String, String)>,
) -> Response {
    if !id.starts_with(app.gateways.prefix.as_str()) {
        return (
            StatusCode::BAD_REQUEST,
            axum::Json(json!({ "error": "not one of the console's orders" })),
        )
            .into_response();
    }
    trade_request(
        &app,
        &account,
        json!({ "type": "cancel", "client_order_id": id }),
        15,
    )
    .await
}

async fn trade_request(
    app: &App,
    account: &str,
    request: serde_json::Value,
    wait: u64,
) -> Response {
    if !app.gateways.addrs.contains_key(account) {
        return no_gateway();
    }
    match app
        .gateways
        .call_as(
            gateways::As::Trade,
            account,
            Some(request),
            Duration::from_secs(wait),
        )
        .await
    {
        Ok((_, Some(r))) => no_store(axum::Json(r)),
        Ok((_, None)) => (
            StatusCode::BAD_GATEWAY,
            axum::Json(json!({ "error": "no response" })),
        )
            .into_response(),
        Err(e) => (StatusCode::BAD_GATEWAY, axum::Json(json!({ "error": e }))).into_response(),
    }
}

/// Close every position of a paper account at market through its gateway:
/// halts opens, cancels open orders, liquidates. The body must name the
/// account in `confirm`.
async fn account_flatten(
    AxumState(app): AxumState<Arc<App>>,
    axum::extract::Path(account): axum::extract::Path<String>,
    body: axum::body::Bytes,
) -> Response {
    let ask: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
    if ask["confirm"].as_str() != Some(account.as_str()) {
        return (
            StatusCode::BAD_REQUEST,
            axum::Json(json!({ "error": "type the account's name to confirm" })),
        )
            .into_response();
    }
    if !app.gateways.addrs.contains_key(&account) {
        return no_gateway();
    }
    if is_live(&app, &account).await {
        return live_refused();
    }
    let reason = ask["reason"]
        .as_str()
        .filter(|r| !r.is_empty())
        .unwrap_or("flatten from the console");
    gateway_request(
        &app,
        &account,
        json!({ "type": "flatten", "reason": reason }),
        60,
    )
    .await
}

/// Allow opening orders again on a paper account (the gateway's `unkill`).
async fn account_resume(
    AxumState(app): AxumState<Arc<App>>,
    axum::extract::Path(account): axum::extract::Path<String>,
    body: axum::body::Bytes,
) -> Response {
    let ask: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
    if !app.gateways.addrs.contains_key(&account) {
        return no_gateway();
    }
    if is_live(&app, &account).await {
        return live_refused();
    }
    let reason = ask["reason"]
        .as_str()
        .filter(|r| !r.is_empty())
        .unwrap_or("resumed from the console");
    gateway_request(
        &app,
        &account,
        json!({ "type": "unkill", "reason": reason }),
        15,
    )
    .await
}

fn no_gateway() -> Response {
    (
        StatusCode::NOT_FOUND,
        axum::Json(json!({ "error": "this account has no order gateway" })),
    )
        .into_response()
}

/// Whether an account's gateway trades live (from its welcome); unreachable counts as live,
/// so Flatten and Resume fail closed.
async fn is_live(app: &App, account: &str) -> bool {
    match app
        .gateways
        .call(account, None, Duration::from_secs(5))
        .await
    {
        Ok((w, _)) => w["environment"] != "paper",
        Err(_) => true,
    }
}

/// Flatten (sell everything at market) and Resume are for paper accounts only: a live
/// account holds more than the console's orders.
fn live_refused() -> Response {
    (
        StatusCode::FORBIDDEN,
        axum::Json(json!({ "error": "not offered for a live account from the console" })),
    )
        .into_response()
}

async fn gateway_request(
    app: &App,
    account: &str,
    request: serde_json::Value,
    wait: u64,
) -> Response {
    if !app.gateways.addrs.contains_key(account) {
        return no_gateway();
    }
    match app
        .gateways
        .call(account, Some(request), Duration::from_secs(wait))
        .await
    {
        Ok((_, Some(r))) => no_store(axum::Json(r)),
        Ok((_, None)) => (
            StatusCode::BAD_GATEWAY,
            axum::Json(json!({ "error": "no response" })),
        )
            .into_response(),
        Err(e) => (StatusCode::BAD_GATEWAY, axum::Json(json!({ "error": e }))).into_response(),
    }
}

fn no_store(body: impl IntoResponse) -> Response {
    ([(header::CACHE_CONTROL, "no-store")], body).into_response()
}

fn env(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_owned())
}
