//! The tinytrader store (decision 0005): the one process that touches the
//! data disks. Other services reach data through it over HTTP.
//!
//! - **Volumes**: each `--volume NAME=PATH` is a disk the store manages;
//!   `NAME=PATH:ro` marks one read-only. New data goes to the writable volume
//!   with the most free space below the critical threshold.
//! - **Catalog** (`GET /catalog`): what datasets each volume holds, which
//!   days they cover, and how full the volume is (see the catalog module).
//! - **Ingest**: `PUT /incoming/{provider}/{batch}/{file}?size=&sha256=`
//!   uploads one file of a provider batch, verified on arrival;
//!   `GET /incoming` lists batches waiting; `POST /incorporate/{provider}/{batch}`
//!   files a complete batch into the standard layout, with provenance and a
//!   migration ledger (see the ingest module). `GET /batches/{provider}`
//!   says which batches the volumes hold, and how. `POST /adopt/{provider}`
//!   with `{ "path": … }` files a provider download folder already on a
//!   writable volume where it is, on the same terms.
//! - **Partial days** (decision 0018, see the partial module): `POST
//!   /import/{provider}/{id}` files DBN that didn't come from a provider
//!   batch, uploaded the same way with an `import.json`; a day file holding a
//!   list of contracts is a partial day, merged with any partial day already
//!   held. `GET /coverage?provider=&dataset=&symbol=&schema=&start=&end=`
//!   says, for each day held, whether it is complete or which contracts and
//!   hours it holds, and the files holding it.
//! - **Day conditions** (see the conditions module): each filed day keeps
//!   the provider's `available` / `degraded` / `missing`, from its batch's
//!   `condition.json`, `POST /conditions/{provider}`, or `POST
//!   /conditions/backfill` over the receipts kept.
//! - **Reading** (see the read module): `GET /files?pattern=&provider=&limit=`
//!   lists the DBN files the last scan found by logical name,
//!   `<volume>/<path>`; `GET /files/{volume}/{path}` sends one, or answers
//!   `503` while its volume is away or not yet scanned. Read-only, finished
//!   files only.
//! - **Holdings that fail closed** (decision 0019): `/catalog` and `/coverage`
//!   answer `503` until the first scan is done. Every volume the store has
//!   used is remembered in its state file (`STORE_STATE`), so one that is
//!   missing is reported in `missing_volumes`, never left out. `POST
//!   /volumes/{name}/retire` forgets one that is away for good, keeping a
//!   record of what it held. `/coverage` answers from the last scan, and
//!   waits for a rescan asked for after a filing. `POST
//!   /volumes/{name}/ignore` skips a path the store can't read, by an
//!   operator's decision.
//!
//! - **Writes** (the uploads, filing, adopting and approving drives) are
//!   refused from a web page of another origin, and, when a token is
//!   configured (`STORE_WRITE_TOKEN`, or the file `STORE_WRITE_TOKEN_FILE`
//!   names), need it as `Authorization: Bearer <token>`. Reads stay open.
//!   Without a token, writes are open to whatever can reach the store.
//!
//! It runs natively (under launchd on a Mac), not in a container, so it sees
//! the real disks, and binds to loopback by default (`STORE_LISTEN`).

mod catalog;
mod conditions;
mod filer;

/// In tests, run between a volume's scan and the check that it is still the
/// same volume, to make it go away mid-scan.
#[cfg(test)]
type MidScan = Box<dyn Fn(&Path) + Send>;
#[cfg(test)]
static MID_SCAN: std::sync::Mutex<Option<MidScan>> = std::sync::Mutex::new(None);
mod ingest;
mod manifest;
#[cfg(test)]
mod openapi;
mod partial;
mod read;
mod rebuild;
mod state;
mod volumes;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tinytrader_observe::lock;

use axum::body::Body;
use axum::extract::{Path as UrlPath, Query, State as AxState};
use axum::http::StatusCode;
use axum::routing::{get, post, put};
use axum::{Json, Router};
use catalog::Thresholds;
use clap::Parser;
use futures_util::StreamExt as _;
use ingest::Volume;
use serde::Deserialize;
use serde_json::{Value, json};
use tinytrader_observe::guard::WriteToken;
use tinytrader_observe::{Events, Observer, State, listen_address, unix_seconds};

/// The tinytrader data store.
#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// A volume as NAME=PATH, or NAME=PATH:ro for read-only. A path under
    /// /Volumes must be a mount point; add `:anydir` to take any folder.
    /// Repeat, or separate with `;`.
    #[arg(long = "volume", env = "STORE_VOLUMES", value_delimiter = ';')]
    volumes: Vec<String>,

    /// Where drives are mounted (such as /Volumes). Drives there carrying an
    /// approval marker are used as they appear; the others are offered for
    /// approval in the console (decision 0007).
    #[arg(long, env = "STORE_DISCOVER")]
    discover: Option<PathBuf>,

    /// How often to rescan the volumes, in seconds.
    #[arg(long, env = "STORE_RESCAN_SECONDS", default_value_t = 300)]
    rescan_seconds: u64,

    /// Volume usage, as a percentage, at which a volume is in warning.
    #[arg(long, default_value_t = 80.0)]
    warning: f64,

    /// Volume usage at which no new data is placed on a volume.
    #[arg(long, default_value_t = 90.0)]
    critical: f64,

    /// Volume usage at which capture stops cleanly.
    #[arg(long, default_value_t = 97.0)]
    floor: f64,

    /// The store's state file: every volume it has used. By default
    /// `~/Library/Application Support/tinytrader/store-state.json`.
    #[arg(long = "state", env = "STORE_STATE")]
    state: Option<PathBuf>,
}

struct Store {
    /// The configured volumes, then approved drives as they are found.
    volumes: std::sync::RwLock<Vec<Volume>>,
    discover: Option<PathBuf>,
    limits: Thresholds,
    /// The last scan; null until the first is done.
    catalog: Mutex<Value>,
    /// Every volume the store has used, kept in `state_path`.
    remembered: Mutex<state::Remembered>,
    state_path: PathBuf,
    /// What the last scan found on each volume: `/coverage` answers from it.
    index: Mutex<Arc<Index>>,
    /// Schema folders updated in place (after a filing) since the scan in
    /// progress started: it reads them again before it answers.
    refreshed: Mutex<Vec<PathBuf>>,
    /// Rescans asked for (after a filing), and how many of them finished
    /// scans have covered: `/coverage` waits for those asked for.
    requested: AtomicU64,
    completed: AtomicU64,
    /// How long `/coverage` waits for a rescan asked for, and for one it
    /// asks for itself (`fresh`).
    wait: Duration,
    fresh_wait: Duration,
    events: Events,
    rescan: tokio::sync::Notify,
    bytes_in: AtomicU64,
    batches_in: AtomicU64,
    /// The filing owner (decision 0030): every write asks it for a turn, and
    /// partial-day filings are filed by it, in groups.
    filer: filer::Filer<PartialJob>,
}

/// A partial-day filing waiting for the filer: the store, the filing, the folders it
/// writes, and where its answer goes.
type PartialJob = (Shared, partial::Filing, Vec<PathBuf>, Answer);

type Answer = tokio::sync::oneshot::Sender<Result<Value, String>>;

/// The most filings filed as one group; the rest wait for the next turn.
const GROUP: usize = 16;

/// The status for a filing that didn't run: 503 when the filer's queue was full.
fn filing_failed(e: &str) -> StatusCode {
    if e == BUSY || e == STOPPED {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

/// What a write asked while the filer's queue is full is told (503).
const BUSY: &str = "busy: the store's filing queue is full; try again";

/// What a write asked while the filer has stopped is told (503).
const STOPPED: &str = "the filer stopped; the store is restarting";

fn refusal(r: &filer::Refused) -> &'static str {
    match r {
        filer::Refused::Busy => BUSY,
        filer::Refused::Stopped => STOPPED,
    }
}

type Shared = Arc<Store>;

/// The places the last scan found, by volume, and when; and when a filing
/// last updated them.
#[derive(Default, Clone)]
struct Index {
    scanned_at: f64,
    updated_at: f64,
    volumes: Vec<(String, Vec<catalog::Place>)>,
    /// The files it found that the store serves, by volume, sorted: `/files`
    /// lists them.
    files: Vec<(String, Vec<catalog::Served>)>,
    /// What the scan couldn't read, on volumes that are there.
    errors: Vec<String>,
}

impl Index {
    /// Every schema folder the scan found in the store layout.
    fn days_folders(&self) -> Vec<PathBuf> {
        self.volumes
            .iter()
            .flat_map(|(_, places)| places)
            .filter_map(|p| match p {
                catalog::Place::Days { dir, .. } => Some(dir.clone()),
                _ => None,
            })
            .collect()
    }
}

type Reply = (StatusCode, Json<Value>);

fn reply(code: StatusCode, v: Value) -> Reply {
    (code, Json(v))
}

fn refused(code: StatusCode, why: impl Into<String>) -> Reply {
    reply(code, json!({ "error": why.into() }))
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    // Writes (uploads, filing, adopting, approving drives) need this bearer
    // token when it is configured; reads stay open.
    let write_token = match WriteToken::from_env("STORE_WRITE_TOKEN") {
        Ok(t) => t,
        Err(e) => {
            eprintln!("store: {e}");
            std::process::exit(2);
        }
    };
    let volumes: Vec<Volume> = args
        .volumes
        .iter()
        .filter_map(|v| {
            let (name, path) = v.split_once('=')?;
            let (mut path, mut writable, mut anydir) = (path.trim(), true, false);
            loop {
                if let Some(p) = path.strip_suffix(":ro") {
                    (path, writable) = (p, false);
                } else if let Some(p) = path.strip_suffix(":anydir") {
                    (path, anydir) = (p, true);
                } else {
                    break;
                }
            }
            Some(Volume {
                name: name.trim().to_owned(),
                path: PathBuf::from(path),
                writable,
                require_mount: !anydir && Path::new(path).starts_with("/Volumes"),
            })
        })
        .collect();
    if volumes.is_empty() && args.discover.is_none() {
        eprintln!("store: give at least one --volume NAME=PATH, or --discover DIR");
        std::process::exit(2);
    }
    let limits = Thresholds {
        warning: args.warning,
        critical: args.critical,
        floor: args.floor,
    };
    let state_path = args.state.clone().unwrap_or_else(state::default_path);
    let remembered = match state::load(&state_path) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("store: {e}");
            std::process::exit(2);
        }
    };
    let store = Store::new(
        volumes,
        args.discover.clone(),
        limits,
        state_path,
        remembered,
    );
    if let Err(e) = store.remember(&[]) {
        eprintln!("store: cannot keep the list of volumes: {e}");
        std::process::exit(2);
    }
    let listen = listen_address("STORE_LISTEN", "127.0.0.1:9120");
    let observer = Observer::new("store", env!("CARGO_PKG_VERSION"));
    let names = store
        .volumes()
        .iter()
        .map(|v| format!("{}{}", v.name, if v.writable { "" } else { " (read-only)" }))
        .collect::<Vec<_>>()
        .join(", ");
    observer.describe(json!({
        "role": format!("The data store: ingests provider downloads into the standard layout and catalogs {names}."),
        "streams": { "catalog": "/catalog", "coverage": "/coverage", "events": "/events" },
        "writes": if write_token.required() { "bearer token" } else { "open" },
        "schedule": [
            { "name": "Store", "kind": "always", "when": "always (launchd keeps it running)" },
            { "name": "Catalog rescan", "kind": "interval", "every_seconds": args.rescan_seconds, "when": format!("every {} min, and after each filing", args.rescan_seconds / 60) },
        ],
    }));
    {
        let s = store.clone();
        observer.set_metrics(move |w| {
            w.counter(
                "tinytrader_store_ingested_bytes_total",
                "Bytes received for ingest.",
                &[],
                s.bytes_in.load(Ordering::Relaxed) as f64,
            );
            w.counter(
                "tinytrader_store_batches_total",
                "Batches incorporated.",
                &[],
                s.batches_in.load(Ordering::Relaxed) as f64,
            );
            let c = lock(&s.catalog);
            for root in c["roots"].as_array().into_iter().flatten() {
                let labels = [("volume", root["name"].as_str().unwrap_or("?"))];
                if let Some(u) = root["volume"]["used_fraction"].as_f64() {
                    w.gauge(
                        "tinytrader_store_volume_used_ratio",
                        "Fraction of the volume in use.",
                        &labels,
                        u,
                    );
                }
                if let Some(b) = root["dataset_bytes"].as_f64() {
                    w.gauge(
                        "tinytrader_store_dataset_bytes",
                        "Bytes in recognised datasets.",
                        &labels,
                        b,
                    );
                }
            }
        });
    }

    let app = router(store.clone(), write_token.clone())
        .merge(observer.router())
        .merge(store.events.router());
    let listener = match tokio::net::TcpListener::bind(&listen).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("store: cannot listen on {listen}: {e}");
            std::process::exit(2);
        }
    };
    eprintln!(
        "store: {names}; endpoints on http://{listen}; writes {}",
        if write_token.required() {
            "need the bearer token (STORE_WRITE_TOKEN)"
        } else {
            "open (no STORE_WRITE_TOKEN)"
        }
    );
    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            eprintln!("store: http server stopped: {e}");
        }
    });

    let every = Duration::from_secs(args.rescan_seconds.max(10));
    loop {
        scan(&store, &observer).await;
        tokio::select! {
            () = tokio::time::sleep(every) => {}
            () = store.rescan.notified() => {}
        }
    }
}

/// The store's own endpoints. Writes can't be triggered by a web page of
/// another origin, and need the bearer token when one is configured.
fn router(store: Shared, token: WriteToken) -> Router {
    Router::new()
        .route("/catalog", get(catalog))
        .route("/incoming", get(list_incoming))
        .route("/incoming/{provider}/{batch}/{file}", put(upload))
        .route("/incorporate/{provider}/{batch}", post(incorporate))
        .route("/import/{provider}/{id}", post(import))
        .route("/coverage", get(coverage))
        .route("/batches/{provider}", get(batches))
        .route("/adopt/{provider}", post(adopt))
        .route("/volumes/approve", post(approve))
        .route("/volumes/{name}/retire", post(retire))
        .route("/volumes/{name}/accept-empty", post(accept_empty))
        .route("/volumes/{name}/ignore", post(ignore).delete(unignore))
        .route("/conditions/backfill", post(backfill_conditions))
        .route("/coverage/rebuild", post(rebuild_coverage))
        .route("/conditions/{provider}", post(post_conditions))
        .route("/files", get(list_files))
        .route("/files/{volume}/{*path}", get(send_file))
        .layer(axum::middleware::from_fn(
            tinytrader_observe::guard::same_origin_writes,
        ))
        .layer(axum::middleware::from_fn_with_state(
            token,
            tinytrader_observe::guard::token_writes,
        ))
        .with_state(store)
}

/// Rescan every volume into the catalog.
async fn scan(store: &Shared, observer: &Observer) {
    // Approved drives join as they are found; one that goes missing stays
    // listed (and reported) until it returns.
    let mut candidates = Vec::new();
    if let Some(dir) = store.discover.clone() {
        let found = tokio::task::spawn_blocking(move || volumes::discover(&dir)).await;
        if let Ok(found) = found {
            let mut list = store
                .volumes
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for v in found.marked {
                // A known drive mounted somewhere new is used there.
                if let Some(x) = list
                    .iter_mut()
                    .find(|x| x.name == v.name && x.path != v.path && volumes::present(x).is_err())
                {
                    eprintln!("store: drive {} is now at {}", v.name, v.path.display());
                    x.path = v.path;
                    continue;
                }
                if !list.iter().any(|x| x.path == v.path || x.name == v.name) {
                    eprintln!(
                        "store: using approved drive {} at {}",
                        v.name,
                        v.path.display()
                    );
                    list.push(v);
                }
            }
            // A drive the configuration already names isn't offered again.
            candidates = found
                .candidates
                .into_iter()
                .filter(|c| !list.iter().any(|v| c["path"].as_str() == v.path.to_str()))
                .collect();
        }
    }
    // Rescans asked for until now are covered by this one, and folders
    // updated in place until now are read by it.
    let target = store.requested.load(Ordering::SeqCst);
    lock(&store.refreshed).clear();
    let (volumes, limits) = (store.volumes(), store.limits);
    let (held, ignored): (BTreeMap<String, Value>, Vec<Value>) = {
        let r = lock(&store.remembered);
        let held = r
            .volumes
            .iter()
            .filter(|(_, k)| state::held_any(k))
            .map(|(n, k)| (n.clone(), k.held.clone()))
            .collect();
        (held, r.ignored.clone())
    };
    let s = store.clone();
    let found = tokio::task::spawn_blocking(move || {
        volumes
            .iter()
            .map(|v| {
                let mine: Vec<&Value> = ignored.iter().filter(|i| i["volume"] == v.name.as_str()).collect();
                let skip: Vec<PathBuf> = mine.iter().filter_map(|i| i["path"].as_str()).map(PathBuf::from).collect();
                // The volume as it was when the scan began: a scan that ends with
                // it gone, or another device there, counts for nothing (P0,
                // 2026-11-10: a drive dropping out mid-scan read as holding less).
                let identity = |p: &Path| {
                    use std::os::unix::fs::MetadataExt as _;
                    std::fs::metadata(p).ok().map(|m| (m.dev(), m.ino()))
                };
                let began = volumes::present(v).ok().and_then(|()| identity(&v.path));
                let scanned = match volumes::present(v) {
                    Ok(()) => {
                        let r = catalog::scan_volume(&v.name, &v.path, limits, &skip);
                        #[cfg(test)]
                        if let Some(f) = lock(&MID_SCAN).as_ref() {
                            f(&v.path);
                        }
                        let ended = volumes::present(v).ok().and_then(|()| identity(&v.path));
                        if ended.is_some() && ended == began {
                            Ok(r)
                        } else {
                            Err(format!(
                                "{} went away or changed during the scan; what it held is kept from before",
                                v.path.display()
                            ))
                        }
                    }
                    Err(e) => Err(e),
                };
                let (mut root, mut places, mut served) = match scanned {
                    Ok(r) => r,
                    Err(e) => {
                        // What it held when last read in full, so its data
                        // shows as away rather than gone.
                        let was = held.get(&v.name).cloned().unwrap_or_else(|| json!({}));
                        let root = json!({ "name": v.name, "path": v.path, "present": false, "datasets": [], "held": was, "unrecognised": [], "errors": [e], "pending_files": [] });
                        (root, Vec::new(), Vec::new())
                    }
                };
                // Settling writes manifests: only while no filing runs (one
                // may be between recording its merge and swapping it in).
                // Otherwise the merge stays listed, for the next scan.
                if let Some(_turn) = s.filer.turn_if_idle() {
                    settle_merges(v, &mut root, &mut places, &mut served);
                }
                let errors: Vec<String> = if root["present"] == true {
                    root["errors"].as_array().into_iter().flatten().filter_map(|e| e.as_str().map(str::to_owned)).collect()
                } else {
                    Vec::new()
                };
                root["ignored"] = json!(mine);
                // Data that was there and is gone is unknown, not absent.
                if root["present"] == true
                    && held.contains_key(&v.name)
                    && root["datasets"].as_array().is_none_or(Vec::is_empty)
                    && let Some(errors) = root["errors"].as_array_mut()
                {
                    errors.push(json!(emptied(v)));
                }
                root["writable"] = json!(v.writable);
                (root, (v.name.clone(), places), (v.name.clone(), served), errors)
            })
            .collect::<Vec<_>>()
    })
    .await
    .unwrap_or_default();
    let mut index = Index {
        scanned_at: unix_seconds(),
        ..Index::default()
    };
    let mut scanned = Vec::new();
    for (root, places, served, errors) in found {
        scanned.push(root);
        index.volumes.push(places);
        index.files.push(served);
        index.errors.extend(errors);
    }
    let scanned_at = index.scanned_at;
    let worst = scanned
        .iter()
        .filter_map(|r| r["volume"]["status"].as_str())
        .max_by_key(|s| catalog::severity(s))
        .unwrap_or("ok")
        .to_owned();
    // A volume that isn't there, or whose datasets couldn't all be read, is
    // missing: what it holds is unknown.
    let missing: Vec<String> = scanned
        .iter()
        .filter(|r| r["present"] != true || r["errors"].as_array().is_some_and(|e| !e.is_empty()))
        .filter_map(|r| r["name"].as_str().map(str::to_owned))
        .collect();
    let mut detail = if missing.is_empty() {
        format!("{} volumes; fullest {worst}", scanned.len())
    } else {
        format!("missing: {}", missing.join(", "))
    };
    if let Err(e) = store.remember(&scanned) {
        eprintln!("store: cannot keep the list of volumes: {e}");
        detail = format!("{detail}; cannot keep the list of volumes: {e}");
    }
    let degraded = !missing.is_empty() || catalog::severity(&worst) >= 2;
    // Folders a filing updated while this scan ran are read again, so the
    // filing isn't lost when this scan's answer replaces the last.
    let mut redo = lock(&store.refreshed);
    let volumes = store.volumes();
    for dir in redo.drain(..) {
        reapply(&volumes, &dir, &mut scanned, &mut index);
    }
    *lock(&store.index) = Arc::new(index);
    *lock(&store.catalog) = json!({
        "generated_at": scanned_at,
        "thresholds": { "warning": store.limits.warning, "critical": store.limits.critical, "floor": store.limits.floor },
        "roots": scanned,
        "missing_volumes": missing,
        "candidates": candidates,
        "discover": store.discover,
    });
    drop(redo);
    observer.set_state(
        if degraded {
            State::Degraded
        } else {
            State::Ready
        },
        &detail,
    );
    store.completed.fetch_max(target, Ordering::SeqCst);
}

/// The schema folders a filing waiting in `parts` would write, on any of its
/// volumes, from its receipt (`import.json`, or a batch's `metadata.json`,
/// filed under `parent` if given): read again after it, whether it
/// succeeded or stopped partway.
fn target_folders(
    parts: &[ingest::Part],
    elsewhere: &[PathBuf],
    parent: Option<&str>,
) -> Vec<PathBuf> {
    let read = |name: &str| -> Option<Value> {
        parts
            .iter()
            .find_map(|p| std::fs::read_to_string(p.inc.join(name)).ok())
            .and_then(|t| serde_json::from_str(&t).ok())
    };
    let level: Option<PathBuf> = if let Some(r) = read(partial::IMPORT) {
        match (
            r["dataset"].as_str(),
            r["symbol"].as_str(),
            r["schema"].as_str(),
        ) {
            (Some(d), Some(s), Some(k)) => Some([d, s, k].iter().collect()),
            _ => None,
        }
    } else if let Some(m) = read("metadata.json") {
        let q = &m["query"];
        let symbols: Vec<&str> = q["symbols"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .collect();
        match (
            q["dataset"].as_str(),
            q["schema"].as_str(),
            parent,
            symbols.as_slice(),
        ) {
            (Some(d), Some(k), Some(p), _) => Some([d, p, k].iter().collect()),
            (Some(d), Some(k), None, ["ALL_SYMBOLS"]) => Some([d, k].iter().collect()),
            (Some(d), Some(k), None, [one]) => Some([d, one, k].iter().collect()),
            _ => None,
        }
    } else {
        None
    };
    let Some(level) = level.filter(|l| {
        l.components()
            .all(|c| matches!(c, std::path::Component::Normal(_)))
    }) else {
        return Vec::new();
    };
    parts
        .iter()
        .map(|p| &p.root)
        .chain(elsewhere)
        .map(|r| r.join(&level))
        .collect()
}

/// Settle the merges a volume's scan found left pending (decision 0018): the
/// day file's hash says whether its merged file is in place. Each folder
/// settled is read again into the scan's answer.
fn settle_merges(
    v: &Volume,
    root: &mut Value,
    places: &mut Vec<catalog::Place>,
    served: &mut Vec<catalog::Served>,
) {
    let merges = root["pending_merges"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let mut folders: Vec<PathBuf> = Vec::new();
    for m in &merges {
        let (Some(folder), Some(day)) = (m["folder"].as_str(), m["day"].as_str()) else {
            continue;
        };
        let folder = PathBuf::from(folder);
        match manifest::settle(&folder, day) {
            Ok(_) => eprintln!(
                "store: settled the merge into {day} in {}",
                folder.display()
            ),
            Err(e) => {
                if let Some(errors) = root["errors"].as_array_mut() {
                    errors.push(json!(e));
                }
            }
        }
        if !folders.contains(&folder) {
            folders.push(folder);
        }
    }
    for folder in folders {
        if let Some(f) = catalog::Folder::read(&v.path, &folder) {
            f.apply_root(root, &v.name);
            f.apply_places(places);
            f.apply_files(served);
        }
    }
}

/// Read the schema folder `dir` again into a scan's answer: the root,
/// places and served files of the volume holding it.
fn reapply(volumes: &[Volume], dir: &Path, roots: &mut [Value], index: &mut Index) {
    let Some(v) = volumes.iter().find(|v| dir.starts_with(&v.path)) else {
        return;
    };
    let Some(folder) = catalog::Folder::read(&v.path, dir) else {
        return;
    };
    if let Some(root) = roots.iter_mut().find(|r| r["name"] == v.name.as_str()) {
        folder.apply_root(root, &v.name);
    }
    if let Some((_, p)) = index.volumes.iter_mut().find(|(n, _)| *n == v.name) {
        folder.apply_places(p);
    }
    if let Some((_, f)) = index.files.iter_mut().find(|(n, _)| *n == v.name) {
        folder.apply_files(f);
    }
}

impl Store {
    /// Update the catalog and the index for schema folders just written (a
    /// filing, an import, a merge, a day's condition), in place: answers
    /// reflect it at once, without scanning every volume again.
    fn refresh(&self, dirs: &[PathBuf]) {
        let volumes = self.volumes();
        let mut redo = lock(&self.refreshed);
        let mut catalog = lock(&self.catalog);
        let mut index = lock(&self.index);
        let index = Arc::make_mut(&mut index);
        for dir in dirs.iter().filter(|d| d.is_dir()) {
            if !redo.contains(dir) {
                redo.push(dir.clone());
            }
            if let Some(roots) = catalog["roots"].as_array_mut() {
                reapply(&volumes, dir, roots, index);
            }
        }
        let now = unix_seconds();
        index.updated_at = now;
        if !catalog.is_null() {
            catalog["updated_at"] = json!(now);
        }
    }

    /// Run a filing in its turn (decision 0030), alone, then read again the
    /// folders it wrote (`targets`, and those its summary names). The write runs in
    /// a task of its own, holding the turn, so a request whose client goes away
    /// can't end the turn while it writes.
    async fn file_alone(
        self: &Arc<Self>,
        targets: Vec<PathBuf>,
        f: impl FnOnce() -> Result<Value, String> + Send + 'static,
    ) -> Result<Result<Value, String>, String> {
        let turn = self
            .filer
            .turn()
            .await
            .map_err(|r| refusal(&r).to_owned())?;
        let (tx, rx) = tokio::sync::oneshot::channel();
        let s = self.clone();
        tokio::spawn(async move {
            let _turn = turn;
            let _ = tokio::task::spawn_blocking(move || {
                let r = f();
                let mut dirs = targets;
                dirs.extend(r.as_ref().map(Self::filed_folders).unwrap_or_default());
                s.refresh(&dirs);
                let _ = tx.send(r);
            })
            .await;
        });
        rx.await
            .map_err(|_| "the filing stopped unexpectedly".to_owned())
    }

    /// File a partial-day filing together with others waiting right behind it
    /// (decision 0024, section 8b), up to `GROUP` in the order asked: the filer
    /// files them as a group, so each day they touch is written once, and the
    /// folders they wrote are read again. Each gets its own answer.
    async fn file_partial(
        self: &Arc<Self>,
        f: partial::Filing,
        targets: Vec<PathBuf>,
    ) -> Result<Result<Value, String>, String> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.filer
            .file_partial((self.clone(), f, targets, tx))
            .map_err(|r| refusal(&r).to_owned())?;
        rx.await
            .map_err(|_| "the filing stopped unexpectedly".to_owned())
    }

    /// Files a group of partial-day filings (on the filer's blocking thread),
    /// answering each.
    fn file_group(&self, queued: Vec<PartialJob>) {
        let (group, rest): (Vec<_>, Vec<_>) =
            queued.into_iter().map(|(_, f, t, tx)| (f, (t, tx))).unzip();
        let done = partial::import_group(&group);
        let mut answers = Vec::new();
        let mut dirs = Vec::new();
        for ((targets, tx), r) in rest.into_iter().zip(done) {
            dirs.extend(targets);
            dirs.extend(r.as_ref().map(Self::filed_folders).unwrap_or_default());
            answers.push((tx, r));
        }
        self.refresh(&dirs);
        for (tx, r) in answers {
            let _ = tx.send(r);
        }
    }

    /// The schema folders of the day files a filing's summary names.
    fn filed_folders(summary: &Value) -> Vec<PathBuf> {
        let mut out: Vec<PathBuf> = ["files", "days"]
            .iter()
            .flat_map(|k| summary[k].as_array().into_iter().flatten())
            .filter_map(|f| f["path"].as_str())
            .filter_map(|p| Path::new(p).parent().map(Path::to_path_buf))
            .collect();
        out.sort();
        out.dedup();
        out
    }

    /// Ask for a rescan (after a filing, an import or a merge, or a change of
    /// what is ignored); `/coverage` waits for it.
    /// The paths an operator has said to skip on the volume `name`.
    fn ignored_paths(&self, name: &str) -> Vec<PathBuf> {
        lock(&self.remembered)
            .ignored
            .iter()
            .filter(|i| i["volume"] == name)
            .filter_map(|i| i["path"].as_str().map(PathBuf::from))
            .collect()
    }

    /// Returns the rescan's number: a scan that started after it covers it.
    fn request_rescan(&self) -> u64 {
        let n = self.requested.fetch_add(1, Ordering::SeqCst) + 1;
        self.rescan.notify_one();
        n
    }

    /// Wait (at most `limit`) for a scan to cover rescan `want`.
    async fn covered(&self, want: u64, limit: Duration) -> Result<(), Reply> {
        let deadline = tokio::time::Instant::now() + limit;
        while self.completed.load(Ordering::SeqCst) < want {
            if tokio::time::Instant::now() >= deadline {
                return Err(refused(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "the store is rescanning; ask again shortly",
                ));
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        Ok(())
    }

    /// With `fresh`, ask for a rescan and wait for it (a scan started after
    /// now; requests meanwhile share it), at most `fresh_wait`; otherwise
    /// wait only for a rescan already asked for, at most `wait`.
    async fn settled(&self, fresh: bool) -> Result<(), Reply> {
        if fresh {
            self.covered(self.request_rescan(), self.fresh_wait).await
        } else {
            self.covered(self.requested.load(Ordering::SeqCst), self.wait)
                .await
        }
    }

    /// A store over the configured volumes, then every remembered volume the
    /// configuration doesn't name, so one that is missing is reported.
    fn new(
        mut volumes: Vec<Volume>,
        discover: Option<PathBuf>,
        limits: Thresholds,
        state_path: PathBuf,
        remembered: state::Remembered,
    ) -> Shared {
        for (name, k) in &remembered.volumes {
            if !volumes.iter().any(|v| &v.name == name) {
                volumes.push(Volume {
                    name: name.clone(),
                    path: k.path.clone(),
                    writable: k.writable,
                    require_mount: k.require_mount,
                });
            }
        }
        let filer = filer::Filer::start(
            GROUP,
            |group: Vec<PartialJob>| {
                if let Some(s) = group.first().map(|j| j.0.clone()) {
                    s.file_group(group);
                }
            },
            |why| {
                // Its state can't be trusted: exit, for the supervisor to start
                // the store again from its scan and recovery on disk.
                eprintln!("store: failed: {why}; exiting to be restarted");
                std::process::exit(70);
            },
        );
        Arc::new(Self {
            volumes: std::sync::RwLock::new(volumes),
            discover,
            limits,
            catalog: Mutex::new(Value::Null),
            index: Mutex::default(),
            refreshed: Mutex::default(),
            requested: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            wait: Duration::from_secs(60),
            fresh_wait: Duration::from_secs(600),
            remembered: Mutex::new(remembered),
            state_path,
            events: Events::default(),
            rescan: tokio::sync::Notify::new(),
            bytes_in: AtomicU64::new(0),
            batches_in: AtomicU64::new(0),
            filer,
        })
    }

    fn volumes(&self) -> Vec<Volume> {
        self.volumes
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Remember every volume in use, and what each root scanned whole holds;
    /// the state file is written when that changes.
    fn remember(&self, roots: &[Value]) -> Result<(), String> {
        let mut r = lock(&self.remembered);
        let before = r.clone();
        for v in self.volumes() {
            let k = r.volumes.entry(v.name.clone()).or_insert(state::Known {
                path: v.path.clone(),
                writable: v.writable,
                require_mount: v.require_mount,
                held: Value::Null,
            });
            k.path = v.path;
            k.writable = v.writable;
            k.require_mount = v.require_mount;
        }
        for root in roots
            .iter()
            .filter(|x| x["present"] == true && x["errors"].as_array().is_some_and(Vec::is_empty))
        {
            if let Some(k) = root["name"].as_str().and_then(|n| r.volumes.get_mut(n)) {
                k.held = state::held(root);
            }
        }
        if *r == before {
            return Ok(());
        }
        state::save(&self.state_path, &r)
    }

    /// Every writable volume's share of an incoming batch.
    fn parts(&self, provider: &str, batch: &str) -> Vec<ingest::Part> {
        self.volumes()
            .iter()
            .filter(|v| v.writable && volumes::present(v).is_ok())
            .map(|v| v.path.join(provider))
            .map(|root| ingest::Part {
                inc: root.join(ingest::INCOMING).join(batch),
                root,
            })
            .filter(|p| p.inc.is_dir())
            .collect()
    }

    /// Where an incoming file goes (decision 0007: each file whole on the
    /// volume with the most room): the volume already holding it, or a part
    /// of it, if any; otherwise the writable volume with the most free space
    /// that stays below the critical threshold with `size` more bytes.
    fn place(
        &self,
        provider: &str,
        batch: &str,
        file: &str,
        size: Option<u64>,
    ) -> Result<PathBuf, String> {
        if let Some(p) = self
            .parts(provider, batch)
            .into_iter()
            .find(|p| p.inc.join(file).exists() || p.inc.join(format!("{file}.partial")).exists())
        {
            return Ok(p.root);
        }
        let need = size.unwrap_or(0);
        self.volumes()
            .iter()
            .filter(|v| v.writable && volumes::present(v).is_ok())
            .filter_map(|v| {
                let total = fs4::total_space(&v.path).ok()?;
                let free = fs4::available_space(&v.path).ok()?.checked_sub(need)?;
                let used = 100.0 * (1.0 - free as f64 / total.max(1) as f64);
                (used < self.limits.critical).then(|| (free, v.path.join(provider)))
            })
            .max_by_key(|(free, _)| *free)
            .map(|(_, root)| root)
            .ok_or_else(|| {
                format!(
                    "no writable volume has room for {file}{} below {}% used",
                    size.map_or(String::new(), |s| format!(" ({s} bytes)")),
                    self.limits.critical
                )
            })
    }
}

#[derive(Deserialize)]
struct Expect {
    size: Option<u64>,
    sha256: Option<String>,
}

async fn upload(
    AxState(store): AxState<Shared>,
    UrlPath((provider, batch, file)): UrlPath<(String, String, String)>,
    Query(expect): Query<Expect>,
    body: Body,
) -> Reply {
    let Some(pdir) = ingest::provider_dir(&provider) else {
        return refused(
            StatusCode::NOT_FOUND,
            format!("unknown provider {provider}"),
        );
    };
    if !ingest::safe(&batch) || !ingest::safe(&file) {
        return refused(StatusCode::BAD_REQUEST, "unsafe batch or file name");
    }
    let root = match store.place(pdir, &batch, &file, expect.size) {
        Ok(r) => r,
        Err(e) => return refused(StatusCode::INSUFFICIENT_STORAGE, e),
    };
    let dir = root.join(ingest::INCOMING).join(&batch);
    let mut up = match tokio::task::block_in_place(|| ingest::Upload::start(&dir, &file)) {
        Ok(u) => u,
        Err(e) => return refused(StatusCode::INTERNAL_SERVER_ERROR, e),
    };
    let mut stream = body.into_data_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = match chunk {
            Ok(c) => c,
            Err(e) => return refused(StatusCode::BAD_REQUEST, format!("upload interrupted: {e}")),
        };
        store
            .bytes_in
            .fetch_add(chunk.len() as u64, Ordering::Relaxed);
        if let Err(e) = tokio::task::block_in_place(|| up.write(&chunk)) {
            return refused(StatusCode::INTERNAL_SERVER_ERROR, e);
        }
    }
    match tokio::task::block_in_place(|| up.finish(expect.size, expect.sha256.as_deref())) {
        Ok((size, sha)) => {
            let text = format!("received {pdir}/{batch}/{file} ({size} bytes)");
            store.events.push(
                text,
                json!({ "file": file, "batch": batch, "bytes": size, "sha256": sha }).to_string(),
            );
            reply(
                StatusCode::OK,
                json!({ "file": file, "bytes": size, "sha256": sha }),
            )
        }
        Err(e) => refused(StatusCode::UNPROCESSABLE_ENTITY, e),
    }
}

/// Batches waiting to be incorporated, with their files.
async fn list_incoming(AxState(store): AxState<Shared>) -> Json<Value> {
    let mut out = Vec::new();
    for v in store.volumes().iter().filter(|v| v.writable) {
        let Ok(providers) = std::fs::read_dir(&v.path) else {
            continue;
        };
        for p in providers.flatten() {
            let inc = p.path().join(ingest::INCOMING);
            let Ok(batches) = std::fs::read_dir(&inc) else {
                continue;
            };
            for b in batches.flatten() {
                let files: Vec<Value> = std::fs::read_dir(b.path())
                    .into_iter()
                    .flatten()
                    .flatten()
                    .filter(|f| !f.file_name().to_string_lossy().ends_with(".sha256"))
                    .map(|f| json!({ "name": f.file_name().to_string_lossy(), "bytes": f.metadata().map(|m| m.len()).unwrap_or(0) }))
                    .collect();
                out.push(json!({ "volume": v.name, "provider": p.file_name().to_string_lossy(), "batch": b.file_name().to_string_lossy(), "files": files }));
            }
        }
    }
    Json(json!({ "batches": out }))
}

#[derive(Deserialize)]
struct IncorporateQuery {
    /// A batch bought as a list of contracts of this parent (`SPY.OPT`) is
    /// filed as partial days (decision 0018).
    parent: Option<String>,
}

async fn incorporate(
    AxState(store): AxState<Shared>,
    UrlPath((provider, batch)): UrlPath<(String, String)>,
    Query(q): Query<IncorporateQuery>,
) -> Reply {
    if q.parent.as_deref().is_some_and(|p| !ingest::safe(p)) {
        return refused(StatusCode::BAD_REQUEST, "unsafe parent symbol");
    }
    let Some(pdir) = ingest::provider_dir(&provider) else {
        return refused(
            StatusCode::NOT_FOUND,
            format!("unknown provider {provider}"),
        );
    };
    if !ingest::safe(&batch) {
        return refused(StatusCode::BAD_REQUEST, "unsafe batch name");
    }
    let parts = store.parts(pdir, &batch);
    if parts.is_empty() {
        return refused(StatusCode::NOT_FOUND, format!("no incoming batch {batch}"));
    }
    // The same provider on every other volume, read-only ones included, is
    // checked for the same days.
    let elsewhere: Vec<PathBuf> = store
        .volumes()
        .iter()
        .map(|v| v.path.join(pdir))
        .filter(|p| !parts.iter().any(|x| x.root == *p))
        .collect();
    let read_only: Vec<PathBuf> = store
        .volumes()
        .iter()
        .filter(|v| !v.writable)
        .map(|v| v.path.join(pdir))
        .collect();
    let targets = target_folders(&parts, &elsewhere, q.parent.as_deref());
    let today = today();
    let b = batch.clone();
    let result = match q.parent {
        Some(parent) => {
            let f = partial::Filing {
                parts,
                elsewhere,
                id: b,
                today,
                parent: Some(parent),
            };
            store.file_partial(f, targets).await
        }
        None => {
            let f = move || ingest::incorporate_on(&parts, &elsewhere, &read_only, &b, &today);
            store.file_alone(targets, f).await
        }
    };
    match result {
        Ok(Ok(summary)) => {
            store.batches_in.fetch_add(1, Ordering::Relaxed);
            let n = summary["files"]
                .as_array()
                .or_else(|| summary["days"].as_array())
                .map_or(0, Vec::len);
            store.events.push(
                format!("incorporated {pdir}/{batch}: {n} files"),
                summary.to_string(),
            );
            // Days refused alone: the rest are filed, the batch waits.
            match summary["refused"].as_array().map_or(0, Vec::len) {
                0 => reply(StatusCode::OK, summary),
                r => {
                    let mut summary = summary;
                    summary["error"] = json!(format!(
                        "{r} days refused, {n} filed; the batch stays in incoming"
                    ));
                    reply(StatusCode::CONFLICT, summary)
                }
            }
        }
        Ok(Err(e)) => {
            store
                .events
                .push(format!("refused {pdir}/{batch}: {e}"), e.clone());
            refused(StatusCode::CONFLICT, e)
        }
        Err(e) => refused(filing_failed(&e), e),
    }
}

/// File an import (decision 0018): DBN that didn't come from a provider
/// batch, waiting in `.incoming/<id>` with its `import.json`.
async fn import(
    AxState(store): AxState<Shared>,
    UrlPath((provider, id)): UrlPath<(String, String)>,
) -> Reply {
    let Some(pdir) = ingest::provider_dir(&provider) else {
        return refused(
            StatusCode::NOT_FOUND,
            format!("unknown provider {provider}"),
        );
    };
    if !ingest::safe(&id) {
        return refused(StatusCode::BAD_REQUEST, "unsafe import name");
    }
    let parts = store.parts(pdir, &id);
    if parts.is_empty() {
        return refused(StatusCode::NOT_FOUND, format!("no incoming import {id}"));
    }
    let elsewhere: Vec<PathBuf> = store
        .volumes()
        .iter()
        .map(|v| v.path.join(pdir))
        .filter(|p| !parts.iter().any(|x| x.root == *p))
        .collect();
    let targets = target_folders(&parts, &elsewhere, None);
    let f = partial::Filing {
        parts,
        elsewhere,
        id: id.clone(),
        today: today(),
        parent: None,
    };
    match store.file_partial(f, targets).await {
        Ok(Ok(summary)) => {
            let n = summary["days"].as_array().map_or(0, Vec::len);
            store.events.push(
                format!("imported {pdir}/{id}: {n} days"),
                summary.to_string(),
            );
            reply(StatusCode::OK, summary)
        }
        Ok(Err(e)) => {
            store
                .events
                .push(format!("refused import {pdir}/{id}: {e}"), e.clone());
            refused(StatusCode::CONFLICT, e)
        }
        Err(e) => refused(filing_failed(&e), e),
    }
}

/// The last scan, or `503` until the first is done.
/// With `fresh=1`, from a scan started after the request.
async fn catalog(AxState(store): AxState<Shared>, Query(q): Query<Fresh>) -> Reply {
    if lock(&store.catalog).is_null() {
        return refused(
            StatusCode::SERVICE_UNAVAILABLE,
            "the store is still scanning",
        );
    }
    if let Err(r) = store.settled(q.fresh()).await {
        return r;
    }
    reply(StatusCode::OK, lock(&store.catalog).clone())
}

/// `fresh=1`: answer from a scan started after the request (decision 0019:
/// a purchase asks this way, so data copied onto a drive since the last
/// scan counts).
#[derive(Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
struct Fresh {
    fresh: Option<String>,
}

impl Fresh {
    fn fresh(&self) -> bool {
        matches!(self.fresh.as_deref(), Some("1" | "true"))
    }
}

#[derive(Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
struct CoverageQuery {
    provider: Option<String>,
    dataset: String,
    symbol: Option<String>,
    schema: String,
    start: String,
    end: String,
    fresh: Option<String>,
}

/// Each held day of a provider's dataset, symbol and schema: complete, or
/// partial with the contracts and hours it holds (decision 0018), on any
/// volume, filed or not, and the remembered volumes that are missing
/// (decision 0019).
async fn coverage(AxState(store): AxState<Shared>, Query(q): Query<CoverageQuery>) -> Reply {
    if lock(&store.catalog).is_null() {
        return refused(
            StatusCode::SERVICE_UNAVAILABLE,
            "the store is still scanning",
        );
    }
    let Some(provider) = q.provider.as_deref() else {
        return refused(
            StatusCode::BAD_REQUEST,
            "give the provider (provider=databento or provider=alpaca)",
        );
    };
    let Some(pdir) = ingest::provider_dir(provider) else {
        return refused(
            StatusCode::NOT_FOUND,
            format!("unknown provider {provider}"),
        );
    };
    let provider = pdir.to_ascii_lowercase();
    if !ingest::safe(&q.dataset)
        || !ingest::safe(&q.schema)
        || q.symbol.as_deref().is_some_and(|s| !ingest::safe(s))
    {
        return refused(StatusCode::BAD_REQUEST, "unsafe dataset, symbol or schema");
    }
    // A day just filed is in the scan after it: wait for that (bounded),
    // or, with `fresh`, for a scan started now.
    let fresh = Fresh {
        fresh: q.fresh.clone(),
    }
    .fresh();
    if let Err(r) = store.settled(fresh).await {
        return r;
    }
    let index = lock(&store.index).clone();
    if let Some(e) = index.errors.first() {
        return refused(StatusCode::INTERNAL_SERVER_ERROR, e.clone());
    }
    // Missing: not there now, or found missing by the last scan (unreadable,
    // or emptied of what it held).
    let (roots, gone): (Vec<Volume>, Vec<Volume>) = store
        .volumes()
        .into_iter()
        .partition(|v| volumes::present(v).is_ok());
    let mut missing: Vec<String> = gone.into_iter().map(|v| v.name).collect();
    for n in lock(&store.catalog)["missing_volumes"]
        .as_array()
        .into_iter()
        .flatten()
    {
        if let Some(n) = n.as_str().filter(|n| !missing.iter().any(|m| m == n)) {
            missing.push(n.to_owned());
        }
    }
    missing.sort();
    let missing = json!(missing);
    // Only volumes there now.
    let names: Vec<String> = roots.into_iter().map(|v| v.name).collect();
    let (scanned_at, updated_at) = (index.scanned_at, index.updated_at);
    let result = tokio::task::spawn_blocking(move || {
        let found: Vec<(&str, &[catalog::Place])> = index
            .volumes
            .iter()
            .filter(|(n, _)| names.contains(n))
            .map(|(n, p)| (n.as_str(), p.as_slice()))
            .collect();
        partial::coverage_in(
            &found,
            &provider,
            &q.dataset,
            q.symbol.as_deref(),
            &q.schema,
            &q.start,
            &q.end,
        )
    })
    .await;
    match result {
        Ok(Ok(days)) => reply(
            StatusCode::OK,
            json!({ "days": days, "missing_volumes": missing, "scanned_at": scanned_at, "updated_at": updated_at }),
        ),
        Ok(Err(e)) => refused(StatusCode::INTERNAL_SERVER_ERROR, e),
        Err(e) => refused(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

/// Every batch the volumes hold for a provider, by batch ID: `filed` (its
/// receipts are in `provenance/`), `incoming` (uploaded, not yet filed) or
/// `folder` (a provider download folder, as at the top of a volume).
async fn batches(AxState(store): AxState<Shared>, UrlPath(provider): UrlPath<String>) -> Reply {
    let Some(pdir) = ingest::provider_dir(&provider) else {
        return refused(
            StatusCode::NOT_FOUND,
            format!("unknown provider {provider}"),
        );
    };
    let volumes = store.volumes();
    let found = tokio::task::spawn_blocking(move || {
        let mut out = serde_json::Map::new();
        let dirs = |p: &std::path::Path| -> Vec<std::fs::DirEntry> {
            std::fs::read_dir(p)
                .into_iter()
                .flatten()
                .flatten()
                .filter(|e| e.path().is_dir())
                .collect()
        };
        for v in &volumes {
            let root = v.path.join(pdir);
            // Raw download folders first, so a filed copy wins.
            let mut parents = vec![v.path.clone(), root.clone()];
            parents.extend(
                dirs(&v.path)
                    .iter()
                    .map(std::fs::DirEntry::path)
                    .filter(|p| *p != root),
            );
            for parent in parents {
                for e in dirs(&parent) {
                    if e.path().join("manifest.json").is_file()
                        && e.path().join("metadata.json").is_file()
                    {
                        let id = e.file_name().to_string_lossy().into_owned();
                        out.entry(id).or_insert(
                            json!({ "volume": v.name, "how": "folder", "path": e.path() }),
                        );
                    }
                }
            }
            for (dir, how) in [
                (root.join(ingest::INCOMING), "incoming"),
                (root.join("provenance"), "filed"),
            ] {
                for e in dirs(&dir) {
                    let id = e.file_name().to_string_lossy().into_owned();
                    out.insert(
                        id,
                        json!({ "volume": v.name, "how": how, "path": e.path() }),
                    );
                }
            }
        }
        Value::Object(out)
    })
    .await
    .unwrap_or_default();
    reply(
        StatusCode::OK,
        json!({ "provider": pdir, "batches": found }),
    )
}

#[derive(Deserialize)]
struct AdoptRequest {
    path: PathBuf,
}

/// File a download folder already on a volume, where it is. It must sit at
/// the top of a writable volume, or one level down (such as a downloader's
/// own folder), and be named for its batch.
async fn adopt(
    AxState(store): AxState<Shared>,
    UrlPath(provider): UrlPath<String>,
    Json(req): Json<AdoptRequest>,
) -> Reply {
    let Some(pdir) = ingest::provider_dir(&provider) else {
        return refused(
            StatusCode::NOT_FOUND,
            format!("unknown provider {provider}"),
        );
    };
    let Ok(dir) = req.path.canonicalize() else {
        return refused(
            StatusCode::NOT_FOUND,
            format!("no folder {}", req.path.display()),
        );
    };
    let all = store.volumes();
    let Some(volume) = all.iter().find(|v| {
        v.path.canonicalize().is_ok_and(|vp| {
            dir.parent() == Some(vp.as_path())
                || dir.parent().and_then(Path::parent) == Some(vp.as_path())
        })
    }) else {
        return refused(
            StatusCode::BAD_REQUEST,
            "the folder must be at the top of a volume, or one level down",
        );
    };
    if !volume.writable {
        return refused(
            StatusCode::FORBIDDEN,
            format!("{} is read-only", volume.name),
        );
    }
    if !dir.join("manifest.json").is_file() || !dir.join("metadata.json").is_file() {
        return refused(
            StatusCode::BAD_REQUEST,
            "not a provider download folder (no manifest.json and metadata.json)",
        );
    }
    let root = volume.path.join(pdir);
    let elsewhere: Vec<PathBuf> = store
        .volumes()
        .iter()
        .map(|v| v.path.join(pdir))
        .filter(|p| *p != root)
        .collect();
    let part = ingest::Part {
        inc: dir.clone(),
        root: root.clone(),
    };
    let targets = target_folders(std::slice::from_ref(&part), &elsewhere, None);
    let today = today();
    let d = dir.clone();
    // In its turn, in a task of its own (`file_alone`): a client that goes away
    // can't end the turn while it writes.
    match store
        .file_alone(targets, move || {
            ingest::adopt(&d, &root, &elsewhere, &today)
        })
        .await
    {
        Ok(Ok(summary)) => {
            store.batches_in.fetch_add(1, Ordering::Relaxed);
            let n = summary["files"].as_array().map_or(0, Vec::len);
            store.events.push(
                format!("filed {} in place: {n} files", dir.display()),
                summary.to_string(),
            );
            reply(StatusCode::OK, summary)
        }
        Ok(Err(e)) => {
            store
                .events
                .push(format!("refused {}: {e}", dir.display()), e.clone());
            refused(StatusCode::CONFLICT, e)
        }
        Err(e) => refused(filing_failed(&e), e),
    }
}

#[derive(Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
struct ListFiles {
    pattern: Option<String>,
    provider: Option<String>,
    limit: Option<usize>,
}

/// The DBN files the last scan found, on the volumes there now, whose
/// logical name matches `pattern` (by default all), in `provider`'s folder
/// if given, at most `limit` of them; `503` until the first scan is done.
/// The volumes aren't walked for it.
async fn list_files(AxState(store): AxState<Shared>, Query(q): Query<ListFiles>) -> Reply {
    if lock(&store.catalog).is_null() {
        return refused(
            StatusCode::SERVICE_UNAVAILABLE,
            "the store is still scanning",
        );
    }
    let index = lock(&store.index).clone();
    let present: Vec<String> = store
        .volumes()
        .into_iter()
        .filter(|v| volumes::present(v).is_ok())
        .map(|v| v.name)
        .collect();
    let pattern = q.pattern.unwrap_or_else(|| "*".to_owned());
    let limit = q.limit.unwrap_or(read::MAX_LISTED).min(read::MAX_LISTED);
    let provider = q.provider;
    match tokio::task::spawn_blocking(move || {
        let found: Vec<(&str, &[catalog::Served])> = index
            .files
            .iter()
            .filter(|(n, _)| present.contains(n))
            .map(|(n, f)| (n.as_str(), f.as_slice()))
            .collect();
        let mut v = read::list(&found, &pattern, provider.as_deref(), limit);
        v["scanned_at"] = json!(index.scanned_at);
        v["updated_at"] = json!(index.updated_at);
        v
    })
    .await
    {
        Ok(v) => reply(StatusCode::OK, v),
        Err(e) => refused(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

/// Why a file that can't be found may still be there: its volume isn't
/// mounted, or found missing by the last scan, or hasn't been scanned yet.
/// `None` when it is there and scanned, so the file really is absent.
fn away(store: &Store, v: &Volume) -> Option<String> {
    if let Err(e) = volumes::present(v) {
        return Some(format!(
            "volume {} is not mounted ({e}); ask again when it is",
            v.name
        ));
    }
    let (first_done, missing) = {
        let c = lock(&store.catalog);
        let missing = c["missing_volumes"]
            .as_array()
            .is_some_and(|m| m.iter().any(|n| n == v.name.as_str()));
        (!c.is_null(), missing)
    };
    if missing {
        return Some(format!(
            "volume {} is missing (its errors are in /catalog); ask again when it is read",
            v.name
        ));
    }
    let scanned = first_done && lock(&store.index).files.iter().any(|(n, _)| *n == v.name);
    (!scanned).then(|| format!("volume {} is being scanned; ask again shortly", v.name))
}

/// One DBN file, as it is on the volume.
async fn send_file(
    AxState(store): AxState<Shared>,
    UrlPath((volume, path)): UrlPath<(String, String)>,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;
    use tokio::io::AsyncReadExt as _;

    let started = std::time::Instant::now();
    let name = format!("{volume}/{path}");
    let volumes = store.volumes();
    // Nothing is served from a volume that isn't mounted (an empty folder
    // left behind holds nothing of it).
    if let Some(v) = volumes.iter().find(|v| v.name == volume)
        && volumes::present(v).is_err()
        && let Some(why) = away(&store, v)
    {
        return refused(StatusCode::SERVICE_UNAVAILABLE, why).into_response();
    }
    // A file not found on a volume found missing, or not yet scanned, may
    // be there: only a scanned volume's absent file is a 404.
    let s = store.clone();
    let found = tokio::task::spawn_blocking(move || {
        read::locate(&volumes, &volume, &path).map_err(|(code, why)| {
            let v = volumes.iter().find(|v| v.name == volume);
            match v.filter(|_| code == 404).and_then(|v| away(&s, v)) {
                Some(why) => (503, why),
                None => (code, why),
            }
        })
    })
    .await;
    let file = match found {
        Ok(Ok(f)) => f,
        Ok(Err((code, why))) => {
            let code = StatusCode::from_u16(code).unwrap_or(StatusCode::BAD_REQUEST);
            return refused(code, why).into_response();
        }
        Err(e) => return refused(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };
    let found_in = started.elapsed();
    let opened = tokio::fs::File::open(&file).await;
    let (f, len) = match opened {
        Ok(f) => match f.metadata().await {
            Ok(m) => (f, m.len()),
            Err(e) => {
                return refused(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
            }
        },
        Err(e) => return refused(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };
    // Each download is logged once sent, with how long it took.
    let chunks = futures_util::stream::unfold((f, 0u64), move |(mut f, sent)| {
        let name = name.clone();
        async move {
            let mut buf = vec![0u8; 256 * 1024];
            match f.read(&mut buf).await {
                Ok(0) => {
                    eprintln!(
                        "store: sent {name}: {sent} bytes in {:.3}s (found in {:.3}s)",
                        started.elapsed().as_secs_f64(),
                        found_in.as_secs_f64()
                    );
                    None
                }
                Ok(n) => {
                    buf.truncate(n);
                    let bytes = axum::body::Bytes::from(buf);
                    Some((Ok::<_, std::io::Error>(bytes), (f, sent + n as u64)))
                }
                Err(e) => {
                    eprintln!(
                        "store: sending {name} failed after {sent} bytes in {:.3}s: {e}",
                        started.elapsed().as_secs_f64()
                    );
                    Some((Err(e), (f, sent)))
                }
            }
        }
    });
    (
        [
            (
                axum::http::header::CONTENT_TYPE,
                "application/octet-stream".to_owned(),
            ),
            (axum::http::header::CONTENT_LENGTH, len.to_string()),
        ],
        Body::from_stream(chunks),
    )
        .into_response()
}

#[derive(Deserialize)]
struct ApproveRequest {
    path: PathBuf,
    name: String,
}

/// Approve a drive found in the mount directory: write its marker and start
/// using it. The console asks the operator first.
async fn approve(AxState(store): AxState<Shared>, Json(req): Json<ApproveRequest>) -> Reply {
    let Some(dir) = store.discover.clone() else {
        return refused(
            StatusCode::NOT_FOUND,
            "this store doesn't look for new drives (no --discover)",
        );
    };
    let taken: Vec<String> = store.volumes().into_iter().map(|v| v.name).collect();
    let (path, name) = (req.path.clone(), req.name.trim().to_owned());
    let result =
        tokio::task::spawn_blocking(move || volumes::approve(&dir, &path, &name, &taken)).await;
    match result {
        Ok(Ok(v)) => {
            store.events.push(
                format!("approved drive {} at {}", v.name, v.path.display()),
                json!({ "name": v.name, "path": v.path }).to_string(),
            );
            store
                .volumes
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(v.clone());
            store.request_rescan();
            reply(StatusCode::OK, json!({ "name": v.name, "path": v.path }))
        }
        Ok(Err(e)) => refused(StatusCode::BAD_REQUEST, e),
        Err(e) => refused(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

#[derive(Deserialize)]
struct IgnoreRequest {
    path: PathBuf,
    reason: Option<String>,
    by: Option<String>,
}

/// The volume `name`, and `path` on it (absolute, or relative to it) as an
/// absolute path strictly inside it.
fn on_volume(store: &Store, name: &str, path: &Path) -> Result<(Volume, PathBuf), Reply> {
    let Some(v) = store.volumes().into_iter().find(|v| v.name == name) else {
        return Err(refused(StatusCode::NOT_FOUND, format!("no volume {name}")));
    };
    let full = if path.is_absolute() {
        path.to_path_buf()
    } else {
        v.path.join(path)
    };
    let inside = full.strip_prefix(&v.path).is_ok_and(|rel| {
        rel.components().next().is_some()
            && rel
                .components()
                .all(|c| matches!(c, std::path::Component::Normal(_)))
    });
    if !inside {
        return Err(refused(
            StatusCode::BAD_REQUEST,
            format!(
                "{} is not a path inside {name} ({})",
                path.display(),
                v.path.display()
            ),
        ));
    }
    Ok((v, full))
}

/// Skip a path on a volume that the store can't read (decision 0019): an
/// operator's decision, recorded with who, when and why. The path is then
/// never counted, nor an error, until it is un-ignored; what it holds is
/// taken as not held.
async fn ignore(
    AxState(store): AxState<Shared>,
    UrlPath(name): UrlPath<String>,
    Json(req): Json<IgnoreRequest>,
) -> Reply {
    let (_, path) = match on_volume(&store, &name, &req.path) {
        Ok(x) => x,
        Err(r) => return r,
    };
    let Some(reason) = req.reason.filter(|r| !r.trim().is_empty()) else {
        return refused(StatusCode::BAD_REQUEST, "say why (\"reason\")");
    };
    let record = json!({
        "volume": name, "path": path, "reason": reason, "by": req.by, "at": unix_seconds(),
    });
    {
        let mut r = lock(&store.remembered);
        if r.ignored
            .iter()
            .any(|i| i["volume"] == name.as_str() && i["path"] == record["path"])
        {
            return refused(
                StatusCode::CONFLICT,
                format!("{} is already ignored", path.display()),
            );
        }
        r.ignored.push(record.clone());
        if let Err(e) = state::save(&store.state_path, &r) {
            r.ignored.pop();
            return refused(StatusCode::INTERNAL_SERVER_ERROR, e);
        }
    }
    store.events.push(
        format!("ignoring {} on {name}", path.display()),
        record.to_string(),
    );
    store.request_rescan();
    reply(StatusCode::OK, record)
}

/// Stop skipping a path: it is read, and counted or an error, again.
async fn unignore(
    AxState(store): AxState<Shared>,
    UrlPath(name): UrlPath<String>,
    Json(req): Json<IgnoreRequest>,
) -> Reply {
    let (_, path) = match on_volume(&store, &name, &req.path) {
        Ok(x) => x,
        Err(r) => return r,
    };
    let removed = {
        let mut r = lock(&store.remembered);
        let before = r.ignored.clone();
        r.ignored
            .retain(|i| !(i["volume"] == name.as_str() && i["path"] == json!(path)));
        if r.ignored.len() == before.len() {
            return refused(
                StatusCode::NOT_FOUND,
                format!("{} isn't ignored", path.display()),
            );
        }
        if let Err(e) = state::save(&store.state_path, &r) {
            r.ignored = before;
            return refused(StatusCode::INTERNAL_SERVER_ERROR, e);
        }
        json!({ "volume": name, "path": path })
    };
    store.events.push(
        format!("no longer ignoring {} on {name}", path.display()),
        removed.to_string(),
    );
    store.request_rescan();
    reply(StatusCode::OK, removed)
}

/// Why a volume that held datasets and now holds none is missing, and what
/// to do about it.
fn emptied(v: &Volume) -> String {
    format!(
        "{} held datasets when last read, and now holds none; mount the right drive, or if it really is empty now, POST /volumes/{}/accept-empty",
        v.path.display(),
        v.name
    )
}

#[derive(Deserialize, Default)]
struct AcceptEmpty {
    /// Who says so, for the record.
    by: Option<String>,
}

/// Accept that a mounted volume which held datasets is now empty (decision
/// 0019): it is no longer missing for that, and the state file keeps a
/// record of who said so, when, and what it held before. Only a volume that
/// is mounted and scans with no datasets, and no errors, is accepted.
async fn accept_empty(
    AxState(store): AxState<Shared>,
    UrlPath(name): UrlPath<String>,
    body: axum::body::Bytes,
) -> Reply {
    let req: AcceptEmpty = if body.is_empty() {
        AcceptEmpty::default()
    } else {
        match serde_json::from_slice(&body) {
            Ok(r) => r,
            Err(e) => return refused(StatusCode::BAD_REQUEST, e.to_string()),
        }
    };
    let Some(volume) = store.volumes().into_iter().find(|v| v.name == name) else {
        return refused(StatusCode::NOT_FOUND, format!("no volume {name}"));
    };
    if let Err(e) = volumes::present(&volume) {
        return refused(StatusCode::CONFLICT, e);
    }
    let (path, limits) = (volume.path.clone(), store.limits);
    let skip = store.ignored_paths(&name);
    let root = match tokio::task::spawn_blocking(move || {
        catalog::scan_volume(&name, &path, limits, &skip).0
    })
    .await
    {
        Ok(r) => r,
        Err(e) => return refused(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };
    if root["datasets"].as_array().is_some_and(|d| !d.is_empty())
        || root["errors"].as_array().is_some_and(|e| !e.is_empty())
    {
        return refused(
            StatusCode::CONFLICT,
            format!("{} doesn't scan empty: {}", volume.name, root["errors"]),
        );
    }
    let record = {
        let mut r = lock(&store.remembered);
        let Some(k) = r.volumes.get_mut(&volume.name) else {
            return refused(StatusCode::NOT_FOUND, format!("no volume {}", volume.name));
        };
        let before = std::mem::replace(&mut k.held, json!({}));
        let record = json!({
            "name": volume.name, "path": volume.path, "accepted_at": unix_seconds(),
            "by": req.by, "held_before": before,
        });
        r.accepted_empty.push(record.clone());
        if let Err(e) = state::save(&store.state_path, &r) {
            r.accepted_empty.pop();
            if let Some(k) = r.volumes.get_mut(&volume.name) {
                k.held = before;
            }
            return refused(StatusCode::INTERNAL_SERVER_ERROR, e);
        }
        record
    };
    {
        let reason = emptied(&volume);
        let mut c = lock(&store.catalog);
        for root in c["roots"].as_array_mut().into_iter().flatten() {
            if root["name"] == volume.name.as_str()
                && let Some(e) = root["errors"].as_array_mut()
            {
                e.retain(|x| x != reason.as_str());
            }
        }
        if let Some(m) = c["missing_volumes"].as_array_mut() {
            m.retain(|x| x != volume.name.as_str());
        }
    }
    store.events.push(
        format!("accepted volume {} as empty", volume.name),
        record.to_string(),
    );
    store.request_rescan();
    reply(StatusCode::OK, record)
}

/// Record days' conditions from the provider's API (decision 0002): `[{
/// dataset, day, state, last_modified? }]`, for every schema and symbol of
/// the dataset held that day. Answers what changed.
async fn post_conditions(
    AxState(store): AxState<Shared>,
    UrlPath(provider): UrlPath<String>,
    Json(body): Json<Vec<Value>>,
) -> Reply {
    let Some(pdir) = ingest::provider_dir(&provider) else {
        return refused(
            StatusCode::NOT_FOUND,
            format!("unknown provider {provider}"),
        );
    };
    let mut rows: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    for r in body {
        let Some(dataset) = r["dataset"].as_str().filter(|d| ingest::safe(d)) else {
            return refused(StatusCode::BAD_REQUEST, format!("no usable dataset in {r}"));
        };
        rows.entry(dataset.to_owned()).or_default().push(r);
    }
    let mut parsed = BTreeMap::new();
    for (dataset, list) in rows {
        match conditions::parse(&json!(list)) {
            Ok(p) => parsed.insert(dataset, p),
            Err(e) => return refused(StatusCode::BAD_REQUEST, e),
        };
    }
    let provider = pdir.to_ascii_lowercase();
    apply_conditions(&store, move |index, out| {
        conditions::apply(index, &provider, &parsed, "provider api", out)
    })
    .await
}

/// Record, once, the conditions in every `condition.json` the store keeps in
/// `provenance/`, as from their batches.
async fn backfill_conditions(AxState(store): AxState<Shared>) -> Reply {
    let volumes = store.volumes();
    let (mut batches, mut rows_read, mut unknown) = (0, 0, Vec::new());
    let (code, Json(mut body)) = apply_conditions(&store, |index, out| {
        for v in volumes.iter().filter(|v| volumes::present(v).is_ok()) {
            for top in catalog::read_entries(&v.path)? {
                let Some(provider) = catalog::provider_name(&catalog_name(&top)) else {
                    continue;
                };
                for (batch, dataset, rows) in conditions::kept(&top)? {
                    let Some(dataset) = dataset else {
                        unknown.push(batch);
                        continue;
                    };
                    batches += 1;
                    rows_read += rows.len();
                    let rows = BTreeMap::from([(dataset, rows)]);
                    conditions::apply(index, &provider, &rows, &format!("batch {batch}"), out)?;
                }
            }
        }
        Ok(())
    })
    .await;
    if code == StatusCode::OK {
        body["batches"] = json!(batches);
        body["rows"] = json!(rows_read);
        body["unknown_dataset"] = json!(unknown);
    }
    (code, Json(body))
}

fn catalog_name(p: &Path) -> String {
    p.file_name()
        .map_or_else(String::new, |n| n.to_string_lossy().into_owned())
}

/// Apply conditions to the days the last scan found, after any rescan a
/// filing asked for, one filing at a time; answers `{ changed, unchanged,
/// not_held }`.
async fn apply_conditions(
    store: &Shared,
    f: impl FnOnce(&[(String, Vec<catalog::Place>)], &mut conditions::Applied) -> Result<(), String>,
) -> Reply {
    if lock(&store.catalog).is_null() {
        return refused(
            StatusCode::SERVICE_UNAVAILABLE,
            "the store is still scanning",
        );
    }
    if let Err(r) = store.settled(false).await {
        return r;
    }
    let _turn = match store.filer.turn().await {
        Ok(t) => t,
        Err(r) => return refused(StatusCode::SERVICE_UNAVAILABLE, refusal(&r)),
    };
    let index = lock(&store.index).clone();
    let mut out = conditions::Applied::default();
    let result = tokio::task::block_in_place(|| f(&index.volumes, &mut out));
    if let Err(e) = result {
        // It may have written some folders before it stopped: read them all
        // again, so `/coverage` never answers from what they were.
        tokio::task::block_in_place(|| store.refresh(&index.days_folders()));
        return refused(StatusCode::INTERNAL_SERVER_ERROR, e);
    }
    if !out.changed.is_empty() {
        store.events.push(
            format!("recorded {} day conditions", out.changed.len()),
            json!(out.changed).to_string(),
        );
        let mut dirs: Vec<PathBuf> = out
            .changed
            .iter()
            .filter_map(|c| c["folder"].as_str().map(PathBuf::from))
            .collect();
        dirs.sort();
        dirs.dedup();
        tokio::task::block_in_place(|| store.refresh(&dirs));
    }
    reply(
        StatusCode::OK,
        json!({ "changed": out.changed, "unchanged": out.unchanged, "not_held": out.not_held }),
    )
}

/// Rebuild, once, the coverage of days merged before coverage was kept in
/// pieces (decision 0018), from the ledgers and receipts of what went into
/// each; a day whose inputs aren't all on record is left as it is, and
/// listed.
async fn rebuild_coverage(AxState(store): AxState<Shared>) -> Reply {
    if lock(&store.catalog).is_null() {
        return refused(
            StatusCode::SERVICE_UNAVAILABLE,
            "the store is still scanning",
        );
    }
    if let Err(r) = store.settled(false).await {
        return r;
    }
    let turn = match store.filer.turn().await {
        Ok(t) => t,
        Err(r) => return refused(StatusCode::SERVICE_UNAVAILABLE, refusal(&r)),
    };
    let volumes: Vec<Volume> = store
        .volumes()
        .into_iter()
        .filter(|v| volumes::present(v).is_ok())
        .collect();
    let roots: Vec<PathBuf> = volumes
        .iter()
        .flat_map(|v| catalog::read_entries(&v.path).unwrap_or_default())
        .filter(|p| p.is_dir() && catalog::provider_name(&catalog_name(p)).is_some())
        .collect();
    let index = lock(&store.index).clone();
    let folders = index.days_folders();
    let s = store.clone();
    // The turn, the rewrite and the reading again in one blocking task: a client
    // that goes away can't end the turn while it writes.
    let done = tokio::task::spawn_blocking(move || {
        let _turn = turn;
        let r = rebuild::rebuild(&roots, &folders);
        match &r {
            Ok(report) => {
                let dirs = rebuild::folders(report);
                if !dirs.is_empty() {
                    s.refresh(&dirs);
                }
            }
            // What it rewrote before it stopped is read again.
            Err(_) => s.refresh(&folders),
        }
        r
    });
    match done.await {
        Ok(Ok(report)) => {
            if !rebuild::folders(&report).is_empty() {
                store.events.push(
                    format!(
                        "rebuilt the coverage of {} days",
                        report["changed"].as_array().map_or(0, Vec::len)
                    ),
                    report.to_string(),
                );
            }
            reply(StatusCode::OK, report)
        }
        Ok(Err(e)) => refused(StatusCode::INTERNAL_SERVER_ERROR, e),
        Err(e) => refused(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

/// Forget a remembered volume that is away for good (decision 0019): it no
/// longer counts as missing, and the state file keeps a record of what it
/// held when last seen, and when it was retired. A volume that is mounted
/// isn't retired.
async fn retire(AxState(store): AxState<Shared>, UrlPath(name): UrlPath<String>) -> Reply {
    let volume = store
        .volumes()
        .into_iter()
        .find(|v| v.name == name)
        .or_else(|| {
            lock(&store.remembered).volumes.get(&name).map(|k| Volume {
                name: name.clone(),
                path: k.path.clone(),
                writable: k.writable,
                require_mount: k.require_mount,
            })
        });
    let Some(volume) = volume else {
        return refused(StatusCode::NOT_FOUND, format!("no volume {name}"));
    };
    let path = volume.path.clone();
    if volumes::present(&volume).is_ok() {
        return refused(
            StatusCode::CONFLICT,
            format!(
                "{name} is mounted at {}; only a volume away for good is retired",
                path.display()
            ),
        );
    }
    // Out of use first, so a scan meanwhile doesn't remember it again.
    let removed: Vec<Volume> = {
        let mut list = store
            .volumes
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (gone, kept) = list.drain(..).partition(|v| v.name == name);
        *list = kept;
        gone
    };
    let record = {
        let mut r = lock(&store.remembered);
        let known = r.volumes.remove(&name);
        let record = json!({
            "name": name, "path": path, "retired_at": unix_seconds(),
            "held": known.as_ref().map(|k| &k.held),
        });
        r.retired.push(record.clone());
        if let Err(e) = state::save(&store.state_path, &r) {
            r.retired.pop();
            if let Some(k) = known {
                r.volumes.insert(name, k);
            }
            store
                .volumes
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .extend(removed);
            return refused(StatusCode::INTERNAL_SERVER_ERROR, e);
        }
        record
    };
    {
        let mut c = lock(&store.catalog);
        for list in ["roots", "missing_volumes"] {
            if let Some(a) = c[list].as_array_mut() {
                a.retain(|x| x != name.as_str() && x["name"] != name.as_str());
            }
        }
    }
    store
        .events
        .push(format!("retired volume {name}"), record.to_string());
    store.request_rescan();
    reply(StatusCode::OK, record)
}

/// Today's UTC date, `YYYY-MM-DD`.
fn today() -> String {
    ingest::day_of((unix_seconds() * 1e9) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    const LIMITS: Thresholds = Thresholds {
        warning: 100.0,
        critical: 100.0,
        floor: 100.0,
    };

    fn scratch(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "tinytrader-store-test-main-{name}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// A store over these writable volumes, remembering them in `state`.
    fn store(volumes: &[(&str, &Path)], state: &Path) -> Shared {
        let volumes = volumes
            .iter()
            .map(|(n, p)| Volume {
                name: (*n).to_owned(),
                path: p.to_path_buf(),
                writable: true,
                require_mount: false,
            })
            .collect();
        let s = Store::new(
            volumes,
            None,
            LIMITS,
            state.to_path_buf(),
            state::load(state).unwrap(),
        );
        s.remember(&[]).unwrap();
        s
    }

    /// Serve a store on a free loopback port, rescanning when asked, as the
    /// store does.
    async fn serve_store(store: Shared, token: WriteToken) -> String {
        let s = store.clone();
        tokio::spawn(async move {
            loop {
                s.rescan.notified().await;
                scanned(&s).await;
            }
        });
        let app = router(store, token);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        addr
    }

    async fn scanned(store: &Shared) {
        scan(store, &Observer::new("store", "test")).await;
    }

    /// A store over one empty volume, scanned once, served on a free
    /// loopback port.
    async fn serve(token: WriteToken) -> (String, PathBuf) {
        let dir = scratch(&format!("auth-{}", token.required()));
        let vol = dir.join("v");
        std::fs::create_dir_all(&vol).unwrap();
        let s = store(&[("v", &vol)], &dir.join("state.json"));
        scanned(&s).await;
        (serve_store(s, token).await, vol)
    }

    /// One HTTP/1.1 request; the response's status code and body.
    async fn call(
        addr: &str,
        method: &str,
        path: &str,
        auth: Option<&str>,
        body: &str,
    ) -> (u16, String) {
        let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
        let auth = auth.map_or(String::new(), |a| format!("authorization: {a}\r\n"));
        let req = format!(
            "{method} {path} HTTP/1.1\r\nhost: {addr}\r\n{auth}content-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        s.write_all(req.as_bytes()).await.unwrap();
        let mut out = String::new();
        s.read_to_string(&mut out).await.unwrap();
        let code = out.split(' ').nth(1).unwrap().parse().unwrap();
        let body = out.split_once("\r\n\r\n").map_or("", |(_, b)| b).to_owned();
        (code, body)
    }

    async fn status(addr: &str, method: &str, path: &str, auth: Option<&str>, body: &str) -> u16 {
        call(addr, method, path, auth, body).await.0
    }

    /// A GET: its status code and JSON body.
    async fn get(addr: &str, path: &str) -> (u16, Value) {
        let (code, body) = call(addr, "GET", path, None, "").await;
        let v = serde_json::from_str(&body).unwrap_or(Value::Null);
        // Every answer of the documented reads fits the OpenAPI document.
        let p = path.split('?').next().unwrap_or(path);
        if ["/catalog", "/coverage", "/files"].contains(&p) && !v.is_null() {
            tinytrader_openapi::assert_conforms("store", "get", p, &code.to_string(), &v);
        }
        (code, v)
    }

    const COVERAGE: &str = "/coverage?provider=databento&dataset=OPRA.PILLAR&symbol=SPY.OPT&schema=cbbo-1m&start=2026-10-01&end=2026-10-09";

    const UPLOAD: &str = "/incoming/databento/B1/notes.txt?size=2";
    const APPROVE: &str = r#"{"path":"/nowhere","name":"x"}"#;

    #[tokio::test(flavor = "multi_thread")]
    async fn writes_need_the_token_when_one_is_set() {
        let (addr, dir) = serve(WriteToken::new("s3cret").unwrap()).await;
        let good = Some("Bearer s3cret");
        assert_eq!(status(&addr, "PUT", UPLOAD, None, "hi").await, 401);
        assert_eq!(
            status(&addr, "PUT", UPLOAD, Some("Bearer nope"), "hi").await,
            401
        );
        assert_eq!(status(&addr, "PUT", UPLOAD, good, "hi").await, 200);
        assert!(dir.join("Databento").exists(), "the upload arrived");
        for (path, body) in [
            ("/incorporate/databento/B1", ""),
            ("/import/databento/I1", ""),
            ("/adopt/databento", APPROVE),
            ("/volumes/approve", APPROVE),
            ("/volumes/v/accept-empty", ""),
            ("/volumes/v/retire", ""),
            ("/volumes/v/ignore", r#"{"path":"x","reason":"y"}"#),
            ("/conditions/databento", "[]"),
            ("/conditions/backfill", ""),
        ] {
            assert_eq!(status(&addr, "POST", path, None, body).await, 401, "{path}");
            // Past the guard, the store answers for itself (not 401).
            assert_ne!(status(&addr, "POST", path, good, body).await, 401, "{path}");
        }
        let unignore = r#"{"path":"x"}"#;
        assert_eq!(
            status(&addr, "DELETE", "/volumes/v/ignore", None, unignore).await,
            401
        );
        // Reads stay open.
        for path in [
            "/catalog",
            "/files",
            "/incoming",
            "/batches/databento",
            COVERAGE,
        ] {
            assert_eq!(status(&addr, "GET", path, None, "").await, 200, "{path}");
        }
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn without_a_token_writes_are_as_before() {
        let (addr, dir) = serve(WriteToken::open()).await;
        assert_eq!(status(&addr, "PUT", UPLOAD, None, "hi").await, 200);
        assert_eq!(
            status(&addr, "POST", "/volumes/approve", None, APPROVE).await,
            404
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn answers_503_until_the_first_scan() {
        let dir = scratch("first-scan");
        let s = store(&[("v", &dir)], &dir.join("state.json"));
        let addr = serve_store(s.clone(), WriteToken::open()).await;
        for path in ["/catalog", COVERAGE] {
            let (code, body) = get(&addr, path).await;
            assert_eq!(code, 503, "{path}");
            assert_eq!(body["error"], "the store is still scanning");
        }
        scanned(&s).await;
        let (code, cat) = get(&addr, "/catalog").await;
        assert_eq!((code, cat["missing_volumes"].clone()), (200, json!([])));
        let (code, cov) = get(&addr, COVERAGE).await;
        assert_eq!(
            (code, cov["days"].clone(), cov["missing_volumes"].clone()),
            (200, json!([]), json!([]))
        );
        assert_eq!(cov["scanned_at"], cat["generated_at"]);
        // The provider must be given.
        let (code, _) = get(&addr, &COVERAGE.replace("provider=databento&", "")).await;
        assert_eq!(code, 400);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn files_are_listed_from_the_scan_and_refused_while_away() {
        let dir = scratch("files");
        let vol = dir.join("v");
        let day = vol.join("Databento/OPRA.PILLAR/SPY.OPT/cbbo-1m");
        std::fs::create_dir_all(&day).unwrap();
        std::fs::write(day.join("2026-10-01.dbn.zst"), b"abc").unwrap();
        let s = store(&[("v", &vol)], &dir.join("state.json"));
        let addr = serve_store(s.clone(), WriteToken::open()).await;
        let name = "v/Databento/OPRA.PILLAR/SPY.OPT/cbbo-1m/2026-10-01.dbn.zst";
        let absent = "/files/v/Databento/OPRA.PILLAR/SPY.OPT/cbbo-1m/2026-10-09.dbn.zst";
        let error = |body: &Value| body["error"].as_str().unwrap_or_default().to_owned();

        // Before the first scan: no listing, and a file not found may yet be
        // there. One that is there is served.
        let (code, body) = get(&addr, "/files").await;
        assert_eq!(
            (code, error(&body)),
            (503, "the store is still scanning".into())
        );
        let (code, body) = get(&addr, absent).await;
        assert_eq!(code, 503);
        assert!(error(&body).contains("volume v is being scanned"), "{body}");
        let (code, bytes) = call(&addr, "GET", &format!("/files/{name}"), None, "").await;
        assert_eq!((code, bytes.as_str()), (200, "abc"));

        scanned(&s).await;
        let (code, body) = get(&addr, "/files?pattern=*cbbo-1m*").await;
        assert_eq!(code, 200);
        assert_eq!(
            body["files"],
            json!([{ "name": name, "volume": "v", "path": &name[2..], "bytes": 3 }])
        );
        assert_eq!(body["truncated"], false);
        assert!(body["scanned_at"].as_f64().is_some());
        // From the scan, not the disk: a file written since is listed after
        // the next.
        std::fs::write(day.join("2026-10-02.dbn.zst"), b"de").unwrap();
        let count = |v: &Value| v["files"].as_array().map_or(0, Vec::len);
        assert_eq!(count(&get(&addr, "/files").await.1), 1);
        scanned(&s).await;
        assert_eq!(count(&get(&addr, "/files").await.1), 2);
        let (_, one) = get(&addr, "/files?limit=1&provider=databento").await;
        assert_eq!((count(&one), one["truncated"].clone()), (1, json!(true)));
        assert_eq!(count(&get(&addr, "/files?provider=alpaca").await.1), 0);

        // Scanned and there: an absent file is a 404.
        let (code, body) = get(&addr, absent).await;
        assert_eq!(code, 404);
        assert!(error(&body).starts_with("no file"), "{body}");
        assert_eq!(get(&addr, "/files/nowhere/a.dbn").await.0, 404);

        // Away: nothing listed, and every file refused as not mounted.
        let away = dir.join("v-away");
        std::fs::rename(&vol, &away).unwrap();
        assert_eq!(count(&get(&addr, "/files").await.1), 0);
        for path in [format!("/files/{name}"), absent.to_owned()] {
            let (code, body) = get(&addr, &path).await;
            assert_eq!(code, 503, "{path}");
            assert!(error(&body).contains("volume v is not mounted"), "{body}");
        }
        std::fs::rename(&away, &vol).unwrap();
        assert_eq!(count(&get(&addr, "/files").await.1), 2);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn remembers_a_volume_until_it_is_retired() {
        let dir = scratch("remember");
        let (a, b) = (dir.join("a"), dir.join("b"));
        let day = b.join("Databento/OPRA.PILLAR/SPY.OPT/cbbo-1m");
        std::fs::create_dir_all(&a).unwrap();
        std::fs::create_dir_all(&day).unwrap();
        std::fs::write(day.join("2026-10-01.dbn.zst"), b"x").unwrap();
        let state_file = dir.join("state/store-state.json");
        let first = store(&[("a", &a), ("b", &b)], &state_file);
        scanned(&first).await;
        let held = state::load(&state_file).unwrap();
        assert_eq!(held.volumes.keys().collect::<Vec<_>>(), ["a", "b"]);
        assert_eq!(
            held.volumes["b"].held["databento/OPRA.PILLAR/SPY.OPT"]["cbbo-1m"]["days"],
            1
        );

        // Restarted with only `a` configured, and `b` unplugged.
        std::fs::rename(&b, dir.join("b-away")).unwrap();
        let s = store(&[("a", &a)], &state_file);
        scanned(&s).await;
        let addr = serve_store(s, WriteToken::open()).await;
        let (_, cat) = get(&addr, "/catalog").await;
        assert_eq!(cat["missing_volumes"], json!(["b"]));
        // What it held shows as away, not gone.
        let away = cat["roots"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["name"] == "b")
            .unwrap();
        assert_eq!(away["present"], false);
        assert_eq!(
            away["held"]["databento/OPRA.PILLAR/SPY.OPT"]["cbbo-1m"]["first"],
            "2026-10-01"
        );
        let (code, cov) = get(&addr, COVERAGE).await;
        assert_eq!((code, cov["missing_volumes"].clone()), (200, json!(["b"])));

        assert_eq!(
            status(&addr, "POST", "/volumes/a/retire", None, "").await,
            409,
            "mounted"
        );
        assert_eq!(
            status(&addr, "POST", "/volumes/zz/retire", None, "").await,
            404
        );
        let (code, body) = call(&addr, "POST", "/volumes/b/retire", None, "").await;
        assert_eq!(code, 200, "{body}");
        let (_, cat) = get(&addr, "/catalog").await;
        assert_eq!(cat["missing_volumes"], json!([]));
        assert!(
            cat["roots"]
                .as_array()
                .unwrap()
                .iter()
                .all(|r| r["name"] != "b")
        );
        let (_, cov) = get(&addr, COVERAGE).await;
        assert_eq!(cov["missing_volumes"], json!([]));
        let after = state::load(&state_file).unwrap();
        assert_eq!(after.volumes.keys().collect::<Vec<_>>(), ["a"]);
        assert_eq!(after.retired[0]["name"], "b");
        assert!(after.retired[0]["retired_at"].is_number());
        assert_eq!(
            after.retired[0]["held"]["databento/OPRA.PILLAR/SPY.OPT"]["cbbo-1m"]["first"],
            "2026-10-01"
        );
        // And it stays forgotten across a restart.
        let s = store(&[("a", &a)], &state_file);
        scanned(&s).await;
        assert_eq!(lock(&s.catalog)["missing_volumes"], json!([]));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_folder_that_is_not_a_mount_point_is_missing() {
        let dir = scratch("not-mounted");
        let volume = Volume {
            name: "x".into(),
            path: dir.join("x"),
            writable: true,
            require_mount: true,
        };
        std::fs::create_dir_all(&volume.path).unwrap();
        let state_file = dir.join("state.json");
        let s = Store::new(
            vec![volume],
            None,
            LIMITS,
            state_file.clone(),
            state::Remembered::default(),
        );
        s.remember(&[]).unwrap();
        assert!(state::load(&state_file).unwrap().volumes["x"].require_mount);
        scanned(&s).await;
        assert!(
            s.place("databento", "B1", "f", None).is_err(),
            "never written to"
        );
        let addr = serve_store(s, WriteToken::open()).await;
        let (_, cat) = get(&addr, "/catalog").await;
        assert_eq!(cat["missing_volumes"], json!(["x"]));
        assert!(
            cat["roots"][0]["errors"][0]
                .as_str()
                .unwrap()
                .contains("not a mounted drive")
        );
        let (_, cov) = get(&addr, COVERAGE).await;
        assert_eq!(cov["missing_volumes"], json!(["x"]));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_volume_emptied_of_what_it_held_is_missing() {
        let dir = scratch("emptied");
        let vol = dir.join("v");
        let day = vol.join("Databento/OPRA.PILLAR/SPY.OPT/cbbo-1m");
        std::fs::create_dir_all(&day).unwrap();
        std::fs::write(day.join("2026-10-01.dbn.zst"), b"x").unwrap();
        let state_file = dir.join("state.json");
        let s = store(&[("v", &vol)], &state_file);
        scanned(&s).await;
        let addr = serve_store(s.clone(), WriteToken::open()).await;
        const ACCEPT: &str = "/volumes/v/accept-empty";
        let by = r#"{"by":"operator"}"#;
        assert_eq!(
            status(&addr, "POST", ACCEPT, None, by).await,
            409,
            "it isn't empty"
        );
        std::fs::remove_dir_all(vol.join("Databento")).unwrap();
        scanned(&s).await;
        let (_, cat) = get(&addr, "/catalog").await;
        assert_eq!(cat["missing_volumes"], json!(["v"]));
        let why = cat["roots"][0]["errors"][0].as_str().unwrap();
        assert!(
            why.contains("now holds none") && why.contains(ACCEPT),
            "{why}"
        );
        let (_, cov) = get(&addr, COVERAGE).await;
        assert_eq!(cov["missing_volumes"], json!(["v"]));
        // What it held is still remembered.
        assert!(state::held_any(
            &state::load(&state_file).unwrap().volumes["v"]
        ));

        // Accepted as empty: no longer missing, and the record kept.
        assert_eq!(
            status(&addr, "POST", "/volumes/zz/accept-empty", None, "").await,
            404
        );
        let (code, body) = call(&addr, "POST", ACCEPT, None, by).await;
        assert_eq!(code, 200, "{body}");
        let (_, cat) = get(&addr, "/catalog").await;
        assert_eq!(cat["missing_volumes"], json!([]));
        let (_, cov) = get(&addr, COVERAGE).await;
        assert_eq!(cov["missing_volumes"], json!([]));
        let after = state::load(&state_file).unwrap();
        assert!(!state::held_any(&after.volumes["v"]));
        let record = &after.accepted_empty[0];
        assert_eq!(record["by"], "operator");
        assert!(record["accepted_at"].is_number());
        assert_eq!(
            record["held_before"]["databento/OPRA.PILLAR/SPY.OPT"]["cbbo-1m"]["days"],
            1
        );
        scanned(&s).await;
        assert_eq!(
            lock(&s.catalog)["missing_volumes"],
            json!([]),
            "and stays so"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Two days of SPY's cbbo-1m on a volume, scanned; the store and its state
    /// file.
    async fn two_days(name: &str) -> (PathBuf, PathBuf, PathBuf, Shared) {
        let dir = scratch(name);
        let vol = dir.join("v");
        let day = vol.join("Databento/OPRA.PILLAR/SPY.OPT/cbbo-1m");
        std::fs::create_dir_all(&day).unwrap();
        for d in ["2026-10-01", "2026-10-02"] {
            std::fs::write(day.join(format!("{d}.dbn.zst")), b"x").unwrap();
        }
        let state_file = dir.join("state.json");
        let s = store(&[("v", &vol)], &state_file);
        scanned(&s).await;
        (dir, vol, state_file, s)
    }

    fn days_held(state_file: &Path) -> Value {
        state::load(state_file).unwrap().volumes["v"].held["databento/OPRA.PILLAR/SPY.OPT"]["cbbo-1m"]["days"].clone()
    }

    /// A day file the scan can't stat (here a link to itself: ELOOP; a drive
    /// failing gives EIO) is an error of the volume, not a day it doesn't hold:
    /// the volume is missing, and what it is remembered to hold isn't shrunk
    /// (P0, 2026-11-10).
    #[tokio::test(flavor = "multi_thread")]
    async fn a_day_the_scan_cannot_stat_doesnt_shrink_what_it_held() {
        let (dir, vol, state_file, s) = two_days("flap-stat").await;
        assert_eq!(days_held(&state_file), 2);
        let day2 = vol.join("Databento/OPRA.PILLAR/SPY.OPT/cbbo-1m/2026-10-02.dbn.zst");
        std::fs::remove_file(&day2).unwrap();
        std::os::unix::fs::symlink(&day2, &day2).unwrap();
        scanned(&s).await;
        let cat = lock(&s.catalog).clone();
        assert_eq!(cat["missing_volumes"], json!(["v"]), "{cat}");
        assert_eq!(days_held(&state_file), 2, "not shrunk");
        let _ = std::fs::remove_dir_all(dir);
    }

    /// A volume that goes away during a scan (here swapped for an empty folder,
    /// as a drive dropping out leaves one) counts for nothing: it is reported
    /// away, with what it held before (P0, 2026-11-10).
    #[tokio::test(flavor = "multi_thread")]
    async fn a_volume_gone_during_the_scan_keeps_what_it_held() {
        let (dir, vol, state_file, s) = two_days("flap-gone").await;
        assert_eq!(days_held(&state_file), 2);
        let (v, aside) = (vol.clone(), dir.join("aside"));
        // Only this test's volume: other tests scan theirs meanwhile.
        *lock(&MID_SCAN) = Some(Box::new(move |p: &Path| {
            if p == v {
                let _ = std::fs::rename(&v, &aside);
                let _ = std::fs::create_dir_all(&v);
            }
        }));
        scanned(&s).await;
        *lock(&MID_SCAN) = None;
        let cat = lock(&s.catalog).clone();
        assert_eq!(cat["missing_volumes"], json!(["v"]), "{cat}");
        let why = cat["roots"][0]["errors"][0]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        assert!(
            why.contains("went away or changed during the scan"),
            "{why}"
        );
        assert_eq!(days_held(&state_file), 2, "not shrunk");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn coverage_answers_from_the_scan_and_waits_for_a_rescan_asked_for() {
        let dir = scratch("index");
        let vol = dir.join("v");
        let day = vol.join("Databento/OPRA.PILLAR/SPY.OPT/cbbo-1m");
        std::fs::create_dir_all(&day).unwrap();
        std::fs::write(day.join("2026-10-01.dbn.zst"), b"x").unwrap();
        let mut s = store(&[("v", &vol)], &dir.join("state.json"));
        // Long enough for a scan on a slow runner: the wait ends when it does.
        Arc::get_mut(&mut s).unwrap().wait = Duration::from_secs(30);
        scanned(&s).await;
        // Served without a rescan loop: rescans happen only as below.
        let app = router(s.clone(), WriteToken::open());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let days = |cov: &Value| -> Vec<String> {
            cov["days"]
                .as_array()
                .unwrap()
                .iter()
                .map(|d| {
                    format!(
                        "{} {}",
                        d["day"].as_str().unwrap(),
                        d["kind"].as_str().unwrap()
                    )
                })
                .collect()
        };
        // Not walked per request: a file that appears waits for a scan...
        std::fs::write(day.join("2026-10-02.dbn.zst"), b"x").unwrap();
        let (_, cov) = get(&addr, COVERAGE).await;
        assert_eq!(days(&cov), ["2026-10-01 complete"]);
        // ...and so does a manifest written behind the store's back (the
        // store's own writes read their folder again at once).
        let entry = json!({ "file": "2026-10-01.dbn.zst", "coverage": { "kind": "contracts", "stype": "raw_symbol", "symbols": ["SPY   261002C00728000"], "window": [0, 1] } });
        std::fs::write(
            day.join("manifest.json"),
            json!({ "days": { "2026-10-01": entry } }).to_string(),
        )
        .unwrap();
        let (_, cov) = get(&addr, COVERAGE).await;
        assert_eq!(days(&cov), ["2026-10-01 complete"]);
        // A rescan asked for (as after a filing) is waited for.
        s.request_rescan();
        let later = s.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            scanned(&later).await;
        });
        let (code, cov) = get(&addr, COVERAGE).await;
        assert_eq!(
            (code, days(&cov)),
            (
                200,
                vec![
                    "2026-10-01 partial".to_owned(),
                    "2026-10-02 complete".to_owned()
                ]
            )
        );
        // Not forever: no rescan comes, so 503 (a store that waits less).
        let mut s = store(&[("v", &vol)], &dir.join("state.json"));
        Arc::get_mut(&mut s).unwrap().wait = Duration::from_millis(300);
        scanned(&s).await;
        let app = router(s.clone(), WriteToken::open());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        s.request_rescan();
        let (code, body) = get(&addr, COVERAGE).await;
        assert_eq!(code, 503, "{body}");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn fresh_answers_wait_for_a_scan_started_after_them() {
        let dir = scratch("fresh");
        let vol = dir.join("v");
        let day = vol.join("Databento/OPRA.PILLAR/SPY.OPT/cbbo-1m");
        std::fs::create_dir_all(&day).unwrap();
        let mut s = store(&[("v", &vol)], &dir.join("state.json"));
        Arc::get_mut(&mut s).unwrap().fresh_wait = Duration::from_millis(400);
        scanned(&s).await;
        // Served without a rescan loop: the one scan below is run by hand.
        let app = router(s.clone(), WriteToken::open());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        // Copied onto the drive behind the store's back.
        std::fs::write(day.join("2026-10-01.dbn.zst"), b"x").unwrap();
        let (_, stale) = get(&addr, COVERAGE).await;
        assert_eq!(stale["days"], json!([]), "the last scan's answer");
        let fresh_cov = format!("{COVERAGE}&fresh=1");
        let (a, b) = (addr.clone(), addr.clone());
        let one = tokio::spawn(async move { get(&a, &fresh_cov).await });
        let two = tokio::spawn(async move { get(&b, "/catalog?fresh=1").await });
        tokio::time::sleep(Duration::from_millis(150)).await;
        scanned(&s).await;
        let ((c1, cov), (c2, cat)) = (one.await.unwrap(), two.await.unwrap());
        assert_eq!((c1, c2), (200, 200));
        assert_eq!(cov["days"][0]["day"], "2026-10-01");
        assert_eq!(
            cov["scanned_at"], cat["generated_at"],
            "both from the one scan"
        );
        // No scan comes: 503, not a stale answer.
        let (code, _) = get(&addr, "/catalog?fresh=1").await;
        assert_eq!(code, 503);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn records_conditions_from_the_provider_and_its_receipts() {
        let dir = scratch("conditions");
        let vol = dir.join("v");
        let day = vol.join("Databento/OPRA.PILLAR/SPY.OPT/cbbo-1m");
        std::fs::create_dir_all(&day).unwrap();
        for d in ["2026-10-01", "2026-10-03"] {
            std::fs::write(day.join(format!("{d}.dbn.zst")), b"x").unwrap();
        }
        let s = store(&[("v", &vol)], &dir.join("state.json"));
        scanned(&s).await;
        let addr = serve_store(s, WriteToken::open()).await;
        let post = |state: &str| {
            json!([
            { "dataset": "OPRA.PILLAR", "day": "2026-10-01", "state": state, "last_modified": "2026-10-02" },
            { "dataset": "OPRA.PILLAR", "day": "2026-10-02", "state": state },
        ]).to_string()
        };
        const POST: &str = "/conditions/databento";
        let (code, body) = call(&addr, "POST", POST, None, &post("degraded")).await;
        let body: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(code, 200, "{body}");
        assert_eq!(
            (
                body["changed"][0]["from"].clone(),
                body["changed"][0]["to"].clone()
            ),
            (Value::Null, json!("degraded"))
        );
        assert_eq!(
            body["not_held"],
            json!([{ "dataset": "OPRA.PILLAR", "day": "2026-10-02" }])
        );
        let (_, again) = call(&addr, "POST", POST, None, &post("degraded")).await;
        let again: Value = serde_json::from_str(&again).unwrap();
        assert_eq!(
            (again["changed"].clone(), again["unchanged"].clone()),
            (json!([]), json!(1))
        );
        // Repaired by the provider.
        let (_, fixed) = call(&addr, "POST", POST, None, &post("available")).await;
        let fixed: Value = serde_json::from_str(&fixed).unwrap();
        assert_eq!(
            (
                fixed["changed"][0]["from"].clone(),
                fixed["changed"][0]["to"].clone()
            ),
            (json!("degraded"), json!("available"))
        );
        let (_, cov) = get(&addr, COVERAGE).await;
        let c = &cov["days"][0];
        assert_eq!(
            (
                c["kind"].clone(),
                c["condition"]["state"].clone(),
                c["condition"]["source"].clone()
            ),
            (json!("complete"), json!("available"), json!("provider api"))
        );
        assert_eq!(cov["days"][1]["condition"], Value::Null, "not known");
        assert_eq!(status(&addr, "POST", POST, None, &post("bad")).await, 400);
        assert_eq!(
            status(&addr, "POST", "/conditions/nobody", None, "[]").await,
            404
        );

        // Backfilled from the receipts kept in provenance.
        let kept = vol.join("Databento/provenance/OPRA-K");
        std::fs::create_dir_all(&kept).unwrap();
        std::fs::write(
            kept.join("metadata.json"),
            json!({ "query": { "dataset": "OPRA.PILLAR" } }).to_string(),
        )
        .unwrap();
        std::fs::write(kept.join("condition.json"), json!([{ "date": "2026-10-03", "condition": "missing", "last_modified_date": "2026-10-04" }]).to_string()).unwrap();
        let (code, body) = call(&addr, "POST", "/conditions/backfill", None, "").await;
        let body: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(code, 200, "{body}");
        assert_eq!(
            (
                body["batches"].clone(),
                body["rows"].clone(),
                body["changed"].as_array().unwrap().len()
            ),
            (json!(1), json!(1), 1)
        );
        let (_, again) = call(&addr, "POST", "/conditions/backfill", None, "").await;
        assert!(again.contains(r#""changed":[]"#), "once is enough: {again}");
        let (_, cov) = get(&addr, COVERAGE).await;
        assert_eq!(cov["days"][1]["condition"]["source"], "batch OPRA-K");
        let (_, cat) = get(&addr, "/catalog").await;
        assert_eq!(
            cat["roots"][0]["datasets"][0]["schemas"]["cbbo-1m"]["missing"],
            json!(["2026-10-03"])
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Serve a store with no rescan loop: answers change only as asked.
    async fn serve_plain(s: &Shared) -> String {
        let app = router(s.clone(), WriteToken::open());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        addr
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_filing_shows_at_once_without_scanning_again() {
        use crate::partial::tests::{C1, C2, DAY, day_bytes, name, stage};
        let dir = scratch("in-place");
        let vol = dir.join("v");
        std::fs::create_dir_all(&vol).unwrap();
        let s = store(&[("v", &vol)], &dir.join("state.json"));
        scanned(&s).await;
        let addr = serve_plain(&s).await;
        stage(
            &vol.join("Databento"),
            "I1",
            &[(&name(), day_bytes(&[(C1, 11)], &[(11, 0)]))],
        );
        let (code, body) = call(&addr, "POST", "/import/databento/I1", None, "").await;
        assert_eq!(code, 200, "{body}");
        let (_, cov) = get(&addr, COVERAGE).await;
        assert_eq!(
            (
                cov["days"][0]["day"].clone(),
                cov["days"][0]["kind"].clone()
            ),
            (json!(DAY), json!("partial"))
        );
        let (_, cat) = get(&addr, "/catalog").await;
        let schema = &cat["roots"][0]["datasets"][0]["schemas"]["cbbo-1m"];
        assert_eq!(schema["partial"], json!([DAY]), "{cat}");
        assert!(cat["updated_at"].is_number());
        // A merge into it, the same way.
        stage(
            &vol.join("Databento"),
            "I2",
            &[(&name(), day_bytes(&[(C2, 12)], &[(12, 1)]))],
        );
        assert_eq!(
            call(&addr, "POST", "/import/databento/I2", None, "")
                .await
                .0,
            200
        );
        let (_, cov) = get(&addr, COVERAGE).await;
        assert_eq!(cov["days"][0]["coverage"]["symbols"], json!([C1, C2]));
        assert_eq!(s.requested.load(Ordering::SeqCst), 0, "no scan asked for");
        // Answered from memory: the manifest isn't read again for it.
        let folder = vol.join("Databento/OPRA.PILLAR/SPY.OPT/cbbo-1m");
        std::fs::rename(folder.join("manifest.d"), dir.join("away")).unwrap();
        std::fs::rename(folder.join("manifest.json"), dir.join("away.json")).unwrap();
        let (code, again) = get(&addr, COVERAGE).await;
        assert_eq!((code, &again["days"]), (200, &cov["days"]), "{again}");
        let _ = std::fs::remove_dir_all(dir);
    }

    /// With the filer's queue full, a write over HTTP is answered 503, busy.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_full_filing_queue_answers_503() {
        let dir = scratch("busy-503");
        let vol = dir.join("v");
        std::fs::create_dir_all(&vol).unwrap();
        let s = store(&[("v", &vol)], &dir.join("state.json"));
        scanned(&s).await;
        let addr = serve_plain(&s).await;
        let _held = s.filer.turn().await.unwrap();
        let waiting: Vec<_> = (0..filer::CAPACITY)
            .map(|_| {
                let f = s.filer.clone();
                tokio::spawn(async move { f.turn().await.map(drop) })
            })
            .collect();
        while s.filer.queued() < filer::CAPACITY {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let (code, body) = call(&addr, "POST", "/coverage/rebuild", None, "").await;
        assert_eq!(code, 503, "{body}");
        assert!(body.contains(BUSY), "{body}");
        for w in waiting {
            w.abort();
        }
        let _ = std::fs::remove_dir_all(dir);
    }

    /// A write whose caller goes away keeps its turn until it has written: the
    /// next turn waits for it.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_cancelled_write_keeps_its_turn() {
        let dir = scratch("cancelled-write");
        let vol = dir.join("v");
        std::fs::create_dir_all(&vol).unwrap();
        let s = store(&[("v", &vol)], &dir.join("state.json"));
        scanned(&s).await;
        let (go, wait) = std::sync::mpsc::sync_channel::<()>(1);
        let (started_tx, started) = std::sync::mpsc::sync_channel::<()>(1);
        let s2 = s.clone();
        let caller = tokio::spawn(async move {
            s2.file_alone(Vec::new(), move || {
                let _ = started_tx.send(());
                let _ = wait.recv();
                Ok(json!({}))
            })
            .await
        });
        // Aborted once the write has started (the turn is granted a moment
        // before it's handed to the write: an abort then drops it with
        // nothing written, which is right, but isn't this test).
        let s3 = s.clone();
        tokio::task::spawn_blocking(move || {
            let _ = started.recv_timeout(Duration::from_secs(10));
            drop(s3);
        })
        .await
        .unwrap();
        caller.abort();
        let next = tokio::time::timeout(Duration::from_millis(200), s.filer.turn()).await;
        assert!(next.is_err(), "the next turn waits for the write");
        go.send(()).unwrap();
        let next = tokio::time::timeout(Duration::from_secs(5), s.filer.turn()).await;
        assert!(matches!(next, Ok(Ok(_))));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test(flavor = "multi_thread")]
    #[expect(
        clippy::await_holding_lock,
        reason = "the catalog held here stops the group at its last step"
    )]
    async fn a_group_finishes_when_a_request_is_dropped() {
        use crate::partial::tests::{C1, C2, C3, C4, day_bytes, name, stage};
        let dir = scratch("dropped");
        let vol = dir.join("v");
        let root = vol.join("Databento");
        std::fs::create_dir_all(&vol).unwrap();
        let s = store(&[("v", &vol)], &dir.join("state.json"));
        scanned(&s).await;
        let addr = serve_plain(&s).await;
        for (id, c, n) in [
            ("G1", C1, 11),
            ("G2", C2, 12),
            ("G3", C3, 13),
            ("G4", C4, 14),
        ] {
            stage(&root, id, &[(&name(), day_bytes(&[(c, n)], &[(n, 0)]))]);
        }
        let post = |id: &str| {
            let (a, p) = (addr.clone(), format!("/import/databento/{id}"));
            tokio::spawn(async move { call(&a, "POST", &p, None, "").await })
        };
        // Three asked while a filing runs: one group.
        let running = s.filer.turn().await.unwrap();
        let (lead, two, three) = (post("G1"), post("G2"), post("G3"));
        while s.filer.queued() < 3 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // Held at its last step, reading its folders into the catalog.
        let catalog = lock(&s.catalog);
        drop(running);
        while s.filer.queued() > 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // The lead's client goes away mid-group: the lock stays held.
        lead.abort();
        let four = post("G4");
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(s.filer.busy() && !four.is_finished());
        drop(catalog);
        for r in [two, three, four] {
            let (code, body) = r.await.unwrap();
            assert_eq!(code, 200, "{body}");
        }
        let day = root.join("OPRA.PILLAR/SPY.OPT/cbbo-1m").join(name());
        assert_eq!(partial::inspect(&day).unwrap().1, 4, "G1 filed too");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn each_scan_settles_merges_left_pending() {
        use crate::partial::tests::{C1, C2, DAY, day_bytes, name, stage};
        let dir = scratch("settle-scan");
        let vol = dir.join("v");
        std::fs::create_dir_all(&vol).unwrap();
        let s = store(&[("v", &vol)], &dir.join("state.json"));
        scanned(&s).await;
        let addr = serve_plain(&s).await;
        stage(
            &vol.join("Databento"),
            "I1",
            &[(&name(), day_bytes(&[(C1, 11)], &[(11, 0)]))],
        );
        assert_eq!(
            call(&addr, "POST", "/import/databento/I1", None, "")
                .await
                .0,
            200
        );
        let folder = vol.join("Databento/OPRA.PILLAR/SPY.OPT/cbbo-1m");
        let day_file = folder.join(name());
        let pending = folder
            .join("manifest.d")
            .join(format!("{DAY}.pending.json"));
        // Stopped after the swap: the merged file is in place.
        std::fs::write(
            &day_file,
            day_bytes(&[(C1, 11), (C2, 12)], &[(11, 0), (12, 1)]),
        )
        .unwrap();
        let sha = ingest::hash_file(&day_file).unwrap();
        let next = json!({ "file": name(), "sha256": sha, "records": 2, "coverage": { "kind": "contracts", "symbols": [C1, C2], "window": [0, 1] } });
        manifest::write_pending(&folder, DAY, &next).unwrap();
        scanned(&s).await;
        assert!(!pending.exists());
        let (_, cov) = get(&addr, COVERAGE).await;
        assert_eq!(cov["days"][0]["coverage"]["symbols"], json!([C1, C2]));
        // Stopped before it: dropped.
        manifest::write_pending(&folder, DAY, &json!({ "sha256": "f00d" })).unwrap();
        scanned(&s).await;
        assert!(!pending.exists());
        assert_eq!(
            manifest::read_day(&folder, DAY).unwrap().unwrap()["sha256"],
            sha.as_str()
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_scan_leaves_merges_alone_while_a_filing_runs() {
        use crate::partial::tests::{C1, DAY, day_bytes, name, stage};
        let dir = scratch("settle-lock");
        let vol = dir.join("v");
        std::fs::create_dir_all(&vol).unwrap();
        let s = store(&[("v", &vol)], &dir.join("state.json"));
        scanned(&s).await;
        let addr = serve_plain(&s).await;
        stage(
            &vol.join("Databento"),
            "I1",
            &[(&name(), day_bytes(&[(C1, 11)], &[(11, 0)]))],
        );
        assert_eq!(
            call(&addr, "POST", "/import/databento/I1", None, "")
                .await
                .0,
            200
        );
        let folder = vol.join("Databento/OPRA.PILLAR/SPY.OPT/cbbo-1m");
        let pending = folder
            .join("manifest.d")
            .join(format!("{DAY}.pending.json"));
        // A merge recorded, its file not yet swapped in, while it is filed.
        manifest::write_pending(&folder, DAY, &json!({ "sha256": "the merged file" })).unwrap();
        let filing = s.filer.turn().await.unwrap();
        scanned(&s).await;
        assert!(pending.exists(), "not settled while a filing runs");
        drop(filing);
        scanned(&s).await;
        assert!(!pending.exists(), "settled once it is done");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_failed_filing_reads_its_folder_again() {
        use crate::partial::tests::{C1, C2, day_bytes, name, stage};
        let dir = scratch("failed-filing");
        let vol = dir.join("v");
        std::fs::create_dir_all(&vol).unwrap();
        let s = store(&[("v", &vol)], &dir.join("state.json"));
        scanned(&s).await;
        let addr = serve_plain(&s).await;
        stage(
            &vol.join("Databento"),
            "I1",
            &[(&name(), day_bytes(&[(C1, 11)], &[(11, 0)]))],
        );
        assert_eq!(
            call(&addr, "POST", "/import/databento/I1", None, "")
                .await
                .0,
            200
        );
        // A day there that the store hasn't read (as a filing that stopped
        // partway would leave).
        let folder = vol.join("Databento/OPRA.PILLAR/SPY.OPT/cbbo-1m");
        std::fs::write(folder.join("2026-10-03.dbn.zst"), b"x").unwrap();
        // An import that is refused: its file isn't the one its receipt names.
        let part = stage(
            &vol.join("Databento"),
            "I2",
            &[(&name(), day_bytes(&[(C2, 12)], &[(12, 1)]))],
        );
        std::fs::write(part.inc.join(name()), day_bytes(&[(C2, 12)], &[(12, 2)])).unwrap();
        assert_eq!(
            call(&addr, "POST", "/import/databento/I2", None, "")
                .await
                .0,
            409
        );
        let (_, cov) = get(&addr, COVERAGE).await;
        let days: Vec<&str> = cov["days"]
            .as_array()
            .unwrap()
            .iter()
            .map(|d| d["day"].as_str().unwrap())
            .collect();
        assert_eq!(days, ["2026-10-02", "2026-10-03"]);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_operator_can_ignore_a_path_the_store_cannot_read() {
        let dir = scratch("ignore");
        let vol = dir.join("v");
        let junk = vol.join("backups/old.dbn.zst");
        std::fs::create_dir_all(junk.parent().unwrap()).unwrap();
        std::fs::write(&junk, b"truncated").unwrap();
        let old = std::time::SystemTime::now() - Duration::from_secs(600);
        std::fs::File::options()
            .write(true)
            .open(&junk)
            .unwrap()
            .set_modified(old)
            .unwrap();
        let state_file = dir.join("state.json");
        let s = store(&[("v", &vol)], &state_file);
        scanned(&s).await;
        let addr = serve_store(s, WriteToken::open()).await;
        let (_, cat) = get(&addr, "/catalog").await;
        assert_eq!(cat["missing_volumes"], json!(["v"]));
        let why = cat["roots"][0]["errors"][0].as_str().unwrap();
        assert!(
            why.contains(&junk.display().to_string()) && why.contains("POST /volumes/v/ignore"),
            "{why}"
        );
        assert_eq!(get(&addr, COVERAGE).await.0, 500);

        const IGNORE: &str = "/volumes/v/ignore";
        let body = json!({ "path": junk, "reason": "a truncated copy; the original is on data", "by": "operator" }).to_string();
        assert_eq!(
            status(
                &addr,
                "POST",
                IGNORE,
                None,
                &json!({ "path": junk }).to_string()
            )
            .await,
            400,
            "a reason is needed"
        );
        assert_eq!(
            status(
                &addr,
                "POST",
                IGNORE,
                None,
                r#"{"path":"/etc","reason":"x"}"#
            )
            .await,
            400,
            "inside the volume only"
        );
        assert_eq!(
            status(
                &addr,
                "POST",
                IGNORE,
                None,
                r#"{"path":"backups/../..","reason":"x"}"#
            )
            .await,
            400
        );
        let (code, rec) = call(&addr, "POST", IGNORE, None, &body).await;
        assert_eq!(code, 200, "{rec}");
        assert_eq!(
            status(&addr, "POST", IGNORE, None, &body).await,
            409,
            "already"
        );
        let (code, _) = get(&addr, COVERAGE).await;
        assert_eq!(code, 200, "after the rescan it asked for");
        let (_, cat) = get(&addr, "/catalog").await;
        assert_eq!(cat["missing_volumes"], json!([]));
        assert_eq!(
            cat["roots"][0]["ignored"][0]["reason"],
            "a truncated copy; the original is on data"
        );
        let kept = state::load(&state_file).unwrap();
        assert_eq!(
            (
                kept.ignored[0]["by"].clone(),
                kept.ignored[0]["at"].is_number()
            ),
            (json!("operator"), true)
        );

        // Un-ignored (by a path relative to the volume): an error again.
        let rel = r#"{"path":"backups/old.dbn.zst"}"#;
        assert_eq!(status(&addr, "DELETE", IGNORE, None, rel).await, 200);
        assert_eq!(status(&addr, "DELETE", IGNORE, None, rel).await, 404);
        assert_eq!(get(&addr, COVERAGE).await.0, 500);
        let (_, cat) = get(&addr, "/catalog").await;
        assert_eq!(cat["missing_volumes"], json!(["v"]));
        assert!(state::load(&state_file).unwrap().ignored.is_empty());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_volume_that_cannot_be_read_is_missing_not_empty() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = scratch("unreadable");
        let vol = dir.join("v");
        let ds = vol.join("Databento/OPRA.PILLAR");
        std::fs::create_dir_all(ds.join("SPY.OPT/cbbo-1m")).unwrap();
        std::fs::set_permissions(&ds, std::fs::Permissions::from_mode(0o000)).unwrap();
        if std::fs::read_dir(&ds).is_ok() {
            // Permissions don't apply (running as root): nothing to show.
            std::fs::set_permissions(&ds, std::fs::Permissions::from_mode(0o755)).unwrap();
            return;
        }
        let s = store(&[("v", &vol)], &dir.join("state.json"));
        scanned(&s).await;
        let addr = serve_store(s, WriteToken::open()).await;
        let (_, cat) = get(&addr, "/catalog").await;
        let (code, _) = get(&addr, COVERAGE).await;
        std::fs::set_permissions(&ds, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert_eq!(cat["missing_volumes"], json!(["v"]));
        assert_eq!(code, 500);
        let _ = std::fs::remove_dir_all(dir);
    }
}
