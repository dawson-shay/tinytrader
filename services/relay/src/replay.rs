//! Replay to file, on request (experimental): `POST
//! /replay?schema=cbbo-1m&start=<RFC 3339>` replays the relay's whole parent
//! set (its `RELAY_SYMBOLS`, such as `SPY.OPT`) from Databento's intraday
//! replay into a DBN file, in the background.
//!
//! This is not a recording of the stream: the relay still forwards and keeps
//! nothing of what it sends (decision 0006). A replay file is made only when
//! asked for, from a separate, short Databento session, like `/session`
//! (session.rs) but for every contract and written to disk instead of held in
//! memory. Databento won't replay `cmbp-1` intraday, so the schemas are those
//! `/session` serves: `cbbo-1m`, `cbbo-1s`, `tcbbo` and `trades`.
//!
//! - **Off unless `RELAY_REPLAY_DIR` is set** (and only on Databento relays):
//!   `/replay` answers `404` saying so.
//! - **Where:** `<RELAY_REPLAY_DIR>/<YYYY-MM-DD>/<dataset>-<roots>-<schema>-<start>.dbn.zst`,
//!   the date the start's in New York, `<roots>` the parents without `.OPT`
//!   joined by `_`, `<start>` Unix seconds. Written as `….dbn.zst.tmp`, renamed
//!   when Databento says the replay is complete; a replay that ends otherwise
//!   (the session closes, an error, the timeout) is kept as
//!   `….partial.dbn.zst`. A sidecar `<file>.json` says what the file holds
//!   (`tinytrader.replay-file/1`).
//! - **The file:** standard DBN v3, its header's schema unset (mixed: the
//!   stream holds symbol mappings and system records beside the schema's; the
//!   name and the sidecar say the schema). The session's metadata, then every record
//!   Databento sent but heartbeats, unchanged, symbol mappings included,
//!   through its `ReplayCompleted` system record,
//!   the file's explicit end), as independent checksummed Zstandard frames of
//!   at most 4 MiB decoded that never split a record (Hyphae's tape layout),
//!   so a partial file reads up to its last whole frame.
//! - **One at a time,** sharing `/session`'s turn: the relay holds at most two
//!   of the dataset's sessions. A second request while one runs is `409`.
//!   A replay is given up after `RELAY_REPLAY_TIMEOUT` seconds (30 minutes).
//!   `RELAY_REPLAY_MAX_MBPS` paces its reads: in market hours it shares the
//!   link with the live feed.
//!
//! The parts are kept apart for what comes next: a [`RecordSource`] yields DBN
//! records (here [`LiveReplay`], a Databento live session from a start until
//! `ReplayCompleted`; next, the same replay played to a stream client), and a
//! [`DbnFile`] writes whole records to a file (next, a recorder of what the
//! relay forwards).

use std::collections::VecDeque;
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use axum::extract::{Path as UrlPath, Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use databento::LiveClient;
use databento::dbn::encode::dbn::MetadataEncoder;
use databento::dbn::{
    ErrorMsg, Metadata, RecordRef, SType, Schema, SystemCode, SystemMsg, VersionUpgradePolicy,
};
use databento::live::Subscription;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::sync::{Semaphore, mpsc};
use tokio::time::Instant;

use crate::session::{replay_schema, replay_start};

/// The sidecar's format.
pub const FORMAT: &str = "tinytrader.replay-file/1";
/// The most decoded bytes in one Zstandard frame.
const FRAME_BYTES: usize = 4 << 20;
/// Records are handed to the writer thread in batches of about this size.
const BATCH_BYTES: usize = 1 << 20;
/// Zstandard level of each frame.
const LEVEL: i32 = 3;
/// Jobs listed, at most; the oldest finished ones go first.
const KEPT: usize = 32;

/// Where DBN records come from: a metadata header, then records.
pub trait RecordSource {
    fn metadata(&self) -> &Metadata;
    /// The next record; `None` when the source closed.
    async fn next_record(&mut self) -> Result<Option<RecordRef<'_>>, String>;
}

/// A Databento live session subscribed from a start: Databento replays from
/// there, sends `ReplayCompleted`, then carries on live.
pub struct LiveReplay {
    client: LiveClient,
    metadata: Metadata,
}

impl LiveReplay {
    pub async fn open(
        key: &str,
        dataset: &str,
        symbols: &[String],
        stype_in: SType,
        schema: Schema,
        start: i64,
    ) -> Result<Self, String> {
        let mut client = LiveClient::builder()
            .key(key)
            .map_err(|e| format!("invalid API key: {e}"))?
            .dataset(dataset)
            .upgrade_policy(VersionUpgradePolicy::UpgradeToV3)
            .build()
            .await
            .map_err(|e| format!("cannot connect to Databento: {e}"))?;
        let from = OffsetDateTime::from_unix_timestamp(start).map_err(|e| e.to_string())?;
        client
            .subscribe(
                Subscription::builder()
                    .symbols(symbols.to_vec())
                    .schema(schema)
                    .stype_in(stype_in)
                    .start(from)
                    .build(),
            )
            .await
            .map_err(|e| format!("cannot subscribe: {e}"))?;
        let metadata = client
            .start()
            .await
            .map_err(|e| format!("cannot start the replay: {e}"))?;
        Ok(Self { client, metadata })
    }

    pub async fn close(mut self) {
        let _ = self.client.close().await;
    }
}

impl RecordSource for LiveReplay {
    fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    async fn next_record(&mut self) -> Result<Option<RecordRef<'_>>, String> {
        self.client
            .next_record()
            .await
            .map_err(|e| format!("replay failed: {e}"))
    }
}

/// Counts and hashes what reaches the file.
struct Tally<W> {
    out: W,
    sha: Sha256,
    bytes: Arc<AtomicU64>,
}

impl<W: Write> Write for Tally<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.out.write(buf)?;
        self.sha.update(&buf[..n]);
        self.bytes.fetch_add(n as u64, Ordering::Relaxed);
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.out.flush()
    }
}

/// A DBN file being written: its metadata, then whole records, cut into
/// independent checksummed Zstandard frames of at most [`FRAME_BYTES`]
/// decoded (a frame never splits a record). Written to a temporary path and
/// renamed by [`DbnFile::finish`]. Blocking: run it off the async threads.
pub struct DbnFile {
    tmp: PathBuf,
    out: Tally<BufWriter<File>>,
    plain: Vec<u8>,
}

impl DbnFile {
    /// Starts `tmp` with `metadata`; `bytes` counts what is written to disk.
    pub fn create(tmp: &Path, metadata: &Metadata, bytes: Arc<AtomicU64>) -> io::Result<Self> {
        use std::os::unix::fs::OpenOptionsExt as _;
        let file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o644)
            .open(tmp)?;
        let mut plain = Vec::with_capacity(FRAME_BYTES);
        MetadataEncoder::new(&mut plain)
            .encode(metadata)
            .map_err(io::Error::other)?;
        Ok(Self {
            tmp: tmp.to_owned(),
            out: Tally {
                out: BufWriter::new(file),
                sha: Sha256::new(),
                bytes,
            },
            plain,
        })
    }

    /// Appends one or more whole DBN records (their bytes, as Databento or
    /// the relay's hub carries them).
    pub fn push(&mut self, records: &[u8]) -> io::Result<()> {
        if !self.plain.is_empty() && self.plain.len() + records.len() > FRAME_BYTES {
            self.frame()?;
        }
        self.plain.extend_from_slice(records);
        Ok(())
    }

    /// Writes the last frame, syncs the file and renames it to `to`; gives the
    /// file's SHA-256 (hex).
    pub fn finish(mut self, to: &Path) -> io::Result<String> {
        self.frame()?;
        let Tally { out, sha, .. } = self.out;
        let file = out.into_inner().map_err(io::IntoInnerError::into_error)?;
        file.sync_all()?;
        fs::rename(&self.tmp, to)?;
        Ok(format!("{:x}", sha.finalize()))
    }

    fn frame(&mut self) -> io::Result<()> {
        if self.plain.is_empty() {
            return Ok(());
        }
        let mut z = zstd::stream::Encoder::new(&mut self.out, LEVEL)?;
        z.include_checksum(true)?;
        z.set_pledged_src_size(Some(self.plain.len() as u64))?;
        z.write_all(&self.plain)?;
        z.finish()?;
        self.plain.clear();
        Ok(())
    }
}

/// A [`DbnFile`] on its own thread, fed batches of records.
struct FileSink {
    tx: mpsc::Sender<Vec<u8>>,
    batch: Vec<u8>,
    task: tokio::task::JoinHandle<io::Result<DbnFile>>,
}

impl FileSink {
    fn new(file: DbnFile) -> Self {
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(8);
        let task = tokio::task::spawn_blocking(move || {
            let mut file = file;
            while let Some(b) = rx.blocking_recv() {
                file.push(&b)?;
            }
            Ok(file)
        });
        Self {
            tx,
            batch: Vec::with_capacity(BATCH_BYTES + 1024),
            task,
        }
    }

    async fn push(&mut self, record: &[u8]) -> io::Result<()> {
        self.batch.extend_from_slice(record);
        if self.batch.len() >= BATCH_BYTES {
            let b = std::mem::replace(&mut self.batch, Vec::with_capacity(BATCH_BYTES + 1024));
            if self.tx.send(b).await.is_err() {
                // The writer stopped: its error is in `finish`.
                return Err(io::Error::other("the file writer stopped"));
            }
        }
        Ok(())
    }

    async fn finish(self, to: PathBuf) -> io::Result<String> {
        let Self { tx, batch, task } = self;
        if !batch.is_empty() {
            let _ = tx.send(batch).await;
        }
        drop(tx);
        let file = task.await.map_err(io::Error::other)??;
        tokio::task::spawn_blocking(move || file.finish(&to))
            .await
            .map_err(io::Error::other)?
    }
}

/// A replay's paths.
#[derive(Debug, PartialEq, Eq)]
pub struct Paths {
    /// The finished file.
    pub file: PathBuf,
    /// While writing.
    pub tmp: PathBuf,
    /// When the replay didn't complete.
    pub partial: PathBuf,
}

/// `<dir>/<YYYY-MM-DD New York>/<dataset>-<roots>-<schema>-<start>.dbn.zst`.
#[must_use]
pub fn paths(dir: &Path, dataset: &str, symbols: &[String], schema: Schema, start: i64) -> Paths {
    let clean = |s: &str| -> String {
        s.chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                    c
                } else {
                    '_'
                }
            })
            .collect()
    };
    let roots: Vec<String> = symbols
        .iter()
        .map(|s| clean(s.strip_suffix(".OPT").unwrap_or(s)))
        .collect();
    let (date, _, _) = tinytrader_schedule::ny_date_time(start);
    let day = dir.join(date);
    let base = format!("{}-{}-{schema}-{start}", clean(dataset), roots.join("_"));
    Paths {
        file: day.join(format!("{base}.dbn.zst")),
        tmp: day.join(format!("{base}.dbn.zst.tmp")),
        partial: day.join(format!("{base}.partial.dbn.zst")),
    }
}

/// A sidecar's path: the file's, plus `.json`.
fn sidecar(file: &Path) -> PathBuf {
    let mut s = file.as_os_str().to_owned();
    s.push(".json");
    PathBuf::from(s)
}

/// How a replay to file went.
#[derive(Debug)]
pub struct Written {
    pub file: PathBuf,
    pub completed: bool,
    pub records: u64,
    /// The last market record's event time (ns), if any.
    pub end: Option<u64>,
    pub sha256: String,
    /// Why it didn't complete.
    pub error: Option<String>,
}

/// What a replay is of, for its sidecar.
pub struct About<'a> {
    pub dataset: &'a str,
    pub symbols: &'a [String],
    pub stype_in: SType,
    pub schema: Schema,
    pub start: i64,
}

/// The file's metadata: the session's, saying what was asked for (a live
/// gateway's header may leave the symbology and symbols out). Its schema is
/// unset, mixed, as decision 0022 section 4 has it: the stream carries symbol
/// mappings and system records beside the schema's, and a reader that trusted
/// a single schema would read them as that schema's records (Python's
/// `DBNStore.to_df()` did). The schema asked for is in the file's name and its
/// sidecar; Python reads it with `to_df(schema="cbbo-1m")`.
fn file_metadata(session: &Metadata, about: &About<'_>) -> Metadata {
    let mut m = session.clone();
    m.dataset = about.dataset.to_owned();
    m.schema = None;
    m.stype_in = Some(about.stype_in);
    m.symbols = about.symbols.to_vec();
    m.start = u64::try_from(about.start).unwrap_or(0) * 1_000_000_000;
    m
}

/// How long to wait before reading more, to keep a replay's reads near
/// `max_mbps` (megabits a second of DBN): `None` when not ahead of it.
#[must_use]
pub fn pace(read: u64, elapsed: Duration, max_mbps: f64) -> Option<Duration> {
    if max_mbps <= 0.0 || !max_mbps.is_finite() {
        return None;
    }
    let due = Duration::from_secs_f64(read as f64 * 8.0 / (max_mbps * 1e6));
    due.checked_sub(elapsed)
        .filter(|d| *d >= Duration::from_millis(5))
}

/// A replay's limits.
#[derive(Clone, Copy)]
pub struct Limits {
    /// Given up then (kept as partial).
    pub deadline: Instant,
    /// Reads paced to this many megabits a second; unlimited when `None`.
    pub max_mbps: Option<f64>,
}

/// Reads `src` into `paths.tmp` until Databento's `ReplayCompleted` (kept as
/// `paths.file`), or until it ends otherwise or `deadline` passes (kept as
/// `paths.partial`); then writes the sidecar. `Err` only when the file can't
/// be written.
pub async fn write_replay<S: RecordSource>(
    src: &mut S,
    paths: &Paths,
    about: &About<'_>,
    limits: Limits,
    records: &AtomicU64,
    bytes: Arc<AtomicU64>,
) -> Result<Written, String> {
    let deadline = limits.deadline;
    let (began, mut read, mut paced) = (Instant::now(), 0u64, 0u64);
    let io = |e: io::Error| format!("cannot write {}: {e}", paths.tmp.display());
    let file = {
        let (tmp, metadata) = (paths.tmp.clone(), file_metadata(src.metadata(), about));
        tokio::task::spawn_blocking(move || DbnFile::create(&tmp, &metadata, bytes))
            .await
            .map_err(|e| e.to_string())?
            .map_err(io)?
    };
    let mut sink = FileSink::new(file);
    let (mut end, mut error, mut completed) = (None, None, false);
    let mut written = 0u64;
    loop {
        let r = match tokio::time::timeout_at(deadline, src.next_record()).await {
            Err(_) => {
                error = Some("the replay timed out".to_owned());
                break;
            }
            Ok(Err(e)) => {
                error = Some(e);
                break;
            }
            Ok(Ok(None)) => {
                error.get_or_insert_with(|| {
                    "Databento ended the session before the replay completed".to_owned()
                });
                break;
            }
            Ok(Ok(Some(r))) => r,
        };
        read += r.as_ref().len() as u64;
        let done = if let Some(m) = r.get::<SystemMsg>() {
            if m.is_heartbeat() {
                // Heartbeats only say the session is alive; everything else is kept.
                continue;
            }
            m.code().ok() == Some(SystemCode::ReplayCompleted)
        } else if let Some(m) = r.get::<ErrorMsg>() {
            error = Some(format!(
                "Databento: {}",
                m.err().unwrap_or("error").to_owned()
            ));
            false
        } else {
            end = Some(r.header().ts_event);
            false
        };
        if let Err(e) = sink.push(r.as_ref()).await {
            // The writer's own error says more.
            return Err(io(sink.finish(paths.tmp.clone()).await.err().unwrap_or(e)));
        }
        written += 1;
        records.store(written, Ordering::Relaxed);
        if done {
            completed = true;
            error = None;
            break;
        }
        // Checked every 64 KiB read.
        if let Some(mbps) = limits.max_mbps
            && read - paced >= 64 << 10
        {
            paced = read;
            if let Some(wait) = pace(read, began.elapsed(), mbps) {
                tokio::time::sleep_until((Instant::now() + wait).min(deadline)).await;
            }
        }
    }
    let to = if completed {
        &paths.file
    } else {
        &paths.partial
    };
    let sha256 = sink.finish(to.clone()).await.map_err(io)?;
    let w = Written {
        file: to.clone(),
        completed,
        records: written,
        end,
        sha256,
        error,
    };
    let size = {
        let to = to.clone();
        tokio::task::spawn_blocking(move || fs::metadata(to).map_or(0, |m| m.len()))
            .await
            .unwrap_or(0)
    };
    let rfc = |ns: i128| {
        OffsetDateTime::from_unix_timestamp_nanos(ns)
            .ok()
            .and_then(|t| t.format(&Rfc3339).ok())
    };
    let meta = json!({
        "format": FORMAT,
        "file": to.file_name().map(|n| n.to_string_lossy()),
        "dataset": about.dataset,
        "parents": about.symbols,
        "stype_in": about.stype_in.to_string(),
        "schema": about.schema.to_string(),
        "start": rfc(i128::from(about.start) * 1_000_000_000),
        "end": w.end.and_then(|ns| rfc(i128::from(ns))),
        "records": w.records,
        "bytes": size,
        "sha256": w.sha256,
        "completed": w.completed,
        "error": w.error,
    });
    let body = serde_json::to_vec_pretty(&meta).map_err(|e| e.to_string())?;
    let (json_tmp, json, partial) = (sidecar(&paths.tmp), sidecar(to), paths.partial.clone());
    tokio::task::spawn_blocking(move || {
        fs::write(&json_tmp, body).and_then(|()| fs::rename(&json_tmp, json))?;
        if completed {
            // An earlier attempt's partial file is superseded.
            let _ = fs::remove_file(sidecar(&partial));
            let _ = fs::remove_file(partial);
        }
        Ok(())
    })
    .await
    .map_err(|e| e.to_string())?
    .map_err(io)?;
    Ok(w)
}

/// One replay to file.
struct Job {
    id: u64,
    schema: Schema,
    start: i64,
    file: PathBuf,
    started: i64,
    finished: Option<i64>,
    state: &'static str,
    error: Option<String>,
    records: Arc<AtomicU64>,
    bytes: Arc<AtomicU64>,
}

impl Job {
    fn json(&self) -> Value {
        json!({
            "id": self.id,
            "state": self.state,
            "schema": self.schema.to_string(),
            "start": self.start,
            "file": self.file,
            "records": self.records.load(Ordering::Relaxed),
            "bytes": self.bytes.load(Ordering::Relaxed),
            "started": self.started,
            "finished": self.finished,
            "error": self.error,
        })
    }
}

/// Where replay files go, and their limits.
pub struct Settings {
    /// `RELAY_REPLAY_DIR`.
    pub dir: PathBuf,
    /// `RELAY_REPLAY_TIMEOUT`.
    pub timeout: Duration,
    /// `RELAY_REPLAY_MAX_MBPS`: reads paced to this; unlimited when `None`.
    pub max_mbps: Option<f64>,
}

/// Replays to file for one Databento relay.
pub struct Replays {
    key: String,
    dataset: String,
    symbols: Vec<String>,
    stype_in: SType,
    dir: PathBuf,
    timeout: Duration,
    max_mbps: Option<f64>,
    /// Shared with `/session`.
    one: Arc<Semaphore>,
    jobs: Mutex<VecDeque<Job>>,
    next: AtomicU64,
}

impl Replays {
    #[must_use]
    pub fn new(
        key: &str,
        dataset: &str,
        symbols: &[String],
        stype_in: SType,
        settings: Settings,
        one: Arc<Semaphore>,
    ) -> Arc<Self> {
        Arc::new(Self {
            key: key.to_owned(),
            dataset: dataset.to_owned(),
            symbols: symbols.to_vec(),
            stype_in,
            dir: settings.dir,
            timeout: settings.timeout,
            max_mbps: settings.max_mbps,
            one,
            jobs: Mutex::default(),
            next: AtomicU64::new(1),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, VecDeque<Job>> {
        self.jobs.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Starts a replay to file in the background, or says why not.
    fn launch(self: &Arc<Self>, schema: Schema, start: i64) -> Result<Value, (StatusCode, String)> {
        let Ok(permit) = self.one.clone().try_acquire_owned() else {
            let running = self
                .lock()
                .iter()
                .find(|j| j.state == "running")
                .map(|j| j.id);
            return Err((
                StatusCode::CONFLICT,
                match running {
                    Some(id) => format!("replay {id} is running; one at a time"),
                    None => "a /session replay is running; try again shortly".to_owned(),
                },
            ));
        };
        let paths = paths(&self.dir, &self.dataset, &self.symbols, schema, start);
        if let Some(day) = paths.file.parent() {
            use std::os::unix::fs::DirBuilderExt as _;
            fs::DirBuilder::new()
                .recursive(true)
                .mode(0o755)
                .create(day)
                .map_err(|e| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("cannot create {}: {e}", day.display()),
                    )
                })?;
        }
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let (records, bytes) = (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
        let job = Job {
            id,
            schema,
            start,
            file: paths.file.clone(),
            started: tinytrader_observe::unix_seconds() as i64,
            finished: None,
            state: "running",
            error: None,
            records: records.clone(),
            bytes: bytes.clone(),
        };
        let answer = json!({ "id": id, "file": job.file, "state": job.state });
        {
            let mut jobs = self.lock();
            while jobs.len() >= KEPT {
                match jobs.iter().position(|j| j.state != "running") {
                    Some(i) => drop(jobs.remove(i)),
                    None => break,
                }
            }
            jobs.push_back(job);
        }
        let me = self.clone();
        tokio::spawn(async move {
            // The turn is held until the job is marked finished, whatever
            // becomes of the replay (a panic in it too).
            let _permit = permit;
            let run = {
                let me = me.clone();
                tokio::spawn(async move { me.run(&paths, schema, start, &records, bytes).await })
            };
            let result = run
                .await
                .unwrap_or_else(|e| Err(format!("the replay stopped unexpectedly: {e}")));
            let mut jobs = me.lock();
            let Some(j) = jobs.iter_mut().find(|j| j.id == id) else {
                return;
            };
            j.finished = Some(tinytrader_observe::unix_seconds() as i64);
            match result {
                Ok(w) => {
                    j.file = w.file;
                    j.state = if w.completed { "done" } else { "failed" };
                    j.error = w.error;
                }
                Err(e) => {
                    j.state = "failed";
                    j.error = Some(e);
                }
            }
            if let Some(e) = &j.error {
                eprintln!("relay: replay {id} to {}: {e}", j.file.display());
            }
        });
        Ok(answer)
    }

    async fn run(
        &self,
        paths: &Paths,
        schema: Schema,
        start: i64,
        records: &AtomicU64,
        bytes: Arc<AtomicU64>,
    ) -> Result<Written, String> {
        let deadline = Instant::now() + self.timeout;
        let open = LiveReplay::open(
            &self.key,
            &self.dataset,
            &self.symbols,
            self.stype_in,
            schema,
            start,
        );
        let mut src = tokio::time::timeout_at(deadline, open)
            .await
            .map_err(|_| "the replay timed out connecting".to_owned())??;
        let about = About {
            dataset: &self.dataset,
            symbols: &self.symbols,
            stype_in: self.stype_in,
            schema,
            start,
        };
        let limits = Limits {
            deadline,
            max_mbps: self.max_mbps,
        };
        let w = write_replay(&mut src, paths, &about, limits, records, bytes).await;
        src.close().await;
        if w.is_err() {
            let tmp = paths.tmp.clone();
            let _ = tokio::task::spawn_blocking(move || fs::remove_file(tmp)).await;
        }
        w
    }
}

#[derive(Deserialize)]
struct Ask {
    schema: Option<String>,
    start: Option<String>,
}

/// `/replay` and `/replay/{id}`; `Err` names why replay files are off.
pub fn router(replays: Result<Arc<Replays>, String>) -> Router {
    Router::new()
        .route("/replay", get(list).post(post))
        .route("/replay/{id}", get(one))
        .with_state(Arc::new(replays))
}

type Shared = State<Arc<Result<Arc<Replays>, String>>>;
type Answer = (StatusCode, Json<Value>);

fn refuse(code: StatusCode, e: impl Into<String>) -> Answer {
    (code, Json(json!({ "error": e.into() })))
}

async fn post(State(r): Shared, Query(ask): Query<Ask>) -> Answer {
    let r = match r.as_ref() {
        Ok(r) => r,
        Err(e) => return refuse(StatusCode::NOT_FOUND, e.clone()),
    };
    let schema = match replay_schema(ask.schema.as_deref()) {
        Ok(s) => s,
        Err(e) => return refuse(StatusCode::BAD_REQUEST, e),
    };
    let now = tinytrader_observe::unix_seconds() as i64;
    let start = match replay_start(ask.start.as_deref(), now) {
        Ok(s) => s,
        Err(e) => return refuse(StatusCode::BAD_REQUEST, e),
    };
    match r.launch(schema, start) {
        Ok(v) => (StatusCode::ACCEPTED, Json(v)),
        Err((code, e)) => refuse(code, e),
    }
}

async fn list(State(r): Shared) -> Answer {
    match r.as_ref() {
        Ok(r) => {
            let jobs: Vec<Value> = r.lock().iter().rev().map(Job::json).collect();
            (StatusCode::OK, Json(json!({ "replays": jobs })))
        }
        Err(e) => refuse(StatusCode::NOT_FOUND, e.clone()),
    }
}

async fn one(State(r): Shared, UrlPath(id): UrlPath<u64>) -> Answer {
    match r.as_ref() {
        Ok(r) => match r.lock().iter().find(|j| j.id == id) {
            Some(j) => (StatusCode::OK, Json(j.json())),
            None => refuse(StatusCode::NOT_FOUND, format!("no replay {id}")),
        },
        Err(e) => refuse(StatusCode::NOT_FOUND, e.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use databento::dbn::decode::{DbnDecoder, DbnMetadata as _, DecodeRecordRef as _};
    use databento::dbn::{RecordBuf, TradeMsg};

    fn spy() -> Vec<String> {
        vec!["SPY.OPT".to_owned()]
    }

    #[test]
    fn names_files_by_day_parents_schema_and_start() {
        // 2026-10-12 09:30 New York.
        let start = tinytrader_schedule::ny_to_unix(2026, 10, 12, 9, 30);
        let p = paths(
            Path::new("/r"),
            "OPRA.PILLAR",
            &spy(),
            Schema::Cbbo1M,
            start,
        );
        let base = format!("/r/2026-10-12/OPRA.PILLAR-SPY-cbbo-1m-{start}");
        assert_eq!(p.file, PathBuf::from(format!("{base}.dbn.zst")));
        assert_eq!(p.tmp, PathBuf::from(format!("{base}.dbn.zst.tmp")));
        assert_eq!(p.partial, PathBuf::from(format!("{base}.partial.dbn.zst")));
        assert_eq!(
            sidecar(&p.file),
            PathBuf::from(format!("{base}.dbn.zst.json"))
        );
        // Several parents; the day is New York's, not UTC's (21:00 ET is
        // the next day in UTC).
        let late = tinytrader_schedule::ny_to_unix(2026, 10, 12, 21, 0);
        let p = paths(
            Path::new("/r"),
            "OPRA.PILLAR",
            &["SPY.OPT".into(), "SPXW.OPT".into(), "a/b".into()],
            Schema::Tcbbo,
            late,
        );
        assert_eq!(
            p.file,
            PathBuf::from(format!(
                "/r/2026-10-12/OPRA.PILLAR-SPY_SPXW_a_b-tcbbo-{late}.dbn.zst"
            ))
        );
    }

    /// Records from memory.
    struct Fake {
        metadata: Metadata,
        records: VecDeque<RecordBuf>,
        current: Option<RecordBuf>,
    }

    impl Fake {
        fn new(trades: u64, complete: bool) -> Self {
            let mut records: VecDeque<RecordBuf> = (0..trades)
                .map(|i| {
                    let mut t = TradeMsg::default();
                    t.hd.ts_event = 1_000 + i;
                    t.price = 1_000_000_000;
                    RecordBuf::from(t)
                })
                .collect();
            records.push_back(RecordBuf::from(SystemMsg::heartbeat(2_000)));
            if complete {
                let done =
                    SystemMsg::new(5_000, Some(SystemCode::ReplayCompleted), "done").unwrap();
                records.push_back(RecordBuf::from(done));
            }
            // As a live gateway's header may be: no schema, symbology or
            // symbols.
            let metadata = Metadata::builder()
                .dataset("OPRA.PILLAR".to_owned())
                .schema(None)
                .start(1_000)
                .stype_in(None)
                .stype_out(SType::InstrumentId)
                .build();
            Self {
                metadata,
                records,
                current: None,
            }
        }
    }

    impl RecordSource for Fake {
        fn metadata(&self) -> &Metadata {
            &self.metadata
        }
        async fn next_record(&mut self) -> Result<Option<RecordRef<'_>>, String> {
            self.current = self.records.pop_front();
            Ok(self.current.as_ref().map(RecordRef::from))
        }
    }

    fn temp_dir(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("relay-replay-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        d
    }

    async fn write(dir: &Path, fake: &mut Fake) -> (Paths, Written) {
        let p = paths(dir, "OPRA.PILLAR", &spy(), Schema::Trades, 1_791_811_800);
        fs::create_dir_all(p.file.parent().unwrap()).unwrap();
        let about = About {
            dataset: "OPRA.PILLAR",
            symbols: &spy(),
            stype_in: SType::Parent,
            schema: Schema::Trades,
            start: 1_791_811_800,
        };
        let limits = Limits {
            deadline: Instant::now() + Duration::from_secs(10),
            max_mbps: None,
        };
        let w = write_replay(fake, &p, &about, limits, &AtomicU64::new(0), Arc::default())
            .await
            .unwrap();
        (p, w)
    }

    fn read_back(file: &Path) -> (Metadata, Vec<u64>) {
        let mut d = DbnDecoder::from_zstd_file(file).unwrap();
        let meta = d.metadata().clone();
        let mut ts = Vec::new();
        while let Some(r) = d.decode_record_ref().unwrap() {
            ts.push(r.header().ts_event);
        }
        (meta, ts)
    }

    #[tokio::test]
    async fn a_completed_replay_is_renamed_with_its_sidecar() {
        let dir = temp_dir("done");
        // Enough records for several frames.
        let n = 2 * (FRAME_BYTES / std::mem::size_of::<TradeMsg>()) as u64;
        let (p, w) = write(&dir, &mut Fake::new(n, true)).await;
        assert!(w.completed && w.error.is_none());
        assert_eq!(w.file, p.file);
        assert!(!p.tmp.exists() && !p.partial.exists());
        let (meta, ts) = read_back(&p.file);
        // The session's header said none of these; the file does.
        assert_eq!(meta.symbols, spy());
        // Mixed: the stream holds a system record beside the trades, so the
        // header names no one schema (the sidecar does).
        assert_eq!(meta.schema, None);
        assert_eq!(meta.stype_in, Some(SType::Parent));
        assert_eq!(meta.start, 1_791_811_800 * 1_000_000_000);
        assert_eq!(ts.len() as u64, n + 1);
        assert_eq!(w.end, Some(1_000 + n - 1));
        let side: Value = serde_json::from_slice(&fs::read(sidecar(&p.file)).unwrap()).unwrap();
        assert_eq!(side["format"], FORMAT);
        assert_eq!(side["completed"], true);
        assert_eq!(side["records"], n + 1);
        assert_eq!(side["schema"], "trades");
        assert_eq!(side["parents"], json!(["SPY.OPT"]));
        let sha = format!("{:x}", Sha256::digest(fs::read(&p.file).unwrap()));
        assert_eq!(side["sha256"], sha);
        let _ = fs::remove_dir_all(&dir);
    }

    /// Records from memory of several kinds, as a replay holds: a symbol
    /// mapping, trades, and the system records around them.
    fn mixed() -> Fake {
        let mut f = Fake::new(5, true);
        let m = databento::dbn::SymbolMappingMsg::new(
            7,
            900,
            SType::Parent,
            "SPY.OPT",
            SType::RawSymbol,
            "SPY   261027C00580000",
            0,
            0,
        )
        .unwrap();
        f.records.push_front(RecordBuf::from(m));
        for (i, r) in f.records.iter_mut().enumerate() {
            if let Some(t) = r.get_mut::<TradeMsg>() {
                t.hd.instrument_id = 7;
                t.size = u32::try_from(i).unwrap();
            }
        }
        f
    }

    /// The replay file for Python's round trip through databento's own reader
    /// (clients/python/tests/test_replay_file.py): written by the relay's
    /// writer, compared byte for byte, `RELAY_FIXTURE_WRITE=1` writes it again.
    #[tokio::test]
    async fn the_python_round_trip_fixture_is_what_the_writer_writes() {
        let dir = temp_dir("fixture");
        let (p, _) = write(&dir, &mut mixed()).await;
        let (meta, ts) = read_back(&p.file);
        assert_eq!(meta.schema, None);
        // The mapping, the trades and ReplayCompleted (heartbeats aren't kept).
        assert_eq!(ts.len(), 1 + 5 + 1);
        let got = fs::read(&p.file).unwrap();
        let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../clients/python/tests/fixtures/replay-mixed.dbn.zst");
        if std::env::var("RELAY_FIXTURE_WRITE").is_ok_and(|v| v == "1") {
            fs::create_dir_all(fixture.parent().unwrap()).unwrap();
            fs::write(&fixture, &got).unwrap();
            panic!("fixture written; unset RELAY_FIXTURE_WRITE and run again");
        }
        assert_eq!(
            got,
            fs::read(&fixture).unwrap(),
            "the writer's output changed"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    /// A file written with a one-schema header is copied with its schema unset
    /// and every record unchanged; the original is never written, and an
    /// existing copy is refused.
    #[test]
    fn an_old_replay_file_is_copied_with_a_mixed_header() {
        use relay::replay_fix::{copy_path, schema_none_copy};
        let dir = temp_dir("fix");
        fs::create_dir_all(&dir).unwrap();
        let old = dir.join("OPRA.PILLAR-SPY-trades-1.dbn.zst");
        let mut meta = mixed().metadata;
        meta.schema = Some(Schema::Trades);
        let mut f = DbnFile::create(&old.with_extension("tmp"), &meta, Arc::default()).unwrap();
        let n = 2 * (FRAME_BYTES / std::mem::size_of::<TradeMsg>());
        let mut fake = mixed();
        for _ in 0..n {
            let mut t = TradeMsg::default();
            t.hd.ts_event = 3_000;
            fake.records.push_back(RecordBuf::from(t));
        }
        for r in &fake.records {
            f.push(RecordRef::from(r).as_ref()).unwrap();
        }
        f.finish(&old).unwrap();
        let before = fs::read(&old).unwrap();
        let copy = copy_path(&old);
        assert_eq!(
            copy,
            dir.join("OPRA.PILLAR-SPY-trades-1.schema-none.dbn.zst")
        );
        assert!(schema_none_copy(&old, &copy).unwrap());
        assert_eq!(fs::read(&old).unwrap(), before, "the original is untouched");
        let (m_old, ts_old) = read_back(&old);
        let (m_new, ts_new) = read_back(&copy);
        assert_eq!(m_old.schema, Some(Schema::Trades));
        assert_eq!(m_new.schema, None);
        assert_eq!(ts_new, ts_old);
        assert!(ts_new.len() > n, "every frame copied");
        assert!(
            schema_none_copy(&old, &copy).is_err(),
            "an existing copy is refused"
        );
        // A copy of a mixed file is the same records, its header unchanged.
        let again = dir.join("again.dbn.zst");
        assert!(!schema_none_copy(&copy, &again).unwrap());
        assert_eq!(read_back(&again), read_back(&copy));
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn an_unfinished_replay_is_kept_as_partial() {
        let dir = temp_dir("partial");
        let (p, w) = write(&dir, &mut Fake::new(10, false)).await;
        assert!(!w.completed);
        assert!(
            w.error
                .as_deref()
                .unwrap()
                .contains("before the replay completed")
        );
        assert_eq!(w.file, p.partial);
        assert!(!p.file.exists() && !p.tmp.exists());
        assert_eq!(read_back(&p.partial).1.len(), 10);
        let side: Value = serde_json::from_slice(&fs::read(sidecar(&p.partial)).unwrap()).unwrap();
        assert_eq!(side["completed"], false);
        // A later complete replay supersedes it.
        let (p, _) = write(&dir, &mut Fake::new(3, true)).await;
        assert!(p.file.exists() && !p.partial.exists() && !sidecar(&p.partial).exists());
        let _ = fs::remove_dir_all(&dir);
    }

    fn replays(one: Arc<Semaphore>, dir: PathBuf) -> Arc<Replays> {
        Replays::new(
            "k",
            "OPRA.PILLAR",
            &spy(),
            SType::Parent,
            Settings {
                dir,
                timeout: Duration::from_secs(60),
                max_mbps: None,
            },
            one,
        )
    }

    #[test]
    fn paces_reads_to_the_limit() {
        // 1 MB at 8 Mb/s is due after a second.
        let mb = 1_000_000;
        assert_eq!(
            pace(mb, Duration::from_millis(250), 8.0),
            Some(Duration::from_millis(750))
        );
        assert_eq!(pace(mb, Duration::from_secs(2), 8.0), None);
        assert_eq!(pace(mb, Duration::ZERO, 0.0), None);
    }

    #[tokio::test]
    async fn off_without_a_directory() {
        let off: Shared = State(Arc::new(Err("replay files are off".to_owned())));
        let ask = Ask {
            schema: None,
            start: None,
        };
        let (code, Json(v)) = post(off.clone(), Query(ask)).await;
        assert_eq!(code, StatusCode::NOT_FOUND);
        assert_eq!(v["error"], "replay files are off");
        assert_eq!(list(off.clone()).await.0, StatusCode::NOT_FOUND);
        assert_eq!(one(off, UrlPath(1)).await.0, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn one_at_a_time_sharing_the_session_turn() {
        let dir = temp_dir("busy");
        let turn = Arc::new(Semaphore::new(1));
        let r = replays(turn.clone(), dir.clone());
        let state: Shared = State(Arc::new(Ok(r.clone())));
        // `/session` (or another replay) holds the turn.
        let held = turn.clone().try_acquire_owned().unwrap();
        let ask = || Ask {
            schema: Some("tcbbo".to_owned()),
            start: None,
        };
        let (code, Json(v)) = post(state.clone(), Query(ask())).await;
        assert_eq!(code, StatusCode::CONFLICT, "{v}");
        // Nothing was started or created.
        assert!(r.lock().is_empty());
        assert!(!dir.exists());
        drop(held);
        // Bad requests are refused before taking the turn.
        let bad = Ask {
            schema: Some("cmbp-1".to_owned()),
            start: None,
        };
        assert_eq!(
            post(state.clone(), Query(bad)).await.0,
            StatusCode::BAD_REQUEST
        );
        let old = Ask {
            schema: None,
            start: Some("2020-01-01T00:00:00Z".to_owned()),
        };
        assert_eq!(
            post(state.clone(), Query(old)).await.0,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(turn.available_permits(), 1);
        assert_eq!(one(state, UrlPath(7)).await.0, StatusCode::NOT_FOUND);
    }
}
