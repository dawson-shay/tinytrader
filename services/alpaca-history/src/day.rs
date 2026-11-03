//! One symbol-day: both lanes fetched, merged and encoded, written as two
//! store batches (the day's records, and its trade conditions), checked by
//! reading them back, and handed to the store to file.
//!
//! A day counts only when both lanes reached their last page and the files
//! read back with the counts and times the pages said (decision 0014). A day
//! is one file per schema, as Databento's are; the receipts (every page's
//! rows, times and SHA-256) go with it to the store's `provenance/`.

use std::num::NonZeroU64;
use std::path::{Path, PathBuf};

use dbn::decode::{DbnMetadata, DecodeRecordRef};
use dbn::encode::{DbnMetadataEncoder, EncodeRecord};
use dbn::{Metadata, SType, Schema, SymbolMapping};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tinytrader_alpaca::{iex, sip};
use tinytrader_dbn::TradeConditions;

use crate::fetch::{Alpaca, Failure, day_ns, lane};

/// Which of Alpaca's feeds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Feed {
    Sip,
    Iex,
}

impl Feed {
    pub fn name(self) -> &'static str {
        match self {
            Self::Sip => "sip",
            Self::Iex => "iex",
        }
    }
    pub fn dataset(self) -> &'static str {
        match self {
            Self::Sip => sip::DATASET,
            Self::Iex => iex::DATASET,
        }
    }
    fn schema(self) -> Schema {
        match self {
            Self::Sip => Schema::Cmbp1,
            Self::Iex => Schema::Mbp1,
        }
    }
    /// The store batch ids of a symbol-day: its records, then its conditions.
    pub fn batches(self, symbol: &str, day: &str) -> (String, String) {
        let d = day.replace('-', "");
        let main = format!("ALPACA-{}-{symbol}-{d}", self.name().to_uppercase());
        (main.clone(), format!("{main}-conditions"))
    }
}

/// What a day came to.
#[derive(Debug, Default, serde::Serialize)]
pub struct Outcome {
    pub trades: usize,
    pub quotes: usize,
    pub pages: usize,
    pub bytes: u64,
}

#[derive(Deserialize)]
struct Page<T> {
    #[serde(alias = "trades", alias = "quotes", default = "none")]
    rows: Option<Vec<T>>,
    #[serde(default)]
    next_page_token: Option<String>,
}
fn none<T>() -> Option<Vec<T>> {
    None
}

/// A page's rows (each parsed by `row`) and its next token.
fn page<'a, R: Deserialize<'a>, T>(
    body: &'a [u8],
    row: impl Fn(&R) -> Result<T, String>,
) -> Result<(Vec<T>, Option<String>), String> {
    let p: Page<R> = serde_json::from_slice(body).map_err(|e| format!("unreadable: {e}"))?;
    let rows = p.rows.unwrap_or_default().iter().map(row).collect::<Result<Vec<_>, _>>()?;
    Ok((rows, p.next_page_token.filter(|t| !t.is_empty())))
}

fn metadata(feed: Feed, symbol: &str, day: &str, schema: Option<Schema>, instrument: u32) -> Result<Metadata, String> {
    let start = day_ns(day).ok_or("bad day")?;
    let date = time::Date::parse(day, time::macros::format_description!("[year]-[month]-[day]"))
        .map_err(|e| e.to_string())?;
    Ok(Metadata::builder()
        .dataset(feed.dataset())
        .schema(schema)
        .start(start)
        .end(NonZeroU64::new(start + 86_400 * 1_000_000_000))
        .stype_in(Some(SType::RawSymbol))
        .stype_out(SType::InstrumentId)
        .symbols(vec![symbol.to_owned()])
        .mappings(vec![SymbolMapping {
            raw_symbol: symbol.to_owned(),
            intervals: vec![dbn::MappingInterval {
                start_date: date,
                end_date: date.next_day().ok_or("no next day")?,
                symbol: instrument.to_string(),
            }],
        }])
        .build())
}

/// Write records (each by `put`) as a zstd DBN file, finished and synced.
fn write_dbn(path: &Path, meta: &Metadata, put: impl FnOnce(&mut dyn std::io::Write) -> Result<(), String>) -> Result<(), String> {
    let file = std::fs::File::create(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let mut z = zstd::Encoder::new(std::io::BufWriter::new(file), 3).map_err(|e| e.to_string())?;
    DbnMetadataEncoder::new(&mut z).encode(meta).map_err(|e| e.to_string())?;
    put(&mut z)?;
    let buf = z.finish().map_err(|e| e.to_string())?;
    let file = buf.into_inner().map_err(|e| e.to_string())?;
    file.sync_all().map_err(|e| e.to_string())
}

/// Read a file back: (records, first ts_event, last ts_event).
fn read_back(path: &Path) -> Result<(usize, Option<u64>, Option<u64>), String> {
    let mut d = dbn::decode::dbn::Decoder::from_zstd_file(path).map_err(|e| e.to_string())?;
    let _ = d.metadata();
    let (mut n, mut first, mut last) = (0, None, None);
    while let Some(r) = d.decode_record_ref().map_err(|e| e.to_string())? {
        let t = r.header().ts_event;
        first.get_or_insert(t);
        last = Some(t);
        n += 1;
    }
    Ok((n, first, last))
}

fn sha(path: &Path) -> Result<(u64, String), String> {
    let bytes = std::fs::read(path).map_err(|e| format!("{}: {e}", path.display()))?;
    Ok((bytes.len() as u64, format!("{:x}", Sha256::digest(&bytes))))
}

/// Fetch, encode and write one symbol-day into `dir` as two batch folders.
/// Returns the folders (records, conditions) and the outcome.
pub async fn build(alpaca: &Alpaca, feed: Feed, symbol: &str, day: &str, dir: &Path) -> Result<(PathBuf, PathBuf, Outcome), Failure> {
    let bad = |e: String| Failure::PageIntegrity(e);
    let f = feed.name();
    // Both lanes at once, sharing the pace.
    let (main_recs, conds, receipts, outcome, instrument) = match feed {
        Feed::Sip => {
            let (t, q) = tokio::join!(
                lane(alpaca, f, symbol, "trades", day, |b| page(b, sip::RawTrade::parse), |r: &sip::Trade| r.ts),
                lane(alpaca, f, symbol, "quotes", day, |b| page(b, sip::RawQuote::parse), |r: &sip::Quote| r.ts),
            );
            let (t, q) = (t?, q?);
            let receipts = json!({ "trades": t.receipts, "quotes": q.receipts });
            let out = Outcome { trades: t.rows.len(), quotes: q.rows.len(), pages: 0, bytes: 0 };
            let rows = sip::merge(t.rows, q.rows).map_err(bad)?;
            let (recs, conds) = sip::encode_day(symbol, &rows).map_err(bad)?;
            (Records::Sip(recs), conds, receipts, out, sip::instrument_id(symbol))
        }
        Feed::Iex => {
            let (t, q) = tokio::join!(
                lane(alpaca, f, symbol, "trades", day, |b| page(b, iex::RawTrade::parse), |r: &iex::Trade| r.ts),
                lane(alpaca, f, symbol, "quotes", day, |b| page(b, iex::RawQuote::parse), |r: &iex::Quote| r.ts),
            );
            let (t, q) = (t?, q?);
            let receipts = json!({ "trades": t.receipts, "quotes": q.receipts });
            let out = Outcome { trades: t.rows.len(), quotes: q.rows.len(), pages: 0, bytes: 0 };
            let rows = iex::merge(t.rows, q.rows).map_err(bad)?;
            let (recs, conds) = iex::encode_day(symbol, &rows).map_err(bad)?;
            (Records::Iex(recs), conds, receipts, out, iex::instrument_id(symbol))
        }
    };
    write_batches(feed, symbol, day, dir, main_recs, &conds, receipts, outcome, instrument).map_err(bad)
}

enum Records {
    Sip(Vec<dbn::Cmbp1Msg>),
    Iex(Vec<dbn::Mbp1Msg>),
}

#[allow(clippy::too_many_arguments)]
fn write_batches(
    feed: Feed,
    symbol: &str,
    day: &str,
    dir: &Path,
    records: Records,
    conds: &[TradeConditions],
    receipts: Value,
    mut outcome: Outcome,
    instrument: u32,
) -> Result<(PathBuf, PathBuf, Outcome), String> {
    let (main_id, cond_id) = feed.batches(symbol, day);
    let d = day.replace('-', "");
    let schema = feed.schema();
    let (main_dir, cond_dir) = (dir.join(&main_id), dir.join(&cond_id));
    for p in [&main_dir, &cond_dir] {
        // A leftover from an interrupted attempt is rebuilt from scratch.
        if p.exists() {
            for e in std::fs::read_dir(p).map_err(|e| e.to_string())? {
                let e = e.map_err(|e| e.to_string())?;
                std::fs::remove_file(e.path()).map_err(|e| e.to_string())?;
            }
        } else {
            std::fs::create_dir_all(p).map_err(|e| e.to_string())?;
        }
    }
    let main_name = format!("alpaca-{}-{d}.{}.dbn.zst", feed.name(), schema.as_str());
    let cond_name = format!("alpaca-{}-{d}.conditions.dbn.zst", feed.name());
    let main_path = main_dir.join(&main_name);
    let n_records = match &records {
        Records::Sip(r) => r.len(),
        Records::Iex(r) => r.len(),
    };
    let meta = metadata(feed, symbol, day, Some(schema), instrument)?;
    write_dbn(&main_path, &meta, |w| {
        let mut enc = dbn::encode::dbn::RecordEncoder::new(w);
        match &records {
            Records::Sip(r) => r.iter().try_for_each(|x| enc.encode_record(x)),
            Records::Iex(r) => r.iter().try_for_each(|x| enc.encode_record(x)),
        }
        .map_err(|e| e.to_string())
    })?;
    let cond_path = cond_dir.join(&cond_name);
    write_dbn(&cond_path, &metadata(feed, symbol, day, None, instrument)?, |w| {
        for c in conds {
            w.write_all(&c.encode().map_err(|e| format!("{e:?}"))?).map_err(|e| e.to_string())?;
        }
        Ok(())
    })?;

    // Read back: the counts and first and last times the pages said.
    let (n, first, last) = read_back(&main_path)?;
    let lane_times = |lane: &str| -> (Option<u64>, Option<u64>) {
        let pages = receipts[lane].as_array().cloned().unwrap_or_default();
        let first = pages.iter().filter_map(|p| p["first_ns"].as_u64()).min();
        let last = pages.iter().filter_map(|p| p["last_ns"].as_u64()).max();
        (first, last)
    };
    let ((tf, tl), (qf, ql)) = (lane_times("trades"), lane_times("quotes"));
    let want_first = [tf, qf].into_iter().flatten().min();
    let want_last = [tl, ql].into_iter().flatten().max();
    if n != n_records || n != outcome.trades + outcome.quotes || first != want_first || last != want_last {
        return Err(format!(
            "the written day doesn't match its pages: {n} records ({} trades + {} quotes paged), times {first:?}..{last:?} vs {want_first:?}..{want_last:?}",
            outcome.trades, outcome.quotes
        ));
    }
    let (cn, _, _) = read_back(&cond_path)?;
    if cn != outcome.trades {
        return Err(format!("{cn} condition records for {} trades", outcome.trades));
    }
    outcome.pages = receipts["trades"].as_array().map_or(0, Vec::len) + receipts["quotes"].as_array().map_or(0, Vec::len);

    let (msize, msha) = sha(&main_path)?;
    let (csize, csha) = sha(&cond_path)?;
    outcome.bytes = msize + csize;
    let request = json!({
        "provider": "alpaca", "feed": feed.name(), "symbol": symbol, "day": day,
        "window": [format!("{day}T00:00:00Z"), format!("{day}T23:59:59.999999999Z")], "limit": 10000, "sort": "asc",
    });
    let encoder = format!("tinytrader-alpaca {} ({})", env!("CARGO_PKG_VERSION"), schema.as_str());
    let receipt = json!({
        "format": "tinytrader.alpaca-history.receipts/1", "request": request, "encoder": encoder,
        "fetched_at": tinytrader_observe::unix_seconds(), "trades": outcome.trades, "quotes": outcome.quotes,
        "records": n, "first_ns": first, "last_ns": last, "file": { "name": main_name, "size": msize, "sha256": msha },
        "pages": receipts,
    });
    let binding = json!({
        "format": "tinytrader.alpaca-history.conditions-binding/1", "batch": main_id,
        "main": { "name": main_name, "size": msize, "sha256": msha }, "trades": outcome.trades,
        "rule": "the nth conditions record belongs to the nth trade (action T) of the main file",
    });
    for (dir, id, s, data, size, hash, extra, extra_name) in [
        (&main_dir, &main_id, Some(schema.as_str()), &main_name, msize, &msha, &receipt, "receipts.json"),
        (&cond_dir, &cond_id, Some("conditions"), &cond_name, csize, &csha, &binding, "binding.json"),
    ] {
        let schema = s.unwrap_or_default();
        let md = json!({
            "job_id": id,
            "query": { "dataset": feed.dataset(), "schema": schema, "symbols": [symbol], "stype_in": "raw_symbol",
                       "start": format!("{day}T00:00:00Z"), "end": format!("{day}T23:59:59.999999999Z"),
                       "encoding": "dbn", "compression": "zstd", "provider": "alpaca", "feed": feed.name() },
            "customizations": { "split_duration": "day", "split_symbols": false },
        });
        let mut files = vec![json!({ "filename": data, "size": size, "hash": format!("sha256:{hash}") })];
        for (name, v) in [("metadata.json", &md), (extra_name, extra)] {
            let text = serde_json::to_vec_pretty(v).map_err(|e| e.to_string())?;
            let p = dir.join(name);
            std::fs::write(&p, &text).map_err(|e| e.to_string())?;
            files.push(json!({ "filename": name, "size": text.len(), "hash": format!("sha256:{:x}", Sha256::digest(&text)) }));
        }
        let manifest = json!({ "job_id": id, "files": files });
        std::fs::write(dir.join("manifest.json"), serde_json::to_vec_pretty(&manifest).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;
    }
    Ok((main_dir, cond_dir, outcome))
}

/// A filing that didn't happen: the store busy past the wait (`busy`: try the day
/// again later, keeping its spool), or a refusal or error.
#[derive(Debug)]
pub struct FileError {
    pub busy: bool,
    pub message: String,
}

impl From<String> for FileError {
    fn from(message: String) -> Self {
        Self { busy: false, message }
    }
}

impl From<&str> for FileError {
    fn from(message: &str) -> Self {
        Self { busy: false, message: message.to_owned() }
    }
}

/// How long a busy store (its filing queue full, 503) is waited for, asking again
/// longer apart each time.
const BUSY_WAIT: std::time::Duration =
    if cfg!(test) { std::time::Duration::from_millis(300) } else { std::time::Duration::from_secs(15 * 60) };

/// Hand a batch folder to the store: every file into its incoming area
/// (checked by size and SHA-256 on arrival), then file it. `token` is the
/// store's write token, when it requires one. A busy store (503) is waited for,
/// up to `BUSY_WAIT`; past that the error is `busy`.
pub async fn file(http: &reqwest::Client, store: &str, token: Option<&str>, batch_dir: &Path) -> Result<Value, FileError> {
    let write = |req: reqwest::RequestBuilder| match token {
        Some(t) => req.bearer_auth(t),
        None => req,
    };
    let id = batch_dir.file_name().and_then(|n| n.to_str()).ok_or("bad batch folder")?;
    let mut names: Vec<String> = std::fs::read_dir(batch_dir)
        .map_err(|e| e.to_string())?
        .filter_map(|e| e.ok()?.file_name().into_string().ok())
        .collect();
    names.sort();
    for name in &names {
        let bytes = tokio::fs::read(batch_dir.join(name)).await.map_err(|e| e.to_string())?;
        let hash = format!("{:x}", Sha256::digest(&bytes));
        let r = write(http.put(format!("{store}/incoming/alpaca/{id}/{name}?size={}&sha256={hash}", bytes.len())))
            .body(bytes)
            .send()
            .await
            .map_err(|e| format!("store: {e}"))?;
        if !r.status().is_success() {
            return Err(format!("store refused {name}: {} {}", r.status(), r.text().await.unwrap_or_default()).into());
        }
    }
    let until = std::time::Instant::now() + BUSY_WAIT;
    let mut wait = std::time::Duration::from_millis(if cfg!(test) { 10 } else { 5_000 });
    let r = loop {
        let r = write(http.post(format!("{store}/incorporate/alpaca/{id}")))
            .send()
            .await
            .map_err(|e| format!("store: {e}"))?;
        if r.status() != reqwest::StatusCode::SERVICE_UNAVAILABLE {
            break r;
        }
        if std::time::Instant::now() + wait > until {
            return Err(FileError { busy: true, message: format!("the store stayed busy filing {id}") });
        }
        tokio::time::sleep(wait).await;
        wait = (wait * 2).min(std::time::Duration::from_secs(60));
    };
    let status = r.status();
    let v: Value = r.json().await.unwrap_or(Value::Null);
    if !status.is_success() {
        return Err(format!("store didn't file {id}: {status} {v}").into());
    }
    Ok(v)
}

#[cfg(test)]
mod filing_tests {
    use super::file;
    use axum::routing::{post, put};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// A store answering incorporate 503 `busy` times, then filing.
    async fn store(busy: u32) -> String {
        let left = Arc::new(AtomicU32::new(busy));
        let app = axum::Router::new().route("/incoming/alpaca/{id}/{name}", put(|| async { "ok" })).route(
            "/incorporate/alpaca/{id}",
            post(move || {
                let left = Arc::clone(&left);
                async move {
                    if left.load(Ordering::SeqCst) > 0 {
                        left.fetch_sub(1, Ordering::SeqCst);
                        return (axum::http::StatusCode::SERVICE_UNAVAILABLE, r#"{"error":"busy"}"#);
                    }
                    (axum::http::StatusCode::OK, r#"{"filed":true}"#)
                }
            }),
        );
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
        format!("http://{addr}")
    }

    fn batch(name: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("ah-filing-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        std::fs::write(d.join("x.dbn"), b"x").unwrap();
        d
    }

    /// A busy store is waited for, and then the batch is filed.
    #[tokio::test]
    async fn a_busy_store_is_waited_for() {
        let url = store(2).await;
        let r = file(&reqwest::Client::new(), &url, None, &batch("wait")).await;
        assert!(r.is_ok(), "{r:?}");
    }

    /// One busy past the wait is a `busy` error: the day is tried again, its spool
    /// kept.
    #[tokio::test]
    async fn a_store_busy_past_the_wait_is_busy() {
        let url = store(1000).await;
        let e = file(&reqwest::Client::new(), &url, None, &batch("busy")).await.unwrap_err();
        assert!(e.busy, "{e:?}");
    }
}

