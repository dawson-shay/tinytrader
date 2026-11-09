//! Market data relay (`tinytrader.relay/1`): holds one provider connection and
//! serves its records to any number of local clients. It only forwards; it
//! keeps no recording. See decision 0006 for the protocol.
//! (`POST /replay`, replay.rs, writes a Databento replay to a file when asked
//! to; that is not a recording of the stream.)
//!
//! A client connects over TCP and sends one line of JSON,
//! `{"protocol":"tinytrader.relay/1"}`, optionally with
//! `"start":"<RFC 3339 time>"` and `"conditions":true`. The relay answers with
//! a DBN v3 stream: reference records (definitions, and statistics where the
//! source has them), any catch-up the source supports, a `ReplayCompleted`
//! system record, then live data.
//!
//! Sources:
//! - `databento`: any live dataset (OPRA by default). Catch-up is the day's
//!   reference data only; Databento can't replay `cmbp-1`.
//! - `alpaca-iex`: Alpaca's IEX quotes and trades as `ALPACA.IEX` MBP-1.
//!   Catch-up replays history from `start`; `conditions` interleaves a
//!   `tinytrader.trade-conditions/1` record after each trade.
//!
//! Credentials: `DATABENTO_API_KEY`, or `APCA_API_KEY_ID` and
//! `APCA_API_SECRET_KEY`; each also as `<NAME>_FILE` naming a file.

mod alpaca;
mod bars;
mod databento;
mod hours;
mod hub;
mod protocol;
mod quotes;
mod replay;
mod session;
mod source;
mod tape;

use std::str::FromStr as _;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tinytrader_observe::secret;

use clap::{Parser, ValueEnum};
use databento::Databento;
use hub::{Hub, Upstream};
use serde_json::json;
use source::Source;
use tinytrader_observe::{Observer, State, listen_address};
use tokio::net::TcpListener;

use ::databento::dbn::{Metadata, SType, Schema};

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Provider {
    Databento,
    AlpacaIex,
}

/// Holds one provider connection and serves its records to local clients.
#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// Where the data comes from.
    #[arg(long, env = "RELAY_SOURCE", value_enum, default_value = "databento")]
    source: Provider,

    /// Dataset to relay.
    #[arg(long, env = "RELAY_DATASET", default_value = "OPRA.PILLAR")]
    dataset: String,

    /// Symbols, comma-separated, e.g. `SPY.OPT`.
    #[arg(long, env = "RELAY_SYMBOLS", value_delimiter = ',', required = true)]
    symbols: Vec<String>,

    /// Schema to relay live.
    #[arg(long, env = "RELAY_SCHEMA", default_value = "cmbp-1")]
    schema: String,

    /// In market hours the relay is `degraded` while more than
    /// `RELAY_STALE_SHARE` of its quotes are older than this many minutes.
    /// On 2026-11-09 no XSP quote was older than 10 (1.2% older than 5), while
    /// the frozen quotes were days old.
    #[arg(long, env = "RELAY_STALE_MINUTES", default_value_t = 10)]
    stale_minutes: u64,

    /// The share of quotes (0 to 1) that may be older than
    /// `RELAY_STALE_MINUTES` before the relay says so.
    #[arg(long, env = "RELAY_STALE_SHARE", default_value_t = 0.05)]
    stale_share: f64,

    /// Reference schemas sent live too, and replayed for clients catching
    /// up, comma-separated.
    #[arg(
        long,
        env = "RELAY_REFERENCE",
        value_delimiter = ',',
        default_value = "definition,statistics"
    )]
    reference: Vec<String>,

    /// Symbology of `--symbols`: `parent`, `raw_symbol`, ...
    #[arg(long, env = "RELAY_STYPE_IN", default_value = "parent")]
    stype_in: String,

    /// Address clients connect to.
    #[arg(long, env = "RELAY_LISTEN", default_value = "127.0.0.1:7600")]
    listen: String,

    /// The most clients served at once; more are refused with an error.
    #[arg(long, env = "RELAY_MAX_CLIENTS", default_value_t = 64)]
    max_clients: u64,

    /// Chunks of up to 64 KiB queued per client before it is disconnected.
    #[arg(long, env = "RELAY_CLIENT_QUEUE", default_value_t = 1024)]
    client_queue: usize,

    /// Seconds without data before a heartbeat is sent.
    #[arg(long, env = "RELAY_HEARTBEAT", default_value_t = 10)]
    heartbeat: u64,

    /// Active hours in New York time, e.g. `09:25-16:20`, set relative to the
    /// regular 09:30-16:00 session (an early close ends them early). Outside
    /// them the relay disconnects from its provider and reports itself idle.
    /// Always active when unset.
    #[arg(long, env = "RELAY_HOURS")]
    hours: Option<tinytrader_schedule::Schedule>,

    /// Market calendar for active hours (`{ "days": [{ "date", "open",
    /// "close" }] }`), e.g. an accounts service's `/calendar`. Without it,
    /// every weekday is a trading day.
    #[arg(long, env = "RELAY_CALENDAR")]
    calendar: Option<String>,

    /// Alpaca only: use Alpaca's test stream (symbol `FAKEPACA`), which runs
    /// at any time of day.
    #[arg(long, env = "RELAY_ALPACA_TEST")]
    alpaca_test: bool,

    /// Databento only: where `POST /replay` writes replay files (one day's
    /// replay of every symbol, to DBN). `/replay` is off when unset.
    #[arg(long, env = "RELAY_REPLAY_DIR")]
    replay_dir: Option<std::path::PathBuf>,

    /// Seconds a replay to file may take before it is given up (and kept as
    /// a partial file).
    #[arg(long, env = "RELAY_REPLAY_TIMEOUT", default_value_t = 1800)]
    replay_timeout: u64,

    /// Megabits a second a replay to file may read, at most; unlimited when
    /// unset. A replay during market hours shares the link with the live feed.
    #[arg(long, env = "RELAY_REPLAY_MAX_MBPS")]
    replay_max_mbps: Option<f64>,
}

#[tokio::main]
async fn main() {
    // One TLS crypto provider for the websocket and HTTP clients.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let args = Args::parse();
    if let Err(e) = run(args).await {
        eprintln!("relay: {e}");
        std::process::exit(2);
    }
}

async fn run(args: Args) -> Result<(), String> {
    let schema =
        Schema::from_str(&args.schema).map_err(|e| format!("--schema {}: {e}", args.schema))?;
    let stype_in = SType::from_str(&args.stype_in)
        .map_err(|e| format!("--stype-in {}: {e}", args.stype_in))?;
    // The source's feed below names its own symbology.
    let replay_stype = stype_in;
    let reference = args
        .reference
        .iter()
        .map(|s| Schema::from_str(s).map_err(|e| format!("--reference {s}: {e}")))
        .collect::<Result<Vec<_>, _>>()?;
    let mut alpaca_source: Option<Arc<alpaca::Alpaca>> = None;
    let source: Arc<dyn Source> = match args.source {
        Provider::Databento => Arc::new(Databento {
            key: secret("DATABENTO_API_KEY")?,
            dataset: args.dataset.clone(),
            symbols: args.symbols.clone(),
            schema,
            reference,
            stype_in,
            stale_after: Duration::from_secs(args.stale_minutes.saturating_mul(60)),
            stale_share: args.stale_share,
        }),
        Provider::AlpacaIex => {
            let a = Arc::new(alpaca::Alpaca {
                key: secret("APCA_API_KEY_ID")?,
                secret: secret("APCA_API_SECRET_KEY")?,
                symbols: args.symbols.clone(),
                test: args.alpaca_test,
                http: reqwest::Client::builder()
                    .timeout(Duration::from_secs(30))
                    .build()
                    .map_err(|e| e.to_string())?,
                history_url: alpaca::HISTORY_URL.to_owned(),
            });
            alpaca_source = Some(a.clone());
            a
        }
    };
    let source::Feed {
        dataset,
        schema: schema_name,
        stype_in,
        provider: provider_id,
        name,
        via,
    } = source.feed();

    let metadata = Metadata::builder()
        .dataset(dataset.clone())
        .schema(None)
        .start(0)
        .stype_in(Some(stype_in))
        .stype_out(SType::InstrumentId)
        .symbols(args.symbols.clone())
        .build();
    let hub = Arc::new(Hub::new(
        metadata,
        args.client_queue,
        Duration::from_secs(args.heartbeat.max(1)),
    ));
    // An Alpaca stream takes symbols while open: the tape can watch more (tape.rs).
    if let Some(a) = alpaca_source {
        hub.tape.watch.enable(&args.symbols);
        // Tapes asked for with nothing held are filled from history.
        tokio::spawn(a.tape_history(hub.clone()));
    }

    let feed = format!("{name} {dataset}");
    let observer = Observer::new("relay", env!("CARGO_PKG_VERSION"));
    observer.set_state(State::Starting, &format!("connecting to {provider_id}"));
    observer.describe(json!({
        "role": format!("Relays {dataset} {schema_name} for {} to local clients ({}).", args.symbols.join(", "), protocol::PROTOCOL),
        "upstream": [{ "id": provider_id, "label": provider_id, "sub": dataset, "kind": "flow", "via": via }],
        "streams": { "events": "/events", "series": "/recent", "bars": "/bars", "quotes": "/quotes", "stats": "/stats", "tape": "/tape", "session": "/session", "replay": "/replay", "clients": "/clients" },
        "panels": [{ "heading": format!("{dataset} {}", args.symbols.join(", ")), "note": "Trade prices of the most active instruments, sampled once per second", "decimals": 2 }],
        "relay": { "protocol": protocol::PROTOCOL, "listen": args.listen, "symbols": args.symbols },
        // Relays carrying the same feed on the same hours are one line on a schedule.
        "schedule": [match args.hours {
            Some(h) => json!({ "name": "Market data session", "kind": "window", "hours": h.to_string(), "when": format!("{h} ET on trading days"), "feed": feed, "symbols": args.symbols }),
            None => json!({ "name": "Market data session", "kind": "always", "when": "always", "feed": feed, "symbols": args.symbols }),
        }],
    }));
    register_metrics(&observer, &hub);

    // Bars for charts: the relay's own symbols, with history where the
    // source keeps it.
    hub.bars.keep(&args.symbols);
    // Today's session for one contract, replayed on request (option relays
    // on Databento), and replays to file; one at a time between them.
    let turn = Arc::new(tokio::sync::Semaphore::new(1));
    let (session, replays) = match args.source {
        Provider::Databento => {
            let key = secret("DATABENTO_API_KEY")?;
            let session = session::Session::new(&key, &args.dataset, &args.symbols, turn.clone());
            let replays = match &args.replay_dir {
                Some(dir) => Ok(replay::Replays::new(
                    &key,
                    &args.dataset,
                    &args.symbols,
                    replay_stype,
                    replay::Settings {
                        dir: dir.clone(),
                        timeout: Duration::from_secs(args.replay_timeout.max(1)),
                        max_mbps: args.replay_max_mbps,
                    },
                    turn,
                )),
                None => Err("replay files are off on this relay: set RELAY_REPLAY_DIR".to_owned()),
            };
            (session, replays)
        }
        Provider::AlpacaIex => (
            None,
            Err("replay files are for Databento relays".to_owned()),
        ),
    };
    let observe_at = listen_address("RELAY_OBSERVE_LISTEN", "127.0.0.1:9110");
    let app = observer
        .router()
        .merge(hub.events.router())
        .merge(hub.series.router())
        .merge(bars::router(
            hub.bars.clone(),
            source.clone(),
            session.clone(),
        ))
        .merge(quotes::router(hub.clone()))
        .merge(tape::router(hub.clone()))
        .merge(session.clone().map(session::router).unwrap_or_default())
        .merge(replay::router(replays))
        .merge({
            let hub = hub.clone();
            axum::Router::new().route(
                "/clients",
                axum::routing::get(move || async move {
                    axum::Json(serde_json::json!({ "clients": hub.roster() }))
                }),
            )
        });
    let observe = tokio::net::TcpListener::bind(&observe_at)
        .await
        .map_err(|e| format!("cannot listen on {observe_at}: {e}"))?;
    tokio::spawn(async move {
        if let Err(e) = axum::serve(observe, app).await {
            eprintln!("relay: http server stopped: {e}");
        }
    });

    let clients = TcpListener::bind(&args.listen)
        .await
        .map_err(|e| format!("cannot listen on {}: {e}", args.listen))?;
    eprintln!(
        "relay: {dataset} {schema_name} for {} on {} (endpoints on http://{observe_at})",
        args.symbols.join(","),
        args.listen
    );
    let hours = Arc::new(hours::Hours::new(
        args.hours,
        args.calendar.clone(),
        reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .map_err(|e| e.to_string())?,
    ));
    tokio::spawn(
        source
            .clone()
            .run(hub.clone(), observer.clone(), hours.clone()),
    );

    tokio::select! {
        () = accept(clients, hub, source, args.max_clients) => Ok(()),
        () = shutdown() => {
            observer.set_state(State::Stopping, "shutting down");
            Ok(())
        }
    }
}

/// Serve every client that connects: refuse what the source can't do, catch
/// up from history where the source has it, then live data.
async fn accept(clients: TcpListener, hub: Arc<Hub>, source: Arc<dyn Source>, max_clients: u64) {
    loop {
        let Ok((stream, peer)) = clients.accept().await else {
            continue;
        };
        let _ = stream.set_nodelay(true);
        let (hub, source) = (hub.clone(), source.clone());
        tokio::spawn(async move {
            let Some((join, out)) = hub::read_join(stream, &hub).await else {
                return;
            };
            if hub.clients.load(Ordering::Relaxed) >= max_clients {
                hub.refuse(
                    out,
                    ::databento::dbn::ErrorCode::InternalError,
                    &format!("this relay serves at most {max_clients} clients at once"),
                )
                .await;
                return;
            }
            if join.conditions && !source.conditions() {
                hub.refuse(
                    out,
                    ::databento::dbn::ErrorCode::InvalidSubscription,
                    "this source has no trade conditions",
                )
                .await;
                return;
            }
            let now = protocol::now_ns();
            let requested = join
                .start
                .and_then(|t| u64::try_from(t.unix_timestamp_nanos()).ok());
            // A source with history catches up from the requested start to
            // now, then continues live from the same point.
            let history = requested
                .filter(|&start| start < now)
                .and_then(|start| source.catch_up(start, now, join.conditions));
            let cut = if history.is_some() { now } else { 0 };
            let who = match (&join.client, &join.account) {
                (Some(c), Some(a)) => format!("{c} (trades {a})"),
                (Some(c), None) => c.clone(),
                _ => "an unnamed client".to_owned(),
            };
            hub.events.push(
                format!("{who} joined from {peer}"),
                format!("{who} joined from {peer}"),
            );
            let _guard = hub.enter(hub::ClientInfo {
                client: join.client.clone(),
                account: join.account.clone(),
                peer: peer.to_string(),
                since: tinytrader_observe::unix_seconds(),
                replay: history.is_some(),
                conditions: join.conditions,
            });
            hub.serve(
                out,
                hub::Serve {
                    who: format!("{who} from {peer}"),
                    // The effective start: live from now unless history is sent.
                    start: if history.is_some() {
                        requested.unwrap_or(now)
                    } else {
                        now
                    },
                    conditions: join.conditions,
                    history,
                    cut,
                },
            )
            .await;
        });
    }
}

fn register_metrics(observer: &Observer, hub: &Arc<Hub>) {
    let hub = hub.clone();
    observer.set_metrics(move |w| {
        let c = |a: &std::sync::atomic::AtomicU64| a.load(Ordering::Relaxed) as f64;
        w.gauge(
            "tinytrader_relay_clients",
            "Connected clients.",
            &[],
            c(&hub.clients),
        );
        w.gauge(
            "tinytrader_relay_quotes",
            "Instruments with a latest quote held for /quotes.",
            &[],
            hub.quotes.len() as f64,
        );
        w.gauge(
            "tinytrader_relay_reference_records",
            "Reference records (definitions, statistics) held for joining clients.",
            &[],
            hub.reference_len() as f64,
        );
        w.counter(
            "tinytrader_relay_upstream_skips_total",
            "Times the provider skipped records because the relay read too slowly.",
            &[],
            c(&hub.upstream_skips),
        );
        w.gauge(
            "tinytrader_relay_upstream_up",
            "1 while the provider connection is up.",
            &[],
            f64::from(u8::from(hub.upstream() == Upstream::Up)),
        );
        let last = hub.last_record_ns.load(Ordering::Relaxed);
        if last > 0 {
            w.gauge(
                "tinytrader_relay_last_record_age_seconds",
                "Seconds since the last record arrived from the provider.",
                &[],
                protocol::now_ns().saturating_sub(last) as f64 / 1e9,
            );
        }
        if last > 0 {
            w.gauge(
                "tinytrader_relay_behind_seconds",
                "How far behind the provider the relay read its latest quote: when it was published less when the provider received it.",
                &[],
                c(&hub.behind_ns) / 1e9,
            );
        }
        if let Some(peak) = hub.behind_peak() {
            w.gauge(
                "tinytrader_relay_behind_peak_seconds",
                "The most the relay fell behind the provider in this minute or the last.",
                &[],
                peak as f64 / 1e9,
            );
        }
        w.counter(
            "tinytrader_relay_records_total",
            "Records received from the provider.",
            &[],
            c(&hub.records),
        );
        w.counter(
            "tinytrader_relay_bytes_total",
            "Bytes received from the provider.",
            &[],
            c(&hub.bytes),
        );
        w.counter(
            "tinytrader_relay_upstream_reconnects_total",
            "Provider connections restored after a loss.",
            &[],
            c(&hub.reconnects),
        );
        w.counter(
            "tinytrader_relay_slow_disconnects_total",
            "Clients disconnected for falling behind.",
            &[],
            c(&hub.slow_disconnects),
        );
        w.counter(
            "tinytrader_relay_refused_total",
            "Join requests refused.",
            &[],
            c(&hub.refused),
        );
    });
}

async fn shutdown() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut term =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok();
        tokio::select! {
            _ = ctrl_c => {}
            () = async { match term.as_mut() { Some(t) => { t.recv().await; } None => std::future::pending().await } } => {}
        }
    }
    #[cfg(not(unix))]
    let _ = ctrl_c.await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::databento::dbn::decode::AsyncDbnDecoder;
    use ::databento::dbn::{ErrorMsg, RecordRefEnum, SystemCode};
    use futures_util::future::BoxFuture;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::TcpStream;

    /// An in-memory source: one record live every 20 ms, and history only if
    /// it has some.
    struct Fake {
        history: Option<&'static str>,
    }

    impl Source for Fake {
        fn feed(&self) -> source::Feed {
            source::Feed {
                dataset: "FAKE.DATA".to_owned(),
                schema: "mbp-1".to_owned(),
                stype_in: SType::RawSymbol,
                provider: "fake",
                name: "Fake",
                via: "memory",
            }
        }

        fn run(
            self: Arc<Self>,
            hub: Arc<Hub>,
            _: Observer,
            _: Arc<hours::Hours>,
        ) -> BoxFuture<'static, ()> {
            Box::pin(async move {
                hub.upstream_up();
                loop {
                    hub.publish(protocol::system_text("live"), 1);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
        }

        fn catch_up(self: Arc<Self>, _: u64, _: u64, _: bool) -> Option<source::History> {
            let text = self.history?;
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            tx.try_send(Ok(protocol::system_text(text))).unwrap();
            Some(rx)
        }
    }

    /// A relay wired as `run` wires it, around a fake source.
    async fn relay(fake: Fake) -> (String, Arc<Hub>) {
        let source: Arc<dyn Source> = Arc::new(fake);
        let metadata = Metadata::builder()
            .dataset(source.feed().dataset)
            .schema(None)
            .start(0)
            .stype_in(Some(source.feed().stype_in))
            .stype_out(SType::InstrumentId)
            .build();
        let hub = Arc::new(Hub::new(metadata, 64, Duration::from_secs(10)));
        let hours = Arc::new(hours::Hours::new(None, None, reqwest::Client::new()));
        let observer = Observer::new("relay", env!("CARGO_PKG_VERSION"));
        tokio::spawn(source.clone().run(hub.clone(), observer, hours));
        let clients = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = clients.local_addr().unwrap().to_string();
        tokio::spawn(accept(clients, hub.clone(), source, 8));
        (addr, hub)
    }

    /// What a client sees up to and including its first live record: system
    /// texts, with `ReplayCompleted` as `replay-completed`.
    async fn joined(addr: &str, line: &str) -> Vec<String> {
        let mut s = TcpStream::connect(addr).await.unwrap();
        s.write_all(line.as_bytes()).await.unwrap();
        let mut dec = AsyncDbnDecoder::new(s).await.unwrap();
        let mut seen = Vec::new();
        while seen.last().map(String::as_str) != Some("live") {
            let r = dec.decode_record_ref().await.unwrap().unwrap();
            if let RecordRefEnum::System(m) = r.as_enum().unwrap() {
                seen.push(match m.code() {
                    Ok(SystemCode::ReplayCompleted) => "replay-completed".to_owned(),
                    _ => m.msg().unwrap().to_owned(),
                });
            }
        }
        seen
    }

    const PAST: &str = r#"{"protocol":"tinytrader.relay/1","start":"2026-10-05T13:30:00Z"}"#;

    #[tokio::test]
    async fn a_source_without_extras_refuses_conditions_and_serves_live() {
        let (addr, hub) = relay(Fake { history: None }).await;

        let mut s = TcpStream::connect(&addr).await.unwrap();
        s.write_all(b"{\"protocol\":\"tinytrader.relay/1\",\"conditions\":true}\n")
            .await
            .unwrap();
        let mut buf = Vec::new();
        s.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf.len(), std::mem::size_of::<ErrorMsg>());
        assert!(String::from_utf8_lossy(&buf).contains("this source has no trade conditions"));
        assert_eq!(hub.refused.load(Ordering::Relaxed), 1);

        // A start in the past is no refusal: no catch-up, straight to live.
        assert_eq!(
            joined(&addr, &format!("{PAST}\n")).await,
            ["replay-completed", "live"]
        );
    }

    #[tokio::test]
    async fn a_source_with_history_catches_up_before_live() {
        let (addr, _) = relay(Fake {
            history: Some("history"),
        })
        .await;
        assert_eq!(
            joined(&addr, &format!("{PAST}\n")).await,
            ["history", "replay-completed", "live"]
        );
        // Live from now: no history.
        assert_eq!(
            joined(&addr, "{\"protocol\":\"tinytrader.relay/1\"}\n").await,
            ["replay-completed", "live"]
        );
    }
}
