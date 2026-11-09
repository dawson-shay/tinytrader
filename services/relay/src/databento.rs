//! Databento source: one live session feeds the hub. It replays the day's
//! reference data (instrument definitions and statistics, by default) when it
//! connects, so the hub holds them for joining clients even after a restart,
//! and then continues live.
//!
//! Market data is not replayed: Databento refuses intraday replay for
//! `cmbp-1`, and missed quotes come from the next day's historical data.

use std::sync::Arc;
use std::time::{Duration, Instant};

use databento::LiveClient;
use databento::dbn::{
    Cmbp1Msg, ErrorCode, ErrorMsg, Mbp1Msg, RecordRef, SType, Schema, SystemCode, SystemMsg,
    VersionUpgradePolicy,
};
use databento::live::Subscription;
use futures_util::future::BoxFuture;
use time::OffsetDateTime;
use tinytrader_observe::{Observer, State};

use crate::hours::{self, Hours};
use crate::hub::Hub;
use crate::source::{Feed, Source};

/// Records are passed to clients in chunks of about this many bytes.
const CHUNK: usize = 64 * 1024;

/// Reading this far behind the provider is reported in the health detail.
const BEHIND: Duration = Duration::from_secs(1);

pub struct Databento {
    pub key: String,
    pub dataset: String,
    pub symbols: Vec<String>,
    pub schema: Schema,
    /// Reference schemas sent live alongside `schema` and replayed for
    /// clients catching up, such as `definition` and `statistics`.
    pub reference: Vec<Schema>,
    pub stype_in: SType,
    /// In market hours, the relay is `degraded` while more than `stale_share`
    /// of its quotes are older than `stale_after`.
    pub stale_after: Duration,
    pub stale_share: f64,
}

/// The quotes too old to trust, in market hours (09:35 to 16:00 New York on a
/// weekday; the relay runs only on trading days): `Some(why)` when more than
/// `share` of the defined instruments' quotes are older than `after`.
fn stale_detail(hub: &Hub, now_ns: u64, after: Duration, share: f64) -> Option<String> {
    let (_, hhmm, weekday) =
        tinytrader_schedule::ny_date_time(i64::try_from(now_ns / 1_000_000_000).unwrap_or(0));
    if !(1..=5).contains(&weekday) || hhmm.as_str() < "09:35" || hhmm.as_str() >= "16:00" {
        return None;
    }
    let (stale, total) =
        hub.stale_quotes(now_ns, u64::try_from(after.as_nanos()).unwrap_or(u64::MAX));
    (total > 0 && stale as f64 > share * total as f64).then(|| {
        format!(
            "{stale} of {total} quotes older than {} min",
            after.as_secs() / 60
        )
    })
}

impl Databento {
    /// A started session for the configured symbols: the schema live, and the
    /// reference schemas replayed from as early as Databento has them (up to
    /// 24 hours) and then live.
    async fn session(&self, heartbeat: Duration) -> Result<LiveClient, String> {
        let mut client = LiveClient::builder()
            .key(&self.key)
            .map_err(|e| format!("invalid API key: {e}"))?
            .dataset(&self.dataset)
            .upgrade_policy(VersionUpgradePolicy::UpgradeToV3)
            .heartbeat_interval(time::Duration::seconds(
                i64::try_from(heartbeat.as_secs()).unwrap_or(10),
            ))
            .build()
            .await
            .map_err(|e| format!("cannot connect to Databento: {e}"))?;
        let replay = Some(OffsetDateTime::UNIX_EPOCH);
        let subs =
            std::iter::once((self.schema, None)).chain(self.reference.iter().map(|s| (*s, replay)));
        for (schema, from) in subs {
            let sub = Subscription::builder()
                .symbols(self.symbols.clone())
                .schema(schema)
                .stype_in(self.stype_in);
            let sub = match from {
                Some(t) => sub.start(t).build(),
                None => sub.build(),
            };
            client
                .subscribe(sub)
                .await
                .map_err(|e| format!("cannot subscribe to {schema}: {e}"))?;
        }
        client
            .start()
            .await
            .map_err(|e| format!("cannot start the session: {e}"))?;
        Ok(client)
    }

    /// Relay the session until it ends: Databento closes it, it fails, or
    /// (when `may_drop` allows) it stays behind long enough to be dropped
    /// for a fresh one.
    async fn stream(&self, hub: &Hub, observer: &Observer, may_drop: bool) -> End {
        let mut client = match self.session(hub.heartbeat()).await {
            Ok(c) => c,
            Err(e) => return End::Failed(e),
        };
        hub.session_started(crate::protocol::now_ns());
        hub.upstream_up();
        observer.set_state(State::Ready, "live");
        // The session id lets Databento's support look the session up.
        let session = client.session_id().to_owned();
        eprintln!(
            "relay: Databento session live for {} (session {session})",
            self.symbols.join(",")
        );
        let mut gaps = Gaps::default();
        let mut stall = Stall::default();
        let mut minute = Minute::new(Instant::now());
        // Whether the health detail says the relay is behind, and when it last said so.
        let (mut said_behind, mut said_at) = (false, Instant::now());
        // Quotes too old to trust (checked each minute), and the detail last said.
        let mut stale: Option<String> = None;
        let mut detail = "live".to_owned();
        let say = |observer: &Observer, detail: &str, stale: &Option<String>| match stale {
            Some(s) => observer.set_state(State::Degraded, &format!("{detail}; {s}")),
            None => observer.set_state(State::Ready, detail),
        };
        loop {
            let mut chunk = Vec::with_capacity(CHUNK);
            let mut n = 0;
            match client.next_record().await {
                Ok(Some(r)) => gaps.take(hub, r, &mut chunk, &mut n),
                Ok(None) => return End::Closed,
                Err(e) => return End::Failed(format!("Databento session failed: {e}")),
            }
            // Take whatever else is already buffered, up to a chunk.
            while chunk.len() < CHUNK {
                match client.try_next_record() {
                    Ok(Some(r)) => gaps.take(hub, r, &mut chunk, &mut n),
                    Ok(None) => break,
                    Err(e) => return End::Failed(format!("Databento session failed: {e}")),
                }
            }
            let now = Instant::now();
            minute.records += n;
            minute.bytes += chunk.len() as u64;
            if !chunk.is_empty() {
                hub.publish(chunk, n);
            }
            let skipped = std::mem::take(&mut gaps.skipped);
            minute.skips += u64::from(skipped);
            let behind = gaps
                .last_recv
                .take()
                .map(|recv| Duration::from_nanos(hub.received(recv)));
            if let Some(behind) = behind {
                minute.worst = minute.worst.max(behind);
                minute.latest = behind;
                let late = behind >= BEHIND;
                if late != said_behind || (late && said_at.elapsed() >= Duration::from_secs(1)) {
                    detail = if late {
                        format!("live, {:.1} s behind Databento", behind.as_secs_f64())
                    } else {
                        "live".to_owned()
                    };
                    say(observer, &detail, &stale);
                    (said_behind, said_at) = (late, now);
                }
            }
            if let Some(line) = minute.roll(now) {
                eprintln!("relay: {line}");
                let was = stale.is_some();
                stale = stale_detail(
                    hub,
                    crate::protocol::now_ns(),
                    self.stale_after,
                    self.stale_share,
                );
                if stale.is_some() != was {
                    let text = match &stale {
                        Some(s) => format!("quotes stale: {s}"),
                        None => "quotes fresh again".to_owned(),
                    };
                    eprintln!("relay: {text}");
                    hub.events.push(text.clone(), text);
                }
                say(observer, &detail, &stale);
            }
            if let Some(stalled) = stall.note(now, behind, skipped)
                && may_drop
            {
                return End::Stalled(format!(
                    "behind Databento (or skipped by it) for {:.0} s without catching up, {:.1} s behind now (session {session}; {}); dropping the session for a fresh one",
                    stalled.as_secs_f64(),
                    minute.latest.as_secs_f64(),
                    minute.so_far(now),
                ));
            }
        }
    }
}

/// How a session ended.
enum End {
    Closed,
    Failed(String),
    /// Dropped for staying behind: a fresh session is opened at once.
    Stalled(String),
}

/// Falling this far behind the provider counts toward a stall.
const STALL_BEHIND: Duration = Duration::from_secs(5);
/// Behind (or skipped by the provider) this long without catching up: the
/// session is dropped and a fresh one opened. On 2026-10-16 two sessions
/// fell 15-20 s behind at the open and never caught up in over an hour,
/// while fresh sessions ran at full rate at once.
const STALL_FOR: Duration = Duration::from_secs(30);
/// Caught up (under a second behind, nothing skipped) this long ends a stall.
const CAUGHT_UP_FOR: Duration = Duration::from_secs(10);
/// At most one stall reconnect this often, so a feed genuinely too heavy to
/// keep up with doesn't become a loop of reconnects (each replays the day's
/// reference data).
const STALL_EVERY: Duration = Duration::from_secs(300);

/// Whether the session has stayed behind without catching up.
#[derive(Default)]
struct Stall {
    since: Option<Instant>,
    caught_up_since: Option<Instant>,
}

impl Stall {
    /// After a chunk: how far behind it was (if it held a quote) and whether
    /// the provider skipped records. How long the session has been stalled,
    /// once that is `STALL_FOR` or more.
    fn note(&mut self, now: Instant, behind: Option<Duration>, skipped: bool) -> Option<Duration> {
        let late = behind.is_some_and(|b| b >= STALL_BEHIND);
        if late || skipped {
            self.caught_up_since = None;
            self.since.get_or_insert(now);
        } else if behind.is_some_and(|b| b < BEHIND) {
            let caught = *self.caught_up_since.get_or_insert(now);
            if now.duration_since(caught) >= CAUGHT_UP_FOR {
                self.since = None;
            }
        } else if behind.is_some() {
            // Between a second and STALL_BEHIND: neither stalled nor caught up.
            self.caught_up_since = None;
        }
        self.since
            .map(|s| now.duration_since(s))
            .filter(|d| *d >= STALL_FOR)
    }
}

/// The last minute of the session, for one log line a minute: what came in,
/// how far behind, and what the provider skipped.
struct Minute {
    from: Instant,
    records: u64,
    bytes: u64,
    skips: u64,
    worst: Duration,
    latest: Duration,
}

impl Minute {
    fn new(from: Instant) -> Self {
        Self {
            from,
            records: 0,
            bytes: 0,
            skips: 0,
            worst: Duration::ZERO,
            latest: Duration::ZERO,
        }
    }

    fn so_far(&self, now: Instant) -> String {
        let secs = now.duration_since(self.from).as_secs_f64().max(1e-3);
        format!(
            "{:.0} records/s, {:.2} MB/s, worst {:.1} s behind, {} skips over {secs:.0} s",
            self.records as f64 / secs,
            self.bytes as f64 / secs / 1e6,
            self.worst.as_secs_f64(),
            self.skips,
        )
    }

    /// Once a minute has passed: its line, and a new minute begun.
    fn roll(&mut self, now: Instant) -> Option<String> {
        (now.duration_since(self.from) >= Duration::from_secs(60)).then(|| {
            let line = format!(
                "last minute {}, {:.1} s behind now",
                self.so_far(now),
                self.latest.as_secs_f64()
            );
            *self = Self {
                latest: self.latest,
                ..Self::new(now)
            };
            line
        })
    }
}

impl Source for Databento {
    fn feed(&self) -> Feed {
        Feed {
            dataset: self.dataset.clone(),
            schema: self.schema.to_string(),
            stype_in: self.stype_in,
            provider: "databento",
            name: "Databento",
            via: "live API",
        }
    }

    /// Keep the standing session up, publishing its records to the hub.
    fn run(
        self: Arc<Self>,
        hub: Arc<Hub>,
        observer: Observer,
        hours: Arc<Hours>,
    ) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            let mut backoff = Duration::from_secs(1);
            let mut last_drop: Option<Instant> = None;
            loop {
                let end = hours.wait_until_active(&hub, &observer).await;
                let began = Instant::now();
                let may_drop = last_drop.is_none_or(|t| t.elapsed() >= STALL_EVERY);
                // At the end of the active window the session is dropped, which
                // disconnects it.
                let outcome = tokio::select! {
                    r = self.stream(&hub, &observer, may_drop) => Some(r),
                    () = hours::until(end) => None,
                };
                let Some(outcome) = outcome else {
                    continue;
                };
                hub.upstream_down();
                let (why, wait) = match outcome {
                    End::Closed => ("Databento closed the session".to_owned(), backoff),
                    End::Failed(e) => (e, backoff),
                    End::Stalled(e) => {
                        last_drop = Some(Instant::now());
                        (e, Duration::ZERO)
                    }
                };
                eprintln!("relay: {why}; reconnecting in {}s", wait.as_secs());
                observer.record_error(&why);
                observer.set_state(State::Degraded, &format!("{why}; reconnecting"));
                if began.elapsed() > Duration::from_secs(60) {
                    backoff = Duration::from_secs(1);
                }
                tokio::time::sleep(wait).await;
                if !wait.is_zero() {
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                }
            }
        })
    }
}

/// Databento's own slow-reader messages are about the relay, and use the
/// codes the relay sends its clients when it drops them; they are turned into
/// the relay's markers instead of being passed on. A skip becomes
/// `upstream-gap gap_from=<ns> gap_to=<ns>`, sent before the first record
/// after it: from the last record before the skip to that record.
#[derive(Default)]
struct Gaps {
    last_ts: u64,
    skipped_since: Option<u64>,
    /// When the provider received the latest quote taken (not reference
    /// records, which are replayed from earlier in the day).
    last_recv: Option<u64>,
    /// Whether the provider skipped records since this was last taken.
    skipped: bool,
}

impl Gaps {
    fn take(&mut self, hub: &Hub, r: RecordRef, chunk: &mut Vec<u8>, n: &mut u64) {
        if let Some(e) = r.get::<ErrorMsg>()
            && e.code().ok() == Some(ErrorCode::SkippedRecordsAfterSlowReading)
        {
            self.skipped_since.get_or_insert(self.last_ts);
            self.skipped = true;
            return;
        }
        if let Some(m) = r.get::<SystemMsg>()
            && m.code().ok() == Some(SystemCode::SlowReaderWarning)
        {
            hub.upstream_slow();
            return;
        }
        let ts = r.header().ts_event;
        if let Some(from) = self.skipped_since.take() {
            chunk.extend(hub.upstream_gap(from, ts));
        }
        if r.get::<ErrorMsg>().is_none() && r.get::<SystemMsg>().is_none() {
            self.last_ts = ts;
        }
        if let Some(recv) = r
            .get::<Cmbp1Msg>()
            .map(|m| m.ts_recv)
            .or_else(|| r.get::<Mbp1Msg>().map(|m| m.ts_recv))
        {
            self.last_recv = Some(recv);
        }
        chunk.extend_from_slice(r.as_ref());
        *n += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use databento::dbn::{Cmbp1Msg, Metadata};

    fn quote(ts: u64) -> Cmbp1Msg {
        let mut q = Cmbp1Msg::default_for_schema(Schema::Cmbp1);
        q.hd.instrument_id = 7;
        q.hd.ts_event = ts;
        q
    }

    #[test]
    fn a_provider_skip_becomes_a_gap_marker() {
        let hub = Hub::new(
            Metadata::builder()
                .dataset("OPRA.PILLAR".to_owned())
                .schema(None)
                .start(0)
                .stype_in(None)
                .stype_out(SType::InstrumentId)
                .build(),
            8,
            Duration::from_secs(10),
        );
        let mut gaps = Gaps::default();
        let (mut chunk, mut n) = (Vec::new(), 0);
        let before = quote(100);
        gaps.take(&hub, RecordRef::from(&before), &mut chunk, &mut n);
        let skip = ErrorMsg::new(
            150,
            Some(ErrorCode::SkippedRecordsAfterSlowReading),
            "Slow client detected for cmbp-1. Skipped records",
            true,
        );
        gaps.take(&hub, RecordRef::from(&skip), &mut chunk, &mut n);
        let after = quote(200);
        gaps.take(&hub, RecordRef::from(&after), &mut chunk, &mut n);
        assert_eq!(n, 2, "the provider's error isn't passed on");
        let text = String::from_utf8_lossy(&chunk);
        assert!(
            text.contains("upstream-gap gap_from=100 gap_to=200"),
            "{text}"
        );
        assert!(!text.contains("Slow client detected"));
        assert_eq!(
            hub.upstream_skips
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn how_far_behind_is_read_from_the_provider_receive_time() {
        let hub = Hub::new(
            Metadata::builder()
                .dataset("OPRA.PILLAR".to_owned())
                .schema(None)
                .start(0)
                .stype_in(None)
                .stype_out(SType::InstrumentId)
                .build(),
            8,
            Duration::from_secs(10),
        );
        let mut gaps = Gaps::default();
        let (mut chunk, mut n) = (Vec::new(), 0);
        let mut q = quote(100);
        let received = crate::protocol::now_ns() - 3_000_000_000;
        q.ts_recv = received;
        gaps.take(&hub, RecordRef::from(&q), &mut chunk, &mut n);
        assert_eq!(gaps.last_recv, Some(received));
        let behind = hub.received(received);
        assert!((3_000_000_000..4_000_000_000).contains(&behind), "{behind}");
        assert_eq!(hub.behind_peak(), Some(behind));
        // A smaller lag later in the minute leaves the peak.
        hub.received(crate::protocol::now_ns());
        assert_eq!(hub.behind_peak(), Some(behind));
    }

    #[test]
    fn a_session_behind_for_30_s_is_dropped_but_a_burst_is_not() {
        let t0 = Instant::now();
        let at = |s: u64| t0 + Duration::from_secs(s);
        let behind = |s: u64| Some(Duration::from_secs(s));

        // 15 s behind and never catching up (2026-10-16): dropped at 30 s.
        let mut stall = Stall::default();
        for s in 0..30 {
            assert_eq!(stall.note(at(s), behind(15), s % 3 == 0), None, "{s}");
        }
        assert_eq!(
            stall.note(at(30), behind(15), false),
            Some(Duration::from_secs(30))
        );

        // A burst at the open that catches up within 20 s: never dropped.
        let mut stall = Stall::default();
        for s in 0..20 {
            assert_eq!(stall.note(at(s), behind(8), true), None);
        }
        for s in 20..100 {
            assert_eq!(
                stall.note(at(s), Some(Duration::from_millis(200)), false),
                None,
                "{s}"
            );
        }
        // Caught up for 10 s ends the stall: a new one starts the count again.
        for s in 100..129 {
            assert_eq!(stall.note(at(s), behind(6), false), None, "{s}");
        }
        assert!(stall.note(at(130), behind(6), false).is_some());

        // Chunks without a quote (reference records) don't end a stall.
        let mut stall = Stall::default();
        stall.note(at(0), behind(10), false);
        for s in 1..40 {
            stall.note(at(s), None, false);
        }
        assert!(stall.note(at(40), None, false).is_some());
    }

    #[test]
    fn a_minute_line_says_rate_lag_and_skips() {
        let t0 = Instant::now();
        let mut m = Minute::new(t0);
        m.records = 6_000_000;
        m.bytes = 480_000_000;
        m.skips = 3;
        m.worst = Duration::from_millis(2500);
        m.latest = Duration::from_millis(200);
        assert_eq!(m.roll(t0 + Duration::from_secs(59)), None);
        let line = m.roll(t0 + Duration::from_secs(60)).unwrap();
        assert_eq!(
            line,
            "last minute 100000 records/s, 8.00 MB/s, worst 2.5 s behind, 3 skips over 60 s, 0.2 s behind now"
        );
        assert_eq!((m.records, m.skips), (0, 0));
    }
}
