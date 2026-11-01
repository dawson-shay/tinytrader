//! One backtest session: the merged sources served to one strategy, the simulated
//! venues fed and clocked in process, everything advanced in lockstep on the
//! strategy's `ready` (decision 0022, section 2).
//!
//! A run goes:
//!
//! 1. The session binds its control and data listeners and starts one gateway per
//!    venue on loopback, each a simulator fed by the session.
//! 2. The strategy connects to the control address (`hello`, answered by `welcome`
//!    with the data and order addresses and its gateway credentials), joins the data
//!    stream (`tinytrader.relay/1`) and its gateways, then says it has with
//!    `ready{as_of: 0}`. Nothing is stepped before, so its connections land at the
//!    same place in every venue's journal on every run.
//! 3. Each step is the next time anything happens: the records of one index time,
//!    timers due then, or both. The session pushes the records to the venues that
//!    quote their source, advances every venue to that time (once each has handled
//!    the order messages the strategy said it sent), sends `step{as_of, records,
//!    timers, reports}`, then writes the records to the data stream. The strategy
//!    reads that many records, reads its gateway events up to `reports`, acts, and
//!    answers `ready{as_of, sent, timers}`. The step goes first so a step larger than
//!    a socket's buffer can't block the session while the strategy waits for it.
//! 4. After the last record, the venues are advanced to the end of the last day, so
//!    day orders expire, and the session sends `end` with the outcome and the audit
//!    hash, and writes the run record.
use crate::config::{Config, Sandbox};
use crate::protocol::{self, FromStrategy};
use dbn::encode::DbnMetadataEncoder;
use dbn::{Metadata, SType, Schema};
use order_gateway::{
    ClientSpec, Clock, Config as GatewayConfig, Feed, Gateway, Profile, Role, SimBroker, Timing,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::{IpAddr, SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tinytrader_dbn::{DbnSource, Merge};
use tinytrader_producer::StoreProducer;

/// The client name and order id prefix the strategy has at every venue.
pub const STRATEGY: &str = "strategy";
const PREFIX: &str = "s-";
const DAY: u64 = 86_400_000_000_000;
/// How long the run waits after its last advance for orders still in flight.
const SETTLE: Duration = Duration::from_millis(200);

/// Where a started session listens: the `READY` line's content.
#[derive(Clone, Debug)]
pub struct Ready {
    /// The session's id.
    pub session: String,
    /// The control address (`TT_SESSION`).
    pub control: SocketAddr,
    /// The data address.
    pub data: SocketAddr,
    /// Each venue's gateway address, by name.
    pub orders: Vec<(String, SocketAddr)>,
}

impl Ready {
    /// The line a session prints when it's ready.
    #[must_use]
    pub fn line(&self) -> String {
        let orders: Vec<String> = self
            .orders
            .iter()
            .map(|(n, a)| format!("{n}={a}"))
            .collect();
        format!(
            "READY session={} mode=backtest control={} data={} orders={}",
            self.session,
            self.control,
            self.data,
            orders.join(",")
        )
    }
}

/// How a run ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Outcome {
    /// `complete`, or `incomplete` with the reason.
    pub status: &'static str,
    /// Why it's incomplete.
    pub reason: Option<String>,
    /// The audit hash.
    pub audit: String,
    /// Steps taken.
    pub steps: u64,
    /// Records delivered.
    pub records: u64,
}

struct Venue {
    name: String,
    gateway: Gateway,
    clock: Clock,
    feed: Feed,
    /// The ranks of the sources it quotes.
    ranks: BTreeSet<u32>,
    token: String,
    script: String,
}

/// A session started and waiting for its strategy.
pub struct Session {
    calendar_sha: Option<String>,
    ready: Ready,
    cfg: Config,
    cfg_text: String,
    dir: PathBuf,
    control: TcpListener,
    data: TcpListener,
    venues: Vec<Venue>,
    merge: Merge<StoreProducer>,
}

/// A venue's seed: the run's, mixed with the venue's name (FNV-1a), so venues draw
/// independent streams and the same run draws the same ones.
fn venue_seed(seed: u64, venue: &str) -> u64 {
    venue
        .bytes()
        .fold(0xcbf2_9ce4_8422_2325_u64 ^ seed, |h, b| {
            (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
        })
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// 32 bytes of the OS's randomness, as hex: tokens nothing about the run predicts.
fn random_hex() -> Result<String, String> {
    let mut b = [0_u8; 32];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut f| std::io::Read::read_exact(&mut f, &mut b))
        .map_err(|e| format!("/dev/urandom: {e}"))?;
    Ok(hex(&b))
}

fn wall_ns() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos())
}

/// Midnight UTC of `YYYY-MM-DD`, ns.
pub(crate) fn day_ns(day: &str) -> Option<u64> {
    let mut it = day.splitn(3, '-').map(i64::from_str);
    let (y, m, d) = (it.next()?.ok()?, it.next()?.ok()?, it.next()?.ok()?);
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    u64::try_from(era * 146_097 + doe - 719_468)
        .ok()?
        .checked_mul(DAY)
}

/// Accepts one connection before `deadline`.
fn accept(l: &TcpListener, deadline: Instant, what: &str) -> Result<TcpStream, String> {
    l.set_nonblocking(true).map_err(|e| e.to_string())?;
    loop {
        match l.accept() {
            Ok((s, _)) => {
                s.set_nonblocking(false).map_err(|e| e.to_string())?;
                return Ok(s);
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                if Instant::now() >= deadline {
                    return Err(format!("no {what} connection in time"));
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(e) => return Err(e.to_string()),
        }
    }
}

impl Session {
    /// Plans the sources, starts the venues and binds the listeners on `ip`.
    /// `dir` is the run's own folder (the venues' journals and the run record); it
    /// must not be under `/tmp`, as for any gateway state.
    ///
    /// # Errors
    /// A source whose holdings can't be planned, a venue that can't start, or a
    /// listener that can't bind.
    pub fn start(cfg_text: &str, dir: &Path, ip: IpAddr) -> Result<Self, String> {
        let cfg = Config::parse(cfg_text)?;
        std::fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
        let id =
            hex(&Sha256::digest(format!("{cfg_text}{}{}", wall_ns(), std::process::id()))[..6]);
        let mut sources = Vec::new();
        for s in &cfg.sources {
            let mut p = StoreProducer::new(s.holdings()?, s.query(), &cfg.days)
                .map_err(|e| format!("source {}: {e}", s.name))?;
            if let Some(n) = s.expiries_within_days {
                p = p.expiries_within_days(n);
            }
            sources.push(DbnSource::new(p, s.latency_offset_ns));
        }
        let pin = json!({"source": "session", "session": id}).to_string();
        // The venues' market calendar: its days, and its SHA-256 for the record.
        let calendar = match &cfg.calendar {
            None => None,
            Some(path) => {
                let bytes =
                    std::fs::read(path).map_err(|e| format!("calendar {}: {e}", path.display()))?;
                let v: Value = serde_json::from_slice(&bytes)
                    .map_err(|e| format!("calendar {}: {e}", path.display()))?;
                let days = v.get("days").cloned().unwrap_or(v);
                if !days.is_array() {
                    return Err(format!(
                        "calendar {}: a list of days, or {{\"days\": [...]}}",
                        path.display()
                    ));
                }
                Some((days, hex(&Sha256::digest(&bytes))))
            }
        };
        let mut venues = Vec::new();
        for v in &cfg.venues {
            let script = v.script(
                cfg.strategy_latency_ms,
                calendar.as_ref().map(|c| &c.0),
                venue_seed(cfg.seed, &v.name),
            )?;
            let (sim, feed) =
                SimBroker::fed(&script, &pin).map_err(|e| format!("venue {}: {e}", v.name))?;
            let token = random_hex()?;
            let d = Profile::default();
            let profile = Profile {
                underlyings: v.underlyings.clone(),
                equities: v.equities.clone(),
                allow_short: v.allow_short,
                futures: v.futures.clone(),
                max_future_qty: v.max_future_qty.unwrap_or(d.max_future_qty),
                max_open: v.max_open.unwrap_or(d.max_open),
                max_notional_cents: v.max_notional.map_or(d.max_notional_cents, |x| x * 100),
                ..d
            };
            let gw = order_gateway::start(
                GatewayConfig {
                    account: v.name.clone(),
                    environment: "paper".into(),
                    listen: SocketAddr::new(ip, 0),
                    listen_any: !ip.is_loopback(),
                    state: dir.join("venues").join(&v.name),
                    forbid_state: Vec::new(),
                    clients: vec![ClientSpec {
                        name: STRATEGY.into(),
                        role: Role::Trade,
                        prefix: PREFIX.into(),
                        token: token.clone(),
                    }],
                    profile,
                    timing: Timing::default(),
                    stall_after_admitted: false,
                },
                sim,
            )
            .map_err(|e| format!("venue {}: {e}", v.name))?;
            let ranks = v
                .quotes
                .iter()
                .filter_map(|q| cfg.sources.iter().position(|s| &s.name == q))
                .filter_map(|r| u32::try_from(r).ok())
                .collect();
            venues.push(Venue {
                name: v.name.clone(),
                clock: gw.clock(),
                gateway: gw,
                feed,
                ranks,
                token,
                script,
            });
        }
        let control = TcpListener::bind(SocketAddr::new(ip, 0)).map_err(|e| e.to_string())?;
        let data = TcpListener::bind(SocketAddr::new(ip, 0)).map_err(|e| e.to_string())?;
        let ready = Ready {
            session: id,
            control: control.local_addr().map_err(|e| e.to_string())?,
            data: data.local_addr().map_err(|e| e.to_string())?,
            orders: venues
                .iter()
                .map(|v| (v.name.clone(), v.gateway.addr()))
                .collect(),
        };
        Ok(Self {
            calendar_sha: calendar.map(|c| c.1),
            ready,
            cfg,
            cfg_text: cfg_text.to_owned(),
            dir: dir.to_owned(),
            control,
            data,
            venues,
            merge: Merge::new(sources),
        })
    }

    /// Where it listens.
    #[must_use]
    pub fn ready(&self) -> &Ready {
        &self.ready
    }

    /// Starts the strategy when the configuration names one, sandboxed if it says
    /// so, from an empty environment plus `PATH`, `HOME` and `TMPDIR` (its scratch
    /// folder), `TT_SESSION`, `TT_SESSION_TOKEN` and the configured variables, in
    /// its own process group, with its output in the run folder.
    fn start_strategy(&self, st: &mut Run) -> Result<(), String> {
        use std::os::unix::process::CommandExt as _;
        let Some(sp) = &self.cfg.strategy else {
            return Ok(());
        };
        let scratch = self.dir.join("strategy-tmp");
        std::fs::create_dir_all(&scratch).map_err(|e| format!("{}: {e}", scratch.display()))?;
        let scratch = scratch.canonicalize().map_err(|e| e.to_string())?;
        let out = |name: &str| {
            std::fs::File::create(self.dir.join(name)).map_err(|e| format!("{name}: {e}"))
        };
        let token = random_hex()?;
        let mut argv = sp.command.clone();
        let sandbox = match sp.sandbox {
            Sandbox::Off => json!("off"),
            Sandbox::Macos => {
                let home = std::env::var_os("HOME").map(PathBuf::from);
                let allow = crate::sandbox::check_allow_read(
                    &sp.allow_read,
                    &sp.deny_read,
                    home.as_deref(),
                )?;
                if let Some(cwd) = &sp.cwd {
                    let c = cwd
                        .canonicalize()
                        .map_err(|e| format!("strategy.cwd: {e}"))?;
                    if !allow.iter().any(|a| c.starts_with(a)) {
                        return Err(format!("strategy.cwd {} isn't in allow_read", c.display()));
                    }
                }
                let mut ports = vec![self.ready.control, self.ready.data];
                ports.extend(self.ready.orders.iter().map(|(_, a)| *a));
                let denied = crate::sandbox::always_denied(home.as_deref(), &sp.deny_read);
                let profile = crate::sandbox::profile(&allow, &denied, &scratch, &ports);
                let path = self.dir.join("sandbox.sb");
                std::fs::write(&path, &profile).map_err(|e| format!("sandbox.sb: {e}"))?;
                let mut wrapped = vec![
                    "/usr/bin/sandbox-exec".to_owned(),
                    "-f".to_owned(),
                    path.to_string_lossy().into_owned(),
                ];
                wrapped.append(&mut argv);
                argv = wrapped;
                json!({"kind": "macos-sandbox-exec", "profile_sha256": hex(&Sha256::digest(profile.as_bytes())),
                       "profile": profile, "allow_read": allow, "deny_read": sp.deny_read})
            }
        };
        let mut cmd = std::process::Command::new(&argv[0]);
        cmd.args(&argv[1..])
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("HOME", &scratch)
            .env("TMPDIR", &scratch)
            .envs(&sp.env)
            .env("TT_SESSION", self.ready.control.to_string())
            .env("TT_SESSION_TOKEN", &token)
            .stdin(std::process::Stdio::null())
            .stdout(out("strategy.out")?)
            .stderr(out("strategy.err")?)
            .process_group(0);
        if let Some(cwd) = &sp.cwd {
            cmd.current_dir(cwd);
        }
        st.process = Some(json!({
            "command": sp.command,
            "sandbox": sandbox,
            "attests": "the control connection presented the token only this process was given (assuming no other process of the same user read it from the child's environment and connected first); a process it starts inherits the sandbox",
        }));
        st.token = Some(token);
        st.child = Some(
            cmd.spawn()
                .map_err(|e| format!("strategy {}: {e}", argv[0]))?,
        );
        Ok(())
    }

    /// Waits a moment for a started strategy to exit after the run, then stops it.
    fn wait_strategy(&self, st: &mut Run) {
        let Some(child) = &mut st.child else {
            return;
        };
        let deadline = Instant::now() + Duration::from_secs(10);
        let status = loop {
            match child.try_wait() {
                Ok(Some(s)) => break s.code().map_or_else(|| json!("signal"), |c| json!(c)),
                Ok(None) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(20))
                }
                _ => {
                    let _ = child.kill();
                    let _ = child.wait();
                    break json!("killed");
                }
            }
        };
        // Whatever it started too: its process group.
        let _ = std::process::Command::new("/bin/kill")
            .args(["-9", &format!("-{}", child.id())])
            .stderr(std::process::Stdio::null())
            .status();
        if let Some(Value::Object(p)) = &mut st.process {
            p.insert("exit".into(), status);
        }
    }

    /// Serves the run to one strategy, then writes the run record and stops the
    /// venues.
    ///
    /// # Errors
    /// Only when the run record can't be written; a run that fails part way is an
    /// `incomplete` outcome.
    pub fn run(mut self) -> Result<Outcome, String> {
        let mut audit = Audit::create(&self.dir.join("audit.log"))?;
        let mut state = Run::default();
        let started = self.start_strategy(&mut state);
        let result = started.and_then(|()| self.serve(&mut audit, &mut state));
        let flushed = audit.log.flush().map_err(|e| format!("audit.log: {e}"));
        let reason = result
            .err()
            .or_else(|| audit.failed.take())
            .or_else(|| flushed.err());
        let outcome = Outcome {
            status: if reason.is_none() {
                "complete"
            } else {
                "incomplete"
            },
            reason,
            audit: hex(&audit.hash.finalize()),
            steps: state.steps,
            records: state.records,
        };
        if let Some(c) = &mut state.control {
            let _ = protocol::send(
                c,
                &json!({"type": "end", "status": outcome.status, "reason": outcome.reason,
                        "audit": outcome.audit, "steps": outcome.steps, "records": outcome.records,
                        "reports": state.reports}),
            );
        }
        self.wait_strategy(&mut state);
        // The venues stop whatever happens to the record.
        let recorded = self.record(&outcome, &state);
        let mut stopped = Ok(());
        for v in self.venues {
            if let Err(e) = v.gateway.stop() {
                stopped = stopped.and(Err(format!("venue {}: {e}", v.name)));
            }
        }
        recorded?;
        stopped?;
        Ok(outcome)
    }

    fn welcome(&self) -> Value {
        let orders: serde_json::Map<String, Value> = self
            .venues
            .iter()
            .map(|v| {
                (
                    v.name.clone(),
                    json!({"addr": v.gateway.addr().to_string(), "client": STRATEGY,
                           "token": v.token, "prefix": PREFIX}),
                )
            })
            .collect();
        json!({"type": "welcome", "protocol": protocol::PROTOCOL, "session": self.ready.session,
               "mode": "backtest", "data": self.ready.data.to_string(), "orders": orders,
               "days": self.cfg.days, "strategy_latency_ms": self.cfg.strategy_latency_ms})
    }

    /// The data stream's header: the one source's schema, or none (mixed) for several.
    fn metadata(&self) -> Metadata {
        let first = &self.cfg.sources[0];
        let one = self.cfg.sources.len() == 1;
        let schema = if one {
            Schema::from_str(&first.schema).ok()
        } else {
            None
        };
        let start = day_ns(&self.cfg.days[0]).unwrap_or(0);
        let end = self
            .cfg
            .days
            .last()
            .and_then(|d| day_ns(d))
            .map(|t| t + DAY);
        let mut m = Metadata::builder()
            .dataset(if one {
                first.dataset.clone()
            } else {
                "MIXED".into()
            })
            .schema(schema)
            .start(start)
            .stype_in(None)
            .stype_out(SType::InstrumentId)
            .symbols(self.cfg.sources.iter().map(|s| s.symbol.clone()).collect())
            .build();
        m.end = end.and_then(std::num::NonZeroU64::new);
        m
    }

    fn serve(&mut self, audit: &mut Audit, st: &mut Run) -> Result<(), String> {
        let wait = Duration::from_secs(self.cfg.ready_timeout_s);
        let deadline = Instant::now() + wait;
        // The last instant of the last day: nothing is stepped after it.
        let last = self
            .cfg
            .days
            .last()
            .and_then(|d| day_ns(d))
            .map(|d| d + DAY - 1)
            .ok_or("no last day")?;
        // The strategy: control first, then data.
        let ctl = accept(&self.control, deadline, "control")?;
        ctl.set_read_timeout(Some(wait))
            .map_err(|e| e.to_string())?;
        ctl.set_write_timeout(Some(wait))
            .map_err(|e| e.to_string())?;
        let mut reader = BufReader::new(ctl.try_clone().map_err(|e| e.to_string())?);
        st.control = Some(ctl);
        let hello = protocol::read(&mut reader)?;
        let FromStrategy::Hello {
            strategy,
            revision,
            token,
        } = hello
        else {
            return Err("the strategy's first message must be hello".into());
        };
        if st.token.is_some() && token != st.token {
            return Err("the control connection isn't from the strategy the session started (no TT_SESSION_TOKEN)".into());
        }
        st.strategy = Some((strategy.clone(), revision.clone()));
        audit.update(format!("hello {strategy} {revision}\n"));
        let w = self.welcome();
        protocol::send(st.control.as_mut().ok_or("no control")?, &w)?;
        let data = accept(&self.data, deadline, "data")?;
        data.set_read_timeout(Some(wait))
            .map_err(|e| e.to_string())?;
        // A strategy that stops reading its data ends the run, never blocks it.
        data.set_write_timeout(Some(wait))
            .map_err(|e| e.to_string())?;
        let mut join = String::new();
        BufReader::new(data.try_clone().map_err(|e| e.to_string())?)
            .read_line(&mut join)
            .map_err(|e| format!("data join: {e}"))?;
        let j: Value = serde_json::from_str(&join).map_err(|e| format!("data join: {e}"))?;
        if j["protocol"] != "tinytrader.relay/1" {
            return Err("the data join must name tinytrader.relay/1".into());
        }
        let mut out = BufWriter::new(data);
        DbnMetadataEncoder::new(&mut out)
            .encode(&self.metadata())
            .map_err(|e| e.to_string())?;
        out.flush().map_err(|e| e.to_string())?;
        let first_timers = match protocol::read(&mut reader)? {
            FromStrategy::Ready {
                as_of: 0,
                sent,
                timers,
            } if sent.iter().all(|(_, n)| *n == 0) => timers,
            _ => {
                return Err(
                    "the strategy's first ready must be ready{as_of: 0}, sending nothing".into(),
                );
            }
        };

        let mut sent: BTreeMap<String, u64> =
            self.venues.iter().map(|v| (v.name.clone(), 0)).collect();
        let mut timers: BTreeMap<(u64, u64), String> = BTreeMap::new();
        let mut timer_seq = 0_u64;
        for (at, id) in first_timers {
            if at == 0 || at > last {
                return Err(format!("timer {id} at {at} is outside the run"));
            }
            timer_seq += 1;
            timers.insert((at, timer_seq), id);
        }
        audit.update(format!("start {timers:?}\n"));
        let mut as_of = 0_u64;
        loop {
            let next_rec = self
                .merge
                .peek_key()
                .map_err(|e| e.to_string())?
                .map(|k| k.ts);
            let next_timer = timers.keys().next().map(|k| k.0);
            let t = match (next_rec, next_timer) {
                (None, None) => break,
                (Some(a), Some(b)) => a.min(b),
                (Some(a), None) | (None, Some(a)) => a,
            };
            // The records of this instant, to the venues, and kept for the data stream.
            let mut n = 0_u64;
            let mut bytes = Vec::new();
            let mut spans: BTreeMap<u32, (u64, u64)> = BTreeMap::new();
            while self
                .merge
                .peek_key()
                .map_err(|e| e.to_string())?
                .is_some_and(|k| k.ts == t)
            {
                let Some(d) = self.merge.next_delivery().map_err(|e| e.to_string())? else {
                    break;
                };
                for v in &self.venues {
                    if v.ranks.contains(&d.key.rank) {
                        v.feed
                            .push(t, d.record())
                            .map_err(|e| format!("venue {}: {e}", v.name))?;
                    }
                }
                bytes.extend_from_slice(d.record().as_ref());
                let span = spans.entry(d.key.rank).or_insert((d.key.pos, d.key.pos));
                span.1 = d.key.pos;
                n += 1;
            }
            // Venue reports at t come after its market data (class 2).
            let (reports, events) = self.advance(&sent, t, wait)?;
            // Then timers (class 3).
            let due: Vec<String> = timers
                .range((t, 0)..=(t, u64::MAX))
                .map(|(_, id)| id.clone())
                .collect();
            timers.retain(|k, _| k.0 != t);
            st.steps += 1;
            st.records += n;
            as_of = t;
            let step = json!({"type": "step", "step": st.steps, "as_of": t, "records": n,
                              "timers": due, "reports": reports});
            audit.update(format!(
                "step {} {t} {spans:?} {due:?} {events}\n",
                st.steps
            ));
            protocol::send(st.control.as_mut().ok_or("no control")?, &step)?;
            out.write_all(&bytes)
                .and_then(|()| out.flush())
                .map_err(|e| format!("data: {e}"))?;
            match protocol::read(&mut reader)? {
                FromStrategy::Ready {
                    as_of: a,
                    sent: s,
                    timers: new,
                } => {
                    if a != t {
                        return Err(format!("ready for {a}, but the step was {t}"));
                    }
                    for (venue, count) in s {
                        let Some(prev) = sent.get_mut(&venue) else {
                            return Err(format!("ready names venue {venue}, which isn't one"));
                        };
                        if count < *prev {
                            return Err(format!("ready's sent for {venue} went back"));
                        }
                        *prev = count;
                    }
                    for (at, id) in new {
                        if at <= t {
                            return Err(format!("timer {id} at {at} isn't after the step at {t}"));
                        }
                        if at > last {
                            return Err(format!(
                                "timer {id} at {at} is after the last day ends at {last}"
                            ));
                        }
                        timer_seq += 1;
                        timers.insert((at, timer_seq), id);
                    }
                    audit.update(format!("ready {t} {sent:?} {timers:?}\n"));
                }
                FromStrategy::Hello { .. } => return Err("hello only once".into()),
            }
        }
        // The rest of the last day, so day orders expire at the close.
        let end = last.max(as_of);
        let (reports, events) = self.advance(&sent, end, wait)?;
        // An order the strategy sent but didn't count may still be on its way: give it
        // a moment, then ask each venue again, so it fails the run rather than
        // slipping past the last check.
        std::thread::sleep(SETTLE);
        self.advance(&sent, end, wait)?;
        st.reports = Some(reports);
        audit.update(format!("end {end} {events}\n"));
        st.sent = sent;
        for v in &self.venues {
            for line in self.order_events(&v.name)? {
                audit.update(format!("{} {line}\n", v.name));
            }
        }
        Ok(())
    }

    /// A venue's journaled order events (submits, cancels, broker order events), each
    /// without its hash-chain link, which also covers connection and status events
    /// whose place in the journal depends on wall time.
    fn order_events(&self, venue: &str) -> Result<Vec<String>, String> {
        let dir = self.dir.join("venues").join(venue).join("journal");
        let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
            .map_err(|e| format!("{}: {e}", dir.display()))?
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|x| x == "jsonl"))
            .collect();
        files.sort();
        let mut out = Vec::new();
        for f in files {
            let text = std::fs::read_to_string(&f).map_err(|e| format!("{}: {e}", f.display()))?;
            for line in text.lines() {
                let Ok(mut v) = serde_json::from_str::<Value>(line) else {
                    return Err(format!("{}: a journal line isn't JSON", f.display()));
                };
                if !matches!(
                    v["kind"].as_str(),
                    Some("submit" | "cancel" | "order" | "activity")
                ) {
                    continue;
                }
                if let Some(o) = v.as_object_mut() {
                    o.remove("prev");
                    o.remove("seq");
                }
                out.push(v.to_string());
            }
        }
        Ok(out)
    }

    /// Advances every venue to `t` once it has handled `sent` order messages; the
    /// last event sequence number of each.
    fn advance(
        &self,
        sent: &BTreeMap<String, u64>,
        t: u64,
        wait: Duration,
    ) -> Result<(Value, String), String> {
        let mut reports = serde_json::Map::new();
        let mut events = Vec::new();
        for v in &self.venues {
            let n = sent.get(&v.name).copied().unwrap_or(0);
            let a = v
                .clock
                .advance(STRATEGY, n, t, wait)
                .map_err(|e| format!("venue {}: {e}", v.name))?;
            // More handled than said means an order the strategy didn't count: it may
            // have been stamped on either side of this advance.
            if a.handled != n {
                return Err(format!(
                    "venue {}: {} order messages handled, but the strategy said {n}",
                    v.name, a.handled
                ));
            }
            reports.insert(v.name.clone(), json!(a.last_seq));
            events.push(format!("{}={}", v.name, a.order_events));
        }
        Ok((Value::Object(reports), events.join(",")))
    }

    /// The run record: what was read, from where, with what pins, and how it ended.
    fn record(&self, o: &Outcome, st: &Run) -> Result<(), String> {
        let sources: Vec<Value> = self
            .cfg
            .sources
            .iter()
            .zip(self.merge.sources())
            .map(|(s, src)| {
                let p = src.decoder();
                let days: Vec<Value> = p
                    .pins()
                    .iter()
                    .map(|d| {
                        json!({"day": d.day, "partial": d.partial, "missing": d.missing,
                               "files": d.files.iter().map(|f| json!({"name": f.name, "sha256": f.sha256,
                                   "bytes": f.bytes, "verified": f.verified, "used": f.used})).collect::<Vec<_>>()})
                    })
                    .collect();
                json!({"name": s.name, "holdings": p.describe(), "reading": p.reading(), "dataset": s.dataset, "symbol": s.symbol,
                       "schema": s.schema, "latency_offset_ns": s.latency_offset_ns,
                       "expiries_within_days": s.expiries_within_days,
                       "records": src.position(), "clamped": src.clamped(), "days": days})
            })
            .collect();
        let venues: Vec<Value> = self
            .venues
            .iter()
            .map(|v| {
                json!({"name": v.name, "model": order_gateway::SIM_MODEL, "script": serde_json::from_str::<Value>(&v.script).unwrap_or(Value::Null),
                       "quotes": v.ranks.iter().map(|r| self.cfg.sources[*r as usize].name.clone()).collect::<Vec<_>>(),
                       "sent": st.sent.get(&v.name)})
            })
            .collect();
        let rec = json!({
            "format": "tinytrader.session-run/1",
            "session": self.ready.session,
            "mode": "backtest",
            "status": o.status,
            "reason": o.reason,
            "audit": o.audit,
            "steps": o.steps,
            "records": o.records,
            "strategy": st.strategy.as_ref().map(|(s, r)| json!({"name": s, "revision": r})),
            "days": self.cfg.days,
            "seed": self.cfg.seed,
            "strategy_process": st.process.clone().unwrap_or(Value::Null),
            "calendar_sha256": self.calendar_sha,
            "lockstep": {"batch": "instant", "strategy_latency_ms": self.cfg.strategy_latency_ms},
            "merge": {"order": "(ts, class, rank, position)", "ranks": self.cfg.sources.iter().map(|s| s.name.clone()).collect::<Vec<_>>()},
            "config_sha256": hex(&Sha256::digest(self.cfg_text.as_bytes())),
            "sources": sources,
            "venues": venues,
        });
        let text = serde_json::to_string_pretty(&rec).map_err(|e| e.to_string())?;
        std::fs::write(self.dir.join("config.toml"), &self.cfg_text).map_err(|e| e.to_string())?;
        std::fs::write(self.dir.join("run.json"), text + "\n").map_err(|e| e.to_string())
    }
}

/// The audit log: every line hashed and written as it happens (0009,
/// "Provenance"), so a long run never holds it in memory and a crash keeps it.
struct Audit {
    hash: Sha256,
    log: BufWriter<std::fs::File>,
    failed: Option<String>,
}

impl Audit {
    fn create(path: &Path) -> Result<Self, String> {
        let f = std::fs::File::create(path).map_err(|e| format!("{}: {e}", path.display()))?;
        Ok(Self {
            hash: Sha256::new(),
            log: BufWriter::new(f),
            failed: None,
        })
    }

    fn update(&mut self, line: impl AsRef<[u8]>) {
        self.hash.update(line.as_ref());
        if let Err(e) = self.log.write_all(line.as_ref())
            && self.failed.is_none()
        {
            self.failed = Some(format!("audit.log: {e}"));
        }
    }
}

#[derive(Default)]
struct Run {
    control: Option<TcpStream>,
    strategy: Option<(String, String)>,
    steps: u64,
    records: u64,
    sent: BTreeMap<String, u64>,
    /// Each venue's last event after the final advance.
    reports: Option<Value>,
    /// The strategy process the session started, and what the record says of it.
    child: Option<std::process::Child>,
    process: Option<Value>,
    /// The token a started strategy must present in its hello.
    token: Option<String>,
}
