//! Loopback listener, token authentication, one reader and one writer thread per
//! connection, and the bounded outbound queue (`slow_consumer` on overflow).
use crate::broker::Broker;
use crate::journal::Journal;
use crate::protocol::{Line, LineError, MAX_LINE, Obj, Op, error_line, parse_line};
use crate::session::{Input, Session};
use sha2::{Digest, Sha256};
use std::collections::VecDeque;
use std::io::{BufRead, BufReader, ErrorKind, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst};
use std::sync::mpsc::{SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// Client role (section 4).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Role {
    /// Submit and cancel within its prefix, plus queries.
    Trade,
    /// Queries and every event.
    Observe,
    /// Observe plus kill, unkill and any cancel.
    Admin,
}

impl Role {
    /// Parses `trade`, `observe` or `admin`.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "trade" => Some(Self::Trade),
            "observe" => Some(Self::Observe),
            "admin" => Some(Self::Admin),
            _ => None,
        }
    }
}

/// One operator-configured client bound to this account.
#[derive(Clone, Debug)]
pub struct ClientSpec {
    /// Client name sent in `hello`.
    pub name: String,
    /// Role.
    pub role: Role,
    /// Required `client_order_id` prefix.
    pub prefix: String,
    /// Token text (never logged).
    pub token: String,
}

/// Protocol and recovery timings. Defaults are the contract values.
#[derive(Clone, Debug)]
pub struct Timing {
    /// `hello` deadline.
    pub hello: Duration,
    /// A silent peer is closed after this.
    pub idle: Duration,
    /// Server heartbeat after this long without sending.
    pub heartbeat: Duration,
    /// Outbound queue bound per connection.
    pub queue_bytes: usize,
    /// Unknown-outcome lookups, as offsets from the outcome.
    pub resolve: Vec<Duration>,
    /// Stream down for longer than this degrades the account.
    pub stream_grace: Duration,
    /// Degraded polling and start-gate retry interval.
    pub poll: Duration,
    /// `cancel_on_disconnect` grace.
    pub disconnect_grace: Duration,
    /// How long an ended foreign order's fill record is kept once unseen.
    pub foreign_keep: Duration,
}

impl Default for Timing {
    fn default() -> Self {
        let s = Duration::from_secs;
        Self {
            hello: s(5),
            idle: s(30),
            heartbeat: s(10),
            queue_bytes: 4 << 20,
            foreign_keep: Duration::from_hours(24),
            resolve: vec![s(1), s(2), s(4)],
            stream_grace: s(5),
            poll: s(1),
            disconnect_grace: s(2),
        }
    }
}

/// One account process.
#[derive(Clone, Debug)]
pub struct Config {
    /// Gateway-local account name.
    pub account: String,
    /// `paper` (live is out of scope for v1).
    pub environment: String,
    /// Listen address: loopback, unless `listen_any` is set.
    pub listen: SocketAddr,
    /// Allows a non-loopback listen address, for a container whose port is
    /// published to loopback by the host (`--listen-any`). The state-root check
    /// still applies.
    pub listen_any: bool,
    /// Durable state root (journal, lock, `KILL`).
    pub state: PathBuf,
    /// Directories the state root must not be under, besides `/tmp`
    /// (`--forbid-state-under`): a deployment's scratch or bulk-data volumes.
    pub forbid_state: Vec<PathBuf>,
    /// Clients bound to this account.
    pub clients: Vec<ClientSpec>,
    /// Allowlist and caps.
    pub profile: crate::safety::Profile,
    /// Timings.
    pub timing: Timing,
    /// Test hook: stop forever between the `admitted` and `sent` journal lines.
    pub stall_after_admitted: bool,
}

/// Refuses a non-loopback listen address and a state root under `/tmp`.
///
/// # Errors
/// The refusal reason.
pub fn check_paths(listen: SocketAddr, state: &Path) -> Result<(), String> {
    if !listen.ip().is_loopback() {
        return Err(format!("--listen {listen} is not a loopback address"));
    }
    check_state(state)
}

/// The state root must not be under `/tmp`.
///
/// # Errors
/// Names the forbidden root.
pub fn check_state(state: &Path) -> Result<(), String> {
    check_state_under(state, &[])
}

/// The state root must not be under `/tmp`, nor under any of `forbidden`
/// (the deployment's own, from `--forbid-state-under`).
///
/// # Errors
/// Names the forbidden root.
pub fn check_state_under(state: &Path, forbidden: &[PathBuf]) -> Result<(), String> {
    let mut probe = state.to_path_buf();
    while !probe.exists() {
        if !probe.pop() {
            break;
        }
    }
    let real = probe.canonicalize().unwrap_or(probe);
    let full = real.join(state.strip_prefix(&real).unwrap_or(Path::new("")));
    let fixed = [Path::new("/tmp"), Path::new("/private/tmp")];
    for bad in fixed
        .into_iter()
        .chain(forbidden.iter().map(PathBuf::as_path))
    {
        if full.starts_with(bad) || state.starts_with(bad) {
            return Err(format!("--state must not be under {}", bad.display()));
        }
    }
    Ok(())
}

pub(crate) struct Outbox {
    /// One writer (the account session) and one reader (the connection's writer
    /// thread), neither holding it for long: the lock the contributing guide allows.
    st: Mutex<(VecDeque<String>, usize, bool, bool)>,
    cv: Condvar,
    limit: usize,
}

enum Pop {
    Line(String),
    Idle,
    Closed,
}

impl Outbox {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            st: Mutex::new((VecDeque::new(), 0, false, false)),
            cv: Condvar::new(),
            limit,
        }
    }
    /// Queues a line; on overflow the queue is replaced by `slow_consumer` and closed.
    pub(crate) fn push(&self, line: String) -> bool {
        self.push_all(vec![line])
    }
    /// Queues lines as one batch (a resume replay overflows as a whole).
    pub(crate) fn push_all(&self, lines: Vec<String>) -> bool {
        let mut g = self.st.lock().unwrap_or_else(PoisonError::into_inner);
        if g.2 {
            return false;
        }
        let size: usize = lines.iter().map(String::len).sum();
        if g.1 + size > self.limit {
            g.0.clear();
            g.0.push_back(error_line(
                "slow_consumer",
                "outbound queue overflow; resume to recover",
            ));
            g.2 = true;
            g.3 = true;
            self.cv.notify_all();
            return false;
        }
        g.1 += size;
        g.0.extend(lines);
        g.3 = true;
        self.cv.notify_all();
        true
    }

    /// Whether it's closed.
    pub(crate) fn closed(&self) -> bool {
        self.st.lock().unwrap_or_else(PoisonError::into_inner).2
    }

    /// Waits until the session has written to it (its welcome) or it's closed (its
    /// hello refused), at most `wait`; whether it was written to and is open.
    fn wait_started(&self, wait: Duration) -> bool {
        let g = self.st.lock().unwrap_or_else(PoisonError::into_inner);
        let (g, _) = self
            .cv
            .wait_timeout_while(g, wait, |g| !g.3 && !g.2)
            .unwrap_or_else(PoisonError::into_inner);
        g.3 && !g.2
    }
    /// Queues a final line (if any) after the pending ones and closes.
    pub(crate) fn close(&self, last: Option<String>) {
        let mut g = self.st.lock().unwrap_or_else(PoisonError::into_inner);
        g.3 = true;
        if !g.2 {
            g.0.extend(last);
            g.2 = true;
        }
        self.cv.notify_all();
    }
    fn pop(&self, wait: Duration) -> Pop {
        let mut g = self.st.lock().unwrap_or_else(PoisonError::into_inner);
        loop {
            if let Some(l) = g.0.pop_front() {
                g.1 = g.1.saturating_sub(l.len());
                return Pop::Line(l);
            }
            if g.2 {
                return Pop::Closed;
            }
            let (next, timeout) = self
                .cv
                .wait_timeout(g, wait)
                .unwrap_or_else(PoisonError::into_inner);
            g = next;
            if timeout.timed_out() && g.0.is_empty() && !g.2 {
                return Pop::Idle;
            }
        }
    }
}

fn writer(mut s: TcpStream, ob: &Outbox, last_seq: &AtomicU64, heartbeat: Duration) {
    let _ = s.set_write_timeout(Some(Duration::from_secs(10)));
    loop {
        let line = match ob.pop(heartbeat) {
            Pop::Line(l) => l,
            Pop::Idle => Obj::msg("heartbeat")
                .n("ts", crate::session::wall_ns())
                .n("last_seq", last_seq.load(SeqCst))
                .done(),
            Pop::Closed => break,
        };
        if s.write_all(line.as_bytes())
            .and_then(|()| s.write_all(b"\n"))
            .is_err()
        {
            ob.close(None);
            break;
        }
    }
    let _ = s.flush();
    let _ = s.shutdown(Shutdown::Both);
}

enum ReadErr {
    TooLong,
    Timeout,
    Io,
}

fn read_line(r: &mut BufReader<TcpStream>, buf: &mut Vec<u8>) -> Result<bool, ReadErr> {
    buf.clear();
    loop {
        let avail = match r.fill_buf() {
            Ok(b) => b,
            Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {
                return Err(ReadErr::Timeout);
            }
            Err(e) if e.kind() == ErrorKind::Interrupted => continue,
            Err(_) => return Err(ReadErr::Io),
        };
        if avail.is_empty() {
            return Ok(false);
        }
        let (take, done) = avail
            .iter()
            .position(|b| *b == b'\n')
            .map_or((avail.len(), false), |p| (p, true));
        buf.extend_from_slice(&avail[..take]);
        r.consume(take + usize::from(done));
        if buf.len() >= MAX_LINE {
            return Err(ReadErr::TooLong);
        }
        if done {
            return Ok(true);
        }
    }
}

fn digest(s: &str) -> [u8; 32] {
    Sha256::digest(s.as_bytes()).into()
}

struct Shared {
    clients: Vec<ClientSpec>,
    timing: Timing,
    last_seq: Arc<AtomicU64>,
    tx: SyncSender<Input>,
    /// The urgent inputs (kill, flatten, clear_order), which the session takes before
    /// anything in the inbox.
    urgent: SyncSender<Input>,
    /// A session clocks the broker: requests wait for room rather than get `busy`.
    lockstep: bool,
    /// Per client: order messages (submits, cancels) answered `busy`, which a session's
    /// clock counts as handled.
    refused: Arc<Vec<AtomicU64>>,
}

/// Inputs the account session may have waiting: past it, a request is answered
/// `busy` at once and never reaches the session.
pub(crate) const INBOX: usize = 1024;

/// Urgent inputs the session may have waiting; a sender waits past that.
pub(crate) const URGENT: usize = 64;

impl Shared {
    /// A connection's lifecycle (hello, closed): waits for room, as the session
    /// must see it.
    fn send(&self, input: Input) {
        let _ = self.tx.send(input);
    }

    /// A request: queued if there's room, else answered `busy` on the connection,
    /// with nothing sent to the broker. A safety request (cancel, kill, unkill,
    /// flatten, clear) is never refused: it waits for room.
    fn request(&self, ob: &Outbox, client: usize, input: Input) {
        let op = match &input {
            Input::Request {
                request: Ok((_, op)),
                ..
            } => Some(op),
            _ => None,
        };
        // Urgent: ahead of the inbox (a kill refuses what's queued behind it as it
        // comes up). An unkill and a cancel keep their places, in order, but never get
        // `busy`: they wait for room.
        if matches!(
            op,
            Some(Op::Kill { .. } | Op::Flatten { .. } | Op::ClearOrder { .. })
        ) {
            let _ = self.urgent.send(input);
            return;
        }
        // Under a session's clock nothing is `busy` (whether a request found the inbox
        // full would depend on timing, and a run must repeat): it waits for room.
        if self.lockstep || matches!(op, Some(Op::Cancel(_) | Op::Unkill)) {
            let _ = self.tx.send(input);
            return;
        }
        match self.tx.try_send(input) {
            Ok(()) | Err(TrySendError::Disconnected(_)) => {}
            Err(TrySendError::Full(Input::Request { request, .. })) => {
                let (rid, op) = match &request {
                    Ok((rid, op)) => (rid.clone(), crate::session::op_name(op).to_owned()),
                    Err((rid, op, _)) => (rid.clone().unwrap_or_default(), op.clone()),
                };
                if matches!(op.as_str(), "submit" | "cancel")
                    && let Some(n) = self.refused.get(client)
                {
                    n.fetch_add(1, SeqCst);
                }
                ob.push(crate::protocol::request_error(
                    &rid,
                    &op,
                    "busy",
                    &Obj::new().n("inbox", INBOX).done(),
                ));
            }
            Err(TrySendError::Full(_)) => {}
        }
    }
}

fn connection(stream: &TcpStream, id: u64, sh: &Shared) {
    let ob = Arc::new(Outbox::new(sh.timing.queue_bytes));
    let Ok(ws) = stream.try_clone() else { return };
    let (wob, seq, hb) = (
        Arc::clone(&ob),
        Arc::clone(&sh.last_seq),
        sh.timing.heartbeat,
    );
    thread::spawn(move || writer(ws, &wob, &seq, hb));
    let fatal = |code: &str, msg: &str| ob.close(Some(error_line(code, msg)));
    let _ = stream.set_read_timeout(Some(sh.timing.hello));
    let Ok(rs) = stream.try_clone() else { return };
    let mut r = BufReader::new(rs);
    let mut buf = Vec::new();
    match read_line(&mut r, &mut buf) {
        Ok(true) => {}
        Ok(false) | Err(ReadErr::Io) => return ob.close(None),
        Err(ReadErr::Timeout) => return fatal("hello_timeout", "no hello within the deadline"),
        Err(ReadErr::TooLong) => return fatal("line_too_long", "line exceeds 65536 bytes"),
    }
    let hello = match parse_line(&buf) {
        Ok(Line::Hello(h)) => h,
        Err(LineError::Fatal(code, msg)) => return fatal(code, &msg),
        _ => return fatal("hello_required", "the first line must be hello"),
    };
    let presented = digest(&hello.token);
    let client = sh.clients.iter().position(|c| {
        let want = digest(&c.token);
        c.name == hello.client && presented.iter().zip(want).fold(0, |a, (x, y)| a | (x ^ y)) == 0
    });
    let Some(client) = client else {
        return fatal("unauthorized", "unknown client or bad token");
    };
    sh.send(Input::Hello {
        id,
        client,
        outbox: Arc::clone(&ob),
        resume: hello.resume_after,
        cancel_on_disconnect: hello.cancel_on_disconnect,
    });
    // From here the session holds the connection: however this ends, it hears so.
    let _closing = Closing { sh, id };
    // No request is read until the session has answered the hello, so nothing (a
    // `busy`, say) goes out before the welcome, or to a client it refuses.
    if !ob.wait_started(sh.timing.hello) {
        return fatal(
            "hello_timeout",
            "the account didn't answer the hello in time",
        );
    }
    let _ = stream.set_read_timeout(Some(sh.timing.idle));
    loop {
        match read_line(&mut r, &mut buf) {
            Ok(true) => {}
            Ok(false) | Err(ReadErr::Io) => break,
            Err(ReadErr::Timeout) => {
                fatal("idle_timeout", "peer silent");
                break;
            }
            Err(ReadErr::TooLong) => {
                fatal("line_too_long", "line exceeds 65536 bytes");
                break;
            }
        }
        match parse_line(&buf) {
            Ok(Line::Heartbeat) => {}
            Ok(Line::Request { request_id, op }) => sh.request(
                &ob,
                client,
                Input::Request {
                    id,
                    request: Ok((request_id, op)),
                },
            ),
            Ok(Line::Hello(_)) => {
                fatal("hello_required", "hello only once");
                break;
            }
            Err(LineError::Fatal(code, msg)) => {
                fatal(code, &msg);
                break;
            }
            Err(LineError::Invalid {
                request_id,
                op,
                invalid,
            }) => sh.request(
                &ob,
                client,
                Input::Request {
                    id,
                    request: Err((request_id, op, invalid)),
                },
            ),
        }
    }
}

/// Tells the session a connection has ended, when dropped.
struct Closing<'a> {
    sh: &'a Shared,
    id: u64,
}

impl Drop for Closing<'_> {
    fn drop(&mut self) {
        self.sh.send(Input::Closed(self.id));
    }
}

/// A running account process.
pub struct Gateway {
    addr: SocketAddr,
    tx: SyncSender<Input>,
    stop: Arc<AtomicBool>,
    session: JoinHandle<Result<(), String>>,
}

/// An in-process clock for a simulated account (decision 0022): the session that owns
/// market time advances the broker through this, never through a client connection.
#[derive(Clone)]
pub struct Clock {
    tx: SyncSender<Input>,
}

impl Clock {
    /// Advances the broker to `to` once `orders` order messages (submits and cancels,
    /// valid or not, counted from the client's first connection) from `client` have been
    /// handled, so an order sent before the strategy's `ready` is always stamped before
    /// the clock moves. Queries don't count. Returns the market time and the account's
    /// last event sequence number after the advance's events. A wait that runs
    /// past `wait` is dropped by the gateway and answered with an error, never run late.
    ///
    /// # Errors
    /// An unknown client, the broker's refusal, the order messages not all arriving
    /// within `wait`, or a stopped gateway.
    pub fn advance(
        &self,
        client: &str,
        orders: u64,
        to: u64,
        wait: Duration,
    ) -> Result<crate::session::Advanced, String> {
        let (reply, rx) = sync_channel(1);
        self.tx
            .send(Input::Clock {
                client: client.to_owned(),
                orders,
                to,
                deadline: Instant::now() + wait,
                reply,
            })
            .map_err(|_| "the gateway has stopped".to_owned())?;
        // The gateway answers at the deadline; the margin covers its tick.
        rx.recv_timeout(wait + Duration::from_secs(5))
            .map_err(|_| "the gateway has stopped answering".to_owned())?
    }
}

impl Gateway {
    /// The account's clock, for a session that owns market time.
    #[must_use]
    pub fn clock(&self) -> Clock {
        Clock {
            tx: self.tx.clone(),
        }
    }

    /// The bound listen address.
    #[must_use]
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
    /// Whether the account session has ended (fatal journal error).
    #[must_use]
    pub fn finished(&self) -> bool {
        self.session.is_finished()
    }
    /// Stops the session (`shutting_down` to every client) and the listener.
    ///
    /// # Errors
    /// The session's fatal error, if it ended with one.
    pub fn stop(self) -> Result<(), String> {
        self.stop.store(true, SeqCst);
        let _ = self.tx.send(Input::Shutdown);
        let _ = TcpStream::connect(self.addr);
        self.session
            .join()
            .unwrap_or_else(|_| Err("session panicked".into()))
    }
}

/// Recovers the journal, binds the loopback listener and starts the account session.
///
/// # Errors
/// Path refusal, lock contention, journal corruption or bind failure.
pub fn start<B: Broker + Send + 'static>(cfg: Config, broker: B) -> Result<Gateway, String> {
    if cfg.listen_any {
        check_state(&cfg.state)?;
    } else {
        check_paths(cfg.listen, &cfg.state)?;
    }
    check_state_under(&cfg.state, &cfg.forbid_state)?;
    let (journal, recovered) = Journal::open(&cfg.state, &cfg.account)?;
    let listener =
        TcpListener::bind(cfg.listen).map_err(|e| format!("bind {}: {e}", cfg.listen))?;
    let addr = listener.local_addr().map_err(|e| e.to_string())?;
    let last_seq = Arc::new(AtomicU64::new(journal.last_seq));
    let (tx, rx) = sync_channel(INBOX);
    let (urgent, urgent_rx) = sync_channel(URGENT);
    let refused: Arc<Vec<AtomicU64>> =
        Arc::new(cfg.clients.iter().map(|_| AtomicU64::new(0)).collect());
    let shared = Arc::new(Shared {
        clients: cfg.clients.clone(),
        timing: cfg.timing.clone(),
        last_seq: Arc::clone(&last_seq),
        tx: tx.clone(),
        urgent,
        lockstep: broker.session_clocked(),
        refused: Arc::clone(&refused),
    });
    let mut session = Session::new(cfg, broker, journal, &recovered, last_seq);
    session.refused = refused;
    let session = thread::spawn(move || session.run(&rx, &urgent_rx));
    let stop = Arc::new(AtomicBool::new(false));
    let accept_stop = Arc::clone(&stop);
    thread::spawn(move || {
        let mut next = 0;
        for stream in listener.incoming() {
            if accept_stop.load(SeqCst) {
                break;
            }
            let Ok(stream) = stream else { continue };
            next += 1;
            let (sh, id) = (Arc::clone(&shared), next);
            thread::spawn(move || connection(&stream, id, &sh));
        }
    });
    Ok(Gateway {
        addr,
        tx,
        stop,
        session,
    })
}

#[cfg(test)]
mod inbox_tests {
    use super::{Outbox, Pop, Shared, URGENT};
    use crate::protocol::Op;
    use crate::session::Input;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;
    use std::sync::mpsc::sync_channel;
    use std::time::Duration;

    /// With the inbox full, a request is answered `busy` on its connection at once,
    /// and nothing more is queued.
    #[test]
    fn a_full_inbox_answers_busy() {
        let (tx, rx) = sync_channel(1);
        let sh = Shared {
            clients: Vec::new(),
            timing: crate::server::Timing::default(),
            last_seq: Arc::new(AtomicU64::new(0)),
            tx,
            urgent: sync_channel(URGENT).0,
            lockstep: false,
            refused: Arc::new(vec![AtomicU64::new(0)]),
        };
        let ob = Outbox::new(1 << 20);
        let req = |rid: &str| Input::Request {
            id: 1,
            request: Ok((rid.to_owned(), Op::Account)),
        };
        sh.request(&ob, 0, req("r1"));
        sh.request(&ob, 0, req("r2"));
        let Pop::Line(line) = ob.pop(Duration::from_millis(10)) else {
            panic!("no answer for the second request");
        };
        let v: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(
            (v["request_id"].as_str(), v["op"].as_str()),
            (Some("r2"), Some("account"))
        );
        assert_eq!(
            (v["outcome"].as_str(), v["code"].as_str()),
            (Some("error"), Some("busy"))
        );
        assert!(matches!(rx.try_recv(), Ok(Input::Request { .. })));
        assert!(rx.try_recv().is_err(), "only the first was queued");
    }

    fn shared(
        cap: usize,
    ) -> (
        Shared,
        std::sync::mpsc::Receiver<Input>,
        std::sync::mpsc::Receiver<Input>,
    ) {
        let (tx, rx) = sync_channel(cap);
        let (urgent, urgent_rx) = sync_channel(URGENT);
        let sh = Shared {
            clients: Vec::new(),
            timing: crate::server::Timing::default(),
            last_seq: Arc::new(AtomicU64::new(0)),
            tx,
            urgent,
            lockstep: false,
            refused: Arc::new(vec![AtomicU64::new(0)]),
        };
        (sh, rx, urgent_rx)
    }

    fn request(rid: &str, op: Op) -> Input {
        Input::Request {
            id: 1,
            request: Ok((rid.to_owned(), op)),
        }
    }

    fn rid(i: Input) -> String {
        match i {
            Input::Request {
                request: Ok((rid, _)),
                ..
            } => rid,
            _ => String::new(),
        }
    }

    /// With the inbox full: a flatten goes ahead of it, on the urgent channel; a
    /// cancel waits for room rather than being refused; a refused submit counts
    /// toward a session clock's order messages, and its retry with the same id is
    /// queued once there's room.
    #[test]
    fn safety_requests_never_get_busy() {
        let (sh, rx, urgent) = shared(1);
        let sh = Arc::new(sh);
        let ob = Arc::new(Outbox::new(1 << 20));
        sh.request(&ob, 0, request("r1", Op::Account));
        sh.request(&ob, 0, request("r2", Op::Submit(serde_json::json!({}))));
        assert!(
            matches!(ob.pop(Duration::from_millis(10)), Pop::Line(_)),
            "r2 busy"
        );
        assert_eq!(sh.refused[0].load(std::sync::atomic::Ordering::SeqCst), 1);
        sh.request(
            &ob,
            0,
            request(
                "r3",
                Op::Flatten {
                    reason: "test".into(),
                },
            ),
        );
        assert_eq!(rid(urgent.try_recv().unwrap()), "r3", "ahead of the inbox");
        let (sh2, ob2) = (Arc::clone(&sh), Arc::clone(&ob));
        let cancel = std::thread::spawn(move || {
            sh2.request(
                &ob2,
                0,
                request(
                    "r4",
                    Op::Cancel(crate::broker::OrderRef::Client("x".into())),
                ),
            );
        });
        std::thread::sleep(Duration::from_millis(50));
        assert!(!cancel.is_finished(), "a cancel waits for room");
        assert!(
            matches!(ob.pop(Duration::from_millis(10)), Pop::Idle),
            "and isn't refused"
        );
        assert_eq!(rid(rx.recv().unwrap()), "r1");
        cancel.join().unwrap();
        assert_eq!(rid(rx.recv().unwrap()), "r4");
        sh.request(&ob, 0, request("r2", Op::Submit(serde_json::json!({}))));
        assert_eq!(rid(rx.recv().unwrap()), "r2");
    }

    /// A connection reads no request until its welcome is written; a hello refused,
    /// or never answered, ends it.
    #[test]
    fn requests_wait_for_the_welcome() {
        let ob = Outbox::new(1 << 20);
        assert!(
            !ob.wait_started(Duration::from_millis(10)),
            "never answered"
        );
        ob.push("{\"type\":\"welcome\"}".into());
        assert!(ob.wait_started(Duration::from_millis(10)));
        let refused = Outbox::new(1 << 20);
        refused.close(Some("{\"type\":\"error\"}".into()));
        assert!(!refused.wait_started(Duration::from_millis(10)));
    }

    /// Under a session's clock a full inbox makes a request wait (a run must repeat,
    /// and `busy` would depend on timing); an unkill waits in the inbox, in order.
    #[test]
    fn lockstep_and_unkill_wait_for_room() {
        for (lockstep, op) in [
            (true, Op::Submit(serde_json::json!({}))),
            (false, Op::Unkill),
        ] {
            let (mut sh, rx, urgent) = shared(1);
            sh.lockstep = lockstep;
            let sh = Arc::new(sh);
            let ob = Arc::new(Outbox::new(1 << 20));
            sh.request(&ob, 0, request("r1", Op::Account));
            let (sh2, ob2) = (Arc::clone(&sh), Arc::clone(&ob));
            let waiting = std::thread::spawn(move || sh2.request(&ob2, 0, request("r2", op)));
            std::thread::sleep(Duration::from_millis(50));
            assert!(!waiting.is_finished(), "waits for room");
            assert!(
                matches!(ob.pop(Duration::from_millis(10)), Pop::Idle),
                "not busy"
            );
            assert!(urgent.try_recv().is_err(), "not ahead of the inbox");
            assert_eq!(rid(rx.recv().unwrap()), "r1");
            waiting.join().unwrap();
            assert_eq!(rid(rx.recv().unwrap()), "r2");
        }
    }
}
