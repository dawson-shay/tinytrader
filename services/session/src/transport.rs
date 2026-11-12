//! How a session reaches its actor (decision 0029). The session owns the clock, the
//! merge and the venues, and keeps every rule of the lockstep; a transport only
//! carries the lockstep's messages: the actor's hello, the welcome and the data
//! stream's header, each step with its records, each `ready`, and the end.
//!
//! - [`SessionSocket`] is 0022's: control over `tinytrader.session/1` and market
//!   data over `tinytrader.relay/1`, on two TCP listeners, with orders going to
//!   the venues' gateways over their own sockets.
//! - [`InProc`] calls a Rust [`Actor`] on a thread of its own, its orders going
//!   to each venue's gateway through an in-process client, the same admission
//!   path a TCP connection takes.
use crate::actor::{Actor, ActorStep, Context, Orders};
use crate::protocol::{self, FromStrategy};
use dbn::Metadata;
use dbn::encode::DbnMetadataEncoder;
use order_gateway::Gateway;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, sync_channel};
use std::time::{Duration, Instant};

/// Why an actor's transport failed (0029). Its text is what the run's `reason`
/// says.
#[derive(Debug)]
pub enum ActorError {
    /// No `ready` by the step's deadline.
    Timeout,
    /// Something it sent breaks the protocol.
    Protocol(String),
    /// The actor exited, panicked or returned an error.
    Exited(String),
    /// The transport itself failed (a socket error, a silent peer).
    Io(String),
}

impl std::fmt::Display for ActorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Timeout => f.write_str("no ready by the step's deadline"),
            Self::Protocol(m) | Self::Exited(m) | Self::Io(m) => f.write_str(m),
        }
    }
}

/// A socket's error, by its text.
impl From<String> for ActorError {
    fn from(m: String) -> Self {
        Self::Io(m)
    }
}

impl From<&str> for ActorError {
    fn from(m: &str) -> Self {
        Self::Io(m.to_owned())
    }
}

impl From<ActorError> for String {
    fn from(e: ActorError) -> Self {
        e.to_string()
    }
}

/// What a transport needs to open the run: the welcome, the data stream's header,
/// when to give up waiting, and each venue's gateway (for one that submits in
/// process).
pub struct RunInfo<'a> {
    pub welcome: &'a Value,
    pub metadata: &'a Metadata,
    pub deadline: Instant,
    pub venues: &'a [(String, &'a Gateway)],
}

/// The actor's hello: its name, its revision, and the token it presents.
pub struct Hello {
    pub strategy: String,
    pub revision: String,
    pub token: Option<String>,
}

/// The actor's answer to a step: the step's time, its order messages sent to each
/// venue so far (0022's `sent`, cumulative), and timers to set.
pub struct Ready {
    pub as_of: u64,
    pub sent: Vec<(String, u64)>,
    pub timers: Vec<(u64, String)>,
}

/// One step for the actor.
pub struct Step<'a> {
    /// Its number, from 1.
    pub step: u64,
    pub as_of: u64,
    /// The instant's records, as DBN record bytes, and how many.
    pub records: u64,
    pub bytes: &'a [u8],
    /// The timers due at it.
    pub timers: &'a [String],
    /// Each venue's last event sequence number.
    pub reports: &'a Value,
}

/// The lockstep's messages, carried to an actor and back.
pub trait Transport {
    /// Waits for the actor and its hello, until `deadline`.
    ///
    /// # Errors
    /// No actor by the deadline, or a first message that isn't a hello.
    fn hello(&mut self, deadline: Instant) -> Result<Hello, ActorError>;
    /// Sends the welcome, opens the data stream with `metadata`, and reads the
    /// actor's first `ready` (its timers), waiting until `deadline`. The session
    /// checks the hello (its token) between the two.
    ///
    /// # Errors
    /// The data stream not joined in time, or a broken connection.
    fn open(&mut self, run: &RunInfo<'_>) -> Result<Ready, ActorError>;
    /// Sends a step: its message, then its records.
    ///
    /// # Errors
    /// A broken or blocked connection.
    fn step(&mut self, step: &Step<'_>) -> Result<(), ActorError>;
    /// The actor's answer to the last step, by `deadline`.
    ///
    /// # Errors
    /// No answer in time, a malformed one, or a second hello.
    fn ready(&mut self, deadline: Instant) -> Result<Ready, ActorError>;
    /// The run's data is over: closes the data stream. The run's end follows.
    fn close_data(&mut self);
    /// Tells the actor the run has ended, if it can still be told.
    fn end(&mut self, end: &Value);
}

const HELLO_AGAIN: &str = "hello only once";

/// 0022's transport: a control and a data listener on TCP.
pub struct SessionSocket {
    control: TcpListener,
    data: TcpListener,
    /// How long a read or write may block.
    wait: Duration,
    ctl: Option<TcpStream>,
    reader: Option<BufReader<TcpStream>>,
    out: Option<BufWriter<TcpStream>>,
}

impl SessionSocket {
    /// Over the session's two listeners; a read or write may block for `wait`.
    #[must_use]
    pub fn new(control: TcpListener, data: TcpListener, wait: Duration) -> Self {
        Self {
            control,
            data,
            wait,
            ctl: None,
            reader: None,
            out: None,
        }
    }

    fn reader(&mut self) -> Result<&mut BufReader<TcpStream>, String> {
        self.reader
            .as_mut()
            .ok_or_else(|| "no control connection".into())
    }

    fn read_ready(&mut self) -> Result<Ready, ActorError> {
        match protocol::read(self.reader()?)? {
            FromStrategy::Ready {
                as_of,
                sent,
                timers,
            } => Ok(Ready {
                as_of,
                sent,
                timers,
            }),
            FromStrategy::Hello { .. } => Err(ActorError::Protocol(HELLO_AGAIN.into())),
        }
    }
}

/// Accepts one connection by `deadline`.
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

impl Transport for SessionSocket {
    fn hello(&mut self, deadline: Instant) -> Result<Hello, ActorError> {
        let ctl = accept(&self.control, deadline, "control")?;
        ctl.set_read_timeout(Some(self.wait))
            .map_err(|e| e.to_string())?;
        ctl.set_write_timeout(Some(self.wait))
            .map_err(|e| e.to_string())?;
        self.reader = Some(BufReader::new(ctl.try_clone().map_err(|e| e.to_string())?));
        self.ctl = Some(ctl);
        match protocol::read(self.reader()?)? {
            FromStrategy::Hello {
                strategy,
                revision,
                token,
            } => Ok(Hello {
                strategy,
                revision,
                token,
            }),
            FromStrategy::Ready { .. } => Err(ActorError::Protocol(
                "the strategy's first message must be hello".into(),
            )),
        }
    }

    fn open(&mut self, run: &RunInfo<'_>) -> Result<Ready, ActorError> {
        protocol::send(self.ctl.as_mut().ok_or("no control")?, run.welcome)?;
        let data = accept(&self.data, run.deadline, "data")?;
        data.set_read_timeout(Some(self.wait))
            .map_err(|e| e.to_string())?;
        // A strategy that stops reading its data ends the run, never blocks it.
        data.set_write_timeout(Some(self.wait))
            .map_err(|e| e.to_string())?;
        let mut join = String::new();
        BufReader::new(data.try_clone().map_err(|e| e.to_string())?)
            .read_line(&mut join)
            .map_err(|e| format!("data join: {e}"))?;
        let j: Value = serde_json::from_str(&join).map_err(|e| format!("data join: {e}"))?;
        if j["protocol"] != "tinytrader.relay/1" {
            return Err(ActorError::Protocol(
                "the data join must name tinytrader.relay/1".into(),
            ));
        }
        let mut out = BufWriter::new(data);
        DbnMetadataEncoder::new(&mut out)
            .encode(run.metadata)
            .map_err(|e| e.to_string())?;
        out.flush().map_err(|e| e.to_string())?;
        self.out = Some(out);
        match self.read_ready() {
            Err(ActorError::Protocol(e)) if e == HELLO_AGAIN => Err(ActorError::Protocol(
                "the strategy's first ready must be ready{as_of: 0}, sending nothing".into(),
            )),
            r => r,
        }
    }

    fn step(&mut self, step: &Step<'_>) -> Result<(), ActorError> {
        let msg = json!({"type": "step", "step": step.step, "as_of": step.as_of,
                         "records": step.records, "timers": step.timers,
                         "reports": step.reports});
        protocol::send(self.ctl.as_mut().ok_or("no control")?, &msg)?;
        let out = self.out.as_mut().ok_or("no data stream")?;
        out.write_all(step.bytes)
            .and_then(|()| out.flush())
            .map_err(|e| ActorError::Io(format!("data: {e}")))
    }

    /// Its deadline is the control socket's read timeout, set to the same wait.
    fn ready(&mut self, _deadline: Instant) -> Result<Ready, ActorError> {
        self.read_ready()
    }

    fn close_data(&mut self) {
        self.out = None;
    }

    fn end(&mut self, end: &Value) {
        if let Some(c) = &mut self.ctl {
            let _ = protocol::send(c, end);
        }
    }
}

/// What the session asks of an in-process actor's thread.
enum Ask {
    Start(Value, BTreeMap<String, Orders>),
    Step(ActorStep),
    End(Value),
}

/// An in-process actor's answer: the timers it set, and each venue's order
/// messages sent so far.
type Answer = Result<(Vec<(u64, String)>, Vec<(String, u64)>), String>;

/// 0029's `inproc` transport: a Rust [`Actor`] on a thread of its own, called with
/// each step. Its orders go to each venue's gateway through an in-process client,
/// the same hello, parsing, inbox and admission a TCP connection has; under the
/// session's clock nothing is `busy`.
///
/// A step's records are handed to the thread as one copy of the step's bytes. A
/// step past its deadline ends the run: a thread can't be stopped, so it is left
/// behind, and the actor's code runs in no other step.
pub struct InProc {
    actor: Option<Box<dyn Actor>>,
    name: (String, String),
    asks: Option<SyncSender<Ask>>,
    answers: Option<Receiver<Answer>>,
    /// The last step's time, which its `ready` answers for.
    as_of: u64,
}

impl InProc {
    /// For `actor`, not yet started.
    #[must_use]
    pub fn new(actor: Box<dyn Actor>) -> Self {
        let name = actor.name();
        Self {
            actor: Some(actor),
            name,
            asks: None,
            answers: None,
            as_of: 0,
        }
    }

    fn answer(&mut self, deadline: Instant) -> Result<Answer, ActorError> {
        let rx = self.answers.as_ref().ok_or("the actor isn't started")?;
        match rx.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
            Ok(a) => Ok(a),
            Err(RecvTimeoutError::Timeout) => Err(ActorError::Timeout),
            Err(RecvTimeoutError::Disconnected) => {
                Err(ActorError::Exited("the actor panicked".into()))
            }
        }
    }

    fn ready_from(&mut self, as_of: u64, deadline: Instant) -> Result<Ready, ActorError> {
        match self.answer(deadline)? {
            Ok((timers, sent)) => Ok(Ready {
                as_of,
                sent,
                timers,
            }),
            Err(e) => Err(ActorError::Exited(e)),
        }
    }
}

/// The actor's thread: each ask answered in turn, until the end or the session
/// goes.
fn actor_thread(mut actor: Box<dyn Actor>, asks: &Receiver<Ask>, answers: &SyncSender<Answer>) {
    let mut ctx: Option<Context> = None;
    while let Ok(ask) = asks.recv() {
        let answer = match ask {
            Ask::Start(welcome, orders) => {
                let c = ctx.insert(Context::new(welcome, orders));
                actor.start(c).map(|t| (t, c.sent()))
            }
            Ask::Step(step) => match ctx.as_mut() {
                Some(c) => actor.step(&step, c).map(|t| (t, c.sent())),
                None => Err("a step before the start".into()),
            },
            Ask::End(end) => {
                actor.end(&end);
                return;
            }
        };
        if answers.send(answer).is_err() {
            return;
        }
    }
}

impl Transport for InProc {
    fn hello(&mut self, _deadline: Instant) -> Result<Hello, ActorError> {
        Ok(Hello {
            strategy: self.name.0.clone(),
            revision: self.name.1.clone(),
            token: None,
        })
    }

    fn open(&mut self, run: &RunInfo<'_>) -> Result<Ready, ActorError> {
        let mut orders = BTreeMap::new();
        for (venue, gateway) in run.venues {
            let o = &run.welcome["orders"][venue];
            let client = gateway
                .connect_local(
                    o["client"].as_str().unwrap_or_default(),
                    o["token"].as_str().unwrap_or_default(),
                    false,
                )
                .map_err(|e| ActorError::Io(format!("venue {venue}: {e}")))?;
            orders.insert(venue.clone(), Orders::new(Box::new(client)));
        }
        let actor = self.actor.take().ok_or("the actor has already run")?;
        // One ask and one answer in flight: lockstep is the bound.
        let (ask_tx, ask_rx) = sync_channel(1);
        let (ans_tx, ans_rx) = sync_channel(1);
        std::thread::Builder::new()
            .name(format!("actor {}", self.name.0))
            .spawn(move || actor_thread(actor, &ask_rx, &ans_tx))
            .map_err(|e| ActorError::Io(format!("the actor's thread: {e}")))?;
        ask_tx
            .send(Ask::Start(run.welcome.clone(), orders))
            .map_err(|_| ActorError::Exited("the actor panicked".into()))?;
        self.asks = Some(ask_tx);
        self.answers = Some(ans_rx);
        self.ready_from(0, run.deadline)
    }

    fn step(&mut self, step: &Step<'_>) -> Result<(), ActorError> {
        let reports = step
            .reports
            .as_object()
            .into_iter()
            .flatten()
            .filter_map(|(k, v)| Some((k.clone(), v.as_u64()?)))
            .collect();
        self.as_of = step.as_of;
        let ask = Ask::Step(ActorStep {
            step: step.step,
            as_of: step.as_of,
            bytes: step.bytes.to_vec(),
            count: step.records,
            timers: step.timers.to_vec(),
            reports,
        });
        self.asks
            .as_ref()
            .ok_or("the actor isn't started")?
            .send(ask)
            .map_err(|_| ActorError::Exited("the actor panicked".into()))
    }

    fn ready(&mut self, deadline: Instant) -> Result<Ready, ActorError> {
        // An in-process actor answers the step it was given: its time is the step's.
        self.ready_from(self.as_of, deadline)
    }

    fn close_data(&mut self) {}

    fn end(&mut self, end: &Value) {
        if let Some(tx) = &self.asks {
            let _ = tx.try_send(Ask::End(end.clone()));
        }
    }
}
