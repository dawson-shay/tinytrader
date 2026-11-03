//! How a session reaches its actor (decision 0029). The session owns the clock, the
//! merge and the venues, and keeps every rule of the lockstep; a transport only
//! carries the lockstep's messages: the actor's hello, the welcome and the data
//! stream's header, each step with its records, each `ready`, and the end.
//!
//! [`SessionSocket`] is 0022's: control over `tinytrader.session/1` and market data
//! over `tinytrader.relay/1`, on two TCP listeners, with orders going to the venues'
//! gateways over their own sockets.
use crate::protocol::{self, FromStrategy};
use dbn::Metadata;
use dbn::encode::DbnMetadataEncoder;
use serde_json::{Value, json};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::time::{Duration, Instant};

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
    fn hello(&mut self, deadline: Instant) -> Result<Hello, String>;
    /// Sends the welcome, opens the data stream with `metadata`, and reads the
    /// actor's first `ready` (its timers), waiting until `deadline` for the data
    /// connection.
    ///
    /// # Errors
    /// The data stream not joined in time, or a broken connection.
    fn open(
        &mut self,
        welcome: &Value,
        metadata: &Metadata,
        deadline: Instant,
    ) -> Result<Ready, String>;
    /// Sends a step: its message, then its records.
    ///
    /// # Errors
    /// A broken or blocked connection.
    fn step(&mut self, step: &Step<'_>) -> Result<(), String>;
    /// The actor's answer to the last step.
    ///
    /// # Errors
    /// No answer in time, a malformed one, or a second hello.
    fn ready(&mut self) -> Result<Ready, String>;
    /// Tells the actor the run has ended, if it can still be told.
    fn end(&mut self, end: &Value);
}

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

    fn read_ready(&mut self) -> Result<Ready, String> {
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
            FromStrategy::Hello { .. } => Err("hello only once".into()),
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
    fn hello(&mut self, deadline: Instant) -> Result<Hello, String> {
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
            FromStrategy::Ready { .. } => Err("the strategy's first message must be hello".into()),
        }
    }

    fn open(
        &mut self,
        welcome: &Value,
        metadata: &Metadata,
        deadline: Instant,
    ) -> Result<Ready, String> {
        protocol::send(self.ctl.as_mut().ok_or("no control")?, welcome)?;
        let data = accept(&self.data, deadline, "data")?;
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
            return Err("the data join must name tinytrader.relay/1".into());
        }
        let mut out = BufWriter::new(data);
        DbnMetadataEncoder::new(&mut out)
            .encode(metadata)
            .map_err(|e| e.to_string())?;
        out.flush().map_err(|e| e.to_string())?;
        self.out = Some(out);
        self.read_ready()
    }

    fn step(&mut self, step: &Step<'_>) -> Result<(), String> {
        let msg = json!({"type": "step", "step": step.step, "as_of": step.as_of,
                         "records": step.records, "timers": step.timers,
                         "reports": step.reports});
        protocol::send(self.ctl.as_mut().ok_or("no control")?, &msg)?;
        let out = self.out.as_mut().ok_or("no data stream")?;
        out.write_all(step.bytes)
            .and_then(|()| out.flush())
            .map_err(|e| format!("data: {e}"))
    }

    fn ready(&mut self) -> Result<Ready, String> {
        self.read_ready()
    }

    fn end(&mut self, end: &Value) {
        if let Some(c) = &mut self.ctl {
            let _ = protocol::send(c, end);
        }
    }
}
