//! A strategy written once and run either way (decision 0029): in process, called
//! with each step by the session's `inproc` transport, or as a client of 0022's
//! sockets through [`run_socket`]. Either way its orders reach each venue through
//! the gateway's own admission path: an in-process client
//! ([`order_gateway::LocalClient`]) or a TCP one.
use crate::protocol::PROTOCOL;
use dbn::RecordRef;
use dbn::decode::dbn::RecordDecoder;
use dbn::decode::{DbnDecoder, DecodeRecordRef};
use order_gateway::LocalClient;
use order_gateway::client::Client;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

/// A strategy the session can run in process, or [`run_socket`] over 0022's
/// sockets.
pub trait Actor: Send + 'static {
    /// Its name and its code's revision, for the hello and the audit.
    fn name(&self) -> (String, String);
    /// Before the first step: its first timers, as `(at, id)`.
    ///
    /// # Errors
    /// Why it can't run; the run ends incomplete with it.
    fn start(&mut self, ctx: &mut Context) -> Result<Vec<(u64, String)>, String>;
    /// One step: its records, the timers due and each venue's last report. Its
    /// answer is the timers to set; its orders go through `ctx`.
    ///
    /// # Errors
    /// Why it can't go on; the run ends incomplete with it.
    fn step(&mut self, step: &ActorStep, ctx: &mut Context) -> Result<Vec<(u64, String)>, String>;
    /// The run's end: its status, reason and audit.
    fn end(&mut self, _end: &Value) {}
}

/// One step, as an actor sees it.
pub struct ActorStep {
    /// Its number, from 1.
    pub step: u64,
    pub as_of: u64,
    /// The instant's records, as DBN record bytes: see [`ActorStep::records`].
    pub bytes: Vec<u8>,
    /// How many records.
    pub count: u64,
    /// The timers due at it.
    pub timers: Vec<String>,
    /// Each venue's last event sequence number: an actor that has read its events
    /// up to it has every report of the step.
    pub reports: BTreeMap<String, u64>,
}

impl ActorStep {
    /// Calls `f` with each record of the step, in order.
    ///
    /// # Errors
    /// Bytes that aren't DBN records, or `f`'s own error.
    pub fn records(
        &self,
        mut f: impl FnMut(RecordRef<'_>) -> Result<(), String>,
    ) -> Result<(), String> {
        let mut dec = RecordDecoder::new(&self.bytes[..]);
        while let Some(r) = dec.decode_record_ref().map_err(|e| e.to_string())? {
            f(r)?;
        }
        Ok(())
    }
}

/// A venue's order connection, as an actor uses it: in process or over TCP.
pub trait OrderLink: Send {
    /// Sends a message of type `ty` with `fields` (a JSON object's members).
    ///
    /// # Errors
    /// A closed connection.
    fn send(&mut self, ty: &str, fields: &str) -> Result<(), String>;
    /// The next line from the gateway, waiting at most `wait` (`None` if none came).
    ///
    /// # Errors
    /// A closed connection.
    fn line(&mut self, wait: Duration) -> Result<Option<String>, String>;
}

impl OrderLink for LocalClient {
    fn send(&mut self, ty: &str, fields: &str) -> Result<(), String> {
        LocalClient::send(self, ty, fields)
    }
    fn line(&mut self, wait: Duration) -> Result<Option<String>, String> {
        LocalClient::line(self, wait)
    }
}

/// A gateway connection over TCP.
struct TcpLink(Client);

impl OrderLink for TcpLink {
    fn send(&mut self, ty: &str, fields: &str) -> Result<(), String> {
        self.0.send(ty, fields).map_err(|e| e.to_string())
    }
    fn line(&mut self, wait: Duration) -> Result<Option<String>, String> {
        self.0.set_timeout(wait).map_err(|e| e.to_string())?;
        match self.0.line() {
            Ok(Some(l)) => Ok(Some(l)),
            Ok(None) => Err("the gateway closed the connection".into()),
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                Ok(None)
            }
            Err(e) => Err(e.to_string()),
        }
    }
}

/// A venue's orders: its connection, and the order messages (submits and cancels,
/// valid or not) sent through it, which the session's `ready.sent` counts.
pub struct Orders {
    link: Box<dyn OrderLink>,
    sent: u64,
}

impl Orders {
    /// Over `link`, nothing sent yet.
    #[must_use]
    pub fn new(link: Box<dyn OrderLink>) -> Self {
        Self { link, sent: 0 }
    }

    /// Sends a message; a `submit` or `cancel` counts as an order message.
    ///
    /// # Errors
    /// A closed connection.
    pub fn send(&mut self, ty: &str, fields: &str) -> Result<(), String> {
        self.link.send(ty, fields)?;
        if matches!(ty, "submit" | "cancel") {
            self.sent += 1;
        }
        Ok(())
    }

    /// The next line from the gateway, waiting at most `wait`.
    ///
    /// # Errors
    /// A closed connection.
    pub fn line(&mut self, wait: Duration) -> Result<Option<String>, String> {
        self.link.line(wait)
    }
}

/// What an actor works with: the session's welcome, and each venue's orders.
pub struct Context {
    pub welcome: Value,
    orders: BTreeMap<String, Orders>,
}

impl Context {
    /// With these venues' orders.
    #[must_use]
    pub fn new(welcome: Value, orders: BTreeMap<String, Orders>) -> Self {
        Self { welcome, orders }
    }

    /// A venue's orders.
    pub fn orders(&mut self, venue: &str) -> Option<&mut Orders> {
        self.orders.get_mut(venue)
    }

    /// Each venue's order messages sent so far: `ready.sent`.
    #[must_use]
    pub fn sent(&self) -> Vec<(String, u64)> {
        self.orders
            .iter()
            .map(|(v, o)| (v.clone(), o.sent))
            .collect()
    }
}

fn timers_json(t: &[(u64, String)]) -> Value {
    Value::Array(
        t.iter()
            .map(|(at, id)| json!({"at": at, "id": id}))
            .collect(),
    )
}

fn sent_json(sent: &[(String, u64)]) -> Value {
    Value::Object(sent.iter().map(|(v, n)| (v.clone(), json!(n))).collect())
}

/// Runs `actor` as a client of a session's sockets (0022): `hello` on the control
/// address (with `token`, if the session started this process), the data stream
/// joined, each venue's gateway over TCP, and a `ready` for every step. Returns
/// the session's `end`.
///
/// # Errors
/// A connection that fails, a message that isn't the protocol, or the actor's own
/// error.
pub fn run_socket(
    mut actor: impl Actor,
    control: SocketAddr,
    token: Option<&str>,
) -> Result<Value, String> {
    let mut ctl = TcpStream::connect(control).map_err(|e| format!("control: {e}"))?;
    let mut cr = BufReader::new(ctl.try_clone().map_err(|e| e.to_string())?);
    let send = |w: &mut TcpStream, v: &Value| {
        w.write_all(format!("{v}\n").as_bytes())
            .map_err(|e| format!("control: {e}"))
    };
    let recv = |r: &mut BufReader<TcpStream>| -> Result<Value, String> {
        let mut line = String::new();
        if r.read_line(&mut line)
            .map_err(|e| format!("control: {e}"))?
            == 0
        {
            return Err("the session closed the control connection".into());
        }
        serde_json::from_str(&line).map_err(|e| format!("control: {e}"))
    };
    let (strategy, revision) = actor.name();
    let mut hello = json!({"type": "hello", "protocol": PROTOCOL, "strategy": strategy,
                           "revision": revision});
    if let Some(t) = token {
        hello["token"] = json!(t);
    }
    send(&mut ctl, &hello)?;
    let welcome = recv(&mut cr)?;
    if welcome["type"] != "welcome" {
        return Err(format!("expected a welcome, got {welcome}"));
    }
    let data_addr = welcome["data"]
        .as_str()
        .ok_or("the welcome has no data address")?;
    let mut data = TcpStream::connect(data_addr).map_err(|e| format!("data: {e}"))?;
    data.write_all(
        format!(
            "{}\n",
            json!({"protocol": "tinytrader.relay/1", "client": strategy})
        )
        .as_bytes(),
    )
    .map_err(|e| format!("data: {e}"))?;
    let mut dec = DbnDecoder::new(BufReader::new(data)).map_err(|e| format!("data: {e}"))?;
    let mut orders = BTreeMap::new();
    for (venue, o) in welcome["orders"].as_object().into_iter().flatten() {
        let addr: SocketAddr = o["addr"]
            .as_str()
            .and_then(|a| a.parse().ok())
            .ok_or_else(|| format!("venue {venue}: no address"))?;
        let c = Client::connect(
            addr,
            o["client"].as_str().unwrap_or_default(),
            o["token"].as_str().unwrap_or_default(),
            None,
            false,
        )
        .map_err(|e| format!("venue {venue}: {e}"))?;
        orders.insert(venue.clone(), Orders::new(Box::new(TcpLink(c))));
    }
    let mut ctx = Context::new(welcome, orders);
    let first = actor.start(&mut ctx)?;
    send(
        &mut ctl,
        &json!({"type": "ready", "as_of": 0, "sent": sent_json(&ctx.sent()),
                "timers": timers_json(&first)}),
    )?;
    loop {
        let msg = recv(&mut cr)?;
        if msg["type"] == "end" {
            actor.end(&msg);
            return Ok(msg);
        }
        let count = msg["records"].as_u64().ok_or("a step needs records")?;
        let mut bytes = Vec::new();
        for _ in 0..count {
            let r = dec
                .decode_record_ref()
                .map_err(|e| format!("data: {e}"))?
                .ok_or("data: the stream ended inside a step")?;
            bytes.extend_from_slice(r.as_ref());
        }
        let step = ActorStep {
            step: msg["step"].as_u64().unwrap_or_default(),
            as_of: msg["as_of"].as_u64().ok_or("a step needs as_of")?,
            bytes,
            count,
            timers: msg["timers"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|t| t.as_str().map(str::to_owned))
                .collect(),
            reports: msg["reports"]
                .as_object()
                .into_iter()
                .flatten()
                .filter_map(|(k, v)| Some((k.clone(), v.as_u64()?)))
                .collect(),
        };
        let new = actor.step(&step, &mut ctx)?;
        send(
            &mut ctl,
            &json!({"type": "ready", "as_of": step.as_of, "sent": sent_json(&ctx.sent()),
                    "timers": timers_json(&new)}),
        )?;
    }
}
