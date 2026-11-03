//! The single `trade_updates` connection: a background thread that authenticates,
//! listens, forwards order events and reconnects with back-off. It reports
//! `StreamDown` on every loss and `StreamUp` after each `listening`; the session then
//! degrades and resyncs through REST.
use super::order_update;
use crate::alpaca::trading::{LISTEN, StreamMessage, parse_stream};
use crate::broker::Update;
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::sync::mpsc::SyncSender;
use std::thread;
use std::time::{Duration, Instant};
use tungstenite::{Message, WebSocket, stream::MaybeTlsStream};

const SLICE: Duration = Duration::from_millis(100);
const HANDSHAKE: Duration = Duration::from_secs(10);
const PING_AFTER: Duration = Duration::from_secs(15);
const SILENT: Duration = Duration::from_secs(40);

type Ws = WebSocket<MaybeTlsStream<TcpStream>>;

/// Stops the thread (within one read slice) when dropped; it is not joined.
pub(crate) struct Stream {
    stop: Arc<AtomicBool>,
}

impl Drop for Stream {
    fn drop(&mut self) {
        self.stop.store(true, SeqCst);
    }
}

enum End {
    Stopped,
    Lost(String),
}

pub(crate) fn spawn(
    url: String,
    auth: String,
    tx: SyncSender<Update>,
    backoff: (Duration, Duration),
) -> Stream {
    let stop = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&stop);
    thread::spawn(move || {
        let mut delay = backoff.0;
        loop {
            let mut up = false;
            match session(&url, &auth, &tx, &flag, &mut up) {
                End::Stopped => return,
                End::Lost(why) => eprintln!("gateway: trade_updates: {why}"),
            }
            if up {
                delay = backoff.0;
                if tx.send(Update::StreamDown).is_err() {
                    return;
                }
            }
            let until = Instant::now() + delay;
            while Instant::now() < until {
                if flag.load(SeqCst) {
                    return;
                }
                thread::sleep(SLICE.min(delay));
            }
            delay = (delay * 2).min(backoff.1);
        }
    });
    Stream { stop }
}

fn connect(url: &str) -> Result<(Ws, TcpStream), String> {
    let (tls, rest) = match url.split_once("://") {
        Some(("wss", r)) => (true, r),
        Some(("ws", r)) => (false, r),
        _ => return Err("stream URL scheme".into()),
    };
    let authority = rest.split('/').next().unwrap_or(rest);
    let target = if authority.contains(':') {
        authority.to_owned()
    } else {
        format!("{authority}:{}", if tls { 443 } else { 80 })
    };
    let addr = target
        .to_socket_addrs()
        .map_err(|e| format!("resolve: {e}"))?
        .next()
        .ok_or("resolve: no address")?;
    let tcp = TcpStream::connect_timeout(&addr, HANDSHAKE).map_err(|e| format!("connect: {e}"))?;
    let handle = tcp.try_clone().map_err(|e| e.to_string())?;
    handle
        .set_read_timeout(Some(HANDSHAKE))
        .map_err(|e| e.to_string())?;
    let (ws, _) = tungstenite::client_tls_with_config(url, tcp, None, None)
        .map_err(|e| format!("handshake: {e}"))?;
    Ok((ws, handle))
}

/// The next message as text (empty for a control frame); `Ok(None)` on a quiet slice.
fn read(ws: &mut Ws) -> Result<Option<String>, String> {
    match ws.read() {
        Ok(Message::Text(t)) => Ok(Some(t.as_str().to_owned())),
        Ok(Message::Binary(b)) => Ok(Some(String::from_utf8_lossy(&b).into_owned())),
        Ok(Message::Close(_)) => Err("closed by broker".into()),
        Ok(_) => Ok(Some(String::new())),
        Err(tungstenite::Error::Io(e))
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

/// Waits for a control reply (`authorization`, `listening`).
fn expect(ws: &mut Ws, want: &StreamMessage, flag: &AtomicBool) -> Result<(), End> {
    let until = Instant::now() + HANDSHAKE;
    while Instant::now() < until && !flag.load(SeqCst) {
        if let Some(t) = read(ws).map_err(End::Lost)? {
            match parse_stream(&t) {
                ref m if m == want => return Ok(()),
                StreamMessage::Unauthorized => {
                    return Err(End::Lost("authorization refused".into()));
                }
                StreamMessage::Error(e) => return Err(End::Lost(format!("broker error: {e}"))),
                _ => {}
            }
        }
    }
    Err(if flag.load(SeqCst) {
        End::Stopped
    } else {
        End::Lost("no reply".into())
    })
}

fn session(
    url: &str,
    auth: &str,
    tx: &SyncSender<Update>,
    flag: &AtomicBool,
    up: &mut bool,
) -> End {
    let (mut ws, tcp) = match connect(url) {
        Ok(x) => x,
        Err(e) => return End::Lost(e),
    };
    let setup = (|| {
        ws.send(Message::text(auth))
            .map_err(|e| End::Lost(e.to_string()))?;
        expect(&mut ws, &StreamMessage::Authorized, flag)?;
        ws.send(Message::text(LISTEN))
            .map_err(|e| End::Lost(e.to_string()))?;
        expect(&mut ws, &StreamMessage::Listening, flag)
    })();
    if let Err(e) = setup {
        return e;
    }
    let _ = tcp.set_read_timeout(Some(SLICE));
    if tx.send(Update::StreamUp).is_err() {
        return End::Stopped;
    }
    *up = true;
    let (mut heard, mut pinged) = (Instant::now(), false);
    loop {
        if flag.load(SeqCst) {
            let _ = ws.close(None);
            return End::Stopped;
        }
        match read(&mut ws) {
            Err(e) => return End::Lost(e),
            Ok(Some(t)) => {
                heard = Instant::now();
                pinged = false;
                match parse_stream(&t) {
                    StreamMessage::Update(u) => {
                        if tx.send(Update::Order(Box::new(order_update(*u)))).is_err() {
                            return End::Stopped;
                        }
                    }
                    StreamMessage::Error(e) => eprintln!("gateway: trade_updates error: {e}"),
                    _ => {}
                }
            }
            Ok(None) => {
                let quiet = heard.elapsed();
                if quiet > SILENT {
                    return End::Lost("silent connection".into());
                }
                if quiet > PING_AFTER && !pinged {
                    pinged = true;
                    if let Err(e) = ws.send(Message::Ping(Vec::new().into())) {
                        return End::Lost(e.to_string());
                    }
                }
            }
        }
    }
}
