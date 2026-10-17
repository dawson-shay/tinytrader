//! The order gateway: a broker-access passthrough for one broker account over the
//! `tinytrader.gateway/1` loopback NDJSON protocol (`README.md`).
//!
//! The account session validates, forwards and correlates client requests, journals
//! every event before delivery, never resends an ambiguous submit, and resolves unknown
//! outcomes by lookup. Brokers sit behind the synchronous [`broker::Broker`] seam: the
//! Alpaca paper broker ([`AlpacaPaper`]) and the simulated broker ([`SimBroker`]). No
//! strategy or portfolio lives here.
mod alpaca;
pub mod broker;
pub mod client;
mod intent;
mod journal;
mod protocol;
pub mod rules;
mod safety;
mod server;
mod session;
mod sim;

pub use alpaca::{AlpacaOptions, AlpacaPaper, load_credentials, trading};
pub use intent::{Intent, Leg, MaxLoss, max_loss};
pub use protocol::{KillLevel, MAX_LINE, SCHEMA};
pub use safety::Profile;
pub use server::{
    ClientSpec, Config, Gateway, Role, Timing, check_paths, check_state, check_state_under, start,
};
pub use session::wall_ns;
pub use sim::SimBroker;
