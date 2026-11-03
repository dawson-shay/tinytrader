//! Sessions (decision 0022): one process per run. In backtest mode a session reads
//! its sources' held days through the store producer, merges them on one clock,
//! serves them to a strategy over `tinytrader.relay/1`, hosts one simulated venue per
//! account as an ordinary gateway, and advances everything in lockstep on the
//! strategy's `ready` (`tinytrader.session/1`). It ends with a run record and an
//! audit hash: the same configuration and strategy give the same hash.
pub mod actor;
pub mod config;
pub mod example;
pub mod fixture;
pub mod protocol;
pub mod run;
pub mod sandbox;
pub mod transport;
pub mod watch;

pub use config::Config;
pub use run::{Outcome, Ready, Session};
