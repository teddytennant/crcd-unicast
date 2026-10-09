//! In-process Isolated-Buffer Optimistic Model for Byzantine-tolerant causal unicast.
//!
//! Patel and Kshemkalyani, arXiv:2610.07368. Application messages carry a constant-size
//! header. Dependency vectors and last-sent snapshots stay in local records.

mod process;
mod sim;
mod wire;

pub use process::Process;
pub use sim::Simulator;
pub use wire::{Emission, Event, WireMsg};
