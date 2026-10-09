use serde::{Deserialize, Serialize};

/// A message that crosses the network. O(1) in the number of processes.
/// Dependency vectors and last-sent snapshots are not represented here.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WireMsg {
    Msg {
        mid: u64,
        per: bool,
        payload: Vec<u8>,
    },
    Ack {
        mid: u64,
    },
    Permit {
        mid: u64,
    },
}

impl WireMsg {
    /// Scalar header plus payload bytes. Ignores codec framing and does not grow with n.
    pub fn wire_len(&self) -> usize {
        match self {
            WireMsg::Msg { payload, .. } => 8 + 1 + payload.len(),
            WireMsg::Ack { .. } | WireMsg::Permit { .. } => 8,
        }
    }

    pub fn mid(&self) -> u64 {
        match self {
            WireMsg::Msg { mid, .. } | WireMsg::Ack { mid } | WireMsg::Permit { mid } => *mid,
        }
    }
}

/// One network send. The destination is the FIFO channel, not causal metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Emission {
    pub dst: usize,
    pub msg: WireMsg,
}

/// Local send/receive trace, in the order the process observed it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    Sent { dst: usize, msg: WireMsg },
    Recv { src: usize, msg: WireMsg },
}
