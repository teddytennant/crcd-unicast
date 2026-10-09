use std::collections::VecDeque;
use std::fmt;

use crate::wire::{Emission, Event, WireMsg};

/// Local message record. Never placed on the wire.
#[derive(Clone, Debug)]
struct Record {
    mid: u64,
    per: bool,
    payload: Vec<u8>,
    sent: bool,
    dv: Vec<i64>,
    lms: Vec<i64>,
}

/// One process in the Isolated-Buffer Optimistic Model (Algorithm 2).
pub struct Process {
    id: usize,
    n: usize,
    m_max: usize,
    q_max: usize,
    ck: u64,
    ld: Vec<i64>,
    lpr: Vec<i64>,
    lms: Vec<i64>,
    u: Vec<VecDeque<Record>>,
    q: Vec<VecDeque<Record>>,
    delivered: Vec<Vec<u8>>,
    events: Vec<Event>,
    /// Drop ACK and PERMIT emissions. MSG is still accepted into the delivery log.
    drop_control: bool,
}

impl Process {
    pub fn new(id: usize, n: usize, m_max: usize, q_max: usize) -> Self {
        assert!(n >= 1, "n >= 1");
        assert!(id < n, "id < n");
        assert!(m_max >= 1, "m_max >= 1");
        assert!(q_max >= 1, "q_max >= 1");
        Self {
            id,
            n,
            m_max,
            q_max,
            ck: 0,
            ld: vec![-1; n],
            lpr: vec![-1; n],
            lms: vec![-1; n],
            u: (0..n).map(|_| VecDeque::new()).collect(),
            q: (0..n).map(|_| VecDeque::new()).collect(),
            delivered: Vec::new(),
            events: Vec::new(),
            drop_control: false,
        }
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn n(&self) -> usize {
        self.n
    }

    pub fn m_max(&self) -> usize {
        self.m_max
    }

    pub fn q_max(&self) -> usize {
        self.q_max
    }

    /// Withhold ACK and PERMIT. Incoming MSG is still delivered.
    pub fn set_drop_control(&mut self, drop: bool) {
        self.drop_control = drop;
    }

    pub fn delivered(&self) -> &[Vec<u8>] {
        &self.delivered
    }

    pub fn u_len(&self, dst: usize) -> usize {
        self.u[dst].len()
    }

    pub fn q_len(&self, dst: usize) -> usize {
        self.q[dst].len()
    }

    pub fn lpr(&self) -> &[i64] {
        &self.lpr
    }

    pub fn ld(&self) -> &[i64] {
        &self.ld
    }

    pub fn lms(&self) -> &[i64] {
        &self.lms
    }

    pub fn events(&self) -> &[Event] {
        &self.events
    }

    /// Payloads in `U[dst]`, head first. Local state, not a wire image.
    pub fn u_payloads(&self, dst: usize) -> Vec<Vec<u8>> {
        self.u[dst].iter().map(|m| m.payload.clone()).collect()
    }

    /// Algorithm 2 `causalSend`. Does not send to self.
    ///
    /// `req_per` is evaluated after a possible eviction and before the new record is enqueued.
    /// A nonempty `U[x]` counts even if those records are already sent.
    pub fn causal_send(&mut self, dst: usize, payload: impl AsRef<[u8]>) -> Vec<Emission> {
        assert!(dst < self.n, "dst in range");
        assert!(dst != self.id, "do not send to self");
        let mut out = Vec::new();
        self.evict_if_full(dst, &mut out);
        let req_per = (0..self.n).any(|x| x != dst && !self.u[x].is_empty());
        let mid = self.ck;
        self.ck += 1;
        let rec = Record {
            mid,
            per: req_per,
            payload: payload.as_ref().to_vec(),
            sent: false,
            dv: self.ld.clone(),
            lms: self.lms.clone(),
        };
        self.u[dst].push_back(rec);
        self.lms[dst] = as_i64(mid);
        self.send_messages(&mut out);
        out
    }

    pub fn receive(&mut self, src: usize, wire: WireMsg) -> Vec<Emission> {
        assert!(src < self.n && src != self.id, "src must be a peer");
        self.events.push(Event::Recv {
            src,
            msg: wire.clone(),
        });
        let mut out = Vec::new();
        match wire {
            WireMsg::Msg { mid, per, payload } => self.recv_msg(src, mid, per, payload, &mut out),
            WireMsg::Ack { mid } => self.recv_ack(src, mid, &mut out),
            WireMsg::Permit { mid } => self.recv_permit(src, mid, &mut out),
        }
        out
    }

    fn evict_if_full(&mut self, dst: usize, out: &mut Vec<Emission>) {
        if self.u[dst].len() < self.m_max {
            return;
        }
        let mut stale = self.u[dst].pop_front().expect("U at cap");
        if !stale.sent {
            stale.sent = true;
            self.emit(
                dst,
                WireMsg::Msg {
                    mid: stale.mid,
                    per: stale.per,
                    payload: stale.payload.clone(),
                },
                out,
            );
            for x in 0..self.n {
                self.lpr[x] = self.lpr[x].max(stale.dv[x]);
            }
        }
        if stale.per {
            if self.q[dst].len() >= self.q_max {
                let m_prime = self.q[dst].pop_front().expect("Q at cap");
                self.emit(dst, WireMsg::Permit { mid: m_prime.mid }, out);
            }
            self.q[dst].push_back(stale);
        }
        self.send_permits(out);
    }

    fn send_messages(&mut self, out: &mut Vec<Emission>) {
        let n = self.n;
        let me = self.id;
        for dst in 0..n {
            let mut k = 0;
            while k < self.u[dst].len() {
                if self.u[dst][k].sent {
                    k += 1;
                    continue;
                }
                let blocked =
                    (0..n).any(|j| j != me && j != dst && self.u[dst][k].dv[j] > self.lpr[j]);
                if blocked {
                    break;
                }
                let mid = self.u[dst][k].mid;
                let per = self.u[dst][k].per;
                let payload = self.u[dst][k].payload.clone();
                self.u[dst][k].sent = true;
                self.emit(dst, WireMsg::Msg { mid, per, payload }, out);
                k += 1;
            }
        }
    }

    fn send_permits(&mut self, out: &mut Vec<Emission>) {
        let n = self.n;
        let me = self.id;
        for dst in 0..n {
            loop {
                let Some((mid, lms)) = self.q[dst]
                    .front()
                    .map(|front| (front.mid, front.lms.clone()))
                else {
                    break;
                };
                let releasable = (0..n).all(|j| {
                    j == me
                        || j == dst
                        || self.u[j].is_empty()
                        || clock_lt_mid(lms[j], self.u[j][0].mid)
                });
                if !releasable {
                    break;
                }
                self.q[dst].pop_front();
                self.emit(dst, WireMsg::Permit { mid }, out);
            }
        }
    }

    fn recv_msg(
        &mut self,
        src: usize,
        mid: u64,
        per: bool,
        payload: Vec<u8>,
        out: &mut Vec<Emission>,
    ) {
        // Gaps are allowed. The clock is shared across destinations, so one peer's
        // mids increase but are not consecutive.
        if mid_leq_clock(mid, self.ld[src]) {
            return;
        }
        if !per {
            self.lpr[src] = as_i64(mid);
        }
        self.ld[src] = as_i64(mid);
        self.emit(src, WireMsg::Ack { mid }, out);
        self.delivered.push(payload);
    }

    fn recv_ack(&mut self, src: usize, mid: u64, out: &mut Vec<Emission>) {
        // Paper leaves U empty undefined. Treat it as already evicted or already acked.
        if self.u[src].is_empty() {
            return;
        }
        if mid < self.u[src][0].mid {
            return;
        }
        let last = self.u[src].back().expect("nonempty").mid;
        if mid > last {
            return;
        }
        while self.u[src].front().is_some_and(|m| m.mid <= mid) {
            let m = self.u[src].pop_front().expect("front");
            if m.sent && m.per {
                if self.q[src].len() >= self.q_max {
                    let m_prime = self.q[src].pop_front().expect("Q at cap");
                    self.emit(src, WireMsg::Permit { mid: m_prime.mid }, out);
                }
                self.q[src].push_back(m);
            }
        }
        self.send_permits(out);
    }

    fn recv_permit(&mut self, src: usize, mid: u64, out: &mut Vec<Emission>) {
        let mid_i = as_i64(mid);
        if self.ld[src] >= mid_i && mid_i > self.lpr[src] {
            self.lpr[src] = mid_i;
            self.send_messages(out);
        }
    }

    fn emit(&mut self, dst: usize, msg: WireMsg, out: &mut Vec<Emission>) {
        if self.drop_control && matches!(msg, WireMsg::Ack { .. } | WireMsg::Permit { .. }) {
            return;
        }
        self.events.push(Event::Sent {
            dst,
            msg: msg.clone(),
        });
        out.push(Emission { dst, msg });
    }
}

impl fmt::Debug for Process {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Process")
            .field("id", &self.id)
            .field("ck", &self.ck)
            .field("ld", &self.ld)
            .field("lpr", &self.lpr)
            .field("lms", &self.lms)
            .field(
                "u_len",
                &self.u.iter().map(VecDeque::len).collect::<Vec<_>>(),
            )
            .field(
                "q_len",
                &self.q.iter().map(VecDeque::len).collect::<Vec<_>>(),
            )
            .field("delivered", &self.delivered.len())
            .finish()
    }
}

fn as_i64(mid: u64) -> i64 {
    i64::try_from(mid).expect("message id fits in i64")
}

fn mid_leq_clock(mid: u64, clock: i64) -> bool {
    match i64::try_from(mid) {
        Ok(m) => m <= clock,
        Err(_) => false,
    }
}

fn clock_lt_mid(clock: i64, mid: u64) -> bool {
    match i64::try_from(mid) {
        Ok(m) => clock < m,
        Err(_) => true,
    }
}
