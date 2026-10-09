use std::collections::VecDeque;

use crate::process::Process;
use crate::wire::{Emission, WireMsg};

/// Deterministic in-process FIFO network. One queue per (src, dst) pair.
///
/// A step delivers one queued wire message. `drain` runs until every queue is empty,
/// or until `step_cap` deliveries, so a bug cannot loop forever.
pub struct Simulator {
    procs: Vec<Process>,
    inflight: Vec<Vec<VecDeque<WireMsg>>>,
    step_cap: usize,
}

impl Simulator {
    pub fn new(n: usize, m_max: usize, q_max: usize) -> Self {
        Self::with_step_cap(n, m_max, q_max, 100_000)
    }

    pub fn with_step_cap(n: usize, m_max: usize, q_max: usize, step_cap: usize) -> Self {
        assert!(n >= 1, "n >= 1");
        assert!(step_cap >= 1, "step_cap >= 1");
        let procs = (0..n).map(|id| Process::new(id, n, m_max, q_max)).collect();
        let inflight = (0..n)
            .map(|_| (0..n).map(|_| VecDeque::new()).collect())
            .collect();
        Self {
            procs,
            inflight,
            step_cap,
        }
    }

    pub fn n(&self) -> usize {
        self.procs.len()
    }

    pub fn process(&self, id: usize) -> &Process {
        &self.procs[id]
    }

    pub fn process_mut(&mut self, id: usize) -> &mut Process {
        &mut self.procs[id]
    }

    pub fn pending(&self) -> usize {
        self.inflight.iter().flatten().map(VecDeque::len).sum()
    }

    pub fn causal_send(
        &mut self,
        src: usize,
        dst: usize,
        payload: impl AsRef<[u8]>,
    ) -> Vec<Emission> {
        let out = self.procs[src].causal_send(dst, payload);
        self.enqueue(src, &out);
        out
    }

    /// Deliver the next wire message in (src, dst) order. Returns false if idle.
    pub fn step(&mut self) -> bool {
        let n = self.procs.len();
        let mut found = None;
        for src in 0..n {
            for dst in 0..n {
                if !self.inflight[src][dst].is_empty() {
                    found = Some((src, dst));
                    break;
                }
            }
            if found.is_some() {
                break;
            }
        }
        let Some((src, dst)) = found else {
            return false;
        };
        let msg = self.inflight[src][dst].pop_front().expect("nonempty");
        let replies = self.procs[dst].receive(src, msg);
        self.enqueue(dst, &replies);
        true
    }

    /// Deliver until quiescence. Panics if `step_cap` is hit.
    pub fn drain(&mut self) -> usize {
        let mut steps = 0;
        while self.step() {
            steps += 1;
            if steps > self.step_cap {
                panic!("simulator step cap {} exceeded", self.step_cap);
            }
        }
        steps
    }

    fn enqueue(&mut self, src: usize, emissions: &[Emission]) {
        for em in emissions {
            assert!(
                em.dst < self.procs.len() && em.dst != src,
                "bad emission dst"
            );
            self.inflight[src][em.dst].push_back(em.msg.clone());
        }
    }
}
