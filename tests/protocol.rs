use crcd_unicast::{Emission, Event, Process, Simulator, WireMsg};

fn utf8(p: &Process) -> Vec<String> {
    p.delivered()
        .iter()
        .map(|b| String::from_utf8(b.clone()).expect("utf8"))
        .collect()
}

fn has(p: &Process, payload: &[u8]) -> bool {
    p.delivered().iter().any(|d| d == payload)
}

fn msg_of(emissions: &[Emission]) -> Vec<(usize, u64, bool, Vec<u8>)> {
    emissions
        .iter()
        .filter_map(|e| match &e.msg {
            WireMsg::Msg { mid, per, payload } => Some((e.dst, *mid, *per, payload.clone())),
            _ => None,
        })
        .collect()
}

fn permits(emissions: &[Emission]) -> Vec<(usize, u64)> {
    emissions
        .iter()
        .filter_map(|e| match e.msg {
            WireMsg::Permit { mid } => Some((e.dst, mid)),
            _ => None,
        })
        .collect()
}

fn deliver_msgs(from: usize, emissions: Vec<Emission>, dst_proc: &mut Process) -> Vec<Emission> {
    let mut acks = Vec::new();
    for em in emissions {
        if em.dst == dst_proc.id() {
            if let WireMsg::Msg { .. } = &em.msg {
                acks.extend(dst_proc.receive(from, em.msg));
            }
        }
    }
    acks
}

fn observe_caps(sim: &Simulator, max_u: &mut usize, max_q: &mut usize) {
    for i in 0..sim.n() {
        for d in 0..sim.n() {
            *max_u = (*max_u).max(sim.process(i).u_len(d));
            *max_q = (*max_q).max(sim.process(i).q_len(d));
        }
    }
}

#[test]
fn wire_msg_len_independent_of_n() {
    fn first_msg(n: usize) -> WireMsg {
        let mut p = Process::new(0, n, 8, 8);
        let out = p.causal_send(1, b"payload");
        let msg = out
            .into_iter()
            .find_map(|e| match e.msg {
                WireMsg::Msg { .. } => Some(e.msg),
                _ => None,
            })
            .expect("msg");
        match &msg {
            WireMsg::Msg { mid, per, payload } => {
                assert_eq!(*mid, 0);
                assert!(!*per);
                assert_eq!(payload, b"payload");
            }
            WireMsg::Ack { .. } | WireMsg::Permit { .. } => panic!("not a msg"),
        }
        msg
    }

    let msg4 = first_msg(4);
    let msg32 = first_msg(32);
    let len4 = bincode::serialize(&msg4).unwrap().len();
    let len32 = bincode::serialize(&msg32).unwrap().len();
    assert_eq!(len4, len32, "bincode Msg length must not depend on n");
    assert_eq!(msg4.wire_len(), msg32.wire_len());
    assert_eq!(msg4.wire_len(), 8 + 1 + b"payload".len());
    assert!(
        len32 < 64,
        "encoding grew enough to hide a dependency vector"
    );

    let value = serde_json::to_value(&msg4).unwrap();
    let obj = value["Msg"].as_object().expect("Msg object");
    let mut keys: Vec<_> = obj.keys().cloned().collect();
    keys.sort();
    assert_eq!(
        keys,
        vec!["mid".to_string(), "payload".to_string(), "per".to_string()]
    );
    assert!(obj.get("dv").is_none());
    assert!(obj.get("DV").is_none());
    assert!(obj.get("lms").is_none());
    assert!(obj.get("LMS").is_none());
}

#[test]
fn fifo_five_payloads_no_self_delivery() {
    let mut sim = Simulator::new(2, 16, 16);
    for i in 0..5 {
        sim.causal_send(0, 1, format!("p{i}"));
    }
    sim.drain();
    assert_eq!(utf8(sim.process(1)), vec!["p0", "p1", "p2", "p3", "p4"]);
    assert!(sim.process(0).delivered().is_empty(), "no self-delivery");
}

#[test]
fn sps_chain_permit_before_dependent_send() {
    let mut sim = Simulator::new(3, 64, 64);
    sim.causal_send(0, 2, b"m0");
    let m1 = sim.causal_send(0, 1, b"m1");
    let m1s = msg_of(&m1);
    assert_eq!(m1s.len(), 1);
    assert!(m1s[0].2, "m1.per must be true while m0 is unacked");
    let m1_mid = m1s[0].1;

    let mut steps = 0;
    while !has(sim.process(1), b"m1") {
        assert!(sim.step(), "idle before m1 delivered");
        steps += 1;
        assert!(steps < 1000);
    }

    let m2_now = sim.causal_send(1, 2, b"m2");
    assert!(
        msg_of(&m2_now).is_empty(),
        "m2 must stay buffered until a covering permit, got {m2_now:?}"
    );
    sim.drain();

    assert_eq!(
        utf8(sim.process(2)),
        vec!["m0".to_string(), "m2".to_string()]
    );

    let events = sim.process(1).events();
    let m2_at = events
        .iter()
        .position(|e| {
            matches!(e, Event::Sent { msg: WireMsg::Msg { payload, .. }, .. } if payload == b"m2")
        })
        .expect("m2 network-sent");
    let permit_at = events
        .iter()
        .position(
            |e| matches!(e, Event::Recv { src: 0, msg: WireMsg::Permit { mid } } if *mid >= m1_mid),
        )
        .expect("permit from 0 covering m1");
    assert!(
        permit_at < m2_at,
        "Msg m2 appears before Permit from 0: {events:?}"
    );
}

#[test]
fn fast_path_no_separate_permit() {
    let mut sim = Simulator::new(3, 64, 64);
    sim.causal_send(0, 2, b"m0");
    sim.drain();
    assert_eq!(utf8(sim.process(2)), vec!["m0".to_string()]);
    assert_eq!(sim.process(0).u_len(2), 0);

    let m1 = sim.causal_send(0, 1, b"m1");
    let m1s = msg_of(&m1);
    assert_eq!(m1s.len(), 1);
    assert!(!m1s[0].2, "m1.per is false once m0 is acked");
    let m1_mid = m1s[0].1;
    sim.drain();
    assert_eq!(sim.process(1).lpr()[0], m1_mid as i64);
    assert!(
        sim.process(1).events().iter().all(|e| !matches!(
            e,
            Event::Recv {
                msg: WireMsg::Permit { .. },
                ..
            }
        )),
        "fast path must not need a separate permit"
    );

    let m2 = sim.causal_send(1, 2, b"m2");
    assert!(
        msg_of(&m2).iter().any(|(_, _, _, p)| p == b"m2"),
        "m2 network-sent without waiting for a permit"
    );
    sim.drain();
    assert_eq!(
        utf8(sim.process(2)),
        vec!["m0".to_string(), "m2".to_string()]
    );
}

#[test]
fn permit_held_until_other_channel_acked() {
    let mut p0 = Process::new(0, 3, 8, 8);
    let mut p1 = Process::new(1, 3, 8, 8);
    let mut p2 = Process::new(2, 3, 8, 8);

    let m0 = p0.causal_send(2, b"m0");
    let m0s = msg_of(&m0);
    assert_eq!(m0s.len(), 1);
    assert!(!m0s[0].2);
    let m0_mid = m0s[0].1;

    let m1 = p0.causal_send(1, b"m1");
    let m1s = msg_of(&m1);
    assert_eq!(m1s.len(), 1);
    assert!(m1s[0].2, "m1.per");
    let m1_mid = m1s[0].1;
    assert_eq!(p0.lms()[2], m0_mid as i64);

    let ack1 = deliver_msgs(0, m1, &mut p1);
    assert!(has(&p1, b"m1"));
    assert_eq!(ack1.len(), 1);

    let after_ack1 = p0.receive(1, ack1[0].msg.clone());
    assert!(
        permits(&after_ack1).is_empty(),
        "no Permit while m0 heads U[2]"
    );
    assert_eq!(p0.u_len(1), 0);
    assert_eq!(p0.q_len(1), 1);
    assert_eq!(p0.u_len(2), 1);
    assert_eq!(p0.u_payloads(2), vec![b"m0".to_vec()]);

    let ack0 = deliver_msgs(0, m0, &mut p2);
    assert!(has(&p2, b"m0"));
    let after_ack0 = p0.receive(2, ack0[0].msg.clone());
    assert_eq!(permits(&after_ack0), vec![(1, m1_mid)]);
    assert_eq!(p0.u_len(2), 0);
    assert_eq!(p0.q_len(1), 0);
}

#[test]
fn byzantine_ack_withhold_is_isolated() {
    let mut p0 = Process::new(0, 3, 2, 2);
    let mut p1 = Process::new(1, 3, 2, 2);
    let mut p2 = Process::new(2, 3, 2, 2);
    p2.set_drop_control(true);

    let mut evicted = Vec::new();
    for i in 0..4 {
        let payload = format!("e{i}");
        let before = p0.u_payloads(2);
        let out = p0.causal_send(2, payload.as_bytes());
        assert!(p0.u_len(2) <= 2, "U[2] exceeded M_max");
        if before.len() >= 2 {
            evicted.push(before[0].clone());
        }
        let acks = deliver_msgs(0, out, &mut p2);
        assert!(acks.is_empty(), "process 2 must not ACK");
    }
    assert_eq!(evicted.len(), 2, "eviction must fire");
    assert!(p0.u_len(2) <= 2);
    for payload in &evicted {
        assert!(
            p2.delivered().iter().any(|d| d == payload),
            "evicted payload missing from process 2: {payload:?}"
        );
    }

    let to1 = p0.causal_send(1, b"for-1");
    assert!(p0.u_len(2) <= 2);
    let acks = deliver_msgs(0, to1, &mut p1);
    assert!(has(&p1, b"for-1"));
    let _ = acks;
}

#[test]
fn replay_delivers_once_and_acks_once() {
    let mut sender = Process::new(0, 2, 4, 4);
    let mut recv = Process::new(1, 2, 4, 4);
    let out = sender.causal_send(1, b"once");
    let msg = out
        .into_iter()
        .find(|e| matches!(e.msg, WireMsg::Msg { .. }))
        .expect("msg")
        .msg;
    let before = recv.delivered().len();
    let first = recv.receive(0, msg.clone());
    assert_eq!(recv.delivered().len(), before + 1);
    assert_eq!(first.len(), 1);
    assert!(matches!(first[0].msg, WireMsg::Ack { .. }));
    let second = recv.receive(0, msg);
    assert_eq!(recv.delivered().len(), before + 1);
    assert!(second.is_empty(), "replay emits no ACK");
}

#[test]
fn high_and_low_ack_do_not_dequeue() {
    let mut p = Process::new(0, 3, 8, 8);
    // mid 0 goes to process 2, so the queue toward 1 starts at mid 1.
    let _ = p.causal_send(2, b"other");
    let to1 = p.causal_send(1, b"a");
    let _ = p.causal_send(1, b"b");
    assert_eq!(p.u_len(1), 2);
    let head = msg_of(&to1)[0].1;
    assert!(head > 0);

    let low = p.receive(1, WireMsg::Ack { mid: head - 1 });
    assert!(low.is_empty());
    assert_eq!(p.u_len(1), 2);

    let high = p.receive(1, WireMsg::Ack { mid: head + 100 });
    assert!(high.is_empty());
    assert_eq!(p.u_len(1), 2);

    let empty = Process::new(0, 2, 4, 4);
    let mut empty = empty;
    let none = empty.receive(1, WireMsg::Ack { mid: 0 });
    assert!(none.is_empty());
    assert_eq!(empty.u_len(1), 0);
}

#[test]
fn stale_or_future_permit_does_not_move_lpr() {
    let mut p = Process::new(0, 2, 4, 4);
    // per=true so delivery advances LD and leaves LPR alone.
    let forged = WireMsg::Msg {
        mid: 5,
        per: true,
        payload: b"dep".to_vec(),
    };
    let _ = p.receive(1, forged);
    assert_eq!(p.ld()[1], 5);
    assert_eq!(p.lpr()[1], -1);

    let future = p.receive(1, WireMsg::Permit { mid: 6 });
    assert!(future.is_empty());
    assert_eq!(p.lpr()[1], -1, "permit mid > LD must not change LPR");

    let _ = p.receive(
        1,
        WireMsg::Msg {
            mid: 7,
            per: false,
            payload: b"fast".to_vec(),
        },
    );
    assert_eq!(p.lpr()[1], 7);
    let stale = p.receive(1, WireMsg::Permit { mid: 7 });
    assert!(stale.is_empty());
    assert_eq!(p.lpr()[1], 7, "permit mid <= LPR must not change LPR");
    let older = p.receive(1, WireMsg::Permit { mid: 5 });
    assert!(older.is_empty());
    assert_eq!(p.lpr()[1], 7);
}

#[test]
fn q_cascading_eviction_emits_proactive_permit() {
    let mut p0 = Process::new(0, 3, 8, 1);
    let mut p1 = Process::new(1, 3, 8, 1);

    let anchor = p0.causal_send(2, b"anchor");
    assert!(!msg_of(&anchor)[0].2);

    let first = p0.causal_send(1, b"p0");
    let first_mid = msg_of(&first)[0].1;
    assert!(msg_of(&first)[0].2);
    let ack = deliver_msgs(0, first, &mut p1);
    let moved = p0.receive(1, ack[0].msg.clone());
    assert!(permits(&moved).is_empty(), "anchor still blocks the permit");
    assert_eq!(p0.q_len(1), 1);

    let second = p0.causal_send(1, b"p1");
    assert!(msg_of(&second)[0].2);
    let ack2 = deliver_msgs(0, second, &mut p1);
    let cascaded = p0.receive(1, ack2[0].msg.clone());
    assert_eq!(
        permits(&cascaded),
        vec![(1, first_mid)],
        "proactive permit must be the evicted Q head"
    );
    assert_eq!(p0.q_len(1), 1);
    assert_eq!(p0.u_len(2), 1);
}

#[test]
fn optimistic_eviction_updates_lpr_and_force_sends() {
    let mut p0 = Process::new(0, 3, 1, 4);
    let mut p1 = Process::new(1, 3, 8, 4);

    // p1 keeps U[2] nonempty so the message to p0 requires a permit.
    let _filler = p1.causal_send(2, b"hold");
    let dep = p1.causal_send(0, b"dep");
    let dep_msg = msg_of(&dep);
    assert!(dep_msg[0].2, "dependency message must require a permit");
    let dep_mid = dep_msg[0].1;
    let _ = p0.receive(1, dep.into_iter().next().unwrap().msg);
    assert_eq!(p0.ld()[1], dep_mid as i64);
    assert_eq!(p0.lpr()[1], -1);

    let blocked = p0.causal_send(2, b"blocked");
    assert!(msg_of(&blocked).is_empty(), "head must sit unsent");
    assert_eq!(p0.u_len(2), 1);
    assert_eq!(p0.u_payloads(2), vec![b"blocked".to_vec()]);
    assert_eq!(p0.lpr()[1], -1);

    let forced = p0.causal_send(2, b"next");
    assert!(
        forced.iter().any(|e| {
            e.dst == 2 && matches!(&e.msg, WireMsg::Msg { payload, .. } if payload == b"blocked")
        }),
        "force-sent the unsent head, got {forced:?}"
    );
    assert_eq!(p0.lpr()[1], dep_mid as i64, "LPR advanced to stale DV");
    assert_eq!(p0.u_len(2), 1);
    assert_eq!(p0.u_payloads(2), vec![b"next".to_vec()]);
}

#[test]
fn causal_diamond_and_congestion_liveness() {
    let mut sim = Simulator::new(3, 64, 64);
    sim.causal_send(0, 2, b"a");
    sim.causal_send(0, 1, b"b");
    let mut steps = 0;
    while !has(sim.process(1), b"b") {
        assert!(sim.step(), "idle before b");
        steps += 1;
        assert!(steps < 1000);
    }
    sim.causal_send(1, 2, b"c");
    sim.drain();
    let log = utf8(sim.process(2));
    let a_at = log.iter().position(|s| s == "a").expect("a delivered");
    let c_at = log.iter().position(|s| s == "c").expect("c delivered");
    assert!(
        a_at < c_at,
        "honest timely path must deliver a before c, got {log:?}"
    );

    // Congestion: process 2 withholds the ACK of a. Eviction may break order.
    // An honest third party still receives.
    let mut p0 = Process::new(0, 3, 1, 4);
    let mut p1 = Process::new(1, 3, 8, 4);
    let first = p0.causal_send(2, b"a");
    assert_eq!(msg_of(&first).len(), 1);
    assert_eq!(p0.u_len(2), 1);
    let next = p0.causal_send(2, b"a2");
    assert_eq!(p0.u_len(2), 1, "eviction keeps U within M_max");
    assert_eq!(p0.u_payloads(2), vec![b"a2".to_vec()]);
    assert!(
        msg_of(&next).iter().any(|(_, _, _, p)| p == b"a2"),
        "evicted channel still accepts a later send"
    );
    let live = p0.causal_send(1, b"live");
    assert!(
        msg_of(&live).iter().any(|(_, _, _, p)| p == b"live"),
        "withhold on channel 2 must not block channel 1"
    );
    let _ = deliver_msgs(0, live, &mut p1);
    assert!(has(&p1, b"live"));
}

#[test]
fn acked_exchange_drains_queues_within_caps() {
    let m_max = 4;
    let q_max = 4;
    let mut sim = Simulator::new(3, m_max, q_max);
    let mut max_u = 0;
    let mut max_q = 0;

    sim.causal_send(0, 2, b"a");
    observe_caps(&sim, &mut max_u, &mut max_q);
    sim.causal_send(0, 1, b"b");
    observe_caps(&sim, &mut max_u, &mut max_q);

    let mut steps = 0;
    while sim.step() {
        observe_caps(&sim, &mut max_u, &mut max_q);
        steps += 1;
        assert!(steps < 10_000);
    }
    assert_eq!(sim.pending(), 0);
    assert_eq!(sim.process(0).u_len(1), 0);
    assert_eq!(sim.process(0).u_len(2), 0);
    assert_eq!(sim.process(0).q_len(1), 0);
    assert_eq!(sim.process(0).q_len(2), 0);
    assert!(max_u <= m_max, "U exceeded cap: {max_u}");
    assert!(max_q <= q_max, "Q exceeded cap: {max_q}");
    assert!(has(sim.process(1), b"b"));
    assert!(has(sim.process(2), b"a"));
}
