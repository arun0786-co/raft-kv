//! Raft driven by hand: messages are routed explicitly, so each scenario is exact.

use raftkv::kv::{Kind, Op};
use raftkv::raft::{Cmd, Config, Msg, Node, NodeId, Role};

fn cluster(n: usize) -> Vec<Node> {
    (0..n).map(|i| Node::new(i, n, Config::default(), 100 + i as u64)).collect()
}

/// Deliver every queued message among `alive` nodes until nothing is left. Messages to others are dropped.
fn settle(nodes: &mut [Node], alive: &[NodeId]) {
    loop {
        let mut queue: Vec<(NodeId, NodeId, Msg)> = Vec::new();
        for &i in alive {
            for (to, m) in nodes[i].take_outbox() {
                queue.push((i, to, m));
            }
        }
        if queue.is_empty() {
            return;
        }
        for (from, to, m) in queue {
            if alive.contains(&to) {
                nodes[to].step(from, m);
            }
        }
    }
}

fn tick_until_leader(nodes: &mut [Node], alive: &[NodeId]) -> NodeId {
    for _ in 0..1000 {
        for &i in alive {
            nodes[i].tick();
        }
        settle(nodes, alive);
        let leaders: Vec<NodeId> = alive.iter().copied().filter(|&i| nodes[i].role == Role::Leader).collect();
        if let Some(&l) = leaders.iter().max_by_key(|&&l| nodes[l].term) {
            return l;
        }
    }
    panic!("no leader");
}

fn op(seq: u64) -> Cmd {
    Cmd::Op(Op { client: 0, seq, kind: Kind::Put(1, seq as u32) })
}

#[test]
fn a_single_node_elects_itself_and_commits() {
    let mut n = Node::new(0, 1, Config::default(), 1);
    for _ in 0..40 {
        n.tick();
    }
    assert_eq!(n.role, Role::Leader);
    let idx = n.propose(op(1)).unwrap();
    assert_eq!(n.commit, idx);
}

#[test]
fn three_nodes_elect_one_leader_and_replicate() {
    let mut nodes = cluster(3);
    let all = [0, 1, 2];
    let l = tick_until_leader(&mut nodes, &all);
    assert_eq!(all.iter().filter(|&&i| nodes[i].role == Role::Leader).count(), 1);
    let idx = (1..=10).map(|s| nodes[l].propose(op(s)).unwrap()).last().unwrap();
    settle(&mut nodes, &all);
    nodes[l].tick();
    for _ in 0..4 {
        nodes[l].tick();
    }
    settle(&mut nodes, &all);
    for n in &nodes {
        assert_eq!(n.log, nodes[l].log);
        assert_eq!(n.commit, idx, "node {} commit", n.id);
    }
}

#[test]
fn a_leader_cut_off_from_the_majority_cannot_commit() {
    let mut nodes = cluster(5);
    let all = [0, 1, 2, 3, 4];
    let l = tick_until_leader(&mut nodes, &all);
    let before = nodes[l].commit;
    let other = (l + 1) % 5;
    nodes[l].propose(op(1));
    settle(&mut nodes, &[l, other]); // only one follower hears it
    for _ in 0..10 {
        nodes[l].tick();
        settle(&mut nodes, &[l, other]);
    }
    assert_eq!(nodes[l].commit, before);
    // the other three elect a new leader of a higher term, and the old leader steps down on contact
    let rest: Vec<NodeId> = all.iter().copied().filter(|&i| i != l && i != other).collect();
    let new = tick_until_leader(&mut nodes, &rest);
    assert!(nodes[new].term > nodes[l].term);
    for _ in 0..10 {
        for &i in &all {
            nodes[i].tick();
        }
        settle(&mut nodes, &all);
    }
    assert_eq!(all.iter().filter(|&&i| nodes[i].role == Role::Leader).count(), 1);
}

#[test]
fn votes_require_an_up_to_date_log() {
    let mut nodes = cluster(3);
    let all = [0, 1, 2];
    let l = tick_until_leader(&mut nodes, &all);
    nodes[l].propose(op(1));
    settle(&mut nodes, &all);
    // a node whose log is behind asks for a vote in a higher term: refused
    let mut stale = Node::new(9, 3, Config::default(), 5);
    stale.term = nodes[l].term + 5;
    nodes[(l + 1) % 3].step(9, Msg::Vote { term: stale.term, last_index: 0, last_term: 0 });
    let out = nodes[(l + 1) % 3].take_outbox();
    assert!(out.iter().any(|(_, m)| matches!(m, Msg::VoteResp { granted: false, .. })));
}

#[test]
fn a_stale_append_does_not_truncate_or_lower_commit() {
    let mut nodes = cluster(3);
    let all = [0, 1, 2];
    let l = tick_until_leader(&mut nodes, &all);
    for s in 1..=5 {
        nodes[l].propose(op(s));
    }
    settle(&mut nodes, &all);
    for _ in 0..5 {
        nodes[l].tick();
    }
    settle(&mut nodes, &all);
    let f = (l + 1) % 3;
    let (len, commit) = (nodes[f].log.len(), nodes[f].commit);
    // an old heartbeat that carries only the first entry arrives late
    let (term, first) = (nodes[l].term, nodes[l].log[1].clone());
    nodes[f].step(l, Msg::Append { term, prev_index: 0, prev_term: 0, entries: vec![first], commit });
    assert_eq!(nodes[f].log.len(), len);
    assert_eq!(nodes[f].commit, commit);
}
