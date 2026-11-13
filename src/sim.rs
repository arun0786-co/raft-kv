//! Deterministic simulation of a Raft key-value cluster under faults.
//!
//! One thread, one random seed. Each tick: deliver the messages due, tick every live node,
//! let clients issue and retry requests, then maybe crash a node, restart one, or change the
//! network partition. Messages are delayed, dropped and duplicated at random. A crash keeps
//! only the node's persistent state (term, vote, log); its key-value state is rebuilt by
//! re-applying committed entries after the restart.
//!
//! Safety is checked as the run goes:
//!   election safety     at most one leader in any term
//!   log matching        if two logs have an entry with the same index and term, they agree up to it
//!   leader completeness a new leader holds every entry already committed
//!   state machine       every node applies the same command at the same index
//!   commit monotonic    a live node's commit index never goes down
//! and at the end the client history is checked for linearizability.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap};
use std::hash::{DefaultHasher, Hash, Hasher};

use crate::kv::{Kind, Kv, Op, Reply};
use crate::linz::{self, HistOp};
use crate::raft::{Cmd, Config, Entry, Index, Msg, Node, NodeId, Persistent, Role, Term};
use crate::rng::Rng;

#[derive(Clone, Debug)]
pub struct SimConfig {
    pub nodes: usize,
    pub clients: usize,
    pub keys: u8,
    pub ticks: u64,            // length of the faulty phase
    pub settle_ticks: u64,     // healed phase at the end, to let requests finish
    pub min_delay: u64,
    pub max_delay: u64,
    pub drop: f64,
    pub dup: f64,
    pub partition_rate: f64,   // chance per tick that the partition changes
    pub crash_rate: f64,       // chance per tick that a live node crashes
    pub restart_min: u64,
    pub restart_max: u64,
    pub client_timeout: u64,
    pub raft: Config,
    pub stale_reads: bool,     // deliberate bug: the leader answers reads from its own state
}

impl SimConfig {
    /// Harsher faults: nodes restart within a few ticks (so a node can come back inside the election
    /// it was voting in), more crashes and partitions, and one entry per append message (so an old
    /// entry can reach a majority without the new leader's first entry travelling with it).
    pub fn aggressive() -> SimConfig {
        let mut c = SimConfig { crash_rate: 0.01, partition_rate: 0.01, restart_min: 1, restart_max: 30, ..SimConfig::default() };
        c.raft.max_batch = 1;
        c
    }
}

impl Default for SimConfig {
    fn default() -> Self {
        SimConfig {
            nodes: 5, clients: 5, keys: 4, ticks: 3000, settle_ticks: 2000, min_delay: 1, max_delay: 6, drop: 0.05, dup: 0.02,
            partition_rate: 0.004, crash_rate: 0.003, restart_min: 20, restart_max: 200, client_timeout: 60,
            raft: Config::default(), stale_reads: false,
        }
    }
}

enum Ev {
    Raft { from: NodeId, to: NodeId, msg: Msg },
    Req { client: usize, node: NodeId, op: Op },
    Resp { client: usize, seq: u64, reply: Result<Reply, Option<NodeId>> },
}

struct Server {
    node: Option<Node>,
    persisted: Persistent,
    kv: Kv,
    pending: HashMap<Index, (usize, u64)>,
    last_commit: Index,
    was_leader: bool,
    restart_at: u64,
}

struct Client {
    seq: u64,
    target: NodeId,
    current: Option<(Op, u64, usize)>, // op, deadline, index in the history
}

#[derive(Debug, Default)]
pub struct Report {
    pub violations: Vec<String>,
    pub completed: usize,
    pub unanswered: usize,
    pub leaders_elected: u64,
    pub crashes: u64,
    pub partitions: u64,
    pub messages: u64,
    pub committed: Index,
    pub longest_without_leader: u64,
    pub latencies: Vec<u64>,
    pub linearizable: bool,
    pub trace: u64,
    pub history: Vec<HistOp>,
}

pub struct Sim {
    cfg: SimConfig,
    rng: Rng,
    now: u64,
    next_id: u64,
    queue: BinaryHeap<Reverse<(u64, u64)>>,
    events: HashMap<u64, Ev>,
    servers: Vec<Server>,
    group: Vec<u8>,
    clients: Vec<Client>,
    applied: BTreeMap<Index, Entry>,
    leader_of_term: HashMap<Term, NodeId>,
    report: Report,
    trace: DefaultHasher,
    seed: u64,
}

impl Sim {
    pub fn new(cfg: SimConfig, seed: u64) -> Sim {
        let servers = (0..cfg.nodes)
            .map(|i| {
                let node = Node::new(i, cfg.nodes, cfg.raft.clone(), seed.wrapping_mul(1000).wrapping_add(i as u64));
                Server { persisted: node.persistent(), node: Some(node), kv: Kv::default(), pending: HashMap::new(), last_commit: 0,
                         was_leader: false, restart_at: 0 }
            })
            .collect();
        let clients = (0..cfg.clients).map(|c| Client { seq: 0, target: c % cfg.nodes, current: None }).collect();
        Sim { group: vec![0; cfg.nodes], rng: Rng::new(seed), now: 0, next_id: 0, queue: BinaryHeap::new(), events: HashMap::new(), servers,
              clients, applied: BTreeMap::new(), leader_of_term: HashMap::new(), report: Report::default(), trace: DefaultHasher::new(), cfg, seed }
    }

    fn push(&mut self, delay: u64, ev: Ev) {
        let id = self.next_id;
        self.next_id += 1;
        self.queue.push(Reverse((self.now + delay, id)));
        self.events.insert(id, ev);
    }

    fn delay(&mut self) -> u64 {
        self.cfg.min_delay + self.rng.below(self.cfg.max_delay - self.cfg.min_delay + 1)
    }

    fn send(&mut self, ev: Ev) {
        if self.rng.chance(self.cfg.drop) {
            return;
        }
        let dup = self.rng.chance(self.cfg.dup);
        if dup {
            let copy = match &ev {
                Ev::Raft { from, to, msg } => Ev::Raft { from: *from, to: *to, msg: msg.clone() },
                Ev::Req { client, node, op } => Ev::Req { client: *client, node: *node, op: op.clone() },
                Ev::Resp { client, seq, reply } => Ev::Resp { client: *client, seq: *seq, reply: reply.clone() },
            };
            let d = self.delay();
            self.push(d, copy);
        }
        let d = self.delay();
        self.push(d, ev);
    }

    fn violation(&mut self, what: String) {
        if self.report.violations.len() < 20 {
            self.report.violations.push(format!("tick {}: {}", self.now, what));
        }
    }

    /// Everything that follows a node doing something: send its messages, apply what it committed,
    /// answer clients, and check the invariants that concern leaders.
    fn after(&mut self, i: NodeId) {
        let Some(node) = self.servers[i].node.as_mut() else { return };
        let out = node.take_outbox();
        let committed = node.take_committed();
        let (role, term, commit, hint) = (node.role, node.term, node.commit, node.leader_hint);
        for (to, msg) in out {
            self.report.messages += 1;
            self.send(Ev::Raft { from: i, to, msg });
        }
        if commit < self.servers[i].last_commit {
            self.violation(format!("node {i} commit index went down from {} to {commit}", self.servers[i].last_commit));
        }
        self.servers[i].last_commit = commit;
        for (idx, entry) in committed {
            match self.applied.get(&idx) {
                Some(e) if *e != entry => self.violation(format!("node {i} applied a different entry at index {idx}")),
                None => {
                    self.applied.insert(idx, entry.clone());
                    self.report.committed = self.report.committed.max(idx);
                }
                _ => {}
            }
            let waiting = self.servers[i].pending.remove(&idx);
            if let Cmd::Op(op) = &entry.cmd {
                let reply = self.servers[i].kv.apply(op);
                if let Some((c, s)) = waiting {
                    let r = if (c, s) == (op.client, op.seq) { Ok(reply) } else { Err(hint) };
                    self.send(Ev::Resp { client: c, seq: s, reply: r });
                }
            } else if let Some((c, s)) = waiting {
                self.send(Ev::Resp { client: c, seq: s, reply: Err(hint) });
            }
        }
        let is_leader = role == Role::Leader;
        if is_leader && !self.servers[i].was_leader {
            self.report.leaders_elected += 1;
            if let Some(&other) = self.leader_of_term.get(&term)
                && other != i {
                    self.violation(format!("two leaders in term {term}: {other} and {i}"));
                }
            self.leader_of_term.insert(term, i);
            let log = &self.servers[i].node.as_ref().unwrap().log;
            let missing = self.applied.iter().find(|(idx, e)| log.get(**idx as usize) != Some(*e)).map(|(idx, _)| *idx);
            if let Some(idx) = missing {
                self.violation(format!("new leader {i} of term {term} lacks committed entry {idx}"));
            }
        }
        self.servers[i].was_leader = is_leader;
    }

    fn check_log_matching(&mut self) {
        let logs: Vec<Vec<Entry>> = self.servers.iter().map(|s| s.node.as_ref().map(|n| n.log.clone()).unwrap_or_else(|| s.persisted.log.clone())).collect();
        for a in 0..logs.len() {
            for b in a + 1..logs.len() {
                let (x, y) = (&logs[a], &logs[b]);
                let common = x.len().min(y.len());
                if let Some(k) = (0..common).rev().find(|&k| x[k].term == y[k].term)
                    && x[..=k] != y[..=k] {
                        self.violation(format!("logs of {a} and {b} share index {k} and term {} but differ before it", x[k].term));
                    }
            }
        }
    }

    fn deliver(&mut self, ev: Ev) {
        match ev {
            Ev::Raft { from, to, msg } => {
                if self.servers[to].node.is_none() || self.group[from] != self.group[to] {
                    return;
                }
                self.servers[to].node.as_mut().unwrap().step(from, msg);
                self.after(to);
            }
            Ev::Req { client, node, op } => {
                if self.servers[node].node.is_none() {
                    return;
                }
                if let Some(r) = self.servers[node].kv.already_applied(op.client, op.seq) {
                    self.send(Ev::Resp { client, seq: op.seq, reply: Ok(r) });
                    return;
                }
                let n = self.servers[node].node.as_mut().unwrap();
                if n.role == Role::Leader && self.cfg.stale_reads
                    && let Kind::Get(k) = op.kind {
                        let r = self.servers[node].kv.read_local(k);
                        self.send(Ev::Resp { client, seq: op.seq, reply: Ok(r) });
                        return;
                    }
                let hint = n.leader_hint;
                match n.propose(Cmd::Op(op.clone())) {
                    Some(idx) => {
                        self.servers[node].pending.insert(idx, (client, op.seq));
                        self.after(node);
                    }
                    None => self.send(Ev::Resp { client, seq: op.seq, reply: Err(hint) }),
                }
            }
            Ev::Resp { client, seq, reply } => {
                let c = &mut self.clients[client];
                let Some((op, _, h)) = c.current.clone() else { return };
                if op.seq != seq {
                    return;
                }
                match reply {
                    Ok(r) => {
                        let rec = &mut self.report.history[h];
                        rec.ret = Some(self.now);
                        rec.result = if matches!(op.kind, Kind::Get(_)) { r } else { None };
                        self.report.latencies.push(self.now - rec.invoke);
                        self.clients[client].current = None;
                    }
                    Err(hint) => {
                        let target = hint.unwrap_or_else(|| self.rng.below(self.cfg.nodes as u64) as usize);
                        self.clients[client].target = target;
                        self.clients[client].current = Some((op.clone(), self.now + self.cfg.client_timeout, h));
                        self.send(Ev::Req { client, node: target, op });
                    }
                }
            }
        }
    }

    fn clients_step(&mut self, issue: bool) {
        for c in 0..self.clients.len() {
            match self.clients[c].current.clone() {
                None if issue && self.rng.chance(0.3) => {
                    self.clients[c].seq += 1;
                    let seq = self.clients[c].seq;
                    let key = self.rng.below(self.cfg.keys as u64) as u8;
                    let value = ((c as u32) << 20) | seq as u32;
                    let kind = match self.rng.below(10) {
                        0..=3 => Kind::Get(key),
                        4..=6 => Kind::Put(key, value),
                        _ => Kind::Append(key, value),
                    };
                    let op = Op { client: c, seq, kind: kind.clone() };
                    self.report.history.push(HistOp { kind, invoke: self.now, ret: None, result: None });
                    let h = self.report.history.len() - 1;
                    self.clients[c].current = Some((op.clone(), self.now + self.cfg.client_timeout, h));
                    let node = self.clients[c].target;
                    self.send(Ev::Req { client: c, node, op });
                }
                Some((op, deadline, h)) if self.now >= deadline => {
                    let node = self.rng.below(self.cfg.nodes as u64) as usize;
                    self.clients[c].target = node;
                    self.clients[c].current = Some((op.clone(), self.now + self.cfg.client_timeout, h));
                    self.send(Ev::Req { client: c, node, op });
                }
                _ => {}
            }
        }
    }

    fn faults(&mut self) {
        for i in 0..self.servers.len() {
            if self.servers[i].node.is_none() && self.now >= self.servers[i].restart_at {
                self.restart(i);
            }
        }
        if self.rng.chance(self.cfg.crash_rate) {
            let i = self.rng.below(self.cfg.nodes as u64) as usize;
            if let Some(n) = self.servers[i].node.take() {
                self.report.crashes += 1;
                self.servers[i].persisted = n.persistent();
                self.servers[i].kv = Kv::default();
                self.servers[i].pending.clear();
                self.servers[i].was_leader = false;
                self.servers[i].last_commit = 0;
                self.servers[i].restart_at = self.now + self.cfg.restart_min + self.rng.below(self.cfg.restart_max - self.cfg.restart_min + 1);
            }
        }
        if self.rng.chance(self.cfg.partition_rate) {
            self.report.partitions += 1;
            if self.rng.chance(0.4) {
                self.group = vec![0; self.cfg.nodes];
            } else {
                self.group = (0..self.cfg.nodes).map(|_| self.rng.below(2) as u8).collect();
            }
        }
    }

    fn restart(&mut self, i: NodeId) {
        let seed = self.seed.wrapping_mul(1000).wrapping_add(i as u64).wrapping_add(self.now << 20);
        let node = Node::restore(i, self.cfg.nodes, self.cfg.raft.clone(), seed, self.servers[i].persisted.clone());
        self.servers[i].node = Some(node);
    }

    fn tick(&mut self, faulty: bool, issue: bool) {
        while let Some(&Reverse((at, id))) = self.queue.peek() {
            if at > self.now {
                break;
            }
            self.queue.pop();
            if let Some(ev) = self.events.remove(&id) {
                self.deliver(ev);
            }
        }
        for i in 0..self.servers.len() {
            if let Some(n) = self.servers[i].node.as_mut() {
                n.tick();
                self.after(i);
            }
        }
        self.clients_step(issue);
        if faulty {
            self.faults();
        } else {
            self.group = vec![0; self.cfg.nodes];
            for i in 0..self.servers.len() {
                if self.servers[i].node.is_none() {
                    self.restart(i);
                }
            }
        }
        if self.now.is_multiple_of(25) {
            self.check_log_matching();
        }
        let leader = self.servers.iter().any(|s| s.node.as_ref().is_some_and(|n| n.role == Role::Leader));
        for s in &self.servers {
            if let Some(n) = &s.node {
                (self.now, n.id, n.term, n.commit, n.role as u8).hash(&mut self.trace);
            }
        }
        if !leader {
            self.report.longest_without_leader += 1; // replaced by the longest stretch below
        }
        self.now += 1;
    }

    pub fn run(mut self) -> Report {
        let mut stretch = 0u64;
        let mut longest = 0u64;
        for _ in 0..self.cfg.ticks {
            let before = self.report.longest_without_leader;
            self.tick(true, true);
            stretch = if self.report.longest_without_leader > before { stretch + 1 } else { 0 };
            longest = longest.max(stretch);
        }
        for _ in 0..self.cfg.settle_ticks {
            self.tick(false, false);
            if self.clients.iter().all(|c| c.current.is_none()) {
                break;
            }
        }
        self.check_log_matching();
        self.report.longest_without_leader = longest;
        self.report.completed = self.report.history.iter().filter(|h| h.ret.is_some()).count();
        self.report.unanswered = self.report.history.len() - self.report.completed;
        match linz::check(&self.report.history) {
            Ok(()) => self.report.linearizable = true,
            Err(k) => {
                self.report.linearizable = false;
                self.violation(format!("history is not linearizable on key {k}"));
            }
        }
        self.report.trace = self.trace.finish();
        self.report
    }
}
