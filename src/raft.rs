//! Raft consensus as a pure state machine (Ongaro and Ousterhout, 2014).
//!
//! A node never does I/O. The caller delivers messages with `step`, advances time with
//! `tick`, and collects outgoing messages and newly committed entries. Persistence is
//! modelled by `persistent()`: whatever it returns after a call must be on stable storage
//! before that call's messages are sent. The simulator does exactly that.
//!
//! Not implemented: snapshots and log compaction, membership changes, pre-vote.

use std::collections::BTreeSet;

use crate::rng::Rng;

pub type NodeId = usize;
pub type Term = u64;
pub type Index = u64;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Cmd {
    /// Appended by every new leader so that it can commit entries from earlier terms.
    Noop,
    Op(crate::kv::Op),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Entry {
    pub term: Term,
    pub cmd: Cmd,
}

#[derive(Clone, Debug)]
pub enum Msg {
    Vote { term: Term, last_index: Index, last_term: Term },
    VoteResp { term: Term, granted: bool },
    Append { term: Term, prev_index: Index, prev_term: Term, entries: Vec<Entry>, commit: Index },
    AppendResp { term: Term, success: bool, match_index: Index, conflict_index: Index },
}

impl Msg {
    pub fn term(&self) -> Term {
        match self {
            Msg::Vote { term, .. } | Msg::VoteResp { term, .. } | Msg::Append { term, .. } | Msg::AppendResp { term, .. } => *term,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

/// Deliberate mistakes, used to show that the tests can find them.
#[derive(Clone, Copy, Debug, Default)]
pub struct Bugs {
    /// Count replicas to commit entries from earlier terms (the scenario of figure 8 in the paper).
    pub commit_old_terms: bool,
    /// Grant votes without checking that the candidate's log is at least as up to date.
    pub vote_without_log_check: bool,
    /// Do not persist the vote, so a restarted node can vote twice in one term.
    pub forget_vote: bool,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub election_ticks: u32, // timeout is drawn from [election_ticks, 2 * election_ticks)
    pub heartbeat_ticks: u32,
    pub max_batch: usize,
    pub bugs: Bugs,
}

impl Default for Config {
    fn default() -> Self {
        Config { election_ticks: 20, heartbeat_ticks: 4, max_batch: 64, bugs: Bugs::default() }
    }
}

#[derive(Clone, Debug)]
pub struct Persistent {
    pub term: Term,
    pub voted_for: Option<NodeId>,
    pub log: Vec<Entry>, // index 0 is a sentinel with term 0
}

pub struct Node {
    pub id: NodeId,
    n: usize,
    cfg: Config,
    rng: Rng,
    // persistent
    pub term: Term,
    pub voted_for: Option<NodeId>,
    pub log: Vec<Entry>,
    // volatile
    pub role: Role,
    pub commit: Index,
    applied: Index,
    pub leader_hint: Option<NodeId>,
    votes: BTreeSet<NodeId>,
    next: Vec<Index>,
    matched: Vec<Index>,
    elapsed: u32,
    timeout: u32,
    outbox: Vec<(NodeId, Msg)>,
}

impl Node {
    pub fn new(id: NodeId, n: usize, cfg: Config, seed: u64) -> Node {
        Node::restore(id, n, cfg, seed, Persistent { term: 0, voted_for: None, log: vec![Entry { term: 0, cmd: Cmd::Noop }] })
    }

    /// A node coming back after a crash: persistent state survives, everything else starts over.
    pub fn restore(id: NodeId, n: usize, cfg: Config, seed: u64, p: Persistent) -> Node {
        let mut rng = Rng::new(seed);
        let timeout = cfg.election_ticks + rng.below(cfg.election_ticks as u64) as u32;
        let voted_for = if cfg.bugs.forget_vote { None } else { p.voted_for };
        Node {
            id, n, cfg, rng, term: p.term, voted_for, log: p.log, role: Role::Follower, commit: 0, applied: 0, leader_hint: None,
            votes: BTreeSet::new(), next: vec![1; n], matched: vec![0; n], elapsed: 0, timeout, outbox: Vec::new(),
        }
    }

    pub fn persistent(&self) -> Persistent {
        Persistent { term: self.term, voted_for: self.voted_for, log: self.log.clone() }
    }

    pub fn last_index(&self) -> Index {
        (self.log.len() - 1) as Index
    }

    fn last_term(&self) -> Term {
        self.log.last().unwrap().term
    }

    fn majority(&self) -> usize {
        self.n / 2 + 1
    }

    fn reset_timer(&mut self) {
        self.elapsed = 0;
        self.timeout = self.cfg.election_ticks + self.rng.below(self.cfg.election_ticks as u64) as u32;
    }

    fn become_follower(&mut self, term: Term) {
        if term > self.term {
            self.term = term;
            self.voted_for = None;
        }
        self.role = Role::Follower;
        self.votes.clear();
    }

    fn start_election(&mut self) {
        self.term += 1;
        self.role = Role::Candidate;
        self.voted_for = Some(self.id);
        self.votes = BTreeSet::from([self.id]);
        self.leader_hint = None;
        self.reset_timer();
        if self.votes.len() >= self.majority() {
            self.become_leader();
            return;
        }
        let (li, lt, me) = (self.last_index(), self.last_term(), self.id);
        for p in (0..self.n).filter(|&p| p != me) {
            self.outbox.push((p, Msg::Vote { term: self.term, last_index: li, last_term: lt }));
        }
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.leader_hint = Some(self.id);
        self.log.push(Entry { term: self.term, cmd: Cmd::Noop });
        let last = self.last_index();
        self.next = vec![last + 1; self.n];
        self.matched = vec![0; self.n];
        self.matched[self.id] = last;
        self.elapsed = 0;
        self.broadcast_append();
        self.advance_commit();
    }

    fn send_append(&mut self, p: NodeId) {
        let prev = self.next[p] - 1;
        let end = (self.log.len() as Index).min(self.next[p] + self.cfg.max_batch as Index);
        let entries = self.log[self.next[p] as usize..end as usize].to_vec();
        let msg = Msg::Append { term: self.term, prev_index: prev, prev_term: self.log[prev as usize].term, entries, commit: self.commit };
        self.outbox.push((p, msg));
    }

    fn broadcast_append(&mut self) {
        let me = self.id;
        for p in (0..self.n).filter(|&p| p != me) {
            self.send_append(p);
        }
    }

    fn advance_commit(&mut self) {
        let mut sorted = self.matched.clone();
        sorted.sort_unstable_by(|a, b| b.cmp(a));
        let candidate = sorted[self.majority() - 1]; // replicated on a majority
        if candidate > self.commit && (self.cfg.bugs.commit_old_terms || self.log[candidate as usize].term == self.term) {
            self.commit = candidate;
        }
    }

    /// Advance time by one tick.
    pub fn tick(&mut self) {
        self.elapsed += 1;
        match self.role {
            Role::Leader => {
                if self.elapsed >= self.cfg.heartbeat_ticks {
                    self.elapsed = 0;
                    self.broadcast_append();
                }
            }
            _ => {
                if self.elapsed >= self.timeout {
                    self.start_election();
                }
            }
        }
    }

    /// Leader only: append a command. Returns its index.
    pub fn propose(&mut self, cmd: Cmd) -> Option<Index> {
        if self.role != Role::Leader {
            return None;
        }
        self.log.push(Entry { term: self.term, cmd });
        let idx = self.last_index();
        self.matched[self.id] = idx;
        self.broadcast_append();
        self.advance_commit();
        Some(idx)
    }

    pub fn step(&mut self, from: NodeId, msg: Msg) {
        if msg.term() > self.term {
            self.become_follower(msg.term());
        }
        match msg {
            Msg::Vote { term, last_index, last_term } => {
                let up_to_date = last_term > self.last_term() || (last_term == self.last_term() && last_index >= self.last_index());
                let grant = term == self.term
                    && (self.voted_for.is_none() || self.voted_for == Some(from))
                    && (up_to_date || self.cfg.bugs.vote_without_log_check);
                if grant {
                    self.voted_for = Some(from);
                    self.reset_timer();
                }
                self.outbox.push((from, Msg::VoteResp { term: self.term, granted: grant }));
            }
            Msg::VoteResp { term, granted } => {
                if self.role == Role::Candidate && term == self.term && granted {
                    self.votes.insert(from);
                    if self.votes.len() >= self.majority() {
                        self.become_leader();
                    }
                }
            }
            Msg::Append { term, prev_index, prev_term, entries, commit } => {
                if term < self.term {
                    self.outbox.push((from, Msg::AppendResp { term: self.term, success: false, match_index: 0, conflict_index: 0 }));
                    return;
                }
                self.role = Role::Follower; // a candidate in the same term steps down
                self.leader_hint = Some(from);
                self.reset_timer();
                if prev_index > self.last_index() {
                    let ci = self.last_index() + 1;
                    self.outbox.push((from, Msg::AppendResp { term: self.term, success: false, match_index: 0, conflict_index: ci }));
                    return;
                }
                if self.log[prev_index as usize].term != prev_term {
                    // skip back over the whole conflicting term in one round trip
                    let bad = self.log[prev_index as usize].term;
                    let mut ci = prev_index;
                    while ci > 1 && self.log[(ci - 1) as usize].term == bad {
                        ci -= 1;
                    }
                    self.outbox.push((from, Msg::AppendResp { term: self.term, success: false, match_index: 0, conflict_index: ci }));
                    return;
                }
                // append, truncating only at a real conflict: a stale or duplicated message must not cut the log
                for (k, e) in entries.iter().enumerate() {
                    let idx = prev_index as usize + 1 + k;
                    if idx < self.log.len() {
                        if self.log[idx].term != e.term {
                            self.log.truncate(idx);
                            self.log.push(e.clone());
                        }
                    } else {
                        self.log.push(e.clone());
                    }
                }
                let last_new = prev_index + entries.len() as Index;
                // a delayed message may carry fewer entries than this node already has committed:
                // the commit index only moves forward
                self.commit = self.commit.max(commit.min(last_new));
                self.outbox.push((from, Msg::AppendResp { term: self.term, success: true, match_index: last_new, conflict_index: 0 }));
            }
            Msg::AppendResp { term, success, match_index, conflict_index } => {
                if self.role != Role::Leader || term != self.term {
                    return;
                }
                if success {
                    if match_index > self.matched[from] {
                        self.matched[from] = match_index;
                    }
                    self.next[from] = self.next[from].max(match_index + 1);
                    self.advance_commit();
                    if self.next[from] <= self.last_index() {
                        self.send_append(from);
                    }
                } else if conflict_index > 0 {
                    // jump back to the follower's hint, by at least one, but never below what it is known to hold
                    let back = conflict_index.min(self.next[from].saturating_sub(1));
                    self.next[from] = back.max(self.matched[from] + 1).max(1);
                    self.send_append(from);
                }
            }
        }
    }

    pub fn take_outbox(&mut self) -> Vec<(NodeId, Msg)> {
        std::mem::take(&mut self.outbox)
    }

    /// Entries committed since the last call, in order, with their indices.
    pub fn take_committed(&mut self) -> Vec<(Index, Entry)> {
        let mut out = Vec::new();
        while self.applied < self.commit {
            self.applied += 1;
            out.push((self.applied, self.log[self.applied as usize].clone()));
        }
        out
    }
}
