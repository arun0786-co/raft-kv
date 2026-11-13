//! The replicated key-value state machine. Every command carries (client, seq) so that a
//! request retried after a timeout is applied once and answered with the same result.

use std::collections::HashMap;

pub type Key = u8;
pub type Value = u32;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Kind {
    Put(Key, Value),
    Append(Key, Value),
    Get(Key),
}

impl Kind {
    pub fn key(&self) -> Key {
        match self {
            Kind::Put(k, _) | Kind::Append(k, _) | Kind::Get(k) => *k,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Op {
    pub client: usize,
    pub seq: u64,
    pub kind: Kind,
}

/// Get returns the list of values appended since the last put (a put starts a new list).
pub type Reply = Option<Vec<Value>>;

#[derive(Default, Clone)]
pub struct Kv {
    data: HashMap<Key, Vec<Value>>,
    last: HashMap<usize, (u64, Reply)>, // per client: last applied seq and its reply
}

impl Kv {
    pub fn apply(&mut self, op: &Op) -> Reply {
        if let Some((seq, reply)) = self.last.get(&op.client)
            && op.seq <= *seq {
                return reply.clone(); // a duplicate: do not apply again
            }
        let reply = match &op.kind {
            Kind::Put(k, v) => {
                self.data.insert(*k, vec![*v]);
                None
            }
            Kind::Append(k, v) => {
                self.data.entry(*k).or_default().push(*v);
                None
            }
            Kind::Get(k) => Some(self.data.get(k).cloned().unwrap_or_default()),
        };
        self.last.insert(op.client, (op.seq, reply.clone()));
        reply
    }

    pub fn already_applied(&self, client: usize, seq: u64) -> Option<Reply> {
        self.last.get(&client).filter(|(s, _)| seq <= *s).map(|(_, r)| r.clone())
    }

    /// Read without going through the log (used only to model the stale-read bug).
    pub fn read_local(&self, k: Key) -> Reply {
        Some(self.data.get(&k).cloned().unwrap_or_default())
    }
}
