//! Linearizability checker for the key-value history (Wing and Gong, 1993, with the memoisation
//! of Lowe, 2017, as in Porcupine). Operations on different keys commute, so each key is checked
//! on its own.
//!
//! An operation can be placed next if no other unplaced operation finished before it started.
//! Operations that never got an answer may take effect at any point after they started, or never.

use std::collections::{HashMap, HashSet};

use crate::kv::{Key, Kind, Value};

#[derive(Clone, Debug)]
pub struct HistOp {
    pub kind: Kind,
    pub invoke: u64,
    pub ret: Option<u64>,               // None: no answer was received
    pub result: Option<Vec<Value>>,     // for a completed Get
}

pub fn check(history: &[HistOp]) -> Result<(), Key> {
    let mut by_key: HashMap<Key, Vec<&HistOp>> = HashMap::new();
    for op in history {
        // an unanswered Get constrains nothing
        if op.ret.is_none() && matches!(op.kind, Kind::Get(_)) {
            continue;
        }
        by_key.entry(op.kind.key()).or_default().push(op);
    }
    let mut keys: Vec<_> = by_key.keys().copied().collect();
    keys.sort();
    for k in keys {
        if !check_key(&by_key[&k]) {
            return Err(k);
        }
    }
    Ok(())
}

fn check_key(ops: &[&HistOp]) -> bool {
    let n = ops.len();
    let completed = ops.iter().filter(|o| o.ret.is_some()).count();
    let words = n.div_ceil(64);
    let mut done = vec![0u64; words];
    let mut seen: HashSet<(Vec<u64>, Vec<Value>)> = HashSet::new();
    let mut state: Vec<Value> = Vec::new();
    dfs(ops, &mut done, 0, completed, &mut state, &mut seen)
}

fn dfs(ops: &[&HistOp], done: &mut Vec<u64>, placed_completed: usize, completed: usize, state: &mut Vec<Value>,
       seen: &mut HashSet<(Vec<u64>, Vec<Value>)>) -> bool {
    if placed_completed == completed {
        return true;
    }
    if !seen.insert((done.clone(), state.clone())) {
        return false;
    }
    let is_done = |d: &Vec<u64>, i: usize| d[i / 64] >> (i % 64) & 1 == 1;
    let min_ret = (0..ops.len()).filter(|&i| !is_done(done, i)).filter_map(|i| ops[i].ret).min().unwrap_or(u64::MAX);
    for i in 0..ops.len() {
        if is_done(done, i) || ops[i].invoke > min_ret {
            continue;
        }
        let op = ops[i];
        let saved = state.clone();
        let ok = match &op.kind {
            Kind::Put(_, v) => {
                *state = vec![*v];
                true
            }
            Kind::Append(_, v) => {
                state.push(*v);
                true
            }
            Kind::Get(_) => op.result.as_ref() == Some(state),
        };
        if ok {
            done[i / 64] |= 1 << (i % 64);
            let pc = placed_completed + usize::from(op.ret.is_some());
            if dfs(ops, done, pc, completed, state, seen) {
                return true;
            }
            done[i / 64] &= !(1 << (i % 64));
        }
        *state = saved;
    }
    false
}
