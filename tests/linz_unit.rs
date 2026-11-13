use raftkv::kv::Kind;
use raftkv::linz::{check, HistOp};

fn op(kind: Kind, invoke: u64, ret: Option<u64>, result: Option<Vec<u32>>) -> HistOp {
    HistOp { kind, invoke, ret, result }
}

#[test]
fn sequential_history_is_linearizable() {
    let h = vec![
        op(Kind::Put(1, 10), 0, Some(1), None),
        op(Kind::Append(1, 11), 2, Some(3), None),
        op(Kind::Get(1), 4, Some(5), Some(vec![10, 11])),
    ];
    assert!(check(&h).is_ok());
}

#[test]
fn a_stale_read_is_caught() {
    // the write finished before the read started, yet the read does not see it
    let h = vec![op(Kind::Put(1, 10), 0, Some(1), None), op(Kind::Put(1, 20), 2, Some(3), None), op(Kind::Get(1), 4, Some(5), Some(vec![10]))];
    assert_eq!(check(&h), Err(1));
}

#[test]
fn overlapping_operations_may_take_either_order() {
    let w = op(Kind::Put(1, 20), 2, Some(10), None);
    let before = vec![op(Kind::Put(1, 10), 0, Some(1), None), w.clone(), op(Kind::Get(1), 3, Some(4), Some(vec![10]))];
    let after = vec![op(Kind::Put(1, 10), 0, Some(1), None), w, op(Kind::Get(1), 3, Some(4), Some(vec![20]))];
    assert!(check(&before).is_ok() && check(&after).is_ok());
}

#[test]
fn an_unanswered_write_may_or_may_not_have_happened() {
    let base = vec![op(Kind::Put(1, 10), 0, Some(1), None), op(Kind::Append(1, 11), 2, None, None)];
    let mut seen = base.clone();
    seen.push(op(Kind::Get(1), 5, Some(6), Some(vec![10, 11])));
    let mut unseen = base.clone();
    unseen.push(op(Kind::Get(1), 5, Some(6), Some(vec![10])));
    assert!(check(&seen).is_ok() && check(&unseen).is_ok());
    // but it cannot take effect before it was issued
    let early = vec![op(Kind::Put(1, 10), 0, Some(1), None), op(Kind::Get(1), 2, Some(3), Some(vec![10, 11])), op(Kind::Append(1, 11), 4, None, None)];
    assert!(check(&early).is_err());
}

#[test]
fn keys_are_checked_independently() {
    let h = vec![op(Kind::Put(1, 10), 0, Some(1), None), op(Kind::Put(2, 99), 0, Some(1), None), op(Kind::Get(2), 2, Some(3), Some(vec![10]))];
    assert_eq!(check(&h), Err(2));
}
