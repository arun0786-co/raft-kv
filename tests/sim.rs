//! The whole cluster under faults. Every run is deterministic in its seed.

use raftkv::sim::{Sim, SimConfig};

fn clean(cfg: SimConfig, seeds: std::ops::Range<u64>) {
    for seed in seeds {
        let r = Sim::new(cfg.clone(), seed).run();
        assert!(r.violations.is_empty(), "seed {seed}: {:?}", r.violations);
        assert!(r.linearizable, "seed {seed}");
        assert_eq!(r.unanswered, 0, "seed {seed}");
        assert!(r.completed > 100, "seed {seed}: only {} operations", r.completed);
    }
}

#[test]
fn correct_code_survives_default_faults() {
    clean(SimConfig::default(), 0..150);
}

#[test]
fn correct_code_survives_aggressive_faults() {
    clean(SimConfig::aggressive(), 0..150);
}

#[test]
fn runs_are_deterministic() {
    let a = Sim::new(SimConfig::aggressive(), 42).run();
    let b = Sim::new(SimConfig::aggressive(), 42).run();
    assert_eq!(a.trace, b.trace);
    assert_eq!((a.completed, a.crashes, a.messages), (b.completed, b.crashes, b.messages));
    assert_ne!(a.trace, Sim::new(SimConfig::aggressive(), 43).run().trace);
}

fn found(cfg: SimConfig, seeds: std::ops::Range<u64>) -> Option<(u64, String)> {
    seeds.into_iter().find_map(|s| {
        let r = Sim::new(cfg.clone(), s).run();
        r.violations.first().map(|v| (s, v.clone()))
    })
}

#[test]
fn votes_without_the_log_check_are_caught() {
    let mut cfg = SimConfig::default();
    cfg.raft.bugs.vote_without_log_check = true;
    let (_, v) = found(cfg, 0..20).expect("not found");
    assert!(v.contains("lacks committed entry") || v.contains("different entry") || v.contains("linearizable"), "{v}");
}

#[test]
fn stale_reads_are_caught_by_the_linearizability_check() {
    let cfg = SimConfig { stale_reads: true, ..SimConfig::default() };
    let (_, v) = found(cfg, 0..20).expect("not found");
    assert!(v.contains("not linearizable"), "{v}");
}

#[test]
fn committing_old_terms_is_caught_under_aggressive_faults() {
    let mut cfg = SimConfig::aggressive();
    cfg.raft.bugs.commit_old_terms = true;
    let (seed, v) = found(cfg, 0..200).expect("not found");
    assert_eq!(seed, 98);
    assert!(v.contains("lacks committed entry"), "{v}");
}

#[test]
fn forgetting_the_vote_is_caught_under_aggressive_faults() {
    let mut cfg = SimConfig::aggressive();
    cfg.raft.bugs.forget_vote = true;
    let (seed, v) = found(cfg, 230..250).expect("not found");
    assert_eq!(seed, 239);
    assert!(v.contains("two leaders in term"), "{v}");
}
