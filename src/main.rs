//! Run the simulation over many seeds and print a summary.
//!
//!   raftkv [seeds] [bug] [profile]
//!   bug: none | commit_old_terms | vote_without_log_check | forget_vote | stale_reads
//!   profile: default | aggressive

use raftkv::sim::{Sim, SimConfig};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let seeds: u64 = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(200);
    let bug = args.get(2).map(String::as_str).unwrap_or("none");
    let profile = args.get(3).map(String::as_str).unwrap_or("default");
    let mut cfg = if profile == "aggressive" { SimConfig::aggressive() } else { SimConfig::default() };
    match bug {
        "none" => {}
        "commit_old_terms" => cfg.raft.bugs.commit_old_terms = true,
        "vote_without_log_check" => cfg.raft.bugs.vote_without_log_check = true,
        "forget_vote" => cfg.raft.bugs.forget_vote = true,
        "stale_reads" => cfg.stale_reads = true,
        other => panic!("unknown bug {other}"),
    }
    let t0 = std::time::Instant::now();
    let (mut failed, mut first) = (0u64, None);
    let (mut ops, mut unanswered, mut crashes, mut parts, mut elections, mut msgs) = (0usize, 0usize, 0u64, 0u64, 0u64, 0u64);
    let mut lat: Vec<u64> = Vec::new();
    let mut longest = 0u64;
    for seed in 0..seeds {
        let r = Sim::new(cfg.clone(), seed).run();
        if !r.violations.is_empty() {
            failed += 1;
            if first.is_none() {
                first = Some((seed, r.violations[0].clone()));
            }
        }
        ops += r.completed;
        unanswered += r.unanswered;
        crashes += r.crashes;
        parts += r.partitions;
        elections += r.leaders_elected;
        msgs += r.messages;
        longest = longest.max(r.longest_without_leader);
        lat.extend(r.latencies);
    }
    lat.sort_unstable();
    let q = |p: f64| lat.get(((lat.len().saturating_sub(1)) as f64 * p) as usize).copied().unwrap_or(0);
    println!("bug: {bug}, profile: {profile}");
    println!("seeds: {seeds}, ticks of faults each: {}, nodes: {}, clients: {}", cfg.ticks, cfg.nodes, cfg.clients);
    println!("runs with a violation: {failed}");
    if let Some((s, v)) = first {
        println!("first: seed {s}, {v}");
    }
    println!("client operations completed: {ops}, unanswered at the end: {unanswered}");
    println!("crashes: {crashes}, partition changes: {parts}, leaders elected: {elections}, raft messages: {msgs}");
    println!("operation latency in ticks: p50 {}, p90 {}, p99 {}, max {}", q(0.5), q(0.9), q(0.99), lat.last().copied().unwrap_or(0));
    println!("longest stretch without any leader: {longest} ticks");
    println!("wall time: {:.1} s", t0.elapsed().as_secs_f64());
}
