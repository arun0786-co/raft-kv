# Raft key-value store

A replicated key-value store on Raft, written in Rust with no dependencies. It is tested by
deterministic simulation: the whole cluster runs in one thread under a seeded random
network that delays, drops, duplicates and partitions messages and crashes nodes. Raft's
safety properties are checked as the run goes and every client history is checked for
linearizability.

## What is in it

```
src/raft.rs   Raft as a pure state machine: elections, log replication, commit, conflict hints
src/kv.rs     the replicated map; every request carries (client, seq), so a retry is applied once
src/sim.rs    the simulator, the fault model and the invariant checks
src/linz.rs   linearizability checker (Wing and Gong, with Lowe's memoisation), one key at a time
src/main.rs   run many seeds: raftkv [seeds] [bug] [default|aggressive]
```

Reads go through the log, so they are linearizable. A new leader appends a no-op so that it
can commit entries from earlier terms. Not implemented: snapshots, membership changes,
pre-vote.

## How it is tested

Messages take 1 to 6 ticks, and 5% are dropped and 2% duplicated. The network splits into
random halves and heals, and nodes crash and restart from their persistent state (term, vote,
log). Five clients issue gets, puts and appends on four keys, retrying on timeouts and
redirects.

While the run goes, the simulator checks that there's at most one leader per term, that the
log matching property holds across all node pairs (live or crashed), that every new leader
holds every committed entry, that every node applies the same command at each index, and that
a live node's commit index never decreases. After the faults stop, the network heals, the
outstanding requests finish, and the whole client history is checked for linearizability.

There's also an aggressive profile, which restarts nodes within a few ticks, crashes and
partitions more often, and sends one log entry per message.

## Results

| Run | Seeds | Runs with a violation | Client operations | Crashes |
|---|---|---|---|---|
| Correct code, default faults | 10,000 | 0 | 5,345,573 | 85,401 |
| Correct code, aggressive faults | 10,000 | 0 | 2,456,047 | 292,379 |

Every operation was answered and every history was linearizable. With default faults,
latency is 13 ticks at the median and 208 at the 99th percentile. 10,000 runs take 41 seconds.

To see whether the checks catch real mistakes, four bugs can be switched on, each a known way
to break Raft:

| Planted bug | Default faults | Aggressive faults | What catches it |
|---|---|---|---|
| Grant votes without checking the candidate's log | 984 of 1,000 | 1,000 of 1,000 | a new leader lacks a committed entry |
| Leader answers reads from its own state | 309 of 1,000 | 625 of 1,000 | linearizability |
| Count replicas to commit entries from earlier terms (figure 8 of the paper) | 0 of 10,000 | 75 of 10,000 | a new leader lacks a committed entry |
| Forget the vote on restart | 0 of 10,000 | 12 of 10,000 | two leaders in one term |

The last two rows are the interesting ones, because the default faults never find them in
10,000 runs each. The figure 8 bug is masked there: the new leader's no-op entry travels in
the same message as the old entry, so the old entry never reaches a majority on its own.
Sending one entry per message exposes it. The forgotten vote needs a quick restart, since the
node has to come back inside the election it voted in, and default restarts take 20 to 200
ticks. So a clean run only means something for the faults it actually tried.

The simulator also found a bug in my own Raft on its first run. A delayed append carrying
fewer entries than the follower had already committed lowered the follower's commit index.

## Run it

```
cargo test --release                              # 17 tests, about a second
cargo run --release -- 10000                      # 10,000 seeds, default faults
cargo run --release -- 1000 forget_vote aggressive
```

Raw output of the runs above: [results/](results/).

## Limits

- No real network or disk. Persistence is modelled: a crash keeps exactly what Raft says must
  be on disk before replying. Torn writes and lost fsyncs aren't modelled.
- No snapshots, so logs grow without bound, and no membership changes either.
- The linearizability check is exponential in the worst case. Five clients keep it fast.
- Time is in ticks, so latencies are relative and not in milliseconds.

## References

- Ongaro, D. and Ousterhout, J. (2014). In search of an understandable consensus algorithm. USENIX ATC.
- Wing, J. and Gong, C. (1993). Testing and verifying concurrent objects. Journal of Parallel and Distributed Computing 17.
- Lowe, G. (2017). Testing for linearizability. Concurrency and Computation: Practice and Experience 29(4).
- Kingsbury, K. Jepsen analyses, https://jepsen.io, for the approach of fault injection plus linearizability checking.
