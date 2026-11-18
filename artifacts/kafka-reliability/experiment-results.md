# Kafka reliability experiment results

Date: 2026-11-16
Branch: `codex/m12-capacity-security-release`

| Experiment | Parameters | Result | Wall time |
|---|---|---|---|
| Ten fault windows | width=1, 10 repetitions/window, lease=2s | PASS (100/100 injections) | 25.64s |
| Wide workflow | width=8 and 32, 10 repetitions each | PASS (20 Runs, 400 Tasks) | 6.60s |
| Three-broker protocol | RF=3, Min ISR=2, acks=all | PASS (one down, leader kill, two down/recover) | 107.68s |

Assertions include Outbox publish state, Kafka redelivery, Claim lease identity, stale fencing rejection, Inbox rollback/deduplication, terminal Run state, output cardinality, replica count and ISR count.

These are deterministic single-machine protocol results, not production throughput or availability measurements.
