---
title: Fleetline vision axes
date: 2026-10-02
kind: note
---

# Fleetline vision axes

What "ideal" means, axis by axis, with the current level (0 to 5) and the evidence behind it. The loop pulls the weakest axis first. Update the table after every change. Charter: loop charter.

| Axis | Level | Evidence | Gap to 5 |
| --- | --- | --- | --- |
| Correctness | 4 | 54 integration tests (replay by persisted offset, crash between commit and publish) on PostGIS, Kafka and NATS; 18M reports reconciled in `evidence/baseline-100k.json` | correctness proven under faults while loaded (see Failure tolerance) |
| Capacity on one machine | 4 | 100k × 900 s; 200k and 300k × 300 s pass; 300k on one writer per processor p95 876 ms (`rung-300k-one-writer`); 500k fails on host CPU | profile at the ceiling on a second load machine; ladder toward 1M |
| Horizontal scale | 2 | edge LB with api ×4 and gateway ×2 replicas (measured 150k, ingest balanced within 6%); configurable RF and NATS servers; PostgreSQL unsharded (Citus decided) | LB + api ×N proven; configurable RF and servers; a database write-scaling design (built or explicit) |
| Failure tolerance | 4 | 100k campaign (9 scenarios, graceful stops and kills of stateless tiers); processor kill, pause and rebalance on one writer (`1cf61ae`); alert drill | faults at 300k (D6); SIGKILL of Kafka and PostgreSQL (D3); RF 3 Kafka, NATS cluster, 2 edges (D5) |
| Operations | 4 | Prometheus per role, kafka/postgres/nats/haproxy exporters, provisioned Grafana dashboard, 12 alerts with promtool tests and a runbook each, alert drill proving 5 alerts fire under real faults (`01ad9d9`) | request ids at the edge; KEDA ScaledObject |
| Security | 4 | security lens closed (`8b05a54`, `c862bbe`): bounded matches per report, gateway byte budget, one body limit, per-user edge limits, least-privilege DB roles, NATS token, Grafana without login, future timestamps clamped | per-socket ingest token bucket (X6); device credentials (out of scope, stated) |
| Dashboard clarity | 3 | dashboard exists; demo panel | legend, demo story, grouped plain-language alerts, verified screenshots |
| Product features | 2 | the brief's features plus insights and the demo | entry/exit, dwell time, history, rules: chosen by the product lens |
| Code quality | 4 | ruff, mypy --strict and vulture (80%) clean and in the gate (`205c6bc`); one msgspec Report; liveness decided in one place | invariant duplication sweep |
| Honest documentation | 4 | claims audit: 10 FALSE and 10 UNSUPPORTED found, most fixed; a second audit is owed | the claims auditor passes every claim |
