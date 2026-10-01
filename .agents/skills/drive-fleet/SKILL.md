---
name: drive-fleet
description: Prove a change to this fleet-tracking service works end to end — boot the Compose stack, run the live driver, the integration suite or the reconciling load benchmark, and read their verdicts. Use after touching ingest, processors, matching, fanout, the dashboard protocol or the generator, and before claiming any throughput number.
---

Run from the repository root (the directory with `docker-compose.yml`). The app is `http://127.0.0.1:8097`.

## Topology

- The `edge` (HAProxy) is the only published port. It sends `/ws` to `gateway` replicas and everything else to `api` replicas, both by `leastconn`.
- Scale with `API_REPLICAS=6 GATEWAYS=3 PROCESSORS=8 docker compose up -d --wait`. More processors than Kafka partitions (24) sit idle.
- Prometheus at `http://127.0.0.1:9097` scrapes every replica every 5 s; Grafana at `http://127.0.0.1:3097` (dashboard "Fleetline") answers "keeping up, shedding, dashboards healthy, database the limit, balanced". Quick reads without a browser: `curl -s 127.0.0.1:9097/api/v1/query --data-urlencode 'query=fleet:consumer_lag:records'`, `…query=ALERTS{alertstate="firing"}`, and `…/api/v1/targets` for which replicas are up.
- Consumer lag comes from kafka-exporter (broker group offsets), not from processors, so a dead processor's partitions still count.

## Pick the smallest check that covers the change

| Changed | Check |
| --- | --- |
| Backend code | `docker compose up --build -d --wait`, then `uv run python scripts/drive.py` |
| Matching, dedup, delivery, protocol | `docker compose --profile test run --build --no-deps --rm tests` |
| Anything on the hot path, or a capacity claim | `uv run python scripts/benchmark.py --devices N --duration S` |

- `drive.py` creates and deletes its own zone and device. It checks two owner sessions plus another user. It never resets data.
- The suite must pass whole. Every test gets its own Kafka topic, consumer group and NATS prefix, and the suite refuses any database other than `geo_test`.
- Never rebuild or restart the stack while a benchmark runs, and never run two benchmarks at once.
- **Is the generator the limit?** `uv run python scripts/ack_sink.py` in one shell, then `uv run python generator.py --url http://127.0.0.1:8099 --devices N --duration 60 --processes 8`. If the sink run cannot sustain the rate, the generator is the bottleneck, not the service. 400k devices (80k/s) is known good.

## Reading a benchmark

The process exits 1 unless `acceptance_passed`. When it fails, read in this order:

1. `workload.checks`: the generator never lost or delayed load (`exact_population`, `sustained_rate`, scheduling).
2. `pipeline.checks`: processors committed exactly what was acked, `consumer_lag` drained, and no counter reset (a reset makes every delta wrong; judge a fault run from `stored_latest` instead).
3. `sessions[*].checks`: per-session counts and checksums for positions and alerts, and no server close.
4. `delivery_latency`: p95 from the scheduled timestamp.
5. `resources.containers`: CPU median and peak, memory peak and growth for every container.

A failing generator check means the load generator itself fell behind. Rerun with more `--processes` before blaming the service. The criteria are fixed in `evidence/acceptance-policy-v4.md`; change them only before a run, never after it. Raw results go to `evidence/`.

## Browser

Only when the user approves a browser run: `scripts/ui_verify.js` is a Playwright MCP driver (`mcp__playwright__browser_run_code_unsafe` with its absolute path as `filename`). Positions follow the map viewport, so a check must move the map to its device before reporting.
