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

## Commit gate

`.agents/skills/drive-fleet/scripts/gate.sh` runs everything a commit needs, stopping at the first failure. Run it from the repository root, with the stack up for the suite.

## Failure campaign

`.agents/skills/drive-fleet/scripts/fault-campaign.sh` runs nine 100k × 180 s scenarios: restarts of NATS, Kafka, PostgreSQL and the edge; a processor kill and a processor pause (a zombie); an api kill and a gateway kill; half the processors stopped at 60 s and started at 120 s (s9, a rebalance each way). `ONLY="s3 s9"` runs a subset and `SUFFIX=-name` keeps earlier results beside the new ones. Each fault fires at 60 s, and a recovery at 90 s where one is needed. Container names come from `docker compose ps`; replicas are not numbered 1..N after scaling, so check the targets exist before a run. It takes about 45 min, so run it in the background with a long timeout. Fault runs are judged by `evidence/acceptance-policy-fault-v2.md`: durability from PostgreSQL rows, not counters.

`scripts/alert-drill.py --output evidence/alerts-drill.json` proves the alerts fire, not only that promtool accepts them. Under a 5k-device load it stops the database, then every processor, kills every api and stops the edge. For each phase it waits for the expected alert, keyed `name` or `name:job`, to reach `firing` on Prometheus, then restores the stack. It takes about 12 minutes and needs no alert firing at the start. A rule that passes promtool can still never fire: for example, `sum()` over series that vanished, or `up == 0` for a target that DNS discovery dropped.

When PostgreSQL does not hold a device's newest report, the benchmark keeps the rows and writes `<output>.mismatch.json` (device, stored and expected timestamp, partition). Before blaming the processor, split the pipeline at Kafka: `scripts/kafka-latest.py <prefix> <start_ms> <end_ms>` prints the newest timestamp per device in that window, with partition and offset. Run it inside the network: `docker compose run --rm -T --no-deps -v "$PWD/.agents/skills/drive-fleet/scripts:/scan" --entrypoint python tests /scan/kafka-latest.py …`. PostgreSQL equal to Kafka means ingest changed the data; Kafka equal to the generator means the processor lost it.

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
