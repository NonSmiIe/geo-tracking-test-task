---
title: Claims auditor, first pass
date: 2026-10-02
kind: audit
---

The claims are mostly sound: every headline number in the README and the design page matches its evidence file. Beyond the 1M sentence you already knew about, I found 4 more FALSE claims about how the system scales and 13 smaller wording or rounding faults.

I audited the last commit (`86f209a`), not the working tree. Someone else is editing files right now (`__main__.py` deleted, a `gateway_app.py` split, a new `kafka_replication` setting), so the working tree doesn't match what was measured.

## FALSE

**F1. Design page, §11 "Beyond the laptop":** "The design does not change; only the counts do." / "API workers, processors and NATS are stateless and simply multiply." **Confirmed false.**
- There is one `db` service, and every processor writes `device_latest` on that one primary.
- The proposed fix (hash-partition `device_latest`) keeps one primary and one write-ahead log, so it adds no writers.
- `api` publishes a single host port (`127.0.0.1:8097:8000`) with no load balancer, so `docker compose up --scale api=N` fails on the port. The 4 uvicorn workers live in one container capped at `cpus: 4`.
- 24 partitions cap the processors at 24.
- NATS is a single server, and clustering it needs configuration.
- **Fix:** "1M needs a load balancer in front of N API replicas, a NATS cluster, a replicated Kafka cluster with more partitions, and multiple PostgreSQL primaries (e.g. Citus or app-level sharding by `device_id`)."

**F2. Design page, §11:** "Kafka needs more brokers, which also adds the replication a single broker lacks." `bus.ensure_topic` hard-codes `NewTopic(..., partitions, 1)`, and compose sets the offsets topic replication factor to 1. Adding brokers changes neither. **Fix:** replication needs a configurable replication factor plus `min.insync.replicas`, which is the uncommitted edit in progress.

**F3. Design page, §02 diagram:** "add api workers, partitions or processors without touching code." `ensure_topic` only creates the topic when it is missing, so changing `GEO_KAFKA_PARTITIONS` does nothing to an existing topic. Increasing partitions by hand remaps `hash(device_id) mod N`, so one device's reports end up on two partitions with two owners. That breaks the one-owner invariant in CLAUDE.md. **Fix:** "processors up to the partition count; changing partitions needs a drained topic or a new one."

**F4. README, Architecture opening paragraph:** "Every component that carries load scales horizontally." PostgreSQL, Kafka, NATS and the API container are all single instances, and the README's own Limits section says so. **Fix:** "Processors scale horizontally up to the partition count; the API scales by worker processes in one container."

**F5. README, Resource bounds:** "In-flight produces per API worker | 8,192" and "Each producer has a bounded in-flight window." `Ingest.publish` checks `produce_window` only for HTTP. `Ingest.stream` (the `/ingest` socket, the default transport) increments `inflight` without checking it, so the real per-worker bound is sockets × 1,024. **Fix:** enforce the window in `stream`, or reword to "8,192 for HTTP; 1,024 per socket".

**F6. README, Architecture diagram:** shows processors publishing `fleet.zones.<user>`. In the code, the API's geozones CRUD calls `zones_changed`. **Fix:** draw the arrow from the API.

**F7. README, Run section:** "Application containers run … with memory and CPU limits." `migrate` inherits the shared `x-app` block, which sets no limits. **Fix:** add `mem_limit`/`cpus` to `x-app`.

**F8. README, Architecture → Fanout:** "gateways forward the bytes without decoding." `Connection.write` calls `data.decode()` on every frame for every recipient before `send_text`. **Fix:** "without parsing JSON".

**F9. Design page, §10 "Measurements":** "the worst p95 was 196 ms." That is positions only. Alert p95 at 100k·300 s is 198 ms (`scaling-100k.json` `delivery_latency.inside_report.p95_ms`). The "Max latency" column is also positions only; alert maxima are 582 / 853 / 1,535 / 1,731 ms.

**F10. Design page, CPU chart:** totals 3.27 and 4.64 are sums of rounded parts. The raw medians sum to 3.26 and 4.65.

## UNSUPPORTED

**U1. Design page, hero:** "190 ms p95 from scheduled send to the browser." The observers in `benchmark.observe` are Python `websockets` clients, not browsers. **Fix:** "to a dashboard WebSocket client".

**U2. README, Measured results:** "Docker with 14 CPUs and 8 GB shared by the stack, the generator…" No v3 result file records the host or the Docker VM size. The benchmark and generator run under `uv` on the macOS host, not inside Docker. **Fix:** record `docker info` NCPU/MemTotal in each result, and say the generator runs on the host.

**U3. README, intro:** "reconciled report by report." The benchmark actually checks a count plus an order-independent 64-bit sum checksum per session. **Fix:** "by count and identity checksum".

**U4. README, Measured results table:** the "Delivery p50/p95/p99" column is positions only. **Fix:** label it "Position delivery", or add the alert columns.

**U5. Design page, §10:** "batch p95 rose from 10 to 27 ms." `processing_ms` is a rolling window of the last 4,096 batches (`metrics.Metrics`). At the same 100k load, the 900 s run shows 12.3–12.9 ms. **Fix:** record a whole-run histogram, or drop the claimed trend.

**U6. Design page, §11:** "That is an upper bound, since measured growth was sub-linear." Growth is accelerating, not flattening:
- PostgreSQL CPU went ×1.36 from 25k to 50k, then ×1.70 from 50k to 100k.
- Batch p95 went 10 → 13 → 27 ms.
- The fixture flatters PostGIS: 99 of the 100 zones sit at (−40, −60), so every report matches exactly one zone.

The page itself names write-ahead-log and GiST costs as coming before CPU. **Fix:** call it an extrapolation. To support it: run 150k/200k rungs with realistic zone density and measure PostgreSQL CPU and WAL bytes/s.

**U7. Design page, §08:** "about 2.6 MB/s per tab." Nothing records it. Per-worker `bytes_enqueued` deltas in `baseline-100k.json` suggest roughly 1.7 MB/s for positions only and roughly 5.6 MB/s for an owner tab (positions plus alerts). **Fix:** measure it per session in the benchmark.

**U8. Design page, §03:** "I reproduced it on the running stack", "400 / 400 → 503", "18,000 alerts to each owner session." Nothing in `evidence/` records these. `test_dense_overlap_never_rejects_ingestion` sends 200 sequential reports and asserts 9,000 alerts. **Fix:** commit the probe output, or quote the test's numbers.

**U9. Design page, §09 decision table:** "20k/s on one node: yes" for Redis Streams, RabbitMQ and NATS JetStream. None of these was measured. **Fix:** mark them "not measured".

**U10. README, Architecture:** "Kafka moves partitions to the survivors when a processor dies" (and the design's hand-over story). Every test runs one processor, and no run kills a processor mid-load. The claim is Kafka's documented behaviour, but this system never exercised it. **Fix:** kill one of 4 processors during a benchmark and reconcile.

The 150k rung (`capacity/rung-150k.json`) failed only `kafka-1_memory_stable`, so "100k is the measured envelope" stays true.

## Counts (README + design page)

| Class | Count |
|---|---|
| MEASURED (all checked numbers match exactly) | 27 |
| TESTED | 28 |
| TRUE BY CODE | 65 |
| ESTIMATE (correctly labelled) | 3 |
| UNSUPPORTED | 10 |
| FALSE | 10 |

Counts are approximate where one sentence holds several claims.

The audited snapshot is at a scratch snapshot of HEAD.
