---
title: Scale architect, 500k to 1M
date: 2026-10-02
kind: research
---

# Scale architect (raw)

Triage in [ideas](../ideas.md).

I found three things that change the plan.

1. **The decided Citus shape gives no gain.** `MATCH_SQL` never touches `device_latest`; it only joins the batch's points with `geozones`. So matching would stay on the coordinator. And every batch would become a two-phase commit (2PC) across all shards.
2. **The processor ceiling is not database round trips.** In `rung-300k-p8-progress-statements.txt` the three batch statements take 130 ms per 2,000 rows, about 65 µs of statement time per record. The other round trips (`BEGIN`, ping, progress, advance, `COMMIT`) cost about 1–2 ms per batch. Batch size is not the lever. Per-row statement cost and parallelism into the database are.
3. **The fixture sets much of the load.** One 300 km zone covers every device, and three sessions watch the whole world. So each report becomes about 5 delivered items: 60k alerts/s at 300k devices and 1M items/s at 1M.

## CPU per report today (300k-p8, 60k reports/s, medians from `resources.containers`)

| Tier | Cores | µs per report |
|---|---|---|
| api ×4 | 2.75 | 46 |
| db | 1.93 | 32 |
| processor ×8 | 1.06 | 18 |
| gateway ×2 | 0.72 | 12 |
| edge | 0.45 | 7.5 |
| kafka | 0.43–1.0 | 7–17 |

The total is about 125–140 µs per report. Without changes, 500k (100k reports/s) needs about 13 cores and 1M needs about 26, plus the generator, out of 14. Reaching 1M needs about 2.5× less CPU per report overall, and the api gives most of it.

Profile first, because the api split below is my estimate: `py-spy record --native` inside a Linux container with `cap_add: SYS_PTRACE`, which avoids the macOS root problem.

## Ranked changes

### 1. Framed `/ingest`: one WebSocket message carries an array of reports (protocol change)
- **Mechanism:** each WebSocket message has a fixed cost: the ASGI receive, the frame, the semaphore, the `Window`, `acks.issue`, the coroutine hops and the counter. I estimate that at about 25 of the 46 µs. With 50 reports per frame it falls to about 0.5 µs per report. Validate the whole frame in one `msgspec.json.decode(type=Frame)` call (a tagged union of `Report` and `Flush`; `Meta(tz=True)` and `pattern` give the same rejections), about 1 µs per report against roughly 6 µs for orjson plus pydantic. The future-timestamp check stays.
- **Gain:** api 46 → about 20 µs per report, roughly 2.3×. The generator does the same work per report and gains a similar factor.
- **Files:** `ingest.py` (`stream`), `schemas.py`, `generator.py`, `scripts/ack_sink.py`, `README.md`, tests.
- **Risk:** each report still becomes its own Kafka record keyed by `device_id`, so ownership is unchanged. An invalid frame must be rejected whole (code 1007, nothing admitted) so the acknowledged prefix stays exact. Take `admit(len(frame))` once per frame.
- **Measure:** api CPU-ms per report at 300k, compared with `rung-300k-p8`.

### 2. confluent-kafka producer with `partitioner=murmur2_random`, lz4, linger 20 ms
- **Mechanism:** aiokafka's `send` is mostly Python per record (the murmur2 loop, accumulator, futures), which I estimate at 12–15 µs. librdkafka's `produce()` costs 1–2 µs. Call `poll(0)` from the loop so delivery callbacks run on the loop thread. Linger also matters today: 15k reports/s per replica × 5 ms ÷ 24 partitions is about 3 records per batch. At 20 ms there are about 4× fewer batches for the broker, which needs about 2 of its 2 cores at 200k/s as things stand.
- **Gain:** api 20 → about 8 µs per report after change 1. Broker CPU about −40% (estimate).
- **Files:** `bus.py`, `ingest.py` (`send`, `settled`), `api/app.py` lifecycle, `pyproject.toml`, `uv.lock`.
- **Risk:** librdkafka's default partitioner (`consistent_random`) sends devices to different partitions, which gives one device two owners. Add a test that compares partitions for 100k ids against aiokafka's `DefaultPartitioner`. Keep `acks=all` and idempotence.
- **Measure:** the same rung; also broker CPU and records per batch.

### 3. Database cost per row: H5, then `COLLATE "C"` on `device_id`
- **Mechanism:** H5 removes the watermark SELECT, 47.6 of the 130 ms (37%). Primary-key probes on `varchar` use the image's `en_US.utf8` collation, so every btree comparison calls `strcoll`. The SELECT cost 24 µs per primary-key lookup, which is far too much for a warm btree. Also drop `pool_pre_ping` in the processor: there are 18,434 `BEGIN` and 9,217 `ROLLBACK` for 9,017 batches.
- **Gain:** about 32 → 20 µs per row from H5, and maybe 15 with the collation (unproven). At 1M that is 3–4 database cores.
- **Files:** `migrations/versions/006_*.py`, `models.py`, `db.py`.
- **Risk:** sort order of `device_id` changes; no guarantee depends on it.
- **Measure:** `mean_ms / rows` per statement on a table holding the same rows (the debris lesson).

### 4. Partition lanes in the processor, plus uvloop
- **Mechanism:** run one transaction per owned partition, concurrently, with ordering kept inside each partition (DB commit → publish → offset). This is safe because one owner holds the partition. Going from 4 to 8 processors showed concurrency is the lever: p50 fell from 32 s to 0.54 s and DB use rose from 1.55 to 1.93 cores. Per-partition transactions are also exactly the single-shard shape Citus needs. `processor.main` uses plain `asyncio.run`, so it misses uvloop.
- **Gain:** latency and headroom until the database saturates. Raise DB `cpus` from 3 to 5 once changes 1–2 free cores. uvloop: about −15% processor CPU.
- **Files:** `processor.py`, `settings.py`, `docker-compose.yml`.
- **Risk:** the consumer_progress advance must stay inside each partition's transaction, and offsets must commit per partition in order.
- **Measure:** batch p95, freshness p95, DB cores.

### 5. Zone candidate filter in the processor
- **Mechanism:** keep an in-memory cell → zone superset index, guarded by a `zone_epoch` row that is read in the batch transaction (reload on change). Only candidate samples go to the exact `ST_DWithin`.
- **Gain:** match is 18% of statement time. The benchmark fixture gives about 0, because every sample hits the 300 km zone. A realistic fleet saves most of it.
- **Risk:** the superset must be proven like `zone_footprint`. The epoch read keeps the current snapshot semantics.
- **Files:** `spatial.py`, `processor.py`, a migration.

### 6. Generator and observers: measure, then frame
- The sink run shows 80k/s at 400k devices on 8 processes. At 1M, I estimate about 50 µs per report: roughly 10 host cores for the generator, plus about 400k items/s into alice's single Python observer.
- Return `getrusage` CPU from each `shard_main` and from the observers.
- Run the sink at 1M before any server rung, otherwise the harness may be what fails.

### 7. Citus, built only past one primary
- **When:** on this laptop Citus adds no cores, only overhead. At about 15–20 µs per row, one primary handles 1M on 4 cores. Citus pays off around 3M devices, or for write isolation.
- **Shape:**
  - Use Citus with metadata synced to the workers.
  - Distribute `device_latest`, with key `(kafka_partition, device_id)`, and `consumer_progress` by `kafka_partition`. Set the shard count to the partition count; set it to 48 before any data, since repartitioning rewrites the table.
  - Keep `geozones` as a reference table.
  - Processors connect to the worker that holds their shards. Each partition's transaction then routes to one shard: no 2PC, and matching runs on that worker.
  - Snapshots and occupancy run as multi-shard queries through the coordinator.
- **Risk:** check that the Citus version supports PostgreSQL 18 and passes `RETURNING old.*` through to shards; otherwise H5 and Citus conflict.
- **Provable locally:** reconciliation, single-shard routing on the workers, and per-worker rows and CPU at about 1/N. Throughput gain cannot be proven on one machine.

## Rejected
- **`synchronous_commit=off`:** this weakens a guarantee. A database crash would lose batches whose Kafka offsets are already committed. Commits are about 140/s, so fsync costs at most about 3% per batch. `pg_stat_statements` misses that time, so measure it with `track_wal_io_timing` and `pg_stat_io`.
- **COPY:** it cannot do `ON CONFLICT … RETURNING`, and unnest arrays are already sent in binary.
- **Bigger batches:** the fixed cost is under 2% of a batch.
- **Per-report Prometheus counter:** about 0.5 µs (under 1%); change 1 removes it anyway.
- **httptools:** not used by the WebSocket path.
- **Packing several devices into one Kafka record:** breaks the per-device key invariant, and the processors are mostly idle.
- **Partitioning `device_latest` on one node:** under 10% gain. It is only worth doing as part of Citus.

## Projection
Applying changes 1–4 gives roughly:

| Tier | µs per report |
|---|---|
| api | 8 |
| db | 15–20 |
| processor | 10 |
| gateway | 12 (fixture fanout) |
| edge | 4 |
| kafka | 6 |

That is about 55–60 µs per report:
- **500k:** about 6 cores, comfortable.
- **1M:** about 11–12 server cores plus 2–3 for the framed generator, at the edge of 14. The 3 world-viewport sessions set the gateway and observer cost.

I did not write this to the vault because the task was read-only. You may want to file it in the `geo-tracking-test-task` unit next to `ideas.md` and `decisions.md`.
