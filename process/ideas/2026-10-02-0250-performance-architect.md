---
title: Performance architect, 100k evidence
date: 2026-10-02
kind: research
---

# Fleetline capacity hypotheses, ranked (read-only analysis, 2026-10-02)

I made no file changes and ran nothing. Every number below is from `evidence/*.json`.

## What the numbers say first

- **The API is busier on one core than the container total shows.** At 100k devices, `reports_ingested` per uvicorn worker is 4.9M, 5.2M, 10.6M and 9.1M (a 2.2× spread). Worker `201d5851` also queued 9.4 of the 19.2 GB of dashboard frames. Its loop-lag p99 is 20 ms; the others sit at 5–9 ms. The container uses 2.32 of 4 cores, but its hottest event loop is close to one full core.
- **Dashboard fanout takes a large share of API CPU.** The four probe sessions receive about 100k items/s (3 whole-world viewports × 20k positions, plus 2 × 20k alerts). That is 21 MB/s in about 24k frames/s, roughly 2.7 items per frame. All of it runs on the same event loops as ingest.
- **The processors are waiting on the database, not using CPU.** Each one runs about 155 batches/s at a p50 of 6.4 ms, so it is busy almost the whole second, on about 34 records per batch. CPU stays at 0.20 from 25k to 100k. Batches grow on their own as load rises, so the processors are not the wall.
- **Postgres is the wall for 1M devices.** Its marginal cost is (0.92−0.40)/15k ≈ 35 µs per report, while batches/s rose only 30%. At 200k reports/s that projects to about 7 cores against a limit of 3.

## H1. Split the dashboard gateway out of the API and even out the ingest workers
- **Change:** run `gateway.py` as its own compose service with its own CPU budget, so `api` only serves ingest and CRUD. Replace the single shared socket behind `uvicorn.run(workers=4)` in `__main__.py` with one socket per worker using SO_REUSEPORT, so the kernel spreads connections evenly. Raise the worker count to 6–8.
- **Why:** the 2.2× imbalance and the 21 MB/s of fanout both land on one event loop (`201d5851`).
- **Expected gain:** the API ceiling rises about 1.5–2× before any per-report saving.
- **Cost and risk:** low. No invariant is touched, because the gateway only consumes NATS.
- **Measure at 150k:** keep if the per-worker `reports_ingested` max/mean is at most 1.2, the hottest worker's loop-lag p99 falls, and API plus gateway CPU-ms per report does not rise.

## H2. Remove the GiST index from `device_latest` so upserts become HOT
- **Change:** drop `device_latest_position` and set `fillfactor` to about 70. With no indexed column changing, the upsert becomes a HOT update: no GiST or btree inserts, little bloat. Viewport snapshots (`devices.py`) and `insights.py` then filter on a coarse integer tile column. A device rarely crosses a tile, so updates stay mostly HOT.
- **Why:** every report today changes an indexed geometry, so each update rewrites the GiST and the primary-key btree. That is the most likely source of the 35 µs per report.
- **Expected gain:** 30–50% less database CPU per report. This is a guess until the split is measured.
- **Before building:** read `pg_stat_user_tables.n_tup_hot_upd/n_tup_upd`. Turning on pg_stat_statements needs a change to the database config, so it waits for the current run to finish.
- **Risk:** snapshot latency at 1M rows. The monotonic `WHERE` clause is unchanged. The zone-matching GiST on `geozones` is untouched.
- **Measure at 200k:** keep if database CPU-ms per report falls and the snapshot p95 stays under 100 ms.

## H3. Swap aiokafka for confluent-kafka (librdkafka) in the API producer
- **Change:** `bus.kafka_producer` and `Ingest.send` move to confluent-kafka. aiokafka hashes every key with a pure-Python `murmur2` (`aiokafka/partitioner.py`) and does its accumulator work, futures and callbacks per report. librdkafka does this in C threads.
- **Invariant risk:** you must set `partitioner=murmur2_random`. The librdkafka default (`consistent_random`, CRC32) would assign devices to different partitions and break "one owner per device" on an existing topic.
- **Cost:** delivery reports have to get back onto the asyncio loop, which means polling in batches rather than one `call_soon_threadsafe` per report.
- **Expected gain:** about 15–25% of API ingest CPU.
- **Measure at 150k:** API CPU-ms per report. Run the full 900 s acceptance afterwards, because this changes the 202 durability path.

## H4. Replace pydantic `Report` with a msgspec Struct, and allow array frames on `/ingest`
These are two separate measurements.
- **msgspec:** it removes the per-report Python `field_validator`, the `astimezone` call, `record()` and the double JSON pass. It must keep exactly the same rejections as today: extra fields forbidden, timezone required, the pattern and range checks, and NaN.
- **Array frames:** let `/ingest` accept a JSON array per message, as `/locations/batch` already does. This spreads the per-message cost (ASGI dict, semaphore, future) across N reports. It also requires a new generator flag, and the ladder must label it as a protocol change.
- **Expected gain:** about 10–20% for msgspec; 20–40% for array frames.
- **Measure:** API CPU-ms per report at the same rung.

## Seeds I rate weak
- **One SQL round trip per batch, and larger batches or COPY:** batching already adapts (`processor_batch` 2000, about 34 used). Per-batch costs shrink on their own as load rises. This helps latency, not the ceiling.
- **Bundling reports per Kafka record:** it collides with the invariant that every report is keyed by its device id. H3 gets the same API saving without that.
- **uvloop:** uvicorn already loads it (`uvicorn[standard]`). The processors are not CPU-bound, so adding it there does nothing.
- **Partition and processor counts:** processors are idle on CPU, so there is nothing to gain yet.

## What saturates first
- **At 150k (30k reports/s):** the hottest API event loop, not the API container. Today it carries about 35% of ingest and about half the fanout, so it reaches one core near 30k/s. Expect loop lag to climb, the ingest window to push back, and the delivery p95 (already 190 ms) to fail, while the container still reads about 3 of 4 cores.
- **At 200k:** the API container, even with balanced workers. Postgres is at about 1.6 cores and is the next wall after that.
- **A risk in the measurement itself:** at 200k the host-side probe clients have to parse about 200k items/s. Check their CPU so the probe client's own limit is not reported as the server's.

**Before choosing between H1, H3 and H4:** run `py-spy top` on the hottest API worker at 150k (charter step 3). Only that profile shows how API CPU splits between ingest and fanout.

I did not write this to the vault. `ideas.md` is the loop's own file, so adding it there is your call.
