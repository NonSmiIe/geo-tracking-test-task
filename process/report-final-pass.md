---
title: "Fleetline final pass: cleanup, 168 tests, runtime check"
date: 2026-10-02
kind: report
---

# Fleetline final pass: cleanup, 168 tests, runtime check


The last pass before hand-off is done. The backend was cleaned up, the suite grew from 75 to 168 tests (97% line coverage), and the rebuilt stack was checked end to end. One load-generator bug was found and fixed. HEAD is `cf78533`, pushed, and the gate is green. Links: [handoff-2026-10-02](handoff-2026-10-02.md), [decisions](decisions.md), [loop-ledger](loop-ledger.md).

## Cleanup

- **Subject naming:** `geo_tracking/subjects.py` owns all of it and caches one subject per tile. `tiles.py` and the copy of the naming in `bus.py` are gone.
- **Processor:**
  - forwards the raw Kafka bytes for positions and dedups inside the read loop;
  - has a single `reset()`;
  - the watchdog runs on the monotonic clock;
  - three unused metrics were deleted.
- **Gateway:** decodes each frame once and counts bytes once per delivery.
- **Ingest:** callers use the produce window directly. `publish` was renamed `produce`.
- **Topic creation:** one `ensure_topic`, through the confluent admin client, that returns the topic ID. The aiokafka admin path was deleted.
- **Shared constants and API helpers:** the shared constants (`ID_LENGTH`, `MAX_RADIUS_M`, `RIGA`) are each defined once. The API helpers are `page()`, `present()`, `lock_owner()` and `zones_changed()`.
- **Build and deploy:**
  - one multi-stage `Dockerfile` with `runtime` and `test` targets; `Dockerfile.test` was deleted;
  - Compose uses the `x-infra` and `x-app` anchors;
  - k8s sets the WebSocket size limit per service.
- **Docs:** README rewritten as contract, guarantees, results and limits, with the design page linked. PRODUCT.md, DESIGN.md and `scripts/explain.py` were deleted.

## Tests

| Before | After |
| --- | --- |
| 75 tests, 95% coverage | 168 tests, 97% coverage (pytest-cov with greenlet concurrency, so async SQLAlchemy is traced) |

New tests cover:
- report, geozone and viewport validation;
- subject tiles and quadkeys, plus a randomised check that 400 viewports cover their points within the subscription limit;
- `BodyLimit` (declared length, chunked overflow, overflow after the response started);
- results formatting;
- NATS publish retry and the deadline;
- a lost offset commit still settles;
- gateway invalid messages and size limit;
- session caps per user and per gateway;
- `/stats` against a fake Prometheus (TTL, NaN→null, 503);
- ingest shedding with `Retry-After`;
- loadgen state and process sizing.

Still uncovered: process entrypoints, signal handlers and NATS reconnect callbacks.

## Runtime check

- **End-to-end drive:** rebuilt stack from the new Dockerfile; `drive.py` passed.
- **Loadgen through the edge, 100k devices / 5 s:** 1,200,000 of 1,200,000 acked at 19,982/s, freshness p95 88 ms. A second start returns 409, stop works, UI 200.
- **Bug, fixed in `cf78533`:** 20k devices at 10k/s showed freshness p95 4.3 s. The generator was the cause: processes were sized by device count only (40k each), so this load got one process, which tops out near 4k/s. `Load.processes` now sizes by rate as well (4k/s per process, at most 8). After the fix: 600,000 of 600,000 acked at 9,993/s, freshness p95 83 ms.
- **Not re-measured:** the 300k benchmark. The laptop was in use. README and design page still cite `rung-300k-final` (p95 384 ms, idle laptop, before this cleanup).

## Process record

The agent-written documents of this unit are exported to the repository under `process/`, without the loop charter (the author's own) and without the passport.
