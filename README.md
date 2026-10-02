# Fleetline: real-time geo-tracking and private geozone alerts

Fleetline ingests positions from a moving fleet, streams them to dashboards, and alerts every open session of a user when one of their devices reports from inside one of their circular geozones. FastAPI, PostgreSQL 18 + PostGIS, Kafka and NATS; api, gateway and processors scale by replicas.

On one laptop it sustains **300,000 devices reporting every 5 s (60,000 reports/s) with p95 delivery of 384 ms**, every report reconciled by count and identity checksum on every dashboard session ([run](evidence/capacity/rung-300k-final.json)).

**Design notes** (the brief, every decision with the options weighed, diagrams, and the measurements behind them): **https://claude.ai/artifact/5qGpLJVYQAY62WckcMui21**

## Run

```sh
cp .env.example .env
docker compose up --build -d --wait
```

Open **http://127.0.0.1:8097** (API docs at `/docs`). Prometheus is on `127.0.0.1:9097`, Grafana on `127.0.0.1:3097`; nothing else is published. Stop with `docker compose stop`. Tooling outside Docker uses [uv](https://docs.astral.sh/uv/): `uv sync --frozen`.

| Service | Role |
| --- | --- |
| `edge` | HAProxy: `/ws` to gateways, `/loadgen` to the load generator, everything else to api |
| `api` ×4 | ingest (`/ingest`, `/locations`) into Kafka, REST |
| `processor` ×8 | Kafka consumer group: dedup, PostGIS matching, persistence, publishing to NATS |
| `gateway` ×2 | dashboard WebSockets, fed by NATS |
| `loadgen` | runs the load generator on request from the dashboard |
| `db`, `kafka`, `nats` | PostgreSQL 18 + PostGIS, Kafka 4.1 (24 partitions), NATS 2.11 |
| `migrate` | `alembic upgrade head` before anything serves |

## Try it

Open two tabs as `alice` and one as `bob` (the avatar switches the mock user). As Alice, add a zone on the map. In the **Load generator** panel choose a device count (up to 500,000), report interval, duration and spread, and start it: vehicles drive around the map centre and report through the edge like a real fleet. Both Alice tabs get the zone's alerts; Bob sees the fleet but never Alice's zones or alerts.

The same generator runs from the command line, and `scripts/drive.py` checks the whole contract against a running stack:

```sh
uv run python generator.py --devices 100000 --interval 5 --duration 300
uv run python scripts/drive.py
```

## Architecture

```
devices ── WS /ingest, POST /locations ─▶ edge ─▶ api ×N ──produce(key=device_id)──▶ Kafka `reports`
                                                                                      │ consumer group
dashboards ◀──────── WS /ws ──────── edge ◀── gateway ×M ◀── NATS ◀── processors ×P ──▶ PostgreSQL/PostGIS
```

- **An ack means durable in Kafka.** api replicas validate and produce each report keyed by `device_id` (idempotent producer, `acks=all`) and never wait for the database. Full windows answer HTTP with `503` and backpressure sockets.
- **One owner per device.** The key puts a device on one partition and the consumer group gives each partition one processor, so watermarks need no locks; a monotonic upsert guards against a zombie owner during a rebalance.
- **One transaction per batch, then publish, then commit offsets.** Each processor has a single writer that upserts the newest position per device, learns each device's previous watermark from the same statement, and matches every fresh sample against active zones in one PostGIS query. A crash anywhere replays from Kafka; already persisted records are re-emitted, never re-applied.
- **Geometry stays in PostGIS.** Zones are geodesic circles in metres; a conservative footprint box with a partial GiST index selects candidates and `ST_DWithin` decides, proven equal to brute force at poles, the antimeridian and boundaries.
- **Routing, not scanning.** Positions are published per map tile as NATS subjects; a gateway subscribes only to the tiles its dashboards look at, and every gateway holding a user's session receives that user's alerts.
- **The dashboard keeps up with the stream.** A Web Worker owns the socket and all positions in typed arrays; the page only draws, clustering once more than 3,000 points are visible.

## Semantics

- A sample is `(device_id, timestamp)`. Timestamps need a time zone and are kept to the microsecond; more than 30 s ahead of the server is a `422`.
- Every fresh sample inside an active zone alerts, including repeated reports from a device already inside. Duplicates and stale samples change nothing and alert nothing.
- Delivery to dashboards is at least once and live only: a rebalance can repeat a frame, and a disconnected dashboard reloads the latest map, not missed alerts.
- Zone edits apply from the next batch; an alert carries the `zone_version` it matched.
- Devices are one shared fleet; zones, occupancy and alerts are private to their owner. Users are identified by `X-User-ID` and `?user_id=`, the mock identity the task allows.

## API

| Interface | Contract |
| --- | --- |
| `WS /ingest` | Device stream: each message is an array of 1–200 reports; `ack` frames carry the durable prefix length in reports; `{"type":"flush"}` for a final count |
| `POST /locations` | One `{device_id, latitude, longitude, timestamp}` → `202 {"accepted": 1}` |
| `POST /locations/batch` | 1–200 reports → `202 {"accepted": n}` |
| `GET/POST /geozones` | List (keyset pagination) or create this user's zones; quota per user |
| `GET/PATCH/DELETE /geozones/{id}` | Owner-scoped; another user's zone is a 404 |
| `GET /devices/latest` | Fleet snapshot; optional `south,west,north,east` box (antimeridian-aware), keyset pagination |
| `GET /insights` | Fleet freshness and live occupancy of this user's zones |
| `WS /ws?user_id=` | `ready` → client `viewport` → `subscribed`, then `positions`, `inside_report`, `zones_changed`, and `resync` after the gateway's NATS connection recovers (live events published meanwhile are gone; the client reloads its snapshot) |
| `GET /loadgen`, `POST /loadgen/start`, `POST /loadgen/stop` | The load generator (the `loadgen` service, routed by the edge): devices 1–500,000, interval 1–60 s, duration 10 s–1 h, spread up to 2,000 km; `409` while a run is active |
| `/health/live` | The only health signal, used by compose and the edge. There is no readiness that depends on Kafka, PostgreSQL or NATS. A dependency outage is answered per request (`503`, or a socket close) and raised by alerts. Processors serve it and `/metrics` on port 9100, and exit when their writer has not polled Kafka for 40 s |
| `/metrics` | Prometheus exposition, per process: api and gateway on their port, processors on 9100. Not routed by the edge; Prometheus scrapes each replica |
| `GET /stats` | Fleet-wide freshness p95, ingest rate and consumer lag, read from Prometheus for the dashboard |

```json
{"type":"positions","items":[["truck-1",56.9496,24.1052,1790866800000100]]}
{"type":"inside_report","items":[{"device_id":"truck-1","latitude":56.9496,"longitude":24.1052,"timestamp":1790866800000100,"zone_id":"…","zone_version":1}]}
{"type":"zones_changed"}
```

## Resource bounds

| Bound | Default |
| --- | --- |
| HTTP request body, every route | 256 KiB (one ASGI limit; `413` before the handler sees a byte past it) |
| HTTP batch | 200 reports |
| In-flight produces per api process (HTTP and sockets) | 8,192 |
| In-flight produces per device socket | 1,024 |
| Processor batch | up to 10,000 records across the partitions a processor owns |
| NATS frame | at most 256 KiB, positions and alerts alike |
| Database pool | 5 per api process, 1 per processor (its single writer), no overflow, 1 s checkout, pre-ping on checkout |
| Statement timeout | 2 s |
| Zones per user | 1,000; radius ≤ 500 km; a zone may overlap at most 50 of its owner's active zones, so a report matches at most 51 zones per user |
| Dashboard sessions | 128 per gateway, 8 per user per gateway, 16 per user at the edge |
| Connections per replica at the edge | 4,096 (queued beyond) |
| Per-session queue / send deadline | 8 MiB / 2 s; 128 MiB queued per gateway in total, beyond which the largest backlog is evicted |
| Viewport subscriptions | 16 tiles per session, at most 4 viewport changes per second, 4 KiB per gateway frame |
| Requests per user at the edge | 600 per 10 s (`429` beyond); request headers within 10 s, keep-alive 5 s |
| Reports per device socket | 2,000 per second, burst 4,000 (token bucket; the socket is closed beyond) |

Every bound is a `GEO_` setting in `geo_tracking/settings.py`.

## Scaling out

| Tier | Here | Beyond one host |
| --- | --- | --- |
| api, gateway | `API_REPLICAS`, `GATEWAYS` behind the edge | more edges |
| processor | `PROCESSORS`, up to 24 (the partition count); KEDA on consumer lag in [`deploy/k8s`](deploy/k8s/README.md) | — |
| Kafka, NATS | single instances; replication factor and server list are settings | 3-node clusters |
| PostgreSQL | one primary, the tier that does not scale out yet | Citus sharded by Kafka partition, zones as a reference table |

Repartitioning is a new topic, never `--alter`: changing the partition count of a live topic moves devices between owners.

## Operations

Prometheus scrapes every replica, HAProxy and the Kafka, PostgreSQL and NATS exporters; Grafana opens on the Fleetline dashboard. Logs are JSON lines, and the edge's `X-Request-ID` follows a request through api and gateway logs. The alert rules are unit-tested with promtool and proven to fire by a fault drill ([evidence](evidence/alerts-drill.json)).

| Alert | Means | First look | Action |
| --- | --- | --- | --- |
| `RetentionAtRisk` | Lag exceeds 30 min of ingest. Retention is 24 h or 1 GiB per partition, whichever comes first; at 300k devices the byte cap holds roughly 2 h (an estimate from 60k reports/s at about 100 bytes each over 24 partitions, not measured), so a longer outage deletes acked reports unread | Consumer lag and partitions per processor | Add processors (up to 24). If PostgreSQL is saturated, more processors will not help: see `Statement time per second` |
| `ConsumerLagGrowing` | Processors commit slower than ingest accepts | Batch time p95, database statement time | Same as above, earlier |
| `PartitionsUnowned` | Fewer partitions assigned than exist, including none at all; those devices are frozen | `fleet_processor_partitions` by instance, `RoleDown` | Restart or scale processors; a rebalance assigns orphans within seconds |
| `FreshnessSlow` | p95 from report to published event is over 1 s | Lag, batch time, event-loop lag | Capacity: see the measured ceiling below |
| `IngestShedding` | Over 1% of HTTP reports are refused for a full window (`503 ingest_capacity`; sockets are backpressured, never refused) | Produce window fill, Kafka health | Add api replicas if the windows are full but Kafka is healthy; otherwise fix Kafka |
| `IngestWindowSaturated` | Kafka acknowledges slower than reports arrive on one replica | Kafka CPU and disk | Kafka capacity, not api |
| `BatchesFailing` | Batches roll back and replay | Processor logs, database errors | Replays are safe; fix the database |
| `DashboardsEvicted`, `GatewaySlowConsumer` | A dashboard was closed for falling behind, or NATS dropped messages for a gateway subscription | Fanout bytes per gateway | Add gateways. An evicted browser reconnects and resnapshots; on a NATS drop the gateway sends `resync` to the dashboards on that subject |
| `EventLoopLag` | A role's loop is blocked over 250 ms at p99 | CPU of that role | Add replicas of that role |
| `TargetDown` | The edge or an exporter is not answering scrapes | `docker compose ps` | Restart it |
| `RoleDown` | No api, gateway or processor replica answers at all. One lost replica is not an alert: it leaves DNS discovery, its peers absorb the load, and symptom alerts fire if they cannot | `docker compose ps`, the edge's `haproxy_*` metrics in Prometheus | Start the role; check why every replica exited (`docker compose logs`) |

## Tests and measurements

```sh
.agents/skills/drive-fleet/scripts/gate.sh
uv run python scripts/benchmark.py --devices 300000 --duration 300
```

The gate runs ruff, mypy `--strict`, vulture, the promtool rule tests, the Kubernetes manifest validation and the integration suite against real PostGIS, Kafka and NATS. The benchmark observes four dashboard sessions and passes only if every acknowledged report was committed and delivered, lag drained, p95 stayed under 1 s and memory stayed flat, under [policy v4](evidence/acceptance-policy-v4.md), declared before the runs.

### Measured results

Apple M4 Pro, Docker with 14 CPUs and 8 GB, the generator on the same machine. The table is generated from `evidence/` by `scripts/results.py` and checked by the gate.

<!-- results:start -->
| Run | Build | Reports/s | Acknowledged | Position delivery p50 / p95 / p99 | api CPU | PostgreSQL | Processors | Verdict |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 25,000 · 300 s | — | 4,999 | 1,500,000 / 1,500,000 | 28 ms / 65 ms / 135 ms | 156% | 40% | 4 × 19% | [passed](evidence/scaling-25k.json) |
| 50,000 · 300 s | — | 9,996 | 3,000,000 / 3,000,000 | 29 ms / 74 ms / 219 ms | 200% | 54% | 4 × 20% | [passed](evidence/scaling-50k.json) |
| 100,000 · 300 s | — | 19,992 | 6,000,000 / 6,000,000 | 41 ms / 196 ms / 422 ms | 234% | 92% | 4 × 19% | [passed](evidence/scaling-100k.json) |
| 100,000 · 900 s | — | 19,998 | 18,000,000 / 18,000,000 | 41 ms / 190 ms / 527 ms | 232% | 90% | 4 × 20% | [passed](evidence/baseline-100k.json) |
| 150,000 · 300 s | — | 29,993 | 9,000,000 / 9,000,000 | 53 ms / 216 ms / 653 ms | 265% | 125% | 4 × 19% | [failed: Kafka memory growth 87 MiB > 64](evidence/capacity/rung-150k.json) |
| 150,000 · 300 s, edge + 4 api + 2 gateway replicas | — | 29,993 | 9,000,000 / 9,000,000 | 71 ms / 278 ms / 510 ms | 4 × 58% | 127% | 4 × 17% | [passed](evidence/capacity/rung-150k-h1.json) |
| 200,000 · 300 s, edge topology | — | 39,940 | 12,000,000 / 12,000,000 | 310 ms / 1,393 ms / 2,093 ms | 4 × 67% | 194% | 4 × 16% | [failed: p95 over 1 s](evidence/capacity/rung-200k.json) |
| 200,000 · 300 s, grid cell instead of device GiST | — | 39,959 | 12,000,000 / 12,000,000 | 114 ms / 448 ms / 768 ms | 4 × 60% | 105% | 4 × 17% | [passed](evidence/capacity/rung-200k-h2.json) |
| 300,000 · 300 s, 4 processors | — | 59,921 | 18,000,000 / 18,000,000 | 32.4 s / 48.7 s / 51.5 s | 4 × 71% | 155% | 4 × 18% | [failed: processors commit 50k/s, lag grows](evidence/capacity/rung-300k.json) |
| 300,000 · 300 s, 8 processors | — | 59,792 | 18,000,000 / 18,000,000 | 539 ms / 3,432 ms / 5,120 ms | 4 × 69% | 193% | 8 × 13% | [failed: p95 over 1 s, Kafka memory growth](evidence/capacity/rung-300k-p8.json) |
| 300,000 · 300 s, 6 api replicas, 8 processors | — | 58,688 | 17,669,957 / 18,000,000 | 100.0 s / 100.0 s / 100.0 s | 6 × 79% | 178% | 8 × 18% | [failed: api readiness probed Kafka; saturated replicas left rotation](evidence/capacity/rung-300k-h5.json) |
| 300,000 · 300 s, framed `/ingest`, PostgreSQL 18 upsert-returns-watermark, Prometheus policy v4 | — | 59,935 | 18,000,000 / 18,000,000 | 249 ms / 753 ms / 1,155 ms | 4 × 61% | 119% | 8 × 14% | [passed](evidence/capacity/rung-300k-s1.json) |
| 500,000 · 300 s, librdkafka producer | — | 99,682 | 30,000,000 / 30,000,000 | 88.5 s / 100.0 s / 100.0 s | 4 × 64% | 171% | 8 × 17% | [failed: processors commit 58k/s](evidence/capacity/rung-500k.json) |
| 500,000 · 300 s, one lane per partition | — | 99,757 | 30,000,000 / 30,000,000 | 66.9 s / 95.4 s / 99.5 s | 4 × 79% | 282% | 8 × 26% | [failed: PostgreSQL at its CPU limit; generator late](evidence/capacity/rung-500k-s4.json) |
| 300,000 · 300 s, after the security lens | — | 59,911 | 18,000,000 / 18,000,000 | 375 ms / 981 ms / 1,448 ms | 4 × 52% | 136% | 8 × 21% | [failed: p95 over 1 s for alerts](evidence/capacity/rung-300k-secured.json) |
| 300,000 · 300 s, one lane per partition, 1 transaction per processor | — | 59,915 | 18,000,000 / 18,000,000 | 564 ms / 1,335 ms / 1,939 ms | 4 × 52% | 124% | 8 × 17% | [failed: ingest clamped future timestamps; a false watchdog kill](evidence/capacity/rung-300k-tx1.json) |
| 300,000 · 300 s, the same, repeated | — | 59,918 | 18,000,000 / 18,000,000 | 574 ms / 1,515 ms / 2,113 ms | 4 × 53% | 134% | 8 × 18% | [failed: ingest clamped future timestamps](evidence/capacity/rung-300k-tx1-repro.json) |
| 300,000 · 300 s, one lane per partition, 4 transactions per processor, exact timestamps | — | 59,823 | 18,000,000 / 18,000,000 | 421 ms / 1,082 ms / 1,598 ms | 4 × 54% | 149% | 8 × 22% | [failed: p95 over 1 s](evidence/capacity/rung-300k-exact-timestamps.json) |
| 300,000 · 300 s, one writer per processor | — | 59,917 | 18,000,000 / 18,000,000 | 314 ms / 876 ms / 1,286 ms | 4 × 51% | 125% | 8 × 18% | [passed](evidence/capacity/rung-300k-one-writer.json) |
| 300,000 · 300 s, one writer, 100 ms fetch window | — | 59,893 | 18,000,000 / 18,000,000 | 322 ms / 1,071 ms / 1,583 ms | 4 × 55% | 128% | 8 × 18% | [failed: p95 over 1 s; window reverted](evidence/capacity/rung-300k-batch-window.json) |
| 300,000 · 300 s, one writer, build 10afc79, repeat 1 | — | 59,899 | 18,000,000 / 18,000,000 | 597 ms / 1,501 ms / 2,111 ms | 4 × 54% | 153% | 8 × 21% | [failed: p95 over 1 s; the laptop was also running a browser and other apps, and held 566,143 leftover devices](evidence/capacity/rung-300k-head-1.json) |
| 300,000 · 300 s, one writer, build 10afc79, repeat 2 | — | 59,915 | 18,000,000 / 18,000,000 | 431 ms / 1,218 ms / 1,678 ms | 4 × 55% | 156% | 8 × 19% | [failed: p95 over 1 s; the laptop was also running a browser and other apps, and held 566,143 leftover devices](evidence/capacity/rung-300k-head-2.json) |
| 300,000 · 300 s, submission build, idle laptop after a reboot | f1df15b | 59,906 | 18,000,000 / 18,000,000 | 140 ms / 384 ms / 680 ms | 4 × 48% | 105% | 8 × 18% | [passed](evidence/capacity/rung-300k-final.json) |
<!-- results:end -->

**The submission build passes 300,000 devices**: p50 / p95 / p99 of 140 / 384 / 680 ms. The failing 300k rows record the path to it; the late ones ran on a laptop also busy with a browser and other apps, and two of them on 566,143 devices left over by a killed run, so compare only runs made back to back. The generator sends frames of 100 reports over 64 sockets, so this is 60,000 reports/s, not 300,000 device connections. **The ceiling of this laptop lies between 300,000 and 500,000 devices**: at 500k the host was saturated and the generator itself fell behind its schedule.

### Failure campaign

Each scenario runs 100,000 devices for 180 s and breaks one container about 60 s in ([fault policy](evidence/acceptance-policy-fault-v2.md)). No scenario lost an acknowledged report.

| Scenario | Devices resent | Delivery p95 | Dashboards | Verdict |
| --- | ---: | ---: | --- | --- |
| NATS restart | 0 | 143 ms | stayed open and silently missed events | [failed](evidence/faults/s1-nats-restart.json) |
| NATS restart, gateway sends `resync` | 0 | 165 ms | all four resynced at 60.9 s and resumed | [passed](evidence/faults/s1-nats-restart-resync.json) |
| Kafka restart | 201,578 | 8,973 ms | open, nothing missed | [passed](evidence/faults/s2-kafka-restart.json) |
| Processor SIGKILL, restarted at 90 s | 0 | 227 ms | open; 648 events re-emitted by replay | [passed](evidence/faults/s3-processor-kill.json) |
| Processor paused 30 s (zombie) | 0 | 201 ms | open; 805 re-emitted | [passed](evidence/faults/s4-processor-pause.json) |
| PostgreSQL restart | 0 | 233 ms | open, nothing missed | [passed](evidence/faults/s5-db-restart.json) |
| api replica SIGKILL | 702 | 184 ms | open, nothing missed | [passed](evidence/faults/s6-api-kill.json) |
| gateway replica SIGKILL | 0 | 190 ms | the one on it reconnected and resumed | [passed](evidence/faults/s7-gateway-kill.json) |
| edge restart | 3,567 | 156 ms | all reconnected and resumed | [passed](evidence/faults/s8-edge-restart.json) |
| half the processors stopped at 60 s, started at 120 s (two rebalances) | 0 | 207 ms | open; 1,089 re-emitted | [passed](evidence/faults/s9-processor-rebalance.json) |
| the same, revoked lanes finish their batch before handing over | 0 | 185 ms | open; 48 re-emitted | [passed](evidence/faults/s9-processor-rebalance-graceful.json) |
| processor killed, one writer per processor | 0 | 186 ms | open or resumed | [passed](evidence/faults/s3-processor-kill-one-writer.json) |
| processor paused (zombie), one writer per processor | 0 | 120 ms | open or resumed | [passed](evidence/faults/s4-processor-pause-one-writer.json) |
| half the processors stopped then started, one writer per processor, handover restarts stages past the deadline | 0 | 139 ms | open or resumed | [passed](evidence/faults/s9-processor-rebalance-batch-window.json) |

## Limits

- **Ingestion is unauthenticated**: anyone who reaches the edge can report any `device_id`. A real deployment needs per-device credentials checked against each report. Zone, rate and session bounds limit the damage meanwhile.
- One Kafka broker, one NATS server and one PostgreSQL: the stack shows scale-out of the stateless tiers, not broker or database replication.
- No movement history and no alert replay after a disconnect.
