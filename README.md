# Fleetline: real-time geo-tracking and private geozone alerts

Fleetline is a FastAPI, async SQLAlchemy and PostgreSQL/PostGIS service that ingests positions from a moving fleet. It streams them to dashboards and pushes an alert to every open session of a user whenever one of that user's devices reports from inside one of their circular geozones. The api, the dashboard gateway and the processors scale out by adding replicas; Kafka, NATS and PostgreSQL run as single instances here (see [Scaling out](#scaling-out)). The measured envelope on one laptop is 300,000 devices reporting every five seconds (60,000 reports/s), verified by count and identity checksum on every session; see [Measured results](#measured-results).

Design notes, with the brief, the reasoning behind each decision and the measurements: **https://claude.ai/artifact/5qGpLJVYQAY62WckcMui21**

## Run

Install Docker with Compose, then from this repository:

```sh
cp .env.example .env
docker compose up --build -d --wait
```

Open **http://127.0.0.1:8097**; API documentation is at **/docs**. Compose starts:

| Service | Role |
| --- | --- |
| `db` | PostgreSQL 18 + PostGIS 3, named volume |
| `kafka` | Kafka 4.1 (KRaft, single broker), topic `reports` with 24 partitions |
| `nats` | NATS 2.11 core, live event routing |
| `migrate` | One-shot `alembic upgrade head` before anything serves traffic |
| `edge` | HAProxy, the only published port: `/ws` to `gateway`, everything else to `api`, both `leastconn` |
| `api` | FastAPI, one process per replica (`API_REPLICAS`, default 4): ingest, REST, demo |
| `gateway` | FastAPI, one process per replica (`GATEWAYS`, default 2): dashboard WebSockets |
| `processor` | Kafka consumer group (`PROCESSORS`, default 8 replicas): dedup, PostGIS matching, persistence, fanout |

Three ports are published, all on `127.0.0.1` only: the edge (`8097`), Prometheus (`9097`) and Grafana (`3097`). Application containers run as a non-root user with a read-only root filesystem, no Linux capabilities, `no-new-privileges`, and memory and CPU limits. Credentials come from `.env`. Stop with `docker compose stop`.

For the generator, benchmark and tooling, install [uv](https://docs.astral.sh/uv/) and run `uv sync --frozen` (Python 3.12+).

## Try it

Open two tabs as `alice` and one as `bob` (the avatar switches the mock user). In one Alice tab, create a zone around `56.9496, 24.1052` with a 5 km radius, then run:

```sh
uv run python generator.py --devices 100000 --duration 60
```

Both Alice tabs receive alerts for every report from inside her zone. Bob sees the same fleet but never Alice's zones or alerts. Closing one Alice tab leaves the other live. The **Запустить демо** panel starts a two-minute, six-vehicle demo around a depot zone that belongs to the current user.

`uv run python scripts/drive.py` verifies the same contract against a running stack without resetting any data.

## Architecture

```
devices ── WS /ingest, POST /locations ─▶ edge ─▶ api ×N ──produce(key=device_id)──▶ Kafka `reports`
                                                   │ zones.<user> (CRUD)                │ consumer group
dashboards ◀─────────── WS /ws ─────────── edge ◀─ gateway ×M                           ▼
                                                   ▲            processors ×P ──▶ PostgreSQL/PostGIS
                                                   └── NATS ◀── pos.<quadkey>, alerts.<user> (after commit)
```

**Ingestion is stateless and acknowledged by Kafka.** api replicas validate each report and produce it to Kafka, keyed by `device_id`, with `acks=all` and an idempotent librdkafka producer (lz4, 20 ms linger). Its partitioner is `murmur2_random`, the Java default, because librdkafka's own default would place a device on a different partition than the rest of the ecosystem; a test pins every key to the partition aiokafka's `DefaultPartitioner` computes. A device WebSocket (`/ingest`) carries frames: each message is a JSON array of 1–200 reports, decoded and validated in one msgspec pass, and a frame with any invalid report is rejected whole (close `1007`, nothing admitted). Frames exist because a per-message cost (receive, admission, wake-ups) dominated the api at one report per message; each report is still its own Kafka record keyed by `device_id`. The device receives `{"type":"ack","count":n}` frames meaning the first `n` reports sent on that socket are durable, so a device that reconnects resends from report `n`. A `{"type":"flush"}` message returns the count once every in-flight report is durable. `POST /locations` and `POST /locations/batch` return `202` after the broker ack. Each api process has one bounded in-flight window shared by HTTP and sockets, and every socket has its own smaller window. A full window answers HTTP with `503` and `Retry-After`, and stops reading a socket, so TCP backpressure reaches the device. Nothing in the ingest path waits for the database.

**The log is durable across restarts and recreation.** Kafka writes to its named volume (it had been writing to the container's `/tmp`, so recreating the container would have dropped every unprocessed report; 500 reports acked with processors stopped now survive a forced recreation). Retention is 24 h or 1 GiB per partition. Processor progress is keyed by Kafka's topic ID, not its name, so a topic recreated under the same name starts with empty progress instead of having its records mistaken for replays.

**One owner per device, without one process.** Keying by `device_id` puts every report of a device on one partition, and the consumer group gives each partition exactly one processor. That ownership removes the watermark race a shared processor pool would have, and Kafka moves partitions to the survivors when a processor dies. Each processor runs one lane per partition it owns. Lanes run concurrently, up to four open transactions per processor, because at 500k devices a single serial chain per processor (fetch, transaction, publish, offset commit) left the database at 1.7 of its 3 cores while lag grew. Each lane polls its partition for up to 2,000 records and, in one transaction:

1. discards equal `(device_id, timestamp)` duplicates, first wins;
2. upserts each device's newest position, guarded by `WHERE reported_at < excluded.reported_at` so a zombie owner during a rebalance can never move a device backwards. `RETURNING old.reported_at` (PostgreSQL 18) hands back the watermark the row had, so one statement both persists and tells which samples are fresh: those newer than that watermark, on rows the guard let through. Everything else is stale;
3. matches **every** fresh sample against active zones in one set-based PostGIS query, so a device that passes through a zone inside one batch still alerts.

After commit the processor publishes to NATS, flushes, and only then commits the Kafka offsets. On a database error it seeks back to the batch's first offsets and retries; nothing is acknowledged and lost. If NATS is unavailable after a commit, the processor keeps flushing for up to 30 s before committing offsets, then exits so that a restarted owner replays the batch. The same transaction records the highest persisted offset per partition, so a replay knows which records were committed but perhaps never published and re-emits every one of them, including each sample of a device that crossed a zone inside the batch. A crash between commit and publish, or a connection lost right after COMMIT, therefore loses no live event. A client resend lands above that offset and is discarded as stale, identical or not. Delivery is at least once: replays never change stored state, but a rebalance can repeat a live frame.

**Spatial matching stays in PostGIS.** Zone centres are `geography(Point,4326)`, so radii are metres on the spheroid. Points are always built longitude first. Each zone also has a generated `footprint` column, produced by the IMMUTABLE SQL function `zone_footprint(center, radius_m)`. It is a lon/lat box that conservatively contains the geodesic circle: the angular radius uses the smallest meridional radius of curvature plus a 1% margin; the longitude extent is `asin(sin θ / cos φ)`; the box becomes two boxes across the antimeridian and spans all longitudes when the circle reaches a pole. A partial GiST index on active footprints selects candidates with `ST_Intersects`, and exact `ST_DWithin(center, point::geography, radius_m)` decides. There are no Python distance calculations and no per-zone queries. A test asserts on real PostGIS that this path returns exactly what brute-force `ST_DWithin` returns, across random zones, poles, the antimeridian, boundary-projected points and radii from 1e-100 m to 1e100 m.

**Fanout is routed by NATS subjects, not by scanning.** Positions are published per Web-Mercator tile as `fleet.pos.<d1>.<d2>…<d8>`, a zoom-8 quadkey with one digit per token. Because quadkeys nest, a coarser tile is a subject prefix, so `fleet.pos.1.2.>` covers everything beneath it. A dashboard sends its viewport; the gateway picks the finest level whose tile cover has at most 16 tiles and subscribes to those subjects. Subscriptions are reference-counted across all of a gateway's connections, so NATS only delivers what some local viewer needs. Alerts go to `fleet.alerts.<hex(user)>` and zone changes to `fleet.zones.<hex(user)>`; every gateway holding a session of that user subscribes, which is how all of a user's sessions get every alert whichever gateway holds them. Processors serialize each frame once and gateways forward it without parsing the JSON.

**Latest positions stay cheap to update.** `device_latest` has no spatial index: an index on a column that changes on every report rules out PostgreSQL's in-place (HOT) updates. Instead each row carries a generated 0.25° grid cell with a btree index, which changes only when a device moves about 28 km. Viewport snapshots and zone occupancy pick their candidate cells with `grid_cells()` and filter exactly with PostGIS. At 200k devices it cut database CPU by 46% (194% → 105%); [an idle probe](evidence/capacity/upsert-probe.txt) shows 98.3% of updates are HOT.

**WebSocket state.** Each gateway keeps a `connection_id → Connection` registry plus `subject → connections` routes. A connection has one bounded byte queue and exactly one writer task, with a send deadline. Fanout only enqueues, so a slow socket never blocks others. A socket that overflows its queue or misses the deadline is closed alone with an explicit reason, and its siblings keep streaming. The browser opens its socket, sends its viewport, waits for `subscribed`, and only then loads the `/devices/latest` snapshot for that box. Snapshot and stream merge by microsecond timestamp, so a slow snapshot never overwrites a fresher position.

**Why Kafka and NATS, and not Redis.** The ingest log needs durable partitions with exclusive, automatically rebalanced ownership; Kafka consumer groups are exactly that, and on Redis Streams it would have to be hand-built. The live path needs interest-based routing to the gateways that hold a viewer, and NATS subject wildcards do it inside the broker; Kafka would make every gateway read and decode the whole stream.

**Database use.** Every process has one async engine with a bounded pool and a fixed checkout timeout (api replicas 5, processors 4, one per concurrent partition transaction), plus a statement timeout. Sessions are short-lived and never belong to a WebSocket. Pool exhaustion or a database fault on REST returns `503`, while ingest keeps accepting into Kafka.

## Semantics

- A sample is identified by `(device_id, timestamp)`. Timestamps must carry a time zone and are normalized to UTC with microsecond precision. A timestamp more than 5 minutes in the future is rejected with `422`, so a bad clock can hold a device's watermark ahead by at most that much.
- Every fresh sample inside an active zone produces an alert for that zone, including repeated reports from a device already inside.
- An HTTP `202` or a socket ack means the report is durable in Kafka; it says nothing about browser delivery. Live events are ephemeral to a browser: a dashboard that is disconnected while they are published misses them, and reconnecting restores the latest map, not missed alerts.
- Zone edits take effect from the next processed batch. An alert carries the `zone_version` it was evaluated against.
- Devices are a shared demo fleet; zones, occupancy and alerts are private to their owner. REST identifies the user with `X-User-ID`, the dashboard socket with `?user_id=`, which the task explicitly permits as mock identity. Ingestion is device traffic and carries no user.

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
| `GET /demo`, `POST /demo/start`, `POST /demo/stop` | Guided demo for the current user; its state lives in PostgreSQL, so any replica can answer |
| `/health/live` | The only health signal, used by compose and the edge. There is no readiness that depends on Kafka, PostgreSQL or NATS. A dependency outage is answered per request (`503`, or a socket close) and raised by alerts. Taking replicas out of rotation for it only turns a partial outage into a total one: at 300k devices a Kafka-probing readiness check timed out on every saturated replica, and the edge had no api server left. Processors serve `/health/live` and `/metrics` on port 9100. A processor whose partition lane has not polled Kafka for the publish deadline plus 10 s exits, so compose restarts it and the group rebalances |
| `/metrics` | Prometheus exposition, per process: api and gateway on their port, processors on 9100. Not routed by the edge; Prometheus scrapes each replica |
| `GET /stats` | Fleet-wide freshness p95, ingest rate and consumer lag, read from Prometheus for the dashboard |

Frames:

```json
{"type":"positions","items":[["truck-1",56.9496,24.1052,1790866800000100]]}
{"type":"inside_report","items":[{"device_id":"truck-1","latitude":56.9496,"longitude":24.1052,"timestamp":1790866800000100,"zone_id":"…","zone_version":1}]}
{"type":"zones_changed"}
```

## Resource bounds

| Bound | Default |
| --- | --- |
| HTTP request body | 256 KiB |
| HTTP batch | 200 reports |
| In-flight produces per api process (HTTP and sockets) | 8,192 |
| In-flight produces per device socket | 1,024 |
| Processor poll | 2,000 records / 50 ms |
| Database pool | 5 per api process, 4 per processor, no overflow, 1 s checkout |
| Statement timeout | 2 s |
| Zones per user | 1,000 |
| Dashboard sessions per gateway | 128 |
| Connections per replica at the edge | 4,096 (queued beyond) |
| Per-session queue / send deadline | 8 MiB / 2 s |
| Viewport subscriptions | 16 tiles per session |
| Alert frame | 1,000 items |

All are `GEO_`-prefixed settings in `geo_tracking/settings.py`.

## Scaling out

Kubernetes manifests for the stateless tiers, with the reasons behind their probes, budgets and autoscaling, are in [`deploy/k8s`](deploy/k8s/README.md), validated strictly with kubeconform.

| Tier | How it scales here | What a multi-host deployment adds |
| --- | --- | --- |
| api | `API_REPLICAS=N`, balanced by the edge | more edge capacity |
| gateway | `GATEWAYS=N`; every gateway can hold any user's session, because alerts route over NATS | nothing new |
| processor | `PROCESSORS=N`, up to the partition count (24); Kafka rebalances partitions | — |
| Kafka | one broker; `GEO_KAFKA_REPLICATION` sets the topic's replication factor | 3+ brokers with replication 3 |
| NATS | one server; `GEO_NATS_SERVERS` takes a comma-separated list | a NATS cluster |
| PostgreSQL | one primary; the write tier that does not scale out yet | sharding `device_latest` by `device_id` (Citus), zones as a reference table |

Changing the partition count of a live topic remaps devices to partitions and breaks the one-owner rule for the moving devices. Repartition by creating a new topic, switching producers to it, and switching processors once the old topic has drained.

## Load generator

`generator.py` simulates N devices across a configurable region (150 km radius around Riga by default). Every device has a heading and speed that drift each tick, which gives realistic tracks rather than jitter. Devices are sharded across processes; each process keeps a monotonic, open-loop schedule with random phases, so a slow server never lowers the offered rate. Reports go over multiplexed `/ingest` sockets by default (`--transport http` posts batches of `--batch-size` reports, 100 by default, to `/locations/batch`; `--batch-size 1` uses `POST /locations`). The generator counts scheduled, sent, acknowledged, dropped and late reports and a 64-bit identity checksum of everything sent.

```sh
uv run python generator.py --devices 100000 --interval 5 --duration 900
```

## Tests and measurements

```sh
docker compose --profile test run --build --no-deps --rm tests
uv run ruff check && uv run ruff format --check
uv run python scripts/benchmark.py --devices 100000 --duration 900
```

The tests run against real PostGIS, Kafka and NATS; each test gets its own topic, consumer group and subject prefix. They cover metre-correct containment and boundaries, high latitudes, poles and the antimeridian, footprint-versus-exact equality, coordinate order, paused and deleted zones, owner scoping, the zone quota, duplicates, stale samples and in-batch zone crossings. They also cover dense overlap (60 zones over the same devices, no rejected report), private delivery to every owner session, viewport routing and retargeting, device-socket acknowledgements, replay after a database failure, a crash between commit and publish, watermarks surviving a processor restart, slow-socket eviction and the guided demo.

The benchmark observes four dashboard sessions, each in its own process: two for the owner of a zone covering the whole fleet, one for another user with a world view, and one for that user with a small probe viewport. It reconciles every position and alert against the generator by count and identity checksum, checks that processors committed exactly what was acknowledged and that consumer lag drained, and records latency from the scheduled timestamp and container resources. The criteria were declared before the runs: [policy v4](evidence/acceptance-policy-v4.md) for runs since the Prometheus metrics, [v3](evidence/acceptance-policy-v3.md) for the ones before.

## Operations

Prometheus (`127.0.0.1:9097`) scrapes every api, gateway and processor replica, plus HAProxy, kafka-exporter (consumer group lag from the broker), postgres-exporter (`pg_stat_statements`, table stats) and the NATS exporter. Grafana (`127.0.0.1:3097`, anonymous viewer) opens on the Fleetline dashboard. Its rows answer, in order: is the pipeline keeping up, is ingest shedding load, are dashboards healthy, is the database the limit, and is the work balanced. The rules in `ops/prometheus/rules.yml` are unit-tested with `promtool test rules`.

| Alert | Means | First look | Action |
| --- | --- | --- | --- |
| `RetentionAtRisk` | Lag exceeds 30 min of ingest. Retention is 24 h or 1 GiB per partition, whichever comes first; at 300k devices the byte cap holds roughly 2 h, so a longer outage deletes acked reports unread | Consumer lag and partitions per processor | Add processors (up to 24). If PostgreSQL is saturated, more processors will not help: see `Statement time per second` |
| `ConsumerLagGrowing` | Processors commit slower than ingest accepts | Batch time p95, database statement time | Same as above, earlier |
| `PartitionsUnowned` | Fewer partitions assigned than exist; those devices are frozen | `fleet_processor_partitions` by instance, `TargetDown` | Restart or scale processors; a rebalance assigns orphans within seconds |
| `FreshnessSlow` | p95 from report to published event is over 1 s | Lag, batch time, event-loop lag | Capacity: see the measured ceiling below |
| `IngestShedding` | Over 1% of HTTP reports are refused for a full window (`503 ingest_capacity`; sockets are backpressured, never refused) | Produce window fill, Kafka health | Add api replicas if the windows are full but Kafka is healthy; otherwise fix Kafka |
| `IngestWindowSaturated` | Kafka acknowledges slower than reports arrive on one replica | Kafka CPU and disk | Kafka capacity, not api |
| `BatchesFailing` | Batches roll back and replay | Processor logs, database errors | Replays are safe; fix the database |
| `DashboardsEvicted`, `GatewaySlowConsumer` | A dashboard was closed for falling behind, or NATS dropped messages for a gateway subscription | Fanout bytes per gateway | Add gateways. An evicted browser reconnects and resnapshots; on a NATS drop the gateway sends `resync` to the dashboards on that subject |
| `EventLoopLag` | A role's loop is blocked over 250 ms at p99 | CPU of that role | Add replicas of that role |
| `TargetDown` | A scrape target is not answering | `docker compose ps` | Restart it |

Logs are one JSON object per line on every role, with `role`, `level`, `logger`, `event` and the event's context. Processors log partition assignments and revocations, failed batches with partition and offset, lost offset commits, and their exit before a restart. Gateways log evictions (user, reason, queued bytes) and resyncs. librdkafka and aiokafka are routed through the same formatter. For example, `docker compose logs --no-log-prefix processor | jq 'select(.event == "partitions assigned")'` shows every rebalance.

Repartitioning (more than 24 processors) is a new topic, not `--alter`: create `reports-v2` with more partitions, point the api at it, and let the processors drain `reports` before moving their group. Keyed ordering per device holds only within one topic.

## Measured results

Apple M4 Pro; the Docker VM has 14 CPUs and 8 GB, shared with unrelated containers, and the generator and the observing clients run on the macOS host. Every run uses 100 zones and four dashboard WebSocket clients; latency is position delivery to those clients, measured from the scheduled report time (alert latency is within 12 ms of it at every percentile in every run and is in each file). The first five rows predate the edge split, when api ran as 4 uvicorn workers in one container.

| Devices · duration | Reports/s | Acknowledged | Position delivery p50 / p95 / p99 | API CPU | Processors | Verdict |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| 25,000 · 300 s | 4,999 | 1,500,000 / 1,500,000 | 28 / 65 / 135 ms | 156% | 4 × 19% | [passed](evidence/scaling-25k.json) |
| 50,000 · 300 s | 9,996 | 3,000,000 / 3,000,000 | 29 / 74 / 219 ms | 200% | 4 × 20% | [passed](evidence/scaling-50k.json) |
| 100,000 · 300 s | 19,992 | 6,000,000 / 6,000,000 | 41 / 196 / 422 ms | 234% | 4 × 19% | [passed](evidence/scaling-100k.json) |
| 100,000 · 900 s | 19,998 | 18,000,000 / 18,000,000 | 41 / 190 / 527 ms | 232% | 4 × 20% | [passed](evidence/baseline-100k.json) |
| 150,000 · 300 s | 29,993 | 9,000,000 / 9,000,000 | 53 / 216 / 653 ms | 265% | 4 × 19% | [failed: Kafka memory growth 87 MiB > 64](evidence/capacity/rung-150k.json) |
| 150,000 · 300 s, edge + 4 api + 2 gateway replicas | 29,993 | 9,000,000 / 9,000,000 | 71 / 278 / 510 ms | 4 × 58% + gateways 46% + edge 51% | 4 × 17% | [passed](evidence/capacity/rung-150k-h1.json) |
| 200,000 · 300 s, edge topology | 39,940 | 12,000,000 / 12,000,000 | 310 / 1,393 / 2,093 ms | 4 × 67%; PostgreSQL 194% | 4 × 16% | [failed: p95 over 1 s](evidence/capacity/rung-200k.json) |
| 200,000 · 300 s, grid cell instead of device GiST | 39,959 | 12,000,000 / 12,000,000 | 114 / 448 / 768 ms | 4 × 60%; PostgreSQL 105% | 4 × 17% | [passed](evidence/capacity/rung-200k-h2.json) |
| 300,000 · 300 s, 4 processors | 59,921 | 18,000,000 / 18,000,000 | 32 / 49 / 52 s | 4 × 70%; PostgreSQL 155% | 4 × 18% | [failed: processors commit 50k/s, lag grows](evidence/capacity/rung-300k.json) |
| 300,000 · 300 s, 8 processors | 59,792 | 18,000,000 / 18,000,000 | 0.5 / 3.4 / 5.1 s | 4 × 69%; PostgreSQL 193% | 8 × 14% | [failed: p95 over 1 s, Kafka memory growth](evidence/capacity/rung-300k-p8.json) |
| 300,000 · 300 s, framed `/ingest`, PostgreSQL 18 upsert-returns-watermark, Prometheus policy v4 | 59,935 | 18,000,000 / 18,000,000 | 249 / 753 / 1,155 ms | 4 × 61%; PostgreSQL 119% | 8 × 14% | [passed](evidence/capacity/rung-300k-s1.json) |
| 500,000 · 300 s, librdkafka producer | 99,682 | 30,000,000 / 30,000,000 | 88 / 100+ s | 4 × 64%; PostgreSQL 171% | 8 × 17% | [failed: processors commit 58k/s](evidence/capacity/rung-500k.json) |
| 500,000 · 300 s, one lane per partition | 99,757 | 30,000,000 / 30,000,000 | 67 / 95 s | 4 × 80%; PostgreSQL 282% of 300% | 8 × 26% | [failed: PostgreSQL at its CPU limit; generator late](evidence/capacity/rung-500k-s4.json) |

**The ceiling on this laptop is between 300,000 and 500,000 devices, and it is the machine, not a tier.** At 500k the server containers used 10.8 cores of 14 (median), and the generator could not hold its own schedule: 126,796 reports were more than 100 ms late, which fails the workload check. `top` showed the host at 99% during that run (observed, not stored). [An idle-database probe](evidence/capacity/upsert-probe.txt) puts the upsert at 11.6 ms per 2,000 rows (5.8 µs per row); under that contention it averaged 143 ms ([statements](evidence/capacity/rung-500k-s4-statements.txt)). Going further needs more hardware: a second machine for the load, or the per-partition shards described in [Scaling out](#scaling-out). Two 300k runs are not in the table because they measured the harness, not the service: one on a table holding 2M rows left by earlier runs: a cold 2,000-id watermark read took 159 ms against 14 ms warm, an observation not stored ([kept](evidence/capacity/rung-300k-p8-progress.json)). In the other, the edge logged "backend 'api' has no server available" while a Kafka-probing readiness check timed out on every saturated replica; the logs were not preserved ([kept](evidence/capacity/rung-300k-h5.json); that check is now deleted). In every passed run all four sessions reconciled exactly, including the probe viewport, which received precisely the positions in its subscribed tiles. The earlier single-process design's 10,000-device runs remain in `evidence/` under [policy v2](evidence/acceptance-policy.md).

### Failure campaign

Each scenario runs 100,000 devices for 180 s and breaks one container about 60 s in: the `docker` actions landed at 60–71 s, as recorded in each file. It is judged by [fault policy v1/v2](evidence/acceptance-policy-fault-v2.md), declared before the runs. Every scheduled report must end up acknowledged, after resends, and PostgreSQL must hold every device's newest acknowledged report (count of devices and sum of their timestamps). Lag must drain. A dashboard that was closed or told to resync must resume, and one that was neither must have missed nothing.

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

No scenario lost an acknowledged report. Re-emitted events are the replay of batches committed but not yet offset-committed, so delivery is at least once.

## Limits

- One Kafka broker, one PostgreSQL and one NATS server: the Compose file demonstrates scale-out of api, gateway and processors, not broker or database replication (see [Scaling out](#scaling-out)).
- At-least-once delivery: a processor rebalance can repeat a live frame or alert.
- Dashboard positions are live tiles plus a snapshot. Alerts are not replayed after a disconnect, and there is no movement history.
- Identity is mock identity, as the assignment allows.
