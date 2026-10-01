# Fleetline: real-time geo-tracking and private geozone alerts

Fleetline is a FastAPI, async SQLAlchemy and PostgreSQL/PostGIS service that ingests positions from a moving fleet. It streams them to dashboards and pushes an alert to every open session of a user whenever one of that user's devices reports from inside one of their circular geozones. The api, the dashboard gateway and the processors scale out by adding replicas; Kafka, NATS and PostgreSQL run as single instances here (see [Scaling out](#scaling-out)). The measured envelope on one laptop is 100,000 devices reporting every five seconds (20,000 reports/s), verified by count and identity checksum on every session; see [Measured results](#measured-results).

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
| `db` | PostgreSQL 17 + PostGIS 3, named volume |
| `kafka` | Kafka 4.1 (KRaft, single broker), topic `reports` with 24 partitions |
| `nats` | NATS 2.11 core, live event routing |
| `migrate` | One-shot `alembic upgrade head` before anything serves traffic |
| `edge` | HAProxy, the only published port: `/ws` to `gateway`, everything else to `api`, both `leastconn` |
| `api` | FastAPI, one process per replica (`API_REPLICAS`, default 4): ingest, REST, demo |
| `gateway` | FastAPI, one process per replica (`GATEWAYS`, default 2): dashboard WebSockets |
| `processor` | Kafka consumer group (`PROCESSORS`, default 4 replicas): dedup, PostGIS matching, persistence, fanout |

Only the edge port is published, and only on `127.0.0.1`. Application containers run as a non-root user with a read-only root filesystem, no Linux capabilities, `no-new-privileges`, and memory and CPU limits. Credentials come from `.env`. Stop with `docker compose stop`.

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

**Ingestion is stateless and acknowledged by Kafka.** api replicas validate each report and produce it to Kafka, keyed by `device_id`, with `acks=all` and an idempotent producer. A device WebSocket (`/ingest`) carries one JSON report per message and receives `{"type":"ack","count":n}` frames meaning the first `n` reports sent on that socket are durable, so a device that reconnects resends from report `n`. A `{"type":"flush"}` message returns the count once every in-flight report is durable. `POST /locations` and `POST /locations/batch` return `202` after the broker ack. Each api process has one bounded in-flight window shared by HTTP and sockets, and every socket has its own smaller window. A full window answers HTTP with `503` and `Retry-After`, and stops reading a socket, so TCP backpressure reaches the device. Nothing in the ingest path waits for the database.

**One owner per device, without one process.** Keying by `device_id` puts every report of a device on one partition, and the consumer group gives each partition exactly one processor. That ownership removes the watermark race a shared processor pool would have, and Kafka moves partitions to the survivors when a processor dies. Each processor polls up to 2,000 records and, in one transaction:

1. discards equal `(device_id, timestamp)` duplicates, first wins;
2. reads the persisted watermark of each device and drops samples at or before it (stale);
3. matches **every** remaining fresh sample against active zones in one set-based PostGIS query, so a device that passes through a zone inside one batch still alerts;
4. upserts only each device's newest position, guarded by `WHERE reported_at < excluded.reported_at` so a zombie owner during a rebalance can never move a device backwards.

After commit the processor publishes to NATS, flushes, and only then commits the Kafka offsets. On a database error it seeks back to the batch's first offsets and retries; nothing is acknowledged and lost. If NATS is unavailable after a commit, the processor keeps flushing for up to 30 s before committing offsets, then exits so that a restarted owner replays the batch. A replayed record identical to the stored row re-emits its position and alerts, so a crash between commit and publish loses no live event. A conflicting duplicate (same key, different content) is still discarded. Delivery is at least once: replays never change stored state, but a rebalance can repeat a live frame.

**Spatial matching stays in PostGIS.** Zone centres are `geography(Point,4326)`, so radii are metres on the spheroid. Points are always built longitude first. Each zone also has a generated `footprint` column, produced by the IMMUTABLE SQL function `zone_footprint(center, radius_m)`. It is a lon/lat box that conservatively contains the geodesic circle: the angular radius uses the smallest meridional radius of curvature plus a 1% margin; the longitude extent is `asin(sin θ / cos φ)`; the box becomes two boxes across the antimeridian and spans all longitudes when the circle reaches a pole. A partial GiST index on active footprints selects candidates with `ST_Intersects`, and exact `ST_DWithin(center, point::geography, radius_m)` decides. There are no Python distance calculations and no per-zone queries. A test asserts on real PostGIS that this path returns exactly what brute-force `ST_DWithin` returns, across random zones, poles, the antimeridian, boundary-projected points and radii from 1e-100 m to 1e100 m.

**Fanout is routed by NATS subjects, not by scanning.** Positions are published per Web-Mercator tile as `fleet.pos.<d1>.<d2>…<d8>`, a zoom-8 quadkey with one digit per token. Because quadkeys nest, a coarser tile is a subject prefix, so `fleet.pos.1.2.>` covers everything beneath it. A dashboard sends its viewport; the gateway picks the finest level whose tile cover has at most 16 tiles and subscribes to those subjects. Subscriptions are reference-counted across all of a gateway's connections, so NATS only delivers what some local viewer needs. Alerts go to `fleet.alerts.<hex(user)>` and zone changes to `fleet.zones.<hex(user)>`; every gateway holding a session of that user subscribes, which is how all of a user's sessions get every alert whichever gateway holds them. Processors serialize each frame once and gateways forward it without parsing the JSON.

**Latest positions stay cheap to update.** `device_latest` has no spatial index: an index on a column that changes on every report rules out PostgreSQL's in-place (HOT) updates. Instead each row carries a generated 0.25° grid cell with a btree index, which changes only when a device moves about 28 km. Viewport snapshots and zone occupancy pick their candidate cells with `grid_cells()` and filter exactly with PostGIS. At 200k devices this raised HOT updates from 2% to 99% and cut database CPU by 46%.

**WebSocket state.** Each gateway keeps a `connection_id → Connection` registry plus `subject → connections` routes. A connection has one bounded byte queue and exactly one writer task, with a send deadline. Fanout only enqueues, so a slow socket never blocks others. A socket that overflows its queue or misses the deadline is closed alone with an explicit reason, and its siblings keep streaming. The browser opens its socket, sends its viewport, waits for `subscribed`, and only then loads the `/devices/latest` snapshot for that box. Snapshot and stream merge by microsecond timestamp, so a slow snapshot never overwrites a fresher position.

**Why Kafka and NATS, and not Redis.** The ingest log needs durable partitions with exclusive, automatically rebalanced ownership; Kafka consumer groups are exactly that, and on Redis Streams it would have to be hand-built. The live path needs interest-based routing to the gateways that hold a viewer, and NATS subject wildcards do it inside the broker; Kafka would make every gateway read and decode the whole stream.

**Database use.** Every process has one async engine with a bounded pool and a fixed checkout timeout (api replicas 5, processors 2), plus a statement timeout. Sessions are short-lived and never belong to a WebSocket. Pool exhaustion or a database fault on REST returns `503`, while ingest keeps accepting into Kafka.

## Semantics

- A sample is identified by `(device_id, timestamp)`. Timestamps must carry a time zone and are normalized to UTC with microsecond precision. A timestamp more than 5 minutes in the future is rejected with `422`, so a bad clock can hold a device's watermark ahead by at most that much.
- Every fresh sample inside an active zone produces an alert for that zone, including repeated reports from a device already inside.
- An HTTP `202` or a socket ack means the report is durable in Kafka; it says nothing about browser delivery. Live events are ephemeral. A crash between a processor's commit and its publish loses those live events, since the replay finds the samples stale; reconnecting restores the latest map, not missed alerts.
- Zone edits take effect from the next processed batch. An alert carries the `zone_version` it was evaluated against.
- Devices are a shared demo fleet; zones, occupancy and alerts are private to their owner. REST identifies the user with `X-User-ID`, the dashboard socket with `?user_id=`, which the task explicitly permits as mock identity. Ingestion is device traffic and carries no user.

## API

| Interface | Contract |
| --- | --- |
| `WS /ingest` | Device stream: one report per message; `ack` frames carry the durable prefix length; `flush` for a final count |
| `POST /locations` | One `{device_id, latitude, longitude, timestamp}` → `202 {"accepted": 1}` |
| `POST /locations/batch` | 1–200 reports → `202 {"accepted": n}` |
| `GET/POST /geozones` | List (keyset pagination) or create this user's zones; quota per user |
| `GET/PATCH/DELETE /geozones/{id}` | Owner-scoped; another user's zone is a 404 |
| `GET /devices/latest` | Fleet snapshot; optional `south,west,north,east` box (antimeridian-aware), keyset pagination |
| `GET /insights` | Fleet freshness and live occupancy of this user's zones |
| `WS /ws?user_id=` | `ready` → client `viewport` → `subscribed`, then `positions`, `inside_report`, `zones_changed` |
| `GET/POST /demo`, `/demo/start`, `/demo/stop` | Guided demo for the current user; its state lives in PostgreSQL, so any replica can answer |
| `/health/live`, `/health/ready` | Liveness; api readiness checks only the Kafka broker (ingest keeps running through a database outage), gateway readiness checks NATS |
| `/metrics` | Counters and distributions from every api, gateway and processor process, gathered over NATS |

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
| Database pool | 5 per api process, 2 per processor, no overflow, 1 s checkout |
| Statement timeout | 2 s |
| Zones per user | 1,000 |
| Dashboard sessions per gateway | 128 |
| Connections per replica at the edge | 4,096 (queued beyond) |
| Per-session queue / send deadline | 8 MiB / 2 s |
| Viewport subscriptions | 16 tiles per session |
| Alert frame | 1,000 items |

All are `GEO_`-prefixed settings in `geo_tracking/settings.py`.

## Scaling out

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

`generator.py` simulates N devices across a configurable region (150 km radius around Riga by default). Every device has a heading and speed that drift each tick, which gives realistic tracks rather than jitter. Devices are sharded across processes; each process keeps a monotonic, open-loop schedule with random phases, so a slow server never lowers the offered rate. Reports go over multiplexed `/ingest` sockets by default (`--transport http` uses one `POST /locations` per report, `--batch-size` the batch endpoint). The generator counts scheduled, sent, acknowledged, dropped and late reports and a 64-bit identity checksum of everything sent.

```sh
uv run python generator.py --devices 100000 --interval 5 --duration 900
```

## Tests and measurements

```sh
docker compose --profile test run --build --no-deps --rm tests
uv run ruff check && uv run ruff format --check
uv run python scripts/benchmark.py --devices 100000 --duration 900
```

The tests run against real PostGIS, Kafka and NATS; each test gets its own topic, consumer group and subject prefix. They cover metre-correct containment and boundaries, high latitudes, poles and the antimeridian, footprint-versus-exact equality, coordinate order, paused and deleted zones, owner scoping, the zone quota, duplicates, stale samples and in-batch zone crossings. They also cover dense overlap (60 zones over the same devices, no rejected report), private delivery to every owner session, viewport routing and retargeting, device-socket acknowledgements, replay after a database failure, watermarks surviving a processor restart, slow-socket eviction and the guided demo.

The benchmark observes four dashboard sessions, each in its own process: two for the owner of a zone covering the whole fleet, one for another user with a world view, and one for that user with a small probe viewport. It reconciles every position and alert against the generator by count and identity checksum, checks that processors committed exactly what was acknowledged and that consumer lag drained, and records latency from the scheduled timestamp and container resources. The criteria were declared before the run in [acceptance policy v3](evidence/acceptance-policy-v3.md).

## Measured results

Apple M4 Pro; the Docker VM has 14 CPUs and 8 GB, shared with unrelated containers, and the generator and the observing clients run on the macOS host. Every run uses 100 zones and four dashboard WebSocket clients; latency is position delivery to those clients, measured from the scheduled report time (alert latency is within 3 ms of it in every run and is in each file). These runs predate the edge split: api ran as 4 uvicorn workers in one container.

| Devices · duration | Reports/s | Acknowledged | Position delivery p50 / p95 / p99 | API CPU | Processors | Verdict |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| 25,000 · 300 s | 4,999 | 1,500,000 / 1,500,000 | 28 / 65 / 135 ms | 156% | 4 × 19% | [passed](evidence/scaling-25k.json) |
| 50,000 · 300 s | 9,996 | 3,000,000 / 3,000,000 | 29 / 74 / 219 ms | 200% | 4 × 20% | [passed](evidence/scaling-50k.json) |
| 100,000 · 300 s | 19,992 | 6,000,000 / 6,000,000 | 41 / 196 / 422 ms | 234% | 4 × 19% | [passed](evidence/scaling-100k.json) |
| 100,000 · 900 s | 19,998 | 18,000,000 / 18,000,000 | 41 / 190 / 527 ms | 232% | 4 × 20% | [passed](evidence/baseline-100k.json) |
| 150,000 · 300 s | 29,993 | 9,000,000 / 9,000,000 | 53 / 216 / 653 ms | 265% | 4 × 19% | [failed: Kafka memory growth 87 MiB > 64](evidence/capacity/rung-150k.json) |
| 150,000 · 300 s, edge + 4 api + 2 gateway replicas | 29,993 | 9,000,000 / 9,000,000 | 71 / 278 / 510 ms | 4 × 58% + gateways 46% + edge 51% | 4 × 17% | [passed](evidence/capacity/rung-150k-h1.json) |

In every passed run all four sessions reconciled exactly, including the probe viewport, which received precisely the positions in its subscribed tiles. The earlier single-process design's 10,000-device runs remain in `evidence/` under [policy v2](evidence/acceptance-policy.md).

## Limits

- One Kafka broker, one PostgreSQL and one NATS server: the Compose file demonstrates scale-out of api, gateway and processors, not broker or database replication (see [Scaling out](#scaling-out)).
- At-least-once delivery: a processor rebalance can repeat a live frame or alert.
- Dashboard positions are live tiles plus a snapshot. Alerts are not replayed after a disconnect, and there is no movement history.
- Identity is mock identity, as the assignment allows.
