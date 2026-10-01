# Fleet tracking and private geozone alerts

A real-time fleet service built with FastAPI, asynchronous SQLAlchemy and PostgreSQL/PostGIS. It receives moving-device reports, streams every accepted report to dashboards, and sends inside-zone alerts to every active session of the zone owner.

The map dashboard includes fleet search, device inspection, follow mode, private circular zones, live activity, occupancy insights, a responsive mobile layout and a dark theme. A configured OpenAI assistant can explain current measurements and propose a zone draft; saving remains an explicit operator action.

## Run

Install Docker with Compose. From this repository:

```sh
cp .env.example .env
docker compose up --build -d --wait
```

Open **http://127.0.0.1:8097**. API documentation is at **/docs**. The default database password is for local demonstration; configure it in `.env` before the first boot. PostgreSQL is private to the Compose network, and its named volume preserves zones and latest positions. The app runs as a non-root user with a read-only filesystem, no Linux capabilities and a 512 MiB memory limit. PostgreSQL has a 1 GiB limit; each container has a two-CPU budget.

The PostGIS image is built on official PostgreSQL with its packaged extension so both ARM64 and AMD64 work without emulation. Startup applies Alembic migrations before accepting traffic. Stop with `docker compose stop`; start existing containers with `docker compose start`.

For development and the generator, install [uv](https://docs.astral.sh/uv/) and run `uv sync --frozen`. Python 3.12+ is used. `uv.lock` pins Python dependencies.

## Try the flow

Open two browser tabs as `alice`, and another as `bob`. Create an Alice zone centred near `56.9496, 24.1052` with a 500 metre radius, then run:

```sh
uv run python generator.py --devices 10000 --interval 5 --duration 60
```

Both Alice tabs receive alerts for inside reports. Bob sees the shared fleet and his own zones and alerts. Closing one Alice tab leaves her other session active.

Zone creation, editing and deletion also notify all of the owner's sessions, so geofence controls stay coherent between tabs.

The reusable driver verifies this independently without resetting the database:

```sh
uv run python scripts/drive.py
```

## API contracts

| Interface | Contract |
| --- | --- |
| `POST /locations` | One `{device_id,latitude,longitude,timestamp}` report |
| `POST /locations/batch` | Array of 1–200 reports, capped by the configured processing batch size |
| `GET/POST /geozones` | List or create this user's zones |
| `GET/PATCH/DELETE /geozones/{id}` | Owner-scoped read, edit or deletion; another user's ID returns 404 |
| `GET /devices/latest` | Shared fleet snapshot with keyset pagination |
| `GET /insights` | Fleet freshness and this user's current zone occupancy |
| `GET /assistant` | Assistant availability; no secret values |
| `POST /assistant` | Read-only structured explanation or zone proposal |
| `WS /ws?user_id=alice` | Ready event, then location and private alert frames |
| `/health/live`, `/health/ready` | Process liveness and processor/database readiness |
| `/metrics` | Bounded-window processing/loop-lag distributions and pipeline counters |

REST user identification uses `X-User-ID`; the WebSocket uses its query parameter. This is deliberately mock identity, as permitted by the assignment. Ingestion represents device traffic and does not use dashboard identity. All demo users see the same fleet; geozones, occupancy and alerts are owner-private.

Zones contain `name`, `latitude`, `longitude`, positive `radius_m` and `active`. Responses include `id` and an incrementing `version`. Snapshot and zone lists return `{items,next_cursor}`; use `after` to load the next page. Zone updates require both latitude and longitude when moving the centre.

Reports require a timezone-aware timestamp, normalized to UTC. `(device_id,timestamp)` identifies a sample. Equal-key reports within one batch are first-wins duplicates; reports at or before a persisted device watermark are stale. HTTP 200 returns a `statuses` array in request order: `accepted`, `duplicate` or `stale`. Device clocks must be consistent. Every distinct fresh report produces an alert for every matching active zone, even when the device was already inside.

Owner sessions also receive `{"type":"zones_changed"}` after a committed zone mutation; refresh private zone data on that event.

Example WebSocket frames:

```json
{"type":"ready","session_id":"..."}
{"type":"locations","items":[{"device_id":"truck-1","latitude":56.9496,"longitude":24.1052,"timestamp":"2026-10-01T10:00:00.000100Z"}]}
{"type":"inside_report","items":[{"device_id":"truck-1","latitude":56.9496,"longitude":24.1052,"timestamp":"2026-10-01T10:00:00.000100Z","zone_id":"...","zone_version":1}]}
```

## Architecture and resource bounds

HTTP admission → bounded ingress → one asynchronous microbatch processor → PostGIS matching and latest-state transaction → commit → per-user fanout → independent WebSocket writers.

The processor collects up to 200 reports over 25 ms. It reads previous watermarks, preserves every distinct fresh report for spatial evaluation, then persists only each device's newest position. An inside-then-outside movement within one batch still generates its inside alert. The processing transaction uses repeatable-read isolation so candidate radii and exact zone data agree.

Geozones use `geography(Point,4326)`, making radii metres rather than longitude/latitude degrees. Points are constructed longitude first. Matching runs in PostgreSQL using exact `ST_DWithin`; there are no Python distance loops or per-zone database requests.

Different zone radii are grouped into generated power-of-two radius buckets. A partial multicolumn GiST index on `(radius_bucket,center)` restricts candidates by bucket and distance, followed by each zone's exact radius. A separate partial bucket index identifies the active buckets. The highest bucket's 33,554,432 metre bound exceeds every WGS84 surface distance; larger valid zone radii retain their exact semantics. This replaces the global maximum-radius approach, whose populated-data spike degraded sharply when one global zone was added. Latest device positions also have GiST indexing for private zone occupancy queries.

The SQLAlchemy pool has five connections and no overflow. At most four CRUD/insight sessions compete for it, leaving processing capacity. Sessions are short-lived and never belong to a WebSocket. Query timeouts, request body size, HTTP concurrency, report backlog, total matches, serialized output and per-socket backlog all have explicit limits.

| Default bound | Value |
| --- | --- |
| Admitted HTTP requests | 512 |
| Request body | 256 KiB |
| Total retained HTTP bodies | 16 MiB |
| Pending reports, including processing | 4,096 |
| Batch | 200 reports / 25 ms |
| Database statement | 2 seconds |
| Matches per batch | 4,000 |
| Total serialized batch output | 1 MiB |
| Dashboard connections, including handshakes | 128 |
| Per-connection backlog | 2 MiB / 128 frames |
| Frame size | 64 KiB |
| Socket send deadline | 2 seconds |

Resource settings use the `GEO_` environment prefix and are defined in `geo_tracking/settings.py`. Empty socket queues must fit a complete maximum batch burst. Ingress or output overload returns 503 with `Retry-After`. Output budget violations roll back the whole batch; alert matches are never silently truncated. Slow/full sockets close individually with an overload reason, while healthy sessions continue.

Frames are serialized once per shared fleet batch or private owner burst. Fanout snapshots recipient connections and enqueues without awaiting network sends. Each connection has exactly one writer. The browser opens its socket before fetching snapshots and merges positions with microsecond precision; its canvas rendering and bounded fleet/activity lists avoid per-device DOM growth.

Run **one application process**. The entry point fixes one Uvicorn worker, and connection state is local. Extra independent workers require shared event routing and coordinated processor ownership; merely increasing `workers` is unsupported. No broker is needed for this deployment. The benchmark defines its measured capacity envelope rather than claiming unlimited horizontal scale.

## Notification semantics

HTTP 200 acknowledges committed latest state and enqueue attempts to the recipient snapshot, excluding sockets explicitly closed for overload. It does not acknowledge browser delivery. Events are live and ephemeral. A crash between commit and fanout can lose notifications; reconnect reconstructs latest map state without replaying missed alerts. A disconnected request may still commit, making a retry stale.

Zone edits take effect at the processing transaction's snapshot. An already evaluated alert can arrive after a later edit or deletion; its zone version identifies the evaluated definition. The service stores latest positions, not an unbounded location history.

## Assistant

Set `GEO_OPENAI_API_KEY` in `.env`, then recreate the app. `GEO_ASSISTANT_MODEL` defaults to `gpt-4.1-mini`. The key remains server-side. Only an explicit assistant request sends the prompt, current fleet summary, this user's zone context and selected centre to OpenAI, with `store=false`.

The assistant uses the Responses API and a strict structured schema. It has no mutation tools; its proposed centre must match a real selected device or supplied map point, and the zone must pass server validation. The operator reviews the draft and clicks Save to create it through normal owner-scoped CRUD. Only two model calls may be in flight; calls have finite deadlines and no automatic retries. Without a key the service reports unavailable and does not fabricate model answers.

SDK integration, invalid drafts, private context and mutation boundaries are tested against a deterministic test provider. A live model call is a separate verification requiring a configured key.

## Tests and measurements

```sh
docker compose --profile test run --build --no-deps --rm tests
uv run ruff check
uv run ruff format --check
uv run python scripts/benchmark.py --duration 900 --docker-stats --output evidence/baseline.json
```

Tests run against real PostGIS in the separate `geo_test` database. They cover metres, boundary tolerance, high latitude, the antimeridian, variable radii, private CRUD and alerts, multiple sessions, duplicate/stale reports, intermediate zone crossings, overload rollback, stalled writers, restart watermarks, processor failure and assistant contracts.

The generator maintains 10,000 moving device states with stable IDs, realistic random drift, randomized reporting phases and a monotonic schedule. Its HTTP connector and outstanding queue are bounded. It records scheduled, attempted, accepted, rejected, dropped, retried and late reports; it does not silently reduce offered load when the server slows. `--synchronized` creates a burst, `--interval 2` offers 5,000 reports/s, and `--batch-size 100` exercises the bulk endpoint separately from the required single-report path.

The benchmark creates 100 private test zones, three sessions across two users, and a coverage zone containing the moving fixture. It reconciles each healthy session's location counts and report-identity checksums; both owner sessions must match the alert stream and the other user's must be empty. Workload success, delivery reconciliation, latency and resource evidence are reported separately. Ten thousand devices at five-second intervals means 2,000 reports/s, not 10,000 producer connections.

The [declared acceptance policy](evidence/acceptance-policy.md) requires exact population, zero loss, at least 99% of the offered rate, bounded scheduling jitter, delivery p95 below one second and stable container memory. Reports over 100 ms late remain visible diagnostics rather than an unrealistic zero-jitter timing requirement.

See [verification evidence](evidence/verification.md) for measured hardware, test results and limits. The populated spatial-plan probe is `scripts/explain.py`; it inserts its fixture inside a transaction and rolls it back. The reusable [drive-fleet skill](.agents/skills/drive-fleet/SKILL.md) preserves the local verification workflow.

The GitHub Actions template is in `ci/github-actions.yml`. Activating it under `.github/workflows` requires a GitHub credential with the `workflow` scope. The publishing account currently has repository access, so the template is included without activating remote automation.
