# Working on Fleetline

Take-home submission: real-time geo-tracking on FastAPI, PostGIS, Kafka and NATS. `README.md` is the reviewer-facing description; keep it true when behaviour changes.

## Commands

- Stack: `docker compose up --build -d --wait`. Python tooling runs through uv: `uv sync --frozen`, `uv run …`.
- Gate before every commit: `uv run ruff check && uv run ruff format --check && uv run mypy geo_tracking && uv run vulture geo_tracking scripts generator.py --min-confidence 80` (never during a benchmark: mypy competes for CPU), plus `docker compose --profile test run --build --no-deps --rm tests` when Python under `geo_tracking/` changed, plus `docker run --rm -v "$PWD/ops/prometheus:/p:ro" -w /p --entrypoint promtool prom/prometheus:v3.5.0 test rules rules_test.yml` when `ops/prometheus` changed.
- Proving behaviour or capacity: the `drive-fleet` skill.

## Invariants that break silently

- **One owner per device.** The producer's partitioner must stay `murmur2_random` (`test_librdkafka_partitions_every_key_like_the_java_default`). Kafka is keyed by `device_id`, and a device's watermark is safe only because one processor owns its partition. Never produce reports with another key, and never let two consumers of the group share a partition.
- **Order inside a partition lane:** DB commit → NATS publish + flush → Kafka offset commit, per partition. Lanes start and stop only in the rebalance listener, so a revoked partition has no lane left fetching it. Committing offsets earlier loses reports on a crash; publishing before the DB commit sends events for rolled-back data.
- **Freshness comes from the upsert's `RETURNING old.reported_at`.** A sample is fresh only if its device came back from the upsert and the sample is newer than that old watermark. The upsert writes only each device's newest sample, but matching runs on every fresh sample; collapsing before matching drops alerts for devices that cross a zone inside one batch.
- **The upsert stays monotonic** (`WHERE device.reported_at < excluded.reported_at`). It is the guard against a zombie owner during a rebalance.
- **`zone_footprint` must stay a superset of the geodesic circle.** It is only a candidate filter; `test_footprint_candidates_equal_exact_geography_everywhere` is the proof. Run that test after any change to the function or the matching query.
- **`grid_cells(area)` must cover every cell a point inside `area` can have**, or snapshots and occupancy silently drop devices. `grid_cell` holds the only cell formula; `test_grid_cells_never_exclude_a_point_inside_the_box` is the proof.
- **`consumer_progress` advances in the same transaction as the batch it describes.** Records at or below a partition's persisted offset were committed but maybe never published, so a replay re-emits all of them; records above it go through the watermark. Moving the advance out of that transaction loses events on a crash or re-alerts on every resend.
- **Position subjects are zoom-8 quadkeys, one digit per token** (`fleet.pos.1.2.…`). Viewport wildcards depend on that nesting; changing the level or the token layout breaks routing for every client.
- **Alert volume never rolls back ingestion.** Bound it with the per-user zone quota and the per-connection queues, never with a per-batch limit; that limit was the original denial-of-service bug.
- Ingest returns `202` once the report is durable in Kafka. It cannot say whether a sample is stale or a duplicate; the processor decides that.

## Conventions

- Schema changes go through Alembic in `migrations/versions/`; models in `geo_tracking/models.py` must match them.
- No prose comments in code. `# noqa` and similar directives are fine.
- `evidence/` is the measurement record: add new runs beside old ones, never rewrite a past result, and declare acceptance criteria before the run they judge.
- Metrics are Prometheus, defined once in `geo_tracking/metrics.py`; Prometheus (`127.0.0.1:9097`) scrapes every replica and Grafana (`127.0.0.1:3097`) shows them. Counters are per process and reset on restart. Compare runs by delta, as `scripts/benchmark.py` does, and trust a delta only when `resets()` over the window is 0.
- Alert rules live in `ops/prometheus/rules.yml`; any change to them or to a metric they use needs `rules_test.yml` updated and passing (command above).
