---
name: drive-fleet
description: Run and verify the standalone geo-tracking fleet service — Kafka ingest, processors, NATS fanout, private zones, multi-session dashboards and sustained 100k-device load. Use for this app's API, stream or browser checks, not Piche web.x.
---

Resolve this skill's location under `.agents/skills/drive-fleet` to find the project root, and run commands from the directory containing `docker-compose.yml`. Default URL is `http://127.0.0.1:8097`; API docs are at `/docs`.

## Choose the check

- Existing stack: `curl -fsS http://127.0.0.1:8097/health/ready`, then `uv run python scripts/drive.py`. The driver creates its own zone and device, checks two owner sessions and another user over viewport subscriptions, and removes its zone. It never wipes data. Its output includes the API and processor instance counts and the consumer lag.
- Boot or changed backend: `docker compose up --build -d --wait`. That starts db, kafka, nats, a one-shot `migrate`, `api` (4 uvicorn workers) and `processor` (`PROCESSORS`, default 4, all in one consumer group). Never restart during a benchmark.
- Integration tests: `docker compose --profile test run --build --no-deps --rm tests`. Each test gets its own Kafka topic, consumer group and NATS subject prefix against the `geo_test` database; the suite refuses any other database.
- Live movement: `uv run python generator.py --devices 100000 --duration 60`. It streams over `/ingest` WebSockets by default; `--transport http` exercises `POST /locations`.
- Sustained proof: `uv run python scripts/benchmark.py --devices 100000 --duration 900`. Read `acceptance_passed`, the workload, pipeline, delivery and resource checks, and every session's counts and checksums. Run one benchmark at a time; `/metrics` counters are cumulative per process.

## Protocol facts

- Ingest acknowledges after Kafka accepts a report (`202 {"accepted": n}`). Duplicates and stale samples are discarded later, by the processor, against PostgreSQL watermarks.
- `/ws?user_id=…` sends `ready`. The client then sends `{"type":"viewport","south","west","north","east"}`, receives `subscribed`, and only then loads `/devices/latest` for that box. Positions arrive as `positions` frames of `[device_id, latitude, longitude, timestamp_us]` for the subscribed map tiles. Alerts (`inside_report`) and `zones_changed` are private to the user and independent of the viewport.
- `/metrics` gathers every API worker and processor over NATS. `roles.processor.consumer_lag` is the backlog still in Kafka.

## Browser checks

Use a real browser only when the user approves that run. `scripts/ui_verify.js` is a Playwright MCP driver (`mcp__playwright__browser_run_code_unsafe` with its absolute `filename`). It moves the map to its test device before reporting, because positions follow the viewport.
