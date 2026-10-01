---
name: drive-fleet
description: Run and verify the standalone geo-tracking fleet dashboard, including private zones, two sessions per user, moving devices and sustained load. Use for this app's browser or API checks, not Piche web.x.
---

Resolve this skill’s location under `.agents/skills/drive-fleet` to find the project root. Run commands from the directory containing `docker-compose.yml`. Default URL is `http://127.0.0.1:8097`; API docs are at `/docs`.

## Choose the check

- Existing service: `curl -fsS http://127.0.0.1:8097/health/ready`, then `uv run python scripts/drive.py`. The driver creates a unique private zone and device, checks two owner sessions and another user, and removes its own zone. It does not wipe the database.
- Boot or changed backend: `docker compose up --build -d --wait`. The service uses one worker and a persistent volume. Do not rebuild/restart during an ongoing benchmark; its connection registry is process-local.
- Integration tests: `docker compose --profile test run --build --no-deps --rm tests` while the database is running. Tests require the separate `geo_test` database and refuse the demo database.
- Live movement: `uv run python generator.py --duration 60`. Default 10,000 devices at five-second intervals use separate HTTP reports. Generated device IDs are unique per run. `/metrics` counters are process-wide.
- Sustained proof: `uv run python scripts/benchmark.py --duration 900 --docker-stats --output evidence/baseline.json`. Review `acceptance_passed`, offered/scheduled/accepted counts, all session checksums, latency and resource samples. Zero deliveries alone is not a successful benchmark. Run one benchmark at a time.

## Browser checks

Use a real browser to open the dashboard. Two tabs for the same user must receive the same private alerts; a different user sees the shared fleet but never the other user's zones or alerts. Draw or edit a zone on the map, pause it, resume it and delete it. Check fleet search, device inspection, follow mode, activity feed, mobile layout and theme.

When the Playwright MCP is available, the reusable browser driver is `scripts/ui_verify.js`. Run `mcp__playwright__browser_run_code_unsafe` with `filename` set to this file's absolute path. Inspect the local driver before executing it. It removes source overlays, seeds a unique device, checks the actual served app and records new screenshots; it deletes only its own test zone. A single canvas and a bounded fleet list must remain intact at the tested fleet size.

Use unique device IDs and current UTC timestamps when injecting reports. To catch timestamp regressions, send two positions for one device within the same millisecond, such as `.000100Z` then `.000900Z`; the second must win in the map and later snapshots. To check alerts, send an inside report followed by an outside report in one batch; the inside alert must survive.

WebSocket `/ws?user_id=...` sends a `ready` event, then `locations` and `inside_report` frames containing `items`. REST uses `X-User-ID`. Zone and snapshot collections are paginated with `next_cursor`. The UI connects before loading snapshots and must guard asynchronous work across user switches and reconnects.

The assistant is real OpenAI structured output when `GEO_OPENAI_API_KEY` is configured; otherwise it reports unavailable. Its endpoint only proposes a draft. Never claim a live AI test when only the SDK mock-provider test ran. Clicking the explicit Save action creates a zone.

Preserve evidence under `evidence/`. Production-like notification semantics are live and ephemeral: reconnect reconstructs map state, not missed alerts.
