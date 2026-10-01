#!/usr/bin/env bash
set -euo pipefail
uv run ruff check
uv run ruff format --check
uv run mypy geo_tracking
uv run vulture geo_tracking scripts generator.py --min-confidence 80
docker run --rm -v "$PWD/ops/prometheus:/p:ro" -w /p --entrypoint promtool prom/prometheus:v3.5.0 test rules rules_test.yml
docker compose --profile test run --build --no-deps --rm tests
