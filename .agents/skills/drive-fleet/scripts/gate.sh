#!/usr/bin/env bash
set -euo pipefail
uv run ruff check
uv run ruff format --check
uv run mypy geo_tracking
uv run vulture geo_tracking scripts generator.py --min-confidence 80
docker run --rm -v "$PWD/ops/prometheus:/p:ro" -w /p --entrypoint promtool prom/prometheus:v3.5.0 test rules rules_test.yml
docker run --rm -v "$PWD/deploy/k8s:/k:ro" registry.k8s.io/kubectl:v1.34.1 kustomize /k 2>/dev/null \
  | docker run --rm -i ghcr.io/yannh/kubeconform:v0.7.0 -strict -summary -schema-location default \
    -schema-location 'https://raw.githubusercontent.com/datreeio/CRDs-catalog/main/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json'
docker compose --profile test run --build --no-deps --rm tests
