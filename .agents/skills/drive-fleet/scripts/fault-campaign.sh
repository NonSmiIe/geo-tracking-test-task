#!/usr/bin/env bash
set -u
P=${PROJECT:-geo-tracking-test-task}
DEVICES=${DEVICES:-100000}
DURATION=${DURATION:-180}
OUT=${OUT:-evidence/faults}
mkdir -p "$OUT"
run() {
  name=$1; shift
  args=()
  for f in "$@"; do args+=(--fault "$f"); done
  uv run python scripts/benchmark.py --devices "$DEVICES" --duration "$DURATION" --processes 6 --connections 8 \
    "${args[@]}" --output "$OUT/$name.json" > "$OUT/$name.log" 2>&1
  echo "$name exit $?"
  sleep 20
}
run s1-nats-restart 60:restart:$P-nats-1
run s2-kafka-restart 60:restart:$P-kafka-1
run s3-processor-kill 60:kill:$P-processor-2 90:start:$P-processor-2
run s4-processor-pause 60:pause:$P-processor-4 90:unpause:$P-processor-4
run s5-db-restart 60:restart:$P-db-1
run s6-api-kill 60:kill:$P-api-1 90:start:$P-api-1
run s7-gateway-kill 60:kill:$P-gateway-1 90:start:$P-gateway-1
run s8-edge-restart 60:restart:$P-edge-1
