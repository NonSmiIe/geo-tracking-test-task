import asyncio
from time import monotonic

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    ProcessCollector,
    generate_latest,
)

ROLES = {role: CollectorRegistry() for role in ("api", "gateway", "processor")}
API, GATEWAY, PROCESSOR = ROLES["api"], ROLES["gateway"], ROLES["processor"]
for registry in ROLES.values():
    ProcessCollector(registry=registry)


def labelled(counter: Counter, *values: str) -> Counter:
    for value in values:
        counter.labels(value)
    return counter


INGESTED = Counter(
    "fleet_ingest_reports",
    "Reports offered to ingest, by transport and outcome",
    ["transport", "outcome"],
    registry=API,
)
WINDOW_USED = Gauge(
    "fleet_ingest_window_used", "Reports sent to Kafka and not yet acknowledged", registry=API
)
WINDOW_SIZE = Gauge("fleet_ingest_window_size", "Capacity of the produce window", registry=API)

CONNECTIONS = Gauge("fleet_gateway_connections", "Open dashboard sockets", registry=GATEWAY)
SUBSCRIPTIONS = Gauge(
    "fleet_gateway_subscriptions", "NATS subjects held for dashboards", registry=GATEWAY
)
FRAME_BYTES = Counter(
    "fleet_gateway_frame_bytes", "Bytes queued to dashboard sockets", registry=GATEWAY
)
EVICTIONS = labelled(
    Counter(
        "fleet_gateway_evictions",
        "Dashboards closed for falling behind",
        ["reason"],
        registry=GATEWAY,
    ),
    "backlog_overflow",
    "gateway_budget",
    "send_timeout",
)
SLOW_CONSUMERS = Counter(
    "fleet_gateway_slow_consumers", "NATS slow-consumer errors", registry=GATEWAY
)

RECORDS = labelled(
    Counter(
        "fleet_processor_records", "Consumed reports, by outcome", ["outcome"], registry=PROCESSOR
    ),
    "committed",
    "stale",
    "duplicate",
    "replayed",
)
BATCHES = labelled(
    Counter(
        "fleet_processor_batches", "Processed batches, by outcome", ["outcome"], registry=PROCESSOR
    ),
    "committed",
    "failed",
)
BATCH_SECONDS = Histogram(
    "fleet_processor_batch_seconds",
    "Database and publish time of one batch",
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
    registry=PROCESSOR,
)
FRESHNESS = Histogram(
    "fleet_processor_freshness_seconds",
    "Report timestamp to published event, oldest per batch",
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300),
    registry=PROCESSOR,
)
PARTITIONS = Gauge(
    "fleet_processor_partitions", "Partitions this processor owns", registry=PROCESSOR
)

LOOP_LAG = {
    role: Histogram(
        "fleet_event_loop_lag_seconds",
        "Event loop scheduling delay",
        buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1),
        registry=registry,
    )
    for role, registry in ROLES.items()
}


def exposition(role: str) -> tuple[bytes, str]:
    return generate_latest(ROLES[role]), CONTENT_TYPE_LATEST


async def monitor_loop(role: str) -> None:
    lag = LOOP_LAG[role]
    while True:
        expected = monotonic() + 0.25
        await asyncio.sleep(0.25)
        lag.observe(max(0.0, monotonic() - expected))
