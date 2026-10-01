import asyncio
from collections import Counter, deque
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from time import monotonic
from uuid import uuid4


def distribution(values: Iterable[float]) -> dict[str, float]:
    ordered = sorted(values)
    if not ordered:
        return {"count": 0}
    return {
        "count": len(ordered),
        "p50": ordered[int((len(ordered) - 1) * 0.5)],
        "p95": ordered[int((len(ordered) - 1) * 0.95)],
        "p99": ordered[int((len(ordered) - 1) * 0.99)],
        "max": ordered[-1],
    }


@dataclass
class Metrics:
    role: str
    instance: str = field(default_factory=lambda: uuid4().hex[:8])
    counts: Counter = field(default_factory=Counter)
    processing_ms: deque = field(default_factory=lambda: deque(maxlen=4096))
    loop_lag_ms: deque = field(default_factory=lambda: deque(maxlen=4096))
    gauges: dict[str, Callable[[], float]] = field(default_factory=dict)

    def high_water(self, name: str, value: float) -> None:
        self.counts[name] = max(self.counts[name], value)

    def snapshot(self) -> dict:
        return {
            "role": self.role,
            "instance": self.instance,
            "counters": dict(self.counts),
            "gauges": {name: read() for name, read in self.gauges.items()},
            "processing_ms": distribution(self.processing_ms),
            "loop_lag_ms": distribution(self.loop_lag_ms),
        }


async def monitor_loop(metrics: Metrics) -> None:
    while True:
        expected = monotonic() + 0.25
        await asyncio.sleep(0.25)
        metrics.loop_lag_ms.append(max(0.0, (monotonic() - expected) * 1000))


def merge(snapshots: list[dict]) -> dict:
    totals: dict[str, dict[str, float]] = {}
    for snapshot in snapshots:
        role = totals.setdefault(snapshot["role"], {"instances": 0})
        role["instances"] += 1
        for name, value in {**snapshot["counters"], **snapshot["gauges"]}.items():
            role[name] = role.get(name, 0) + value
    return {"roles": totals, "instances": sorted(snapshots, key=lambda item: item["role"])}
