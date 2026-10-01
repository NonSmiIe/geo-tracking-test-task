from collections import Counter, deque
from dataclasses import dataclass, field


@dataclass
class Metrics:
    counts: Counter = field(default_factory=Counter)
    processing_ms: deque = field(default_factory=lambda: deque(maxlen=4096))
    loop_lag_ms: deque = field(default_factory=lambda: deque(maxlen=4096))

    def snapshot(self) -> dict:
        def distribution(values):
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

        return {
            "counters": dict(self.counts),
            "processing_ms": distribution(self.processing_ms),
            "loop_lag_ms": distribution(self.loop_lag_ms),
        }
