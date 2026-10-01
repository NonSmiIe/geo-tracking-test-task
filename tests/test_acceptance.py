from copy import deepcopy

from scripts.acceptance import assess_pipeline, assess_resources, assess_workload


def generated() -> dict:
    return {
        "counters": {
            "expected_scheduled": 1000,
            "scheduled": 1000,
            "sent": 1000,
            "acked": 1000,
            "late_over_100ms": 1,
        },
        "acked_reports_per_second": 1999,
        "offered_reports_per_second": 2000,
        "schedule_lag": {"p99_ms": 3, "max_ms": 150},
    }


def test_workload_fails_on_any_loss_slow_rate_or_timer_tail() -> None:
    assert assess_workload(generated())["passed"]
    for counter in ("generator_dropped", "rejected", "transport_errors"):
        damaged = deepcopy(generated())
        damaged["counters"][counter] = 1
        assert not assess_workload(damaged)["passed"]
    unacked = deepcopy(generated())
    unacked["counters"]["acked"] = 999
    assert not assess_workload(unacked)["passed"]
    slow = deepcopy(generated())
    slow["acked_reports_per_second"] = 1900
    assert not assess_workload(slow)["passed"]
    stalled = deepcopy(generated())
    stalled["schedule_lag"] = {"p99_ms": 300, "max_ms": 1200}
    assert not assess_workload(stalled)["passed"]


def test_pipeline_requires_exact_commits_drained_lag_and_no_evictions() -> None:
    def snapshot(committed: int, lag: float | None = 0, evicted: int = 0, resets: int = 0) -> dict:
        return {
            "committed": committed,
            "consumer_lag": lag,
            "evicted": evicted,
            "counter_resets": resets,
        }

    assert assess_pipeline(500, snapshot(100), snapshot(600))["passed"]
    assert not assess_pipeline(500, snapshot(100), snapshot(599))["passed"]
    assert not assess_pipeline(500, snapshot(100), snapshot(600, lag=3))["passed"]
    assert not assess_pipeline(500, snapshot(100), snapshot(600, lag=None))["passed"]
    assert not assess_pipeline(500, snapshot(100), snapshot(600, evicted=1))["passed"]
    assert not assess_pipeline(500, snapshot(100), snapshot(600, resets=1))["passed"]


def test_resources_require_samples_headroom_and_steady_memory() -> None:
    def samples(last: str) -> list[dict]:
        usage = ["100MiB", "300MiB", "300MiB", "300MiB", "300MiB", "300MiB", "300MiB", last]
        return [
            {"containers": [{"Name": "api", "MemUsage": f"{u} / 1GiB", "CPUPerc": "10%"}]}
            for u in usage
        ]

    assert assess_resources(samples("310MiB"))["passed"]
    assert not assess_resources(samples("950MiB"))["passed"]
    assert not assess_resources(samples("400MiB")[:2])["passed"]
    assert not assess_resources([])["passed"]
