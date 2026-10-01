from copy import deepcopy

from scripts.acceptance import assess_fault_run, assess_pipeline, assess_resources, assess_workload


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


def test_fault_runs_judge_durability_and_resumption_not_counters() -> None:
    workload = {"checks": {"exact_population": True, "no_loss_or_overload": True}}
    pipeline = {
        "checks": {"database_holds_every_latest_report": True, "consumer_lag_drained": True}
    }

    def session(index: int, closed: bool, exact: bool, resumed: bool) -> dict:
        return {
            "session": index,
            "closures_at_seconds": [42.0] if closed else [],
            "received_after_last_closure": {"positions": 5} if resumed else {},
            "checks": {"positions": exact, "inside_report": exact, "not_closed": not closed},
        }

    assert assess_fault_run(workload, pipeline, [session(0, False, True, False)])["passed"]
    assert assess_fault_run(workload, pipeline, [session(0, True, False, True)])["passed"]
    assert not assess_fault_run(workload, pipeline, [session(0, True, False, False)])["passed"]
    assert not assess_fault_run(workload, pipeline, [session(0, False, False, False)])["passed"]
    lost = {"checks": {**pipeline["checks"], "database_holds_every_latest_report": False}}
    assert not assess_fault_run(workload, lost, [session(0, False, True, False)])["passed"]
