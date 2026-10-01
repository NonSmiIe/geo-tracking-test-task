from copy import deepcopy

from scripts.acceptance import assess_resources, assess_workload


def healthy_measurement():
    return {
        "counters": {
            "expected_scheduled": 1800000,
            "scheduled": 1800000,
            "accepted": 1800000,
            "late_over_100ms": 126,
        },
        "accepted_reports_per_second": 1999.9,
        "schedule_lag": {"p99_ms": 3, "max_ms": 150},
    }


def test_small_os_jitter_does_not_hide_loss_or_slow_offering():
    healthy = healthy_measurement()
    assert assess_workload(healthy, 10000, 5)["passed"]
    for counter in ("generator_dropped", "rejected", "transport_errors", "http_503"):
        damaged = deepcopy(healthy)
        damaged["counters"][counter] = 1
        assert not assess_workload(damaged, 10000, 5)["passed"]
    slow = deepcopy(healthy)
    slow["accepted_reports_per_second"] = 1200
    assert not assess_workload(slow, 10000, 5)["passed"]


def test_zero_or_missing_population_and_large_timer_lag_fail():
    empty = healthy_measurement()
    empty["counters"] = {"expected_scheduled": 1800000, "scheduled": 1800000, "accepted": 0}
    assert not assess_workload(empty, 10000, 5)["passed"]
    stalled = healthy_measurement()
    stalled["schedule_lag"] = {"p99_ms": 300, "max_ms": 1200}
    assert not assess_workload(stalled, 10000, 5)["passed"]
    delayed = healthy_measurement()
    delayed["counters"]["late_over_100ms"] = 20000
    assert not assess_workload(delayed, 10000, 5)["passed"]


def test_memory_growth_and_missing_samples_fail_required_resource_proof():
    state = {
        "pending_reports": 10,
        "connections": 3,
        "database_connections_checked_out": 1,
        "counters": {"ingress_high_water": 256},
    }
    samples = [
        state | {"resources": [{"Name": name, "MemUsage": memory} for name in ("app", "db")]}
        for memory in ("100MiB / 512MiB", "105MiB / 512MiB")
    ]
    assert assess_resources(samples, state, True)["passed"]
    samples[-1]["resources"][0]["MemUsage"] = "300MiB / 512MiB"
    assert not assess_resources(samples, state, True)["passed"]
    assert not assess_resources([], state, True)["passed"]
