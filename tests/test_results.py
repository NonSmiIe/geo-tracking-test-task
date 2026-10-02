import pytest

from scripts.results import cpu, latency, share


@pytest.mark.parametrize(
    ("milliseconds", "shown"),
    [(0, "0 ms"), (384.4, "384 ms"), (9_999, "9,999 ms"), (10_000, "10.0 s"), (61_250, "61.2 s")],
)
def test_latency_switches_to_seconds_at_ten(milliseconds: float, shown: str) -> None:
    assert latency(milliseconds) == shown


def test_cpu_shares_are_grouped_by_role_and_summarised() -> None:
    containers = {
        "fleet-api-1": {"cpu_median_percent": 120.0},
        "fleet-api-2": {"cpu_median_percent": 80.0},
        "fleet-db-1": {"cpu_median_percent": 250.4},
        "fleet-processor-1": {"cpu_median_percent": 30.0},
        "fleet-processor-2": {"cpu_median_percent": 50.0},
    }
    assert cpu(containers, "api") == [120.0, 80.0]
    assert share(cpu(containers, "api")) == "2 × 100%"
    assert share(cpu(containers, "db")) == "250%"
    assert share(cpu(containers, "processor")) == "2 × 40%"
    assert share(cpu(containers, "gateway")) == "—"
