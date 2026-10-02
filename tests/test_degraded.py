from collections.abc import Iterator

import httpx
import pytest
from fastapi import FastAPI

from geo_tracking.api import health
from tests.helpers import report
from tests.stack import Server, Stack, free_port


class Prometheus:
    def __init__(self) -> None:
        self.values: dict[str, list] = {
            "fleet:freshness:p95": [{"value": [0, "0.25"]}],
            "fleet:ingest_accepted:rate1m": [{"value": [0, "NaN"]}],
            "fleet:consumer_lag:records": [],
        }
        self.queries = 0
        app = FastAPI()

        @app.get("/api/v1/query")
        async def query(query: str) -> dict:
            self.queries += 1
            return {"data": {"result": self.values[query]}}

        self.server = Server(app)


@pytest.fixture
def prometheus() -> Iterator[Prometheus]:
    fake = Prometheus()
    fake.server.start()
    yield fake
    fake.server.stop()


@pytest.fixture(autouse=True)
def fresh_stats(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(health, "cached", (0.0, {}))


def test_stats_read_prometheus_once_per_ttl_and_map_missing_to_null(settings, prometheus) -> None:
    url = f"http://127.0.0.1:{prometheus.server.port}"
    with Stack(settings.model_copy(update={"prometheus_url": url})) as stack:
        first = httpx.get(f"{stack.url}/stats").json()
        assert first == {
            "freshness_p95_seconds": 0.25,
            "reports_per_second": None,
            "consumer_lag": None,
        }
        prometheus.values["fleet:freshness:p95"] = [{"value": [0, "9"]}]
        assert httpx.get(f"{stack.url}/stats").json() == first
        assert prometheus.queries == 3


def test_stats_answer_503_when_prometheus_is_down(settings) -> None:
    dead = f"http://127.0.0.1:{free_port()}"
    with Stack(settings.model_copy(update={"prometheus_url": dead})) as stack:
        response = httpx.get(f"{stack.url}/stats")
        assert response.status_code == 503 and response.json() == {
            "detail": "prometheus_unavailable"
        }


def test_a_full_produce_window_sheds_load_with_retry_after(settings) -> None:
    tight = settings.model_copy(update={"produce_window": 1, "admission_timeout_seconds": 0.2})
    with Stack(tight) as stack, httpx.Client(base_url=stack.url, timeout=10) as http:
        refused = http.post("/locations/batch", json=[report(offset=1), report(offset=2)])
        assert refused.status_code == 503 and refused.headers["retry-after"] == "1"
        assert refused.json() == {"detail": "ingest_capacity"}
        assert http.post("/locations", json=report(offset=3)).status_code == 202
