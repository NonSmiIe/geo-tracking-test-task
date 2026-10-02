from collections.abc import Iterator

import httpx
import pytest

from geo_tracking.loadgen import create_loadgen_app
from tests.helpers import wait_for
from tests.stack import Server

LOAD = {"devices": 40, "interval_seconds": 2, "duration_seconds": 10, "spread_km": 2}


@pytest.fixture
def loadgen(stack) -> Iterator[httpx.Client]:
    settings = stack.settings.model_copy(
        update={"loadgen_target_url": stack.url, "loadgen_prefix": "lg"}
    )
    server = Server(create_loadgen_app(settings))
    server.start()
    with httpx.Client(base_url=f"http://127.0.0.1:{server.port}", timeout=15) as client:
        yield client
        client.post("/loadgen/stop")
    server.stop()


def test_a_configured_run_reports_every_device_and_finishes(stack, http, loadgen) -> None:
    started = loadgen.post("/loadgen/start", json=LOAD).json()
    assert started["running"] and started["offered_reports_per_second"] == 20
    assert loadgen.post("/loadgen/start", json=LOAD).status_code == 409
    assert wait_for(lambda: not loadgen.get("/loadgen").json()["running"], timeout=40)
    result = loadgen.get("/loadgen").json()["result"]
    assert result["acked"] == result["scheduled"] > 0
    devices = http.get("/devices/latest", headers={"X-User-ID": "alice"}).json()["items"]
    assert len({item["device_id"] for item in devices if item["device_id"].startswith("lg-")}) == 40


def test_a_stopped_run_ends_at_once_and_can_start_again(loadgen) -> None:
    loadgen.post("/loadgen/start", json=LOAD | {"duration_seconds": 600})
    stopped = loadgen.post("/loadgen/stop").json()
    assert not stopped["running"] and stopped["stopped_early"]
    assert loadgen.post("/loadgen/start", json=LOAD).json()["running"]


def test_load_outside_the_bounds_is_refused(loadgen) -> None:
    for change in ({"devices": 500_001}, {"interval_seconds": 0.5}, {"duration_seconds": 3601}):
        assert loadgen.post("/loadgen/start", json=LOAD | change).status_code == 422


def test_an_idle_generator_reports_nothing_running_and_stops_cleanly(loadgen) -> None:
    assert loadgen.get("/health/live").json() == {"status": "alive"}
    assert loadgen.get("/loadgen").json() == {"running": False}
    assert loadgen.post("/loadgen/stop").json() == {"running": False}


def test_a_run_reports_its_offered_rate_and_echoes_the_load(loadgen) -> None:
    load = LOAD | {"devices": 30, "interval_seconds": 3, "latitude": 10, "longitude": -20}
    started = loadgen.post("/loadgen/start", json=load).json()
    assert started["offered_reports_per_second"] == 10
    assert started["load"] == load and started["result"] is None
    assert loadgen.post("/loadgen/start", json={"devices": 1, "colour": "red"}).status_code == 422
