import httpx
import pytest
from prometheus_client.parser import text_string_to_metric_families

from tests.helpers import report, wait_for, zone


def test_owner_scoped_crud_and_version(http: httpx.Client) -> None:
    created = zone(http)
    path = f"/geozones/{created['id']}"
    for method in ("get", "patch", "delete"):
        kwargs = {"json": {"radius_m": 200}} if method == "patch" else {}
        response = getattr(http, method)(path, headers={"X-User-ID": "bob"}, **kwargs)
        assert response.status_code == 404
    assert http.get("/geozones", headers={"X-User-ID": "bob"}).json()["items"] == []
    response = http.patch(
        path,
        headers={"X-User-ID": "alice"},
        json={"radius_m": 300, "latitude": 80, "longitude": 179.9},
    )
    assert response.status_code == 200
    updated = response.json()
    assert updated["version"] == 2 and updated["latitude"] == 80
    assert updated["longitude"] == 179.9 and updated["radius_m"] == 300
    assert http.delete(path, headers={"X-User-ID": "alice"}).status_code == 204
    assert http.get(path, headers={"X-User-ID": "alice"}).status_code == 404


@pytest.mark.parametrize(
    "payload",
    [
        {"radius_m": 0},
        {"radius_m": "Infinity"},
        {"radius_m": "NaN"},
        {"latitude": 91},
        {"longitude": -181},
        {"user_id": "bob"},
    ],
)
def test_zone_invalid_values_rejected(http: httpx.Client, payload: dict) -> None:
    body = {"name": "invalid", "latitude": 0, "longitude": 0, "radius_m": 1} | payload
    assert http.post("/geozones", headers={"X-User-ID": "alice"}, json=body).status_code == 422


@pytest.mark.parametrize("payload", [{}, {"radius_m": None}, {"latitude": 0}, {"active": None}])
def test_invalid_patch(http: httpx.Client, payload: dict) -> None:
    created = zone(http)
    response = http.patch(
        f"/geozones/{created['id']}", headers={"X-User-ID": "alice"}, json=payload
    )
    assert response.status_code == 422


def test_zone_quota_is_per_user(http: httpx.Client, stack) -> None:
    stack.settings.max_zones_per_user = 2
    zone(http)
    zone(http)
    response = http.post(
        "/geozones",
        headers={"X-User-ID": "alice"},
        json={"name": "third", "latitude": 0, "longitude": 0, "radius_m": 1},
    )
    assert response.status_code == 409 and response.json()["detail"] == "zone_quota_exceeded"
    zone(http, owner="bob")


def test_pagination_identity_and_bounding_box_snapshot(http: httpx.Client) -> None:
    assert http.get("/geozones").status_code == 422
    for name in ("one", "two", "three"):
        zone(http, name=name)
    first = http.get("/geozones?limit=2", headers={"X-User-ID": "alice"}).json()
    second = http.get(
        f"/geozones?limit=2&after={first['next_cursor']}", headers={"X-User-ID": "alice"}
    ).json()
    assert len(first["items"]) == 2 and len(second["items"]) == 1
    reports = [
        report("a"),
        report("b"),
        report("c"),
        report("east", latitude=-15, longitude=179.5),
        report("west", latitude=-15, longitude=-179.5),
    ]
    assert http.post("/locations/batch", json=reports).status_code == 202
    headers = {"X-User-ID": "alice"}
    wait_for(lambda: len(http.get("/devices/latest", headers=headers).json()["items"]) == 5)
    first = http.get("/devices/latest?limit=2", headers=headers).json()
    rest = http.get(f"/devices/latest?after={first['next_cursor']}", headers=headers).json()
    assert [item["device_id"] for item in first["items"] + rest["items"]] == [
        "a",
        "b",
        "c",
        "east",
        "west",
    ]
    riga = http.get("/devices/latest?south=56&west=23&north=58&east=25", headers=headers).json()
    assert {item["device_id"] for item in riga["items"]} == {"a", "b", "c"}
    wrapped = http.get(
        "/devices/latest?south=-20&west=179&north=-10&east=-179", headers=headers
    ).json()
    assert {item["device_id"] for item in wrapped["items"]} == {"east", "west"}
    assert http.get("/devices/latest?south=1", headers=headers).status_code == 422


def test_report_validation_and_bounded_body(http: httpx.Client) -> None:
    for payload in (
        report(latitude=100),
        report(timestamp="2026-01-01T00:00:00"),
        report(user_id="alice"),
        report(timestamp="2999-01-01T00:00:00+00:00"),
    ):
        assert http.post("/locations", json=payload).status_code == 422
    assert http.post("/locations/batch", json=[]).status_code == 422
    assert http.post("/locations/batch", json=[report()] * 201).status_code == 422
    assert http.post("/locations", content=b"x" * 300000).status_code == 413
    assert http.post("/locations", json=report()).json() == {"accepted": 1}


def samples(text: str) -> dict[tuple[str, tuple], float]:
    return {
        (sample.name, tuple(sorted(sample.labels.items()))): sample.value
        for family in text_string_to_metric_families(text)
        for sample in family.samples
    }


def test_health_and_prometheus_exposition_on_every_role(stack, http: httpx.Client) -> None:
    assert http.get("/health/live").json() == {"status": "alive"}
    accepted = ("fleet_ingest_reports_total", (("outcome", "accepted"), ("transport", "http")))
    before = samples(http.get("/metrics").text).get(accepted, 0)
    batch = [report(offset=1), report(offset=2)]
    assert http.post("/locations/batch", json=batch).status_code == 202
    assert samples(http.get("/metrics").text)[accepted] == before + 2
    processor = f"http://127.0.0.1:{stack.settings.metrics_port}"
    committed = ("fleet_processor_records_total", (("outcome", "committed"),))
    assert wait_for(lambda: samples(httpx.get(processor + "/metrics").text).get(committed, 0) >= 2)
    assert httpx.get(processor + "/health/live").json() == {"status": "alive"}
    gateway = stack.ws_url.replace("ws", "http", 1)
    assert ("fleet_gateway_connections", ()) in samples(httpx.get(gateway + "/metrics").text)
