from datetime import UTC, datetime, timedelta

import pytest


def zone(client, owner="alice", **changes):
    payload = {"name": "Home", "latitude": 56.9496, "longitude": 24.1052, "radius_m": 100}
    response = client.post("/geozones", json=payload | changes, headers={"X-User-ID": owner})
    assert response.status_code == 201, response.text
    return response.json()


def report(device="device-1", offset=0, **changes):
    return {
        "device_id": device,
        "latitude": 56.9496,
        "longitude": 24.1052,
        "timestamp": (datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=offset)).isoformat(),
    } | changes


def test_owner_scoped_crud_and_version(client):
    created = zone(client)
    path = f"/geozones/{created['id']}"
    for method in ("get", "patch", "delete"):
        kwargs = {"json": {"radius_m": 200}} if method == "patch" else {}
        response = getattr(client, method)(path, headers={"X-User-ID": "bob"}, **kwargs)
        assert response.status_code == 404
    assert client.get("/geozones", headers={"X-User-ID": "bob"}).json()["items"] == []
    response = client.patch(
        path,
        headers={"X-User-ID": "alice"},
        json={"radius_m": 300, "latitude": 80, "longitude": 179.9},
    )
    assert response.status_code == 200
    updated = response.json()
    assert updated["version"] == 2 and updated["latitude"] == 80
    assert updated["longitude"] == 179.9 and updated["radius_m"] == 300
    assert client.delete(path, headers={"X-User-ID": "alice"}).status_code == 204
    assert client.get(path, headers={"X-User-ID": "alice"}).status_code == 404


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
def test_zone_invalid_values_rejected(client, payload):
    response = client.post(
        "/geozones",
        headers={"X-User-ID": "alice"},
        json={
            "name": "invalid",
            "latitude": 0,
            "longitude": 0,
            "radius_m": 1,
        }
        | payload,
    )
    assert response.status_code == 422


@pytest.mark.parametrize("payload", [{}, {"radius_m": None}, {"latitude": 0}, {"active": None}])
def test_invalid_patch(client, payload):
    created = zone(client)
    assert (
        client.patch(
            f"/geozones/{created['id']}", headers={"X-User-ID": "alice"}, json=payload
        ).status_code
        == 422
    )


def test_multiple_fresh_samples_duplicates_and_stale_state(client):
    samples = [report(offset=1), report(offset=2, longitude=25), report(offset=1, longitude=30)]
    response = client.post("/locations/batch", json=samples)
    assert response.status_code == 200, response.text
    assert response.json()["statuses"] == ["accepted", "accepted", "duplicate"]
    assert client.post("/locations", json=report(offset=0)).json()["statuses"] == ["stale"]
    assert client.post("/locations", json=report(offset=2)).json()["statuses"] == ["stale"]
    snapshot = client.get("/devices/latest", headers={"X-User-ID": "alice"}).json()["items"]
    assert len(snapshot) == 1 and snapshot[0]["longitude"] == 25
    assert snapshot[0]["timestamp"].startswith("2026-01-01T00:00:02")


def test_pagination_and_identity(client):
    assert client.get("/geozones").status_code == 422
    for name in ("one", "two", "three"):
        zone(client, name=name)
    first = client.get("/geozones?limit=2", headers={"X-User-ID": "alice"}).json()
    second = client.get(
        f"/geozones?limit=2&after={first['next_cursor']}", headers={"X-User-ID": "alice"}
    ).json()
    assert len(first["items"]) == 2 and len(second["items"]) == 1
    client.post("/locations/batch", json=[report(device=name) for name in ("a", "b", "c")])
    first = client.get("/devices/latest?limit=2", headers={"X-User-ID": "alice"}).json()
    second = client.get(
        f"/devices/latest?after={first['next_cursor']}", headers={"X-User-ID": "alice"}
    ).json()
    assert [item["device_id"] for item in first["items"] + second["items"]] == ["a", "b", "c"]


def test_report_validation_and_bounded_body(client):
    for payload in (
        report(latitude=100),
        report(timestamp="2026-01-01T00:00:00"),
        report(user_id="alice"),
    ):
        assert client.post("/locations", json=payload).status_code == 422
    assert client.post("/locations/batch", json=[]).status_code == 422
    assert client.post("/locations/batch", json=[report()] * 201).status_code == 422
    assert client.post("/locations", content=b"x" * 300000).status_code == 413


def test_two_sessions_private_alerts_and_disconnect_sibling(client):
    created = zone(client)
    zone(client, owner="bob", latitude=0, longitude=0)
    with (
        client.websocket_connect("/ws?user_id=alice") as laptop,
        client.websocket_connect("/ws?user_id=bob") as bob,
    ):
        laptop.receive_json()
        bob.receive_json()
        with client.websocket_connect("/ws?user_id=alice") as phone:
            phone.receive_json()
            response = client.post(
                "/locations/batch", json=[report(offset=1), report(offset=2, longitude=25)]
            )
            assert response.json()["statuses"] == ["accepted", "accepted"]
            for socket in (laptop, phone):
                assert len(socket.receive_json()["items"]) == 2
                alerts = socket.receive_json()
                assert alerts["type"] == "inside_report"
                assert [item["zone_id"] for item in alerts["items"]] == [created["id"]]
            assert bob.receive_json()["type"] == "locations"
        client.post("/locations", json=report(offset=3))
        assert laptop.receive_json()["type"] == "locations"
        assert laptop.receive_json()["type"] == "inside_report"
        assert bob.receive_json()["type"] == "locations"
        client.post("/locations", json=report(offset=4))
        assert laptop.receive_json()["type"] == "locations"
        assert laptop.receive_json()["type"] == "inside_report"
        assert bob.receive_json()["type"] == "locations"


def test_paused_and_deleted_zones_stop_future_alerts(client):
    created = zone(client)
    client.post("/locations", json=report(offset=1))
    assert client.get("/metrics").json()["counters"]["alerts_generated"] == 1
    client.patch(
        f"/geozones/{created['id']}", headers={"X-User-ID": "alice"}, json={"active": False}
    )
    client.post("/locations", json=report(offset=2))
    assert client.get("/metrics").json()["counters"]["alerts_generated"] == 1
    client.delete(f"/geozones/{created['id']}", headers={"X-User-ID": "alice"})
    client.post("/locations", json=report(offset=3))
    assert client.get("/metrics").json()["counters"]["alerts_generated"] == 1


def test_zone_edits_notify_both_owner_sessions_and_never_other_user(client):
    with (
        client.websocket_connect("/ws?user_id=alice") as laptop,
        client.websocket_connect("/ws?user_id=alice") as phone,
        client.websocket_connect("/ws?user_id=bob") as bob,
    ):
        for socket in (laptop, phone, bob):
            socket.receive_json()
        created = zone(client)
        for socket in (laptop, phone):
            assert socket.receive_json()["type"] == "zones_changed"
        client.patch(
            f"/geozones/{created['id']}", headers={"X-User-ID": "alice"}, json={"active": False}
        )
        for socket in (laptop, phone):
            assert socket.receive_json()["type"] == "zones_changed"
        client.delete(f"/geozones/{created['id']}", headers={"X-User-ID": "alice"})
        for socket in (laptop, phone):
            assert socket.receive_json()["type"] == "zones_changed"
        client.post("/locations", json=report())
        assert bob.receive_json()["type"] == "locations"
