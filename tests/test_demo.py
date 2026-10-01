from tests.test_api import zone


def test_demo_start_stop_owner_scope_and_real_alerts(client):
    with client.websocket_connect("/ws?user_id=alice") as socket:
        socket.receive_json()
        state = client.post("/demo/start", headers={"X-User-ID": "alice"}).json()
        assert state["running"] and state["devices"] == 6
        assert not client.get("/demo", headers={"X-User-ID": "bob"}).json()["running"]
        kinds = set()
        while "inside_report" not in kinds:
            message = socket.receive_json()
            kinds.add(message["type"])
            if message["type"] == "inside_report":
                assert all(item["zone_id"] == state["zone_id"] for item in message["items"])
        assert "locations" in kinds
        assert client.post("/demo/start", headers={"X-User-ID": "alice"}).json() == state
        assert client.post("/demo/stop", headers={"X-User-ID": "bob"}).status_code == 200
        assert client.get("/demo", headers={"X-User-ID": "alice"}).json()["running"]
        stopped = client.post("/demo/stop", headers={"X-User-ID": "alice"})
        assert stopped.status_code == 200 and not stopped.json()["running"]


def test_restart_demo_reuses_zone_and_preserves_other_zones(client):
    own = zone(client)
    first = client.post("/demo/start", headers={"X-User-ID": "alice"}).json()
    client.post("/demo/stop", headers={"X-User-ID": "alice"})
    second = client.post("/demo/start", headers={"X-User-ID": "alice"}).json()
    assert first["zone_id"] == second["zone_id"]
    zones = client.get("/geozones", headers={"X-User-ID": "alice"}).json()["items"]
    assert {item["id"] for item in zones} == {own["id"], first["zone_id"]}
