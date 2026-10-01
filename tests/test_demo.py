from tests.helpers import RIGA, collect, dashboard, zone


def test_demo_start_stop_owner_scope_and_real_alerts(stack, http) -> None:
    socket = dashboard(stack, "alice", RIGA)
    state = http.post("/demo/start", headers={"X-User-ID": "alice"}).json()
    assert state["running"] and state["devices"] == 6
    assert not http.get("/demo", headers={"X-User-ID": "bob"}).json()["running"]
    alerts = collect(socket, "inside_report", 1)
    assert alerts[0]["zone_id"] == state["zone_id"]
    assert collect(socket, "positions", 6)
    assert http.post("/demo/start", headers={"X-User-ID": "alice"}).json() == state
    assert http.post("/demo/stop", headers={"X-User-ID": "bob"}).status_code == 200
    assert http.get("/demo", headers={"X-User-ID": "alice"}).json()["running"]
    stopped = http.post("/demo/stop", headers={"X-User-ID": "alice"})
    assert stopped.status_code == 200 and not stopped.json()["running"]
    socket.close()


def test_restart_demo_reuses_zone_and_preserves_other_zones(stack, http) -> None:
    own = zone(http)
    first = http.post("/demo/start", headers={"X-User-ID": "alice"}).json()
    http.post("/demo/stop", headers={"X-User-ID": "alice"})
    second = http.post("/demo/start", headers={"X-User-ID": "alice"}).json()
    assert first["zone_id"] == second["zone_id"]
    zones = http.get("/geozones", headers={"X-User-ID": "alice"}).json()["items"]
    assert {item["id"] for item in zones} == {own["id"], first["zone_id"]}
    http.post("/demo/stop", headers={"X-User-ID": "alice"})
