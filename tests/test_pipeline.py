import asyncio

import httpx
import orjson
import pytest
from sqlalchemy import text
from websockets.exceptions import ConnectionClosed
from websockets.sync.client import connect

from geo_tracking.db import Database
from geo_tracking.metrics import PROCESSOR
from geo_tracking.processor import Processor, PublishStalled
from tests.helpers import RIGA, collect, dashboard, look, micros, report, silent, wait_for, zone


def latest(http: httpx.Client, device: str) -> dict | None:
    items = http.get("/devices/latest", headers={"X-User-ID": "alice"}).json()["items"]
    return next((item for item in items if item["device_id"] == device), None)


def test_private_alerts_reach_every_owner_session_and_never_another_user(stack, http) -> None:
    created = zone(http)
    zone(http, owner="bob", latitude=0, longitude=0)
    laptop, phone, bob = (dashboard(stack, user) for user in ("alice", "alice", "bob"))
    http.post("/locations/batch", json=[report(offset=1), report(offset=2, longitude=24.1053)])
    for socket in (laptop, phone):
        assert len(collect(socket, "positions", 2)) == 2
        alerts = collect(socket, "inside_report", 2)
        assert {item["zone_id"] for item in alerts} == {created["id"]}
        assert {item["timestamp"] for item in alerts} == {micros(1), micros(2)}
    assert len(collect(bob, "positions", 2)) == 2
    silent(bob, "inside_report")
    phone.close()
    http.post("/locations", json=report(offset=3))
    assert collect(laptop, "inside_report", 1)[0]["timestamp"] == micros(3)
    for socket in (laptop, bob):
        socket.close()


def test_duplicates_and_stale_reports_change_nothing(stack, http) -> None:
    zone(http)
    socket = dashboard(stack, "alice")
    batch = [report(offset=1), report(offset=2, longitude=24.1053), report(offset=1, longitude=30)]
    assert http.post("/locations/batch", json=batch).status_code == 202
    assert len(collect(socket, "inside_report", 2)) == 2
    http.post("/locations", json=report(offset=0))
    http.post("/locations", json=report(offset=2, longitude=24.2))
    silent(socket, "inside_report")
    snapshot = latest(http, "device-1")
    assert snapshot["longitude"] == 24.1053
    assert snapshot["timestamp"].startswith("2026-01-01T00:00:02")
    socket.close()


def test_a_resent_report_is_stale_whether_identical_or_conflicting(stack, http) -> None:
    zone(http)
    socket = dashboard(stack, "alice")
    http.post("/locations", json=report(offset=1))
    assert collect(socket, "inside_report", 1)[0]["timestamp"] == micros(1)
    http.post("/locations", json=report(offset=1))
    http.post("/locations", json=report(offset=1, longitude=24.1053))
    silent(socket, "inside_report")
    assert latest(http, "device-1")["longitude"] == 24.1052
    socket.close()


@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
def test_a_crash_between_commit_and_publish_replays_every_sample(stack, http, monkeypatch) -> None:
    zone(http)
    socket = dashboard(stack, "alice")
    delivered = Processor.publish

    async def stalled(self: Processor, messages: list) -> None:
        raise PublishStalled

    monkeypatch.setattr(Processor, "publish", stalled)
    http.post("/locations/batch", json=[report(offset=1), report(offset=2, latitude=57.5)])
    assert wait_for(lambda: (latest(http, "device-1") or {}).get("latitude") == 57.5)
    silent(socket, "positions", 0.5)
    monkeypatch.setattr(Processor, "publish", delivered)
    stack.restart_processor()
    assert [item[3] for item in collect(socket, "positions", 2)] == [micros(1), micros(2)]
    assert [item["timestamp"] for item in collect(socket, "inside_report", 1)] == [micros(1)]
    http.post("/locations", json=report(offset=2, latitude=57.5))
    silent(socket, "positions", 0.5)
    socket.close()


def test_inside_then_outside_in_one_batch_keeps_the_inside_alert(stack, http) -> None:
    zone(http)
    socket = dashboard(stack, "alice")
    http.post("/locations/batch", json=[report(offset=1), report(offset=2, latitude=57.5)])
    alerts = collect(socket, "inside_report", 1)
    assert alerts[0]["timestamp"] == micros(1)
    assert wait_for(lambda: (latest(http, "device-1") or {}).get("latitude") == 57.5)
    socket.close()


def test_dense_overlap_never_rejects_ingestion(stack, http) -> None:
    for index in range(60):
        zone(http, name=f"overlap-{index}", radius_m=10000)
    owner = dashboard(stack, "alice")
    reports = [report(f"device-{i}", offset=1, latitude=56.95 if i % 4 else 10) for i in range(200)]
    statuses = {http.post("/locations", json=item).status_code for item in reports}
    assert statuses == {202}
    alerts = collect(owner, "inside_report", 150 * 60, timeout=30)
    assert len({(item["device_id"], item["zone_id"]) for item in alerts}) == 150 * 60
    owner.close()


def test_paused_and_deleted_zones_stop_future_alerts(stack, http) -> None:
    created = zone(http)
    socket = dashboard(stack, "alice")
    http.post("/locations", json=report(offset=1))
    collect(socket, "inside_report", 1)
    http.patch(f"/geozones/{created['id']}", headers={"X-User-ID": "alice"}, json={"active": False})
    http.post("/locations", json=report(offset=2))
    collect(socket, "positions", 1)
    silent(socket, "inside_report", 0.5)
    http.delete(f"/geozones/{created['id']}", headers={"X-User-ID": "alice"})
    http.post("/locations", json=report(offset=3))
    collect(socket, "positions", 1)
    silent(socket, "inside_report", 0.5)
    socket.close()


def test_zone_edits_notify_both_owner_sessions_and_never_another_user(stack, http) -> None:
    laptop, phone, bob = (dashboard(stack, user, None) for user in ("alice", "alice", "bob"))
    created = zone(http)
    http.patch(f"/geozones/{created['id']}", headers={"X-User-ID": "alice"}, json={"active": False})
    http.delete(f"/geozones/{created['id']}", headers={"X-User-ID": "alice"})
    for socket in (laptop, phone):
        assert len(collect(socket, "zones_changed", 3)) == 3
    silent(bob, "zones_changed")
    for socket in (laptop, phone, bob):
        socket.close()


def test_viewport_limits_positions_and_follows_retargeting(stack, http) -> None:
    socket = dashboard(stack, "alice", RIGA)
    http.post("/locations", json=report("far", offset=1, latitude=10, longitude=-40))
    http.post("/locations", json=report("near", offset=1))
    assert [item[0] for item in collect(socket, "positions", 1)] == ["near"]
    look(socket, {"south": 9, "west": -41, "north": 11, "east": -39})
    http.post("/locations", json=report("far", offset=2, latitude=10, longitude=-40))
    http.post("/locations", json=report("near", offset=2))
    assert [item[0] for item in collect(socket, "positions", 1)] == ["far"]
    silent(socket, "positions", 0.5)
    socket.close()


def test_device_websocket_acknowledges_every_report_and_rejects_invalid(stack, http) -> None:
    observer = dashboard(stack, "alice")
    with connect(stack.ingest_url) as device:
        for index in range(50):
            device.send(orjson.dumps(report(f"stream-{index}", offset=1)).decode())
        device.send('{"type":"flush"}')
        acks = 0
        while acks < 50:
            message = orjson.loads(device.recv(timeout=10))
            assert message["type"] == "ack"
            acks = message["count"]
        assert acks == 50
    assert len(collect(observer, "positions", 50)) == 50
    with connect(stack.ingest_url) as device:
        device.send(orjson.dumps(report(latitude=100)).decode())
        with pytest.raises(ConnectionClosed) as closed:
            device.recv(timeout=5)
        assert closed.value.rcvd.code == 1007
    observer.close()


def test_database_failure_replays_the_batch_after_recovery(stack, http) -> None:
    async def alter(statement: str) -> None:
        db = Database(stack.settings)
        try:
            async with db.engine.begin() as connection:
                await connection.execute(text(statement))
        finally:
            await db.close()

    asyncio.run(alter("ALTER TABLE device_latest ADD CONSTRAINT blocked CHECK (device_id <> 'x')"))
    socket = dashboard(stack, "alice")

    def failed() -> float:
        labels = {"outcome": "failed"}
        return PROCESSOR.get_sample_value("fleet_processor_batches_total", labels) or 0

    before = failed()
    try:
        assert http.post("/locations", json=report("x", offset=1)).status_code == 202
        assert wait_for(lambda: failed() > before)
        silent(socket, "positions", 0.5)
    finally:
        asyncio.run(alter("ALTER TABLE device_latest DROP CONSTRAINT blocked"))
    assert [item[0] for item in collect(socket, "positions", 1)] == ["x"]
    assert latest(http, "x") is not None
    socket.close()


def test_processor_restart_preserves_watermarks(stack, http) -> None:
    zone(http)
    socket = dashboard(stack, "alice")
    http.post("/locations", json=report(offset=3))
    collect(socket, "inside_report", 1)
    stack.restart_processor()
    http.post("/locations", json=report(offset=2))
    http.post("/locations", json=report(offset=4))
    assert collect(socket, "inside_report", 1)[0]["timestamp"] == micros(4)
    silent(socket, "inside_report", 0.5)
    socket.close()
