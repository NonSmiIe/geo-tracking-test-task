import asyncio

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from geo_tracking.events import Frame
from geo_tracking.main import create_app
from geo_tracking.metrics import Metrics
from geo_tracking.sessions import Connection, Sessions
from tests.test_api import report, zone


class Socket:
    def __init__(self, stalled=False):
        self.sent = []
        self.stalled = stalled
        self.started = asyncio.Event()

    async def send_text(self, value):
        self.started.set()
        if self.stalled:
            await asyncio.Event().wait()
        self.sent.append(value)


class HandshakeSocket(Socket):
    def __init__(self):
        super().__init__()
        self.release = asyncio.Event()
        self.accept_started = asyncio.Event()
        self.closed = False

    async def accept(self):
        self.accept_started.set()
        await self.release.wait()

    async def receive_text(self):
        await asyncio.Event().wait()

    async def close(self, **kwargs):
        self.closed = True


async def test_handshake_reservation_never_publishes_an_unmanaged_session(settings):
    settings = settings.model_copy(update={"max_connections": 1})
    sessions = Sessions(settings, Metrics())
    socket = HandshakeSocket()
    serving = asyncio.create_task(sessions.serve(socket, "alice"))
    try:
        await socket.accept_started.wait()
        assert sessions.count == 0 and sessions.opening == 1
        rejected = HandshakeSocket()
        await sessions.serve(rejected, "bob")
        assert rejected.closed and not rejected.accept_started.is_set()
        await sessions.broadcast((Frame(b"location"),), {})
        socket.release.set()
        await socket.started.wait()
        assert sessions.count == 1 and sessions.opening == 0
        assert '"type":"ready"' in socket.sent[0]
        await sessions.close()
        await asyncio.wait_for(serving, 1)
        assert sessions.count == 0 and socket.closed
    finally:
        serving.cancel()
        await asyncio.gather(serving, return_exceptions=True)


async def test_stalled_writer_does_not_block_healthy_session(settings):
    settings = settings.model_copy(update={"send_timeout_seconds": 0.02})
    sessions = Sessions(settings, Metrics())
    slow, fast = (
        Connection("alice", Socket(True), settings),
        Connection("alice", Socket(), settings),
    )
    sessions.users["alice"] = {slow.id: slow, fast.id: fast}
    slow.writer_task = asyncio.create_task(slow.write())
    fast.writer_task = asyncio.create_task(fast.write())
    try:
        await sessions.broadcast((Frame(b"location"),), {"alice": (Frame(b"alert"),)})
        await fast.socket.started.wait()
        await asyncio.sleep(0)
        assert fast.socket.sent == ["location", "alert"]
        with pytest.raises(TimeoutError):
            await slow.writer_task
    finally:
        fast.writer_task.cancel()
        await asyncio.gather(fast.writer_task, return_exceptions=True)


async def test_ready_send_overflow_closes_and_never_resurrects_connection(settings):
    settings = settings.model_copy(update={"websocket_queue_bytes": 100})
    sessions = Sessions(settings, Metrics())
    socket = HandshakeSocket()
    socket.stalled = True
    socket.release.set()
    serving = asyncio.create_task(sessions.serve(socket, "alice"))
    try:
        await socket.started.wait()
        assert sessions.count == 1
        await sessions.broadcast((Frame(b"x" * 30),), {})
        await sessions.broadcast((Frame(b"y" * 30),), {})
        await asyncio.wait_for(serving, 1)
        assert sessions.count == 0 and sessions.opening == 0
        assert socket.closed and not socket.sent
    finally:
        serving.cancel()
        await asyncio.gather(serving, return_exceptions=True)


async def test_full_burst_rejected_atomically_and_sibling_kept(settings):
    settings = settings.model_copy(update={"websocket_queue_bytes": 20})
    metrics = Metrics()
    sessions = Sessions(settings, metrics)
    slow, fast = Connection("alice", Socket(), settings), Connection("alice", Socket(), settings)
    slow.enqueue((Frame(b"old" + b" " * 12),))
    sessions.users["alice"] = {slow.id: slow, fast.id: fast}
    await sessions.broadcast((Frame(b"location"),), {"alice": (Frame(b"alert"),)})
    assert slow.reason == "backlog_overflow"
    assert tuple(frame.data.strip() for frame in slow.queue) == (b"old",)
    assert tuple(frame.data for frame in fast.queue) == (b"location", b"alert")
    assert sessions.count == 1 and fast.id in sessions.users["alice"]


def test_output_overload_rolls_back_and_emits_no_alerts(settings):
    settings = settings.model_copy(update={"max_matches": 1})
    with TestClient(create_app(settings)) as client:
        zone(client, name="one")
        zone(client, name="two")
        response = client.post("/locations", json=report())
        assert response.status_code == 503
        assert response.json()["detail"] == "fanout_budget_exceeded"
        assert client.get("/devices/latest", headers={"X-User-ID": "alice"}).json()["items"] == []
        assert client.get("/metrics").json()["counters"].get("alerts_generated", 0) == 0


def test_database_error_returns_503_and_recovers(client):
    async def block():
        async with client.app.state.db.engine.begin() as connection:
            await connection.execute(
                text("ALTER TABLE device_latest ADD CHECK (device_id <> 'blocked')")
            )

    client.portal.call(block)
    assert client.post("/locations", json=report(device="blocked")).status_code == 503
    assert client.get("/devices/latest", headers={"X-User-ID": "alice"}).json()["items"] == []
    assert client.post("/locations", json=report(device="works")).status_code == 200

    async def unblock():
        async with client.app.state.db.engine.begin() as connection:
            await connection.execute(
                text("ALTER TABLE device_latest DROP CONSTRAINT device_latest_device_id_check")
            )

    client.portal.call(unblock)


def test_restart_preserves_watermarks_and_zones(settings):
    with TestClient(create_app(settings)) as first:
        created = zone(first)
        assert first.post("/locations", json=report(offset=3)).status_code == 200
    with TestClient(create_app(settings)) as second:
        assert second.post("/locations", json=report(offset=2)).json()["statuses"] == ["stale"]
        assert (
            second.get(f"/geozones/{created['id']}", headers={"X-User-ID": "alice"}).status_code
            == 200
        )


def test_driver_connection_error_is_503_and_pipeline_recovers(client, monkeypatch):
    def refused(self):
        raise ConnectionRefusedError("database fault probe")

    with monkeypatch.context() as patch:
        patch.setattr(type(client.app.state.db.engine.sync_engine), "connect", refused)
        assert client.get("/health/ready").status_code == 503
        assert client.post("/locations", json=report()).status_code == 503
        assert client.get("/geozones", headers={"X-User-ID": "alice"}).status_code == 503
        assert client.app.state.pipeline.running
    assert client.get("/health/ready").status_code == 200
    assert client.post("/locations", json=report()).status_code == 200


def test_processor_failure_fails_pending_requests_and_readiness(client):
    async def failure():
        pipeline = client.app.state.pipeline
        pipeline.task.cancel()
        await asyncio.gather(pipeline.task, return_exceptions=True)

    client.portal.call(failure)
    assert client.get("/health/ready").status_code == 503
    assert client.get("/health/live").status_code == 200
    assert client.post("/locations", json=report()).status_code == 503
