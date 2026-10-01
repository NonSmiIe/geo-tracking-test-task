import asyncio

from geo_tracking.gateway import RESYNC, Connection, Gateway
from geo_tracking.metrics import GATEWAY
from geo_tracking.settings import Settings


class Socket:
    def __init__(self, stalled: bool = False):
        self.sent: list[str] = []
        self.stalled = stalled
        self.started = asyncio.Event()

    async def send_text(self, value: str) -> None:
        self.started.set()
        if self.stalled:
            await asyncio.Event().wait()
        self.sent.append(value)


def gateway(settings: Settings) -> Gateway:
    return Gateway(settings, None)


async def test_stalled_writer_times_out_without_blocking_its_sibling() -> None:
    settings = Settings(send_timeout_seconds=0.02)
    hub = gateway(settings)
    slow, fast = (
        Connection("alice", Socket(True), settings),
        Connection("alice", Socket(), settings),
    )
    hub.routes["fleet.alerts.x"] = {slow, fast}
    slow.writer = asyncio.create_task(slow.write())
    fast.writer = asyncio.create_task(fast.write())
    try:
        hub.deliver("fleet.alerts.x", b'{"type":"inside_report"}')
        await fast.socket.started.wait()
        await asyncio.sleep(0)
        assert fast.socket.sent == ['{"type":"inside_report"}']
        (outcome,) = await asyncio.gather(slow.writer, return_exceptions=True)
        assert isinstance(outcome, TimeoutError)
    finally:
        fast.writer.cancel()
        await asyncio.gather(fast.writer, return_exceptions=True)


async def test_backlog_overflow_evicts_only_the_full_connection() -> None:
    settings = Settings(websocket_queue_bytes=1024)
    hub = gateway(settings)
    full, healthy = Connection("alice", Socket(), settings), Connection("alice", Socket(), settings)
    labels = {"reason": "backlog_overflow"}
    before = GATEWAY.get_sample_value("fleet_gateway_evictions_total", labels) or 0
    full.enqueue(b"x" * 1000)
    hub.routes["fleet.pos.0"] = {full, healthy}
    hub.deliver("fleet.pos.0", b"y" * 100)
    assert full.reason == "backlog_overflow" and list(full.queue) == [b"x" * 1000]
    assert healthy.reason is None and list(healthy.queue) == [b"y" * 100]
    assert GATEWAY.get_sample_value("fleet_gateway_evictions_total", labels) == before + 1


async def test_resync_reaches_every_open_dashboard() -> None:
    settings = Settings()
    hub = gateway(settings)
    first, second = Connection("alice", Socket(), settings), Connection("bob", Socket(), settings)
    hub.connections = {"a": first, "b": second}
    await hub.resync()
    assert list(first.queue) == [RESYNC] and list(second.queue) == [RESYNC]
