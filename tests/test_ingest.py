import asyncio

import orjson
import pytest
from aiokafka.errors import KafkaTimeoutError

from geo_tracking.ingest import Acknowledgements, Ingest, Overloaded, Window
from geo_tracking.metrics import Metrics
from geo_tracking.schemas import ReportAdapter
from geo_tracking.settings import Settings
from tests.helpers import report


def test_acknowledgement_counts_only_the_contiguous_durable_prefix() -> None:
    acks = Acknowledgements()
    first, second, third = acks.issue(), acks.issue(), acks.issue()
    acks.settle(third)
    assert acks.count == 0 and not acks.idle.is_set()
    acks.settle(first)
    assert acks.count == 1
    acks.settle(second)
    assert acks.count == 3 and acks.idle.is_set()


class FailingProducer:
    def __init__(self, raise_on_send: bool):
        self.raise_on_send = raise_on_send

    async def send(self, *args: object, **kwargs: object) -> asyncio.Future:
        if self.raise_on_send:
            raise KafkaTimeoutError()
        future: asyncio.Future = asyncio.get_running_loop().create_future()
        future.set_exception(KafkaTimeoutError())
        return future


class DeviceSocket:
    def __init__(self, messages: list[str]):
        self.messages = messages
        self.sent: list[str] = []
        self.closed: int | None = None

    async def accept(self) -> None:
        pass

    async def receive_text(self) -> str:
        if self.closed is not None or not self.messages:
            await asyncio.Event().wait()
        return self.messages.pop(0)

    async def send_text(self, value: str) -> None:
        self.sent.append(value)

    async def close(self, code: int, reason: str) -> None:
        self.closed = code


def ingest(raise_on_send: bool, window: int = 4) -> Ingest:
    settings = Settings(produce_window=window, admission_timeout_seconds=0.05)
    return Ingest(settings, FailingProducer(raise_on_send), Metrics("test"))


@pytest.mark.parametrize("raise_on_send", [True, False])
async def test_failed_http_produce_returns_its_window(raise_on_send: bool) -> None:
    service = ingest(raise_on_send)
    for _ in range(3):
        with pytest.raises(KafkaTimeoutError):
            await service.publish([ReportAdapter.validate_python(report())] * 4)
    assert service.inflight == 0
    with pytest.raises(Overloaded):
        await service.publish([ReportAdapter.validate_python(report())] * 5)


@pytest.mark.parametrize("raise_on_send", [True, False])
async def test_failed_stream_produce_closes_the_socket_and_returns_the_window(
    raise_on_send: bool,
) -> None:
    service = ingest(raise_on_send)
    socket = DeviceSocket([orjson.dumps(report()).decode(), '{"type":"flush"}'])
    await asyncio.wait_for(service.stream(socket), 2)
    await asyncio.sleep(0)
    assert socket.closed == 1011 and service.inflight == 0


async def test_window_grants_in_order_and_a_large_request_is_not_starved() -> None:
    window = Window(4)
    await window.acquire(3)
    big = asyncio.create_task(window.acquire(4))
    await asyncio.sleep(0)
    small = asyncio.create_task(window.acquire(1))
    await asyncio.sleep(0)
    assert not big.done() and not small.done()
    window.release(3)
    await asyncio.sleep(0)
    assert big.done() and not small.done()
    window.release(4)
    await asyncio.sleep(0)
    assert small.done() and window.free == 3


async def test_window_cancellation_returns_nothing_it_did_not_take() -> None:
    window = Window(2)
    await window.acquire(2)
    waiting = asyncio.create_task(window.acquire(2))
    await asyncio.sleep(0)
    waiting.cancel()
    await asyncio.gather(waiting, return_exceptions=True)
    window.release(2)
    assert window.free == 2 and not window.waiters
