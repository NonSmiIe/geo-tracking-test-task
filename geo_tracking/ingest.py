import asyncio
from collections import deque
from collections.abc import Sequence
from functools import partial

import orjson
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from aiokafka.structs import RecordMetadata
from pydantic import ValidationError
from starlette.websockets import WebSocket, WebSocketDisconnect

from geo_tracking.metrics import Metrics
from geo_tracking.schemas import Report, ReportAdapter
from geo_tracking.settings import Settings


class Overloaded(Exception):
    pass


class Acknowledgements:
    def __init__(self) -> None:
        self.count = 0
        self.issued = 0
        self.settled: set[int] = set()
        self.failed = False
        self.idle = asyncio.Event()
        self.idle.set()

    def issue(self) -> int:
        self.idle.clear()
        self.issued += 1
        return self.issued - 1

    def settle(self, sequence: int) -> None:
        self.settled.add(sequence)
        while self.count in self.settled:
            self.settled.remove(self.count)
            self.count += 1
        if self.count == self.issued:
            self.idle.set()

    def fail(self) -> None:
        self.failed = True
        self.idle.set()


class Window:
    def __init__(self, size: int) -> None:
        self.free = size
        self.waiters: deque[tuple[int, asyncio.Future[None]]] = deque()

    async def acquire(self, count: int) -> None:
        if not self.waiters and self.free >= count:
            self.free -= count
            return
        grant: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self.waiters.append((count, grant))
        try:
            await grant
        except BaseException:
            if grant.done() and not grant.cancelled():
                self.release(count)
            else:
                self.waiters.remove((count, grant))
                self.wake()
            raise

    def release(self, count: int) -> None:
        self.free += count
        self.wake()

    def wake(self) -> None:
        while self.waiters and self.waiters[0][0] <= self.free:
            count, grant = self.waiters.popleft()
            self.free -= count
            grant.set_result(None)


class Ingest:
    def __init__(self, settings: Settings, producer: AIOKafkaProducer, metrics: Metrics) -> None:
        self.settings, self.producer, self.metrics = settings, producer, metrics
        self.window = Window(settings.produce_window)
        self.inflight = 0
        self.closers: set[asyncio.Task[None]] = set()
        metrics.gauges["produce_inflight"] = lambda: self.inflight

    async def admit(self, count: int) -> None:
        await self.window.acquire(count)
        self.inflight += count
        self.metrics.high_water("produce_inflight_high_water", self.inflight)

    def release(self, count: int) -> None:
        self.inflight -= count
        self.window.release(count)

    async def send(self, report: Report) -> asyncio.Future[RecordMetadata]:
        return await self.producer.send(
            self.settings.kafka_topic,
            value=orjson.dumps(report.record()),
            key=report.device_id.encode(),
        )

    async def publish(self, reports: Sequence[Report]) -> None:
        try:
            async with asyncio.timeout(self.settings.admission_timeout_seconds):
                await self.admit(len(reports))
        except TimeoutError:
            self.metrics.counts["ingest_rejected"] += len(reports)
            raise Overloaded from None
        try:
            await asyncio.gather(*[await self.send(report) for report in reports])
        finally:
            self.release(len(reports))
        self.metrics.counts["reports_ingested"] += len(reports)

    async def stream(self, socket: WebSocket) -> None:
        await socket.accept()
        own = asyncio.Semaphore(self.settings.ingest_window)
        acks = Acknowledgements()

        def broken() -> None:
            acks.fail()
            closer = asyncio.create_task(socket.close(code=1011, reason="produce_failed"))
            self.closers.add(closer)
            closer.add_done_callback(self.closers.discard)

        def settled(sequence: int, future: asyncio.Future[RecordMetadata]) -> None:
            own.release()
            self.release(1)
            if future.cancelled() or future.exception() is not None:
                broken()
                return
            acks.settle(sequence)
            self.metrics.counts["reports_ingested"] += 1

        async def acknowledge() -> None:
            await socket.send_text(orjson.dumps({"type": "ack", "count": acks.count}).decode())

        async def periodic() -> None:
            sent = 0
            while True:
                await asyncio.sleep(self.settings.ack_interval_seconds)
                if acks.count != sent:
                    sent = acks.count
                    await acknowledge()

        ticker = asyncio.create_task(periodic())
        try:
            while not acks.failed:
                message = orjson.loads(await socket.receive_text())
                if isinstance(message, dict) and message.get("type") == "flush":
                    await acks.idle.wait()
                    if not acks.failed:
                        await acknowledge()
                    continue
                report = ReportAdapter.validate_python(message)
                await own.acquire()
                await self.admit(1)
                sequence = acks.issue()
                try:
                    future = await self.send(report)
                except KafkaError:
                    own.release()
                    self.release(1)
                    broken()
                    break
                future.add_done_callback(partial(settled, sequence))
        except (ValidationError, orjson.JSONDecodeError):
            self.metrics.counts["ingest_invalid"] += 1
            await socket.close(code=1007, reason="invalid_report")
        except (WebSocketDisconnect, OSError, RuntimeError):
            pass
        finally:
            ticker.cancel()
            await asyncio.gather(ticker, return_exceptions=True)
