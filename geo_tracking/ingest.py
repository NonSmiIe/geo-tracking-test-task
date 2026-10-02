import asyncio
from collections import deque
from collections.abc import Sequence
from functools import partial
from time import monotonic

import msgspec
import orjson
from starlette.websockets import WebSocket, WebSocketDisconnect

from geo_tracking.bus import ProduceFailed, Producer
from geo_tracking.metrics import INGESTED, WINDOW_SIZE, WINDOW_USED
from geo_tracking.schemas import Flush, Report, frame_decoder
from geo_tracking.settings import Settings

HTTP_ACCEPTED = INGESTED.labels("http", "accepted")
HTTP_OVERLOADED = INGESTED.labels("http", "overloaded")
HTTP_FAILED = INGESTED.labels("http", "failed")
STREAM_ACCEPTED = INGESTED.labels("ws", "accepted")
STREAM_INVALID = INGESTED.labels("ws", "invalid")
STREAM_FAILED = INGESTED.labels("ws", "failed")


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


class TokenBucket:
    def __init__(self, rate: float, burst: float) -> None:
        self.rate, self.burst = rate, burst
        self.tokens = burst
        self.stamp = monotonic()

    async def take(self, count: int) -> None:
        while True:
            now = monotonic()
            self.tokens = min(self.burst, self.tokens + (now - self.stamp) * self.rate)
            self.stamp = now
            if self.tokens >= count:
                self.tokens -= count
                return
            await asyncio.sleep((count - self.tokens) / self.rate)


class Window:
    def __init__(self, size: int) -> None:
        self.size = self.free = size
        self.waiters: deque[tuple[int, asyncio.Future[None]]] = deque()

    async def acquire(self, count: int) -> None:
        if not self.waiters and self.free >= count:
            self.free -= count
            return
        grant: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self.waiters.append((count, grant))
        self.wake()
        try:
            await grant
        except BaseException:
            if grant.done() and not grant.cancelled():
                self.release(count)
            else:
                self.wake()
            raise

    def release(self, count: int) -> None:
        self.free += count
        self.wake()

    def wake(self) -> None:
        while self.waiters:
            count, grant = self.waiters[0]
            if grant.done():
                self.waiters.popleft()
                continue
            if count > self.free:
                return
            self.waiters.popleft()
            self.free -= count
            grant.set_result(None)


class Ingest:
    def __init__(self, settings: Settings, producer: Producer) -> None:
        self.settings, self.producer = settings, producer
        self.window = Window(settings.produce_window)
        self.closers: set[asyncio.Task[None]] = set()
        WINDOW_SIZE.set(self.window.size)
        WINDOW_USED.set_function(lambda: self.window.size - self.window.free)

    def send(self, report: Report) -> asyncio.Future[None]:
        return self.producer.send(
            self.settings.kafka_topic, report.device_id.encode(), orjson.dumps(report.record())
        )

    async def produce(self, reports: Sequence[Report]) -> None:
        try:
            async with asyncio.timeout(self.settings.admission_timeout_seconds):
                await self.window.acquire(len(reports))
        except TimeoutError:
            HTTP_OVERLOADED.inc(len(reports))
            raise Overloaded from None
        try:
            await asyncio.gather(*[self.send(report) for report in reports])
        except BaseException:
            HTTP_FAILED.inc(len(reports))
            raise
        finally:
            self.window.release(len(reports))
        HTTP_ACCEPTED.inc(len(reports))

    async def stream(self, socket: WebSocket) -> None:
        await socket.accept()
        own = Window(self.settings.ingest_window)
        bucket = TokenBucket(self.settings.socket_reports_per_second, self.settings.socket_burst)
        acks = Acknowledgements()
        frames = frame_decoder(self.settings.batch_reports)

        def broken() -> None:
            acks.fail()
            closer = asyncio.create_task(socket.close(code=1011, reason="produce_failed"))
            self.closers.add(closer)
            closer.add_done_callback(self.closers.discard)

        def settled(sequence: int, future: asyncio.Future[None]) -> None:
            own.release(1)
            self.window.release(1)
            if future.cancelled() or future.exception() is not None:
                STREAM_FAILED.inc()
                broken()
                return
            acks.settle(sequence)
            STREAM_ACCEPTED.inc()

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
                frame = frames.decode(await socket.receive_text())
                if isinstance(frame, Flush):
                    await acks.idle.wait()
                    if not acks.failed:
                        await acknowledge()
                    continue
                await bucket.take(len(frame))
                await own.acquire(len(frame))
                await self.window.acquire(len(frame))
                for index, report in enumerate(frame):
                    sequence = acks.issue()
                    try:
                        future = self.send(report)
                    except ProduceFailed:
                        unsent = len(frame) - index
                        STREAM_FAILED.inc(unsent)
                        own.release(unsent)
                        self.window.release(unsent)
                        broken()
                        break
                    future.add_done_callback(partial(settled, sequence))
        except msgspec.DecodeError:
            STREAM_INVALID.inc()
            await socket.close(code=1007, reason="invalid_report")
        except (WebSocketDisconnect, OSError, RuntimeError):
            pass
        finally:
            ticker.cancel()
            await asyncio.gather(ticker, return_exceptions=True)
