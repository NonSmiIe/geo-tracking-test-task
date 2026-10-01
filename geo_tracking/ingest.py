import asyncio
from collections.abc import Sequence
from functools import partial

import orjson
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
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


class Ingest:
    def __init__(self, settings: Settings, producer: AIOKafkaProducer, metrics: Metrics):
        self.settings, self.producer, self.metrics = settings, producer, metrics
        self.inflight = 0
        self.room = asyncio.Event()
        self.room.set()
        metrics.gauges["produce_inflight"] = lambda: self.inflight

    def reserve(self, count: int) -> None:
        self.inflight += count
        self.metrics.high_water("produce_inflight_high_water", self.inflight)
        if self.inflight >= self.settings.produce_window:
            self.room.clear()

    def release(self, count: int) -> None:
        self.inflight -= count
        if self.inflight < self.settings.produce_window:
            self.room.set()

    async def send(self, report: Report) -> asyncio.Future:
        return await self.producer.send(
            self.settings.kafka_topic,
            value=orjson.dumps(report.record()),
            key=report.device_id.encode(),
        )

    async def publish(self, reports: Sequence[Report]) -> None:
        if self.inflight + len(reports) > self.settings.produce_window:
            self.metrics.counts["ingest_rejected"] += len(reports)
            raise Overloaded
        self.reserve(len(reports))
        try:
            await asyncio.gather(*[await self.send(report) for report in reports])
        finally:
            self.release(len(reports))
        self.metrics.counts["reports_ingested"] += len(reports)

    async def stream(self, socket: WebSocket) -> None:
        await socket.accept()
        window = asyncio.Semaphore(self.settings.ingest_window)
        acks = Acknowledgements()
        failure: BaseException | None = None

        def settled(sequence: int, future: asyncio.Future) -> None:
            nonlocal failure
            window.release()
            self.release(1)
            if future.cancelled() or future.exception() is not None:
                failure = failure or KafkaError("produce_failed")
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
            while failure is None:
                message = orjson.loads(await socket.receive_text())
                if isinstance(message, dict) and message.get("type") == "flush":
                    await acks.idle.wait()
                    await acknowledge()
                    continue
                report = ReportAdapter.validate_python(message)
                await window.acquire()
                while self.inflight >= self.settings.produce_window:
                    await self.room.wait()
                self.reserve(1)
                (await self.send(report)).add_done_callback(partial(settled, acks.issue()))
            await socket.close(code=1011, reason="produce_failed")
        except (ValidationError, orjson.JSONDecodeError):
            self.metrics.counts["ingest_invalid"] += 1
            await socket.close(code=1007, reason="invalid_report")
        except (WebSocketDisconnect, OSError):
            pass
        finally:
            ticker.cancel()
            await asyncio.gather(ticker, return_exceptions=True)
