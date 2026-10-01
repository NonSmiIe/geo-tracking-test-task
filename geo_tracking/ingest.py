import asyncio
from collections.abc import Sequence

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


class Ingest:
    def __init__(self, settings: Settings, producer: AIOKafkaProducer, metrics: Metrics):
        self.settings, self.producer, self.metrics = settings, producer, metrics
        self.inflight = 0
        metrics.gauges["produce_inflight"] = lambda: self.inflight

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
        self.inflight += len(reports)
        self.metrics.high_water("produce_inflight_high_water", self.inflight)
        try:
            await asyncio.gather(*[await self.send(report) for report in reports])
        finally:
            self.inflight -= len(reports)
        self.metrics.counts["reports_ingested"] += len(reports)

    async def stream(self, socket: WebSocket) -> None:
        await socket.accept()
        window = asyncio.Semaphore(self.settings.ingest_window)
        acked = 0
        failure: BaseException | None = None

        def settled(future: asyncio.Future) -> None:
            nonlocal acked, failure
            window.release()
            self.inflight -= 1
            if future.cancelled() or future.exception() is not None:
                failure = failure or KafkaError("produce_failed")
            else:
                acked += 1
                self.metrics.counts["reports_ingested"] += 1

        async def acknowledge() -> None:
            await socket.send_text(orjson.dumps({"type": "ack", "count": acked}).decode())

        async def periodic() -> None:
            sent = 0
            while True:
                await asyncio.sleep(self.settings.ack_interval_seconds)
                if acked != sent:
                    sent = acked
                    await acknowledge()

        async def drain() -> None:
            for _ in range(self.settings.ingest_window):
                await window.acquire()
            for _ in range(self.settings.ingest_window):
                window.release()

        ticker = asyncio.create_task(periodic())
        try:
            while failure is None:
                message = orjson.loads(await socket.receive_text())
                if isinstance(message, dict) and message.get("type") == "flush":
                    await drain()
                    await acknowledge()
                    continue
                report = ReportAdapter.validate_python(message)
                await window.acquire()
                self.inflight += 1
                (await self.send(report)).add_done_callback(settled)
            await socket.close(code=1011, reason="produce_failed")
        except (ValidationError, orjson.JSONDecodeError):
            self.metrics.counts["ingest_invalid"] += 1
            await socket.close(code=1007, reason="invalid_report")
        except (WebSocketDisconnect, OSError):
            pass
        finally:
            ticker.cancel()
            await asyncio.gather(ticker, return_exceptions=True)
