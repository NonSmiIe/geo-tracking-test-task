import asyncio
import logging
import signal
from collections import defaultdict
from time import monotonic, time

import orjson
from aiohttp import web
from aiokafka import AIOKafkaConsumer, ConsumerRecord, TopicPartition
from aiokafka.errors import CommitFailedError, IllegalStateError
from nats.aio.client import Client
from nats.errors import Error as NatsError

from geo_tracking.bus import Subjects, connect_nats, ensure_topic
from geo_tracking.db import DATABASE_ERRORS, Database
from geo_tracking.metrics import (
    ALERTS,
    BATCH_SECONDS,
    BATCHES,
    COMMITS_LOST,
    FRESHNESS,
    LAST_POLL,
    PARTITIONS,
    PUBLISH_RETRIES,
    RECORDS,
    exposition,
    monitor_loop,
)
from geo_tracking.settings import Settings
from geo_tracking.spatial import Record, advance, match_records, persist_latest, persisted, stored
from geo_tracking.tiles import position_subject

logger = logging.getLogger(__name__)


class PublishStalled(Exception):
    pass


def frame(kind: str, items: list) -> bytes:
    return orjson.dumps({"type": kind, "items": items})


class Processor:
    def __init__(self, settings: Settings, db: Database, nats: Client, consumer: AIOKafkaConsumer):
        self.settings, self.db, self.nats, self.consumer = settings, db, nats, consumer
        self.subjects = Subjects(settings.subject_prefix)
        self.polled = time()

    def unique(self, records: list[Record], seen: set[tuple[str, int]]) -> list[Record]:
        kept = []
        for record in records:
            key = record[0], record[3]
            if key in seen:
                RECORDS.labels("duplicate").inc()
                continue
            seen.add(key)
            kept.append(record)
        return kept

    async def process(self, batches: dict[TopicPartition, list[ConsumerRecord]]) -> None:
        topic = self.settings.kafka_topic
        async with self.db.sessions() as session, session.begin():
            done = await persisted(session, topic)
            replayed: list[Record] = []
            live: list[Record] = []
            for partition, items in batches.items():
                limit = done.get(partition.partition, -1)
                for item in items:
                    (replayed if item.offset <= limit else live).append(orjson.loads(item.value))
            seen: set[tuple[str, int]] = set()
            replayed, live = self.unique(replayed, seen), self.unique(live, seen)
            known = await stored(session, list({record[0] for record in live}))
            fresh = [
                record for record in live if record[0] not in known or record[3] > known[record[0]]
            ]
            emitted = replayed + fresh
            matches = await match_records(session, emitted) if emitted else []
            await persist_latest(session, fresh)
            await advance(
                session,
                topic,
                {partition.partition: items[-1].offset for partition, items in batches.items()},
            )
        RECORDS.labels("replayed").inc(len(replayed))
        RECORDS.labels("stale").inc(len(live) - len(fresh))
        RECORDS.labels("committed").inc(len(fresh))
        ALERTS.inc(len(matches))
        positions: dict[str, list] = defaultdict(list)
        for record in emitted:
            positions[position_subject(self.settings.subject_prefix, record[1], record[2])].append(
                record
            )
        alerts: dict[str, list] = defaultdict(list)
        for match in matches:
            record = emitted[match["report_index"]]
            alerts[match["user_id"]].append(
                {
                    "device_id": record[0],
                    "latitude": record[1],
                    "longitude": record[2],
                    "timestamp": record[3],
                    "zone_id": str(match["zone_id"]),
                    "zone_version": match["zone_version"],
                }
            )
        messages = [(subject, frame("positions", items)) for subject, items in positions.items()]
        size = self.settings.alert_frame_items
        for user_id, items in alerts.items():
            subject = self.subjects.alerts(user_id)
            messages += [
                (subject, frame("inside_report", items[start : start + size]))
                for start in range(0, len(items), size)
            ]
        await self.publish(messages)
        if emitted:
            FRESHNESS.observe(time() - min(record[3] for record in emitted) / 1_000_000)

    async def publish(self, messages: list[tuple[str, bytes]]) -> None:
        deadline = monotonic() + self.settings.publish_deadline_seconds
        sent = 0
        while True:
            try:
                while sent < len(messages):
                    await self.nats.publish(*messages[sent])
                    sent += 1
                await self.nats.flush(timeout=2)
                return
            except (NatsError, TimeoutError):
                if monotonic() > deadline:
                    raise PublishStalled from None
                logger.warning(
                    "NATS unavailable after commit; waiting to deliver the batch's events"
                )
                PUBLISH_RETRIES.inc()
                await asyncio.sleep(self.settings.processor_retry_seconds)

    async def run(self, stopping: asyncio.Event) -> None:
        while not stopping.is_set():
            batches: dict[TopicPartition, list[ConsumerRecord]] = await self.consumer.getmany(
                timeout_ms=self.settings.processor_poll_ms,
                max_records=self.settings.processor_batch,
            )
            self.polled = time()
            LAST_POLL.set(self.polled)
            PARTITIONS.set(len(self.consumer.assignment()))
            if not batches:
                continue
            started = monotonic()
            try:
                await self.process(batches)
            except DATABASE_ERRORS:
                logger.exception("Batch processing failed; replaying from the first offset")
                BATCHES.labels("failed").inc()
                owned = self.consumer.assignment()
                for partition, items in batches.items():
                    if partition in owned:
                        self.consumer.seek(partition, items[0].offset)
                await asyncio.sleep(self.settings.processor_retry_seconds)
                continue
            BATCH_SECONDS.observe(monotonic() - started)
            BATCHES.labels("committed").inc()
            owned = self.consumer.assignment()
            offsets = {
                partition: items[-1].offset + 1
                for partition, items in batches.items()
                if partition in owned
            }
            try:
                if offsets:
                    await self.consumer.commit(offsets)
            except (CommitFailedError, IllegalStateError):
                logger.warning("Offset commit lost to a rebalance; the new owner replays as stale")
                COMMITS_LOST.inc()


def consumer(settings: Settings) -> AIOKafkaConsumer:
    return AIOKafkaConsumer(
        settings.kafka_topic,
        bootstrap_servers=settings.kafka_bootstrap,
        group_id=settings.kafka_group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=settings.processor_batch,
        fetch_max_bytes=16777216,
        max_partition_fetch_bytes=4194304,
    )


def health_app(processor: Processor, settings: Settings) -> web.Application:
    async def metrics(request: web.Request) -> web.Response:
        body, content_type = exposition("processor")
        return web.Response(body=body, headers={"Content-Type": content_type})

    async def live(request: web.Request) -> web.Response:
        stalled = time() - processor.polled > settings.publish_deadline_seconds + 10
        return web.json_response(
            {"status": "stalled" if stalled else "alive"}, status=503 if stalled else 200
        )

    app = web.Application()
    app.router.add_get("/metrics", metrics)
    app.router.add_get("/health/live", live)
    return app


async def serve(
    settings: Settings, stopping: asyncio.Event, assigned: asyncio.Event | None = None
) -> None:
    await ensure_topic(settings)
    db = Database(settings, pool_size=2)
    nats = await connect_nats(settings)
    kafka = consumer(settings)
    await kafka.start()
    processor = Processor(settings, db, nats, kafka)
    runner = web.AppRunner(health_app(processor, settings), access_log=None)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", settings.metrics_port).start()
    monitor = asyncio.create_task(monitor_loop("processor"))
    running = asyncio.create_task(processor.run(stopping))
    try:
        if assigned is not None:
            while not kafka.assignment() and not running.done():  # noqa: ASYNC110
                await asyncio.sleep(0.05)
            assigned.set()
        await running
    finally:
        running.cancel()
        monitor.cancel()
        await asyncio.gather(running, monitor, return_exceptions=True)
        await runner.cleanup()
        await kafka.stop()
        await (nats.drain() if nats.is_connected else nats.close())
        await db.close()


async def main() -> None:
    stopping = asyncio.Event()
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, stopping.set)
    await serve(Settings(), stopping)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
