import asyncio
import logging
import signal
from collections import defaultdict
from time import monotonic, time
from typing import Any

import orjson
from aiohttp import web
from aiokafka import AIOKafkaConsumer, ConsumerRebalanceListener, ConsumerRecord, TopicPartition
from aiokafka.errors import CommitFailedError, IllegalStateError
from nats.aio.client import Client
from nats.errors import Error as NatsError

from geo_tracking.bus import Subjects, connect_nats, ensure_topic, kafka_topic_id
from geo_tracking.db import DATABASE_ERRORS, Database
from geo_tracking.logs import configure
from geo_tracking.metrics import (
    ALERTS,
    BATCH_SECONDS,
    BATCHES,
    COMMITS_LOST,
    FRESHNESS,
    PARTITIONS,
    PUBLISH_RETRIES,
    RECORDS,
    exposition,
    monitor_loop,
)
from geo_tracking.settings import Settings
from geo_tracking.spatial import Record, advance, match_records, persist_latest, persisted
from geo_tracking.tiles import position_subject

logger = logging.getLogger("geo_tracking.processor")


class PublishStalled(Exception):
    pass


class LaneStalled(Exception):
    pass


def frame(kind: str, items: list[Any]) -> bytes:
    return orjson.dumps({"type": kind, "items": items})


class Processor(ConsumerRebalanceListener):
    def __init__(
        self,
        settings: Settings,
        db: Database,
        nats: Client,
        consumer: AIOKafkaConsumer,
        topic_id: str,
    ):
        self.settings, self.db, self.nats, self.consumer = settings, db, nats, consumer
        self.topic_id = topic_id
        self.subjects = Subjects(settings.subject_prefix)
        self.polls: dict[TopicPartition, float] = {}
        self.lanes: dict[TopicPartition, asyncio.Task[None]] = {}
        self.leaving: dict[TopicPartition, asyncio.Event] = {}
        self.transactions = asyncio.Semaphore(settings.processor_transactions)

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

    async def process(self, partition: int, items: list[ConsumerRecord]) -> None:
        async with self.transactions, self.db.sessions() as session, session.begin():
            limit = await persisted(session, self.topic_id, partition)
            replayed: list[Record] = []
            live: list[Record] = []
            for item in items:
                (replayed if item.offset <= limit else live).append(orjson.loads(item.value))
            seen: set[tuple[str, int]] = set()
            replayed, live = self.unique(replayed, seen), self.unique(live, seen)
            advanced = await persist_latest(session, live)
            fresh = [
                record
                for record in live
                if record[0] in advanced
                and ((previous := advanced[record[0]]) is None or record[3] > previous)
            ]
            emitted = replayed + fresh
            matches = await match_records(session, emitted) if emitted else []
            await advance(session, self.topic_id, partition, items[-1].offset)
        RECORDS.labels("replayed").inc(len(replayed))
        RECORDS.labels("stale").inc(len(live) - len(fresh))
        RECORDS.labels("committed").inc(len(fresh))
        ALERTS.inc(len(matches))
        positions: dict[str, list[Any]] = defaultdict(list[Any])
        for record in emitted:
            positions[position_subject(self.settings.subject_prefix, record[1], record[2])].append(
                record
            )
        alerts: dict[str, list[Any]] = defaultdict(list[Any])
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
        stall = self.settings.publish_deadline_seconds + 10
        while not stopping.is_set():
            now = time()
            oldest = min(self.polls.values(), default=now)
            PARTITIONS.set(len(self.lanes))
            if now - oldest > stall:
                raise LaneStalled(f"a partition lane has not polled Kafka for {now - oldest:.0f} s")
            for partition, lane in tuple(self.lanes.items()):
                if lane.done():
                    del self.lanes[partition]
                    self.polls.pop(partition, None)
                    self.leaving.pop(partition, None)
                    lane.result()
            await asyncio.sleep(0.1)
        await self.on_partitions_revoked(set(self.lanes))

    async def on_partitions_assigned(self, assigned: set[TopicPartition]) -> None:
        logger.info(
            "partitions assigned", extra={"partitions": sorted(p.partition for p in assigned)}
        )
        for partition in assigned - self.lanes.keys():
            self.polls[partition] = time()
            self.leaving[partition] = asyncio.Event()
            self.lanes[partition] = asyncio.create_task(self.lane(partition))

    async def on_partitions_revoked(self, revoked: set[TopicPartition]) -> None:
        logger.info(
            "partitions revoked", extra={"partitions": sorted(p.partition for p in revoked)}
        )
        for partition in revoked:
            self.polls.pop(partition, None)
            if partition in self.leaving:
                self.leaving.pop(partition).set()
        leaving = [self.lanes.pop(partition) for partition in revoked if partition in self.lanes]
        if not leaving:
            return
        _, unfinished = await asyncio.wait(leaving, timeout=self.settings.publish_deadline_seconds)
        for lane in unfinished:
            lane.cancel()
        await asyncio.gather(*leaving, return_exceptions=True)

    async def lane(self, partition: TopicPartition) -> None:
        leaving = self.leaving[partition]
        while not leaving.is_set():
            fetched = await self.consumer.getmany(
                partition,
                timeout_ms=self.settings.processor_poll_ms,
                max_records=self.settings.processor_batch,
            )
            self.polls[partition] = time()
            items = fetched.get(partition)
            if not items:
                continue
            started = monotonic()
            try:
                await self.process(partition.partition, items)
            except DATABASE_ERRORS:
                logger.exception(
                    "batch failed; replaying from its first offset",
                    extra={"partition": partition.partition, "offset": items[0].offset},
                )
                BATCHES.labels("failed").inc()
                self.consumer.seek(partition, items[0].offset)
                await asyncio.sleep(self.settings.processor_retry_seconds)
                continue
            BATCH_SECONDS.observe(monotonic() - started)
            BATCHES.labels("committed").inc()
            try:
                await self.consumer.commit({partition: items[-1].offset + 1})
            except (CommitFailedError, IllegalStateError):
                logger.warning(
                    "offset commit lost to a rebalance; the new owner replays",
                    extra={"partition": partition.partition, "offset": items[-1].offset + 1},
                )
                COMMITS_LOST.inc()


def consumer(settings: Settings) -> AIOKafkaConsumer:
    return AIOKafkaConsumer(
        bootstrap_servers=settings.kafka_bootstrap,
        group_id=settings.kafka_group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=settings.processor_batch,
        fetch_max_bytes=16777216,
        max_partition_fetch_bytes=4194304,
    )


def health_app() -> web.Application:
    async def metrics(request: web.Request) -> web.Response:
        body, content_type = exposition("processor")
        return web.Response(body=body, headers={"Content-Type": content_type})

    async def live(request: web.Request) -> web.Response:
        return web.json_response({"status": "alive"})

    app = web.Application()
    app.router.add_get("/metrics", metrics)
    app.router.add_get("/health/live", live)
    return app


async def serve(
    settings: Settings, stopping: asyncio.Event, assigned: asyncio.Event | None = None
) -> None:
    await ensure_topic(settings)
    topic_id = await kafka_topic_id(settings)
    db = Database(settings, pool_size=settings.processor_transactions)
    nats = await connect_nats(settings)
    kafka = consumer(settings)
    processor = Processor(settings, db, nats, kafka, topic_id)
    kafka.subscribe([settings.kafka_topic], listener=processor)
    await kafka.start()
    runner = web.AppRunner(health_app(), access_log=None)
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
    try:
        await serve(Settings(), stopping)
    except Exception:
        logger.exception("processor exiting for a restart")
        raise SystemExit(1) from None


if __name__ == "__main__":
    configure("processor")
    asyncio.run(main())
