import asyncio
import logging
import signal
from collections import defaultdict
from dataclasses import dataclass
from time import monotonic, time

import orjson
from aiohttp import web
from aiokafka import AIOKafkaConsumer, ConsumerRebalanceListener, ConsumerRecord, TopicPartition
from aiokafka.errors import CommitFailedError, IllegalStateError
from nats.aio.client import Client
from nats.errors import Error as NatsError

from geo_tracking.bus import connect_nats, ensure_topic
from geo_tracking.db import DATABASE_ERRORS, Database
from geo_tracking.logs import configure
from geo_tracking.metrics import (
    BATCH_SECONDS,
    BATCHES,
    FRESHNESS,
    PARTITIONS,
    RECORDS,
    exposition,
    monitor_loop,
)
from geo_tracking.settings import Settings
from geo_tracking.spatial import Record, advance, match_records, persist_latest, persisted
from geo_tracking.subjects import Subjects

logger = logging.getLogger("geo_tracking.processor")


class PublishStalled(Exception):
    pass


class Stalled(Exception):
    pass


@dataclass(frozen=True)
class Batch:
    offsets: dict[TopicPartition, int]
    messages: list[tuple[str, bytes]]
    oldest: int | None
    started: float


def numbered(offsets: dict[TopicPartition, int]) -> dict[int, int]:
    return {tp.partition: offset for tp, offset in offsets.items()}


COMMITTED, STALE, DUPLICATE, REPLAYED = (
    RECORDS.labels(outcome) for outcome in ("committed", "stale", "duplicate", "replayed")
)
BATCH_COMMITTED, BATCH_FAILED = BATCHES.labels("committed"), BATCHES.labels("failed")


def frames(kind: str, encoded_items: list[bytes], limit: int) -> list[bytes]:
    head, tail = b'{"type":"' + kind.encode() + b'","items":[', b"]}"
    chunks: list[list[bytes]] = [[]]
    size = 0
    for encoded in encoded_items:
        if chunks[-1] and size + len(encoded) > limit:
            chunks.append([])
            size = 0
        chunks[-1].append(encoded)
        size += len(encoded) + 1
    return [head + b",".join(chunk) + tail for chunk in chunks if chunk]


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
        self.owned: set[TopicPartition] = set()
        self.settled = asyncio.Event()
        self.stages: list[asyncio.Task[None]] = []
        self.reset()

    def reset(self) -> None:
        self.outbox: asyncio.Queue[Batch] = asyncio.Queue(maxsize=1)
        self.inflight = 0
        self.settled.set()
        self.polled = monotonic()

    async def write(self, fetched: dict[TopicPartition, list[ConsumerRecord]]) -> Batch:
        started = monotonic()
        async with self.db.sessions() as session, session.begin():
            limits = await persisted(session, self.topic_id, [tp.partition for tp in fetched])
            replayed: list[tuple[Record, bytes]] = []
            live: list[tuple[Record, bytes]] = []
            seen: set[tuple[str, int]] = set()
            for tp, items in fetched.items():
                limit = limits[tp.partition]
                for item in items:
                    record = orjson.loads(item.value)
                    key = record[0], record[3]
                    if key in seen:
                        DUPLICATE.inc()
                        continue
                    seen.add(key)
                    (replayed if item.offset <= limit else live).append((record, item.value))
            advanced = await persist_latest(session, [record for record, _ in live])
            fresh = [
                (record, raw)
                for record, raw in live
                if record[0] in advanced
                and ((previous := advanced[record[0]]) is None or record[3] > previous)
            ]
            emitted = replayed + fresh
            records = [record for record, _ in emitted]
            matches = await match_records(session, records) if records else []
            await advance(
                session,
                self.topic_id,
                {tp.partition: items[-1].offset for tp, items in fetched.items()},
            )
        REPLAYED.inc(len(replayed))
        STALE.inc(len(live) - len(fresh))
        COMMITTED.inc(len(fresh))
        positions: dict[str, list[bytes]] = defaultdict(list)
        for record, raw in emitted:
            positions[self.subjects.position(record[1], record[2])].append(raw)
        alerts: dict[str, list[bytes]] = defaultdict(list)
        for match in matches:
            record = records[match["report_index"]]
            alerts[match["user_id"]].append(
                orjson.dumps(
                    {
                        "device_id": record[0],
                        "latitude": record[1],
                        "longitude": record[2],
                        "timestamp": record[3],
                        "zone_id": match["zone_id"],
                        "zone_version": match["zone_version"],
                    }
                )
            )
        limit = self.settings.frame_bytes
        messages = [
            (subject, payload)
            for subject, items in positions.items()
            for payload in frames("positions", items, limit)
        ]
        messages += [
            (self.subjects.alerts(user_id), payload)
            for user_id, items in alerts.items()
            for payload in frames("inside_report", items, limit)
        ]
        return Batch(
            offsets={tp: items[-1].offset + 1 for tp, items in fetched.items()},
            messages=messages,
            oldest=min((record[3] for record in records), default=None),
            started=started,
        )

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
                await asyncio.sleep(self.settings.processor_retry_seconds)

    def begin(self) -> None:
        self.inflight += 1
        self.settled.clear()

    def end(self) -> None:
        self.inflight -= 1
        if not self.inflight:
            self.settled.set()

    async def consume(self) -> None:
        while True:
            fetched = await self.consumer.getmany(timeout_ms=self.settings.processor_poll_ms)
            self.polled = monotonic()
            if not fetched:
                continue
            self.begin()
            try:
                batch = await self.write(fetched)
            except DATABASE_ERRORS:
                firsts = {tp: items[0].offset for tp, items in fetched.items()}
                logger.exception("batch failed; replaying it", extra={"offsets": numbered(firsts)})
                BATCH_FAILED.inc()
                for tp, offset in firsts.items():
                    self.consumer.seek(tp, offset)
                self.end()
                await asyncio.sleep(self.settings.processor_retry_seconds)
                continue
            await self.outbox.put(batch)
            self.polled = monotonic()

    async def deliver(self) -> None:
        while True:
            batch = await self.outbox.get()
            await self.publish(batch.messages)
            BATCH_SECONDS.observe(monotonic() - batch.started)
            BATCH_COMMITTED.inc()
            if batch.oldest is not None:
                FRESHNESS.observe(time() - batch.oldest / 1_000_000)
            try:
                await self.consumer.commit(batch.offsets)
            except (CommitFailedError, IllegalStateError):
                logger.warning(
                    "offset commit lost to a rebalance; the new owner replays",
                    extra={"offsets": numbered(batch.offsets)},
                )
            self.end()

    def start(self) -> None:
        self.reset()
        self.stages = [asyncio.create_task(self.consume()), asyncio.create_task(self.deliver())]

    async def halt(self) -> None:
        for stage in self.stages:
            stage.cancel()
        await asyncio.gather(*self.stages, return_exceptions=True)

    async def run(self, stopping: asyncio.Event) -> None:
        stall = self.settings.publish_deadline_seconds + 10
        self.start()
        try:
            while not stopping.is_set():
                for stage in self.stages:
                    if stage.done():
                        stage.result()
                PARTITIONS.set(len(self.owned))
                if (idle := monotonic() - self.polled) > stall:
                    raise Stalled(f"no Kafka poll for {idle:.0f} s")
                await asyncio.sleep(0.1)
            await self.on_partitions_revoked(set(self.owned))
        finally:
            await self.halt()

    async def on_partitions_assigned(self, assigned: set[TopicPartition]) -> None:
        logger.info(
            "partitions assigned", extra={"partitions": sorted(p.partition for p in assigned)}
        )
        self.owned = set(assigned)

    async def on_partitions_revoked(self, revoked: set[TopicPartition]) -> None:
        logger.info(
            "partitions revoked", extra={"partitions": sorted(p.partition for p in revoked)}
        )
        self.owned -= revoked
        try:
            await asyncio.wait_for(
                self.settled.wait(), timeout=self.settings.publish_deadline_seconds
            )
        except TimeoutError:
            logger.warning("batches still in flight at handover; dropped for the new owner")
            await self.halt()
            self.start()


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
    topic_id = await ensure_topic(settings)
    db = Database(settings, pool_size=1)
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
