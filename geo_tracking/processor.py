import asyncio
import logging
from collections import defaultdict
from time import monotonic

import orjson
from aiokafka import AIOKafkaConsumer, ConsumerRecord, TopicPartition
from nats.aio.client import Client

from geo_tracking.bus import Subjects, connect_nats, ensure_topic, serve_metrics
from geo_tracking.db import DATABASE_ERRORS, Database
from geo_tracking.metrics import Metrics, monitor_loop
from geo_tracking.settings import Settings
from geo_tracking.spatial import Record, match_records, persist_latest, watermarks
from geo_tracking.tiles import position_subject

logger = logging.getLogger(__name__)


def frame(kind: str, items: list) -> bytes:
    return orjson.dumps({"type": kind, "items": items})


class Processor:
    def __init__(self, settings: Settings, db: Database, nats: Client, consumer: AIOKafkaConsumer):
        self.settings, self.db, self.nats, self.consumer = settings, db, nats, consumer
        self.subjects = Subjects(settings.subject_prefix)
        self.metrics = Metrics("processor")
        self.consumer_lag = 0
        self.metrics.gauges["consumer_lag"] = lambda: self.consumer_lag

    async def refresh_lag(self) -> None:
        total = 0
        for partition in self.consumer.assignment():
            highwater = self.consumer.highwater(partition)
            if highwater is not None:
                total += max(0, highwater - await self.consumer.position(partition))
        self.consumer_lag = total

    async def process(self, records: list[Record]) -> None:
        seen = set()
        unique = []
        for record in records:
            key = record[0], record[3]
            if key in seen:
                self.metrics.counts["duplicates"] += 1
                continue
            seen.add(key)
            unique.append(record)
        async with self.db.sessions() as session, session.begin():
            known = await watermarks(session, list({record[0] for record in unique}))
            fresh = [record for record in unique if record[3] > known.get(record[0], -1)]
            matches = await match_records(session, fresh) if fresh else []
            await persist_latest(session, fresh)
        self.metrics.counts["stale"] += len(unique) - len(fresh)
        self.metrics.counts["reports_committed"] += len(fresh)
        self.metrics.counts["alerts_generated"] += len(matches)
        positions: dict[str, list] = defaultdict(list)
        for record in fresh:
            positions[position_subject(self.settings.subject_prefix, record[1], record[2])].append(
                record
            )
        alerts: dict[str, list] = defaultdict(list)
        for match in matches:
            record = fresh[match["report_index"]]
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
        for subject, items in positions.items():
            await self.nats.publish(subject, frame("positions", items))
        size = self.settings.alert_frame_items
        for user_id, items in alerts.items():
            subject = self.subjects.alerts(user_id)
            for start in range(0, len(items), size):
                chunk = items[start : start + size]
                await self.nats.publish(subject, frame("inside_report", chunk))
        await self.nats.flush()

    async def run(self) -> None:
        while True:
            batches: dict[TopicPartition, list[ConsumerRecord]] = await self.consumer.getmany(
                timeout_ms=self.settings.processor_poll_ms,
                max_records=self.settings.processor_batch,
            )
            if not batches:
                await self.refresh_lag()
                continue
            records = [orjson.loads(item.value) for items in batches.values() for item in items]
            started = monotonic()
            try:
                await self.process(records)
            except DATABASE_ERRORS:
                logger.exception("Batch processing failed; replaying from the first offset")
                self.metrics.counts["batches_failed"] += 1
                for partition, items in batches.items():
                    self.consumer.seek(partition, items[0].offset)
                await asyncio.sleep(self.settings.processor_retry_seconds)
                continue
            self.metrics.processing_ms.append((monotonic() - started) * 1000)
            self.metrics.counts["batches_committed"] += 1
            offsets = {partition: items[-1].offset + 1 for partition, items in batches.items()}
            await self.consumer.commit(offsets)
            await self.refresh_lag()


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


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    settings = Settings()
    await ensure_topic(settings)
    db = Database(settings, pool_size=2)
    nats = await connect_nats(settings)
    kafka = consumer(settings)
    await kafka.start()
    processor = Processor(settings, db, nats, kafka)
    responder = await serve_metrics(nats, processor.subjects, processor.metrics.snapshot)
    monitor = asyncio.create_task(monitor_loop(processor.metrics))
    try:
        await processor.run()
    finally:
        monitor.cancel()
        await responder.unsubscribe()
        await kafka.stop()
        await nats.drain()
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
