import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass
from time import monotonic

from sqlalchemy import select

from geo_tracking.db import DATABASE_ERRORS, Database
from geo_tracking.events import OutputOverload, frames
from geo_tracking.metrics import Metrics
from geo_tracking.models import DeviceLatest
from geo_tracking.schemas import Report
from geo_tracking.sessions import Sessions
from geo_tracking.settings import Settings
from geo_tracking.spatial import match_reports, persist_latest

logger = logging.getLogger(__name__)


@dataclass
class Failure:
    reason: str


@dataclass
class Packet:
    reports: list[Report]
    result: asyncio.Future


class Pipeline:
    def __init__(self, db: Database, sessions: Sessions, metrics: Metrics, settings: Settings):
        self.db, self.sessions, self.metrics, self.settings = db, sessions, metrics, settings
        self.queue: asyncio.Queue[Packet] = asyncio.Queue(maxsize=settings.ingress_reports)
        self.pending_reports = 0
        self.accepting = False
        self.task: asyncio.Task | None = None
        self.idle = asyncio.Event()
        self.idle.set()

    @property
    def running(self):
        return self.accepting and self.task is not None and not self.task.done()

    def start(self):
        self.accepting = True
        self.task = asyncio.create_task(self.run())

    async def submit(self, reports: list[Report]):
        if not self.running:
            return Failure("processor_unavailable")
        if self.pending_reports + len(reports) > self.settings.ingress_reports:
            self.metrics.counts["ingress_rejected"] += len(reports)
            return Failure("ingress_capacity")
        future = asyncio.get_running_loop().create_future()
        self.pending_reports += len(reports)
        self.metrics.counts["ingress_high_water"] = max(
            self.metrics.counts["ingress_high_water"], self.pending_reports
        )
        self.idle.clear()
        self.queue.put_nowait(Packet(reports, future))
        return await asyncio.shield(future)

    def finish(self, packets: list[Packet], results):
        for packet, result in zip(packets, results, strict=True):
            self.pending_reports -= len(packet.reports)
            packet.result.set_result(result)
        if self.pending_reports == 0:
            self.idle.set()

    async def process(self, packets: list[Packet]):
        reports = [report for packet in packets for report in packet.reports]
        async with self.db.processing_session() as session:
            result = await session.execute(
                select(DeviceLatest.device_id, DeviceLatest.reported_at).where(
                    DeviceLatest.device_id.in_({report.device_id for report in reports})
                )
            )
            watermarks = dict(result.all())
            seen = set()
            fresh = []
            statuses = []
            for report in reports:
                key = report.device_id, report.timestamp
                if key in seen:
                    statuses.append("duplicate")
                elif (
                    report.device_id in watermarks
                    and report.timestamp <= watermarks[report.device_id]
                ):
                    statuses.append("stale")
                else:
                    fresh.append(report)
                    statuses.append("accepted")
                seen.add(key)
            alerts = defaultdict(list)
            matches = (
                await match_reports(session, fresh, self.settings.max_matches) if fresh else []
            )
            if len(matches) > self.settings.max_matches:
                raise OutputOverload("fanout_budget_exceeded")
            for match in matches:
                report = fresh[match["report_index"]]
                alerts[match["user_id"]].append(
                    {
                        "device_id": report.device_id,
                        "timestamp": report.timestamp,
                        "latitude": report.latitude,
                        "longitude": report.longitude,
                        "zone_id": str(match["zone_id"]),
                        "zone_version": match["zone_version"],
                    }
                )
            locations = frames(
                "locations", [report.model_dump() for report in fresh], self.settings.frame_bytes
            )
            private = {
                owner: frames("inside_report", items, self.settings.frame_bytes)
                for owner, items in alerts.items()
            }
            output_bytes = sum(frame.size for frame in locations) + sum(
                frame.size for burst in private.values() for frame in burst
            )
            if output_bytes > self.settings.output_bytes:
                raise OutputOverload("fanout_budget_exceeded")
            await persist_latest(session, fresh)
        await self.sessions.broadcast(locations, private)
        self.metrics.counts["reports_accepted"] += len(fresh)
        self.metrics.counts["alerts_generated"] += len(matches)
        self.metrics.counts["batches_committed"] += 1
        offset = 0
        outcomes = []
        for packet in packets:
            outcomes.append({"statuses": statuses[offset : offset + len(packet.reports)]})
            offset += len(packet.reports)
        return outcomes

    async def run(self):
        active = []
        carry = None
        try:
            while True:
                first = carry or await self.queue.get()
                carry = None
                active = [first]
                count = len(first.reports)
                deadline = monotonic() + self.settings.batch_delay_seconds
                while count < self.settings.batch_reports:
                    remaining = deadline - monotonic()
                    if remaining <= 0:
                        break
                    try:
                        packet = await asyncio.wait_for(self.queue.get(), remaining)
                    except TimeoutError:
                        break
                    if count + len(packet.reports) > self.settings.batch_reports:
                        carry = packet
                        break
                    active.append(packet)
                    count += len(packet.reports)
                started = monotonic()
                try:
                    results = await self.process(active)
                except OutputOverload as error:
                    self.metrics.counts["output_rejected"] += count
                    results = [Failure(str(error)) for _ in active]
                except DATABASE_ERRORS:
                    logger.exception("Database processing failed")
                    self.metrics.counts["database_failed"] += count
                    results = [Failure("database_unavailable") for _ in active]
                self.metrics.processing_ms.append((monotonic() - started) * 1000)
                self.finish(active, results)
                active = []
        finally:
            self.accepting = False
            pending = active + ([carry] if carry else [])
            while not self.queue.empty():
                pending.append(self.queue.get_nowait())
            self.finish(pending, [Failure("processor_unavailable") for _ in pending])

    async def stop(self):
        self.accepting = False
        try:
            await asyncio.wait_for(self.idle.wait(), self.settings.shutdown_timeout_seconds)
        except TimeoutError:
            pass
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
