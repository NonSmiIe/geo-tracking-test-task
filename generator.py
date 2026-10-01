import argparse
import asyncio
import hashlib
import heapq
import math
import random
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import monotonic
from uuid import uuid4

import aiohttp
import orjson


def fingerprint(report):
    timestamp = datetime.fromisoformat(report["timestamp"]).isoformat(timespec="microseconds")
    data = f"{report['device_id']}|{timestamp}".encode()
    return int.from_bytes(hashlib.blake2b(data, digest_size=8).digest(), "big")


@dataclass
class Histogram:
    buckets: Counter = field(default_factory=Counter)

    def add(self, seconds):
        self.buckets[min(100000, max(0, math.ceil(seconds * 1000)))] += 1

    def summary(self):
        total = sum(self.buckets.values())
        if not total:
            return {"count": 0}
        result = {"count": total, "max_ms": max(self.buckets)}
        for name, fraction in (("p50_ms", 0.5), ("p95_ms", 0.95), ("p99_ms", 0.99)):
            cumulative = 0
            for bucket, count in sorted(self.buckets.items()):
                cumulative += count
                if cumulative >= math.ceil(total * fraction):
                    result[name] = bucket
                    break
        return result


@dataclass
class Config:
    url: str = "http://127.0.0.1:8097"
    devices: int = 10000
    interval: float = 5
    duration: float = 60
    concurrency: int = 256
    queue_size: int = 4096
    batch_size: int = 1
    retries: int = 0
    seed: int = 42
    latitude: float = 56.9496
    longitude: float = 24.1052
    synchronized: bool = False
    prefix: str = field(default_factory=lambda: f"run-{uuid4().hex[:8]}")


@dataclass
class Stats:
    counters: Counter = field(default_factory=Counter)
    latency: Histogram = field(default_factory=Histogram)
    schedule_lag: Histogram = field(default_factory=Histogram)
    accepted_checksum: int = 0
    started: float = 0

    def snapshot(self):
        elapsed = monotonic() - self.started
        return {
            "elapsed_seconds": elapsed,
            "counters": dict(self.counters),
            "accepted_reports_per_second": self.counters["accepted"] / max(0.001, elapsed),
            "http_latency": self.latency.summary(),
            "schedule_lag": self.schedule_lag.summary(),
            "accepted_checksum": str(self.accepted_checksum),
        }


async def run(config: Config, on_accepted=None):
    rng = random.Random(config.seed)
    coordinates = [
        (
            config.latitude + rng.uniform(-0.008, 0.008),
            config.longitude + rng.uniform(-0.008, 0.008),
        )
        for _ in range(config.devices)
    ]
    schedule = [
        (0 if config.synchronized else rng.uniform(0, config.interval), device)
        for device in range(config.devices)
    ]
    heapq.heapify(schedule)
    queue = asyncio.Queue(maxsize=config.queue_size)
    stats = Stats(started=monotonic())
    stats.counters["expected_scheduled"] = sum(
        max(0, math.ceil((config.duration - phase) / config.interval)) for phase, _ in schedule
    )
    wall_start = datetime.now(UTC)

    async def worker(client):
        while True:
            items = [await queue.get()]
            while len(items) < config.batch_size and not queue.empty():
                items.append(queue.get_nowait())
            payload = items[0] if config.batch_size == 1 else items
            body = orjson.dumps(payload)
            path = "/locations" if config.batch_size == 1 else "/locations/batch"
            try:
                for attempt in range(config.retries + 1):
                    stats.counters["attempted"] += len(items)
                    started = monotonic()
                    try:
                        async with client.post(
                            config.url + path,
                            data=body,
                            headers={"Content-Type": "application/json"},
                        ) as response:
                            result = await response.json()
                            stats.latency.add(monotonic() - started)
                            if response.status == 200:
                                for report, status in zip(items, result["statuses"], strict=True):
                                    stats.counters[status] += 1
                                    if status == "accepted":
                                        stats.accepted_checksum = (
                                            stats.accepted_checksum + fingerprint(report)
                                        ) % (1 << 64)
                                        if on_accepted:
                                            on_accepted(report)
                                break
                            stats.counters[f"http_{response.status}"] += len(items)
                            if response.status != 503 or attempt == config.retries:
                                stats.counters["rejected"] += len(items)
                                break
                    except (aiohttp.ClientError, TimeoutError):
                        stats.counters["transport_errors"] += len(items)
                        if attempt == config.retries:
                            stats.counters["failed"] += len(items)
                            break
                    stats.counters["retried"] += len(items)
                    await asyncio.sleep(0.05 * (attempt + 1))
            finally:
                for _ in items:
                    queue.task_done()

    async def progress():
        while True:
            await asyncio.sleep(5)
            print(orjson.dumps(stats.snapshot()).decode(), flush=True)

    connector = aiohttp.TCPConnector(limit=config.concurrency)
    async with aiohttp.ClientSession(
        connector=connector, timeout=aiohttp.ClientTimeout(total=10)
    ) as client:
        workers = [asyncio.create_task(worker(client)) for _ in range(config.concurrency)]
        progress_task = asyncio.create_task(progress())
        try:
            iterations = 0
            while schedule:
                due, device = heapq.heappop(schedule)
                if due >= config.duration:
                    break
                remaining = stats.started + due - monotonic()
                if remaining > 0:
                    await asyncio.sleep(remaining)
                lag = monotonic() - stats.started - due
                stats.schedule_lag.add(lag)
                if lag > 0.1:
                    stats.counters["late_over_100ms"] += 1
                latitude, longitude = coordinates[device]
                latitude = max(-89.9, min(89.9, latitude + rng.uniform(-10, 10) / 111320))
                longitude += rng.uniform(-10, 10) / (111320 * math.cos(math.radians(latitude)))
                longitude = (longitude + 180) % 360 - 180
                coordinates[device] = latitude, longitude
                report = {
                    "device_id": f"{config.prefix}-{device:05d}",
                    "latitude": latitude,
                    "longitude": longitude,
                    "timestamp": (wall_start + timedelta(seconds=due)).isoformat(),
                }
                stats.counters["scheduled"] += 1
                try:
                    queue.put_nowait(report)
                except asyncio.QueueFull:
                    stats.counters["generator_dropped"] += 1
                stats.counters["queue_high_water"] = max(
                    stats.counters["queue_high_water"], queue.qsize()
                )
                heapq.heappush(schedule, (due + config.interval, device))
                iterations += 1
                if iterations % 100 == 0:
                    await asyncio.sleep(0)
            await asyncio.wait_for(queue.join(), 30)
        finally:
            for task in [*workers, progress_task]:
                task.cancel()
            await asyncio.gather(*workers, progress_task, return_exceptions=True)
    result = stats.snapshot()
    result["configuration"] = vars(config)
    return result


def arguments():
    parser = argparse.ArgumentParser(description="Open-loop moving-device load generator")
    parser.add_argument("--url", default="http://127.0.0.1:8097")
    parser.add_argument("--devices", type=int, default=10000)
    parser.add_argument("--interval", type=float, default=5)
    parser.add_argument("--duration", type=float, default=60)
    parser.add_argument("--concurrency", type=int, default=256)
    parser.add_argument("--queue-size", type=int, default=4096)
    parser.add_argument("--batch-size", type=int, default=1)
    parser.add_argument("--retries", type=int, default=0)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--latitude", type=float, default=56.9496)
    parser.add_argument("--longitude", type=float, default=24.1052)
    parser.add_argument("--synchronized", action="store_true")
    parser.add_argument("--prefix", default=f"run-{uuid4().hex[:8]}")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if (
        min(
            args.devices,
            args.interval,
            args.duration,
            args.concurrency,
            args.queue_size,
            args.batch_size,
        )
        <= 0
    ):
        parser.error("counts, interval and duration must be positive")
    if args.retries < 0 or args.batch_size > 200:
        parser.error("retries must be nonnegative and batch-size at most 200")
    if not -89.8 <= args.latitude <= 89.8 or not -180 <= args.longitude <= 180:
        parser.error("latitude must be -89.8..89.8 and longitude -180..180")
    return args


if __name__ == "__main__":
    args = arguments()
    config = Config(**{key: value for key, value in vars(args).items() if key != "output"})
    result = asyncio.run(run(config))
    content = orjson.dumps(result, option=orjson.OPT_INDENT_2)
    print(content.decode())
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_bytes(content + b"\n")
