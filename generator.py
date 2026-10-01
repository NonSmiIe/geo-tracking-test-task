import argparse
import asyncio
import hashlib
import heapq
import math
import os
import random
import time
from collections import Counter, deque
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import aiohttp
import orjson
import websockets

from geo_tracking.tiles import position_subject

EARTH_RADIUS = 6371008.8


DRAIN_SECONDS = 120


class LaneLost(Exception):
    pass


def fingerprint(device_id: str, timestamp_us: int) -> int:
    data = f"{device_id}|{timestamp_us}".encode()
    return int.from_bytes(hashlib.blake2b(data, digest_size=8).digest(), "big")


def iso(timestamp_us: int) -> str:
    seconds, micros = divmod(timestamp_us, 1_000_000)
    return datetime.fromtimestamp(seconds, UTC).replace(microsecond=micros).isoformat()


def matches(patterns: list[str], subject: str) -> bool:
    return any(
        subject.startswith(pattern[:-1]) if pattern.endswith(">") else subject == pattern
        for pattern in patterns
    )


@dataclass
class Histogram:
    buckets: Counter = field(default_factory=Counter)

    def add(self, seconds: float) -> None:
        self.buckets[min(100000, max(0, math.ceil(seconds * 1000)))] += 1

    def merge(self, other: "Histogram") -> None:
        self.buckets.update(other.buckets)

    def summary(self) -> dict:
        total = sum(self.buckets.values())
        if not total:
            return {"count": 0}
        result = {"count": total, "max_ms": max(self.buckets)}
        ordered = sorted(self.buckets.items())
        for name, fraction in (("p50_ms", 0.5), ("p95_ms", 0.95), ("p99_ms", 0.99)):
            cumulative = 0
            for bucket, count in ordered:
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
    processes: int = max(1, min(8, (os.cpu_count() or 2) // 2))
    connections: int = 8
    transport: str = "ws"
    concurrency: int = 64
    batch_size: int = 1
    queue_size: int = 8192
    seed: int = 42
    latitude: float = 56.9496
    longitude: float = 24.1052
    spread_km: float = 150
    synchronized: bool = False
    prefix: str = field(default_factory=lambda: f"run-{uuid4().hex[:8]}")
    start_at: float = 0


@dataclass
class Lane:
    pending: deque[str] = field(default_factory=deque)
    confirmed: int = 0
    drained: bool = False


@dataclass
class Shard:
    counters: Counter = field(default_factory=Counter)
    schedule_lag: Histogram = field(default_factory=Histogram)
    latency: Histogram = field(default_factory=Histogram)
    checksum: int = 0
    latest: dict[int, int] = field(default_factory=dict)
    probes: list[list[int]] = field(default_factory=list)


class Fleet:
    def __init__(self, config: Config, index: int):
        self.config = config
        count, rest = divmod(config.devices, config.processes)
        self.first = index * count + min(index, rest)
        self.size = count + (index < rest)
        self.rng = random.Random(config.seed * 1000 + index)
        self.positions = []
        for _ in range(self.size):
            distance = config.spread_km * 1000 * math.sqrt(self.rng.random())
            bearing = self.rng.uniform(0, 2 * math.pi)
            self.positions.append(
                [
                    *self.move(config.latitude, config.longitude, bearing, distance),
                    self.rng.uniform(0, 2 * math.pi),
                    self.rng.uniform(4, 25),
                ]
            )

    @staticmethod
    def move(latitude: float, longitude: float, bearing: float, distance: float) -> tuple:
        angle = distance / EARTH_RADIUS
        lat1, lon1 = math.radians(latitude), math.radians(longitude)
        lat2 = math.asin(
            math.sin(lat1) * math.cos(angle) + math.cos(lat1) * math.sin(angle) * math.cos(bearing)
        )
        lon2 = lon1 + math.atan2(
            math.sin(bearing) * math.sin(angle) * math.cos(lat1),
            math.cos(angle) - math.sin(lat1) * math.sin(lat2),
        )
        return math.degrees(lat2), (math.degrees(lon2) + 540) % 360 - 180

    def step(self, local: int) -> tuple[float, float]:
        state = self.positions[local]
        state[2] += self.rng.gauss(0, math.radians(20))
        state[3] = min(30, max(0, state[3] + self.rng.gauss(0, 1.5)))
        state[0], state[1] = self.move(
            state[0], state[1], state[2], state[3] * self.config.interval
        )
        state[0] = max(-89.9, min(89.9, state[0]))
        return state[0], state[1]

    def device_id(self, local: int) -> str:
        return f"{self.config.prefix}-{self.first + local:07d}"


async def produce(config: Config, index: int, probes: list[list[str]]) -> Shard:
    fleet = Fleet(config, index)
    shard = Shard(probes=[[0, 0] for _ in probes])
    rng = random.Random(config.seed * 7919 + index)
    schedule = [
        (0.0 if config.synchronized else rng.uniform(0, config.interval), local)
        for local in range(fleet.size)
    ]
    heapq.heapify(schedule)
    shard.counters["expected_scheduled"] = sum(
        max(0, math.ceil((config.duration - phase) / config.interval)) for phase, _ in schedule
    )
    lanes = config.connections if config.transport == "ws" else 1
    queues = [asyncio.Queue(maxsize=max(1, config.queue_size // lanes)) for _ in range(lanes)]

    async def acknowledged(socket: websockets.ClientConnection, state: Lane) -> None:
        async for data in socket:
            count = orjson.loads(data)["count"]
            for _ in range(count - state.confirmed):
                state.pending.popleft()
            shard.counters["acked"] += count - state.confirmed
            state.confirmed = count
            if state.drained and not state.pending:
                return
        raise websockets.ConnectionClosedError(None, None)

    async def stream(lane: asyncio.Queue) -> None:
        url = config.url.replace("http", "ws", 1) + "/ingest"
        state = Lane()
        while True:
            try:
                async with websockets.connect(url, max_queue=None, write_limit=1 << 20) as socket:
                    state.confirmed = 0
                    reader = asyncio.create_task(acknowledged(socket, state))
                    try:
                        shard.counters["resent"] += len(state.pending)
                        for message in tuple(state.pending):
                            await socket.send(message)
                        while not state.drained:
                            message = await lane.get()
                            if message is None:
                                state.drained = True
                                break
                            state.pending.append(message)
                            shard.counters["sent"] += 1
                            await socket.send(message)
                        await socket.send('{"type":"flush"}')
                        await asyncio.wait_for(reader, 60)
                        return
                    finally:
                        reader.cancel()
            except (TimeoutError, websockets.ConnectionClosed, OSError) as error:
                shard.counters["reconnects"] += 1
                if shard.counters["reconnects"] > 64 * config.connections:
                    raise LaneLost(
                        f"gave up after {shard.counters['reconnects']} reconnects"
                    ) from error
                await asyncio.sleep(0.5)

    async def post(lane: asyncio.Queue, client: aiohttp.ClientSession) -> None:
        while True:
            first = await lane.get()
            if first is None:
                await lane.put(None)
                return
            items = [first]
            while len(items) < config.batch_size and not lane.empty():
                item = lane.get_nowait()
                if item is None:
                    await lane.put(None)
                    break
                items.append(item)
            path, body = (
                ("/locations", items[0])
                if config.batch_size == 1
                else ("/locations/batch", b"[" + b",".join(items) + b"]")
            )
            shard.counters["sent"] += len(items)
            started = time.monotonic()
            try:
                async with client.post(
                    config.url + path, data=body, headers={"Content-Type": "application/json"}
                ) as response:
                    await response.read()
                    shard.latency.add(time.monotonic() - started)
                    if response.status == 202:
                        shard.counters["acked"] += len(items)
                    else:
                        shard.counters[f"http_{response.status}"] += len(items)
                        shard.counters["rejected"] += len(items)
            except (aiohttp.ClientError, TimeoutError):
                shard.counters["transport_errors"] += len(items)

    async def schedule_reports() -> None:
        iterations = 0
        while schedule:
            due, local = heapq.heappop(schedule)
            if due >= config.duration:
                break
            remaining = config.start_at + due - time.time()
            if remaining > 0:
                await asyncio.sleep(remaining)
            lag = time.time() - config.start_at - due
            shard.schedule_lag.add(lag)
            if lag > 0.1:
                shard.counters["late_over_100ms"] += 1
            latitude, longitude = fleet.step(local)
            device_id = fleet.device_id(local)
            timestamp = int((config.start_at + due) * 1_000_000)
            message = orjson.dumps(
                {
                    "device_id": device_id,
                    "latitude": latitude,
                    "longitude": longitude,
                    "timestamp": iso(timestamp),
                }
            )
            shard.counters["scheduled"] += 1
            lane = queues[local % lanes]
            try:
                lane.put_nowait(message if config.transport == "http" else message.decode())
            except asyncio.QueueFull:
                shard.counters["generator_dropped"] += 1
            else:
                shard.latest[local] = timestamp
                mark = fingerprint(device_id, timestamp)
                shard.checksum = (shard.checksum + mark) % (1 << 64)
                if probes:
                    subject = position_subject("p", latitude, longitude)
                    for slot, patterns in zip(shard.probes, probes, strict=True):
                        if matches(patterns, subject):
                            slot[0] += 1
                            slot[1] = (slot[1] + mark) % (1 << 64)
            shard.counters["queue_high_water"] = max(
                shard.counters["queue_high_water"], lane.qsize()
            )
            heapq.heappush(schedule, (due + config.interval, local))
            iterations += 1
            if iterations % 200 == 0:
                await asyncio.sleep(0)

    if config.transport == "ws":
        deadline = config.start_at + config.duration + DRAIN_SECONDS - time.time()
        try:
            async with asyncio.timeout(deadline), asyncio.TaskGroup() as group:
                for lane in queues:
                    group.create_task(stream(lane))
                await schedule_reports()
                for lane in queues:
                    await lane.put(None)
        except TimeoutError:
            raise LaneLost(f"lanes not drained {DRAIN_SECONDS} s after the schedule") from None
    else:
        connector = aiohttp.TCPConnector(limit=config.concurrency)
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as client:
            senders = [
                asyncio.create_task(post(queues[0], client)) for _ in range(config.concurrency)
            ]
            await schedule_reports()
            await queues[0].put(None)
            await asyncio.gather(*senders)
    return shard


def shard_main(config: Config, index: int, probes: list[list[str]]) -> dict:
    shard = asyncio.run(produce(config, index, probes))
    return {
        "counters": dict(shard.counters),
        "schedule_lag": dict(shard.schedule_lag.buckets),
        "latency": dict(shard.latency.buckets),
        "checksum": shard.checksum,
        "latest_devices": len(shard.latest),
        "latest_sum": sum(shard.latest.values()),
        "probes": shard.probes,
    }


async def run(config: Config, probes: list[list[str]] | None = None) -> dict:
    probes = probes or []
    config.start_at = config.start_at or time.time() + 3
    loop = asyncio.get_running_loop()
    with ProcessPoolExecutor(config.processes) as pool:
        shards = await asyncio.gather(
            *[
                loop.run_in_executor(pool, shard_main, config, index, probes)
                for index in range(config.processes)
            ]
        )
    counters, lag, latency, checksum = Counter(), Histogram(), Histogram(), 0
    latest_devices = latest_sum = 0
    totals = [[0, 0] for _ in probes]
    for shard in shards:
        high = shard["counters"].pop("queue_high_water", 0)
        counters.update(shard["counters"])
        counters["queue_high_water"] = max(counters["queue_high_water"], high)
        lag.merge(Histogram(Counter({int(k): v for k, v in shard["schedule_lag"].items()})))
        latency.merge(Histogram(Counter({int(k): v for k, v in shard["latency"].items()})))
        checksum = (checksum + shard["checksum"]) % (1 << 64)
        latest_devices += shard["latest_devices"]
        latest_sum += shard["latest_sum"]
        for total, (count, mark) in zip(totals, shard["probes"], strict=True):
            total[0] += count
            total[1] = (total[1] + mark) % (1 << 64)
    duration = time.time() - config.start_at
    return {
        "configuration": asdict(config),
        "counters": dict(counters),
        "elapsed_seconds": duration,
        "acked_reports_per_second": counters["acked"] / max(0.001, duration),
        "offered_reports_per_second": config.devices / config.interval,
        "schedule_lag": lag.summary(),
        "http_latency": latency.summary(),
        "checksum": str(checksum),
        "latest": {"devices": latest_devices, "timestamp_sum": str(latest_sum)},
        "probes": [{"count": count, "checksum": str(mark)} for count, mark in totals],
    }


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Open-loop moving-device load generator")
    defaults = Config()
    parser.add_argument("--url", default=defaults.url)
    parser.add_argument("--devices", type=int, default=defaults.devices)
    parser.add_argument("--interval", type=float, default=defaults.interval)
    parser.add_argument("--duration", type=float, default=defaults.duration)
    parser.add_argument("--processes", type=int, default=defaults.processes)
    parser.add_argument("--connections", type=int, default=defaults.connections)
    parser.add_argument("--transport", choices=("ws", "http"), default=defaults.transport)
    parser.add_argument("--concurrency", type=int, default=defaults.concurrency)
    parser.add_argument("--batch-size", type=int, default=defaults.batch_size)
    parser.add_argument("--queue-size", type=int, default=defaults.queue_size)
    parser.add_argument("--seed", type=int, default=defaults.seed)
    parser.add_argument("--latitude", type=float, default=defaults.latitude)
    parser.add_argument("--longitude", type=float, default=defaults.longitude)
    parser.add_argument("--spread-km", type=float, default=defaults.spread_km)
    parser.add_argument("--synchronized", action="store_true")
    parser.add_argument("--prefix", default=defaults.prefix)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    positive = (args.devices, args.interval, args.duration, args.processes, args.connections)
    if min(*positive, args.concurrency, args.batch_size, args.queue_size) <= 0:
        parser.error("counts, interval and duration must be positive")
    if args.batch_size > 200 or (args.batch_size > 1 and args.transport != "http"):
        parser.error("batch-size is at most 200 and only applies to the HTTP transport")
    if not -89 <= args.latitude <= 89 or not -180 <= args.longitude <= 180:
        parser.error("latitude must be -89..89 and longitude -180..180")
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
