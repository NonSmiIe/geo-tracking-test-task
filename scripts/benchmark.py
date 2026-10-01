import argparse
import asyncio
import multiprocessing
import sys
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import aiohttp
import orjson
import websockets

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from generator import Config, Histogram, fingerprint, run
from geo_tracking.tiles import viewport_subjects
from scripts.acceptance import POLICY, assess_pipeline, assess_resources, assess_workload

WORLD = {"south": -90, "west": -180, "north": 90, "east": 180}
PROBE = {"south": 56.8, "west": 23.8, "north": 57.1, "east": 24.4}


def fault(value: str) -> str:
    at, action, container = value.split(":", 2)
    float(at)
    if not action or not container:
        raise argparse.ArgumentTypeError("expected SECONDS:docker-action:container")
    return value


def observe(url: str, user: str, viewport: dict, prefix: str, interval: float, ready, stop) -> dict:
    period = interval * 1_000_000

    async def subscribe() -> websockets.ClientConnection:
        socket = await websockets.connect(
            url.replace("http", "ws", 1) + f"/ws?user_id={user}", max_size=None, max_queue=None
        )
        assert orjson.loads(await socket.recv())["type"] == "ready"
        await socket.send(orjson.dumps({"type": "viewport", **viewport}).decode())
        while orjson.loads(await socket.recv())["type"] != "subscribed":
            pass
        return socket

    async def main() -> dict:
        counts, checksums, duplicates = Counter(), Counter(), Counter()
        seen: dict[str, dict[str, tuple[int, int]]] = {"positions": {}, "inside_report": {}}
        latency = {"positions": Histogram(), "inside_report": Histogram()}
        closures: list[float] = []
        socket = await subscribe()
        ready.set()
        started = last = time.monotonic()
        while True:
            try:
                data = await asyncio.wait_for(socket.recv(), 1)
            except TimeoutError:
                if stop.is_set() and time.monotonic() - last > 3:
                    break
                continue
            except websockets.ConnectionClosed:
                closures.append(round(time.monotonic() - started, 2))
                while not stop.is_set():
                    await asyncio.sleep(0.5)
                    try:
                        socket = await subscribe()
                        break
                    except (OSError, websockets.WebSocketException):
                        continue
                if stop.is_set():
                    break
                continue
            last = time.monotonic()
            message = orjson.loads(data)
            kind = message["type"]
            if kind not in latency:
                continue
            now = time.time() * 1_000_000
            histogram = latency[kind]
            for item in message["items"]:
                if kind == "positions":
                    device, stamp = item[0], item[3]
                else:
                    device, stamp = item["device_id"], item["timestamp"]
                if not device.startswith(prefix):
                    continue
                anchor, bits = seen[kind].get(device, (stamp, 0))
                index = round((stamp - anchor) / period)
                if index < 0:
                    anchor, bits, index = stamp, bits << -index, 0
                if bits >> index & 1:
                    duplicates[kind] += 1
                    continue
                seen[kind][device] = anchor, bits | 1 << index
                counts[kind] += 1
                checksums[kind] = (checksums[kind] + fingerprint(device, stamp)) % (1 << 64)
                histogram.add((now - stamp) / 1_000_000)
        await socket.close()
        return {
            "counts": dict(counts),
            "duplicates": dict(duplicates),
            "closures_at_seconds": closures,
            "checksums": {key: str(value) for key, value in checksums.items()},
            "latency": {key: value.summary() for key, value in latency.items()},
            "latency_buckets": {key: dict(value.buckets) for key, value in latency.items()},
        }

    return asyncio.run(main())


async def stored_latest(project: str, prefix: str) -> dict:
    query = (
        "SELECT count(*), coalesce(sum((extract(epoch FROM reported_at) * 1000000)::bigint), 0) "
        f"FROM device_latest WHERE device_id LIKE '{prefix}-%'"
    )
    process = await asyncio.create_subprocess_exec(
        "docker",
        "compose",
        "-p",
        project,
        "exec",
        "-T",
        "db",
        "psql",
        "-U",
        "geo",
        "-d",
        "geo",
        "-tAF",
        ",",
        "-c",
        query,
        stdout=asyncio.subprocess.PIPE,
    )
    stdout, _ = await process.communicate()
    devices, total = stdout.decode().strip().split(",")
    return {"devices": int(devices), "timestamp_sum": str(int(total))}


async def docker_stats(project: str) -> list[dict]:
    process = await asyncio.create_subprocess_exec(
        "docker",
        "stats",
        "--no-stream",
        "--format",
        "{{json .}}",
        *[
            name
            for name in (
                await (
                    await asyncio.create_subprocess_exec(
                        "docker",
                        "ps",
                        "--filter",
                        f"label=com.docker.compose.project={project}",
                        "--format",
                        "{{.Names}}",
                        stdout=asyncio.subprocess.PIPE,
                    )
                ).communicate()
            )[0]
            .decode()
            .split()
        ],
        stdout=asyncio.subprocess.PIPE,
    )
    stdout, _ = await process.communicate()
    return [orjson.loads(row) for row in stdout.splitlines()]


async def inject(faults: list[str], log: list[dict]) -> None:
    started = time.monotonic()
    for fault in sorted(faults, key=lambda item: float(item.split(":")[0])):
        at, action, container = fault.split(":", 2)
        await asyncio.sleep(max(0.0, float(at) - (time.monotonic() - started)))
        process = await asyncio.create_subprocess_exec(
            "docker",
            action,
            container,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        output, _ = await process.communicate()
        log.append(
            {
                "at_seconds": round(time.monotonic() - started, 2),
                "fault": fault,
                "exit_code": process.returncode,
                "output": output.decode().strip(),
            }
        )


async def benchmark(args: argparse.Namespace) -> dict:
    prefix = f"bench-{uuid4().hex[:8]}"
    alice, bob = prefix + "-alice", prefix + "-bob"
    sessions = [(alice, WORLD), (alice, WORLD), (bob, WORLD), (bob, PROBE)]
    zones = []
    samples: list[dict] = []
    faults: list[dict] = []
    async with aiohttp.ClientSession() as client:
        for index in range(args.zones):
            owner = alice if index == 0 else bob
            payload = {
                "name": f"benchmark-{index}",
                "latitude": args.latitude if index == 0 else -40 + index * 0.01,
                "longitude": args.longitude if index == 0 else -60,
                "radius_m": args.spread_km * 1000 * 2 if index == 0 else 50 + index % 950,
            }
            async with client.post(
                args.url + "/geozones", json=payload, headers={"X-User-ID": owner}
            ) as response:
                response.raise_for_status()
                zones.append((owner, (await response.json())["id"]))

        async def metrics() -> dict:
            async with client.get(args.url + "/metrics") as response:
                return await response.json()

        baseline = await metrics()

        async def sample() -> None:
            while True:
                reading = {"sampled_at": datetime.now(UTC).isoformat(), "metrics": await metrics()}
                reading["containers"] = await docker_stats(args.project)
                samples.append(reading)
                await asyncio.sleep(10)

        loop = asyncio.get_running_loop()
        manager = multiprocessing.Manager()
        stop = manager.Event()
        readies = [manager.Event() for _ in sessions]
        sampler = asyncio.create_task(sample())
        try:
            with ProcessPoolExecutor(len(sessions)) as pool:
                watchers = [
                    loop.run_in_executor(
                        pool,
                        observe,
                        args.url,
                        user,
                        view,
                        prefix,
                        args.interval,
                        readies[index],
                        stop,
                    )
                    for index, (user, view) in enumerate(sessions)
                ]
                for ready in readies:
                    await loop.run_in_executor(None, ready.wait, 30)
                injector = asyncio.create_task(inject(args.fault, faults))
                generated = await run(
                    Config(
                        url=args.url,
                        devices=args.devices,
                        interval=args.interval,
                        duration=args.duration,
                        processes=args.processes,
                        connections=args.connections,
                        latitude=args.latitude,
                        longitude=args.longitude,
                        spread_km=args.spread_km,
                        prefix=prefix,
                    ),
                    probes=[sorted(viewport_subjects("p", **PROBE, limit=16))],
                )
                await injector
                for _ in range(120):
                    final = await metrics()
                    if final["roles"]["processor"].get("consumer_lag", 1) == 0:
                        break
                    await asyncio.sleep(1)
                stop.set()
                observed = await asyncio.gather(*watchers)
                stored = await stored_latest(args.project, prefix)
        finally:
            sampler.cancel()
            await asyncio.gather(sampler, return_exceptions=True)
            for owner, zone_id in zones:
                async with client.delete(
                    args.url + f"/geozones/{zone_id}", headers={"X-User-ID": owner}
                ):
                    pass
        final = await metrics()
    acked = generated["counters"].get("acked", 0)
    checksum = generated["checksum"]
    probe = generated["probes"][0]
    expectations = [
        {"positions": (acked, checksum), "inside_report": (acked, checksum)},
        {"positions": (acked, checksum), "inside_report": (acked, checksum)},
        {"positions": (acked, checksum), "inside_report": (0, None)},
        {"positions": (probe["count"], probe["checksum"]), "inside_report": (0, None)},
    ]
    delivery = []
    combined = {"positions": Histogram(), "inside_report": Histogram()}
    for index, (result, expected) in enumerate(zip(observed, expectations, strict=True)):
        checks = {
            kind: result["counts"].get(kind, 0) == count and result["checksums"].get(kind) == mark
            for kind, (count, mark) in expected.items()
        }
        checks["not_closed"] = not result["closures_at_seconds"]
        for kind, buckets in result.pop("latency_buckets").items():
            combined[kind].merge(Histogram(Counter({int(k): v for k, v in buckets.items()})))
        delivery.append(
            {
                "session": index,
                "user": sessions[index][0],
                "viewport": sessions[index][1],
                **result,
                "checks": checks,
                "passed": all(checks.values()),
            }
        )
    latency = {kind: value.summary() for kind, value in combined.items()}
    latency_passed = all(value.get("p95_ms", 100000) < 1000 for value in latency.values())
    workload = assess_workload(generated)
    pipeline = assess_pipeline(acked, baseline, final)
    pipeline["checks"]["database_holds_every_latest_report"] = stored == generated["latest"]
    pipeline["stored_latest"] = stored
    pipeline["passed"] = all(pipeline["checks"].values())
    resources = assess_resources(samples)
    reconciled = all(item["passed"] for item in delivery)
    faults_applied = all(item["exit_code"] == 0 for item in faults) and len(faults) == len(
        args.fault
    )
    return {
        "acceptance_policy": POLICY,
        "faults_applied": faults_applied,
        "acceptance_passed": faults_applied
        and workload["passed"]
        and pipeline["passed"]
        and reconciled
        and latency_passed
        and resources["passed"],
        "started_at": samples[0]["sampled_at"] if samples else None,
        "fixture": {
            "devices": args.devices,
            "interval_seconds": args.interval,
            "duration_seconds": args.duration,
            "zones": args.zones,
            "coverage_zone_radius_m": args.spread_km * 2000,
            "spread_km": args.spread_km,
            "probe_viewport": PROBE,
            "sessions": [{"user": user, "viewport": view} for user, view in sessions],
        },
        "faults": faults,
        "workload": workload,
        "pipeline": pipeline,
        "delivery_reconciled": reconciled,
        "delivery_latency": latency,
        "latency_passed": latency_passed,
        "resources": resources,
        "generator": generated,
        "sessions": delivery,
        "metrics_baseline": baseline,
        "metrics_final": final,
        "samples": samples,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8097")
    parser.add_argument("--project", default="geo-tracking-test-task")
    parser.add_argument("--devices", type=int, default=100000)
    parser.add_argument("--interval", type=float, default=5)
    parser.add_argument("--duration", type=float, default=900)
    parser.add_argument("--processes", type=int, default=8)
    parser.add_argument("--connections", type=int, default=8)
    parser.add_argument("--zones", type=int, default=100)
    parser.add_argument("--latitude", type=float, default=56.9496)
    parser.add_argument("--longitude", type=float, default=24.1052)
    parser.add_argument("--spread-km", type=float, default=150)
    parser.add_argument(
        "--fault",
        action="append",
        type=fault,
        default=[],
        help="SECONDS:docker-action:container, e.g. 60:kill:geo-tracking-test-task-processor-2",
    )
    parser.add_argument("--output", type=Path, default=Path("evidence/baseline-100k.json"))
    args = parser.parse_args()
    result = asyncio.run(benchmark(args))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(orjson.dumps(result, option=orjson.OPT_INDENT_2) + b"\n")
    print(
        orjson.dumps(
            {
                key: result[key]
                for key in (
                    "acceptance_passed",
                    "workload",
                    "pipeline",
                    "delivery_reconciled",
                    "delivery_latency",
                )
            }
            | {"sessions": [{k: s[k] for k in ("counts", "checks")} for s in result["sessions"]]},
            option=orjson.OPT_INDENT_2,
        ).decode()
    )
    if not result["acceptance_passed"]:
        raise SystemExit(1)
