import argparse
import asyncio
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import aiohttp
import orjson
import websockets

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from generator import Config, Histogram, fingerprint, run
from scripts.acceptance import assess_resources, assess_workload


async def benchmark(args):
    prefix = f"bench-{uuid4().hex[:8]}"
    owners = [prefix + "-alice", prefix + "-bob"]
    zones = []
    counters = [Counter() for _ in range(args.sessions)]
    checksums = [Counter() for _ in range(args.sessions)]
    delivery = Histogram()
    samples = []
    started = datetime.now(UTC).isoformat()
    async with aiohttp.ClientSession() as client:
        for index in range(args.zones):
            owner = owners[0] if index == 0 else owners[1]
            payload = {
                "name": f"benchmark-{index}",
                "latitude": 56.9496 if index == 0 else 10 + index * 0.002,
                "longitude": 24.1052 if index == 0 else -40,
                "radius_m": 10000 if index == 0 else 50 + index % 950,
            }
            async with client.post(
                args.url + "/geozones", json=payload, headers={"X-User-ID": owner}
            ) as response:
                response.raise_for_status()
                zones.append((owner, (await response.json())["id"]))
        baseline = await (await client.get(args.url + "/metrics")).json()

        async def observe(index, socket):
            async for data in socket:
                message = orjson.loads(data)
                if message["type"] not in ("locations", "inside_report"):
                    continue
                for item in message["items"]:
                    if not item["device_id"].startswith(prefix):
                        continue
                    kind = message["type"]
                    counters[index][kind] += 1
                    checksums[index][kind] = (checksums[index][kind] + fingerprint(item)) % (
                        1 << 64
                    )
                    if index == 0 and kind == "locations":
                        delivery.add(
                            (
                                datetime.now(UTC) - datetime.fromisoformat(item["timestamp"])
                            ).total_seconds()
                        )
                    if kind == "inside_report" and item["zone_id"] != zones[0][1]:
                        counters[index]["unexpected_zone"] += 1

        sockets, tasks = [], []

        async def sample():
            while True:
                async with client.get(args.url + "/metrics") as response:
                    reading = await response.json()
                    reading["sampled_at"] = datetime.now(UTC).isoformat()
                    if args.docker_stats:
                        process = await asyncio.create_subprocess_exec(
                            "docker",
                            "stats",
                            "--no-stream",
                            "--format",
                            "{{json .}}",
                            "geo-tracking-test-task-app-1",
                            "geo-tracking-test-task-db-1",
                            stdout=asyncio.subprocess.PIPE,
                        )
                        stdout, _ = await process.communicate()
                        reading["resources"] = [orjson.loads(row) for row in stdout.splitlines()]
                    samples.append(reading)
                await asyncio.sleep(5)

        sampler = asyncio.create_task(sample())
        try:
            for index in range(args.sessions):
                owner = owners[0] if index < 2 else owners[1]
                socket = await websockets.connect(
                    args.url.replace("http", "ws", 1) + f"/ws?user_id={owner}", max_size=2097152
                )
                await socket.recv()
                sockets.append(socket)
                tasks.append(asyncio.create_task(observe(index, socket)))
            generated = await run(
                Config(
                    url=args.url,
                    duration=args.duration,
                    interval=args.interval,
                    devices=10000,
                    concurrency=args.concurrency,
                    prefix=prefix,
                )
            )
            await asyncio.sleep(1)
            expected = generated["counters"].get("accepted", 0)
            checksum = int(generated["accepted_checksum"])
            observed = []
            valid = True
            for index in range(args.sessions):
                alerts_expected = expected if index < 2 else 0
                passed = (
                    counters[index]["locations"] == expected
                    and checksums[index]["locations"] == checksum
                    and counters[index]["inside_report"] == alerts_expected
                    and checksums[index]["inside_report"] == (checksum if index < 2 else 0)
                    and not counters[index]["unexpected_zone"]
                )
                valid &= passed
                observed.append(
                    {
                        "session": index,
                        "counters": dict(counters[index]),
                        "checksums": {key: str(value) for key, value in checksums[index].items()},
                        "passed": passed,
                    }
                )
            pipeline_final = await (await client.get(args.url + "/metrics")).json()
            workload = assess_workload(generated, 10000, args.interval)
            resources = assess_resources(samples, pipeline_final, args.docker_stats)
            latency_passed = delivery.summary().get("p95_ms", 100000) < 1000
            result = {
                "started_at": started,
                "fixture": {
                    "zones": args.zones,
                    "sessions": args.sessions,
                    "inside_zones_per_report": 1,
                    "active_radius_m": 10000,
                    "device_spread_degrees": 0.016,
                },
                "generator": generated,
                "delivery_latency": delivery.summary(),
                "sessions": observed,
                "delivery_reconciled": valid,
                "acceptance_policy": "operational-v2",
                "workload": workload,
                "resources": resources,
                "workload_passed": workload["passed"],
                "acceptance_passed": valid
                and workload["passed"]
                and latency_passed
                and resources["passed"],
                "pipeline_baseline": baseline,
                "pipeline_final": pipeline_final,
                "samples": samples,
            }
        finally:
            sampler.cancel()
            for socket in sockets:
                await socket.close()
            for task in tasks:
                task.cancel()
            await asyncio.gather(sampler, *tasks, return_exceptions=True)
            for owner, zone_id in zones:
                async with client.delete(
                    args.url + f"/geozones/{zone_id}", headers={"X-User-ID": owner}
                ):
                    pass
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8097")
    parser.add_argument("--duration", type=float, default=900)
    parser.add_argument("--interval", type=float, default=5)
    parser.add_argument("--concurrency", type=int, default=256)
    parser.add_argument("--zones", type=int, default=100)
    parser.add_argument("--sessions", type=int, default=3)
    parser.add_argument("--docker-stats", action="store_true")
    parser.add_argument("--output", type=Path, default=Path("evidence/baseline.json"))
    args = parser.parse_args()
    if not 1 <= args.zones <= 10000 or not 2 <= args.sessions <= 100:
        parser.error("zones must be 1–10000 and sessions 2–100")
    result = asyncio.run(benchmark(args))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(orjson.dumps(result, option=orjson.OPT_INDENT_2) + b"\n")
    print(
        orjson.dumps(
            {
                "acceptance_passed": result["acceptance_passed"],
                "delivery_reconciled": result["delivery_reconciled"],
                "generator": result["generator"],
                "delivery_latency": result["delivery_latency"],
            },
            option=orjson.OPT_INDENT_2,
        ).decode()
    )
    if not result["acceptance_passed"]:
        raise SystemExit(1)
