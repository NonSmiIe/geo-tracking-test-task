import argparse
import asyncio
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import aiohttp
import orjson

PROMETHEUS = "http://127.0.0.1:9097"


def containers(project: str, service: str) -> list[str]:
    names = subprocess.run(
        ["docker", "compose", "-p", project, "ps", "-a", "--format", "{{.Name}}", service],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split()
    return sorted(names)


def docker(action: str, names: list[str]) -> None:
    subprocess.run(["docker", action, *names], capture_output=True, check=True)


async def firing(session: aiohttp.ClientSession) -> set[str]:
    async with session.get(PROMETHEUS + "/api/v1/alerts") as response:
        alerts = (await response.json())["data"]["alerts"]
    names = set()
    for alert in alerts:
        if alert["state"] == "firing":
            labels = alert["labels"]
            names.add(labels["alertname"])
            if "job" in labels:
                names.add(f"{labels['alertname']}:{labels['job']}")
    return names


async def drill(
    session: aiohttp.ClientSession,
    name: str,
    expected: set[str],
    inject: list[tuple[str, list[str]]],
    recover: list[tuple[str, list[str]]],
    patience: float,
) -> dict:
    started = time.monotonic()
    for action, names in inject:
        docker(action, names)
    seen: dict[str, float] = {}
    while time.monotonic() - started < patience and not expected <= seen.keys():
        for alert in await firing(session) & expected:
            seen.setdefault(alert, round(time.monotonic() - started, 1))
        await asyncio.sleep(2)
    for action, names in recover:
        docker(action, names)
    return {
        "drill": name,
        "expected": sorted(expected),
        "fired_after_seconds": seen,
        "passed": expected <= seen.keys(),
    }


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default="geo-tracking-test-task")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=150)
    args = parser.parse_args()
    processors = containers(args.project, "processor")
    db, apis = containers(args.project, "db"), containers(args.project, "api")
    edge = containers(args.project, "edge")
    load = await asyncio.create_subprocess_exec(
        sys.executable,
        "generator.py",
        *("--devices", "5000", "--duration", "1200", "--processes", "1"),
        *("--connections", "2", "--prefix", "drill"),
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    results = []
    try:
        await asyncio.sleep(20)
        async with aiohttp.ClientSession() as session:
            assert not await firing(session), "alerts already firing before the drill"
            plan = [
                ("database stopped", {"BatchesFailing"}, [("stop", db)], [("start", db)]),
                (
                    "every processor stopped",
                    {"PartitionsUnowned", "RoleDown:processor"},
                    [("stop", processors)],
                    [("start", processors)],
                ),
                ("every api killed", {"RoleDown:api"}, [("kill", apis)], [("start", apis)]),
                ("edge stopped", {"TargetDown:edge"}, [("stop", edge)], [("start", edge)]),
            ]
            for name, expected, inject, recover in plan:
                results.append(await drill(session, name, expected, inject, recover, args.timeout))
                print(orjson.dumps(results[-1]).decode(), flush=True)
                await asyncio.sleep(45)
    finally:
        if load.returncode is None:
            load.terminate()
        await load.wait()
    report = {
        "ran_at": datetime.now(UTC).isoformat(),
        "drills": results,
        "passed": all(result["passed"] for result in results),
    }
    args.output.write_bytes(orjson.dumps(report, option=orjson.OPT_INDENT_2))
    print(orjson.dumps(report).decode())


asyncio.run(main())
