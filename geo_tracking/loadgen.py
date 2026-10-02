import asyncio
import math
import os
import signal
import sys
import tempfile
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Any

import orjson
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from geo_tracking.logs import configure
from geo_tracking.schemas import RIGA, Latitude, Longitude
from geo_tracking.settings import Settings

GENERATOR = Path(__file__).resolve().parents[1] / "generator.py"
DEVICES_PER_PROCESS = 40_000
REPORTS_PER_PROCESS = 4_000
MAX_PROCESSES = 8


class Load(BaseModel):
    model_config = ConfigDict(extra="forbid")
    devices: Annotated[int, Field(ge=1, le=500_000)] = 10_000
    interval_seconds: Annotated[float, Field(ge=1, le=60)] = 5
    duration_seconds: Annotated[float, Field(ge=10, le=3600)] = 300
    spread_km: Annotated[float, Field(gt=0, le=2000)] = 50
    latitude: Latitude = RIGA[0]
    longitude: Longitude = RIGA[1]

    @property
    def rate(self) -> float:
        return self.devices / self.interval_seconds

    @property
    def processes(self) -> int:
        needed = max(self.devices / DEVICES_PER_PROCESS, self.rate / REPORTS_PER_PROCESS)
        return min(MAX_PROCESSES, math.ceil(needed))


class Run:
    def __init__(self, load: Load, process: asyncio.subprocess.Process, output: Path):
        self.load, self.process, self.output = load, process, output
        self.started = time.time()
        self.stopped: float | None = None
        self.result: dict[str, Any] | None = None

    async def finish(self) -> None:
        await self.process.wait()
        self.stopped = time.time()
        if await asyncio.to_thread(self.output.exists):
            result = orjson.loads(await asyncio.to_thread(self.output.read_bytes))
            self.result = {
                "acked": result["counters"].get("acked", 0),
                "scheduled": result["counters"].get("scheduled", 0),
                "acked_reports_per_second": result["acked_reports_per_second"],
            }
            await asyncio.to_thread(self.output.unlink)

    def state(self) -> dict[str, Any]:
        running = self.process.returncode is None
        return {
            "running": running,
            "load": self.load.model_dump(),
            "offered_reports_per_second": self.load.rate,
            "started_at": self.started,
            "elapsed_seconds": (self.stopped or time.time()) - self.started,
            "result": self.result,
            "stopped_early": not running and self.result is None,
        }


class LoadGenerator:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.run: Run | None = None
        self.waiter: asyncio.Task[None] | None = None
        self.lock = asyncio.Lock()

    def state(self) -> dict[str, Any]:
        return self.run.state() if self.run else {"running": False}

    async def start(self, load: Load) -> dict[str, Any]:
        async with self.lock:
            if self.run and self.run.process.returncode is None:
                raise HTTPException(409, "load_running")
            output = Path(tempfile.mkdtemp(prefix="loadgen-")) / "result.json"
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                str(GENERATOR),
                *("--url", self.settings.loadgen_target_url),
                *("--devices", str(load.devices), "--interval", str(load.interval_seconds)),
                *("--duration", str(load.duration_seconds), "--spread-km", str(load.spread_km)),
                *("--latitude", str(load.latitude), "--longitude", str(load.longitude)),
                *("--processes", str(load.processes), "--connections", "4"),
                *("--prefix", self.settings.loadgen_prefix, "--output", str(output)),
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
                start_new_session=True,
            )
            self.run = Run(load, process, output)
            self.waiter = asyncio.create_task(self.run.finish())
            return self.run.state()

    async def stop(self) -> dict[str, Any]:
        async with self.lock:
            if self.run and self.run.process.returncode is None:
                os.killpg(self.run.process.pid, signal.SIGTERM)
                await asyncio.wait_for(asyncio.shield(self.run.process.wait()), 10)
            if self.waiter:
                await self.waiter
            return self.state()


def create_loadgen_app(settings: Settings | None = None) -> FastAPI:
    generator = LoadGenerator(settings or Settings())

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        configure("loadgen")
        yield
        await generator.stop()

    app = FastAPI(title="Fleetline load generator", lifespan=lifespan)

    @app.get("/loadgen")
    async def status() -> dict[str, Any]:
        return generator.state()

    @app.post("/loadgen/start")
    async def start(load: Load) -> dict[str, Any]:
        return await generator.start(load)

    @app.post("/loadgen/stop")
    async def stop() -> dict[str, Any]:
        return await generator.stop()

    @app.get("/health/live")
    async def live() -> dict[str, str]:
        return {"status": "alive"}

    return app


app = create_loadgen_app()
