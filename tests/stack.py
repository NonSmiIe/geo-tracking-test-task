import asyncio
import socket
import threading

import uvicorn

from geo_tracking.api.app import create_app
from geo_tracking.gateway_app import create_gateway_app
from geo_tracking.processor import serve
from geo_tracking.settings import Settings


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


class Worker:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.ready = threading.Event()
        self.loop: asyncio.AbstractEventLoop | None = None
        self.task: asyncio.Task | None = None
        self.thread = threading.Thread(target=asyncio.run, args=(self.main(),), daemon=True)

    async def main(self) -> None:
        self.loop = asyncio.get_running_loop()
        assigned = asyncio.Event()
        self.stopping = asyncio.Event()
        self.task = asyncio.create_task(serve(self.settings, self.stopping, assigned))
        await assigned.wait()
        self.ready.set()
        try:
            await self.task
        except asyncio.CancelledError:
            pass

    def start(self) -> "Worker":
        self.thread.start()
        assert self.ready.wait(30), "processor never received partitions"
        return self

    def stop(self) -> None:
        if self.thread.is_alive():
            self.loop.call_soon_threadsafe(self.stopping.set)
        self.thread.join(15)


class Server:
    def __init__(self, app: object, ws_max_size: int = 65536):
        self.port = free_port()
        self.server = uvicorn.Server(
            uvicorn.Config(
                app, host="127.0.0.1", port=self.port, log_level="warning", ws_max_size=ws_max_size
            )
        )
        self.thread = threading.Thread(target=self.server.run, daemon=True)

    def start(self) -> None:
        self.thread.start()
        for _ in range(300):
            if self.server.started:
                return
            threading.Event().wait(0.05)
        raise AssertionError("server did not start")

    def stop(self) -> None:
        self.server.should_exit = True
        self.thread.join(15)


class Stack:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.api = Server(create_app(settings))
        self.gateway = Server(create_gateway_app(settings), ws_max_size=4096)
        self.url = f"http://127.0.0.1:{self.api.port}"
        self.ingest_url = f"ws://127.0.0.1:{self.api.port}/ingest"
        self.ws_url = f"ws://127.0.0.1:{self.gateway.port}"
        self.processor: Worker | None = None

    def __enter__(self) -> "Stack":
        self.api.start()
        self.gateway.start()
        self.processor = Worker(self.settings).start()
        return self

    def restart_processor(self) -> None:
        self.processor.stop()
        self.processor = Worker(self.settings).start()

    def __exit__(self, *exc: object) -> None:
        self.processor.stop()
        self.gateway.stop()
        self.api.stop()
