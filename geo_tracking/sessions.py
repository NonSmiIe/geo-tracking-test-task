import asyncio
from collections import deque
from uuid import uuid4

import orjson
from starlette.websockets import WebSocket, WebSocketDisconnect

from geo_tracking.events import Frame
from geo_tracking.metrics import Metrics
from geo_tracking.settings import Settings


class Connection:
    def __init__(self, user_id: str, socket: WebSocket, settings: Settings):
        self.id = str(uuid4())
        self.user_id = user_id
        self.socket = socket
        self.settings = settings
        self.queue: deque[Frame] = deque()
        self.queued_bytes = 0
        self.wake = asyncio.Event()
        self.reason: str | None = None
        self.writer_task: asyncio.Task | None = None

    def enqueue(self, burst: tuple[Frame, ...]) -> bool:
        size = sum(frame.size for frame in burst)
        if (
            self.reason is not None
            or self.queued_bytes + size > self.settings.websocket_queue_bytes
            or len(self.queue) + len(burst) > self.settings.websocket_queue_frames
        ):
            return False
        self.queue.extend(burst)
        self.queued_bytes += size
        self.wake.set()
        return True

    def stop(self, reason: str):
        self.reason = reason
        if self.writer_task:
            self.writer_task.cancel()
        self.wake.set()

    async def write(self):
        while True:
            await self.wake.wait()
            while self.queue:
                frame = self.queue[0]
                await asyncio.wait_for(
                    self.socket.send_text(frame.data.decode()), self.settings.send_timeout_seconds
                )
                self.queue.popleft()
                self.queued_bytes -= frame.size
            self.wake.clear()

    async def read(self):
        while True:
            await self.socket.receive_text()


class Sessions:
    def __init__(self, settings: Settings, metrics: Metrics):
        self.settings = settings
        self.metrics = metrics
        self.users: dict[str, dict[str, Connection]] = {}
        self.opening = 0

    @property
    def count(self):
        return sum(len(sessions) for sessions in self.users.values())

    def remove(self, connection: Connection):
        group = self.users.get(connection.user_id)
        if group:
            group.pop(connection.id, None)
            if not group:
                self.users.pop(connection.user_id)

    async def serve(self, socket: WebSocket, user_id: str):
        if self.count + self.opening >= self.settings.max_connections:
            await socket.close(code=1013, reason="session_capacity")
            return
        connection = Connection(user_id, socket, self.settings)
        self.opening += 1
        published = False
        try:
            await asyncio.wait_for(socket.accept(), self.settings.send_timeout_seconds)
            ready = orjson.dumps({"type": "ready", "session_id": connection.id})
            connection.enqueue((Frame(ready),))
            connection.writer_task = asyncio.create_task(connection.write())
            reader = asyncio.create_task(connection.read())
            self.users.setdefault(user_id, {})[connection.id] = connection
            self.opening -= 1
            published = True
            tasks = (connection.writer_task, reader)
            try:
                done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    if not task.cancelled():
                        task.result()
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
        except TimeoutError:
            connection.reason = "send_timeout"
            self.metrics.counts["slow_connections_closed"] += 1
        except (WebSocketDisconnect, OSError):
            pass
        finally:
            if not published:
                self.opening -= 1
            self.remove(connection)
            try:
                await asyncio.wait_for(
                    socket.close(
                        code=1013 if connection.reason else 1000,
                        reason=connection.reason or "disconnected",
                    ),
                    0.5,
                )
            except (TimeoutError, RuntimeError, OSError):
                pass

    async def broadcast(self, locations: tuple[Frame, ...], alerts: dict[str, tuple[Frame, ...]]):
        recipients = [(user, tuple(group.values())) for user, group in self.users.items()]
        for user_id, connections in recipients:
            burst = locations + alerts.get(user_id, ())
            if not burst:
                continue
            for connection in connections:
                if not connection.enqueue(burst):
                    self.remove(connection)
                    connection.stop("backlog_overflow")
                    self.metrics.counts["slow_connections_closed"] += 1
                else:
                    self.metrics.counts["frames_enqueued"] += len(burst)
                    self.metrics.counts["bytes_enqueued"] += sum(frame.size for frame in burst)
            await asyncio.sleep(0)

    async def close(self):
        for group in tuple(self.users.values()):
            for connection in tuple(group.values()):
                connection.stop("server_shutdown")
