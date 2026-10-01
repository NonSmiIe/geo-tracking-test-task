import asyncio
import logging
from collections import deque
from collections.abc import Awaitable, Callable
from uuid import uuid4

import orjson
from nats.aio.client import Client
from nats.aio.msg import Msg
from nats.aio.subscription import Subscription
from pydantic import ValidationError
from starlette.websockets import WebSocket, WebSocketDisconnect

from geo_tracking.bus import Subjects
from geo_tracking.metrics import (
    CONNECTIONS,
    EVICTIONS,
    FRAME_BYTES,
    FRAMES,
    SLOW_CONSUMERS,
    SUBSCRIPTIONS,
)
from geo_tracking.schemas import ViewportAdapter
from geo_tracking.settings import Settings
from geo_tracking.tiles import viewport_subjects

SUBSCRIBED = b'{"type":"subscribed"}'
RESYNC = b'{"type":"resync"}'


logger = logging.getLogger(__name__)


class Connection:
    def __init__(self, user_id: str, socket: WebSocket, settings: Settings):
        self.id = uuid4().hex
        self.user_id = user_id
        self.socket = socket
        self.settings = settings
        self.queue: deque[bytes] = deque()
        self.queued_bytes = 0
        self.wake = asyncio.Event()
        self.reason: str | None = None
        self.subjects: set[str] = set()
        self.writer: asyncio.Task[None] | None = None

    def enqueue(self, data: bytes) -> bool:
        if self.reason is not None:
            return False
        if self.queued_bytes + len(data) > self.settings.websocket_queue_bytes:
            return False
        self.queue.append(data)
        self.queued_bytes += len(data)
        self.wake.set()
        return True

    def stop(self, reason: str) -> None:
        self.reason = reason
        if self.writer:
            self.writer.cancel()

    async def write(self) -> None:
        while True:
            await self.wake.wait()
            while self.queue:
                data = self.queue[0]
                await asyncio.wait_for(
                    self.socket.send_text(data.decode()), self.settings.send_timeout_seconds
                )
                self.queue.popleft()
                self.queued_bytes -= len(data)
            self.wake.clear()


class Gateway:
    def __init__(self, settings: Settings, nats: Client):
        self.settings, self.nats = settings, nats
        self.subjects = Subjects(settings.subject_prefix)
        self.routes: dict[str, set[Connection]] = {}
        self.subscriptions: dict[str, Subscription] = {}
        self.connections: dict[str, Connection] = {}
        self.opening = 0
        self.lock = asyncio.Lock()
        CONNECTIONS.set_function(lambda: len(self.connections))
        SUBSCRIPTIONS.set_function(lambda: len(self.subscriptions))

    async def resync(self) -> None:
        logger.warning(
            "NATS reconnected; resyncing every dashboard",
            extra={"dashboards": len(self.connections)},
        )
        for connection in tuple(self.connections.values()):
            connection.enqueue(RESYNC)

    async def dropped(self, subject: str) -> None:
        SLOW_CONSUMERS.inc()
        listeners = tuple(self.routes.get(subject, ()))
        logger.warning(
            "NATS dropped a subscription's messages; resyncing",
            extra={"subject": subject, "dashboards": len(listeners)},
        )
        for connection in listeners:
            connection.enqueue(RESYNC)

    def deliver(self, subject: str, data: bytes) -> None:
        for connection in tuple(self.routes.get(subject, ())):
            if connection.enqueue(data):
                FRAMES.inc()
                FRAME_BYTES.inc(len(data))
            else:
                self.evict(connection, "backlog_overflow")

    def evict(self, connection: Connection, reason: str) -> None:
        if connection.reason is None:
            EVICTIONS.labels(reason).inc()
            logger.warning(
                "dashboard evicted",
                extra={
                    "user_id": connection.user_id,
                    "reason": reason,
                    "queued_bytes": connection.queued_bytes,
                },
            )
        connection.stop(reason)

    async def route(self, connection: Connection, subjects: set[str]) -> None:
        async with self.lock:
            for subject in connection.subjects - subjects:
                listeners = self.routes[subject]
                listeners.discard(connection)
                if not listeners:
                    del self.routes[subject]
                    await self.subscriptions.pop(subject).unsubscribe()
            for subject in subjects - connection.subjects:
                if subject not in self.routes:
                    self.routes[subject] = set()
                    self.subscriptions[subject] = await self.nats.subscribe(
                        subject, cb=self.handler(subject), pending_bytes_limit=268435456
                    )
                self.routes[subject].add(connection)
            connection.subjects = set(subjects)

    def handler(self, subject: str) -> Callable[[Msg], Awaitable[None]]:
        async def receive(message: Msg) -> None:
            self.deliver(subject, message.data)

        return receive

    def private(self, user_id: str) -> set[str]:
        return {self.subjects.alerts(user_id), self.subjects.zones(user_id)}

    def viewport(self, connection: Connection, raw: str) -> set[str]:
        view = ViewportAdapter.validate_json(raw)
        return self.private(connection.user_id) | viewport_subjects(
            self.settings.subject_prefix,
            view.south,
            view.west,
            view.north,
            view.east,
            self.settings.viewport_tiles,
        )

    async def read(self, connection: Connection) -> None:
        while True:
            raw = await connection.socket.receive_text()
            try:
                subjects = self.viewport(connection, raw)
            except ValidationError:
                connection.reason = "invalid_message"
                return
            await self.route(connection, subjects)
            connection.enqueue(SUBSCRIBED)

    async def serve(self, socket: WebSocket, user_id: str) -> None:
        if len(self.connections) + self.opening >= self.settings.max_connections:
            await socket.close(code=1013, reason="session_capacity")
            return
        connection = Connection(user_id, socket, self.settings)
        self.opening += 1
        try:
            await asyncio.wait_for(socket.accept(), self.settings.send_timeout_seconds)
        except (TimeoutError, OSError):
            self.opening -= 1
            return
        self.opening -= 1
        self.connections[connection.id] = connection
        connection.enqueue(orjson.dumps({"type": "ready", "session_id": connection.id}))
        connection.writer = asyncio.create_task(connection.write())
        try:
            await self.route(connection, self.private(user_id))
            reader = asyncio.create_task(self.read(connection))
            tasks = (connection.writer, reader)
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
            self.evict(connection, "send_timeout")
        except (WebSocketDisconnect, OSError):
            pass
        finally:
            del self.connections[connection.id]
            await self.route(connection, set())
            try:
                await asyncio.wait_for(
                    socket.close(
                        code=1013 if connection.reason else 1000,
                        reason=connection.reason or "disconnected",
                    ),
                    0.5,
                )
            except (TimeoutError, RuntimeError, OSError, WebSocketDisconnect):
                pass

    def close(self) -> None:
        for connection in tuple(self.connections.values()):
            connection.stop("server_shutdown")
