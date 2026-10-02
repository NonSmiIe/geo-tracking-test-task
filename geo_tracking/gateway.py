import asyncio
import logging
from collections import deque
from collections.abc import Awaitable, Callable
from time import monotonic
from uuid import uuid4

import orjson
from nats.aio.client import Client
from nats.aio.msg import Msg
from nats.aio.subscription import Subscription
from nats.errors import Error as NatsError
from pydantic import ValidationError
from starlette.websockets import WebSocket, WebSocketDisconnect

from geo_tracking.logs import REQUEST_ID
from geo_tracking.metrics import (
    CONNECTIONS,
    EVICTIONS,
    FRAME_BYTES,
    SLOW_CONSUMERS,
    SUBSCRIPTIONS,
)
from geo_tracking.schemas import ViewportAdapter
from geo_tracking.settings import Settings
from geo_tracking.subjects import Subjects

Frame = tuple[str, int]


def frame(data: bytes) -> Frame:
    return data.decode(), len(data)


SUBSCRIBED = frame(b'{"type":"subscribed"}')
RESYNC = frame(b'{"type":"resync"}')


logger = logging.getLogger(__name__)


class Budget:
    def __init__(self) -> None:
        self.used = 0


class Connection:
    def __init__(self, user_id: str, socket: WebSocket, settings: Settings, budget: Budget):
        self.id = uuid4().hex
        self.request_id = REQUEST_ID.get()
        self.budget = budget
        self.user_id = user_id
        self.socket = socket
        self.settings = settings
        self.queue: deque[Frame] = deque()
        self.queued_bytes = 0
        self.wake = asyncio.Event()
        self.reason: str | None = None
        self.subjects: set[str] = set()
        self.writer: asyncio.Task[None] | None = None

    def enqueue(self, item: Frame) -> bool:
        if self.reason is not None:
            return False
        size = item[1]
        if self.queued_bytes + size > self.settings.websocket_queue_bytes:
            return False
        self.queue.append(item)
        self.queued_bytes += size
        self.budget.used += size
        self.wake.set()
        return True

    def stop(self, reason: str) -> None:
        self.reason = reason
        self.budget.used -= self.queued_bytes
        self.queued_bytes = 0
        self.queue.clear()
        if self.writer:
            self.writer.cancel()

    async def write(self) -> None:
        while True:
            await self.wake.wait()
            while self.queue:
                text, size = self.queue[0]
                await asyncio.wait_for(
                    self.socket.send_text(text), self.settings.send_timeout_seconds
                )
                self.queue.popleft()
                self.queued_bytes -= size
                self.budget.used -= size
            self.wake.clear()


class Gateway:
    def __init__(self, settings: Settings, nats: Client):
        self.settings, self.nats = settings, nats
        self.subjects = Subjects(settings.subject_prefix)
        self.routes: dict[str, set[Connection]] = {}
        self.subscriptions: dict[str, Subscription] = {}
        self.connections: dict[str, Connection] = {}
        self.budget = Budget()
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
        item, delivered = frame(data), 0
        for connection in tuple(self.routes.get(subject, ())):
            if connection.enqueue(item):
                delivered += 1
            else:
                self.evict(connection, "backlog_overflow")
        FRAME_BYTES.inc(delivered * item[1])
        if self.budget.used > self.settings.gateway_queue_bytes:
            backlog = sorted(self.connections.values(), key=lambda c: c.queued_bytes, reverse=True)
            for connection in backlog:
                if (
                    self.budget.used <= self.settings.gateway_queue_bytes
                    or not connection.queued_bytes
                ):
                    break
                self.evict(connection, "gateway_budget")

    def evict(self, connection: Connection, reason: str) -> None:
        if connection.reason is None:
            EVICTIONS.labels(reason).inc()
            logger.warning(
                "dashboard evicted",
                extra={
                    "user_id": connection.user_id,
                    "session_id": connection.id,
                    "request_id": connection.request_id,
                    "reason": reason,
                    "queued_bytes": connection.queued_bytes,
                },
            )
        connection.stop(reason)

    async def route(self, connection: Connection, subjects: set[str]) -> bool:
        async with self.lock:
            created = False
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
                        subject,
                        cb=self.handler(subject),
                        pending_bytes_limit=self.settings.nats_pending_bytes,
                    )
                    created = True
                self.routes[subject].add(connection)
            connection.subjects = set(subjects)
            if not created:
                return True
            try:
                async with asyncio.timeout(self.settings.send_timeout_seconds):
                    await self.nats.flush()
            except (NatsError, TimeoutError):
                return False
            return True

    def handler(self, subject: str) -> Callable[[Msg], Awaitable[None]]:
        async def receive(message: Msg) -> None:
            self.deliver(subject, message.data)

        return receive

    def private(self, user_id: str) -> set[str]:
        return {self.subjects.alerts(user_id), self.subjects.zones(user_id)}

    def viewport(self, connection: Connection, raw: str) -> set[str]:
        view = ViewportAdapter.validate_json(raw)
        return self.private(connection.user_id) | self.subjects.viewport(
            view.south, view.west, view.north, view.east, self.settings.viewport_tiles
        )

    async def read(self, connection: Connection) -> None:
        applied = 0.0
        while True:
            raw = await connection.socket.receive_text()
            wait = applied + self.settings.viewport_interval_seconds - monotonic()
            if wait > 0:
                await asyncio.sleep(wait)
            applied = monotonic()
            try:
                subjects = self.viewport(connection, raw)
            except ValidationError:
                connection.reason = "invalid_message"
                return
            if await self.route(connection, subjects):
                connection.enqueue(SUBSCRIBED)

    async def serve(self, socket: WebSocket, user_id: str) -> None:
        if len(self.connections) + self.opening >= self.settings.max_connections:
            await socket.close(code=1013, reason="session_capacity")
            return
        if (
            sum(c.user_id == user_id for c in self.connections.values())
            >= self.settings.max_sessions_per_user
        ):
            await socket.close(code=1013, reason="user_session_capacity")
            return
        connection = Connection(user_id, socket, self.settings, self.budget)
        self.opening += 1
        try:
            await asyncio.wait_for(socket.accept(), self.settings.send_timeout_seconds)
        except (TimeoutError, OSError):
            return
        finally:
            self.opening -= 1
        self.connections[connection.id] = connection
        connection.enqueue(frame(orjson.dumps({"type": "ready", "session_id": connection.id})))
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
