from datetime import UTC, datetime, timedelta
from time import monotonic, sleep

import httpx
import orjson
from websockets.sync.client import ClientConnection, connect

from geo_tracking.schemas import RIGA as HOME

WORLD = {"south": -90, "west": -180, "north": 90, "east": 180}
RIGA = {"south": 56.5, "west": 23.5, "north": 57.5, "east": 25}
START = datetime(2026, 1, 1, tzinfo=UTC)


def zone(http: httpx.Client, owner: str = "alice", **changes: object) -> dict:
    payload = {"name": "Home", "latitude": HOME[0], "longitude": HOME[1], "radius_m": 100}
    response = http.post("/geozones", json=payload | changes, headers={"X-User-ID": owner})
    assert response.status_code == 201, response.text
    return response.json()


def report(device: str = "device-1", offset: float = 0, **changes: object) -> dict:
    return {
        "device_id": device,
        "latitude": HOME[0],
        "longitude": HOME[1],
        "timestamp": (START + timedelta(seconds=offset)).isoformat(),
    } | changes


def micros(offset: float) -> int:
    return int(round((START + timedelta(seconds=offset)).timestamp() * 1_000_000))


def dashboard(stack, user: str, viewport: dict | None = WORLD) -> ClientConnection:
    socket = connect(f"{stack.ws_url}/ws?user_id={user}", max_size=None)
    assert orjson.loads(socket.recv(timeout=5))["type"] == "ready"
    if viewport is not None:
        look(socket, viewport)
    return socket


def look(socket: ClientConnection, viewport: dict) -> None:
    socket.send(orjson.dumps({"type": "viewport", **viewport}).decode())
    while orjson.loads(socket.recv(timeout=5))["type"] != "subscribed":
        pass


def collect(socket: ClientConnection, kind: str, count: int, timeout: float = 15) -> list:
    items: list = []
    deadline = monotonic() + timeout
    while len(items) < count:
        message = orjson.loads(socket.recv(timeout=max(0.01, deadline - monotonic())))
        if message["type"] == kind:
            items.extend(message.get("items", [message]))
    assert len(items) >= count, items
    return items


def silent(socket: ClientConnection, kind: str, seconds: float = 1.5) -> None:
    deadline = monotonic() + seconds
    try:
        while (remaining := deadline - monotonic()) > 0:
            message = orjson.loads(socket.recv(timeout=remaining))
            assert message["type"] != kind, message
    except TimeoutError:
        pass


def wait_for(predicate, timeout: float = 15, interval: float = 0.1):
    deadline = monotonic() + timeout
    while monotonic() < deadline:
        value = predicate()
        if value:
            return value
        sleep(interval)
    raise AssertionError("condition was not met in time")
