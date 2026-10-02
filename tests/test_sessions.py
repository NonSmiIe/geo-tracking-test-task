import orjson
import pytest
from websockets.exceptions import ConnectionClosed, InvalidStatus
from websockets.sync.client import connect

from tests.helpers import RIGA, WORLD, dashboard, look, report, wait_for
from tests.stack import Stack


def closed(socket) -> tuple[int, str]:
    with pytest.raises(ConnectionClosed):
        while True:
            socket.recv(timeout=5)
    return socket.close_code, socket.close_reason


@pytest.mark.parametrize(
    "message",
    [
        "not json",
        '{"type":"viewport","south":10,"west":0,"north":0,"east":1}',
        '{"type":"viewport","south":0,"west":0,"north":1,"east":1,"zoom":4}',
        '{"type":"subscribe","subject":"fleet.pos.>"}',
    ],
)
def test_an_invalid_message_closes_only_that_dashboard(stack, message: str) -> None:
    bad, good = dashboard(stack, "alice", None), dashboard(stack, "alice")
    bad.send(message)
    assert closed(bad) == (1013, "invalid_message")
    look(good, RIGA)
    good.close()


def test_a_message_over_the_frame_limit_is_refused(stack) -> None:
    socket = dashboard(stack, "alice", None)
    socket.send(orjson.dumps({"type": "viewport", "padding": "x" * 5000}).decode())
    assert closed(socket)[0] == 1009


def test_sessions_are_capped_per_user_and_per_gateway(settings) -> None:
    capped = settings.model_copy(update={"max_connections": 3, "max_sessions_per_user": 2})
    with Stack(capped) as stack:
        alice = [dashboard(stack, "alice", None) for _ in range(2)]
        with pytest.raises(InvalidStatus):
            dashboard(stack, "alice", None)
        bob = dashboard(stack, "bob", None)
        with pytest.raises(InvalidStatus):
            dashboard(stack, "carol", None)
        alice.pop().close()
        assert wait_for(lambda: reconnect(stack, "alice"))
        for socket in (*alice, bob):
            socket.close()


def reconnect(stack, user: str) -> bool:
    try:
        dashboard(stack, user, None).close()
    except InvalidStatus:
        return False
    return True


def test_the_handshake_names_the_session_and_requires_a_user(stack) -> None:
    socket = connect(f"{stack.ws_url}/ws?user_id=alice")
    ready = orjson.loads(socket.recv(timeout=5))
    assert ready["type"] == "ready" and ready["session_id"]
    socket.close()
    with pytest.raises(InvalidStatus):
        connect(f"{stack.ws_url}/ws")


def test_a_reconnected_dashboard_sees_new_positions(stack, http) -> None:
    first = dashboard(stack, "alice", WORLD)
    first.close()
    second = dashboard(stack, "alice", RIGA)
    assert http.post("/locations", json=report("again", offset=1)).status_code == 202
    while (message := orjson.loads(second.recv(timeout=10)))["type"] != "positions":
        pass
    assert message["items"][0][0] == "again"
    second.close()
