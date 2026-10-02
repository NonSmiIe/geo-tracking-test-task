import orjson
from starlette.types import Message, Receive, Scope, Send

from geo_tracking.api.limits import BodyLimit

LIMIT = 1024


class Echo:
    def __init__(self, respond_first: bool = False) -> None:
        self.respond_first = respond_first
        self.received: list[Message] = []
        self.called = False

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        self.called = True
        if self.respond_first:
            await send({"type": "http.response.start", "status": 200, "headers": []})
        body = b""
        while True:
            message = await receive()
            self.received.append(message)
            if message["type"] == "http.disconnect":
                return
            body += message.get("body", b"")
            if not message.get("more_body"):
                break
        if not self.respond_first:
            await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": body})


async def call(app: Echo, chunks: list[bytes], scope_type: str = "http", length: int | None = None):
    headers = [] if length is None else [(b"content-length", str(length).encode())]
    queue = [
        {"type": "http.request", "body": chunk, "more_body": index < len(chunks) - 1}
        for index, chunk in enumerate(chunks)
    ]
    sent: list[Message] = []

    async def receive() -> Message:
        return queue.pop(0) if queue else {"type": "http.disconnect"}

    async def send(message: Message) -> None:
        sent.append(message)

    await BodyLimit(app, LIMIT)({"type": scope_type, "headers": headers}, receive, send)
    return sent


def status(sent: list[Message]) -> int:
    return next(message["status"] for message in sent if message["type"] == "http.response.start")


async def test_a_body_within_the_limit_passes_through_whole() -> None:
    app = Echo()
    sent = await call(app, [b"a" * 600, b"b" * 424], length=LIMIT)
    assert status(sent) == 200 and sent[-1]["body"] == b"a" * 600 + b"b" * 424


async def test_a_declared_length_over_the_limit_never_reaches_the_app() -> None:
    app = Echo()
    sent = await call(app, [b"x"], length=LIMIT + 1)
    assert not app.called and status(sent) == 413
    assert orjson.loads(sent[-1]["body"]) == {"detail": "body_capacity"}


async def test_a_chunked_body_over_the_limit_is_cut_off_with_413() -> None:
    app = Echo()
    sent = await call(app, [b"x" * 600, b"x" * 600, b"x" * 600])
    assert status(sent) == 413 and len(sent) == 2
    assert app.received[-1] == {"type": "http.disconnect"}
    assert len(app.received) == 2


async def test_an_overflow_after_the_response_started_sends_nothing_more() -> None:
    app = Echo(respond_first=True)
    sent = await call(app, [b"x" * 600, b"x" * 600])
    assert [message["type"] for message in sent] == ["http.response.start"]
    assert status(sent) == 200


async def test_non_http_scopes_are_not_inspected() -> None:
    app = Echo()
    await call(app, [b"x" * (LIMIT * 4)], scope_type="websocket", length=LIMIT * 4)
    assert app.called and app.received[0]["body"] == b"x" * (LIMIT * 4)
