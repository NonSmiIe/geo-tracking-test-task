import orjson
from starlette.types import ASGIApp, Message, Receive, Scope, Send

TOO_LARGE = orjson.dumps({"detail": "body_capacity"})


class BodyLimit:
    def __init__(self, app: ASGIApp, limit: int) -> None:
        self.app, self.limit = app, limit

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        length = dict(scope["headers"]).get(b"content-length")
        if length is not None and int(length) > self.limit:
            await self.refuse(send)
            return
        seen = 0
        started = refused = False

        async def limited() -> Message:
            nonlocal seen, refused
            message = await receive()
            seen += len(message.get("body", b""))
            if seen > self.limit and not refused:
                refused = True
                if not started:
                    await self.refuse(send)
                return {"type": "http.disconnect"}
            return message

        async def tracked(message: Message) -> None:
            nonlocal started
            if refused:
                return
            started = started or message["type"] == "http.response.start"
            await send(message)

        await self.app(scope, limited, tracked)

    async def refuse(self, send: Send) -> None:
        await send(
            {
                "type": "http.response.start",
                "status": 413,
                "headers": [(b"content-type", b"application/json")],
            }
        )
        await send({"type": "http.response.body", "body": TOO_LARGE})
