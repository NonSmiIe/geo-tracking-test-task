from geo_tracking.api.limits import App, Receive, Scope, Send
from geo_tracking.logs import REQUEST_ID


class RequestContext:
    def __init__(self, app: App) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        value = dict(scope.get("headers", ())).get(b"x-request-id")
        if value is None:
            await self.app(scope, receive, send)
            return
        token = REQUEST_ID.set(value.decode("latin-1")[:128])
        try:
            await self.app(scope, receive, send)
        finally:
            REQUEST_ID.reset(token)
