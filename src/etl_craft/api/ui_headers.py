"""Browser defenses on documents, assets and action responses."""

from uuid import uuid4

from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

POLICY = (
    "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; "
    "connect-src 'self'; frame-src 'self'; frame-ancestors 'self'; "
    "base-uri 'none'; form-action 'self'; object-src 'none'"
)


def protect(headers: MutableHeaders, request_id: str) -> None:
    """Apply the same defenses to regular responses and outer-boundary error responses."""
    for name, value in {
        "Content-Security-Policy": POLICY,
        "X-Frame-Options": "SAMEORIGIN",
        "X-Content-Type-Options": "nosniff",
        "Referrer-Policy": "same-origin",
        "Cache-Control": "no-store",
    }.items():
        headers.setdefault(name, value)
    headers["X-Request-ID"] = request_id


class BrowserHeaders:
    """Use a server-generated request id and refuse cross-site framing and caching."""

    def __init__(self, app: ASGIApp) -> None:
        """Keep the middleware independent of database or session state."""
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Websocket lifecycles pass through; HTTP responses share one request identity."""
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        request_id = uuid4().hex
        scope.setdefault("state", {})["request_id"] = request_id

        async def guarded(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = MutableHeaders(scope=message)
                protect(headers, request_id)
            await send(message)

        await self.app(scope, receive, guarded)
