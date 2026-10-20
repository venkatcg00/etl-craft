"""Readiness, bounded request draining and explicit proxy trust for the API tier."""

import asyncio
import logging
import os
import threading
from dataclasses import dataclass, field
from ipaddress import ip_network

from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

from etl_craft.core.enums import HttpScheme
from etl_craft.core.errors import UsageError
from etl_craft.core.http import forwarded_visitor
from etl_craft.services.operations import OperationContext


@dataclass
class Gateway:
    """Process-local connection lifecycle; all application state remains in the Engine DB."""

    stop: threading.Event
    active: set[asyncio.Task[object]] = field(default_factory=set)
    changed: asyncio.Event = field(default_factory=asyncio.Event)

    deadline: threading.Timer | None = field(default=None, init=False)

    def begin_shutdown(self, seconds: float) -> None:
        """Bound even blocked executor work that cannot respond to task cancellation."""
        self.stop.set()
        self.deadline = threading.Timer(seconds, self._exit_at_deadline)
        self.deadline.daemon = True
        self.deadline.start()

    def close(self) -> None:
        """Cancel the forced exit after normal server cleanup completes."""
        if self.deadline is not None:
            self.deadline.cancel()

    @staticmethod
    def _exit_at_deadline() -> None:
        try:
            logging.getLogger(__name__).warning("UI shutdown grace expired; closing remaining work")
        finally:
            os._exit(0)

    async def drain(self, seconds: float) -> None:
        """Let admitted requests finish until the configured shutdown deadline."""
        self.stop.set()
        try:
            async with asyncio.timeout(seconds):
                while self.active:
                    self.changed.clear()
                    await self.changed.wait()
        except TimeoutError:
            pass


class GatewayMiddleware:
    """Refuse new work during shutdown, track streams, and trust only configured proxies."""

    def __init__(self, app: ASGIApp, *, ctx: OperationContext, gateway: Gateway | None) -> None:
        """Read the same explicit proxy networks used by catalog publication."""
        self.app = app
        self.gateway = gateway
        site = ctx.config.docs_site
        self.trusted = (
            tuple(ip_network(cidr, strict=False) for cidr in site.trusted_proxies) if site else ()
        )

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Keep a request admitted before SIGTERM alive through the end of its stream."""
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        state = self.gateway
        if state is not None and state.stop.is_set():
            await JSONResponse({"message": "ui is shutting down; retry another copy"}, 503)(
                scope, receive, send
            )
            return
        try:
            self._proxy(scope)
        except UsageError as error:
            await JSONResponse({"message": str(error)}, 400)(scope, receive, send)
            return
        task = asyncio.current_task()
        assert task is not None
        if state is not None:
            state.active.add(task)
        try:
            await self.app(scope, receive, send)
        finally:
            if state is not None:
                state.active.discard(task)
                state.changed.set()

    def _proxy(self, scope: Scope) -> None:
        peer = scope.get("client")
        if peer is None:
            return
        headers = scope["headers"]
        forwarded = [v.decode("latin-1") for k, v in headers if k.lower() == b"x-forwarded-for"]
        visitor = forwarded_visitor(peer[0], forwarded, self.trusted)
        if not visitor.trusted_proxy:
            return
        scope["client"] = (visitor.address, 0)
        proto = [v.decode("latin-1") for k, v in headers if k.lower() == b"x-forwarded-proto"]
        if not proto:
            return
        if len(proto) != 1 or proto[0] not in {scheme.value for scheme in HttpScheme}:
            raise UsageError(
                f"X-Forwarded-Proto={proto!r}; expected one http or https value; "
                "configure the trusted proxy to overwrite the header"
            )
        scope["scheme"] = HttpScheme(proto[0])
