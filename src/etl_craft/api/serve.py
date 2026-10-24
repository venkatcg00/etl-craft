"""Serve HTTP alone or with the controller in one shutdown lifecycle."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from types import FrameType

import anyio.to_thread
import uvicorn
from fastapi import FastAPI

from etl_craft.api.app import create_app
from etl_craft.api.gateway import Gateway
from etl_craft.config.model import parse_api_address
from etl_craft.controller.supervisor import serve
from etl_craft.core.http import bind_api_address
from etl_craft.services.operations.context import OperationContext


def serve_http(ctx: OperationContext, stop: threading.Event, *, controller: bool = True) -> None:
    """Bind HTTP before taking leadership, then share the process shutdown lifecycle."""
    host, port = parse_api_address(ctx.config.api_address)
    listener = bind_api_address(host, port)
    errors: list[BaseException] = []

    gateway = None if controller else Gateway(stop)

    class StoppableServer(uvicorn.Server):
        draining: asyncio.Task[None] | None = None

        async def on_tick(self, counter: int) -> bool:
            if gateway is not None and stop.is_set() and self.draining is None:
                gateway.begin_shutdown(ctx.config.limits.shutdown_grace_seconds)
                self.should_exit = False
                self.draining = asyncio.create_task(self.drain_ui())
            return (controller and stop.is_set()) or await super().on_tick(counter)

        def handle_exit(self, sig: int, frame: FrameType | None) -> None:
            super().handle_exit(sig, frame)
            if gateway is None:
                stop.set()
            elif not stop.is_set():
                gateway.begin_shutdown(ctx.config.limits.shutdown_grace_seconds)
                self.should_exit = False
                self.draining = asyncio.create_task(self.drain_ui())

        async def drain_ui(self) -> None:
            assert gateway is not None
            await gateway.drain(ctx.config.limits.shutdown_grace_seconds)
            self.should_exit = True

    def supervise() -> None:
        try:
            serve(ctx, stop)
        except BaseException as error:
            errors.append(error)
            server.should_exit = True

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if ctx.config.sizing is not None:
            limiter = anyio.to_thread.current_default_thread_limiter()
            limiter.total_tokens = ctx.config.sizing.server_threads.value
        thread = (
            threading.Thread(target=supervise, name="etl-craft-controller") if controller else None
        )
        if thread is not None:
            thread.start()
        try:
            yield
        finally:
            stop.set()
            if thread is not None:
                await asyncio.to_thread(thread.join)

    app = create_app(ctx, gateway=gateway)
    app.router.lifespan_context = lifespan
    server = StoppableServer(
        uvicorn.Config(
            app,
            host=host,
            port=port,
            log_config=None,
            access_log=False,
            timeout_graceful_shutdown=ctx.config.limits.shutdown_grace_seconds if controller else 0,
            proxy_headers=False,
        )
    )
    try:
        server.run(sockets=[listener])
    finally:
        if gateway is not None:
            gateway.close()
        listener.close()
        if errors:
            raise errors[0]
