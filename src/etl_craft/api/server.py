"""Serve HTTP and the existing local overseer within one shutdown lifecycle."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from etl_craft.api.app import create_app
from etl_craft.config.model import parse_api_address
from etl_craft.overseer.server import serve
from etl_craft.services.operations.context import OperationContext


def serve_http(ctx: OperationContext, stop: threading.Event) -> None:
    """Run HTTP in the main thread; stop and join the overseer before disposing its DB."""
    host, port = parse_api_address(ctx.config.api_address)
    errors: list[BaseException] = []

    def supervise() -> None:
        try:
            serve(ctx, stop)
        except BaseException as error:
            errors.append(error)
            server.should_exit = True

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        thread = threading.Thread(target=supervise, name="etl-craft-overseer")
        thread.start()
        try:
            yield
        finally:
            stop.set()
            await asyncio.to_thread(thread.join)

    app = create_app(ctx)
    app.router.lifespan_context = lifespan
    server = uvicorn.Server(
        uvicorn.Config(
            app,
            host=host,
            port=port,
            log_config=None,
            access_log=False,
            timeout_graceful_shutdown=ctx.config.limits.shutdown_grace_seconds,
        )
    )
    try:
        server.run()
    finally:
        if errors:
            raise errors[0]
