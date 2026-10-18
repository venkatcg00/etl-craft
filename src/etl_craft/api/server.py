"""Serve HTTP and the existing local overseer within one shutdown lifecycle."""

from __future__ import annotations

import asyncio
import socket
import threading
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import anyio.to_thread
import uvicorn
from fastapi import FastAPI

from etl_craft.api.app import create_app
from etl_craft.config.model import parse_api_address
from etl_craft.core.errors import RunRefusedError
from etl_craft.overseer.server import serve
from etl_craft.services.operations.context import OperationContext


def bind_api_address(host: str, port: int) -> socket.socket:
    """Bind the listening socket before anything starts, refusing an address already in use."""
    family = socket.AF_INET6 if ":" in host else socket.AF_INET
    sock = socket.socket(family, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.bind((host, port))
    except OSError as error:
        sock.close()
        raise RunRefusedError(
            f"Orchestration.Api_address {host}:{port} cannot be used: {error.strerror}; stop the "
            "process listening there, or set Api_address to a free port (port 0 picks one)"
        ) from error
    return sock


def serve_http(ctx: OperationContext, stop: threading.Event) -> None:
    """Run HTTP in the main thread; stop and join the overseer before disposing its DB.

    The address is bound first, so a port in use refuses the server before it takes leadership
    or supervises anything.
    """
    host, port = parse_api_address(ctx.config.api_address)
    listener = bind_api_address(host, port)
    errors: list[BaseException] = []

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
        server.run(sockets=[listener])
    finally:
        listener.close()
        if errors:
            raise errors[0]
