"""Bound warehouse event queries in a fresh, cancellable interpreter."""

from __future__ import annotations

import multiprocessing
import os
import threading
import time
from datetime import UTC, datetime
from multiprocessing.connection import Connection
from typing import Any, cast

from etl_craft.config import ConnectorConfig
from etl_craft.core.actor import Actor, acting_as, current_actor
from etl_craft.core.errors import HandlerError, LockTimeoutError
from etl_craft.dialects.engine import build_engine
from etl_craft.handlers.event_wait import Event, probe


def probe_sql(
    config: ConnectorConfig, event: Event, remaining_seconds: float, cancel: threading.Event | None
) -> dict[str, Any] | None:
    """Check at most thirty seconds, or the remaining event budget; stop on cancellation."""
    context = multiprocessing.get_context("spawn")
    receive, send = context.Pipe(duplex=False)
    limit = min(30, remaining_seconds)
    # A spawned interpreter starts with no actor; the probe acts as whoever asked for it.
    args = (send, config, event, limit, current_actor())
    process = context.Process(target=_query, args=args, daemon=True)
    deadline = time.monotonic() + limit
    try:
        process.start()
        send.close()
        while time.monotonic() < deadline and not (cancel and cancel.is_set()):
            if receive.poll(min(0.1, max(0, deadline - time.monotonic()))):
                try:
                    status, result = receive.recv()
                except EOFError as error:
                    raise HandlerError(
                        "EVENT_SQL probe exited without a result; check the warehouse connection"
                    ) from error
                if status == "waiting":
                    raise LockTimeoutError(result)
                if status == "error":
                    raise HandlerError(f"EVENT_SQL probe failed: {result}")
                return cast("dict[str, Any] | None", result)
        raise LockTimeoutError(
            "EVENT_SQL probe is still waiting; check again within the event budget"
        )
    finally:
        send.close()
        receive.close()
        if process.pid is not None:
            process.join(timeout=0.1)
            if process.is_alive():
                process.terminate()
                process.join(timeout=0.5)
            if process.is_alive():
                process.kill()
                process.join()
            process.close()


def _query(
    send: Connection, config: ConnectorConfig, event: Event, limit: float, actor: Actor
) -> None:
    threading.Thread(target=_watch_parent, args=(limit,), daemon=True).start()
    engine = None
    try:
        engine = build_engine(config)
        with acting_as(actor):
            evidence, _ = probe(event, config, engine, datetime.now(UTC), {}, set())
        send.send(("ready", evidence))
    except LockTimeoutError as error:
        send.send(("waiting", str(error)))
    except Exception as error:
        send.send(("error", f"{type(error).__name__}: {error}"))
    finally:
        if engine is not None:
            engine.dispose()
        send.close()


def _watch_parent(limit: float) -> None:
    # A killed supervisor cannot reap its probe or enforce its time limit.
    parent = multiprocessing.parent_process()
    assert parent is not None
    deadline = time.monotonic() + limit
    while parent.is_alive() and time.monotonic() < deadline:
        time.sleep(0.1)
    os._exit(1)
