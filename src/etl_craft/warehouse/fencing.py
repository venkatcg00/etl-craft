"""Confirm an expired lock holder's warehouse statements are gone before its lock moves.

A statement keeps running on a warehouse server after its client dies, and may still commit.
So a new holder stops the expired holder's tagged statements and waits until none remain; only
then may the lock pass. Asking is not enough: a cancel can race the statement's own commit.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass

from sqlalchemy.exc import SQLAlchemyError

from etl_craft.config import ConnectorConfig
from etl_craft.core.errors import EtlCraftError, LockTimeoutError
from etl_craft.warehouse.connection import build_warehouse_engine, warehouse_dialect

logger = logging.getLogger(__name__)


@dataclass
class Cancellation:
    """A signal remains cancellation even if a driver replaces the interrupt with its error."""

    config: ConnectorConfig
    tag: str
    requested: bool = False


_cancellation: ContextVar[Cancellation | None] = ContextVar("query_cancellation", default=None)

STOP_WAIT_SECONDS = 30
POLL_SECONDS = 0.5


def cluster_problem(config: ConnectorConfig) -> str | None:
    """Why cluster execution cannot fence this deployment's warehouse, or ``None``."""
    if config.warehouse is None:
        return None
    dialect = warehouse_dialect(config)
    if dialect.session_fencing:
        return None
    return (
        f"Deployment.Profile is CONTROLLER_AGENTS, but a {dialect.spec.display_name} warehouse "
        "cannot stop an expired holder's statements, so a lock could pass while its query "
        "still writes; use local execution (Deployment.Profile LAPTOP or CONTROLLER)"
    )


def confirm_stopped(config: ConnectorConfig, tag: str) -> None:
    """Stop every statement labelled ``tag`` and return once none remains running.

    A warehouse without session fencing runs only local execution (``cluster_problem``
    refuses the cluster), where reconciliation has already verified the holder's process
    ended; its server statements are not stopped.
    """
    if config.warehouse is None or not warehouse_dialect(config).session_fencing:
        return
    dialect = warehouse_dialect(config)
    deadline = time.monotonic() + STOP_WAIT_SECONDS
    engine = build_warehouse_engine(config)
    try:
        while True:
            # One transaction per check: PostgreSQL keeps one activity snapshot per transaction.
            with engine.begin() as conn:
                dialect.stop_tagged(conn, tag)
            with engine.begin() as conn:
                running = dialect.running_tagged(conn, tag)
            if running == 0:
                return
            if time.monotonic() >= deadline:
                raise LockTimeoutError(
                    f"{running} warehouse statement(s) tagged {tag} still run after "
                    f"{STOP_WAIT_SECONDS}s; stop them in the warehouse, then retry"
                )
            time.sleep(POLL_SECONDS)
    except SQLAlchemyError as error:
        raise LockTimeoutError(
            f"cannot confirm that statements tagged {tag} stopped: {type(error).__name__}: "
            f"{error}; check the warehouse connection, then retry"
        ) from error
    finally:
        engine.dispose()


@contextmanager
def cancellation_scope(config: ConnectorConfig, tag: str) -> Iterator[None]:
    """Bind the session identity a task or probe's SIGTERM handler must cancel."""
    state = Cancellation(config, tag)
    token = _cancellation.set(state)
    try:
        try:
            yield
        except Exception as error:
            if state.requested:
                raise KeyboardInterrupt("warehouse task cancelled") from error
            raise
        if state.requested:
            raise KeyboardInterrupt("warehouse task cancelled")
    finally:
        _cancellation.reset(token)


def stop_current_queries() -> None:
    """Cancel before exiting on SIGTERM; the supervisor independently confirms after exit."""
    scope = _cancellation.get()
    if scope is None:
        return
    scope.requested = True
    config, tag = scope.config, scope.tag
    if config.warehouse is None or warehouse_dialect(config).process_local_queries:
        logger.info("%s: stopping the embedded warehouse process ends its query", tag)
        return
    if not warehouse_dialect(config).session_fencing:
        logger.warning("%s: only the process is stopping; warehouse queries are unconfirmed", tag)
        return
    try:
        confirm_stopped(config, tag)
        logger.info("%s: warehouse queries confirmed stopped", tag)
    except EtlCraftError as error:
        logger.error("%s: warehouse cancellation remains unconfirmed: %s", tag, error)
