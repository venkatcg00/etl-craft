"""Confirm an expired lock holder's warehouse statements are gone before its lock moves.

A statement keeps running on a warehouse server after its client dies, and may still commit.
So a new holder stops the expired holder's tagged statements and waits until none remain; only
then may the lock pass. Asking is not enough: a cancel can race the statement's own commit.
"""

from __future__ import annotations

import time

from sqlalchemy.exc import SQLAlchemyError

from etl_craft.config import ConnectorConfig
from etl_craft.core.errors import LockTimeoutError
from etl_craft.warehouse.connection import build_warehouse_engine, warehouse_dialect

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
