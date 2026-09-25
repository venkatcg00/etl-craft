"""Connection tests run before a pipeline run starts.

``run --pipeline_code`` and ``run --init-only`` test the connections the run will use before they
start or resume it, and stop when one fails, so no run is recorded and no task starts:

- the warehouse, when a task's handler is ``SQL`` or ``BUSINESS_RULES`` or cloning is on, and
  with cloning on, that the warehouse profile's ``schema``, where cloning writes, exists;
- the email relay, when a task's handler is ``EMAIL_ALERT``, or SLA emails are on and the
  pipeline has an SLA.

The Engine DB needs no test of its own: finding the pipeline already used it. ``PYTHON`` scripts
connect by themselves, so they add nothing. A single-writer warehouse (a DuckDB file) is not
tested: there is no server to be down, and a running task may hold its one writer's lock.
"""

from __future__ import annotations

import logging
import smtplib

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from etl_craft.config import ConnectorConfig
from etl_craft.core.enums import Handler
from etl_craft.core.errors import ConnectionTestError, EtlCraftError
from etl_craft.warehouse.connection import (
    READ_ONLY_WAIT_SECONDS,
    is_single_writer,
    open_warehouse,
    warehouse_schema_problem,
)

logger = logging.getLogger(__name__)

SMTP_TEST_TIMEOUT_SECONDS = 10.0
"""How long the email relay test waits for the relay."""

WAREHOUSE_HANDLERS = frozenset({Handler.SQL, Handler.BUSINESS_RULES})
"""The handlers whose tasks the engine runs against the warehouse."""


def probe_warehouse(
    config: ConnectorConfig, engine_db: Engine | None = None, *, schema: bool = False
) -> str | None:
    """Open the warehouse as a task does and run ``SELECT 1``; return the problem, or ``None``.

    With ``schema``, the profile's schema must exist too.
    """
    try:
        with open_warehouse(config, engine_db, wait_seconds=READ_ONLY_WAIT_SECONDS) as warehouse:
            with warehouse.connect() as conn:
                conn.execute(text("SELECT 1"))
            return warehouse_schema_problem(config, warehouse) if schema else None
    except (EtlCraftError, SQLAlchemyError, OSError) as error:
        return str(error)


def probe_email_relay(config: ConnectorConfig) -> str | None:
    """Reach the email relay and send ``NOOP``; return the problem, or ``None``.

    It does not log in, so the test never spends an attempt against a relay that locks
    accounts out.
    """
    if config.email is None:
        return None
    profile = config.email.active
    try:
        with smtplib.SMTP(profile.host, profile.port, timeout=SMTP_TEST_TIMEOUT_SECONDS) as relay:
            relay.noop()
    except (smtplib.SMTPException, OSError) as error:
        return f"{profile.host}:{profile.port}: {error}"
    return None


def check_run_connections(
    engine: Engine,
    config: ConnectorConfig,
    pipeline_code: str,
    handlers: set[str],
    *,
    sends_sla_email: bool = False,
) -> None:
    """Test each connection a run of the pipeline uses; ``ConnectionTestError`` names failures."""
    failures: list[str] = []
    uses_warehouse = bool(handlers & WAREHOUSE_HANDLERS) or config.cloning.enabled
    if config.warehouse is not None and uses_warehouse and not is_single_writer(config):
        problem = probe_warehouse(config, engine, schema=config.cloning.enabled)
        if problem is not None:
            failures.append(f"warehouse: {problem}")
    uses_email = Handler.EMAIL_ALERT in handlers or sends_sla_email
    if config.email is not None and uses_email:
        problem = probe_email_relay(config)
        if problem is not None:
            failures.append(f"email relay: {problem}")
    if failures:
        raise ConnectionTestError(
            f"{pipeline_code}: a connection test failed, so no run was started — "
            + "; ".join(failures)
        )
    logger.debug("%s: connection tests passed", pipeline_code)
