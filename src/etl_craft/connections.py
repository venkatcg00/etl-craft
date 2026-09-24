"""Connection tests: one probe per connection, used by `doctor`, `setup` and run initialization.

[ADDITION, 2026-09-24] Per explicit instruction: "the connection tests should
happen at initialize time and fail if connections fail". Two things in this
engine initialize, and both now test before they do anything:

* **`setup`** initializes a deployment. It runs every `doctor` check -- the
  Engine DB, the warehouse and the email relay of the selected profiles --
  before creating or migrating anything, and fails naming each one that did
  not connect.
* **A pipeline run** is initialized by `run --init-only` (a generated DAG's
  first step) or by `run --pipeline_code X` in local mode, before either
  mints or resumes a run. `check_run_connections` tests the connections that
  run will use: the warehouse when a task is `SQL` or `BUSINESS_RULES` or
  cloning is on, the email relay when a task is `EMAIL_ALERT`. A failure
  stops the run before a `pipeline_run_id` exists, so nothing is written to
  the audit log and, under an orchestrator, no task starts. The Engine DB
  needs no separate test there: resolving the pipeline already used it.

[CHOICE] A single-writer warehouse (a DuckDB file) is not probed at run
initialization. There is no server to be unreachable, and a pipeline running
at the same moment legitimately holds the one writer's lock: a probe queued
behind it would fail this run for a busy file, not a broken connection.
`setup` and `doctor` still open it.
"""

from __future__ import annotations

import smtplib

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from etl_craft.config import ConfigError, ConnectorConfig
from etl_craft.db import build_engine
from etl_craft.warehouse import READ_ONLY_WAIT_SECONDS, is_single_writer, open_warehouse

SMTP_PROBE_TIMEOUT_SECONDS = 10.0

# The handlers whose tasks open the warehouse. PYTHON scripts connect by
# themselves, so the engine has nothing of theirs to test.
WAREHOUSE_HANDLERS = frozenset({"SQL", "BUSINESS_RULES"})


class ConnectionTestError(Exception):
    """A connection a pipeline run needs failed its test before the run started."""


def probe_engine_db(config: ConnectorConfig) -> str | None:
    """Connect to the Engine DB and run SELECT 1; return the failure, or None."""
    engine = None
    try:
        engine = build_engine(config)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except (ConfigError, SQLAlchemyError) as exc:
        return str(exc)
    finally:
        # A probe that leaves pooled connections open blocks anything trying
        # to drop the database afterwards.
        if engine is not None:
            engine.dispose()
    return None


def probe_warehouse(
    config: ConnectorConfig,
    engine_db: Engine | None,
    *,
    wait_seconds: int = READ_ONLY_WAIT_SECONDS,
) -> str | None:
    """Open the warehouse the way a task does and run SELECT 1; return the failure, or None.

    Goes through `open_warehouse`, so a single-writer warehouse queues behind a
    running task (for up to `wait_seconds`) instead of failing on its file lock.
    `engine_db` holds that queue; without one the probe runs unserialized.
    """
    try:
        with (
            open_warehouse(config, engine_db, wait_seconds=wait_seconds) as warehouse_engine,
            warehouse_engine.connect() as conn,
        ):
            conn.execute(text("SELECT 1"))
    except (ConfigError, SQLAlchemyError, NotImplementedError) as exc:
        return str(exc)
    return None


def probe_email(config: ConnectorConfig) -> str | None:
    """Reach the SMTP relay and send NOOP; return the failure, or None.

    Deliberately no login: a NOOP proves reachability without spending an
    authentication attempt against a relay that may rate-limit or lock out.
    """
    if config.email is None:
        return None
    profile = config.email.active
    try:
        with smtplib.SMTP(profile.host, profile.port, timeout=SMTP_PROBE_TIMEOUT_SECONDS) as server:
            server.noop()
    except (smtplib.SMTPException, OSError) as exc:
        return f"{profile.host}:{profile.port}: {exc}"
    return None


def check_run_connections(
    engine: Engine, config: ConnectorConfig, pipeline_code: str, handlers: set[str]
) -> None:
    """Test every connection a run of this pipeline will use; raise naming each failure."""
    failures: list[str] = []
    uses_warehouse = bool(handlers & WAREHOUSE_HANDLERS) or config.cloning.enabled
    if config.warehouse is not None and uses_warehouse and not is_single_writer(config):
        problem = probe_warehouse(config, engine)
        if problem is not None:
            failures.append(f"warehouse: {problem}")
    if config.email is not None and "EMAIL_ALERT" in handlers:
        problem = probe_email(config)
        if problem is not None:
            failures.append(f"email relay: {problem}")
    if failures:
        raise ConnectionTestError(
            f"{pipeline_code}: connection test failed, so no run was started — "
            + "; ".join(failures)
        )
