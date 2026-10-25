"""Publish durable agent chunks after the attempt's independent outcome commit."""

import logging
from dataclasses import replace
from datetime import UTC, datetime

from sqlalchemy.engine import Connection

from etl_craft.core.actor import SYSTEM_ACTOR, acting_as
from etl_craft.core.enums import ActionName, ActionOutcome
from etl_craft.core.errors import EtlCraftError
from etl_craft.core.faults import fault_point
from etl_craft.core.text import byte_tail, sha256_hex
from etl_craft.engine.audit import ActionRequest
from etl_craft.engine.logs import (
    LIVE_ATTEMPTS,
    log_key,
    log_location,
    publish_log,
    read_log_attempt,
    shared_log,
)
from etl_craft.engine.queries import statement
from etl_craft.services.operations.context import OperationContext

logger = logging.getLogger(__name__)


def tail(conn: Connection, attempt_id: int, maximum: int) -> str:
    """Read backwards only until the configured output budget is filled."""
    budget = maximum
    parts: list[str] = []
    chunk = 0
    while budget > 0:
        content = conn.execute(
            statement(conn, "log_tail"), {"attempt_id": attempt_id, "chunk_number": chunk}
        ).scalar_one_or_none()
        if content is None:
            break
        part = byte_tail(content, budget)
        parts.append(part)
        budget -= len(content.encode())
        chunk += 1
    return "".join(reversed(parts))


def publish(ctx: OperationContext, attempt_id: int) -> None:
    """Record failed publication separately, leaving the committed task outcome intact."""
    try:
        _publish(ctx, attempt_id)
    except EtlCraftError as error:
        with ctx.engine.begin() as conn:
            row = read_log_attempt(conn, attempt_id)
            ActionRequest(
                ActionName.AGENT_LOG,
                {"attempt_id": attempt_id, "error": str(error)},
                datetime.now(UTC),
                actor=ctx.actor,
                outcome=ActionOutcome.FAILURE,
                exit_code=error.exit_code,
                project_id=None if row is None else row.project_id,
            ).write(conn)
        raise


def _publish(ctx: OperationContext, attempt_id: int) -> None:
    """Serialize publication with retirement; LOG_PATH acknowledges a complete external file."""
    with ctx.engine.begin() as conn:
        conn.execute(statement(conn, "log_lock"), {"attempt_id": attempt_id})
        row = read_log_attempt(conn, attempt_id)
        if (
            row is None
            or row.log_path is not None
            or row.log_removed_at is not None
            or row.status in LIVE_ATTEMPTS
        ):
            return
        key = log_key(
            row.engine_id, row.project_id, row.pipeline_run_id, row.task_run_id, attempt_id
        )
        location = log_location(ctx.config, key)
        publish_log(ctx.config, key, shared_log(conn, attempt_id))
        fault_point("logs.after_upload")
        conn.execute(
            statement(conn, "log_published"), {"attempt_id": attempt_id, "log_path": location}
        )
        ActionRequest(
            ActionName.AGENT_LOG,
            {"attempt_id": attempt_id, "log_path": location},
            datetime.now(UTC),
            actor=ctx.actor,
            outcome=ActionOutcome.SUCCESS,
            project_id=row.project_id,
        ).write(conn)
    fault_point("logs.after_publication")


def publish_pending(ctx: OperationContext) -> None:
    """Retry terminal chunks across restarts, recording each failed upload independently."""
    after = 0
    ctx = replace(ctx, actor=SYSTEM_ACTOR)
    with acting_as(SYSTEM_ACTOR):
        while True:
            with ctx.engine.connect() as conn:
                rows = conn.execute(statement(conn, "log_pending"), {"after_id": after}).all()
            if not rows:
                return
            for row in rows:
                after = row.attempt_id
                try:
                    publish(ctx, row.attempt_id)
                except EtlCraftError as error:
                    # Controller maintenance boundary: the outcome and retry source remain intact.
                    logger.error("attempt_id=%s: %s", row.attempt_id, error)


def ensure_source(conn: Connection, attempt_id: int) -> None:
    """Keep a durable pending-upload source even for an empty log."""
    if (
        conn.execute(statement(conn, "agent_chunk_last"), {"attempt_id": attempt_id}).first()
        is None
    ):
        conn.execute(
            statement(conn, "agent_chunk_insert"),
            {
                "attempt_id": attempt_id,
                "sequence": 0,
                "hash": sha256_hex(b""),
                "content": "",
                "offset": 0,
                "length": 0,
                "now": datetime.now(UTC),
            },
        )
