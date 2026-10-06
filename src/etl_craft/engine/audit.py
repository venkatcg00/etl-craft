"""Immutable command requests and metadata changes made through the Engine DB."""

from __future__ import annotations

import json
import logging
import socket
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from etl_craft.core.actor import current_actor

logger = logging.getLogger(__name__)

MUTATING_COMMANDS = frozenset(
    {
        "run",
        "mark",
        "cancel",
        "reconcile",
        "upgrade-targets",
        "pause",
        "resume",
        "migrate",
        "setup",
        "init-db",
        "clone",
        "generate-docs",
        "publish-docs",
        "docs-version",
        "lineage",
    }
)
SENSITIVE = ("password", "secret", "token", "credential", "private_key")


def mask_arguments(arguments: Mapping[str, Any]) -> dict[str, Any]:
    """Mask values of sensitive names, including nested arguments, before serialization."""
    return {
        key: "[REDACTED]"
        if any(word in key.lower() for word in SENSITIVE)
        else mask_arguments(value)
        if isinstance(value, dict)
        else value
        for key, value in arguments.items()
        if key not in {"handler"}
    }


@dataclass
class ActionRequest:
    """One command request, recorded once when its Engine DB becomes available."""

    command: str
    arguments: dict[str, Any]
    requested_at: datetime
    engine: Engine | None = None
    recorded: bool = False

    def record(self) -> None:
        """Record the request itself; flow outcomes belong to run and attempt logs."""
        if self.recorded or self.engine is None:
            return
        engine = self.engine
        with engine.begin() as conn:
            inspector = inspect(conn)
            schema = None if engine.dialect.name == "sqlite" else inspector.default_schema_name
            name = "AUD_ACTIONS" if engine.dialect.name == "sqlite" else "aud_actions"
            if not inspector.has_table(name, schema=schema):
                return
            pipeline = conn.execute(
                text(
                    "SELECT PIPELINE_ID AS pipeline_id FROM CFG_PIPELINES "
                    "WHERE PIPELINE_CODE=:code AND ACTIVE_FLAG='Y'"
                ),
                {"code": self.arguments.get("pipeline_code")},
            ).scalar_one_or_none()
            task = None
            if pipeline is not None and self.arguments.get("task_code"):
                task = conn.execute(
                    text(
                        "SELECT TASK_ID AS task_id FROM CFG_TASKS WHERE PIPELINE_ID=:pipeline "
                        "AND TASK_CODE=:code AND ACTIVE_FLAG='Y'"
                    ),
                    {"pipeline": pipeline, "code": self.arguments["task_code"]},
                ).scalar_one_or_none()
            actor = current_actor()
            payload = json.dumps(self.arguments, default=str)
            arguments = (
                "CAST(:arguments AS JSONB)" if engine.dialect.name == "postgresql" else ":arguments"
            )
            conn.execute(
                text(
                    "INSERT INTO AUD_ACTIONS (STARTED_AT, ENDED_AT, ACTOR, ACTOR_KIND, HOST, "
                    "COMMAND, "
                    "ARGUMENTS, PIPELINE_ID, TASK_ID, OUTCOME) VALUES "
                    f"(:started, :ended, :actor, :kind, :host, :command, {arguments}, "
                    ":pipeline, :task, 'REQUESTED')"
                ),
                {
                    "started": self.requested_at,
                    "ended": datetime.now(UTC),
                    "actor": actor.name,
                    "kind": actor.kind.value,
                    "host": socket.gethostname(),
                    "command": self.command,
                    "arguments": payload,
                    "pipeline": pipeline,
                    "task": task,
                },
            )
        self.recorded = True


_request: ContextVar[ActionRequest | None] = ContextVar("etl_craft_action_request", default=None)


def register_engine(engine: Engine) -> None:
    """Bind a command's first Engine DB and record its request, including rejected operations."""
    request = _request.get()
    if request is None:
        return
    request.engine = engine
    request.record()


@contextmanager
def command_request(command: str, arguments: Mapping[str, Any]) -> Iterator[None]:
    """Record state-changing command requests; read-only commands make no audit writes."""
    if command not in MUTATING_COMMANDS or (command == "docs-version" and arguments.get("check")):
        yield
        return
    request = ActionRequest(command, mask_arguments(arguments), datetime.now(UTC))
    token = _request.set(request)
    try:
        yield
    finally:
        try:
            request.record()
        except SQLAlchemyError:
            logger.exception("could not record %s request in the Engine DB", command)
        finally:
            _request.reset(token)
