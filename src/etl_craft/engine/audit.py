"""Immutable command requests and metadata changes made through the Engine DB."""

from __future__ import annotations

import json
import logging
import socket
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from etl_craft.core.actor import Actor, current_actor
from etl_craft.core.enums import ActionOutcome
from etl_craft.core.text import is_secret_name
from etl_craft.dialects.engine import for_engine
from etl_craft.engine.queries import statement

logger = logging.getLogger(__name__)

MUTATING_COMMANDS = frozenset(
    {
        "login",
        "data edit",
        "data unlock",
        "logout",
        "account add",
        "account set-role",
        "account disable",
        "account reset-password",
        "token create",
        "token revoke",
        "run",
        "server",
        "mark",
        "cancel",
        "reconcile",
        "upgrade-targets",
        "rehash",
        "pause",
        "resume",
        "migrate",
        "config apply",
        "setup",
        "init-db",
        "clone",
        "generate-docs",
        "publish-docs",
        "docs-version",
        "lineage",
    }
)


def mask_arguments(arguments: Mapping[str, Any]) -> dict[str, Any]:
    """Mask values of sensitive names, including nested arguments, before serialization."""
    return {
        key: "[REDACTED]"
        if is_secret_name(key)
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
    actor: Actor = field(default_factory=current_actor)
    engine: Engine | None = None
    recorded: bool = False
    outcome: ActionOutcome = ActionOutcome.REQUESTED
    exit_code: int | None = None

    def record(self) -> None:
        """Record the request itself; flow outcomes belong to run and attempt logs."""
        if self.recorded or self.engine is None:
            return
        engine = self.engine
        if not for_engine(engine).existing_tables(engine, ("aud_actions",)):
            return
        with engine.begin() as conn:
            pipeline = conn.execute(
                statement(conn, "action_pipeline"), {"code": self.arguments.get("pipeline_code")}
            ).scalar_one_or_none()
            task = None
            if pipeline is not None and self.arguments.get("task_code"):
                task = conn.execute(
                    statement(conn, "action_task"),
                    {"pipeline": pipeline, "code": self.arguments["task_code"]},
                ).scalar_one_or_none()
            actor = self.actor
            payload = json.dumps(self.arguments, default=str)
            conn.execute(
                statement(conn, "action_insert"),
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
                    "outcome": self.outcome,
                    "exit_code": self.exit_code,
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
    checking = (command == "docs-version" and arguments.get("check")) or (
        command == "rehash" and arguments.get("dry_run")
    )
    if command not in MUTATING_COMMANDS or checking:
        yield
        return
    existing = _request.get()
    if existing is not None and existing.command == command and existing.actor == current_actor():
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
