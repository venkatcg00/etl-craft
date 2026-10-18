"""The caller's Engine DB, configuration and audit identity."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy.engine import Engine

from etl_craft.config import ConnectorConfig
from etl_craft.core.actor import Actor, acting_as
from etl_craft.core.enums import ApiRole
from etl_craft.engine.audit import command_request, register_engine
from etl_craft.execution.attempt_types import ChildOptions
from etl_craft.services.operations.permissions import authorize


@dataclass(frozen=True)
class OperationContext:
    """Resources owned by the caller; operations never dispose the Engine DB.

    actor scopes requests and dispatched work; automatic transitions retain their system actor.
    """

    engine: Engine
    config: ConnectorConfig
    actor: Actor
    child: ChildOptions = field(default_factory=ChildOptions)
    role: ApiRole | None = None


@contextmanager
def operation(
    ctx: OperationContext,
    command: str,
    arguments: Mapping[str, Any],
    *,
    provisioning: bool = False,
) -> Iterator[None]:
    """Record one request before executing it, including domain refusals.

    ``provisioning`` is local Engine DB setup before any admin exists; it skips only the role
    check, which has no account to check yet.
    """
    if not provisioning:
        authorize(ctx, command, arguments)
    with acting_as(ctx.actor), command_request(command, arguments):
        register_engine(ctx.engine)
        yield
