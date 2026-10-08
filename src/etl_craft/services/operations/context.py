"""The caller's Engine DB, configuration and audit identity."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy.engine import Engine

from etl_craft.config import ConnectorConfig
from etl_craft.core.actor import Actor, acting_as
from etl_craft.engine.audit import command_request, register_engine
from etl_craft.execution.runner import ChildOptions


@dataclass(frozen=True)
class OperationContext:
    """Resources owned by the caller; operations never dispose the Engine DB.

    actor scopes requests and dispatched work; automatic transitions retain their system actor.
    """

    engine: Engine
    config: ConnectorConfig
    actor: Actor
    child: ChildOptions = field(default_factory=ChildOptions)


@contextmanager
def operation(ctx: OperationContext, command: str, arguments: Mapping[str, Any]) -> Iterator[None]:
    """Record one request before executing it, including domain refusals."""
    with acting_as(ctx.actor), command_request(command, arguments):
        register_engine(ctx.engine)
        yield
