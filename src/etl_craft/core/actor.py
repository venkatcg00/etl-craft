"""The identity responsible for an Engine DB action."""

from __future__ import annotations

import unicodedata
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from enum import StrEnum

from etl_craft.core.errors import ConfigurationError


class ActorKind(StrEnum):
    """How an actor reached the engine."""

    HUMAN = "HUMAN"
    SCHEDULE = "SCHEDULE"
    ORCHESTRATOR = "ORCHESTRATOR"
    WORKER = "WORKER"
    SYSTEM = "SYSTEM"


@dataclass(frozen=True)
class Actor:
    """A validated name and its source."""

    name: str
    kind: ActorKind

    def __post_init__(self) -> None:
        """Refuse identities that cannot safely fit an audit record."""
        if (
            not self.name.strip()
            or len(self.name) > 128
            or any(unicodedata.category(c).startswith("C") for c in self.name)
        ):
            raise ConfigurationError(
                f"actor name={self.name!r}; expected a non-empty name of at most 128 characters "
                "without control characters; use a valid registered account name"
            )


SYSTEM_ACTOR = Actor("etl-craft", ActorKind.SYSTEM)
_actor: ContextVar[Actor | None] = ContextVar("etl_craft_actor", default=None)
purpose: ContextVar[str] = ContextVar("etl_craft_purpose", default="")
migration: ContextVar[str] = ContextVar("etl_craft_migration", default="")


def current_actor() -> Actor:
    """Return the scoped identity, or the engine's system identity for library calls."""
    return _actor.get() or SYSTEM_ACTOR


@contextmanager
def acting_as(actor: Actor) -> Iterator[None]:
    """Scope an identity without leaking it to the next command or transaction."""
    token = _actor.set(actor)
    try:
        yield
    finally:
        _actor.reset(token)
