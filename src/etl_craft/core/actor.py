"""The identity responsible for an Engine DB action."""

from __future__ import annotations

import getpass
import os
import socket
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
                "ETL_CRAFT_ACTOR must contain a non-empty name of at most 128 characters "
                "without control characters; set it to the person, token or worker responsible"
            )


SYSTEM_ACTOR = Actor("etl-craft", ActorKind.SYSTEM)
_actor: ContextVar[Actor | None] = ContextVar("etl_craft_actor", default=None)
purpose: ContextVar[str] = ContextVar("etl_craft_purpose", default="")
migration: ContextVar[str] = ContextVar("etl_craft_migration", default="")


def resolve_actor() -> Actor:
    """Resolve the command's identity from its environment or the local user and host."""
    name = os.environ.get("ETL_CRAFT_ACTOR")
    if name is None:
        try:
            name = f"{getpass.getuser()}@{socket.gethostname()}"
        except (KeyError, OSError) as error:
            raise ConfigurationError(
                "cannot resolve ETL_CRAFT_ACTOR from the local user and host; "
                "set ETL_CRAFT_ACTOR to the person, token or worker responsible"
            ) from error
    written_kind = os.environ.get("ETL_CRAFT_ACTOR_KIND", ActorKind.HUMAN)
    try:
        kind = ActorKind(written_kind)
    except ValueError:
        raise ConfigurationError(
            f"ETL_CRAFT_ACTOR_KIND={written_kind!r}; expected one of "
            f"{', '.join(ActorKind)}; set the source of the actor"
        ) from None
    return Actor(name, kind)


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
