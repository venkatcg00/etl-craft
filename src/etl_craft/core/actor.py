"""The identity responsible for an Engine DB action."""

from __future__ import annotations

import unicodedata
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass

from etl_craft.core.enums import ActorKind as ActorKind
from etl_craft.core.errors import ConfigurationError


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
sso_groups: ContextVar[tuple[int, ...]] = ContextVar("etl_craft_sso_groups", default=())
purpose: ContextVar[str] = ContextVar("etl_craft_purpose", default="")
migration: ContextVar[str] = ContextVar("etl_craft_migration", default="")


def current_actor() -> Actor:
    """Return the explicitly scoped identity; refuse unattributed work."""
    actor = scoped_actor()
    if actor is None:
        raise ConfigurationError(
            "actor=None; expected an explicitly scoped account; "
            "wrap the operation in core.actor.acting_as with its requesting or system account"
        )
    return actor


def scoped_actor() -> Actor | None:
    """Return the connection marker, allowing unauthenticated reads without a writer."""
    return _actor.get()


@contextmanager
def acting_as(actor: Actor) -> Iterator[None]:
    """Scope an identity without leaking it to the next command or transaction."""
    token = _actor.set(actor)
    try:
        yield
    finally:
        _actor.reset(token)
