"""Create, revoke and authenticate opaque API tokens without retaining their values."""

from __future__ import annotations

import re
import secrets
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import ClassVar

from sqlalchemy.engine import RowMapping

from etl_craft.core.actor import Actor, ActorKind
from etl_craft.core.errors import UsageError
from etl_craft.core.text import sha256_hex
from etl_craft.engine.queries import statement
from etl_craft.services.operations.context import OperationContext, operation
from etl_craft.services.operations.snapshots import timestamp

ROLES = ("viewer", "operator", "admin")


@dataclass(frozen=True)
class TokenView:
    """A token's identity and lifetime, without its value or hash."""

    SCHEMA: ClassVar[str] = "etl-craft/token/1"
    token_id: int
    name: str
    role: str
    project_id: int | None
    created_by: str
    created_at: datetime
    expires_at: datetime | None
    revoked_at: datetime | None

    @property
    def actor(self) -> Actor:
        """Attribute requests to the token's validated name."""
        return Actor(self.name, ActorKind.HUMAN)


@dataclass(frozen=True)
class TokensView:
    """Token metadata visible to an administrator."""

    SCHEMA: ClassVar[str] = "etl-craft/tokens/1"
    tokens: tuple[TokenView, ...]


@dataclass(frozen=True)
class CreatedToken:
    """The value returned once, alongside safe token metadata."""

    SCHEMA: ClassVar[str] = "etl-craft/created-token/1"
    token: str
    metadata: TokenView


def _view(row: RowMapping) -> TokenView:
    values = dict(row)
    for key in ("created_at", "expires_at", "revoked_at"):
        values[key] = timestamp(values[key])
    return TokenView(**values)


def create_token(
    ctx: OperationContext, name: str, role: str, expires: str | None = None
) -> CreatedToken:
    """Print-once credentials contain 256 random bits; only their SHA-256 is stored."""
    Actor(name, ActorKind.HUMAN)
    if role not in ROLES:
        raise UsageError(f"token role={role!r}; expected {', '.join(ROLES)}")
    now = datetime.now(UTC)
    expires_at = None
    if expires is not None:
        if re.fullmatch(r"[0-9]{1,5}d", expires) is None:
            raise UsageError("token --expires must be a positive number of days, such as 90d")
        days = int(expires[:-1])
        if not 1 <= days <= 36500:
            raise UsageError("token --expires must be between 1d and 36500d")
        expires_at = now + timedelta(days=days)
    token = secrets.token_urlsafe(32)
    with (
        operation(ctx, "token create", {"name": name, "role": role, "expires": expires}),
        ctx.engine.begin() as conn,
    ):
        token_id = conn.execute(
            statement(conn, "api_token_create"),
            {
                "name": name,
                "role": role,
                "actor": ctx.actor.name,
                "digest": sha256_hex(token.encode()),
                "now": now,
                "expires_at": expires_at,
            },
        ).scalar_one()
    return CreatedToken(
        token, TokenView(token_id, name, role, None, ctx.actor.name, now, expires_at, None)
    )


def list_tokens(ctx: OperationContext) -> TokensView:
    """Return safe metadata; credential hashes never enter operation JSON."""
    with ctx.engine.connect() as conn:
        return TokensView(
            tuple(_view(row._mapping) for row in conn.execute(statement(conn, "api_token_list")))
        )


def revoke_token(ctx: OperationContext, token_id: int) -> TokensView:
    """Revoke a token immediately; each request authenticates against current storage."""
    with operation(ctx, "token revoke", {"token_id": token_id}), ctx.engine.begin() as conn:
        found = conn.execute(
            statement(conn, "api_token_revoke"),
            {"token_id": token_id, "now": datetime.now(UTC)},
        ).scalar_one_or_none()
        if found is None:
            raise UsageError(f"token_id={token_id}: token is missing or already revoked")
    return list_tokens(ctx)


def authenticate(ctx: OperationContext, token: str) -> TokenView | None:
    """Refuse unknown, expired, revoked and unsupported project-scoped tokens."""
    if not 1 <= len(token) <= 256:
        return None
    with ctx.engine.connect() as conn:
        row = conn.execute(
            statement(conn, "api_token_find"), {"digest": sha256_hex(token.encode())}
        ).one_or_none()
    if row is None:
        return None
    view = _view(row._mapping)
    if (
        view.revoked_at is not None
        or view.project_id is not None
        or (view.expires_at is not None and view.expires_at <= datetime.now(UTC))
    ):
        return None
    return view
