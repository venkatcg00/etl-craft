"""Create, revoke and authenticate opaque API tokens without retaining their values."""

from __future__ import annotations

import json
import logging
import re
import secrets
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from threading import BoundedSemaphore, Thread
from typing import ClassVar

from sqlalchemy.engine import Connection, RowMapping
from sqlalchemy.exc import SQLAlchemyError

from etl_craft.core.actor import Actor, ActorKind, acting_as
from etl_craft.core.enums import AccountKind, ApiRole, TokenKind, YesNo
from etl_craft.core.errors import ConfigurationError, UsageError
from etl_craft.core.text import sha256_hex
from etl_craft.core.time import as_utc
from etl_craft.engine.connection import engine_db, read_snapshot
from etl_craft.engine.placement import require_pool
from etl_craft.engine.queries import statement
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.requests import NoInput, Request
from etl_craft.services.operations.snapshots import timestamp

logger = logging.getLogger(__name__)
_USAGE_SLOT = BoundedSemaphore(1)


@dataclass(frozen=True)
class TokenView:
    """A token's identity and lifetime, without its value or hash."""

    SCHEMA: ClassVar[str] = "etl-craft/token/1"
    token_id: int
    name: str
    account_kind: AccountKind
    token_kind: TokenKind
    role: str
    project_id: int | None
    created_by: str
    created_at: datetime
    expires_at: datetime | None
    revoked_at: datetime | None
    pool: str | None = None
    attempt_id: int | None = None
    last_used_at: datetime | None = None

    @property
    def actor(self) -> Actor:
        """Attribute requests to the token's validated name."""
        return Actor(self.name, ActorKind(self.account_kind))


@dataclass(frozen=True)
class TokensView:
    """Token metadata visible to an administrator."""

    SCHEMA: ClassVar[str] = "etl-craft/tokens/1"
    tokens: tuple[TokenView, ...]


@dataclass(frozen=True)
class TokenGrant:
    """What to issue: whose credential, which kind, until when, and the refresh token behind it."""

    account_id: int
    name: str
    kind: TokenKind
    expires_at: datetime | None
    refresh_token_id: int | None = None
    sso_group_ids: tuple[int, ...] = ()
    pool: str | None = None
    attempt_id: int | None = None


@dataclass(frozen=True)
class CreatedToken:
    """The value returned once, alongside safe token metadata."""

    SCHEMA: ClassVar[str] = "etl-craft/created-token/1"
    token: str
    metadata: TokenView


def _view(row: RowMapping) -> TokenView:
    values = dict(row)
    values["account_kind"] = AccountKind(values["account_kind"])
    values["token_kind"] = TokenKind(values["token_kind"])
    if values["token_kind"] in (TokenKind.AGENT, TokenKind.ATTEMPT):
        values["role"] = ApiRole.AGENT
    for key in ("created_at", "expires_at", "revoked_at", "last_used_at"):
        values[key] = timestamp(values[key])
    return TokenView(**values)


@dataclass(frozen=True, kw_only=True)
class TokenCreate(Request):
    """A service account's name, its role and an optional lifetime in days, such as ``90d``."""

    name: str
    role: ApiRole
    expires: str = "365d"
    pool: str | None = None

    def __post_init__(self) -> None:
        """Check the name and the lifetime before any account is created."""
        Actor(self.name, ActorKind.SERVICE)
        if (self.role == ApiRole.AGENT) != (self.pool is not None):
            raise UsageError(
                f"--role={self.role}, --pool={self.pool!r}; --pool P is required only for "
                "agent tokens; supply it for agents and omit it for other roles"
            )
        if not isinstance(self.expires, str) or re.fullmatch(r"[0-9]{1,5}d", self.expires) is None:
            raise UsageError("`expires` must be a positive number of days, such as 365d")
        if not 1 <= int(self.expires[:-1]) <= 36500:
            raise UsageError("`expires` must be between 1d and 36500d")


@dataclass(frozen=True, kw_only=True)
class TokenRevoke(Request):
    """One token, by id."""

    token_id: int


def create_token(ctx: OperationContext, request: TokenCreate) -> CreatedToken:
    """Print-once credentials contain 256 random bits; only their SHA-256 is stored."""
    name = request.name
    role = ApiRole.VIEWER if request.role == ApiRole.AGENT else request.role
    now = datetime.now(UTC)
    expires_at = now + timedelta(days=int(request.expires[:-1]))
    with ctx.engine.begin() as conn:
        if request.pool is not None:
            require_pool(conn, request.pool, f"token {name!r}")
        conn.execute(
            statement(conn, "api_token_account_create"),
            {"name": name, "kind": AccountKind.SERVICE, "role": role},
        )
        account = conn.execute(statement(conn, "api_token_account"), {"name": name}).one()
        if (
            account.active_flag != YesNo.YES
            or account.role != role
            or account.kind != AccountKind.SERVICE
        ):
            raise UsageError(
                f"account {name!r}: role={account.role!r}, ACTIVE_FLAG={account.active_flag!r}; "
                f"expected an active account with role={role!r} and kind=SERVICE; "
                "use its current role or ask "
                "an administrator to use `etl-craft account set-role`"
            )
        return issue_token(
            conn,
            ctx.actor.name,
            TokenGrant(
                account.account_id,
                name,
                TokenKind.AGENT if request.role == ApiRole.AGENT else TokenKind.SERVICE,
                expires_at,
                pool=request.pool,
            ),
        )


def list_tokens(ctx: OperationContext, request: NoInput) -> TokensView:
    """Return safe metadata; credential hashes never enter operation JSON."""
    with ctx.engine.connect() as conn:
        return TokensView(
            tuple(_view(row._mapping) for row in conn.execute(statement(conn, "api_token_list")))
        )


def revoke_token(ctx: OperationContext, request: TokenRevoke) -> TokensView:
    """Revoke a token immediately; each request authenticates against current storage."""
    token_id = request.token_id
    with ctx.engine.begin() as conn:
        found = conn.execute(
            statement(conn, "api_token_revoke"),
            {"token_id": token_id, "now": datetime.now(UTC)},
        ).scalar_one_or_none()
        if found is None:
            raise UsageError(f"token_id={token_id}: token is missing or already revoked")
    return list_tokens(ctx, NoInput())


def authenticate(ctx: OperationContext, token: str) -> TokenView | None:
    """Refuse refresh tokens and unknown, expired, revoked or project-scoped tokens."""
    view = find_token(ctx, token)
    if view is None or view.token_kind == TokenKind.REFRESH:
        return None
    _record_use(ctx, view)
    return view


def find_token(ctx: OperationContext, token: str) -> TokenView | None:
    """Return a usable token of any kind; refresh tokens only renew access."""
    if not 1 <= len(token) <= 256:
        return None
    with read_snapshot(ctx.engine) as conn:
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
    if view.attempt_id is not None:
        with read_snapshot(ctx.engine) as conn:
            live = conn.execute(
                statement(conn, "attempt_credential_live"), {"attempt_id": view.attempt_id}
            ).scalar_one_or_none()
        if not live:
            return None
    return view


def _record_use(ctx: OperationContext, view: TokenView) -> None:
    if view.token_kind not in (TokenKind.SERVICE, TokenKind.AGENT):
        return
    now = datetime.now(UTC)
    if view.last_used_at is not None and now - as_utc(view.last_used_at) < timedelta(hours=1):
        return
    if _USAGE_SLOT.acquire(blocking=False):
        try:
            Thread(
                target=_write_use, args=(ctx, view, now), daemon=True, name="token-usage"
            ).start()
        except RuntimeError:
            _USAGE_SLOT.release()
            logger.warning("token usage writer is shutting down; request continues")


def _write_use(ctx: OperationContext, view: TokenView, now: datetime) -> None:
    # An independent connection cannot consume request slots; daemon work never delays CLI exit.
    writer = None
    try:
        with acting_as(view.actor):
            writer = engine_db(ctx.config, supervised=True)
        with acting_as(view.actor), writer.begin() as conn:
            conn.execute(
                statement(conn, "api_token_used"),
                {"token_id": view.token_id, "now": now, "before": now - timedelta(hours=1)},
            )
    except (SQLAlchemyError, ConfigurationError):
        # Usage telemetry cannot affect authorization: the credential was already validated.
        logger.warning("token_id=%s: LAST_USED_AT update failed; request continues", view.token_id)
    finally:
        if writer is not None:
            writer.dispose()
        _USAGE_SLOT.release()


def issue_token(conn: Connection, actor: str, grant: TokenGrant) -> CreatedToken:
    """Issue a credential inside its caller's account-validation transaction."""
    token = secrets.token_urlsafe(32)
    digest = sha256_hex(token.encode())
    conn.execute(
        statement(conn, "api_token_create"),
        {
            "name": grant.name,
            "account_id": grant.account_id,
            "actor": actor,
            "digest": digest,
            "now": datetime.now(UTC),
            "expires_at": grant.expires_at,
            "kind": grant.kind,
            "refresh_token_id": grant.refresh_token_id,
            "sso_group_ids": json.dumps(grant.sso_group_ids),
            "pool": grant.pool,
            "attempt_id": grant.attempt_id,
        },
    )
    metadata = _view(
        conn.execute(statement(conn, "api_token_find"), {"digest": digest}).one()._mapping
    )
    return CreatedToken(token, metadata)
