"""User sign-in, renewal and sign-out, whatever starts them: the CLI, the API or the UI."""

from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import ClassVar

from sqlalchemy.engine import Row

from etl_craft.core.actor import Actor, ActorKind
from etl_craft.core.enums import AccountKind, TokenKind, YesNo
from etl_craft.core.errors import EtlCraftError, UsageError
from etl_craft.core.text import sha256_hex
from etl_craft.engine import locks
from etl_craft.engine.queries import statement
from etl_craft.services import oidc
from etl_craft.services.operations.context import OperationContext, operation
from etl_craft.services.operations.permissions import authorize
from etl_craft.services.operations.tokens import TokenGrant, TokenView, find_token, issue_token
from etl_craft.services.passwords import DECOY, verify_password

ACCESS_LIFETIME = timedelta(hours=1)
"""How long a server session's access token lasts before its refresh token renews it."""


@dataclass(frozen=True)
class Session:
    """A signed-in user's access token, and the refresh token of a server session."""

    SCHEMA: ClassVar[str] = "etl-craft/session/1"
    access_token: str
    metadata: TokenView
    refresh_token: str | None = None
    refresh_expires_at: datetime | None = None


@dataclass(frozen=True)
class SignInMethods:
    """How a person can sign in: always with a password, and with SSO when it is configured."""

    SCHEMA: ClassVar[str] = "etl-craft/sign-in-methods/1"
    sso: bool


@dataclass(frozen=True)
class Identity:
    """The signed-in account and its current deployment role."""

    SCHEMA: ClassVar[str] = "etl-craft/identity/1"
    name: str
    role: str


@dataclass(frozen=True)
class SsoStart:
    """The code the person approves at the issuer, and what each poll sends back."""

    SCHEMA: ClassVar[str] = "etl-craft/sso-start/1"
    user_code: str
    verification_uri: str
    device_code: str
    nonce: str
    interval: int
    expires_in: int


@dataclass(frozen=True)
class SsoPoll:
    """The session once the person approved, or how long to wait before polling again."""

    SCHEMA: ClassVar[str] = "etl-craft/sso-poll/1"
    session: Session | None
    interval: int


def sign_in(
    ctx: OperationContext,
    name: str,
    password: str | None = None,
    *,
    subject: str | None = None,
    refreshable: bool = False,
) -> Session:
    """Require proof of a registered user; SSO subjects come only from the verified device flow.

    A refreshable session is a server sign-in: a one-hour access token renewed by a refresh token
    that lasts ``Auth.Refresh_days``. Otherwise the session is one access token that lasts
    ``Auth.Session_hours``.
    """
    with locks.ACCOUNTS.hold(ctx.engine, wait_seconds=30):
        with ctx.engine.connect() as conn:
            row = conn.execute(statement(conn, "account_find"), {"name": name}).one_or_none()
        proven = _proven(row, password, subject)
        if row is None or not proven:
            with operation(ctx, "login", {"name": name}):
                raise UsageError(
                    f"account {name!r}: sign-in refused; expected an active registered USER "
                    "with matching credentials; ask an administrator to check the account"
                )
        caller = replace(ctx, actor=Actor(name, ActorKind.USER))
        now = datetime.now(UTC)
        with operation(caller, "login", {"name": name}), ctx.engine.begin() as conn:
            if not refreshable:
                session_end = now + timedelta(hours=ctx.config.auth.session_hours)
                created = issue_token(
                    conn, name, TokenGrant(row.account_id, name, TokenKind.ACCESS, session_end)
                )
                return Session(created.token, created.metadata)
            refresh_end = now + timedelta(days=ctx.config.auth.refresh_days)
            refresh = issue_token(
                conn, name, TokenGrant(row.account_id, name, TokenKind.REFRESH, refresh_end)
            )
            access = issue_token(
                conn,
                name,
                TokenGrant(
                    row.account_id,
                    name,
                    TokenKind.ACCESS,
                    now + ACCESS_LIFETIME,
                    refresh.metadata.token_id,
                ),
            )
            return Session(
                access.token, access.metadata, refresh.token, refresh.metadata.expires_at
            )


def _proven(row: Row[tuple[object, ...]] | None, password: str | None, subject: str | None) -> bool:
    """Check the proof; a password costs one scrypt whether or not the account can use one."""
    user = row is not None and row.kind == AccountKind.USER and row.active_flag == YesNo.YES
    if subject is not None:
        return user and row is not None and subject == row.oidc_subject
    encoded = row.password_hash if user and row is not None and row.password_hash else DECOY
    matched = password is not None and verify_password(password, encoded)
    return user and matched and encoded != DECOY


def sign_in_subject(ctx: OperationContext, subject: str, *, refreshable: bool = False) -> Session:
    """Map a verified issuer subject to exactly one pre-registered account."""
    with ctx.engine.connect() as conn:
        name = conn.execute(
            statement(conn, "account_subject"), {"subject": subject}
        ).scalar_one_or_none()
    if name is None:
        with operation(ctx, "login", {"oidc_subject": subject}):
            raise UsageError(
                "SSO subject has no active registered USER account; ask an administrator"
            )
    return sign_in(ctx, name, subject=subject, refreshable=refreshable)


def start_sso(ctx: OperationContext) -> SsoStart:
    """Begin the issuer's device authorization; the caller shows the code and polls."""
    try:
        grant = oidc.start_device(ctx.config.auth)
    except EtlCraftError:
        with operation(ctx, "login", {"method": "SSO"}):
            raise
    return SsoStart(
        grant.user_code,
        grant.verification_uri,
        grant.device_code,
        grant.nonce,
        grant.interval,
        grant.expires_in,
    )


def poll_sso(
    ctx: OperationContext, device_code: str, nonce: str, *, refreshable: bool = False
) -> SsoPoll:
    """Poll the issuer once; sign in the account its verified subject belongs to."""
    try:
        found = oidc.poll_device(ctx.config.auth, device_code, nonce)
    except EtlCraftError:
        with operation(ctx, "login", {"method": "SSO"}):
            raise
    if isinstance(found, int):
        return SsoPoll(None, found)
    return SsoPoll(sign_in_subject(ctx, found, refreshable=refreshable), 0)


def refresh(ctx: OperationContext, refresh_token: str) -> Session:
    """Issue a new access token from a live refresh token of an active user."""
    found = find_token(ctx, refresh_token)
    if found is None or found.token_kind != TokenKind.REFRESH:
        with operation(ctx, "refresh", {}):
            raise UsageError(
                "refresh token is unknown, expired or revoked; run `etl-craft login` again"
            )
    caller = replace(ctx, actor=found.actor)
    with operation(caller, "refresh", {}), ctx.engine.begin() as conn:
        account_id = (
            conn.execute(statement(conn, "api_token_account"), {"name": found.name})
            .one()
            .account_id
        )
        access = issue_token(
            conn,
            found.name,
            TokenGrant(
                account_id,
                found.name,
                TokenKind.ACCESS,
                datetime.now(UTC) + ACCESS_LIFETIME,
                found.token_id,
            ),
        )
    return Session(access.token, access.metadata, refresh_expires_at=found.expires_at)


def sign_out(ctx: OperationContext, token: str) -> None:
    """Revoke a session token, and a refresh token's access tokens, as the token's own account.

    An expired or unknown token is revoked under the caller's actor, so signing out always
    succeeds.
    """
    found = find_token(ctx, token)
    caller = ctx if found is None else replace(ctx, actor=found.actor)
    with operation(caller, "logout", {}), ctx.engine.begin() as conn:
        conn.execute(
            statement(conn, "session_revoke"),
            {
                "digest": sha256_hex(token.encode()),
                "now": datetime.now(UTC),
            },
        )


def sign_in_methods(ctx: OperationContext) -> SignInMethods:
    """Say whether SSO is configured, so a sign-in can offer it."""
    return SignInMethods(sso=bool(ctx.config.auth.oidc_issuer))


def whoami(ctx: OperationContext) -> Identity:
    """Return the caller's account and its current role."""
    authorize(ctx, "whoami")
    return Identity(ctx.actor.name, str(ctx.role))
