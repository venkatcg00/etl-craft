"""User sign-in, renewal and sign-out, whatever starts them: the CLI, the API or the UI."""

from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from typing import ClassVar

from sqlalchemy.engine import Row

from etl_craft.core.actor import Actor, ActorKind, acting_as
from etl_craft.core.enums import (
    AccountKind,
    ActionName,
    ActionOutcome,
    ApiRole,
    AuthMode,
    TokenKind,
    Transport,
    YesNo,
)
from etl_craft.core.enums_access import Permission
from etl_craft.core.errors import EtlCraftError, UsageError
from etl_craft.core.text import sha256_hex
from etl_craft.core.time import as_utc
from etl_craft.engine import locks
from etl_craft.engine.audit import current_request
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.accounts import find_account, unlocked_admins
from etl_craft.engine.repository.group_claims import token_groups
from etl_craft.services import oidc
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.permissions import project_permission
from etl_craft.services.operations.personal import Settings, settings_from
from etl_craft.services.operations.recording import recorded
from etl_craft.services.operations.requests import NoInput, Request
from etl_craft.services.operations.schema_status import require_current_schema
from etl_craft.services.operations.tokens import TokenGrant, TokenView, find_token, issue_token
from etl_craft.services.passwords import DECOY, verify_password

ACCESS_LIFETIME = timedelta(hours=1)
"""How long a server session's access token lasts before its refresh token renews it."""


@dataclass(frozen=True, kw_only=True)
class SignIn(Request):
    """An account name and its password, sent once."""

    name: str
    password: str


@dataclass(frozen=True, kw_only=True)
class SsoPollInput(Request):
    """The device code and nonce returned by the SSO start."""

    device_code: str
    nonce: str


@dataclass(frozen=True, kw_only=True)
class TokenInput(Request):
    """A refresh token to renew access with, or a session token to revoke."""

    token: str


@dataclass(frozen=True)
class Session:
    """A signed-in user's access token, and the refresh token of a server session."""

    SCHEMA: ClassVar[str] = "etl-craft/session/1"
    access_token: str
    metadata: TokenView
    refresh_token: str | None = None
    refresh_expires_at: datetime | None = None
    must_change_password: bool = False


@dataclass(frozen=True)
class SignInMethods:
    """How a person can sign in: always with a password, and with SSO when it is configured."""

    SCHEMA: ClassVar[str] = "etl-craft/sign-in-methods/1"
    sso: bool
    local: bool = True


@dataclass(frozen=True)
class Identity:
    """The signed-in account and its current deployment role."""

    SCHEMA: ClassVar[str] = "etl-craft/identity/1"
    name: str
    role: str
    account_id: int | None = None
    account_kind: AccountKind | None = None
    local_password: bool = False
    must_change_password: bool = False
    groups: tuple[str, ...] = ()
    project_permission: Permission | None = None
    settings: Settings = field(default_factory=Settings)
    last_signin: datetime | None = None
    session_expires_at: datetime | None = None


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


def sign_in(ctx: OperationContext, request: SignIn) -> Session:
    """Sign a registered user in with a password."""
    return _sign_in(ctx, request.name, request.password)


def _sign_in(
    ctx: OperationContext,
    name: str,
    password: str | None = None,
    *,
    claims: oidc.IdentityClaims | None = None,
) -> Session:
    """Require proof of a registered user; SSO subjects come only from the verified device flow.

    A sign-in over HTTP is a server session: a one-hour access token renewed by a refresh token
    that lasts ``Auth.Refresh_days``. A local sign-in is one access token that lasts
    ``Auth.Session_hours``.
    """
    require_current_schema(ctx)
    refreshable = ctx.transport == Transport.HTTP and not ctx.browser_session
    with ctx.engine.connect() as conn:
        row = find_account(conn, name)
    proven = _proven(row, password, None if claims is None else claims.subject)
    with locks.ACCOUNTS.hold(ctx.engine, wait_seconds=30):
        with ctx.engine.connect() as conn:
            current = find_account(conn, name)
        unchanged = (
            row is not None
            and current is not None
            and all(
                getattr(row, field) == getattr(current, field)
                for field in (
                    "account_id",
                    "kind",
                    "role",
                    "password_hash",
                    "oidc_subject",
                    "active_flag",
                )
            )
        )
        if not proven or not unchanged:
            if unchanged and claims is None:
                _failed_password(ctx, current)
            _refuse(ctx, name, "sign-in refused; account name or password is incorrect.")
        assert current is not None
        if claims is None and current.locked_date is not None:
            _refuse(ctx, name, "Account locked, contact your administrator")
        caller = replace(ctx, actor=Actor(name, ActorKind.USER))
        matched, unmatched = _sign_in_groups(ctx, claims)
        with recorded(
            caller,
            ActionName.LOGIN,
            {"name": name, "unmatched_groups": unmatched},
            on_completion=True,
        ):
            return _issue_session(caller, current, claims, refreshable, matched)


def _refuse(ctx: OperationContext, name: str, message: str) -> None:
    """Record an authentication refusal without returning account existence."""
    with recorded(ctx, ActionName.LOGIN, {"name": name}, on_completion=True) as audit:
        error = UsageError(message)
        audit.outcome, audit.exit_code = ActionOutcome.REFUSED, error.exit_code
        raise error


def _failed_password(ctx: OperationContext, row: Row[tuple[object, ...]] | None) -> None:
    """Count local USER failures under the same lock as successful session writes."""
    if row is None or row.kind != AccountKind.USER or row.oidc_subject or row.locked_date:
        return
    if row.active_flag != YesNo.YES:
        return
    limit = ctx.config.auth.lockout_after
    count = row.failed_signins + 1
    locked = datetime.now(UTC) if limit and count >= limit else None
    with ctx.engine.begin() as conn:
        if locked is not None and row.role == ApiRole.ADMIN and unlocked_admins(conn) <= 1:
            # Recovery must retain one interactive administrator.
            locked = None
        conn.execute(
            statement(conn, "account_failed_signin"),
            {"account_id": row.account_id, "count": count, "locked": locked},
        )


def _sign_in_groups(
    ctx: OperationContext,
    claims: oidc.IdentityClaims | None,
) -> tuple[tuple[int, ...], tuple[str, ...]]:
    with ctx.engine.connect() as conn:
        active = {
            r.name: r.group_id
            for r in conn.execute(statement(conn, "group_list"))
            if r.active_flag == YesNo.YES
        }
    named = () if claims is None else claims.groups
    return tuple(sorted({active[n] for n in named if n in active})), tuple(
        sorted(set(named) - active.keys())
    )


def _issue_session(
    ctx: OperationContext,
    row: Row[tuple[object, ...]],
    claims: oidc.IdentityClaims | None,
    refreshable: bool,
    matched: tuple[int, ...],
) -> Session:
    """Commit successful proof, retained groups and session credentials together."""
    now = datetime.now(UTC)
    with ctx.engine.begin() as conn:
        conn.execute(statement(conn, "account_signin_success"), {"account_id": row.account_id})
        session_end = now + timedelta(hours=ctx.config.auth.session_hours)
        if not refreshable:
            created = issue_token(
                conn,
                row.name,
                TokenGrant(
                    row.account_id, row.name, TokenKind.ACCESS, session_end, sso_group_ids=matched
                ),
            )
            session = Session(
                created.token,
                created.metadata,
                must_change_password=row.must_change_password == YesNo.YES,
            )
        else:
            refresh_end = (
                session_end
                if claims is not None or ctx.browser_session
                else now + timedelta(days=ctx.config.auth.refresh_days)
            )
            refresh = issue_token(
                conn,
                row.name,
                TokenGrant(
                    row.account_id, row.name, TokenKind.REFRESH, refresh_end, sso_group_ids=matched
                ),
            )
            access = issue_token(
                conn,
                row.name,
                TokenGrant(
                    row.account_id,
                    row.name,
                    TokenKind.ACCESS,
                    min(now + ACCESS_LIFETIME, refresh_end),
                    refresh.metadata.token_id,
                    matched,
                ),
            )
            session = Session(
                access.token,
                access.metadata,
                refresh.token,
                refresh.metadata.expires_at,
                row.must_change_password == YesNo.YES,
            )
        audit = current_request()
        assert audit is not None
        audit.outcome = ActionOutcome.SUCCESS
        audit.write(conn)
    return session


def _proven(row: Row[tuple[object, ...]] | None, password: str | None, subject: str | None) -> bool:
    """Check the proof; a password costs one scrypt whether or not the account can use one."""
    user = row is not None and row.kind == AccountKind.USER and row.active_flag == YesNo.YES
    if subject is not None:
        return user and row is not None and subject == row.oidc_subject
    encoded = row.password_hash if user and row is not None and row.password_hash else DECOY
    matched = password is not None and verify_password(password, encoded)
    return user and matched and encoded != DECOY


def sign_in_subject(ctx: OperationContext, claims: oidc.IdentityClaims) -> Session:
    """Map a verified issuer subject to exactly one pre-registered account."""
    require_current_schema(ctx)
    with ctx.engine.connect() as conn:
        name = conn.execute(
            statement(conn, "account_subject"), {"subject": claims.subject}
        ).scalar_one_or_none()
    if name is None:
        with recorded(ctx, ActionName.LOGIN, {"oidc_subject": claims.subject}):
            raise UsageError(
                "SSO subject has no active registered USER account; ask an administrator"
            )
    return _sign_in(ctx, name, claims=claims)


def start_sso(ctx: OperationContext, request: NoInput) -> SsoStart:
    """Begin the issuer's device authorization; the caller shows the code and polls."""
    require_current_schema(ctx)
    try:
        grant = oidc.start_device(ctx.config.auth)
    except EtlCraftError:
        with recorded(ctx, ActionName.LOGIN, {"method": AuthMode.SSO}):
            raise
    return SsoStart(
        grant.user_code,
        grant.verification_uri,
        grant.device_code,
        grant.nonce,
        grant.interval,
        grant.expires_in,
    )


def poll_sso(ctx: OperationContext, request: SsoPollInput) -> SsoPoll:
    """Poll the issuer once; sign in the account its verified subject belongs to."""
    require_current_schema(ctx)
    try:
        found = oidc.poll_device(ctx.config.auth, request.device_code, request.nonce)
    except EtlCraftError:
        with recorded(ctx, ActionName.LOGIN, {"method": AuthMode.SSO}):
            raise
    if isinstance(found, int):
        return SsoPoll(None, found)
    return SsoPoll(sign_in_subject(ctx, found), 0)


def refresh(ctx: OperationContext, request: TokenInput) -> Session:
    """Issue a new access token from a live refresh token of an active user."""
    require_current_schema(ctx)
    found = find_token(ctx, request.token)
    if found is None or found.token_kind != TokenKind.REFRESH or found.expires_at is None:
        raise UsageError(
            "refresh token is unknown, expired or revoked; run `etl-craft login` again"
        )
    with ctx.engine.connect() as conn:
        retained_groups = token_groups(conn, found.token_id)
    if retained_groups:
        absolute_end = found.created_at + timedelta(hours=ctx.config.auth.session_hours)
        found = replace(found, expires_at=min(found.expires_at, absolute_end))
        assert found.expires_at is not None
        if found.expires_at <= datetime.now(UTC):
            raise UsageError(
                "SSO refresh token exceeded Auth.Session_hours from sign-in; "
                "run `etl-craft login` again to refresh group membership"
            )
    assert found.expires_at is not None
    caller = replace(ctx, actor=found.actor)
    with acting_as(caller.actor), ctx.engine.begin() as conn:
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
                min(datetime.now(UTC) + ACCESS_LIFETIME, found.expires_at),
                found.token_id,
                retained_groups,
            ),
        )
    return Session(access.token, access.metadata, refresh_expires_at=found.expires_at)


def sign_out(ctx: OperationContext, request: TokenInput) -> None:
    """Revoke a session token, and a refresh token's access tokens, as the token's own account.

    An expired or unknown token is revoked under the caller's actor, so signing out always
    succeeds.
    """
    token = request.token
    found = find_token(ctx, token)
    caller = ctx if found is None else replace(ctx, actor=found.actor)
    with recorded(caller, ActionName.LOGOUT, {}), ctx.engine.begin() as conn:
        conn.execute(
            statement(conn, "session_revoke"),
            {
                "digest": sha256_hex(token.encode()),
                "now": datetime.now(UTC),
            },
        )


def sign_in_methods(ctx: OperationContext, request: NoInput) -> SignInMethods:
    """Say whether SSO is configured, so a sign-in can offer it."""
    return SignInMethods(sso=bool(ctx.config.auth.oidc_issuer))


def whoami(ctx: OperationContext, request: NoInput) -> Identity:
    """Return the caller's account and its current role."""
    with ctx.engine.connect() as conn:
        row = find_account(conn, ctx.actor.name)
        if row is None:
            return Identity(ctx.actor.name, str(ctx.role))
        groups = tuple(
            group.name
            for group in conn.execute(
                statement(conn, "permission_groups"),
                {"account": ctx.actor.name},
            )
            if group.explicit_member == YesNo.YES or group.group_id in ctx.sso_group_ids
        )
        project = conn.execute(
            statement(conn, "project_by_code"), {"code": ctx.project or ctx.config.project}
        ).one_or_none()
        level = (
            None
            if project is None
            else project_permission(
                conn,
                replace(ctx, project=project.project_code, project_id=project.project_id),
            )
        )
        signin = conn.execute(
            statement(conn, "account_last_signin"), {"name": ctx.actor.name}
        ).scalar_one_or_none()
        expiry = (
            None
            if ctx.credential_token_id is None
            else conn.execute(
                statement(conn, "account_session_expiry"),
                {"token_id": ctx.credential_token_id},
            ).scalar_one_or_none()
        )
    return Identity(
        ctx.actor.name,
        str(ctx.role),
        row.account_id,
        AccountKind(row.kind),
        row.kind == AccountKind.USER and row.oidc_subject is None,
        row.must_change_password == YesNo.YES,
        groups,
        level,
        settings_from(row.ui_settings),
        None if signin is None else as_utc(signin),
        None if expiry is None else as_utc(expiry),
    )
