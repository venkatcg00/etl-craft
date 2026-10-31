"""Registered accounts, administrator controls and local writer-credential recovery."""

from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import ClassVar

from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

from etl_craft.core.actor import Actor, ActorKind
from etl_craft.core.enums import AccountKind, ApiRole, YesNo
from etl_craft.core.errors import UsageError
from etl_craft.core.time import as_utc
from etl_craft.dialects.engine import for_engine
from etl_craft.engine import locks
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.accounts import find_account, unlocked_admins
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.requests import NoInput, Request
from etl_craft.services.passwords import hash_password


@dataclass(frozen=True)
class Account:
    """Public account metadata, without password hashes."""

    account_id: int
    name: str
    kind: AccountKind
    role: ApiRole
    email: str | None
    oidc_subject: str | None
    active_flag: YesNo
    failed_signins: int = 0
    locked_at: datetime | None = None
    last_signin_at: datetime | None = None


@dataclass(frozen=True)
class Accounts:
    """Safe account administration results."""

    SCHEMA: ClassVar[str] = "etl-craft/accounts/1"
    accounts: tuple[Account, ...]


@dataclass(frozen=True, kw_only=True)
class AccountAdd(Request):
    """A new user or service account; a user signs in with a password or an SSO subject.

    ``recovery`` is local setup's first or recovery admin, refused once an active admin exists.
    """

    name: str
    role: ApiRole
    kind: AccountKind = AccountKind.USER
    email: str | None = None
    oidc_subject: str | None = None
    password: str | None = None
    recovery: bool = False

    def __post_init__(self) -> None:
        """Check the name and how the account proves itself."""
        if self.role == ApiRole.AGENT:
            raise UsageError(
                "agent is a token role only; use `etl-craft token create --role agent`"
            )
        Actor(self.name, ActorKind.USER)
        if self.kind == AccountKind.SERVICE and (self.password is not None or self.oidc_subject):
            raise UsageError(
                f"account {self.name!r}: SERVICE expects a token, not `password` or `oidc_subject`"
            )
        if self.kind == AccountKind.USER and not (self.password or self.oidc_subject):
            raise UsageError(f"account {self.name!r}: USER requires `password` or `oidc_subject`")
        if self.oidc_subject is not None and not self.oidc_subject.strip():
            raise UsageError(
                f"account {self.name!r}: `oidc_subject` is empty; supply the provider subject"
            )
        if self.recovery and (self.role != ApiRole.ADMIN or self.kind != AccountKind.USER):
            raise UsageError("local recovery requires a USER account with the admin role")


@dataclass(frozen=True, kw_only=True)
class AccountName(Request):
    """One account, by name."""

    name: str


@dataclass(frozen=True, kw_only=True)
class AccountRole(AccountName):
    """An account's new role, inherited by every credential it holds."""

    role: ApiRole

    def __post_init__(self) -> None:
        """Keep the agent capability on tokens, never on an account."""
        if self.role == ApiRole.AGENT:
            raise UsageError(
                "agent is a token role only; use `etl-craft token create --role agent`"
            )


@dataclass(frozen=True, kw_only=True)
class AccountPassword(AccountName):
    """A user's new password."""

    password: str

    def __post_init__(self) -> None:
        """Refuse an empty password."""
        if not self.password:
            raise UsageError("`password` must not be empty")


PROVISIONING_COMMANDS = frozenset({"setup", "init-db", "migrate", "doctor"})
"""Commands a local installation may run before any admin exists, to create or repair one."""


def local_provisioning(engine: Engine, command: str) -> bool:
    """Whether ``command`` runs as local provisioning: before the account schema or an admin.

    ``doctor`` always may; ``setup`` may while no active admin exists; ``init-db`` and ``migrate``
    may while no account exists at all.
    """
    if command not in PROVISIONING_COMMANDS:
        return False
    if command == "doctor" or not for_engine(engine).existing_tables(engine, ("cfg_accounts",)):
        return True
    if command == "setup":
        return not has_admin(engine)
    with engine.connect() as conn:
        return not conn.execute(statement(conn, "account_count")).scalar_one()


def has_admin(engine: Engine) -> bool:
    """Check whether interactive provisioning requires a signed-in administrator."""
    with engine.connect() as conn:
        return bool(unlocked_admins(conn))


def list_accounts(ctx: OperationContext, request: NoInput) -> Accounts:
    """List accounts with their sign-in state, including disabled identities kept for audit."""
    with ctx.engine.connect() as conn:
        state = {
            row["account_id"]: row
            for row in conn.execute(statement(conn, "account_signin_state")).mappings()
        }
    return Accounts(
        tuple(
            replace(
                account,
                failed_signins=int(state[account.account_id]["failed_signins"]),
                locked_at=_instant(state[account.account_id]["locked_at"]),
                last_signin_at=_instant(state[account.account_id]["last_signin_at"]),
            )
            for account in _accounts(ctx).accounts
        )
    )


def _accounts(ctx: OperationContext) -> Accounts:
    """List the identities alone; the result of every account change."""
    with ctx.engine.connect() as conn:
        return Accounts(
            tuple(Account(**row._mapping) for row in conn.execute(statement(conn, "account_list")))
        )


def _instant(value: object) -> datetime | None:
    if value is None:
        return None
    return as_utc(value if isinstance(value, datetime) else datetime.fromisoformat(str(value)))


def add_account(ctx: OperationContext, request: AccountAdd) -> Accounts:
    """Add a user or service account; local recovery is serialized with credential changes."""
    password_hash = hash_password(request.password) if request.password is not None else None
    with locks.ACCOUNTS.hold(ctx.engine, wait_seconds=30):
        if request.recovery and has_admin(ctx.engine):
            raise UsageError("an active admin already exists; sign in before managing accounts")
        try:
            with ctx.engine.begin() as conn:
                conn.execute(
                    statement(conn, "account_add"),
                    {
                        "name": request.name,
                        "kind": request.kind,
                        "role": request.role,
                        "email": request.email,
                        "oidc_subject": request.oidc_subject,
                        "password_hash": password_hash,
                        "actor": ctx.actor.name,
                    },
                )
        except IntegrityError as error:
            raise UsageError(
                f"account {request.name!r}: name or OIDC subject already exists; "
                "choose a distinct identity (accounts are never deleted)"
            ) from error
    return _accounts(ctx)


def set_role(ctx: OperationContext, request: AccountRole) -> Accounts:
    """Change the role inherited by every credential of this account."""
    if request.role == ApiRole.AGENT:
        raise UsageError("agent is a token role only; use `etl-craft token create --role agent`")
    return _update(ctx, request.name, "account_role", {"role": request.role})


def disable(ctx: OperationContext, request: AccountName) -> Accounts:
    """Disable the account and revoke all its credentials in the same transaction."""
    return _update(ctx, request.name, "account_disable", {}, revoke=True)


def unlock(ctx: OperationContext, request: AccountName) -> Accounts:
    """Unlock a local user without changing its password or authority."""
    return _update(ctx, request.name, "account_unlock", {})


def reset_password(ctx: OperationContext, request: AccountPassword) -> Accounts:
    """Replace a user's password and revoke existing sessions."""
    values: dict[str, object] = {"password_hash": hash_password(request.password)}
    return _update(ctx, request.name, "account_password", values, revoke=True)


def _update(
    ctx: OperationContext, name: str, query: str, values: dict[str, object], *, revoke: bool = False
) -> Accounts:
    with locks.ACCOUNTS.hold(ctx.engine, wait_seconds=30), ctx.engine.begin() as conn:
        if query in ("account_disable", "account_role") and values.get("role") != ApiRole.ADMIN:
            target = find_account(conn, name)
            if (
                target is not None
                and target.role == ApiRole.ADMIN
                and target.active_flag == YesNo.YES
                and target.locked_date is None
                and unlocked_admins(conn) <= 1
            ):
                raise UsageError(
                    f"account {name!r}: last unlocked active admin; expected another recovery "
                    "administrator; add one before disabling or changing this account's role"
                )
        account_id = conn.execute(
            statement(conn, query), {"name": name, **values}
        ).scalar_one_or_none()
        if account_id is None:
            raise UsageError(
                f"account {name!r}: not found or wrong kind; use `etl-craft account list`"
            )
        if revoke:
            conn.execute(
                statement(conn, "account_revoke"),
                {
                    "account_id": account_id,
                    "now": datetime.now(UTC),
                },
            )
    return _accounts(ctx)
