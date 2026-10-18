"""Registered accounts, administrator controls and local writer-credential recovery."""

from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import ClassVar

from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

from etl_craft.core.actor import Actor, ActorKind
from etl_craft.core.enums import AccountKind, ApiRole, YesNo
from etl_craft.core.errors import UsageError
from etl_craft.dialects.engine import for_engine
from etl_craft.engine import locks
from etl_craft.engine.queries import statement
from etl_craft.services.operations.context import OperationContext, operation
from etl_craft.services.operations.permissions import authorize
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


@dataclass(frozen=True)
class Accounts:
    """Safe account administration results."""

    SCHEMA: ClassVar[str] = "etl-craft/accounts/1"
    accounts: tuple[Account, ...]


LOCAL_PROVISIONING = frozenset({"setup", "init-db", "migrate", "doctor"})
"""Commands a local installation may run before any admin exists, to create or repair one."""


def local_provisioning(engine: Engine, command: str) -> bool:
    """Whether ``command`` runs as local provisioning: before the account schema or an admin.

    ``doctor`` always may; ``setup`` may while no active admin exists; ``init-db`` and ``migrate``
    may while no account exists at all.
    """
    if command not in LOCAL_PROVISIONING:
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
        return bool(conn.execute(statement(conn, "account_admin")).scalar_one())


def list_accounts(ctx: OperationContext) -> Accounts:
    """List accounts, including disabled identities retained for audit history."""
    authorize(ctx, "account list")
    with ctx.engine.connect() as conn:
        return Accounts(
            tuple(Account(**row._mapping) for row in conn.execute(statement(conn, "account_list")))
        )


def add_account(
    ctx: OperationContext, account: Account, password: str | None = None, *, recovery: bool = False
) -> None:
    """Add a user or service account; local recovery is serialized with credential changes."""
    if not recovery:
        authorize(ctx, "account add")
    Actor(account.name, ActorKind.USER)
    if account.kind == AccountKind.SERVICE and (password is not None or account.oidc_subject):
        raise UsageError(f"account {account.name!r}: SERVICE expects a token, not password or SSO")
    if account.kind == AccountKind.USER and not (password or account.oidc_subject):
        raise UsageError(f"account {account.name!r}: USER requires a password or OIDC subject")
    if account.oidc_subject is not None and not account.oidc_subject.strip():
        raise UsageError(
            f"account {account.name!r}: OIDC subject is empty; supply the provider subject"
        )
    password_hash = hash_password(password) if password is not None else None
    with locks.ACCOUNTS.hold(ctx.engine, wait_seconds=30):
        if recovery:
            if has_admin(ctx.engine):
                raise UsageError("an active admin already exists; sign in before managing accounts")
            if account.role != ApiRole.ADMIN or account.kind != AccountKind.USER:
                raise UsageError("local recovery requires a USER account with the admin role")
        try:
            with (
                operation(
                    replace(ctx, role=ApiRole.ADMIN) if recovery else ctx,
                    "account add",
                    {"name": account.name, "recovery": recovery},
                ),
                ctx.engine.begin() as conn,
            ):
                conn.execute(
                    statement(conn, "account_add"),
                    {
                        "name": account.name,
                        "kind": account.kind,
                        "role": account.role,
                        "email": account.email,
                        "oidc_subject": account.oidc_subject,
                        "password_hash": password_hash,
                        "actor": ctx.actor.name,
                    },
                )
        except IntegrityError as error:
            raise UsageError(
                f"account {account.name!r}: name or OIDC subject already exists; "
                "choose a distinct identity (accounts are never deleted)"
            ) from error


def set_role(ctx: OperationContext, name: str, role: ApiRole) -> None:
    """Change the role inherited by every credential of this account."""
    _update(ctx, name, "account_role", {"role": role})


def disable(ctx: OperationContext, name: str) -> None:
    """Disable the account and revoke all its credentials in the same transaction."""
    _update(ctx, name, "account_disable", {}, revoke=True)


def reset_password(ctx: OperationContext, name: str, password: str) -> None:
    """Replace a user's password and revoke existing sessions."""
    _update(ctx, name, "account_password", {"password_hash": hash_password(password)}, revoke=True)


def _update(
    ctx: OperationContext, name: str, query: str, values: dict[str, object], *, revoke: bool = False
) -> None:
    command = {
        "account_role": "account set-role",
        "account_disable": "account disable",
        "account_password": "account reset-password",
    }[query]
    authorize(ctx, command)
    with (
        locks.ACCOUNTS.hold(ctx.engine, wait_seconds=30),
        operation(ctx, command, {"name": name}),
        ctx.engine.begin() as conn,
    ):
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
