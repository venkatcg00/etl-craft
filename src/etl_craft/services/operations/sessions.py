"""User sign-in and short-lived, revocable account sessions."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta

from etl_craft.core.actor import Actor, ActorKind
from etl_craft.core.enums import AccountKind, YesNo
from etl_craft.core.errors import UsageError
from etl_craft.core.text import sha256_hex
from etl_craft.engine import locks
from etl_craft.engine.queries import statement
from etl_craft.services.operations.context import OperationContext, operation
from etl_craft.services.operations.tokens import CreatedToken, issue_token
from etl_craft.services.passwords import verify_password


def sign_in(
    ctx: OperationContext, name: str, password: str | None = None, *, subject: str | None = None
) -> CreatedToken:
    """Require proof of a registered user; SSO subjects come only from the verified device flow."""
    with locks.ACCOUNTS.hold(ctx.engine, wait_seconds=30):
        with ctx.engine.connect() as conn:
            row = conn.execute(statement(conn, "account_find"), {"name": name}).one_or_none()
        if (
            row is None
            or row.kind != AccountKind.USER
            or row.active_flag != YesNo.YES
            or not (
                (subject is not None and subject == row.oidc_subject)
                or (
                    subject is None
                    and password is not None
                    and verify_password(password, row.password_hash)
                )
            )
        ):
            with operation(ctx, "login", {"name": name}):
                raise UsageError(
                    f"account {name!r}: sign-in refused; expected an active registered USER "
                    "with matching credentials; ask an administrator to check the account"
                )
        caller = replace(ctx, actor=Actor(name, ActorKind.USER))
        with operation(caller, "login", {"name": name}), ctx.engine.begin() as conn:
            return issue_token(
                conn,
                name,
                row.account_id,
                name,
                datetime.now(UTC) + timedelta(hours=ctx.config.auth.session_hours),
            )


def sign_in_subject(ctx: OperationContext, subject: str) -> CreatedToken:
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
    return sign_in(ctx, name, subject=subject)


def sign_out(ctx: OperationContext, token: str) -> None:
    """Revoke even an expired session; do not require it to authenticate first."""
    with operation(ctx, "logout", {}), ctx.engine.begin() as conn:
        conn.execute(
            statement(conn, "session_revoke"),
            {
                "digest": sha256_hex(token.encode()),
                "now": datetime.now(UTC),
            },
        )
