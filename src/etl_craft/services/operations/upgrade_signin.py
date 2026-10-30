"""Password proof reserved for the local migrate command across account-schema upgrades."""

from dataclasses import dataclass, replace

from etl_craft.core.actor import Actor, ActorKind
from etl_craft.core.enums import ActionName, ActionOutcome, ApiRole, YesNo
from etl_craft.core.errors import PermissionDeniedError
from etl_craft.engine.migrations import pending_migrations
from etl_craft.engine.queries import statement
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.recording import recorded
from etl_craft.services.passwords import DECOY, verify_password


@dataclass(frozen=True)
class UpgradeAdmin:
    """One local upgrade's proven actor, without issuing a session or token."""

    actor: Actor
    role: ApiRole = ApiRole.ADMIN


def pending_engine_upgrade(ctx: OperationContext) -> bool:
    """Whether the local upgrade needs the original-column authentication path."""
    return any(
        name.startswith("engine/")
        for name in pending_migrations(ctx.engine, project_default=ctx.config.migrations_dir)
    )


def sign_in_for_upgrade(ctx: OperationContext, name: str, password: str) -> UpgradeAdmin:
    """Read only original account columns; never add later account columns to this query.

    Only the local migrate command calls this proof. It grants no reusable session and never
    reads lockout counters or other account fields added after CFG_ACCOUNTS first shipped.
    """
    if not pending_engine_upgrade(ctx):
        raise PermissionDeniedError(
            "upgrade sign-in requires pending Engine migrations; run `etl-craft login` normally"
        )
    with ctx.engine.connect() as conn:
        row = conn.execute(statement(conn, "account_upgrade_signin"), {"name": name}).one_or_none()
    proven = verify_password(password, row.password_hash if row is not None else DECOY)
    accepted = bool(
        row is not None and proven and row.role == ApiRole.ADMIN and row.active_flag == YesNo.YES
    )
    caller = replace(ctx, actor=Actor(name, ActorKind.USER)) if accepted else ctx
    with recorded(
        caller, ActionName.MIGRATE, {"upgrade_signin": name}, on_completion=True
    ) as audit:
        if not accepted:
            error = PermissionDeniedError(
                "upgrade sign-in refused; expected an active admin and its correct password; "
                "ask an administrator to run `etl-craft migrate`"
            )
            audit.outcome, audit.exit_code = ActionOutcome.REFUSED, error.exit_code
            raise error
        audit.outcome = ActionOutcome.SUCCESS
    return UpgradeAdmin(caller.actor)
