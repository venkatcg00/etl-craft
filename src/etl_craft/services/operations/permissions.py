"""One deployment role policy for service operations and their transport adapters."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from etl_craft.core.actor import acting_as
from etl_craft.core.enums import ActionOutcome, ApiRole
from etl_craft.core.errors import ConfigurationError, PermissionDeniedError
from etl_craft.engine.audit import ActionRequest, mask_arguments
from etl_craft.engine.migrations import pending_migrations

if TYPE_CHECKING:
    from etl_craft.services.operations.context import OperationContext


ROLES: dict[str, ApiRole | None] = {
    **dict.fromkeys(("login", "logout", "refresh")),
    **dict.fromkeys(
        (
            "whoami",
            "data provenance",
            "list",
            "graph",
            "steps",
            "status",
            "explain",
            "history",
            "audit",
            "health",
            "attempt",
            "attempt log",
            "logs",
            "catalog gaps",
            "validate",
            "doctor",
            "lineage",
            "generate-yml",
            "config plan",
            "config export",
            "docs-version check",
            "rehash dry-run",
            "setup print-grants",
        ),
        ApiRole.VIEWER,
    ),
    **dict.fromkeys(
        ("run", "mark", "stop", "abort", "hold", "release", "pause", "resume", "reconcile"),
        ApiRole.OPERATOR,
    ),
    **dict.fromkeys(
        ("config apply", "migrate", "generate-docs", "lineage refresh"),
        ApiRole.DEVELOPER,
    ),
    **dict.fromkeys(
        (
            "account add",
            "data edit",
            "data unlock",
            "account list",
            "account set-role",
            "account disable",
            "account reset-password",
            "token create",
            "token list",
            "token revoke",
            "setup",
            "init-db",
            "migrate engine",
            "upgrade-targets",
            "rehash",
            "clone",
            "server",
            "publish-docs",
            "docs-version",
        ),
        ApiRole.ADMIN,
    ),
}


def operation_name(command: str, arguments: Mapping[str, object]) -> str:
    """Select subcommands and write/check variants without transport-specific role rules."""
    if command in {"account", "token", "config"}:
        return f"{command} {arguments.get(command + '_verb')}"
    for name, flag, suffix in (
        ("lineage", "refresh", "refresh"),
        ("docs-version", "check", "check"),
        ("rehash", "dry_run", "dry-run"),
        ("setup", "print_grants", "print-grants"),
    ):
        if command == name and arguments.get(flag):
            return f"{name} {suffix}"
    return command


def required_role(command: str, arguments: Mapping[str, object]) -> ApiRole | None:
    """Fail closed for an operation missing from the shared policy."""
    name = operation_name(command, arguments)
    if name not in ROLES:
        raise ConfigurationError(
            f"operation {name!r}: no role policy; expected a registered operation; "
            "add its policy before exposing it"
        )
    return ROLES[name]


def has_role(role: ApiRole | None, required: ApiRole | None) -> bool:
    """Whether ``role`` meets ``required``; an unproven role meets only no requirement."""
    return required is None or (
        role is not None and tuple(ApiRole).index(role) >= tuple(ApiRole).index(required)
    )


def authorize(
    ctx: OperationContext, command: str, arguments: Mapping[str, object] | None = None
) -> None:
    """Refuse and audit insufficient roles before any operation can read or mutate state."""
    arguments = arguments or {}
    required = required_role(command, arguments)
    if command == "migrate" and ctx.role == ApiRole.DEVELOPER:
        pending = pending_migrations(
            ctx.engine,
            project_dir=(
                directory
                if isinstance(directory := arguments.get("migrations_dir"), (str, Path))
                else None
            ),
            project_default=ctx.config.migrations_dir,
        )
        if any(name.startswith("engine/") for name in pending):
            required = ROLES["migrate engine"]
    if has_role(ctx.role, required):
        return
    name = operation_name(command, arguments)
    error = PermissionDeniedError(
        f"account {ctx.actor.name!r}: role={ctx.role or 'unproven'}; operation {name!r} "
        f"{required} role is required; ask an administrator to grant {required} or a higher role"
    )
    with acting_as(ctx.actor):
        ActionRequest(
            name,
            mask_arguments(arguments),
            datetime.now(UTC),
            actor=ctx.actor,
            engine=ctx.engine,
            outcome=ActionOutcome.REFUSED,
            exit_code=error.exit_code,
        ).record()
    raise error
