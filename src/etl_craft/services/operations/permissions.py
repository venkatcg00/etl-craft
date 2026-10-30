"""Current pipeline grants, shared by action admission and filtered read selections."""

import json
from dataclasses import dataclass, field, replace

from sqlalchemy.engine import Connection

from etl_craft.core.enums import ApiRole, YesNo
from etl_craft.core.enums_access import Permission
from etl_craft.core.errors import PermissionDeniedError, ResourceNotFoundError
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.pipelines import current_project_id, read_tags
from etl_craft.services.operations.context import OperationContext


@dataclass(frozen=True)
class PipelineAccess:
    """Effective roles by stable pipeline code in one database snapshot."""

    roles: dict[str, ApiRole | None]
    levels: dict[str, Permission | None] = field(default_factory=dict)
    sources: dict[str, tuple[str, ...]] = field(default_factory=dict)

    def permits(self, code: str, required: ApiRole = ApiRole.VIEWER) -> bool:
        """Whether the account has the required role on this pipeline."""
        role = self.roles.get(code)
        return (
            role is not None
            and role != ApiRole.AGENT
            and required != ApiRole.AGENT
            and tuple(ApiRole).index(role) >= tuple(ApiRole).index(required)
        )

    def require(self, ctx: OperationContext, code: str, required: ApiRole) -> None:
        """Refuse without revealing the pipeline's tasks, runs or logs."""
        if self.roles.get(code) is None:
            raise ResourceNotFoundError(
                f"pipeline {code!r} is missing or not permitted; select an accessible pipeline "
                "or ask a project manager for access"
            )
        if not self.permits(code, required):
            raise PermissionDeniedError(
                f"account {ctx.actor.name!r}: pipeline {code!r} needs {required} role; "
                f"effective role={self.roles.get(code) or 'none'}, "
                f"effective level={self.levels.get(code) or 'none'}; "
                "ask an administrator or CAN_MANAGE holder for the needed permission "
                "on this pipeline or tag"
            )


def visibility_parameters(ctx: OperationContext) -> dict[str, object]:
    """Bind the authenticated principal to the shared SQL permission relations."""
    return {
        "project_id": ctx.project_id or current_project_id(),
        "account": ctx.actor.name,
        "role": ctx.role,
        "sso_group_ids": json.dumps(ctx.sso_group_ids),
        "after_project": None,
        "project_limit": 1,
    }


def validate_visibility(conn: Connection, params: dict[str, object]) -> None:
    """Validate one SQL-selected malformed definition with the shared tag diagnostic."""
    row = conn.execute(statement(conn, "permission_invalid"), params).one_or_none()
    if row is not None:
        read_tags(row.pipeline_parameters, row.pipeline_code)


def eligible_grants(
    conn: Connection,
    ctx: OperationContext,
) -> list[tuple[str | None, str | None, Permission, str]]:
    """Read current principal matches from the shared query-catalog relation."""
    if ctx.role is None or ctx.role == ApiRole.AGENT:
        return []
    return [
        (r.pipeline_code, r.tag, Permission(r.permission), r.source)
        for r in conn.execute(statement(conn, "permission_eligible"), visibility_parameters(ctx))
    ]


def strongest(levels: list[Permission]) -> Permission | None:
    """Resolve the highest applicable level, never raising deployment administration rights."""
    return max(levels, key=tuple(Permission).index, default=None)


def project_permission(conn: Connection, ctx: OperationContext) -> Permission | None:
    """Use the shared SQL project policy, including principal and active-project checks."""
    if ctx.role is None or ctx.role == ApiRole.AGENT:
        return None
    row = conn.execute(
        statement(conn, "permission_projects"), visibility_parameters(ctx)
    ).one_or_none()
    return tuple(Permission)[row.level - 1] if row is not None else None


def project_not_found(code: str | None) -> ResourceNotFoundError:
    """Use the same response for an absent project and a hidden one."""
    return ResourceNotFoundError(
        f"project {code!r} is missing or not permitted; select an accessible project with "
        "--project or ask a deployment administrator or project manager for access"
    )


def require_project(
    conn: Connection, ctx: OperationContext, needed: Permission = Permission.CAN_VIEW
) -> None:
    """Gate project-owned reads and writes before resolving their objects."""
    level = project_permission(conn, ctx)
    if level is None:
        raise project_not_found(ctx.project)
    if tuple(Permission).index(level) < tuple(Permission).index(needed):
        raise PermissionDeniedError(
            f"account {ctx.actor.name!r}: project {ctx.project!r} effective level={level}; "
            f"expected {needed}; ask a deployment administrator or project CAN_MANAGE holder "
            "to grant the required permission"
        )


def tag_permission(conn: Connection, ctx: OperationContext, tag: str) -> Permission | None:
    """Only a direct tag permission can authorize delegation on that tag."""
    matched = eligible_grants(conn, ctx)
    project = project_permission(conn, ctx)
    if project == Permission.CAN_MANAGE:
        return project
    if project is None:
        return None
    return strongest([level for _, target, level, _ in matched if target == tag])


def pipeline_access(conn: Connection, ctx: OperationContext) -> PipelineAccess:
    """Resolve exactly the same effective SQL visibility used by aggregate reads."""
    validate_visibility(conn, visibility_parameters(ctx))
    roles: dict[str, ApiRole | None] = {}
    levels: dict[str, Permission | None] = {}
    sources: dict[str, tuple[str, ...]] = {}
    for row in conn.execute(statement(conn, "permission_effective"), visibility_parameters(ctx)):
        if row.pipeline_id is None:
            continue
        level = tuple(Permission)[row.level - 1] if row.level else None
        code = row.pipeline_code
        levels[code] = level
        roles[code] = (
            ApiRole.ADMIN
            if ctx.role == ApiRole.ADMIN
            else ApiRole.VIEWER
            if level == Permission.CAN_VIEW
            else ApiRole.OPERATOR
            if level is not None
            else None
        )
        values = json.loads(row.sources) if isinstance(row.sources, str) else row.sources
        sources[code] = tuple(values)
    return PipelineAccess(roles, levels, sources)


def visible_codes(conn: Connection, ctx: OperationContext, codes: dict[str, int]) -> dict[str, int]:
    """Filter before pagination or reading another pipeline's execution details."""
    access = pipeline_access(conn, ctx)
    return {code: identity for code, identity in codes.items() if access.permits(code)}


def require_child_groups(conn: Connection, ctx: OperationContext, code: str) -> None:
    """Require every retained SSO group to remain active before starting another child."""
    active = {
        row.group_id
        for row in conn.execute(statement(conn, "group_list"))
        if row.active_flag == YesNo.YES
    }
    missing = sorted(set(ctx.sso_group_ids) - active)
    if missing:
        raise PermissionDeniedError(
            f"account {ctx.actor.name!r}: pipeline {code!r} needs operator role; "
            f"retained group IDs {missing} are missing or inactive; expected ACTIVE_FLAG='Y'; "
            "ask an administrator to restore the groups and an active group grant"
        )


def visible_dependents(conn: Connection, ctx: OperationContext, code: str) -> tuple[str, ...]:
    """List incoming cross-project dependencies only when both pipelines are visible."""
    visible = []
    for row in conn.execute(
        statement(conn, "project_dependents"),
        {
            "project_id": ctx.project_id or current_project_id(),
            "pipeline_code": code,
        },
    ):
        scoped = replace(ctx, project=row.project_code, project_id=row.project_id)
        if pipeline_access(conn, scoped).permits(row.pipeline_code):
            visible.append(f"{row.project_code}.{row.pipeline_code}")
    return tuple(visible)
