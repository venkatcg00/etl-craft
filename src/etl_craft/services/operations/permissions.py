"""Current pipeline grants, shared by action admission and filtered read selections."""

from collections.abc import Sequence
from dataclasses import dataclass, field, replace

from sqlalchemy.engine import Connection, Row

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


def eligible_grants(
    conn: Connection, ctx: OperationContext, grants: Sequence[Row[tuple[object, ...]]] | None = None
) -> list[tuple[str | None, str | None, Permission, str]]:
    """Match principals against current accounts/roles and active retained group identities."""
    if ctx.role is None or ctx.role == ApiRole.AGENT:
        return []
    groups = {
        row.group_id
        for row in conn.execute(statement(conn, "permission_groups"), {"account": ctx.actor.name})
        if row.explicit_member == YesNo.YES or row.group_id in ctx.sso_group_ids
    }
    selected = []
    if grants is None:
        grants = conn.execute(
            statement(conn, "permission_grants"),
            {"project_id": ctx.project_id or current_project_id()},
        ).all()
    for row in grants:
        source = None
        if row.group_id in groups:
            source = f"group:{row.group_id}"
        elif row.account_name == ctx.actor.name and row.account_active == YesNo.YES:
            source = f"account:{row.account_name}"
        elif (
            row.grantee_role
            and ctx.role is not None
            and tuple(ApiRole).index(ctx.role) >= tuple(ApiRole).index(ApiRole(row.grantee_role))
        ):
            source = f"role:{row.grantee_role}"
        if source is not None:
            selected.append((row.pipeline_code, row.tag, Permission(row.permission), source))
    return selected


def strongest(levels: list[Permission]) -> Permission | None:
    """Resolve the highest applicable level, never raising deployment administration rights."""
    return max(levels, key=tuple(Permission).index, default=None)


def _project_level(
    ctx: OperationContext, matched: list[tuple[str | None, str | None, Permission, str]]
) -> Permission | None:
    if ctx.role == ApiRole.ADMIN:
        return Permission.CAN_MANAGE
    return strongest(
        [level for pipeline, tag, level, _ in matched if pipeline is None and tag is None]
    )


def project_permission(conn: Connection, ctx: OperationContext) -> Permission | None:
    """Resolve project grants independently of pipeline and tag grants."""
    return _project_level(ctx, eligible_grants(conn, ctx))


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
    project = _project_level(ctx, matched)
    if project == Permission.CAN_MANAGE:
        return project
    if project is None:
        return None
    return strongest([level for _, target, level, _ in matched if target == tag])


def pipeline_access(conn: Connection, ctx: OperationContext) -> PipelineAccess:
    """Resolve project gating and matching grants without opening restricted pipelines."""
    params = {"project_id": ctx.project_id or current_project_id()}
    grants = tuple(conn.execute(statement(conn, "permission_grants"), params))
    matched = eligible_grants(conn, ctx, grants)
    project = _project_level(ctx, matched)
    roles: dict[str, ApiRole | None] = {}
    levels: dict[str, Permission | None] = {}
    sources: dict[str, tuple[str, ...]] = {}
    for row in conn.execute(statement(conn, "permission_pipelines"), params):
        code = row.pipeline_code
        tags = read_tags(row.pipeline_parameters, code)
        applicable = [g for g in grants if g.pipeline_code == code or g.tag in tags]
        assigned = [
            (level, source)
            for pipeline, tag, level, source in matched
            if pipeline == code or tag in tags
        ]
        selected: tuple[str, ...]
        if project is None:
            level, selected = None, ()
        elif project == Permission.CAN_MANAGE or not applicable:
            level = project
            selected = ("project:" + str(project),)
        else:
            level = strongest([level for level, _ in assigned])
            selected = tuple(source for _, source in assigned)
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
        sources[code] = selected
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
