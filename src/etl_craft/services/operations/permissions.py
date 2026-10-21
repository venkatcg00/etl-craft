"""Current pipeline grants, shared by action admission and filtered read selections."""

from dataclasses import dataclass

from sqlalchemy.engine import Connection

from etl_craft.core.enums import ApiRole, YesNo
from etl_craft.core.errors import PermissionDeniedError
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.pipelines import read_tags
from etl_craft.services.operations.context import OperationContext


@dataclass(frozen=True)
class PipelineAccess:
    """Effective roles by stable pipeline code in one database snapshot."""

    roles: dict[str, ApiRole | None]

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
        if not self.permits(code, required):
            raise PermissionDeniedError(
                f"account {ctx.actor.name!r}: pipeline {code!r} needs {required} role; "
                f"effective role={self.roles.get(code) or 'none'}; "
                "ask an administrator for an active group grant on this pipeline or its tag"
            )


def pipeline_access(conn: Connection, ctx: OperationContext) -> PipelineAccess:
    """Resolve active grants against current memberships and retained SSO group identities."""
    groups = {
        row.group_id
        for row in conn.execute(statement(conn, "permission_groups"), {"account": ctx.actor.name})
        if row.explicit_member == YesNo.YES or row.group_id in ctx.sso_group_ids
    }
    grants = tuple(conn.execute(statement(conn, "permission_grants")))
    roles: dict[str, ApiRole | None] = {}
    for row in conn.execute(statement(conn, "permission_pipelines")):
        tags = read_tags(row.pipeline_parameters, row.pipeline_code)
        applicable = [g for g in grants if g.pipeline_code == row.pipeline_code or g.tag in tags]
        if ctx.role is None or ctx.role == ApiRole.ADMIN or not applicable:
            roles[row.pipeline_code] = ctx.role
        else:
            assigned = [ApiRole(g.role) for g in applicable if g.group_id in groups]
            roles[row.pipeline_code] = max(assigned, key=tuple(ApiRole).index, default=None)
    return PipelineAccess(roles)


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
