"""Attributed permission delegation, independent of developer config files."""

from dataclasses import replace

from sqlalchemy.engine import Connection

from etl_craft.core.enums import ActionName, ActionOutcome, ApiRole, YesNo
from etl_craft.core.enums_access import AccessChange, AccessIntent, Permission
from etl_craft.core.errors import PermissionDeniedError, ResourceNotFoundError, UsageError
from etl_craft.engine import locks
from etl_craft.engine.audit import command_request
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.pipelines import read_tags
from etl_craft.services.operations.access_inputs import Validated, inputs, project, validate
from etl_craft.services.operations.access_models import AccessFilter as AccessFilter
from etl_craft.services.operations.access_models import AccessView as AccessView
from etl_craft.services.operations.access_models import AccessWrite as AccessWrite
from etl_craft.services.operations.access_models import Diff, Grant
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.permissions import (
    eligible_grants,
    pipeline_access,
    project_permission,
    require_project,
    tag_permission,
)
from etl_craft.services.operations.requests import arguments


def required_role(ctx: OperationContext, request: object) -> ApiRole:
    """Bulk permissions remain deployment administrator operations."""
    return (
        ApiRole.ADMIN
        if isinstance(request, AccessWrite) and request.content is not None
        else ApiRole.VIEWER
    )


def list_access(ctx: OperationContext, request: AccessFilter) -> AccessView:
    """List visible permissions and the caller's effective levels and their sources."""
    with ctx.engine.connect() as conn:
        project_id = project(conn, request.project)
        assert request.project is not None
        scoped = replace(ctx, project=request.project, project_id=project_id)
        require_project(conn, scoped)
        access = pipeline_access(conn, scoped)
        grants = tuple(
            Grant(
                row.project,
                row.group_name,
                row.account,
                row.role,
                row.pipeline,
                row.tag,
                row.permission,
                row.active_flag,
            )
            for row in conn.execute(statement(conn, "access_list"), {"project": request.project})
            if (request.group is None or row.group_name == request.group)
            and (request.account is None or row.account == request.account)
            and (request.role is None or row.role == request.role)
            and (request.pipeline is None or row.pipeline == request.pipeline)
            and (request.tag is None or row.tag == request.tag)
            and (
                ctx.role == ApiRole.ADMIN
                or (
                    access.permits(row.pipeline)
                    if row.pipeline
                    else tag_permission(conn, scoped, row.tag) is not None
                    if row.tag
                    else project_permission(conn, scoped) is not None
                )
            )
        )
        tagged = {
            row.pipeline_code
            for row in conn.execute(
                statement(conn, "permission_pipelines"), {"project_id": scoped.project_id}
            )
            if request.tag is None
            or request.tag in read_tags(row.pipeline_parameters, row.pipeline_code)
        }
        effective = tuple(
            (code, level, access.sources[code])
            for code, level in access.levels.items()
            if level is not None
            and code in tagged
            and (request.pipeline is None or code == request.pipeline)
        )
        level = project_permission(conn, scoped)
        sources = tuple(
            source
            for pipeline, tag, _, source in eligible_grants(conn, scoped)
            if pipeline is None and tag is None
        )
    return AccessView(
        grants=grants, effective=effective, project_permission=level, project_sources=sources
    )


def grant_access(ctx: OperationContext, request: AccessWrite) -> AccessView:
    """Validate and apply the submitted changes in one locked transaction."""
    return write(ctx, request, AccessIntent.GRANT)


def revoke_access(ctx: OperationContext, request: AccessWrite) -> AccessView:
    """Retire an identity without deleting its history."""
    if request.content is not None or request.dry_run:
        raise UsageError(
            "access revoke: expected a single identity; use access grant --file for bulk"
        )
    return write(ctx, request, AccessIntent.REVOKE)


def authorize(conn: Connection, ctx: OperationContext, row: Validated) -> None:
    """Check delegation in the same transaction as publication."""
    if ctx.role == ApiRole.ADMIN:
        return
    scoped = replace(ctx, project=row.values["PROJECT"], project_id=row.project_id)
    require_project(conn, scoped)
    if project_permission(conn, scoped) == Permission.CAN_MANAGE:
        return
    pipeline, tag = row.values["PIPELINE_CODE"], row.values["TAG"]
    level = (
        pipeline_access(conn, scoped).levels.get(pipeline)
        if pipeline
        else tag_permission(conn, scoped, tag)
        if tag
        else project_permission(conn, scoped)
    )
    if level != Permission.CAN_MANAGE:
        raise PermissionDeniedError(
            f"account {ctx.actor.name!r}: securable "
            f"{pipeline or tag or scoped.project!r} "
            f"in project {scoped.project!r}: effective level={level or 'none'}; "
            "expected CAN_MANAGE; ask an administrator or a CAN_MANAGE holder on this securable"
        )


def publish(conn: Connection, request: AccessWrite, row: Validated) -> Diff:
    """Diff one resolved identity, retaining a revoked row's existing permission."""
    values = row.values
    pipeline, tag = values["PIPELINE_CODE"] or None, values["TAG"] or None
    old = next(
        (
            r
            for r in conn.execute(statement(conn, "access_list"), {"project": values["PROJECT"]})
            if (r.group_name, r.account, r.role, r.pipeline, r.tag)
            == (
                values["GROUP_NAME"] or None,
                values["ACCOUNT"] or None,
                values["ROLE"] or None,
                pipeline,
                tag,
            )
        ),
        None,
    )
    active = YesNo.YES if values["ACTION"] == AccessIntent.GRANT else YesNo.NO
    permission = old.permission if active == YesNo.NO and old is not None else values["PERMISSION"]
    change = AccessChange.UNCHANGED
    if active == YesNo.YES and (old is None or old.active_flag == YesNo.NO):
        change = AccessChange.ADDED
    elif active == YesNo.NO and old is not None and old.active_flag == YesNo.YES:
        change = AccessChange.REVOKED
    elif active == YesNo.YES and old is not None and old.permission != permission:
        change = AccessChange.CHANGED_PERMISSION
    grant = Grant(
        values["PROJECT"],
        values["GROUP_NAME"] or None,
        values["ACCOUNT"] or None,
        values["ROLE"] or None,
        pipeline,
        tag,
        permission,
        active,
    )
    if not request.dry_run and change != AccessChange.UNCHANGED:
        params = {
            "project_id": row.project_id,
            "group_id": row.group_id,
            "account_id": row.account_id,
            "grantee_role": grant.role,
            "pipeline": pipeline,
            "tag": tag,
            "permission": permission,
            "active_flag": active,
            "grant_id": None if old is None else old.grant_id,
        }
        conn.execute(statement(conn, "access_add" if old is None else "access_set"), params)
    return Diff(row.line, grant, None if old is None else old.permission, change)


def write(ctx: OperationContext, request: AccessWrite, intent: AccessIntent) -> AccessView:
    """Record delegated refusals after the locked check has made its final decision."""
    action = ActionName.ACCESS_GRANT if intent == AccessIntent.GRANT else ActionName.ACCESS_REVOKE
    with command_request(action, arguments(request)) as audit:
        audit.engine = ctx.engine
        try:
            return apply_changes(ctx, request, intent)
        except (PermissionDeniedError, ResourceNotFoundError) as error:
            audit.outcome = ActionOutcome.REFUSED
            audit.exit_code = error.exit_code
            raise


def apply_changes(ctx: OperationContext, request: AccessWrite, intent: AccessIntent) -> AccessView:
    """Validate all rows before any write, serialized with identity and metadata changes."""
    if not request.reason.strip():
        raise UsageError(
            "access change reason is empty; expected a reason; supply --reason "
            "explaining the change"
        )
    if request.content is not None and ctx.role != ApiRole.ADMIN:
        raise PermissionDeniedError(
            f"access grant bulk: account {ctx.actor.name!r} role={ctx.role}; "
            f"expected admin; ask a deployment administrator"
        )
    rows = inputs(request, intent)
    with (
        locks.MIGRATE.hold(ctx.engine, wait_seconds=30),
        locks.ACCOUNTS.hold(ctx.engine, wait_seconds=30),
        ctx.engine.begin() as conn,
    ):
        if (
            ctx.role != ApiRole.ADMIN
            and request.pipeline
            and project_permission(conn, ctx) != Permission.CAN_MANAGE
        ):
            pipeline_access(conn, ctx).require(ctx, request.pipeline, ApiRole.VIEWER)
        checked = validate(conn, request, rows)
        for row in checked:
            authorize(conn, ctx, row)
        changes = tuple(publish(conn, request, row) for row in checked)
    return AccessView(changes=changes, dry_run=request.dry_run)
