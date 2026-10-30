"""Report grant matching changes without guessing accounts' external group membership."""

from sqlalchemy.engine import Connection

from etl_craft.core.enums import YesNo
from etl_craft.core.enums_access import Permission
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.pipelines import read_tags
from etl_craft.services.config_models import ConfigFiles, TagAccessChange, TagGrantMatch
from etl_craft.services.operations.context import OperationContext


def tag_access_changes(
    conn: Connection, ctx: OperationContext, files: ConfigFiles
) -> tuple[TagAccessChange, ...]:
    """Compare active grant matches before and after only the submitted pipeline tag edits."""
    submitted = files.rows.get("pipelines.csv", {})
    if not submitted:
        return ()
    previous = {
        row.pipeline_code: read_tags(row.pipeline_parameters, row.pipeline_code)
        for row in conn.execute(statement(conn, "permission_pipelines"))
    }
    grants = [
        row
        for row in conn.execute(statement(conn, "access_list"), {"project": ctx.project})
        if row.active_flag == YesNo.YES
    ]
    changes = []
    for row in submitted.values():
        code = str(row.values["PIPELINE_CODE"])
        before = set(previous.get(code, ()))
        after = set(read_tags(row.values["PIPELINE_PARAMETERS"], code))
        if before == after:
            continue
        matches = tuple(
            TagGrantMatch(
                f"group:{grant.group_name}"
                if grant.group_name
                else f"account:{grant.account}"
                if grant.account
                else f"role:{grant.role}",
                grant.tag,
                Permission(grant.permission),
                grant.tag in after,
            )
            for grant in grants
            if grant.tag is not None and (grant.tag in before) != (grant.tag in after)
        )
        was_restricted = any(g.pipeline == code or g.tag in before for g in grants)
        is_restricted = any(g.pipeline == code or g.tag in after for g in grants)
        if matches or was_restricted != is_restricted:
            policy = None
            if was_restricted != is_restricted:
                policy = (
                    "open -> restricted: project-level access no longer applies; only matching "
                    "pipeline and tag grants (plus project CAN_MANAGE) reach this pipeline"
                    if is_restricted
                    else "restricted -> open: the project level now applies"
                )
            changes.append(TagAccessChange(code, was_restricted, is_restricted, matches, policy))
    return tuple(changes)
