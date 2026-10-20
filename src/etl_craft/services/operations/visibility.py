"""Remove execution details of restricted related pipelines from operation documents."""

from dataclasses import fields, is_dataclass, replace

from sqlalchemy.engine import Connection

from etl_craft.core.enums import ApiRole
from etl_craft.engine.queries import statement
from etl_craft.services.lineage import Edge, TaskLineage
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.diagnostics import LineageView
from etl_craft.services.operations.models import GraphView, RunView, TaskRunView
from etl_craft.services.operations.permissions import PipelineAccess, pipeline_access
from etl_craft.services.operations.status import DependencyView


def visible_result(ctx: OperationContext, result: object) -> object:
    """Keep related pipeline names while withholding their tasks and run identities."""
    if ctx.role == ApiRole.ADMIN or not is_dataclass(result):
        return result
    with ctx.engine.connect() as conn:
        return _visible(conn, pipeline_access(conn, ctx), result)


def _run_allowed(conn: Connection, access: PipelineAccess, run_id: int | None) -> bool:
    if run_id is None:
        return False
    code = conn.execute(statement(conn, "api_run_code"), {"run_id": run_id}).scalar_one_or_none()
    return code is not None and access.permits(code)


def _visible(conn: Connection, access: PipelineAccess, value: object) -> object:
    if (
        isinstance(value, DependencyView)
        and not access.permits(value.upstream.partition(".")[0])
        and ("." in value.upstream or value.pipeline_dependency_id is not None)
    ):
        return DependencyView(
            value.upstream.partition(".")[0],
            value.dependency_type,
            None,
            value.met,
            "Related pipeline details require a viewer grant.",
        )
    if isinstance(value, (TaskLineage, Edge)):
        label = value.task
        if label and not access.permits(label.partition(".")[0]):
            if isinstance(value, TaskLineage):
                return TaskLineage(label.partition(".")[0], None, [], sources=())
            return replace(value, task=label.partition(".")[0], transformation="")
    if isinstance(value, (RunView, TaskRunView)):
        children = tuple(i for i in value.child_run_ids if _run_allowed(conn, access, i))
        value = replace(value, child_run_ids=children)
        if isinstance(value, RunView) and not _run_allowed(conn, access, value.parent_run_id):
            value = replace(value, parent_run_id=None, triggered_by_task_run_id=None)
    if isinstance(value, LineageView):
        value = replace(
            value,
            upstream_tables=tuple(
                (depth, table, _related_label(access, task))
                for depth, table, task in value.upstream_tables
            ),
            downstream_tables=tuple(
                (depth, table, _related_label(access, task))
                for depth, table, task in value.downstream_tables
            ),
        )
    if isinstance(value, GraphView):
        value = replace(
            value,
            depends_on={
                task: tuple(
                    (
                        _related_label(access, label),
                        kind,
                    )
                    for label, kind in dependencies
                )
                for task, dependencies in value.depends_on.items()
            },
        )
    if is_dataclass(value) and not isinstance(value, type):
        return replace(
            value, **{f.name: _visible(conn, access, getattr(value, f.name)) for f in fields(value)}
        )
    if isinstance(value, tuple):
        return tuple(_visible(conn, access, item) for item in value)
    if isinstance(value, list):
        return [_visible(conn, access, item) for item in value]
    if isinstance(value, dict):
        return {key: _visible(conn, access, item) for key, item in value.items()}
    return value


def _related_label(access: PipelineAccess, label: str) -> str:
    """Keep the related pipeline name, without a task the account cannot view."""
    return (
        label
        if "." not in label or access.permits(label.partition(".")[0])
        else label.partition(".")[0]
    )
