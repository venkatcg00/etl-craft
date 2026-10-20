"""Read-only project gap comparisons under the authenticated caller's identity."""

from dataclasses import dataclass, replace

from etl_craft.core.enums import ApiRole
from etl_craft.services.catalog.collect import collect_catalog
from etl_craft.services.catalog.gaps import GapSnapshot, gap_snapshot
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.permissions import pipeline_access
from etl_craft.services.operations.requests import TagFilter


@dataclass(frozen=True, kw_only=True)
class GapsInput(TagFilter):
    """Whether to compare the documented columns with the warehouse as well."""

    compare: bool = False


def get_gaps(ctx: OperationContext, request: GapsInput) -> GapSnapshot:
    """Read metadata only; do not publish lineage or read warehouse business rows."""
    catalog = collect_catalog(
        ctx.engine, ctx.config, with_warehouse=request.compare, record_lineage=False
    )
    if not request.tag and ctx.role == ApiRole.ADMIN:
        return gap_snapshot(catalog)
    with ctx.engine.connect() as conn:
        access = pipeline_access(conn, ctx)
    if ctx.role != ApiRole.ADMIN:
        tasks = {
            name: task
            for name, task in catalog.tasks.items()
            if access.permits(task.row.pipeline_code)
        }
        catalog = replace(
            catalog,
            tasks=tasks,
            tables={
                name: replace(
                    table,
                    writers=[t for t in table.writers if t in tasks],
                    readers=[t for t in table.readers if t in tasks],
                )
                for name, table in catalog.tables.items()
            },
            rules={key: rule for key, rule in catalog.rules.items() if rule.task in tasks},
        )
    snapshot = gap_snapshot(catalog)
    codes = {
        code
        for code, pipeline in catalog.pipelines.items()
        if access.permits(code) and set(request.tag) <= set(pipeline.row.tags)
    }
    return replace(
        snapshot,
        records=tuple(
            replace(record, tasks=tuple(t for t in record.tasks if t.partition(".")[0] in codes))
            for record in snapshot.records
            if any(t.partition(".")[0] in codes for t in record.tasks)
        ),
    )
