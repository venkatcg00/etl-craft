"""Read-only project gap comparisons under the authenticated caller's identity."""

from etl_craft.core.actor import acting_as
from etl_craft.services.catalog import collect_catalog
from etl_craft.services.catalog_gaps import GapSnapshot, gap_snapshot
from etl_craft.services.operations.context import OperationContext


def get_gaps(ctx: OperationContext, *, compare: bool = False) -> GapSnapshot:
    """Read metadata only; do not publish lineage or read warehouse business rows."""
    with acting_as(ctx.actor):
        catalog = collect_catalog(
            ctx.engine, ctx.config, with_warehouse=compare, record_lineage=False
        )
        return gap_snapshot(catalog)
