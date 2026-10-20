"""Read-only project gap comparisons under the authenticated caller's identity."""

from dataclasses import dataclass

from etl_craft.services.catalog.collect import collect_catalog
from etl_craft.services.catalog.gaps import GapSnapshot, gap_snapshot
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.requests import Request


@dataclass(frozen=True, kw_only=True)
class GapsInput(Request):
    """Whether to compare the documented columns with the warehouse as well."""

    compare: bool = False


def get_gaps(ctx: OperationContext, request: GapsInput) -> GapSnapshot:
    """Read metadata only; do not publish lineage or read warehouse business rows."""
    catalog = collect_catalog(
        ctx.engine, ctx.config, with_warehouse=request.compare, record_lineage=False
    )
    return gap_snapshot(catalog)
