"""Configuration, metadata and lineage documents shared by CLI and Python callers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from etl_craft.config import ConnectorConfig
from etl_craft.config.targets import active_catalog
from etl_craft.core.actor import acting_as
from etl_craft.core.errors import UsageError
from etl_craft.services import doctor, lineage, validate
from etl_craft.services.operations.context import OperationContext


@dataclass(frozen=True)
class DoctorView:
    """Every configuration check and whether the configuration can run."""

    SCHEMA: ClassVar[str] = "etl-craft/doctor/1"
    checks: tuple[doctor.Check, ...]
    failed: bool


def check_configuration(config: ConnectorConfig) -> DoctorView:
    """Report unreachable services rather than requiring an Engine DB connection first."""
    checks = tuple(doctor.run_checks(config))
    return DoctorView(checks, any(c.status == doctor.Status.FAIL for c in checks))


def validate_metadata(ctx: OperationContext, pipeline_code: str | None = None) -> validate.Report:
    """Validate metadata and files under the caller's actor without running tasks."""
    with acting_as(ctx.actor):
        return validate.validate(ctx.engine, ctx.config, pipeline_code)


@dataclass(frozen=True)
class LineageView:
    """Collected lineage and the selected upstream and downstream paths."""

    SCHEMA: ClassVar[str] = "etl-craft/lineage/1"
    lineages: tuple[lineage.TaskLineage, ...]
    table: str | None
    column: str | None
    upstream_tables: tuple[tuple[int, str, str], ...]
    downstream_tables: tuple[tuple[int, str, str], ...]
    upstream_columns: tuple[tuple[int, lineage.Edge], ...]
    downstream_columns: tuple[tuple[int, lineage.Edge], ...]
    upstream: bool
    downstream: bool
    failed: bool


def trace_lineage(
    ctx: OperationContext,
    *,
    table: str | None = None,
    column: str | None = None,
    upstream: bool = True,
    downstream: bool = True,
    depth: int | None = None,
    refresh: bool = False,
) -> LineageView:
    """Collect and select lineage using the same validation for every caller."""
    if column and not table:
        raise UsageError("--column needs --table")
    if depth is not None and depth < 1:
        raise UsageError(f"--depth must be 1 or more, got {depth}")
    with acting_as(ctx.actor):
        lineages = tuple(lineage.collect(ctx.engine, ctx.config, refresh=refresh))
    graph = lineage.LineageGraph.of(lineages)
    if table is not None:
        catalog = active_catalog(ctx.config) if ctx.config.warehouse is not None else None
        table = lineage.table_name(table.split("."), catalog)
    if column is not None:
        column = column.lower()
        known = graph.columns(table or "")
        if column not in known:
            hint = f"; its traced columns: {', '.join(known)}" if known else ""
            raise UsageError(f"no lineage mentions {table}.{column}{hint}")
    return LineageView(
        lineages,
        table,
        column,
        tuple(graph.upstream_tables(table)) if table and upstream and not column else (),
        tuple(graph.downstream_tables(table)) if table and downstream and not column else (),
        tuple(graph.upstream(table, column, depth)) if table and column and upstream else (),
        tuple(graph.downstream(table, column, depth)) if table and column and downstream else (),
        upstream,
        downstream,
        any(item.error for item in lineages),
    )
