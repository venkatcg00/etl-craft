"""Configuration, metadata and lineage documents shared by CLI and Python callers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar

from etl_craft.config import ConnectorConfig
from etl_craft.config.targets import active_catalog
from etl_craft.core.actor import acting_as
from etl_craft.core.errors import ResourceNotFoundError, UsageError
from etl_craft.engine.repository.lineage import fetch_lineage_versions
from etl_craft.services import doctor, lineage, validate
from etl_craft.services.operations.config_files import resolve_as_of
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


@dataclass(frozen=True)
class LineageQuery:
    """Related selectors validated before collecting or reading stored lineage."""

    table: str | None = None
    column: str | None = None
    upstream: bool = True
    downstream: bool = True
    depth: int | None = None
    refresh: bool = False
    as_of: str | None = None
    version: int | None = None

    def validate(self) -> None:
        """Reject incompatible selectors before reading or writing the Engine DB."""
        if self.column and not self.table:
            raise UsageError("--column needs --table")
        if self.depth is not None and self.depth < 1:
            raise UsageError(f"--depth must be 1 or more, got {self.depth}")
        if self.as_of is not None and self.version is not None:
            raise UsageError("--as-of and --version are mutually exclusive; choose one")
        if self.refresh and (self.as_of is not None or self.version is not None):
            raise UsageError("--refresh cannot read history; omit it with --as-of or --version")
        if self.version is not None and (self.table is None or self.version < 1):
            raise UsageError(
                f"--version needs --table and a positive version number; got table {self.table!r}, "
                f"version {self.version}; choose a table and version"
            )


def trace_lineage(ctx: OperationContext, query: LineageQuery) -> LineageView:
    """Collect and select lineage using the same validation for every caller."""
    query.validate()
    table, column = query.table, query.column
    upstream, downstream, depth = query.upstream, query.downstream, query.depth
    moment = None if query.as_of is None else resolve_as_of(ctx, query.as_of)
    if query.version is not None and query.table is not None:
        moment = _version_moment(ctx, query.table, query.version)
    with acting_as(ctx.actor):
        lineages = tuple(
            lineage.collect(ctx.engine, ctx.config, refresh=query.refresh)
            if moment is None
            else lineage.historical_lineage(ctx.engine, ctx.config, moment)
        )
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


def _version_moment(ctx: OperationContext, table: str, version: int) -> datetime:
    """Select one table's version, then read the graph at its opening time."""
    catalog = active_catalog(ctx.config) if ctx.config.warehouse is not None else None
    name = lineage.table_name(table.split("."), catalog)
    with ctx.engine.connect() as conn:
        versions = fetch_lineage_versions(conn)
    for item in versions:
        if (
            item.version_number == version
            and lineage.table_name(
                (item.catalog_name or "", item.schema_name, item.table_name), catalog
            )
            == name
        ):
            return item.valid_from
    raise ResourceNotFoundError(
        f"table {table!r} has no lineage version {version}; "
        "choose a version listed in its catalog history"
    )
