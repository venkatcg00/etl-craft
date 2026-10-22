"""Configuration, metadata and lineage documents shared by CLI and Python callers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar

from etl_craft.config.targets import active_catalog
from etl_craft.core.enums import ApiRole, DeploymentProfile
from etl_craft.core.errors import ResourceNotFoundError, UsageError
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.lineage import fetch_lineage_versions
from etl_craft.engine.repository.pipelines import tagged_pipelines
from etl_craft.services import doctor, lineage, validate
from etl_craft.services.operations.agents import AgentsView, list_agents
from etl_craft.services.operations.config_files import resolve_as_of
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.permissions import visible_codes
from etl_craft.services.operations.requests import (
    LineageInput,
    LineageRefreshInput,
    NoInput,
    ValidateInput,
)
from etl_craft.services.validate import Report as Report


@dataclass(frozen=True)
class DoctorView:
    """Every configuration check and whether the configuration can run."""

    SCHEMA: ClassVar[str] = "etl-craft/doctor/1"
    checks: tuple[doctor.Check, ...]
    failed: bool
    agents: AgentsView | None = None


def check_configuration(ctx: OperationContext, request: NoInput) -> DoctorView:
    """Report unreachable services rather than requiring an Engine DB connection first."""
    checks = tuple(doctor.run_checks(ctx.config))
    failed = any(c.status == doctor.Status.FAIL for c in checks)
    agents = (
        list_agents(ctx, NoInput())
        if not failed and ctx.config.deployment == DeploymentProfile.CONTROLLER_AGENTS
        else None
    )
    if agents is not None and not any(any(a.free.values()) for a in agents.agents):
        checks += (doctor.warn("Agent availability", "no eligible agent; attempts stay QUEUED"),)
    return DoctorView(checks, failed, agents)


def validate_metadata(ctx: OperationContext, request: ValidateInput) -> validate.Report:
    """Validate metadata and files under the caller's actor without running tasks."""
    report = validate.validate(ctx.engine, ctx.config, request.pipeline_code)
    if request.tag or ctx.role != ApiRole.ADMIN:
        with ctx.engine.connect() as conn:
            codes = visible_codes(conn, ctx, tagged_pipelines(conn, request.tag))
            if request.pipeline_code is not None:
                codes = {code: i for code, i in codes.items() if code == request.pipeline_code}
            tasks = sum(
                1
                for row in conn.execute(statement(conn, "catalog_tasks"))
                if row.pipeline_code in codes
            )
        report.findings = [
            finding for finding in report.findings if finding.where.partition(".")[0] in codes
        ]
        report.pipelines, report.tasks = len(codes), tasks
    return report


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


def trace_lineage(ctx: OperationContext, request: LineageInput) -> LineageView:
    """Trace the stored lineage, now or at a recorded moment."""
    return _trace(ctx, request, refresh=False)


def refresh_lineage(ctx: OperationContext, request: LineageRefreshInput) -> LineageView:
    """Work every task's lineage out again, record it, and trace it."""
    return _trace(ctx, request, refresh=True)


def _trace(ctx: OperationContext, query: LineageInput, *, refresh: bool) -> LineageView:
    table, column = query.table, query.column
    upstream, downstream, depth = query.upstream, query.downstream, query.depth
    moment = None if query.as_of is None else resolve_as_of(ctx, query.as_of)
    if query.version is not None and query.table is not None:
        moment = _version_moment(ctx, query.table, query.version)
    lineages = tuple(
        lineage.collect(ctx.engine, ctx.config, refresh=refresh)
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
