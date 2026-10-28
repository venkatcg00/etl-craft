"""Validate Engine DB definitions against immutable project files and record the result."""

import json
import re
from dataclasses import asdict, dataclass
from datetime import UTC, datetime

from sqlalchemy.engine import Connection

from etl_craft.config import ConnectorConfig
from etl_craft.config.project import project_config
from etl_craft.core.enums import RunStatus
from etl_craft.core.errors import EtlCraftError, ResourceNotFoundError
from etl_craft.core.text import sha256_hex
from etl_craft.core.time import as_utc
from etl_craft.dialects.engine import for_engine
from etl_craft.engine import locks
from etl_craft.engine.bundles import Bundle
from etl_craft.engine.connection import read_snapshot
from etl_craft.engine.project_files import collect, decode_files
from etl_craft.engine.queries import statement
from etl_craft.engine.refreshes import RefreshWrite, record
from etl_craft.services import lineage, validate
from etl_craft.services.doctor import Status
from etl_craft.services.operations.bundles import PublishBundle, publish
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.permissions import pipeline_access
from etl_craft.services.operations.requests import Request
from etl_craft.services.refresh_changes import differences
from etl_craft.services.refresh_models import PipelineRefresh, RefreshFinding
from etl_craft.services.refresh_models import RefreshResults as RefreshResults


@dataclass(frozen=True, kw_only=True)
class RefreshInput(Request):
    """One pipeline, or every pipeline in the selected project."""

    pipeline_code: str | None = None


def history(ctx: OperationContext, request: RefreshInput) -> RefreshResults:
    """Read persisted outcomes without changing the current files or metadata."""
    with ctx.engine.connect() as conn:
        policy = pipeline_access(conn, ctx)
        rows = conn.execute(
            statement(conn, "refresh_history"),
            {"project_id": ctx.project_id, "pipeline_code": request.pipeline_code},
        ).all()
    return RefreshResults(
        tuple(
            PipelineRefresh(
                int(row.refresh_id),
                row.pipeline_code,
                as_utc(row.refreshed_at),
                row.actor,
                RunStatus(row.status),
                row.metadata_version,
                row.bundle_id,
                tuple(RefreshFinding(**finding) for finding in json.loads(row.findings)),
                row.previous_version is None
                or (row.metadata_version, row.bundle_id)
                != (row.previous_version, row.previous_bundle),
                (),
                None if row.previous_time is None else as_utc(row.previous_time),
                None if row.previous_status is None else RunStatus(row.previous_status),
            )
            for row in rows
            if policy.permits(row.pipeline_code)
        )
    )


def refresh(ctx: OperationContext, request: RefreshInput) -> RefreshResults:
    """Publish checked bytes and capture the matching metadata under the config writer lock."""
    assert ctx.project_id is not None
    for_engine(ctx.engine).artifact_location(ctx.config)
    with locks.MIGRATE.hold(ctx.engine):
        return _refresh_locked(ctx, request)


def _refresh_locked(
    ctx: OperationContext, request: RefreshInput, *, missing_only: bool = False
) -> RefreshResults:
    assert ctx.project_id is not None
    outcomes = []
    with read_snapshot(ctx.engine) as conn:
        pipelines = conn.execute(
            statement(conn, "refresh_pipelines"),
            {
                "project_id": ctx.project_id,
                "pipeline_code": request.pipeline_code,
                "missing_only": missing_only,
            },
        ).all()
        if not pipelines:
            if request.pipeline_code is not None:
                raise ResourceNotFoundError(
                    f"pipeline {request.pipeline_code!r}: no active pipeline in project "
                    f"{ctx.project!r}; select an active pipeline shown by `etl-craft pipelines`"
                )
            return RefreshResults(())
        config, bundle_id, bundle_problem = _project_bundle(ctx)
        checked = _project_findings(conn, ctx, config)
        for pipeline in pipelines:
            snapshot = conn.execute(
                statement(conn, "refresh_current"), {"pipeline_id": pipeline.pipeline_id}
            ).scalar_one()
            findings = tuple(
                finding
                for where, finding in checked
                if where.partition(".")[0] == pipeline.pipeline_code or where.startswith("source ")
            )
            if bundle_problem is not None:
                findings = (*findings, bundle_problem)
            with ctx.engine.connect() as published:
                changes, previous_time, previous_status = differences(
                    published, int(pipeline.pipeline_id), snapshot, bundle_id
                )
            passed = not any(finding.failed for finding in findings)
            now = datetime.now(UTC)
            with ctx.engine.begin() as writer:
                refresh_id = record(
                    writer,
                    RefreshWrite(
                        ctx.project_id,
                        int(pipeline.pipeline_id),
                        snapshot,
                        bundle_id,
                        tuple(asdict(finding) for finding in findings),
                        now,
                        passed,
                    ),
                )
            outcomes.append(
                PipelineRefresh(
                    refresh_id,
                    pipeline.pipeline_code,
                    now,
                    ctx.actor.name,
                    RunStatus.SUCCESS if passed else RunStatus.FAILED,
                    sha256_hex(snapshot.encode()),
                    bundle_id,
                    findings,
                    bool(changes) or previous_time is None,
                    changes,
                    previous_time,
                    previous_status,
                )
            )
    return RefreshResults(tuple(outcomes))


def _project_bundle(
    ctx: OperationContext,
) -> tuple[ConnectorConfig, str | None, RefreshFinding | None]:
    """Check the existing publication contract and freeze bytes once per project refresh."""
    bundle_id = None
    config = ctx.config
    bundle_problem = None
    try:
        files = collect(config.project_dir)
        manifest, _ = decode_files(files)
        with ctx.engine.connect() as conn:
            latest = conn.execute(
                statement(conn, "bundle_latest"), {"project_id": ctx.project_id}
            ).scalar_one_or_none()
            previous = None if latest is None else Bundle.load(conn, latest)
        location = for_engine(ctx.engine).artifact_location(config)
        if (
            previous is not None
            and dict(previous.files) == manifest
            and previous.store == location
            and previous.profile == config.artifacts.profile
        ):
            bundle_id = previous.bundle_id
        else:
            bundle_id = publish(ctx, PublishBundle(files=files)).bundle_id
        with ctx.engine.connect() as conn:
            bundle = Bundle.load(conn, bundle_id)
        config = project_config(config, bundle.materialize(config) / "craft-connector.yml")
    except EtlCraftError as error:
        bundle_problem = RefreshFinding(
            str(config.config_path),
            None,
            str(error),
            str(error),
            "Correct the named project file or store and run `etl-craft pipelines refresh`.",
        )
    return config, bundle_id, bundle_problem


def _project_findings(
    conn: Connection,
    ctx: OperationContext,
    config: ConnectorConfig,
) -> tuple[tuple[str, RefreshFinding], ...]:
    """Run shared checks once per project; a pipeline request filters these same findings."""
    report = validate.validate_snapshot(conn, ctx.engine, config)
    found = [(finding.where, _validation_finding(finding)) for finding in report.findings]
    try:
        for task in lineage.collect(conn, config, record=False):
            if task.error is not None:
                found.append(
                    (
                        task.task,
                        RefreshFinding(
                            f"Engine DB: {task.task}",
                            None,
                            task.task,
                            task.error,
                            "Declare source columns or correct the named SQL, then refresh again.",
                            False,
                        ),
                    )
                )
    except EtlCraftError as error:
        found.append(
            (
                "source lineage",
                RefreshFinding(
                    "Engine DB: lineage",
                    None,
                    str(error),
                    str(error),
                    "Correct the named definition and run `etl-craft pipelines refresh`.",
                ),
            )
        )
    return tuple(found)


def _validation_finding(finding: validate.Finding) -> RefreshFinding:
    """Preserve real script locations; database rows have no invented physical line number."""
    source = re.search(r"SCRIPT_FILE=['\"]([^'\"]+)['\"]", finding.message)
    line = re.search(r"\bline (\d+)", finding.message, re.IGNORECASE)
    return RefreshFinding(
        source[1] if source is not None else f"Engine DB: {finding.where}",
        int(line[1]) if line is not None else None,
        finding.message,
        finding.message,
        finding.message.partition("; remedy: ")[2]
        or "Correct the named metadata or project file and run `etl-craft pipelines refresh`.",
        finding.status == Status.FAIL,
    )


def initialize_missing(ctx: OperationContext) -> RefreshResults:
    """Establish upgrade baselines without replacing any seeded successful version."""
    from etl_craft.core.actor import acting_as
    from etl_craft.core.enums import ActionName
    from etl_craft.engine.audit import ActionRequest

    assert ctx.project_id is not None
    for_engine(ctx.engine).artifact_location(ctx.config)
    with acting_as(ctx.actor), locks.MIGRATE.hold(ctx.engine):
        with ctx.engine.connect() as conn:
            missing = conn.execute(
                statement(conn, "refresh_pipelines"),
                {"project_id": ctx.project_id, "pipeline_code": None, "missing_only": True},
            ).first()
        if missing is None:
            return RefreshResults(())
        ActionRequest(
            ActionName.PIPELINE_REFRESH,
            {"upgrade": True},
            datetime.now(UTC),
            actor=ctx.actor,
            engine=ctx.engine,
            project_id=ctx.project_id,
        ).record()
        return _refresh_locked(ctx, RefreshInput(), missing_only=True)
