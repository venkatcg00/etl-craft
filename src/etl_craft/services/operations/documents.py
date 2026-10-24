"""Documentation sites and documentation versions, whatever requests them."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar

from etl_craft.config import ConnectorConfig
from etl_craft.core.enums import ActionOutcome
from etl_craft.core.errors import EtlCraftError, ExitCode
from etl_craft.engine.audit import ActionRequest, current_request
from etl_craft.services.catalog.collect import build_catalog
from etl_craft.services.catalog.site import write_site
from etl_craft.services.documentation import DocumentationVersion, refresh_versions
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.permissions import pipeline_access
from etl_craft.services.operations.requests import NoInput, Request

DEFAULT_FOLDER = "catalog"
"""The documentation site's folder in the project when ``Docs_site.Output`` is unset."""


@dataclass(frozen=True, kw_only=True)
class DocsInput(Request):
    """Where to write the site, and how to build it.

    ``with_warehouse`` compares the documented columns with the warehouse; ``strict`` writes
    nothing while a task's columns cannot be traced.
    """

    output: Path | None = None
    strict: bool = False
    with_warehouse: bool = False


@dataclass(frozen=True, kw_only=True)
class PublishInput(Request):
    """Which folder to serve, and where: this machine, ngrok, or ``Docs_site.External_url``."""

    output: Path | None = None
    local_only: bool = False
    host: str = "127.0.0.1"
    port: int = 0
    accept_new_url: bool = False


@dataclass(frozen=True)
class UntracedTask:
    """A SQL task whose columns could not be traced, and why."""

    task: str
    error: str


@dataclass(frozen=True)
class DocsBuild:
    """The site that was written, or nothing when ``strict`` refused; and what the build found."""

    SCHEMA: ClassVar[str] = "etl-craft/docs-build/1"
    folder: Path | None
    pages: int | None
    untraced: tuple[UntracedTask, ...]
    warehouse_findings: tuple[str, ...]


@dataclass(frozen=True)
class DocsVersions:
    """Each task with documentation: its version, and whether this request changed it."""

    SCHEMA: ClassVar[str] = "etl-craft/docs-versions/1"
    versions: tuple[DocumentationVersion, ...]


def docs_folder(config: ConnectorConfig, output: Path | None = None) -> Path:
    """Choose the site folder: the request's, else ``Docs_site.Output``, else ``catalog/``."""
    return output or config.docs_site.output or config.project_dir / DEFAULT_FOLDER


def generate_docs(ctx: OperationContext, request: DocsInput) -> DocsBuild:
    """Build the catalog and write its site; ``strict`` writes nothing while a task is untraced.

    Beside its request, the build records one completion row with its outcome and exit code.
    """
    requested = current_request()
    assert requested is not None
    completion = ActionRequest(
        requested.command,
        requested.arguments,
        requested.requested_at,
        actor=ctx.actor,
        engine=ctx.engine,
        outcome=ActionOutcome.FAILURE,
        project_id=ctx.project_id,
        exit_code=ExitCode.UNEXPECTED,
    )
    try:
        catalog = build_catalog(ctx.engine, ctx.config, with_warehouse=request.with_warehouse)
        untraced = tuple(
            UntracedTask(task.label, task.lineage_error or "") for task in catalog.untraced
        )
        if request.strict and untraced:
            completion.outcome, completion.exit_code = ActionOutcome.FAILURE, ExitCode.FAILURE
            return DocsBuild(None, None, untraced, ())
        site = write_site(catalog, ctx.config, docs_folder(ctx.config, request.output))
        completion.outcome, completion.exit_code = ActionOutcome.SUCCESS, ExitCode.SUCCESS
        findings = tuple(
            f"{table.name}: {finding}"
            for table in catalog.tables.values()
            for finding in table.warehouse_findings
        )
        return DocsBuild(site.folder, site.pages, untraced, findings)
    except EtlCraftError as error:
        completion.exit_code = error.exit_code
        raise
    finally:
        completion.record()


def check_versions(ctx: OperationContext, request: NoInput) -> DocsVersions:
    """Report which task documentation changed, recording nothing."""
    with ctx.engine.begin() as conn:
        access = pipeline_access(conn, ctx)
        return DocsVersions(
            tuple(
                v for v in refresh_versions(conn, record=False) if access.permits(v.pipeline_code)
            )
        )


def record_versions(ctx: OperationContext, request: NoInput) -> DocsVersions:
    """Record a new version of each changed task documentation."""
    with ctx.engine.begin() as conn:
        return DocsVersions(tuple(refresh_versions(conn, record=True)))
