"""Documentation sites and documentation versions, whatever requests them."""

from dataclasses import dataclass
from pathlib import Path

from etl_craft.config import ConnectorConfig
from etl_craft.core.enums import ActionOutcome
from etl_craft.core.errors import EtlCraftError, ExitCode
from etl_craft.engine.audit import ActionRequest
from etl_craft.services.catalog import Catalog, build_catalog
from etl_craft.services.catalog_site import Site, write_site
from etl_craft.services.documentation import DocumentationVersion, refresh_versions
from etl_craft.services.operations.context import OperationContext, operation

DEFAULT_FOLDER = "catalog"
"""The documentation site's folder in the project when ``Docs_site.Output`` is unset."""


@dataclass(frozen=True)
class DocsBuild:
    """The catalog that was built, and the site written from it unless ``--strict`` refused."""

    catalog: Catalog
    site: Site | None


def docs_folder(config: ConnectorConfig, output: Path | None = None) -> Path:
    """Choose the site folder: the request's, else ``Docs_site.Output``, else ``catalog/``."""
    return output or config.docs_site.output or config.project_dir / DEFAULT_FOLDER


def generate_docs(
    ctx: OperationContext, *, with_warehouse: bool, output: Path | None, strict: bool
) -> DocsBuild:
    """Build the catalog and write its site; ``strict`` writes nothing while a task is untraced."""
    arguments = {"with_warehouse": with_warehouse, "output": output, "strict": strict}
    with operation(ctx, "generate-docs", arguments) as request:
        assert request is not None
        completion = ActionRequest(
            "generate-docs",
            request.arguments,
            request.requested_at,
            actor=ctx.actor,
            engine=ctx.engine,
            outcome=ActionOutcome.FAILURE,
            exit_code=ExitCode.UNEXPECTED,
        )
        try:
            catalog = build_catalog(ctx.engine, ctx.config, with_warehouse=with_warehouse)
            if strict and catalog.untraced:
                completion.outcome, completion.exit_code = ActionOutcome.FAILURE, ExitCode.FAILURE
                return DocsBuild(catalog, None)
            site = write_site(catalog, ctx.config, docs_folder(ctx.config, output))
            completion.outcome, completion.exit_code = ActionOutcome.SUCCESS, ExitCode.SUCCESS
            return DocsBuild(catalog, site)
        except EtlCraftError as error:
            completion.exit_code = error.exit_code
            raise
        finally:
            completion.record()


def documentation_versions(
    ctx: OperationContext, *, check: bool = False
) -> list[DocumentationVersion]:
    """Record a new version of each changed task documentation, or only report them."""
    with operation(ctx, "docs-version", {"check": check}), ctx.engine.begin() as conn:
        return refresh_versions(conn, record=not check)
