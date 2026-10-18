"""Documentation sites, documentation versions and Airflow DAG files, whatever requests them."""

from dataclasses import dataclass
from pathlib import Path

from etl_craft.config import ConnectorConfig
from etl_craft.core.errors import UsageError
from etl_craft.services.catalog import Catalog, build_catalog
from etl_craft.services.catalog_site import Site, write_site
from etl_craft.services.documentation import DocumentationVersion, refresh_versions
from etl_craft.services.generate_yml import docs_dag, global_dag, pipeline_dag, to_yaml
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
    with operation(ctx, "generate-docs", arguments):
        catalog = build_catalog(ctx.engine, ctx.config, with_warehouse=with_warehouse)
        if strict and catalog.untraced:
            return DocsBuild(catalog, None)
        return DocsBuild(catalog, write_site(catalog, ctx.config, docs_folder(ctx.config, output)))


def documentation_versions(
    ctx: OperationContext, *, check: bool = False
) -> list[DocumentationVersion]:
    """Record a new version of each changed task documentation, or only report them."""
    with operation(ctx, "docs-version", {"check": check}), ctx.engine.begin() as conn:
        return refresh_versions(conn, record=not check)


def dag_yaml(
    ctx: OperationContext,
    *,
    pipeline_code: str | None = None,
    global_dag_file: bool = False,
    docs: bool = False,
    airflow_version: str = ">=2.2.0",
) -> str:
    """Render one Airflow DAG file: a pipeline's, the global one, or the documentation DAG."""
    arguments = {"pipeline_code": pipeline_code, "global_dag": global_dag_file, "docs": docs}
    with operation(ctx, "generate-yml", arguments):
        if docs:
            return to_yaml(docs_dag(ctx.config), ctx.config.mode)
        if not global_dag_file and pipeline_code is None:
            raise UsageError("generate-yml: pass a pipeline code, the global DAG or the docs DAG")
        with ctx.engine.connect() as conn:
            dag = (
                global_dag(conn, ctx.config)
                if global_dag_file
                else pipeline_dag(
                    conn, ctx.config, pipeline_code or "", airflow_version=airflow_version
                )
            )
        return to_yaml(dag, ctx.config.mode)
