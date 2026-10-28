"""Informational warehouse overlap from approved project connection definitions."""

from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy.engine import Engine

from etl_craft.config import ConnectorConfig
from etl_craft.config.project import project_config
from etl_craft.config.targets import active_catalog, parse_warehouse_url
from etl_craft.core.errors import EtlCraftError
from etl_craft.engine.bundles import Bundle
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.pipelines import selected_project


@dataclass(frozen=True)
class WarehouseComparison:
    """Known overlaps and explicit unavailable comparisons, without blocking administration."""

    shared: frozenset[str]
    unavailable: tuple[str, ...]


def shared_warehouses(engine: Engine, config: ConnectorConfig) -> WarehouseComparison:
    """Name projects with identical configured databases/catalogs and schemas; block nothing."""
    groups: dict[tuple[object, ...], list[str]] = defaultdict(list)
    with engine.connect() as conn:
        projects = conn.execute(
            statement(conn, "clone_projects"), {"bootstrap_project": config.project}
        ).all()
        bundles = []
        for project in projects:
            with selected_project(project.project_id):
                identity = conn.execute(statement(conn, "bundle_latest")).scalar_one_or_none()
                bundles.append(
                    (
                        project.project_code,
                        project.project_id,
                        identity,
                    )
                )
    unavailable = []
    for code, project_id, identity in bundles:
        try:
            if identity is None:
                declared = config
            else:
                with engine.connect() as conn, selected_project(project_id):
                    bundle = Bundle.load(conn, identity)
                declared = project_config(
                    config, bundle.materialize(config) / "craft-connector.yml"
                )
        except (EtlCraftError, OSError) as error:
            # This diagnostic reports unavailable evidence; it cannot block admin changes.
            unavailable.append(
                f"project {code}, bundle {identity}: "
                f"warehouse comparison unavailable ({type(error).__name__}); expected a readable "
                "approved bundle and resolvable connection settings; "
                "restore the bundle and host variables"
            )
            continue
        profile = declared.warehouse
        if profile is None:
            continue
        url = parse_warehouse_url(profile.jdbc_url)
        if url.path == ":memory:":
            continue
        database = (
            str((declared.project_dir / Path(url.path)).resolve()) if url.path else url.database
        )
        groups[
            (
                url.dialect.split("+")[0],
                (url.host or "").lower(),
                url.port,
                database,
                active_catalog(declared),
                profile.schema,
            )
        ].append(code)
    return WarehouseComparison(
        frozenset(code for codes in groups.values() if len(codes) > 1 for code in codes),
        tuple(unavailable),
    )
