"""Automatic baselines for projects that predate stored pipeline refreshes."""

from sqlalchemy.engine import Engine

from etl_craft.config import ConnectorConfig
from etl_craft.core.actor import SYSTEM_ACTOR
from etl_craft.core.enums import ApiRole
from etl_craft.engine.queries import statement
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.refresh_models import RefreshResults


def initialize(engine: Engine, config: ConnectorConfig) -> RefreshResults:
    """Use this project's explicit connector; never infer another project's files."""
    from etl_craft.services.operations.pipeline_refresh import initialize_missing

    with engine.connect() as conn:
        project = conn.execute(
            statement(conn, "project_by_code"), {"code": config.project}
        ).one_or_none()
    if project is None:
        return RefreshResults(())
    return initialize_missing(
        OperationContext(
            engine,
            config,
            SYSTEM_ACTOR,
            role=ApiRole.ADMIN,
            project=config.project,
            project_id=int(project.project_id),
        )
    )
