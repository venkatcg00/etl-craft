"""Materialize a task's admitted bundle without changing its host services or resource limits."""

from dataclasses import replace

from sqlalchemy.engine import Engine

from etl_craft.config import ConnectorConfig
from etl_craft.config.project import project_config
from etl_craft.core.enums import DeploymentProfile
from etl_craft.core.errors import ConfigurationError
from etl_craft.engine.bundles import Bundle
from etl_craft.handlers.registry import TaskContext


def prepare_task(engine: Engine, context: TaskContext) -> TaskContext:
    """Use the working tree for development or the run's exact publication."""
    if context.bundle_id is None:
        if context.config.deployment == DeploymentProfile.CONTROLLER_AGENTS:
            raise ConfigurationError(
                f"run {context.pipeline_run_id}: no pinned bundle for agent "
                f"execution; expected a published project; configure "
                f"Artifacts.Store, publish and start a new run"
            )
        return context
    with engine.connect() as conn:
        bundle = Bundle.load(conn, context.bundle_id)
    root = bundle.materialize(context.config)
    return replace(context, config=project_config(context.config, root / "craft-connector.yml"))


def run_config(engine: Engine, config: ConnectorConfig, run_id: int) -> ConnectorConfig:
    """Schedulers and event probes read project settings from the run's publication."""
    from etl_craft.core.definitions import current
    from etl_craft.engine.queries import statement

    definition = current.get()
    if definition is not None and definition.run_id == run_id:
        bundle_id = definition.bundle_id
    else:
        with engine.connect() as conn:
            bundle_id = (
                conn.execute(statement(conn, "run_definition"), {"run_id": run_id}).one().bundle_id
            )
    if bundle_id is None:
        return config
    with engine.connect() as conn:
        bundle = Bundle.load(conn, bundle_id)
    root = bundle.materialize(config)
    return project_config(config, root / "craft-connector.yml")
