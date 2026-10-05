"""Connecting to the Engine DB."""

from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from etl_craft.config import ConnectorConfig
from etl_craft.core.errors import ConfigurationError
from etl_craft.dialects.engine import build_engine, for_engine
from etl_craft.engine.audit import register_engine


def engine_db(config: ConnectorConfig, **engine_kwargs: Any) -> Engine:
    """Build the active Engine profile and record a scoped command request when present."""
    engine = build_engine(config, **engine_kwargs)
    try:
        register_engine(engine)
    except SQLAlchemyError as error:
        engine.dispose()
        raise ConfigurationError(f"could not connect to the Engine DB: {error}") from error
    return engine


def check_reachable(engine: Engine, schema: str = "") -> None:
    """Connect once; ``ConfigurationError`` when the Engine DB or its ``schema`` is unusable.

    Every command needs the Engine DB, so an unreachable one, or a schema that does not exist,
    is a configuration problem.
    """
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            problem = for_engine(engine).schema_problem(conn, schema) if schema else None
    except SQLAlchemyError as error:
        raise ConfigurationError(f"could not connect to the Engine DB: {error}") from error
    if problem is not None:
        raise ConfigurationError(problem)
