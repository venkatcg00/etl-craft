"""Connecting to the Engine DB."""

from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from etl_craft.config import ConnectorConfig
from etl_craft.core.errors import ConfigurationError
from etl_craft.dialects.engine import build_engine


def engine_db(config: ConnectorConfig, **engine_kwargs: Any) -> Engine:
    """Build a SQLAlchemy engine for the active Engine profile; nothing connects yet."""
    return build_engine(config, **engine_kwargs)


def check_reachable(engine: Engine) -> None:
    """Connect once, raising ``ConfigurationError`` when the Engine DB cannot be reached.

    Every command needs the Engine DB, so an unreachable one is a configuration problem.
    """
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except SQLAlchemyError as error:
        raise ConfigurationError(f"could not connect to the Engine DB: {error}") from error
