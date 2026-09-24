"""What every command that reads craft-connector.yml does first."""

from __future__ import annotations

import argparse
import logging

from sqlalchemy.engine import Engine

from etl_craft.config import ConnectorConfig, load_config, resolve_config_path
from etl_craft.engine.connection import check_reachable, engine_db

logger = logging.getLogger(__name__)


def load_command_config(args: argparse.Namespace) -> ConnectorConfig:
    """Find and load craft-connector.yml: ``--config``, ``$ETL_CRAFT_CONFIG``, or a search."""
    path = resolve_config_path(args.config)
    logger.debug("reading %s", path)
    return load_config(path)


def connect_engine_db(config: ConnectorConfig) -> Engine:
    """Build the Engine DB engine and check it can be reached."""
    engine = engine_db(config)
    check_reachable(engine)
    return engine
