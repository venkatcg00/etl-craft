"""Publish FastAPI's generated contract without connecting to deployment resources."""

import json
from pathlib import Path

import mkdocs_gen_files
from sqlalchemy import create_engine

from etl_craft.api.app import create_app
from etl_craft.config import parse_config
from etl_craft.core.actor import SYSTEM_ACTOR
from etl_craft.services.operations import OperationContext

config = parse_config(
    {
        "Secrets": {"Source_type": "environment"},
        "Orchestration": {"Mode": "local"},
        "Engine": {"docs": {"jdbc_url": "jdbc:sqlite::memory:", "schema": "main"}},
    },
    Path("craft-connector.yml"),
)
engine = create_engine("sqlite://")
try:
    schema = create_app(OperationContext(engine, config, SYSTEM_ACTOR)).openapi()
    with mkdocs_gen_files.open("reference/openapi.json", "w", encoding="utf-8") as handle:
        json.dump(schema, handle, indent=2)
finally:
    engine.dispose()
