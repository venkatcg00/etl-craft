"""Export the demo's graphs for contract tests in an isolated Airflow environment."""

from __future__ import annotations

import argparse
import json
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory

from sqlalchemy import text

from etl_craft.config import DagDefaults, DocsSiteConfig, parse_config
from etl_craft.core.enums import Mode
from etl_craft.core.errors import RemoteUnsupportedError
from etl_craft.dialects.engine import for_engine
from etl_craft.engine.connection import engine_db
from etl_craft.engine.queries import run_script
from etl_craft.engine.schema import init_db
from etl_craft.services.generate_yml import docs_dag, global_dag, pipeline_dag, to_yaml


def export_contracts(output: Path) -> None:
    with TemporaryDirectory() as directory:
        config = parse_config(
            {
                "Secrets": {"Source_type": "environment"},
                "Orchestration": {"Mode": "local"},
                "Engine": {"SQLITE": {"jdbc_url": "jdbc:sqlite:engine.db", "schema": "main"}},
            },
            Path(directory) / "craft-connector.yml",
        )
        engine = engine_db(config)
        try:
            init_db(engine)
            with engine.begin() as conn:
                source = (
                    Path(__file__).resolve().parents[1]
                    / "examples/demo/metadata/support_insights.sql"
                )
                run_script(
                    conn, for_engine(engine).split_statements(source.read_text(encoding="utf-8"))
                )
            with engine.connect() as conn:
                codes = conn.execute(
                    text("SELECT PIPELINE_CODE AS code FROM CFG_PIPELINES")
                ).scalars()
                refused = []
                for code in codes:
                    for mode in (Mode.LOCAL, Mode.REMOTE):
                        try:
                            dag = pipeline_dag(conn, replace(config, mode=mode), code)
                        except RemoteUnsupportedError:
                            refused.append(code)
                            continue
                        folder = output / mode.value
                        folder.mkdir(parents=True, exist_ok=True)
                        (folder / f"{code}.yml").write_text(to_yaml(dag, mode), encoding="utf-8")
                (output / "refused.json").write_text(json.dumps(refused), encoding="utf-8")
                folder = output / "other"
                folder.mkdir(parents=True, exist_ok=True)
                dag = global_dag(conn, replace(config, dag_defaults=DagDefaults(global_dag=True)))
                (folder / "global.yml").write_text(to_yaml(dag), encoding="utf-8")
                dag = docs_dag(replace(config, docs_site=DocsSiteConfig(schedule="0 2 * * *")))
                (folder / "docs.yml").write_text(to_yaml(dag), encoding="utf-8")
        finally:
            engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    export_contracts(parser.parse_args().output)
