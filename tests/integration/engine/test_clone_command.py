"""``etl-craft clone`` from each Engine DB into a DuckDB warehouse, and the checks before it."""

import json
import logging
from dataclasses import replace

import duckdb
import pytest
import yaml
from sqlalchemy import text

from etl_craft.cli import main
from etl_craft.config import CloningConfig, ConnectionProfile, ConnectionSection
from etl_craft.core.enums import CloningScope
from etl_craft.core.errors import ExitCode
from etl_craft.services.cloning import cloning_problem
from etl_craft.services.doctor import Status, run_checks
from fixtures.metadata import add_pipeline


@pytest.fixture(autouse=True)
def restore_logger():
    logger = logging.getLogger("etl_craft")
    handlers, level = list(logger.handlers), logger.level
    yield
    logger.handlers[:] = handlers
    logger.setLevel(level)


def write_config(engine_db, root, cloning):
    profile = engine_db.config.engine.active
    schema = "public" if profile.jdbc_url.startswith("jdbc:postgresql") else "main"
    block = {"jdbc_url": profile.jdbc_url, "schema": schema}
    if profile.auth_mode != "none":
        block |= {
            "user": profile.user,
            "auth_mode": profile.auth_mode,
            "secret": profile.secret_var,
        }
    raw = {
        "Secrets": {"Source_type": "environment"},
        "Orchestration": {"Mode": "local"},
        "Engine": {"dev": block},
        "Warehouse": {"dev": {"jdbc_url": "jdbc:duckdb:analytics.duckdb", "schema": "main"}},
        "Cloning": {"dev": cloning},
    }
    (root / "craft-connector.yml").write_text(yaml.safe_dump(raw, sort_keys=False), "utf-8")


@pytest.fixture
def project(engine_db, tmp_path, monkeypatch):
    root = (engine_db.config.config_path or tmp_path / "craft-connector.yml").parent
    monkeypatch.chdir(root)
    monkeypatch.delenv("ETL_CRAFT_CONFIG", raising=False)
    with engine_db.engine.begin() as conn:
        p = add_pipeline(conn, "SALES")
        conn.execute(
            text("UPDATE CFG_PIPELINES SET PIPELINE_PARAMETERS = :v WHERE PIPELINE_ID = :p"),
            {"p": p, "v": json.dumps({"TAGS": ["sales"]})},
        )
    return root


def test_the_command_copies_the_scope_and_says_what_it_did(engine_db, project, capsys):
    write_config(engine_db, project, {"Enabled": True, "Scope": "cfg"})
    assert main(["clone"]) == ExitCode.SUCCESS
    lines = capsys.readouterr().out.splitlines()
    assert "CFG_PIPELINES\tanalytics.main.CFG_PIPELINES\t1\tcreated" in lines
    assert all(line.startswith("CFG_") for line in lines)
    assert main(["clone"]) == ExitCode.SUCCESS
    assert "CFG_PIPELINES\tanalytics.main.CFG_PIPELINES\t1" in capsys.readouterr().out.splitlines()
    with duckdb.connect(str(project / "analytics.duckdb")) as conn:
        (params,) = conn.execute("SELECT PIPELINE_PARAMETERS FROM main.CFG_PIPELINES").fetchone()
    assert json.loads(params) == {"TAGS": ["sales"]}


@pytest.mark.parametrize(
    "cloning", [{"Enabled": False, "Scope": "all"}, {"Enabled": True, "Scope": "none"}]
)
def test_the_command_says_when_cloning_is_off(engine_db, project, capsys, cloning):
    write_config(engine_db, project, cloning)
    assert main(["clone"]) == ExitCode.CONFIGURATION
    assert "set Cloning.Enabled: true and a Scope of cfg, aud or all" in capsys.readouterr().err


@pytest.mark.engine_postgres
def test_cloning_refuses_the_engine_dbs_own_schema(postgres_database):
    engine = replace(postgres_database.config.engine.active, schema="public")
    warehouse = ConnectionProfile(
        "WAREHOUSE",
        "dev",
        engine.jdbc_url,
        engine.user,
        engine.auth_mode,
        dict(engine.extra),
        schema="PUBLIC",
    )
    config = replace(
        postgres_database.config,
        engine=ConnectionSection("dev", {"dev": engine}),
        warehouse=ConnectionSection("dev", {"dev": warehouse}),
        cloning=CloningConfig(enabled=True, scope=CloningScope.ALL),
    )
    problem = cloning_problem(config)
    assert problem is not None and problem.startswith(
        "the Warehouse schema PUBLIC is the Engine DB's own schema in the same database"
    )
    checks = {c.name: c for c in run_checks(config)}
    assert checks["Cloning"].status is Status.FAIL
    elsewhere = replace(warehouse, schema="mirror")
    assert (
        cloning_problem(replace(config, warehouse=ConnectionSection("dev", {"dev": elsewhere})))
        is None
    )
