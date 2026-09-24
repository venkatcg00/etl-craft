"""``etl-craft init-db`` and ``etl-craft migrate`` from the command line."""

import logging

import pytest

from etl_craft.cli import main

CONFIG = """\
Secrets:
  Source_type: environment
Orchestration:
  Mode: local
Engine:
  dev:
    jdbc_url: {jdbc_url}
"""


@pytest.fixture(autouse=True)
def restore_logger():
    logger = logging.getLogger("etl_craft")
    handlers, level = list(logger.handlers), logger.level
    yield
    logger.handlers[:] = handlers
    logger.setLevel(level)


@pytest.fixture
def project(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ETL_CRAFT_CONFIG", raising=False)
    monkeypatch.delenv("ETL_CRAFT_MIGRATIONS_DIR", raising=False)
    (tmp_path / "craft-connector.yml").write_text(
        CONFIG.format(jdbc_url="jdbc:sqlite:state/engine.db"), encoding="utf-8"
    )
    return tmp_path


@pytest.mark.engine_sqlite
def test_init_db_then_migrate(project, capsys):
    assert main(["init-db"]) == 0
    assert capsys.readouterr().out.startswith("init-db: applied ")
    assert (project / "state" / "engine.db").is_file()

    assert main(["migrate"]) == 0
    assert capsys.readouterr().out == "migrate: already up to date\n"

    migrations = project / "sql" / "migrations"
    migrations.mkdir(parents=True)
    (migrations / "0001_owner.sql").write_text(
        "ALTER TABLE CFG_PIPELINES ADD COLUMN OWNER VARCHAR;", encoding="utf-8"
    )
    assert main(["migrate"]) == 0
    assert capsys.readouterr().out == "applied 0001_owner.sql\n"


@pytest.mark.engine_sqlite
def test_init_db_twice_is_a_run_failure(project, capsys):
    assert main(["init-db"]) == 0
    capsys.readouterr()
    assert main(["init-db"]) == 1
    assert "already has Engine DB table(s)" in capsys.readouterr().err


@pytest.mark.engine_sqlite
def test_an_explicit_config_and_migrations_dir(tmp_path, capsys, monkeypatch):
    monkeypatch.chdir(tmp_path)
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    config = elsewhere / "craft-connector.yml"
    config.write_text(CONFIG.format(jdbc_url="jdbc:sqlite:engine.db"), encoding="utf-8")
    migrations = tmp_path / "project-migrations"
    migrations.mkdir()
    (migrations / "0001_x.sql").write_text("CREATE TABLE X (ID INT);", encoding="utf-8")
    assert main(["--config", str(config), "init-db"]) == 0
    assert (elsewhere / "engine.db").is_file()
    assert main(["migrate", "--config", str(config), "--migrations-dir", str(migrations)]) == 0
    assert capsys.readouterr().out.endswith("applied 0001_x.sql\n")


@pytest.mark.unit
def test_a_missing_config_is_a_configuration_error(tmp_path, capsys, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ETL_CRAFT_CONFIG", raising=False)
    assert main(["init-db", "--config", str(tmp_path / "nope.yml")]) == 2
    assert "craft-connector.yml not found" in capsys.readouterr().err


@pytest.mark.unit
def test_an_unreachable_engine_db_is_a_configuration_error(tmp_path, capsys, monkeypatch):
    monkeypatch.setenv("ETL_CRAFT_TEST_UNREACHABLE_SECRET", "x")
    config = tmp_path / "craft-connector.yml"
    config.write_text(
        CONFIG.format(jdbc_url="jdbc:postgresql://127.0.0.1:1/etl?connect_timeout=2")
        + "    user: etl\n    auth_mode: password\n    secret: ETL_CRAFT_TEST_UNREACHABLE_SECRET\n",
        encoding="utf-8",
    )
    assert main(["--config", str(config), "migrate"]) == 2
    assert "could not connect to the Engine DB" in capsys.readouterr().err
