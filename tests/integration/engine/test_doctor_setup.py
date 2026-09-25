"""``doctor`` and ``setup``, on both Engine DBs, with a DuckDB warehouse and the Mailpit relay."""

import logging
import sys
from dataclasses import replace

import pytest

from etl_craft.cli import main
from etl_craft.config import ConnectionProfile, ConnectionSection, EmailConfig, EmailProfile
from etl_craft.config.model import SettingSource
from etl_craft.core.enums import Mode
from etl_craft.core.errors import ExitCode
from etl_craft.engine.schema import existing_engine_tables
from etl_craft.services.doctor import Status, run_checks
from etl_craft.services.setup import setup
from fixtures.services import require


@pytest.fixture(autouse=True)
def restore_logger():
    logger = logging.getLogger("etl_craft")
    handlers, level = list(logger.handlers), logger.level
    yield
    logger.handlers[:] = handlers
    logger.setLevel(level)


@pytest.fixture
def config(empty_engine_db, tmp_path, monkeypatch):
    """The empty Engine DB, a DuckDB warehouse file and the Mailpit relay, in ``tmp_path``."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ETL_CRAFT_MIGRATIONS_DIR", raising=False)
    relay = require("mailpit_smtp")
    warehouse = ConnectionProfile(
        "WAREHOUSE", "dev", f"jdbc:duckdb:{tmp_path / 'wh.duckdb'}", "", "none", schema="main"
    )
    email = EmailProfile("EMAIL", "dev", relay.host, relay.port, "etl@example.com", use_tls=False)
    return replace(
        empty_engine_db.config,
        config_path=tmp_path / "craft-connector.yml",
        warehouse=ConnectionSection("dev", {"dev": warehouse}),
        email=EmailConfig("dev", {"dev": email}),
    )


def found(checks):
    return {check.name: (check.status, check.detail) for check in checks}


def failed(checks):
    return [check.name for check in checks if check.status is Status.FAIL]


def test_setup_creates_the_engine_db_then_changes_nothing(config, empty_engine_db):
    checks = found(run_checks(config))
    assert checks["Engine DB tables"] == (
        Status.FAIL,
        "the Engine DB has no etl-craft tables yet: run `etl-craft setup`",
    )
    assert checks["Warehouse connection"][0] is Status.OK
    assert checks["Email relay"][0] is Status.OK

    first = setup(config)
    assert not first.failed and first.created_tables
    assert existing_engine_tables(empty_engine_db.engine)
    assert failed(run_checks(config)) == []
    assert found(run_checks(config))["Engine DB migrations"] == (Status.OK, "up to date")

    again = setup(config)
    assert not again.created_tables and again.applied_migrations == []


def test_a_project_migration_is_pending_until_setup_applies_it(config, tmp_path):
    setup(config)
    (tmp_path / "migrations").mkdir()
    (tmp_path / "migrations" / "0001_owner.sql").write_text(
        "ALTER TABLE CFG_PIPELINES ADD COLUMN OWNER VARCHAR(100);", encoding="utf-8"
    )
    assert found(run_checks(config))["Engine DB migrations"] == (
        Status.FAIL,
        "1 pending: project/0001_owner.sql; run `etl-craft migrate`",
    )
    assert setup(config).applied_migrations == ["0001_owner.sql"]
    assert failed(run_checks(config)) == []


def test_setup_changes_nothing_when_a_check_fails(config, empty_engine_db):
    warehouse = replace(config.warehouse.active, schema="missing")
    broken = replace(config, warehouse=ConnectionSection("dev", {"dev": warehouse}))
    result = setup(broken)
    assert result.failed and failed(result.checks) == ["Warehouse connection"]
    assert "missing" in found(result.checks)["Warehouse connection"][1]
    assert not result.created_tables
    assert existing_engine_tables(empty_engine_db.engine) == []


def test_every_problem_is_reported_not_only_the_first(config):
    email = replace(config.email.active, port=1)
    warehouse = ConnectionProfile("WAREHOUSE", "dev", "jdbc:duckdb:", "", "none", schema="main")
    broken = replace(
        config,
        mode=Mode.REMOTE,
        warehouse=ConnectionSection("dev", {"dev": warehouse}),
        email=EmailConfig("dev", {"dev": email}),
        settings=(SettingSource("Engine.dev.user", "ETL_USER"),),
    )
    checks = run_checks(broken)
    assert failed(checks) == ["Engine DB tables", "Warehouse", "Email relay"]
    assert "an in-memory DuckDB warehouse" in found(checks)["Warehouse"][1]
    warnings = [c for c in checks if c.status is Status.WARN]
    assert warnings[0].detail.startswith("Engine.dev.user is 'ETL_USER': no variable")
    if config.engine.active.jdbc_url.startswith("jdbc:sqlite"):
        assert "in remote mode" in found(checks)["Engine DB kind"][1]


def test_an_engine_db_that_cannot_be_reached_is_one_failed_check(config):
    engine = config.engine.active
    if not engine.jdbc_url.startswith("jdbc:postgresql"):
        engine = replace(engine, jdbc_url="jdbc:sqlite:/nonexistent/dir/engine.db")
    else:
        engine = replace(engine, schema="missing")
    broken = replace(config, engine=ConnectionSection(engine.name, {engine.name: engine}))
    checks = found(run_checks(broken))
    assert [name for name, (status, _) in checks.items() if status is Status.FAIL] == [
        "Engine DB connection"
    ]
    assert ("nonexistent" if "sqlite" in engine.jdbc_url else "missing") in checks[
        "Engine DB connection"
    ][1]


def test_email_secrets_auth_and_sendmail(config, monkeypatch):
    monkeypatch.delenv("ETL_CRAFT_TEST_UNSET_SECRET", raising=False)
    oauth = replace(
        config.email.active,
        auth_mode="oauth",
        user="etl@example.com",
        extra={"secret_var": "ETL_CRAFT_TEST_UNSET_SECRET"},
    )
    checks = found(run_checks(replace(config, email=EmailConfig("dev", {"dev": oauth}))))
    assert checks["Email secret"][0] is Status.FAIL
    assert "ETL_CRAFT_TEST_UNSET_SECRET" in checks["Email secret"][1]
    assert checks["Email auth"][0] is Status.WARN
    assert checks["Email relay"][0] is Status.OK

    program = replace(config.email.active, transport="sendmail", sendmail_path=sys.executable)
    checks = found(run_checks(replace(config, email=EmailConfig("dev", {"dev": program}))))
    assert checks["Email"] == (Status.OK, f"sendmail at {sys.executable}, from etl@example.com")
    missing = replace(program, sendmail_path=str(config.project_dir / "no-sendmail"))
    checks = found(run_checks(replace(config, email=EmailConfig("dev", {"dev": missing}))))
    assert checks["Email"][0] is Status.FAIL and "does not exist" in checks["Email"][1]


@pytest.mark.engine_sqlite
def test_the_commands(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ETL_CRAFT_CONFIG", raising=False)
    monkeypatch.delenv("ETL_CRAFT_MIGRATIONS_DIR", raising=False)
    (tmp_path / "craft-connector.yml").write_text(
        "Secrets:\n  Source_type: environment\nOrchestration:\n  Mode: local\n"
        "Engine:\n  dev:\n    jdbc_url: jdbc:sqlite:engine.db\n    schema: main\n",
        encoding="utf-8",
    )
    assert main(["doctor"]) == ExitCode.FAILURE
    out = capsys.readouterr().out.splitlines()
    assert (
        "[FAIL] Engine DB tables: the Engine DB has no etl-craft tables yet: run `etl-craft setup`"
        in out
    )
    assert out[-1].endswith("1 failed")

    assert main(["setup"]) == ExitCode.SUCCESS
    out = capsys.readouterr().out.splitlines()
    assert out[-1] == "setup: created the Engine DB tables"
    assert main(["setup"]) == ExitCode.SUCCESS
    assert capsys.readouterr().out.splitlines()[-1] == "setup: the Engine DB is already up to date"
    assert main(["doctor"]) == ExitCode.SUCCESS
    assert capsys.readouterr().out.splitlines()[-1].endswith("0 failed")

    (tmp_path / "migrations").mkdir()
    (tmp_path / "migrations" / "0001_owner.sql").write_text(
        "ALTER TABLE CFG_PIPELINES ADD COLUMN OWNER VARCHAR(100);", encoding="utf-8"
    )
    assert main(["setup"]) == ExitCode.SUCCESS
    assert capsys.readouterr().out.splitlines()[-1] == "setup: applied 0001_owner.sql"

    with (tmp_path / "craft-connector.yml").open("a", encoding="utf-8") as config:
        config.write("Warehouse:\n  dev:\n    jdbc_url: 'jdbc:duckdb:'\n    schema: main\n")
    assert main(["setup"]) == ExitCode.FAILURE
    out = capsys.readouterr().out.splitlines()
    assert out[-1] == "setup: nothing changed; fix the failed check(s) and run setup again"
