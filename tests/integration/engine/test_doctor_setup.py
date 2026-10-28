"""``doctor`` and ``setup``, on both Engine DBs, with a DuckDB warehouse and the Mailpit relay."""

import logging
import sys
from dataclasses import replace

import pytest

from etl_craft.cli import main
from etl_craft.config import ConnectionProfile, EmailProfile
from etl_craft.config.model import SettingSource
from etl_craft.config.object_models import Artifacts
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
        "WAREHOUSE", "dev", f"jdbc:duckdb:{tmp_path / 'wh.duckdb'}", "", "NONE", schema="main"
    )
    email = EmailProfile("EMAIL", "dev", relay.host, relay.port, "etl@example.com", tls_mode="NONE")
    return replace(
        empty_engine_db.config,
        config_path=tmp_path / "craft-connector.yml",
        warehouse=warehouse,
        email=email,
        artifacts=Artifacts((tmp_path / "artifacts").as_uri()),
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
    assert failed(run_checks(config)) == ["Active admin"]
    assert found(run_checks(config))["Engine DB migrations"] == (Status.OK, "up to date")

    again = setup(config)
    assert not again.created_tables and again.applied_migrations == []


def test_setup_checks_the_proposed_budget_before_creating_tables(
    config, empty_engine_db, monkeypatch
):
    from etl_craft.dialects.engine import for_engine

    monkeypatch.setattr(for_engine(empty_engine_db.engine), "max_connections", lambda _: 10)
    result = setup(config)
    budget = found(result.checks)["Engine DB connection budget"]
    assert budget[0] == Status.FAIL
    assert "max_connections=10" in budget[1]
    assert "proposed controller" in budget[1]
    assert "expected at most 80%" in budget[1]
    assert "lower Db_pool_size" in budget[1]
    assert not result.created_tables
    assert existing_engine_tables(empty_engine_db.engine) == []


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
    assert failed(run_checks(config)) == ["Active admin"]


def test_setup_changes_nothing_when_a_check_fails(config, empty_engine_db):
    warehouse = replace(config.warehouse, schema="missing")
    broken = replace(config, warehouse=warehouse)
    result = setup(broken)
    assert result.failed and failed(result.checks) == ["Warehouse connection"]
    assert "missing" in found(result.checks)["Warehouse connection"][1]
    assert not result.created_tables
    assert existing_engine_tables(empty_engine_db.engine) == []


def test_every_problem_is_reported_not_only_the_first(config):
    email = replace(config.email, port=1)
    warehouse = ConnectionProfile("WAREHOUSE", "dev", "jdbc:duckdb:", "", "NONE", schema="main")
    broken = replace(
        config,
        warehouse=warehouse,
        email=email,
        settings=(SettingSource("Engine.dev.user", "ETL_USER"),),
    )
    checks = run_checks(broken)
    assert failed(checks) == ["Engine DB tables", "Warehouse", "Email relay"]
    assert "an in-memory DuckDB warehouse" in found(checks)["Warehouse"][1]
    warnings = [c for c in checks if c.status is Status.WARN]
    assert warnings[0].detail.startswith("Engine.dev.user is 'ETL_USER': no variable")


def test_an_engine_db_that_cannot_be_reached_is_one_failed_check(config):
    engine = config.engine
    if not engine.jdbc_url.startswith("jdbc:postgresql"):
        engine = replace(engine, jdbc_url="jdbc:sqlite:/nonexistent/dir/engine.db")
    else:
        engine = replace(engine, schema="missing")
    broken = replace(config, engine=engine)
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
        config.email,
        auth_mode="OAUTH",
        user="etl@example.com",
        extra={"secret_var": "ETL_CRAFT_TEST_UNSET_SECRET"},
    )
    checks = found(run_checks(replace(config, email=oauth)))
    assert checks["Email secret"][0] is Status.FAIL
    assert "ETL_CRAFT_TEST_UNSET_SECRET" in checks["Email secret"][1]
    assert checks["Email auth"][0] is Status.WARN
    assert checks["Email TLS"][0] is Status.FAIL
    assert "Auth_mode=OAUTH" in checks["Email TLS"][1]

    program = replace(config.email, transport="SENDMAIL", sendmail_path=sys.executable)
    checks = found(run_checks(replace(config, email=program)))
    assert checks["Email"] == (Status.OK, f"sendmail at {sys.executable}, from etl@example.com")
    missing = replace(program, sendmail_path=str(config.project_dir / "no-sendmail"))
    checks = found(run_checks(replace(config, email=missing)))
    assert checks["Email"][0] is Status.FAIL and "does not exist" in checks["Email"][1]


@pytest.mark.engine_sqlite
def test_the_commands(tmp_path, monkeypatch, capsys):

    from etl_craft.config import load_config
    from etl_craft.engine.connection import engine_db
    from fixtures.auth import service_token

    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda _: "test-owner")
    monkeypatch.setattr("getpass.getpass", lambda _: "password for test owner")
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ETL_CRAFT_CONFIG", raising=False)
    monkeypatch.delenv("ETL_CRAFT_MIGRATIONS_DIR", raising=False)
    (tmp_path / "craft-connector.yml").write_text(
        "Secrets:\n  Source_type: ENVIRONMENT\nOrchestration:\n"
        "Engine:\n  dev:\n    Jdbc_url: jdbc:sqlite:engine.db\n    Schema: main\n",
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
    signed_config = load_config(tmp_path / "craft-connector.yml")
    signed_engine = engine_db(signed_config)
    try:
        monkeypatch.setenv("ETL_CRAFT_TOKEN", service_token(signed_engine))
    finally:
        signed_engine.dispose()
    out = capsys.readouterr().out.splitlines()
    assert "setup: created the Engine DB tables" in out
    assert out[-1] == "setup: created admin test-owner; run `etl-craft login`"
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
        config.write("Warehouse:\n  dev:\n    Jdbc_url: 'jdbc:duckdb:'\n    Schema: main\n")
    assert main(["setup"]) == ExitCode.FAILURE
    out = capsys.readouterr().out.splitlines()
    assert out[-1] == "setup: nothing changed; fix the failed check(s) and run setup again"


def test_docs_site_publishing_is_only_warned_about(config, monkeypatch):
    from etl_craft.config import DocsSiteConfig

    site = replace(config, docs_site=DocsSiteConfig(authtoken_var="ETL_CRAFT_TEST_NGROK"))
    monkeypatch.delenv("ETL_CRAFT_TEST_NGROK", raising=False)
    checks = found(run_checks(site))
    assert checks["Docs site authtoken"][0] is Status.WARN
    assert checks["Docs site publishing"][0] is Status.WARN
    monkeypatch.setenv("ETL_CRAFT_TEST_NGROK", "secret-value")
    assert found(run_checks(site))["Docs site authtoken"] == (
        Status.OK,
        "resolved from ETL_CRAFT_TEST_NGROK",
    )


def test_relaxed_dependency_gates_are_warned_about(config):
    assert "Dependency gates" not in found(run_checks(config))
    status, detail = found(run_checks(replace(config, dependency_gates="OFF")))["Dependency gates"]
    assert status is Status.WARN
    assert detail.startswith("Orchestration.Dependency_gates is OFF: runs go ahead when")
    assert "(they are not checked at all)" in detail
    warned = found(run_checks(replace(config, dependency_gates="WARN")))["Dependency gates"]
    assert "not checked at all" not in warned[1]


def test_organization_catalog_hosting_needs_no_tunnel_sdk_or_credentials(config, monkeypatch):
    from etl_craft.config import DocsSiteConfig

    monkeypatch.setitem(sys.modules, "ngrok", None)
    monkeypatch.delenv("ETL_CRAFT_TEST_NGROK", raising=False)
    site = replace(
        config,
        docs_site=DocsSiteConfig(
            external_url="https://docs.example.com/catalog/",
            authtoken_var="ETL_CRAFT_TEST_NGROK",
        ),
    )
    checks = found(run_checks(site))
    assert checks["Docs site publishing"] == (
        Status.OK,
        "organization URL https://docs.example.com/catalog/; no ngrok required",
    )
    assert "Docs site authtoken" not in checks


def test_doctor_reports_unset_reject_lifetimes_and_refuses_unsupported_maintenance(
    config, empty_engine_db
):
    from sqlalchemy import text

    result = setup(config)
    assert result.created_tables
    with empty_engine_db.engine.begin() as conn:
        conn.execute(
            text("INSERT INTO CFG_TABLES (SCHEMA_NAME,TABLE_NAME) VALUES ('main','operational')")
        )
    checks = run_checks(config)
    assert any(
        check.name == "Warehouse retention" and "rejects are kept forever" in check.detail
        for check in checks
    )
    with empty_engine_db.engine.begin() as conn:
        conn.execute(text("UPDATE CFG_TABLES SET SNAPSHOT_RETENTION_DAYS=7"))
    checks = run_checks(config)
    assert any(
        check.status is Status.FAIL and "expire_snapshots unsupported" in check.detail
        for check in checks
    )


def test_doctor_probes_the_configured_log_store_before_any_attempt(config, tmp_path):
    from etl_craft.config.object_models import Logs

    root = tmp_path / "log-store"
    root.write_text("not a directory")
    configured = replace(config, logs=Logs(root.as_uri()))
    status, message = found(run_checks(configured, engine_state=False))["Log store"]
    assert status is Status.FAIL
    assert str(root) in message and "expected" in message
    root.unlink()
    assert found(run_checks(configured, engine_state=False))["Log store"][0] is Status.OK
