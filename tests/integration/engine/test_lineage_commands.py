"""``lineage`` and ``docs-version`` on the command line, both Engine DBs."""

import logging

import pytest
import yaml
from sqlalchemy import text

from etl_craft.cli import main
from etl_craft.config import load_config
from etl_craft.core.errors import ExitCode
from etl_craft.services.lineage import collect
from fixtures.metadata import add_pipeline, add_task


@pytest.fixture(autouse=True)
def restore_logger():
    logger = logging.getLogger("etl_craft")
    handlers, level = list(logger.handlers), logger.level
    yield
    logger.handlers[:] = handlers
    logger.setLevel(level)


@pytest.fixture
def project(engine_db, tmp_path, monkeypatch):
    """INGEST stages raw orders, SALES converts them, MART sums them; one task cannot be traced."""
    profile = engine_db.config.engine.active
    schema = "public" if profile.jdbc_url.startswith("jdbc:postgresql") else "main"
    block = {"jdbc_url": profile.jdbc_url, "schema": schema}
    if profile.auth_mode != "none":
        block |= {
            "user": profile.user,
            "auth_mode": profile.auth_mode,
            "secret": profile.secret_var,
        }
    root = (engine_db.config.config_path or tmp_path / "craft-connector.yml").parent
    raw = {
        "Secrets": {"Source_type": "environment"},
        "Orchestration": {"Mode": "local"},
        "Engine": {"dev": block},
        "Warehouse": {"dev": {"jdbc_url": "jdbc:duckdb:analytics.duckdb", "schema": "main"}},
    }
    (root / "craft-connector.yml").write_text(yaml.safe_dump(raw, sort_keys=False), "utf-8")
    (root / "sql_files").mkdir(exist_ok=True)
    (root / "sql_files" / "convert.sql").write_text(
        "SELECT o.id, o.amount * r.rate AS amount_usd\n"
        "FROM analytics.staging.orders o JOIN ref.rates r ON r.currency = o.currency\n"
        "WHERE $$pipeline_id_filter\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(root)
    engine = engine_db.engine
    with engine.begin() as conn:
        ingest = add_pipeline(conn, "INGEST")
        add_task(
            conn,
            ingest,
            "stage",
            SQL_ACTION="CREATE_TABLE",
            TARGET_OBJECT="staging.orders",
            SOURCE_SQL="SELECT id, amt AS amount, currency FROM raw.orders",
        )
        sales = add_pipeline(conn, "SALES")
        add_task(
            conn,
            sales,
            "convert",
            SQL_ACTION="OVERWRITE_TABLE",
            TARGET_OBJECT="sales.orders",
            SOURCE_SQL_FILE="convert.sql",
            PIPELINE_ID_FILTER="true",
            DOCUMENTATION="Converts orders to US dollars.",
        )
        mart = add_pipeline(conn, "MART")
        add_task(
            conn,
            mart,
            "daily",
            SQL_ACTION="OVERWRITE_TABLE",
            TARGET_OBJECT="mart.daily",
            SOURCE_SQL="SELECT SUM(amount_usd) AS total FROM sales.orders",
        )
        add_task(
            conn,
            mart,
            "copy_all",
            SQL_ACTION="CREATE_TABLE",
            TARGET_OBJECT="mart.everything",
            SOURCE_SQL="SELECT * FROM sales.orders",
        )
        add_task(conn, mart, "cleanup", SQL_ACTION="DROP_TABLE", TARGET_OBJECT="mart.everything")
    return engine, load_config(root / "craft-connector.yml")


def run(capsys, *args):
    code = main(list(args))
    return code, capsys.readouterr().out


def test_the_summary_lists_every_task_and_what_cannot_be_traced(project, capsys):
    code, out = run(capsys, "lineage")
    assert code == ExitCode.SUCCESS
    assert out.splitlines() == [
        "TASK\tTARGET\tCOLUMNS\tSOURCE_TABLES",
        "INGEST.stage\tstaging.orders\t3\traw.orders",
        "MART.daily\tmart.daily\t1\tsales.orders",
        "SALES.convert\tsales.orders\t2\tref.rates, staging.orders",
        "not traced: MART.copy_all: SELECT * cannot be traced without knowing the table's "
        "columns; list them",
    ]
    assert main(["lineage", "--strict"]) == ExitCode.FAILURE


def test_a_column_is_traced_to_its_first_sources_and_last_consumers(project, capsys):
    code, out = run(
        capsys, "lineage", "--table", "analytics.sales.orders", "--column", "AMOUNT_USD"
    )
    assert code == ExitCode.SUCCESS
    assert out.splitlines() == [
        "sales.orders.amount_usd",
        "  <- ref.rates.rate  [o.amount * r.rate]  (SALES.convert)",
        "  <- staging.orders.amount  [o.amount * r.rate]  (SALES.convert)",
        "    <- raw.orders.amt  [copy]  (INGEST.stage)",
        "  -> mart.daily.total  [SUM(amount_usd)]  (MART.daily)",
        "(1 SQL task(s) could not be traced; run lineage without --table)",
    ]
    code, out = run(capsys, "lineage", "--table", "raw.orders", "--downstream")
    assert out.splitlines()[:5] == [
        "raw.orders",
        "  downstream:",
        "    staging.orders  (INGEST.stage)",
        "      sales.orders  (SALES.convert)",
        "        mart.daily  (MART.daily)",
    ]


def test_lineage_is_stored_and_worked_out_again_only_when_the_sql_changes(project):
    engine, config = project
    first = {lineage.task: lineage.cached for lineage in collect(engine, config)}
    assert first == {
        "INGEST.stage": False,
        "MART.copy_all": False,
        "MART.daily": False,
        "SALES.convert": False,
    }
    second = {lineage.task: lineage.cached for lineage in collect(engine, config)}
    assert second["INGEST.stage"] and second["SALES.convert"] and not second["MART.copy_all"]
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE CFG_TASK_PARAMETERS SET PARAMETER_VALUE = 'SELECT id FROM raw.orders' "
                "WHERE PARAMETER_NAME = 'SOURCE_SQL' AND PARAMETER_VALUE LIKE '%amt AS amount%'"
            )
        )
    third = {lineage.task: lineage for lineage in collect(engine, config)}
    assert not third["INGEST.stage"].cached
    assert [e.target_column for e in third["INGEST.stage"].edges] == ["id"]


def test_usage_mistakes(project, capsys):
    assert main(["lineage", "--column", "id"]) == ExitCode.USAGE
    assert main(["lineage", "--table", "sales.orders", "--column", "nope"]) == ExitCode.USAGE
    assert "its traced columns: amount_usd, id" in capsys.readouterr().err


def test_docs_version_records_a_version_only_when_the_text_changes(project, capsys):
    engine, _ = project
    assert run(capsys, "docs-version", "--check")[1].splitlines()[1] == "SALES\tconvert\t1\tyes"
    assert run(capsys, "docs-version")[1].splitlines()[1] == "SALES\tconvert\t1\tyes"
    assert run(capsys, "docs-version")[1].splitlines()[1] == "SALES\tconvert\t1\tno"
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE CFG_TASK_PARAMETERS SET PARAMETER_VALUE = 'Converts orders to euros.' "
                "WHERE PARAMETER_NAME = 'DOCUMENTATION'"
            )
        )
    assert run(capsys, "docs-version")[1].splitlines()[1] == "SALES\tconvert\t2\tyes"
