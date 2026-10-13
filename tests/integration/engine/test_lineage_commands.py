"""``lineage`` and ``docs-version`` on the command line, both Engine DBs."""

import logging

import pytest
import yaml
from sqlalchemy import text

from etl_craft.cli import main
from etl_craft.config import load_config
from etl_craft.core.errors import ExitCode
from etl_craft.services.lineage import collect
from fixtures.metadata import add_pipeline, add_source_column, add_task


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
    profile = engine_db.config.engine
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
        "WHERE $$pipeline_run_id_filter\n",
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
            PIPELINE_RUN_ID_FILTER="true",
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
            SOURCE_SQL="SELECT * FROM sales.orders o JOIN ref.rates r ON o.id = r.id",
        )
        add_task(conn, mart, "cleanup", SQL_ACTION="DROP_TABLE", TARGET_OBJECT="mart.everything")
    with engine.begin() as conn:
        for position, column in enumerate(("id", "amt", "currency"), 1):
            add_source_column(conn, "raw.orders", column, "VARCHAR", position)
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
        "columns (ref.rates, sales.orders); define them in columns.csv or list them",
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


def test_catalog_and_parser_changes_invalidate_stored_lineage(project, monkeypatch):
    from etl_craft.services import lineage

    engine, config = project
    monkeypatch.setattr(lineage, "dialect_and_catalog", lambda config: ("postgres", "one"))
    assert not collect(engine, config)[0].cached
    assert collect(engine, config)[0].cached
    monkeypatch.setattr(lineage, "dialect_and_catalog", lambda config: ("postgres", "two"))
    assert not collect(engine, config)[0].cached
    assert collect(engine, config)[0].cached
    monkeypatch.setattr(lineage, "SQLGLOT_VERSION", "different")
    assert not collect(engine, config)[0].cached


@pytest.fixture
def lineage_clock(monkeypatch):
    from datetime import UTC, datetime

    class Clock:
        instant = datetime(2026, 10, 13, 8, tzinfo=UTC)

        def __new__(cls, *args, **kwargs):
            return datetime(*args, **kwargs)

        @classmethod
        def now(cls, tz=None):
            return cls.instant

    monkeypatch.setattr("etl_craft.engine.repository.lineage.datetime", Clock)
    return Clock


def version_rows(engine):
    with engine.connect() as conn:
        return (
            conn.execute(
                text(
                    "SELECT TARGET_TABLE_NAME AS table_name, VERSION_NUMBER AS version_number, "
                    "VALID_FROM AS valid_from, VALID_TO AS valid_to, CURRENT_FLAG AS current_flag "
                    "FROM AUD_LINEAGE_VERSIONS ORDER BY LINEAGE_VERSION_ID"
                )
            )
            .mappings()
            .all()
        )


def change_stage(engine):
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE CFG_TASK_PARAMETERS SET PARAMETER_VALUE='SELECT id, amt AS new_amount, "
                "currency FROM raw.orders' WHERE PARAMETER_NAME='SOURCE_SQL' AND TASK_ID IN "
                "(SELECT TASK_ID FROM CFG_TASKS WHERE TASK_CODE='stage')"
            )
        )


def test_table_versions_ignore_cache_recomputation_and_close_on_retirement(
    project, lineage_clock, monkeypatch
):
    from datetime import timedelta

    from etl_craft.core.time import as_utc
    from etl_craft.services import lineage

    engine, config = project
    first = collect(engine, config)
    assert first
    assert len(version_rows(engine)) == 3
    monkeypatch.setattr(lineage, "SQLGLOT_VERSION", "changed-parser-version")
    collect(engine, config, refresh=True)
    assert len(version_rows(engine)) == 3
    lineage_clock.instant += timedelta(hours=1)
    change_stage(engine)
    collect(engine, config)
    stages = [r for r in version_rows(engine) if r.table_name == "orders"]
    stage_versions = [r for r in stages if as_utc(r.valid_from).hour == 8 or r.version_number == 2]
    assert any(r.version_number == 2 and r.current_flag == "Y" for r in stage_versions)
    with engine.begin() as conn:
        conn.execute(text("UPDATE CFG_TASKS SET ACTIVE_FLAG='N' WHERE TASK_CODE='stage'"))
    lineage_clock.instant += timedelta(hours=1)
    collect(engine, config)
    with engine.connect() as conn:
        current = conn.execute(
            text(
                "SELECT COUNT(*) AS total FROM AUD_LINEAGE_VERSIONS "
                "WHERE TARGET_SCHEMA_NAME='staging' AND CURRENT_FLAG='Y'"
            )
        ).scalar_one()
    assert current == 0


def test_history_selectors_read_stored_versions_without_computing(
    project, lineage_clock, monkeypatch, capsys
):
    from datetime import timedelta

    engine, config = project
    collect(engine, config)
    first_time = (lineage_clock.instant + timedelta(minutes=30)).isoformat()
    lineage_clock.instant += timedelta(hours=1)
    change_stage(engine)
    collect(engine, config)
    before = version_rows(engine)

    def no_compute(*args, **kwargs):
        raise AssertionError("history must not compute lineage")

    monkeypatch.setattr("etl_craft.services.lineage.resolve_select", no_compute)
    for selector in (
        ("--as-of", first_time),
        ("--as-of", "2026-10-13T08:30:00"),
        ("--as-of", "2026-10-13T14:00:00+05:30"),
        ("--version", "1"),
    ):
        code, out = run(
            capsys, "lineage", "--table", "staging.orders", "--column", "amount", *selector
        )
        assert code == ExitCode.SUCCESS
        assert "raw.orders.amt" in out
        assert "new_amount" not in out
    code, out = run(
        capsys, "lineage", "--table", "staging.orders", "--column", "new_amount", "--version", "2"
    )
    assert code == ExitCode.SUCCESS
    assert "raw.orders.amt" in out
    assert version_rows(engine) == before


@pytest.mark.parametrize(
    "arguments,expected",
    [
        (
            ("--as-of", "2026-10-12T00:00:00Z"),
            "earliest recorded time is 2026-10-13T08:00:00+00:00",
        ),
        (("--version", "1"), "--version needs --table"),
        (("--table", "staging.orders", "--version", "999"), "has no lineage version 999"),
        (("--table", "staging.orders", "--version", "0"), "positive version number"),
        (("--as-of", "2026-10-13T08:00:00Z", "--refresh"), "--refresh cannot read history"),
        (("--as-of", "not-a-time"), "neither a pipeline_run_id nor an ISO 8601 time"),
    ],
)
def test_invalid_lineage_history_selectors_name_the_remedy(
    project, lineage_clock, capsys, arguments, expected
):
    from etl_craft.core.errors import MetadataError, ResourceNotFoundError, UsageError

    engine, config = project
    collect(engine, config)
    code = main(["lineage", *arguments])
    out = capsys.readouterr().err
    assert code in (MetadataError.exit_code, ResourceNotFoundError.exit_code, UsageError.exit_code)
    assert expected in out


def test_concurrent_collectors_publish_one_table_version(project):
    from etl_craft.core.actor import Actor, ActorKind, acting_as
    from fixtures.races import two_at_once

    engine, config = project

    def record():
        with acting_as(Actor("lineage-test", ActorKind.HUMAN)):
            return collect(engine, config, refresh=True)

    results = two_at_once(record, record, at="etl_craft.services.lineage.advisory_key")
    assert all(results)
    rows = version_rows(engine)
    assert len(rows) == 3
    assert all(r.version_number == 1 for r in rows)


def test_catalog_table_history_lists_versions_and_source_changes(project, lineage_clock, tmp_path):
    from datetime import timedelta

    from etl_craft.services.catalog import build_catalog
    from etl_craft.services.catalog_site import table_url, write_site

    engine, config = project
    collect(engine, config)
    lineage_clock.instant += timedelta(hours=1)
    change_stage(engine)
    site = tmp_path / "history-site"
    write_site(build_catalog(engine, config), config, site)
    page = (site / table_url("staging.orders")).read_text()
    assert "Current lineage version 2" in page
    assert "2026-10-13T09:00:00+00:00" in page
    assert "2026-10-13T08:00:00+00:00" in page
    assert "Added: new_amount" in page
    assert "Removed: amount" in page
    source = (site / table_url("raw.orders")).read_text()
    assert "No lineage versions recorded for this table" in source


def test_python_history_selectors_reject_unknown_keys(project):
    from etl_craft.core.actor import Actor, ActorKind
    from etl_craft.core.errors import UsageError
    from etl_craft.services.operations.context import OperationContext
    from etl_craft.services.operations.diagnostics import trace_lineage

    engine, config = project
    ctx = OperationContext(engine, config, Actor("lineage-test", ActorKind.HUMAN))
    with pytest.raises(UsageError, match=r"unknown lineage selectors.*asof.*documented selector"):
        trace_lineage(ctx, asof="2026-10-13T08:00:00Z")


def test_drop_action_closes_history_and_retargeting_starts_a_new_table_version(project):
    from fixtures.metadata import add_table

    engine, config = project
    collect(engine, config)
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE CFG_TASK_PARAMETERS SET PARAMETER_VALUE='drop_table' "
                "WHERE PARAMETER_NAME='SQL_ACTION' AND TASK_ID IN "
                "(SELECT TASK_ID FROM CFG_TASKS WHERE TASK_CODE='stage')"
            )
        )
    collect(engine, config)
    with engine.connect() as conn:
        assert (
            conn.execute(
                text(
                    "SELECT COUNT(*) FROM AUD_LINEAGE_VERSIONS "
                    "WHERE TARGET_SCHEMA_NAME='staging' AND CURRENT_FLAG='Y'"
                )
            ).scalar_one()
            == 0
        )
    with engine.begin() as conn:
        target = add_table(conn, "mart.retargeted")
        conn.execute(
            text(
                "UPDATE CFG_TASK_PARAMETERS SET PARAMETER_VALUE='CREATE_TABLE' "
                "WHERE PARAMETER_NAME='SQL_ACTION' AND TASK_ID IN "
                "(SELECT TASK_ID FROM CFG_TASKS WHERE TASK_CODE='stage')"
            )
        )
        conn.execute(
            text("UPDATE CFG_TASKS SET TARGET_TABLE_ID=:target WHERE TASK_CODE='stage'"),
            {"target": target},
        )
    collect(engine, config)
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT VERSION_NUMBER AS version_number,CURRENT_FLAG AS current_flag "
                "FROM AUD_LINEAGE_VERSIONS WHERE TARGET_TABLE_NAME='retargeted'"
            )
        ).one()
    assert row.version_number == 1 and row.current_flag == "Y"
