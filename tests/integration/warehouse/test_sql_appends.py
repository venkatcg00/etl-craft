"""Append loads converge after exceptions and process loss on all local warehouses."""

import multiprocessing

import pytest

from etl_craft.core.errors import ConfigurationError, HandlerError
from etl_craft.services.upgrade_targets import upgrade_targets
from etl_craft.warehouse.connection import warehouse_dialect
from fixtures.sql_appends import (
    check_append_retries,
    check_legacy_upgrade,
    create_legacy_append_target,
    run_killed_append,
)


def test_append_retries_replace_only_their_task_run(sql_world):
    check_append_retries(sql_world, "retry_events")


def test_legacy_appends_are_refused_until_upgraded_without_assigning_history(sql_world):
    check_legacy_upgrade(sql_world, "legacy_events")


def test_killed_append_converges_when_retried(sql_world):
    w = sql_world
    w.setup("killed_events", "SELECT CAST(1 AS BIGINT) AS id", "APPEND_TABLE")
    params = {
        "SQL_ACTION": "APPEND_TABLE",
        "TARGET_OBJECT": "killed_events",
        "SOURCE_SQL": "SELECT CAST(1 AS BIGINT) AS id UNION ALL SELECT 2",
    }
    context = w.task("killed_load", **params)
    process = multiprocessing.get_context("spawn").Process(
        target=run_killed_append, args=(context,)
    )
    try:
        process.start()
        process.join(60)
        assert process.exitcode == 137
        result = w.run("killed_load", **params)
        assert (result.insert_count, result.target_count) == (2, 2)
        assert sorted(w.rows(f"SELECT id FROM {w.name('killed_events')}")) == [(1,), (2,)]
    finally:
        if process.is_alive():
            process.terminate()
            process.join(5)


def test_invalid_task_run_id_is_refused_before_replacing_a_batch(sql_world):
    w = sql_world
    dialect = warehouse_dialect(w.config)
    create_legacy_append_target(w, "invalid_load")
    w.execute(
        f"{dialect.alter_table_keyword()} {w.name('invalid_load')} ADD COLUMN TASK_RUN_ID VARCHAR"
    )
    w.task(
        "bad_load",
        SQL_ACTION="APPEND_TABLE",
        TARGET_OBJECT="invalid_load",
        SOURCE_SQL="SELECT 1 AS id",
    )
    for call in (
        lambda: w.run(
            "bad_load",
            SQL_ACTION="APPEND_TABLE",
            TARGET_OBJECT="invalid_load",
            SOURCE_SQL="SELECT 1 AS id",
        ),
        lambda: upgrade_targets(w.engine_db, w.config, action="APPEND_TABLE"),
    ):
        with pytest.raises(HandlerError, match="expected BIGINT"):
            call()
    assert w.rows(f"SELECT COUNT(*) FROM {w.name('invalid_load')}") == [(0,)]


def test_upgrade_requires_an_active_append_writer(sql_world):
    w = sql_world
    assert upgrade_targets(w.engine_db, w.config, action="APPEND_TABLE") == []
    with pytest.raises(HandlerError, match="no active APPEND_TABLE"):
        upgrade_targets(w.engine_db, w.config, action="APPEND_TABLE", target=f"{w.schema}.missing")


def test_empty_retry_removes_its_old_batch(sql_world):
    w = sql_world
    w.setup("empty_retry", "SELECT CAST(1 AS BIGINT) AS id", "APPEND_TABLE")
    params = {"SQL_ACTION": "APPEND_TABLE", "TARGET_OBJECT": "empty_retry"}
    w.run("load", SOURCE_SQL="SELECT CAST(1 AS BIGINT) AS id", **params)
    result = w.run("load", SOURCE_SQL="SELECT CAST(1 AS BIGINT) AS id WHERE 1 = 0", **params)
    assert (result.insert_count, result.target_count, result.rows_written) == (0, 0, 0)


def test_upgrade_deduplicates_qualified_targets_and_refuses_format_conflicts(sql_world):
    w = sql_world
    create_legacy_append_target(w, "shared_load")
    for code, target in (("a", "shared_load"), ("b", w.name("shared_load"))):
        w.task(code, SQL_ACTION="APPEND_TABLE", TARGET_OBJECT=target, SOURCE_SQL="SELECT 1 AS id")
    result = upgrade_targets(w.engine_db, w.config, action="APPEND_TABLE", dry_run=True)
    assert len(result) == 1 and result[0].changed
    # Conflicting contracts are rejected before the warehouse's ALTER statement.
    from etl_craft.core.enums import TableFormat

    opposite = (
        TableFormat.NATIVE
        if w.config.warehouse_table_format == TableFormat.ICEBERG
        else TableFormat.ICEBERG
    )
    w.task(
        "b",
        SQL_ACTION="APPEND_TABLE",
        TARGET_OBJECT=w.name("shared_load"),
        TABLE_FORMAT=opposite,
        SOURCE_SQL="SELECT 1 AS id",
    )
    with pytest.raises(
        (HandlerError, ConfigurationError),
        match=r"different table formats|format is fixed|always native",
    ):
        upgrade_targets(w.engine_db, w.config, action="APPEND_TABLE")
    assert "task_run_id" not in w.columns("shared_load")


def test_upgrade_command_validates_then_adds_and_records_requests(sql_world, capsys):
    import yaml

    from etl_craft.cli import main

    w = sql_world
    create_legacy_append_target(w, "cli_load")
    w.task("load", SQL_ACTION="APPEND_TABLE", TARGET_OBJECT="cli_load", SOURCE_SQL="SELECT 1 AS id")
    profile = w.config.warehouse
    fields = {
        "jdbc_url": profile.jdbc_url,
        "schema": profile.schema,
        "user": profile.user,
        "auth_mode": str(profile.auth_mode),
        **profile.extra,
    }
    if name := fields.pop("secret_var", None):
        fields["secret"] = name
    raw = {
        "Secrets": {"Source_type": "environment"},
        "Orchestration": {"Mode": "local"},
        "Engine": {
            "dev": {"jdbc_url": f"jdbc:sqlite:{w.engine_db.url.database}", "schema": "main"}
        },
        "Warehouse": {
            "Name": {
                "duckdb": "DuckDB",
                "duckdb_iceberg": "DuckDB",
                "postgres": "Postgres",
                "trino_iceberg": "Trino",
            }[w.kind],
            "Table_format": str(w.config.warehouse_table_format),
            "dev": fields,
        },
    }
    path = w.config.config_path
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    args = [
        "upgrade-targets",
        "--config",
        str(path),
        "--action",
        "APPEND_TABLE",
        "--target",
        f"{w.schema}.cli_load",
    ]
    assert main([*args, "--dry-run"]) == 0
    assert "would add nullable PIPELINE_ID BIGINT, TASK_RUN_ID BIGINT" in capsys.readouterr().out
    assert "task_run_id" not in w.columns("cli_load")
    assert main(args) == 0
    assert "historical rows retain NULL" in capsys.readouterr().out
    assert "task_run_id" in w.columns("cli_load")
    assert main(args) == 0
    assert "identity columns already exist" in capsys.readouterr().out
    from sqlalchemy import text

    with w.engine_db.connect() as conn:
        assert (
            conn.execute(
                text("SELECT COUNT(*) FROM AUD_ACTIONS WHERE COMMAND = 'upgrade-targets'")
            ).scalar_one()
            == 3
        )


@pytest.mark.parametrize(
    "action", ["CREATE_TABLE", "OVERWRITE_TABLE", "APPEND_TABLE", "SCD1_MERGE", "SCD2_MERGE"]
)
def test_every_sql_writer_records_pipeline_and_task_run_identities(sql_world, action):
    from etl_craft.handlers import sql

    w = sql_world
    source = "SELECT CAST(1 AS BIGINT) AS id, CAST('a' AS VARCHAR(20)) AS name"
    params = {"SQL_ACTION": action, "TARGET_OBJECT": "identities", "SOURCE_SQL": source}
    if action.startswith("SCD"):
        params.update(MERGE_KEY="id", MERGE_COMPARE_COLUMNS="name")
    if action != "CREATE_TABLE":
        w.setup("identities", source, action)
    first = w.task("first_writer", **params)
    sql.run(first, w.engine_db)
    first_ids = (first.pipeline_id, first.pipeline_run_id, first.task_run_id)
    assert w.rows(
        f"SELECT pipeline_id, pipeline_run_id, task_run_id FROM {w.name('identities')}"
    ) == [first_ids]
    w.new_run()
    params["SOURCE_SQL"] = source.replace("'a'", "'b'")
    second = w.task("second_writer", **params)
    sql.run(second, w.engine_db)
    second_ids = (second.pipeline_id, second.pipeline_run_id, second.task_run_id)
    found = w.rows(f"SELECT pipeline_id, pipeline_run_id, task_run_id FROM {w.name('identities')}")
    expected = (
        [first_ids, second_ids]
        if action == "APPEND_TABLE"
        else [second_ids] * (2 if action == "SCD2_MERGE" else 1)
    )
    assert sorted(found) == sorted(expected)
    if action.startswith("SCD"):
        deleted = w.task(
            "soft_delete",
            SQL_ACTION="DELETE_ROWS",
            TARGET_OBJECT="identities",
            SOURCE_SQL="SELECT 1 AS id",
            MERGE_KEY="id",
        )
        sql.run(deleted, w.engine_db)
        assert set(
            w.rows(f"SELECT pipeline_id, pipeline_run_id, task_run_id FROM {w.name('identities')}")
        ) == {(deleted.pipeline_id, deleted.pipeline_run_id, deleted.task_run_id)}


def test_upgrade_includes_ingestion_targets_without_a_sql_row_id(sql_world):
    from fixtures.sql_appends import check_ingestion_target_upgrade

    check_ingestion_target_upgrade(sql_world, "ingested")


def test_sql_input_identities_match_target_provenance(sql_world):
    from fixtures.sql_appends import check_identity_inputs

    check_identity_inputs(sql_world, "input_identities")
