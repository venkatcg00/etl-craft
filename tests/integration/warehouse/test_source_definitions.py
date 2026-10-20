"""Warehouse drift and repository source seeding on every local warehouse."""

import pytest

from etl_craft.core.enums import SqlAction
from etl_craft.services.catalog.collect import build_catalog
from etl_craft.services.catalog.site import write_site
from etl_craft.services.config_files import export_config, read_config_files, sync_config
from etl_craft.services.source_columns import warehouse_columns
from etl_craft.services.validate import validate
from fixtures.metadata import add_source_column, add_task


@pytest.mark.parametrize("action", [SqlAction.SCD1_MERGE, SqlAction.SCD2_MERGE])
def test_scd_engine_columns_match_the_action_contract(sql_world, action, tmp_path):
    world = sql_world
    select = "SELECT 1 AS id, 2 AS amount"
    world.setup("managed", select, action.lower())
    world.run(
        "writer",
        SQL_ACTION=f" {action.lower()} ",
        TARGET_OBJECT="managed",
        SOURCE_SQL=select,
        MERGE_KEY="id",
        MERGE_COMPARE_COLUMNS="amount",
    )
    catalog = build_catalog(world.engine_db, world.config, with_warehouse=True)
    folder = write_site(catalog, world.config, tmp_path / "site").folder
    assert "Engine managed" in (folder / "tables" / f"{world.schema}.managed.html").read_text()
    from etl_craft.core.enums import GapState
    from etl_craft.services.catalog.gaps import gap_snapshot

    snapshot = gap_snapshot(catalog)
    records = [r for r in snapshot.records if r.engine_managed]
    assert records and all(r.state == GapState.MATCH for r in records), [
        (r.column, r.declared, r.deployed, r.state) for r in records if r.state != GapState.MATCH
    ]
    assert {"created_by", "updated_by", "active_flag", "delete_flag", "hash_key"} <= {
        r.column for r in records
    }


def test_export_seeds_sources_and_validate_names_all_drift(sql_world, tmp_path):
    world = sql_world
    source = f"{world.schema}.source_feed"
    world.execute(
        f"CREATE TABLE {world.name('source_feed')} (id INTEGER, amount INTEGER, removed INTEGER)"
    )
    with world.engine_db.begin() as conn:
        add_task(
            conn,
            world.pipeline_id,
            "source_reader",
            SQL_ACTION="CREATE_TABLE",
            TARGET_OBJECT=f"{world.schema}.output",
            SOURCE_SQL=f"SELECT id FROM {source}",
        )
    folder = tmp_path / "seed"
    result = export_config(world.engine_db, folder, config=world.config)
    assert result.rows["columns.csv"] == 3
    loaded = sync_config(world.engine_db, world.config, read_config_files(folder), apply=True)
    assert not loaded.failed
    assert len([change for change in loaded.changes if change.file == "columns.csv"]) == 3
    export_config(world.engine_db, folder, config=world.config, overwrite=True)
    assert not sync_config(
        world.engine_db, world.config, read_config_files(folder), apply=False
    ).changes
    report = validate(world.engine_db, world.config)
    assert not [finding for finding in report.findings if finding.where.startswith("source ")]
    world.execute(f"ALTER TABLE {world.name('source_feed')} ADD COLUMN extra INTEGER")
    world.execute(f"ALTER TABLE {world.name('source_feed')} DROP COLUMN removed")
    world.execute(
        f"ALTER TABLE {world.name('source_feed')} ALTER COLUMN amount SET DATA TYPE BIGINT"
    )
    findings = [
        finding.message
        for finding in validate(world.engine_db, world.config).findings
        if finding.where == f"source {source}"
    ]
    assert len(findings) == 3
    assert any("adds column extra" in message for message in findings)
    assert any("lacks column removed" in message for message in findings)
    assert any("amount has warehouse type" in message for message in findings)
    catalog = build_catalog(world.engine_db, world.config, with_warehouse=True)
    table = catalog.tables[source]
    assert table.warehouse_checked and table.in_warehouse
    assert len(table.warehouse_findings) == 3
    assert table.columns["amount"].declared_type != table.columns["amount"].warehouse_type
    write_site(catalog, world.config, tmp_path / "catalog")
    html = (tmp_path / "catalog" / "tables" / f"{source}.html").read_text()
    assert "Declared type" in html and "Deployed type" in html
    assert "warehouse adds column extra" in html and "warehouse lacks column removed" in html

    with world.warehouse.connect() as conn:
        assert warehouse_columns(conn, world.config, f"{world.schema}.future_source") == []
    with world.engine_db.begin() as conn:
        add_source_column(conn, f"{world.schema}.future_source", "id", "INT", 1)
    assert not any(
        finding.where.endswith(".future_source")
        for finding in validate(world.engine_db, world.config).findings
    )


def test_catalog_finds_sql_target_gaps_without_flagging_audit_columns(sql_world):
    world = sql_world
    world.run(
        "documented_target",
        SQL_ACTION="CREATE_TABLE",
        TARGET_OBJECT="documented",
        SOURCE_SQL="SELECT 1 AS id, 2 AS removed",
    )
    world.execute(f"DROP TABLE {world.name('documented')}")
    world.execute(
        f"CREATE TABLE {world.name('documented')} "
        "(id INTEGER, extra INTEGER, row_id BIGINT, pipeline_id BIGINT, "
        "pipeline_run_id BIGINT, task_run_id BIGINT)"
    )
    table = build_catalog(world.engine_db, world.config, with_warehouse=True).tables[
        f"{world.schema}.documented"
    ]
    assert len(table.warehouse_findings) == 2
    assert any("lacks documented column removed" in message for message in table.warehouse_findings)
    assert any("adds undocumented column extra" in message for message in table.warehouse_findings)


def test_declared_nullability_and_kind_are_compared_with_deployed_structure(sql_world, tmp_path):
    from sqlalchemy import text

    world = sql_world
    source = f"{world.schema}.nullable_source"
    world.execute(f"CREATE TABLE {world.name('nullable_source')} (id INTEGER)")
    with world.engine_db.begin() as conn:
        add_task(
            conn,
            world.pipeline_id,
            "reader",
            SQL_ACTION="CREATE_TABLE",
            TARGET_OBJECT=f"{world.schema}.output",
            SOURCE_SQL=f"SELECT id FROM {source}",
        )
    folder = tmp_path / "contracts"
    export_config(world.engine_db, folder, config=world.config)
    assert sync_config(world.engine_db, world.config, read_config_files(folder), apply=True).applied
    with world.engine_db.begin() as conn:
        conn.execute(text("UPDATE CFG_COLUMNS SET IS_NULLABLE='N' WHERE COLUMN_NAME='id'"))
        conn.execute(
            text("UPDATE CFG_TABLES SET OBJECT_KIND='VIEW' WHERE TABLE_NAME='nullable_source'")
        )
    findings = [
        finding.message
        for finding in validate(world.engine_db, world.config).findings
        if finding.where == f"source {source}"
    ]
    assert len(findings) == 2
    assert any("nullability Y, declared N" in message for message in findings)
    assert any("object kind TABLE, declared VIEW" in message for message in findings)
    table = build_catalog(world.engine_db, world.config, with_warehouse=True).tables[source]
    assert (table.declared_kind, table.warehouse_kind) == ("VIEW", "TABLE")
    assert (table.columns["id"].declared_nullable, table.columns["id"].warehouse_nullable) == (
        "N",
        "Y",
    )
    assert len(table.warehouse_findings) == 2
