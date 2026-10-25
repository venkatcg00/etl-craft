"""An archive is verified before source deletion and survives interrupted delivery."""

from dataclasses import replace

import pytest
from sqlalchemy import text

from etl_craft.config import ConnectionProfile
from etl_craft.core.enums import ActionName, AuthMode, RunStatus
from etl_craft.core.errors import ArchiveError
from etl_craft.services.operations.actions import invoke
from etl_craft.services.operations.retention import PurgeInput
from etl_craft.warehouse.connection import build_warehouse_engine
from fixtures.retention import count


@pytest.fixture
def archived(scene, tmp_path):
    ctx, *ids = scene
    from etl_craft.engine.migrations import mark_packaged_migrations_applied

    with ctx.engine.begin() as conn:
        mark_packaged_migrations_applied(conn)
    config = replace(
        ctx.config,
        warehouse=ConnectionProfile(
            "WAREHOUSE",
            "local",
            f"jdbc:duckdb:{tmp_path / 'archive.duckdb'}",
            "",
            AuthMode.NONE,
            schema="main",
        ),
    )
    warehouse = build_warehouse_engine(config)
    with warehouse.begin() as conn:
        conn.execute(text("CREATE SCHEMA history"))
    try:
        yield replace(ctx, config=config), warehouse, ids
    finally:
        warehouse.dispose()


def test_verified_archive_preserves_complete_rows_before_purge(archived):
    ctx, warehouse, ids = archived
    result = invoke(ctx, ActionName.PURGE, PurgeInput(archive_to="history"))
    assert result.counts["AUD_TASK_ATTEMPTS"] == 1
    assert count(ctx, "AUD_TASK_ATTEMPTS") == 0
    assert count(ctx, "AUD_RETENTION_ARCHIVES") == 0
    with warehouse.connect() as conn:
        row = conn.execute(
            text(
                "SELECT ATTEMPT_ID AS attempt_id,STATUS AS status,"
                "_ETL_ARCHIVE_SCHEMA_VERSION AS version "
                "FROM history.AUD_TASK_ATTEMPTS"
            )
        ).one()
    assert row.attempt_id == ids[-1]
    assert row.status == RunStatus.SUCCESS
    assert row.version.endswith(".sql")


def test_archive_dry_run_never_creates_destination_tables(archived):
    ctx, warehouse, _ = archived
    result = invoke(ctx, ActionName.PURGE, PurgeInput(archive_to="history", dry_run=True))
    assert result.counts["AUD_TASK_ATTEMPTS"] == 1
    assert count(ctx, "AUD_TASK_ATTEMPTS") == 1
    with warehouse.connect() as conn:
        assert (
            conn.execute(
                text(
                    "SELECT COUNT(*) AS count FROM information_schema.tables "
                    "WHERE table_schema='history'"
                )
            ).scalar_one()
            == 0
        )


@pytest.mark.parametrize("schema", ["main", "history; DROP SCHEMA main", "catalog.history"])
def test_archive_requires_a_dedicated_schema_identifier(archived, schema):
    ctx, _, _ = archived
    with pytest.raises(ArchiveError, match="expected"):
        invoke(ctx, ActionName.PURGE, PurgeInput(archive_to=schema))
    assert count(ctx, "AUD_TASK_ATTEMPTS") == 1


def test_acknowledgement_loss_reuses_the_prepared_snapshot(archived, monkeypatch):
    from etl_craft.core.errors import InjectedFaultError

    ctx, warehouse, _ = archived
    monkeypatch.setenv("ETL_CRAFT_FAULT", "archive.after_warehouse_commit")
    with pytest.raises(InjectedFaultError):
        invoke(ctx, ActionName.PURGE, PurgeInput(archive_to="history"))
    assert count(ctx, "AUD_TASK_ATTEMPTS") == 1
    assert count(ctx, "AUD_RETENTION_ARCHIVES") == 1
    with pytest.raises(ArchiveError, match="pending archive"):
        invoke(ctx, ActionName.PURGE, PurgeInput())
    monkeypatch.delenv("ETL_CRAFT_FAULT")
    invoke(ctx, ActionName.PURGE, PurgeInput(archive_to="history"))
    assert count(ctx, "AUD_TASK_ATTEMPTS") == 0
    assert count(ctx, "AUD_RETENTION_ARCHIVES") == 0
    with warehouse.connect() as conn:
        assert (
            conn.execute(
                text("SELECT COUNT(*) AS count FROM history.AUD_TASK_ATTEMPTS")
            ).scalar_one()
            == 1
        )


def test_a_changed_source_is_retained_after_archive_verification(archived, monkeypatch):
    from etl_craft.core.errors import InjectedFaultError
    from etl_craft.services.retention import retaining

    ctx, warehouse, ids = archived
    monkeypatch.setenv("ETL_CRAFT_FAULT", "archive.after_verify")
    with pytest.raises(InjectedFaultError):
        invoke(ctx, ActionName.PURGE, PurgeInput(archive_to="history"))
    with retaining(), ctx.engine.begin() as conn:
        conn.execute(
            text("UPDATE AUD_TASK_ATTEMPTS SET TASK_LOG='newer' WHERE ATTEMPT_ID=:id"),
            {"id": ids[-1]},
        )
    monkeypatch.delenv("ETL_CRAFT_FAULT")
    result = invoke(ctx, ActionName.PURGE, PurgeInput(archive_to="history"))
    assert result.counts["retained_changed"] == 1
    assert count(ctx, "AUD_TASK_ATTEMPTS") == 1
    with warehouse.connect() as conn:
        assert (
            conn.execute(text("SELECT TASK_LOG AS log FROM history.AUD_TASK_ATTEMPTS")).scalar_one()
            is None
        )


def test_changed_archive_values_refuse_source_deletion(archived, monkeypatch):
    from etl_craft.core.errors import InjectedFaultError

    ctx, warehouse, _ = archived
    monkeypatch.setenv("ETL_CRAFT_FAULT", "archive.after_warehouse_commit")
    with pytest.raises(InjectedFaultError):
        invoke(ctx, ActionName.PURGE, PurgeInput(archive_to="history"))
    with warehouse.begin() as conn:
        conn.execute(text("UPDATE history.AUD_TASK_ATTEMPTS SET STATUS='FAILED'"))
    monkeypatch.delenv("ETL_CRAFT_FAULT")
    with pytest.raises(ArchiveError, match="changed archive row"):
        invoke(ctx, ActionName.PURGE, PurgeInput(archive_to="history"))
    assert count(ctx, "AUD_TASK_ATTEMPTS") == 1
    assert count(ctx, "AUD_RETENTION_ARCHIVES") == 1


def test_archive_duplicate_identity_refuses_source_deletion(archived, monkeypatch):
    from etl_craft.core.errors import InjectedFaultError

    ctx, warehouse, _ = archived
    monkeypatch.setenv("ETL_CRAFT_FAULT", "archive.after_warehouse_commit")
    with pytest.raises(InjectedFaultError):
        invoke(ctx, ActionName.PURGE, PurgeInput(archive_to="history"))
    with warehouse.begin() as conn:
        conn.execute(
            text("INSERT INTO history.AUD_TASK_ATTEMPTS SELECT * FROM history.AUD_TASK_ATTEMPTS")
        )
    monkeypatch.delenv("ETL_CRAFT_FAULT")
    with pytest.raises(ArchiveError, match="duplicate or changed"):
        invoke(ctx, ActionName.PURGE, PurgeInput(archive_to="history"))
    assert count(ctx, "AUD_TASK_ATTEMPTS") == 1


def test_failed_archive_keeps_file_and_log_content(archived, monkeypatch):
    from etl_craft.config.model import Retention
    from etl_craft.core.errors import InjectedFaultError
    from etl_craft.services.retention import retaining

    ctx, warehouse, ids = archived
    ctx = replace(ctx, config=replace(ctx.config, retention=Retention(attempt_log_days=1)))
    from etl_craft.execution.attempt_records import attempt_log_path

    path = attempt_log_path(ctx.config, "retention", ids[2], "load", 1)
    path.parent.mkdir(parents=True)
    path.write_text("complete log")
    with retaining(), ctx.engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE AUD_TASK_ATTEMPTS SET TASK_LOG='tail',LOG_PATH=:path WHERE ATTEMPT_ID=:id"
            ),
            {"path": str(path), "id": ids[-1]},
        )
        conn.execute(
            text("UPDATE AUD_TASK_RUN_LOG SET TASK_LOG='tail' WHERE TASK_RUN_ID=:id"),
            {"id": ids[-2]},
        )
    with ctx.engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO AUD_ATTEMPT_LOG_CHUNKS "
                "(ATTEMPT_ID,SEQUENCE_NUMBER,CONTENT_SHA256,CONTENT,BYTE_OFFSET,"
                "BYTE_LENGTH,CREATED_AT) VALUES (:id,0,:hash,'shared log',0,10,:old)"
            ),
            {"id": ids[-1], "hash": "0" * 64, "old": "2000-01-01"},
        )
    monkeypatch.setenv("ETL_CRAFT_FAULT", "archive.after_verify")
    with pytest.raises(InjectedFaultError):
        invoke(ctx, ActionName.PURGE, PurgeInput(archive_to="history"))
    assert path.read_text() == "complete log"
    monkeypatch.delenv("ETL_CRAFT_FAULT")
    invoke(ctx, ActionName.PURGE, PurgeInput(archive_to="history"))
    assert not path.exists()
    assert count(ctx, "AUD_ATTEMPT_LOG_CHUNKS") == 0
    with warehouse.connect() as conn:
        assert (
            conn.execute(
                text("SELECT CONTENT AS content FROM history.AUD_ATTEMPT_LOG_CHUNKS")
            ).scalar_one()
            == "shared log"
        )
        assert (
            conn.execute(
                text(
                    "SELECT TASK_LOG AS log FROM history.AUD_TASK_ATTEMPTS "
                    "WHERE TASK_LOG IS NOT NULL"
                )
            ).scalar_one()
            == "tail"
        )


def test_concurrent_archive_purges_share_one_content_identity(archived):
    from etl_craft.engine.locks import EngineLock
    from fixtures.races import two_at_once

    ctx, warehouse, _ = archived
    results = two_at_once(
        lambda: invoke(ctx, ActionName.PURGE, PurgeInput(archive_to="history")),
        lambda: invoke(ctx, ActionName.PURGE, PurgeInput(archive_to="history")),
        at=(EngineLock, "hold"),
    )
    assert sorted(result.counts["AUD_TASK_ATTEMPTS"] for result in results) == [0, 1]
    with warehouse.connect() as conn:
        assert (
            conn.execute(
                text("SELECT COUNT(*) AS count FROM history.AUD_TASK_ATTEMPTS")
            ).scalar_one()
            == 1
        )


def test_pending_archive_refuses_changed_catalog_storage(archived, monkeypatch):
    from etl_craft.core.errors import InjectedFaultError

    ctx, _, _ = archived
    monkeypatch.setenv("ETL_CRAFT_FAULT", "archive.after_prepare")
    with pytest.raises(InjectedFaultError):
        invoke(ctx, ActionName.PURGE, PurgeInput(archive_to="history"))
    monkeypatch.delenv("ETL_CRAFT_FAULT")
    changed = replace(ctx.config.warehouse, extra={"catalog_uri": "https://different.example"})
    ctx = replace(ctx, config=replace(ctx.config, warehouse=changed))
    with pytest.raises(ArchiveError, match="destination differs"):
        invoke(ctx, ActionName.PURGE, PurgeInput(archive_to="history"))
    assert count(ctx, "AUD_TASK_ATTEMPTS") == 1
    assert count(ctx, "AUD_RETENTION_ARCHIVES") == 1


def test_narrowed_archive_column_refuses_source_deletion(archived):
    ctx, warehouse, _ = archived
    with warehouse.begin() as conn:
        conn.execute(
            text(
                "CREATE TABLE history.AUD_TASK_ATTEMPTS (ATTEMPT_ID SMALLINT,"
                "_ETL_ARCHIVE_ENGINE_ID VARCHAR,_ETL_ARCHIVE_ROW_SHA256 VARCHAR,"
                "_ETL_ARCHIVE_SCHEMA_VERSION VARCHAR)"
            )
        )
    with pytest.raises(ArchiveError, match=r"AUD_TASK_ATTEMPTS.ATTEMPT_ID: type=.*expected"):
        invoke(ctx, ActionName.PURGE, PurgeInput(archive_to="history"))
    assert count(ctx, "AUD_TASK_ATTEMPTS") == 1
    assert count(ctx, "AUD_RETENTION_ARCHIVES") == 1


def test_archives_and_auxiliary_cleanup_share_the_warehouse_reservation(archived):
    from datetime import UTC, datetime

    ctx, warehouse, ids = archived
    with ctx.engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO CFG_TABLES (SCHEMA_NAME,TABLE_NAME,OBJECT_KIND,REJECT_RETENTION_DAYS) "
                "VALUES ('main','owned_aux','TABLE',1)"
            )
        )
    with warehouse.begin() as conn:
        conn.execute(text("CREATE TABLE owned_aux (id BIGINT)"))
        conn.execute(text("INSERT INTO owned_aux VALUES (42)"))
        conn.execute(
            text(
                "CREATE TABLE owned_aux__rejects (id BIGINT,PIPELINE_ID BIGINT,"
                "PIPELINE_RUN_ID BIGINT,TASK_RUN_ID BIGINT,CREATE_DATE TIMESTAMPTZ,"
                "CREATED_BY VARCHAR,REJECT_REASON VARCHAR,REJECT_COLUMN VARCHAR)"
            )
        )
        conn.execute(
            text("INSERT INTO owned_aux__rejects VALUES (1,:pipeline,1,1,:old,'test','bad','id')"),
            {"pipeline": ids[0], "old": datetime(2000, 1, 1, tzinfo=UTC)},
        )
    result = invoke(ctx, ActionName.PURGE, PurgeInput(archive_to="history"))
    assert result.counts["archive.main.owned_aux__rejects"] == 1
    assert count(ctx, "AUD_TASK_ATTEMPTS") == 0
    with warehouse.connect() as conn:
        assert conn.execute(text("SELECT id AS id FROM owned_aux")).scalar_one() == 42
        assert (
            conn.execute(text("SELECT COUNT(*) AS count FROM owned_aux__rejects")).scalar_one() == 0
        )
