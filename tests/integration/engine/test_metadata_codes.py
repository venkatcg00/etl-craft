"""Metadata code checks on fresh schemas, released schemas and generated commands."""

from dataclasses import replace
from pathlib import Path
from shlex import split

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DataError, IntegrityError

from etl_craft.core.enums import Mode
from etl_craft.core.errors import MetadataError, MigrationError
from etl_craft.dialects.engine import for_engine
from etl_craft.engine.migrations import apply_pending_migrations
from etl_craft.engine.queries import run_script
from etl_craft.services.generate_yml import FINALIZE_TASK, INIT_TASK, pipeline_dag
from fixtures.metadata import add_dependency, add_pipeline, add_task


@pytest.mark.parametrize(
    "code",
    ["__init__", "__finalize__", "load $(touch x); echo", "1load", "a" * 129, "task-name", "P\n"],
)
@pytest.mark.parametrize("object", ["pipeline", "task"])
def test_database_refuses_invalid_metadata_codes(engine_db, code, object):
    with engine_db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
    with pytest.raises(IntegrityError), engine_db.engine.begin() as conn:
        if object == "pipeline":
            add_pipeline(conn, code)
        else:
            add_task(conn, pipeline, code)


@pytest.fixture
def legacy(empty_engine_db):
    engine = empty_engine_db.engine
    kind = "sqlite" if engine.dialect.name == "sqlite" else "postgres"
    schema = Path(__file__).parents[2] / "fixtures" / "schemas" / "0.1.0" / f"{kind}.sql"
    with engine.begin() as conn:
        run_script(conn, for_engine(engine).split_statements(schema.read_text("utf-8")))
    return empty_engine_db


def test_migration_lists_all_invalid_codes_before_changing_anything(legacy, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ETL_CRAFT_MIGRATIONS_DIR", raising=False)
    with legacy.engine.begin() as conn:
        pipeline = add_pipeline(conn, "bad code")
        add_task(conn, pipeline, "__init__")
    with pytest.raises(MigrationError) as caught:
        apply_pending_migrations(legacy.engine)
    message = str(caught.value)
    assert "CFG_PIPELINES.PIPELINE_CODE" in message and "'bad code'" in message
    assert "CFG_TASKS.TASK_CODE" in message and "'__init__'" in message
    assert "rename" in message
    with legacy.engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM SCHEMA_MIGRATIONS")).scalar_one() == 0
    with legacy.engine.begin() as conn:
        conn.execute(text("UPDATE CFG_PIPELINES SET PIPELINE_CODE = 'P'"))
        conn.execute(text("UPDATE CFG_TASKS SET TASK_CODE = 'load'"))
    assert apply_pending_migrations(legacy.engine)
    assert apply_pending_migrations(legacy.engine) == []


@pytest.mark.parametrize("mode", [Mode.LOCAL, Mode.REMOTE])
@pytest.mark.parametrize("object", ["pipeline", "task"])
def test_generation_refuses_legacy_codes_even_without_database_checks(legacy, mode, object):
    with legacy.engine.begin() as conn:
        code = "bad$code" if object == "pipeline" else "P"
        pipeline = add_pipeline(conn, code)
        add_task(conn, pipeline, "__init__" if object == "task" else "load")
    with legacy.engine.connect() as conn, pytest.raises(MetadataError, match="rename"):
        pipeline_dag(conn, replace(legacy.config, mode=mode), code)


@pytest.mark.parametrize("count", [0, 1, 2, 5, 10])
@pytest.mark.parametrize("mode", [Mode.LOCAL, Mode.REMOTE])
def test_generated_dags_have_controls_and_no_self_dependency(engine_db, count, mode):
    with engine_db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        tasks = [add_task(conn, pipeline, f"task_{position}") for position in range(count)]
        for position in range(1, count):
            for upstream in range(position):
                if (position + upstream) % 3 != 0 or upstream == position - 1:
                    add_dependency(conn, pipeline, tasks[position], tasks[upstream])
    with engine_db.engine.connect() as conn:
        dag = pipeline_dag(conn, replace(engine_db.config, mode=mode), "P")
    assert list(dag["tasks"]).count(INIT_TASK) == list(dag["tasks"]).count(FINALIZE_TASK) == 1
    for code, step in dag["tasks"].items():
        assert code not in step["depends_on"]
        arguments = split(step["bash_command"])
        assert arguments[:4] == ["etl-craft", "run", "--pipeline_code", "P"]
        if "--task_code" in arguments:
            assert arguments[arguments.index("--task_code") + 1] == code
        if "--run-date" in arguments:
            assert arguments[arguments.index("--run-date") + 1] == "{{ data_interval_end | ds }}"
            assert arguments[arguments.index("--run-key") + 1] == "orchestrator:{{ run_id }}"


def test_upgrade_preserves_metadata_references_identity_counters_and_custom_index(
    legacy, monkeypatch, tmp_path
):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ETL_CRAFT_MIGRATIONS_DIR", raising=False)
    with legacy.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        task = add_task(conn, pipeline, "load")
        removed_task = add_task(conn, pipeline, "removed")
        removed_pipeline = add_pipeline(conn, "REMOVED")
        conn.execute(text("DELETE FROM CFG_TASKS WHERE TASK_ID = :id"), {"id": removed_task})
        conn.execute(
            text("DELETE FROM CFG_PIPELINES WHERE PIPELINE_ID = :id"), {"id": removed_pipeline}
        )
        run = conn.execute(
            text(
                "INSERT INTO AUD_PIPELINES_RUN_LOG (PIPELINE_ID, STATUS) "
                "VALUES (:id, 'IN-PROGRESS') RETURNING PIPELINE_RUN_ID AS pipeline_run_id"
            ),
            {"id": pipeline},
        ).scalar_one()
        conn.exec_driver_sql("CREATE INDEX custom_tasks_handler ON CFG_TASKS (HANDLER)")
        conn.exec_driver_sql("CREATE VIEW custom_tasks AS SELECT TASK_CODE FROM CFG_TASKS")
    apply_pending_migrations(legacy.engine)
    with legacy.engine.begin() as conn:
        assert (
            conn.execute(
                text("SELECT PIPELINE_ID FROM CFG_TASKS WHERE TASK_ID = :id"), {"id": task}
            ).scalar_one()
            == pipeline
        )
        assert (
            conn.execute(
                text("SELECT PIPELINE_ID FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_RUN_ID = :id"),
                {"id": run},
            ).scalar_one()
            == pipeline
        )
        assert conn.execute(text("SELECT TASK_CODE FROM custom_tasks")).scalar_one() == "load"
        assert add_task(conn, pipeline, "next") > removed_task
        assert add_pipeline(conn, "NEXT") > removed_pipeline
        if conn.dialect.name == "sqlite":
            assert conn.exec_driver_sql("PRAGMA foreign_keys").scalar_one() == 1
            assert conn.exec_driver_sql("PRAGMA foreign_key_check").all() == []
            assert (
                conn.exec_driver_sql(
                    "SELECT name FROM sqlite_schema WHERE name='custom_tasks_handler'"
                ).scalar_one()
                == "custom_tasks_handler"
            )
    with pytest.raises(IntegrityError), legacy.engine.begin() as conn:
        add_task(conn, pipeline, "__finalize__")


def test_custom_metadata_columns_are_preserved_or_refused_before_rebuild(
    legacy, monkeypatch, tmp_path
):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ETL_CRAFT_MIGRATIONS_DIR", raising=False)
    with legacy.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        conn.exec_driver_sql("ALTER TABLE CFG_PIPELINES ADD COLUMN OWNER VARCHAR")
        conn.execute(text("UPDATE CFG_PIPELINES SET OWNER = 'team'"))
    if legacy.engine.dialect.name == "sqlite":
        with pytest.raises(MigrationError, match=r"CFG_PIPELINES.*owner.*would discard"):
            apply_pending_migrations(legacy.engine)
    else:
        apply_pending_migrations(legacy.engine)
    with legacy.engine.connect() as conn:
        assert (
            conn.execute(
                text("SELECT OWNER FROM CFG_PIPELINES WHERE PIPELINE_ID = :id"), {"id": pipeline}
            ).scalar_one()
            == "team"
        )
        if conn.dialect.name == "sqlite":
            assert conn.exec_driver_sql("PRAGMA foreign_keys").scalar_one() == 1
            assert conn.exec_driver_sql("PRAGMA legacy_alter_table").scalar_one() == 0


def test_a_failed_metadata_rebuild_rolls_back_and_restores_connection_settings(
    legacy, monkeypatch, tmp_path
):
    from etl_craft.engine import migrations

    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ETL_CRAFT_MIGRATIONS_DIR", raising=False)
    with legacy.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        add_task(conn, pipeline, "load")
    original = migrations._record

    def refuse_ledger(conn, migration):
        if migration.version == "0005_metadata_codes.sql":
            raise MigrationError("ledger refused")
        original(conn, migration)

    monkeypatch.setattr(migrations, "_record", refuse_ledger)
    with pytest.raises(MigrationError, match="ledger refused"):
        apply_pending_migrations(legacy.engine)
    with legacy.engine.begin() as conn:
        assert conn.execute(text("SELECT TASK_CODE FROM CFG_TASKS")).scalar_one() == "load"
        assert (
            conn.execute(
                text(
                    "SELECT COUNT(*) FROM SCHEMA_MIGRATIONS "
                    "WHERE VERSION = '0005_metadata_codes.sql'"
                )
            ).scalar_one()
            == 0
        )
        add_task(conn, pipeline, "__init__")
        if conn.dialect.name == "sqlite":
            assert conn.exec_driver_sql("PRAGMA foreign_keys").scalar_one() == 1
            assert conn.exec_driver_sql("PRAGMA legacy_alter_table").scalar_one() == 0
    with legacy.engine.begin() as conn:
        conn.execute(text("DELETE FROM CFG_TASKS WHERE TASK_CODE = '__init__'"))
    monkeypatch.setattr(migrations, "_record", original)
    assert apply_pending_migrations(legacy.engine) == [
        "0005_metadata_codes.sql",
        "0006_run_backfill_constraint.sql",
        "0007_identity.sql",
        "0008_actors_and_audit_guards.sql",
        "0009_preserve_request_actors.sql",
        "0010_gate_repairs.sql",
        "0011_target_hash_version.sql",
        "0013_execution_identity_comment.sql",
        "0014_overseers.sql",
        "0015_schedules.sql",
        "0016_gate_waits.sql",
        "0017_retries.sql",
        "0018_api_tokens.sql",
        "0019_target_view_statement.sql",
    ]


def test_database_refuses_embedded_nul_codes(engine_db):
    with engine_db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
    error = DataError if engine_db.engine.dialect.name == "postgresql" else IntegrityError
    with pytest.raises(error), engine_db.engine.begin() as conn:
        add_task(conn, pipeline, "P\x00hidden")
