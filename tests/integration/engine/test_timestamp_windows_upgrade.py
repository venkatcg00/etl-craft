"""Released positions retain their exact bytes when pending ownership is added."""

from dataclasses import replace

import pytest
from sqlalchemy import inspect, text

from etl_craft.engine import migrations
from etl_craft.engine.repository.offsets import StoredOffset, fetch_task_offset
from fixtures.metadata import add_pipeline
from fixtures.released_schema import SCHEMAS, install, seed_offset


@pytest.mark.parametrize("release", sorted(p.name for p in SCHEMAS.iterdir() if p.is_dir()))
def test_window_upgrade_preserves_positions_and_adds_null_pending_fields(
    empty_engine_db, release, monkeypatch, tmp_path
):
    db = empty_engine_db
    install(db, release)
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv(migrations.MIGRATIONS_DIR_ENV_VAR, raising=False)
    values = {}
    with db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "POSITIONS")
        for kind, value in [
            ("NUMBER", "37.000"),
            ("TEXT", "a:é ' "),
            ("TIMESTAMP", "2026-10-28T01:02:03.123456+00:00"),
        ]:
            task = conn.execute(
                text(
                    "INSERT INTO CFG_TASKS (PIPELINE_ID,TASK_CODE,TASK_TYPE,HANDLER) "
                    "VALUES (:pipeline,:code,'INGESTION','PYTHON') RETURNING TASK_ID"
                ),
                {"pipeline": pipeline, "code": kind},
            ).scalar_one()
            values[task] = StoredOffset(kind, value)
            seed_offset(conn, task, values[task])
    original = migrations.migration_streams

    def before_windows(*args, **kwargs):
        return [
            replace(s, files=tuple(f for f in s.files if f.version < "0071"))
            for s in original(*args, **kwargs)
        ]

    with monkeypatch.context() as checkpoint:
        checkpoint.setattr(migrations, "migration_streams", before_windows)
        migrations.apply_pending_migrations(
            db.engine, project_default=tmp_path / "migrations", warehouse_dialect="duckdb"
        )
    with db.engine.begin() as conn:
        task = conn.execute(
            text(
                "INSERT INTO CFG_TASKS (PIPELINE_ID,TASK_CODE,TASK_TYPE,HANDLER) "
                "VALUES (:pipeline,'PARTITIONS','INGESTION','PYTHON') RETURNING TASK_ID"
            ),
            {"pipeline": pipeline},
        ).scalar_one()
        values[task] = StoredOffset("PARTITIONS", '{ "3":120, "0":17 }')
        seed_offset(conn, task, values[task])
    migrations.apply_pending_migrations(
        db.engine, project_default=tmp_path / "migrations", warehouse_dialect="duckdb"
    )
    with db.engine.connect() as conn:
        for task, expected in values.items():
            found = fetch_task_offset(conn, task)
            assert found == expected
            assert found.pending_task_run_id is None and found.pending_upper_bound is None
        foreign_keys = inspect(conn).get_foreign_keys(
            "aud_task_offset_tracker"
            if db.engine.dialect.name == "postgresql"
            else "AUD_TASK_OFFSET_TRACKER"
        )
        assert any(
            [name.lower() for name in fk["constrained_columns"]] == ["pending_task_run_id"]
            for fk in foreign_keys
        )
