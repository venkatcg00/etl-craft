"""Each released schema preserves Python identities and scalar/partitioned positions."""

from dataclasses import replace

import pytest
from sqlalchemy import text

from etl_craft.engine import migrations
from etl_craft.engine.repository.offsets import StoredOffset, fetch_task_offset, save_task_offset
from fixtures.metadata import add_pipeline
from fixtures.released_schema import SCHEMAS, install

RELEASES = sorted(path.name for path in SCHEMAS.iterdir() if path.is_dir())


@pytest.mark.parametrize("release", RELEASES)
def test_script_kind_upgrade_preserves_every_python_offset(
    empty_engine_db, release, monkeypatch, tmp_path
):
    db = empty_engine_db
    install(db, release)
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv(migrations.MIGRATIONS_DIR_ENV_VAR, raising=False)
    positions = {}
    with db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "PYTHON_UPGRADE")
        for kind, value in [
            ("NUMBER", "37"),
            ("TEXT", "cursor-9"),
            ("TIMESTAMP", "2026-10-28 01:02:03+00:00"),
        ]:
            task = conn.execute(
                text(
                    "INSERT INTO CFG_TASKS (PIPELINE_ID,TASK_CODE,TASK_TYPE,HANDLER) "
                    "VALUES (:pipeline,:code,'INGESTION','PYTHON') RETURNING TASK_ID"
                ),
                {"pipeline": pipeline, "code": kind},
            ).scalar_one()
            save_task_offset(conn, task, StoredOffset(kind, value))
            positions[task] = StoredOffset(kind, value)
    original = migrations.migration_streams

    def before_kinds(*args, **kwargs):
        return [
            replace(stream, files=tuple(f for f in stream.files if f.version < "0069"))
            for stream in original(*args, **kwargs)
        ]

    with monkeypatch.context() as checkpoint:
        checkpoint.setattr(migrations, "migration_streams", before_kinds)
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
        positions[task] = StoredOffset("PARTITIONS", '{"0":17,"3":120}')
        save_task_offset(conn, task, positions[task])
    migrations.apply_pending_migrations(
        db.engine, project_default=tmp_path / "migrations", warehouse_dialect="duckdb"
    )
    with db.engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT TASK_ID AS task,PARAMETER_VALUE AS kind FROM CFG_TASK_PARAMETERS "
                "WHERE PARAMETER_NAME='SCRIPT_KIND' AND ACTIVE_FLAG='Y'"
            )
        ).all()
        assert {row.task: row.kind for row in rows} == dict.fromkeys(positions, "INGESTION")
        for task, position in positions.items():
            assert fetch_task_offset(conn, task) == position
