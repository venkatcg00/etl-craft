"""An Engine DB made by an earlier release upgrades to exactly the schema a new one starts with.

``tests/fixtures/schemas/<version>/`` holds each release's packaged schema, as it was tagged.
Each is applied to an empty database, then ``migrate`` runs the packaged migrations; the result
must match a fresh initialization in columns, constraints, indexes and triggers, and retain
representative rows by named column.
"""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import inspect, text
from sqlalchemy.exc import IntegrityError

from etl_craft.dialects.engine import build_engine
from etl_craft.engine.migrations import apply_pending_migrations
from etl_craft.engine.schema import init_db
from fixtures.catalog import snapshot
from fixtures.engine_db import engine_config, sqlite_engine_db
from fixtures.released_schema import install
from fixtures.upgrade_rows import assert_retained, seed

SCHEMAS = Path(__file__).parents[2] / "fixtures" / "schemas"
RELEASES = sorted(path.name for path in SCHEMAS.iterdir() if path.is_dir())


@pytest.mark.parametrize("release", RELEASES)
def test_an_earlier_engine_db_upgrades_to_the_current_schema(
    empty_engine_db, release, monkeypatch, tmp_path
):
    engine = empty_engine_db.engine
    folder = "postgres" if engine.dialect.name == "postgresql" else "sqlite"
    install(empty_engine_db, release)
    monkeypatch.delenv("ETL_CRAFT_MIGRATIONS_DIR", raising=False)
    monkeypatch.chdir(tmp_path)
    retained = seed(engine, tmp_path)

    upgrade_started = datetime.now(UTC)
    apply_pending_migrations(
        engine, project_default=tmp_path / "migrations", warehouse_dialect="duckdb"
    )
    upgrade_ended = datetime.now(UTC)
    with engine.connect() as conn:
        flags = (
            conn.execute(text("SELECT STEWARDABLE_FLAG AS stewardable_flag FROM CFG_PIPELINES"))
            .scalars()
            .all()
        )
        assert flags and all(flag == "N" for flag in flags)
        assert conn.execute(text("SELECT COUNT(*) FROM AUD_CLONE_STATE")).scalar_one() == 0
        assert (
            conn.execute(
                text("SELECT COUNT(*) FROM AUD_PIPELINES_RUN_LOG WHERE UPDATED_AT IS NULL")
            ).scalar_one()
            == 0
        )
    assert {"AUD_TASK_EVENTS", "AUD_EVENT_FILES", "AUD_EVENT_KEYS"} <= {
        name.upper() for name in inspect(engine).get_table_names()
    }
    with engine.connect() as conn:
        columns = conn.execute(text("SELECT * FROM AUD_EVENT_KEYS WHERE 1=0")).keys()
        assert {"task_id", "event_key", "task_run_id", "created_by", "created_at"} <= {
            name.lower() for name in columns
        }

    if folder == "postgres":
        with engine.begin() as conn:
            conn.execute(text("CREATE SCHEMA fresh"))
        fresh = build_engine(engine_config(replace(empty_engine_db.config.engine, schema="fresh")))
    else:
        directory = tmp_path / "fresh"
        directory.mkdir()
        fresh = sqlite_engine_db(directory).engine
    try:
        init_db(fresh)
        upgraded = snapshot(engine)
        for section, expected in snapshot(fresh).items():
            assert upgraded[section] == expected, section
    finally:
        fresh.dispose()
    assert_retained(engine, retained, (upgrade_started, upgrade_ended))
    # The partial unique indexes came along too: one open pause per pipeline.
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE) "
                "VALUES ('P', 'P', 'FULL')"
            )
        )
    insert = text(
        "INSERT INTO AUD_PIPELINE_PAUSES (PIPELINE_ID, PAUSED_BY, REASON) "
        "SELECT PIPELINE_ID, 'op', 'why' FROM CFG_PIPELINES WHERE PIPELINE_CODE = 'P'"
    )
    with engine.begin() as conn:
        conn.execute(insert)
    with pytest.raises(IntegrityError), engine.begin() as conn:
        conn.execute(insert)
    # A second migrate has nothing left to do.
    assert (
        apply_pending_migrations(
            engine, project_default=tmp_path / "migrations", warehouse_dialect="duckdb"
        )
        == []
    )


def test_signal_migration_keeps_existing_file_consumption_and_evidence(engine_db):
    from etl_craft.engine import transitions
    from etl_craft.engine.repository.events import consumed_files, consumed_keys, record_event
    from fixtures.metadata import add_pipeline, add_task, start_run

    db = engine_db
    with db.engine.begin() as conn:
        conn.execute(text("DROP TABLE AUD_EVENT_KEYS"))
        pipeline = add_pipeline(conn, "FILES")
        task = add_task(conn, pipeline, "wait", "EVENT_WAIT", EVENT_FILE="inbox/*")
        run = start_run(conn, pipeline)
        task_run = transitions.find_or_create_task_run(conn, task, run).task_run_id
        evidence = {"files": [{"path": "/kept/batch.csv", "size": 7}]}
        record_event(conn, task, task_run, evidence)
        migration = db.dialect.schema_path().parent / "migrations" / "0049_event_keys.sql"
        for sql in db.dialect.split_statements(migration.read_text()):
            conn.exec_driver_sql(sql)
    with db.engine.connect() as conn:
        assert consumed_files(conn, task) == {"/kept/batch.csv"}
        assert consumed_keys(conn, task, ["any-key"]) == set()
        row = conn.execute(
            text("SELECT EVIDENCE AS evidence FROM AUD_TASK_EVENTS WHERE TASK_RUN_ID=:id"),
            {"id": task_run},
        ).one()
        import json

        assert json.loads(row.evidence) == evidence


@pytest.mark.parametrize("release", RELEASES)
def test_browser_account_migration_retains_real_users_and_sessions(
    empty_engine_db, release, monkeypatch, tmp_path
):
    import json
    import shutil

    from etl_craft.core.actor import SYSTEM_ACTOR
    from etl_craft.core.enums import ActionName, ApiRole
    from etl_craft.dialects.engine import EngineDialect
    from etl_craft.services.operations import OperationContext, tokens
    from fixtures.actions import act

    db = empty_engine_db
    install(db, release)
    directory = db.dialect.migrations_dir()
    before_browser = tmp_path / "before-browser"
    before_browser.mkdir()
    for migration in directory.glob("*.sql"):
        if migration.stem < "0075":
            shutil.copyfile(migration, before_browser / migration.name)
    selected = [before_browser]
    monkeypatch.setattr(EngineDialect, "migrations_dir", lambda self: selected[0])
    apply_pending_migrations(
        db.engine, project_default=tmp_path / "migrations", warehouse_dialect="duckdb"
    )
    ctx = OperationContext(db.engine, db.config, SYSTEM_ACTOR, role=ApiRole.ADMIN)
    act(ctx, ActionName.ACCOUNT_ADD, name="owner", role=ApiRole.ADMIN, password="owner password")
    act(ctx, ActionName.ACCOUNT_ADD, name="alice", role=ApiRole.VIEWER, password="local password")
    with db.engine.connect() as conn:
        before = conn.execute(
            text(
                "SELECT ACCOUNT_ID AS id, PASSWORD_HASH AS hash FROM CFG_ACCOUNTS WHERE "
                "ACCOUNT_NAME='alice'"
            )
        ).one()
    from etl_craft.core.enums import TokenKind

    with db.engine.begin() as conn:
        session = tokens.issue_token(
            conn,
            "alice",
            tokens.TokenGrant(
                before.id,
                "alice",
                TokenKind.ACCESS,
                datetime.now(UTC) + timedelta(days=1),
            ),
        )
    selected[0] = directory
    import sys

    import yaml

    from etl_craft.cli import main
    from etl_craft.core.errors import MigrationError
    from etl_craft.services.operations import processes
    from etl_craft.services.operations.requests import NoInput
    from fixtures.engine_db import profile_block

    newest = max(path.stem for path in directory.glob("[0-9]*.sql"))
    expected = rf"Engine DB .* is at migration 0074.*this etl-craft needs {newest}.*migrate.*admin"
    with pytest.raises(MigrationError, match=expected):
        act(ctx, ActionName.LOGIN, name="owner", password="owner password")
    with pytest.raises(MigrationError, match=expected):
        processes.ui(ctx, NoInput())
    config = tmp_path / "craft-connector.yml"
    config.write_text(
        yaml.safe_dump(
            {
                "Secrets": {"Source_type": "ENVIRONMENT"},
                "Orchestration": {"Project": "default"},
                "Engine": {"test": profile_block(db.config.engine, db.config.engine.schema)},
            },
            sort_keys=False,
        )
    )
    monkeypatch.setenv("ETL_CRAFT_CONFIG", str(config))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "no-session"))
    monkeypatch.delenv("ETL_CRAFT_TOKEN", raising=False)
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    assert main(["login"]) == 13
    assert main(["ui"]) == 13
    monkeypatch.setenv("ETL_CRAFT_TOKEN", session.token)
    for name, password in (("owner", "wrong"), ("alice", "local password")):
        monkeypatch.setattr("builtins.input", lambda _, name=name: name)
        monkeypatch.setattr("getpass.getpass", lambda _, password=password: password)
        assert main(["migrate"]) == 26
    with db.engine.connect() as conn:
        assert (
            conn.execute(
                text(
                    "SELECT COUNT(*) FROM AUD_ACTIONS WHERE COMMAND='migrate' AND OUTCOME='REFUSED'"
                )
            ).scalar_one()
            == 2
        )
    monkeypatch.setattr("builtins.input", lambda _: "owner")
    monkeypatch.setattr("getpass.getpass", lambda _: "owner password")
    assert main(["migrate"]) == 0
    from etl_craft.core.errors import PermissionDeniedError
    from etl_craft.services.operations.upgrade_signin import sign_in_for_upgrade

    with pytest.raises(PermissionDeniedError, match="requires pending Engine migrations"):
        sign_in_for_upgrade(ctx, "owner", "owner password")
    with db.engine.connect() as conn:
        after = conn.execute(
            text(
                "SELECT ACCOUNT_ID AS id, PASSWORD_HASH AS hash, FAILED_SIGNINS AS failed, "
                "LOCKED_DATE AS locked, MUST_CHANGE_PASSWORD AS must_change, "
                "CAST(UI_SETTINGS AS TEXT) AS settings FROM CFG_ACCOUNTS WHERE "
                "ACCOUNT_NAME='alice'"
            )
        ).one()
    assert (after.id, after.hash) == (before.id, before.hash)
    assert after.failed == 0 and after.locked is None and after.must_change == "N"
    assert json.loads(after.settings) == {}
    assert tokens.authenticate(ctx, session.token).name == "alice"
