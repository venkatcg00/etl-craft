"""Actor attribution, immutable history and ordinary-connection write refusal."""

import json
import logging
import re
import sqlite3
from datetime import UTC, datetime
from uuid import uuid4

import psycopg
import pytest
from sqlalchemy import inspect, text
from sqlalchemy.exc import DBAPIError

from etl_craft.core.actor import Actor, ActorKind, acting_as
from etl_craft.engine import migrations, transitions
from etl_craft.engine.audit import command_request, register_engine
from etl_craft.engine.privileges import extra_write_grants
from fixtures.metadata import add_pipeline, add_pipeline_dependency, add_task
from fixtures.released_schema import install
from fixtures.services import POSTGRES_PASSWORD, POSTGRES_USER, require

ALICE = Actor("alice", ActorKind.HUMAN)


@pytest.fixture(autouse=True)
def restore_logger():
    logger = logging.getLogger("etl_craft")
    handlers, level = list(logger.handlers), logger.level
    yield
    logger.handlers[:] = handlers
    logger.setLevel(level)


def test_attribution_and_immutable_command_requests(engine_db):
    engine = engine_db.engine
    with acting_as(ALICE), engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        run = transitions.create_active_run(conn, pipeline)
        row = conn.execute(
            text("SELECT STARTED_BY, STARTED_BY_KIND FROM AUD_PIPELINES_RUN_LOG")
        ).one()
        assert tuple(row) == ("alice", "HUMAN")
        transitions.finalize_pipeline_run(conn, run, "SUCCESS")
        assert tuple(
            conn.execute(text("SELECT ENDED_BY, ENDED_BY_KIND FROM AUD_PIPELINES_RUN_LOG")).one()
        ) == ("etl-craft", "SYSTEM")
    with acting_as(ALICE), engine.begin() as conn:
        skipped = transitions.create_active_run(conn, pipeline)
        assert transitions.end_run_if(conn, skipped, "IN-PROGRESS", "SKIPPED")
        assert tuple(
            conn.execute(
                text(
                    "SELECT ENDED_BY, ENDED_BY_KIND FROM AUD_PIPELINES_RUN_LOG "
                    "WHERE PIPELINE_RUN_ID=:run"
                ),
                {"run": skipped},
            ).one()
        ) == ("etl-craft", "SYSTEM")
    with acting_as(ALICE), command_request("run", {"pipeline_code": "P", "token": "sensitive"}):
        register_engine(engine)
        register_engine(engine)
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT ACTOR, ACTOR_KIND, OUTCOME, ARGUMENTS FROM AUD_ACTIONS")
        ).one()
        assert tuple(row)[:3] == ("alice", "HUMAN", "REQUESTED")
        assert "sensitive" not in str(row[3])
    for sql in ("UPDATE AUD_ACTIONS SET OUTCOME='SUCCESS'", "DELETE FROM AUD_ACTIONS"):
        with pytest.raises(DBAPIError, match=r"immutable|append-only"), engine.begin() as conn:
            conn.execute(text(sql))
    with acting_as(ALICE), command_request("history", {}):
        register_engine(engine)
    with engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM AUD_ACTIONS")).scalar_one() == 1


def test_project_changes_record_before_after_actor_and_migration(engine_db, tmp_path):
    with engine_db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        task = add_task(conn, pipeline, "load", NOTE="before")
    project = tmp_path / "migrations"
    project.mkdir()
    (project / "0001_edit.sql").write_text(
        f"UPDATE CFG_TASK_PARAMETERS SET PARAMETER_VALUE='after' WHERE TASK_ID={task};\n"
        f"UPDATE CFG_PIPELINES SET ACTIVE_FLAG='N' WHERE PIPELINE_ID={pipeline};\n"
        "ALTER TABLE CFG_PIPELINES ADD COLUMN PROJECT_OWNER VARCHAR;\n"
        f"UPDATE CFG_PIPELINES SET PROJECT_OWNER='team' WHERE PIPELINE_ID={pipeline};\n"
    )
    # Fixture initialization applies the schema without the migration ledger.
    from etl_craft.engine.migrations import mark_packaged_migrations_applied

    with engine_db.engine.begin() as conn:
        mark_packaged_migrations_applied(conn)
    with acting_as(ALICE):
        migrations.apply_pending_migrations(engine_db.engine, project)
    with engine_db.engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT ACTOR, ACTOR_KIND, TABLE_NAME, BEFORE_JSON, AFTER_JSON "
                "FROM AUD_METADATA_CHANGES WHERE MIGRATION='0001_edit.sql' "
                "ORDER BY CHANGE_ID"
            )
        ).all()
        assert len(rows) == 3
        assert all(tuple(row)[:2] == ("alice", "HUMAN") for row in rows)
        objects = [
            (
                a if isinstance(a, dict) else json.loads(a),
                b if isinstance(b, dict) else json.loads(b),
            )
            for _, _, _, a, b in rows
        ]
        assert objects[0][0]["parameter_value"] == "before"
        assert objects[0][1]["parameter_value"] == "after"
        assert objects[1][0]["active_flag"] == "Y"
        assert objects[1][1]["active_flag"] == "N"
        assert objects[2][0]["project_owner"] is None
        assert objects[2][1]["project_owner"] == "team"
        assert objects[2][1]["updated_by"] == "alice"
    with (
        pytest.raises(DBAPIError, match=r"immutable|append-only"),
        engine_db.engine.begin() as conn,
    ):
        conn.execute(text("DELETE FROM AUD_METADATA_CHANGES"))


def seeded_tables(db):
    engine = db.engine
    with engine.begin() as conn:
        p1 = add_pipeline(conn, "DOWN")
        p2 = add_pipeline(conn, "UP")
        t1 = add_task(conn, p1, "load")
        t2 = add_task(conn, p2, "load")
        dependency = add_pipeline_dependency(conn, p1, p2)
        r1 = transitions.create_active_run(conn, p1)
        r2 = transitions.create_active_run(conn, p2)
        task_run = transitions.find_or_create_task_run(conn, t1, r1).task_run_id
        values = {
            "pipeline_id": p1,
            "task_id": t1,
            "depends_on_pipeline_id": p2,
            "depends_on_task_id": t2,
            "pipeline_run_id": r1,
            "task_run_id": task_run,
            "pipeline_dependency_id": dependency,
            "consumed_pipeline_run_id": r2,
            "selected_pipeline_run_id": r2,
            "dependency_type": "SUCCESS",
            "status": "SUCCESS",
            "action": "MARK",
            "requested_by": "alice",
            "paused_by": "alice",
            "reason": "test",
            "business_rule_type": "REPORT",
            "business_rule_sql": "SELECT 1",
            "offset_type": "NUMBER",
            "result": "SATISFIED",
            "actor": "alice",
            "actor_kind": "HUMAN",
            "host": "host",
            "command": "run",
            "arguments": "{}",
            "outcome": "REQUESTED",
            "target_object": "warehouse.s.t",
            "recomputed_at": datetime.now(UTC),
        }
        inspector = inspect(conn)
        schema = None if engine.dialect.name == "sqlite" else inspector.default_schema_name
        declared = re.findall(r"CREATE TABLE (\w+)", db.dialect.schema_path().read_text())
        tables = [
            table if engine.dialect.name == "sqlite" else table.lower()
            for table in declared
            if table.startswith(("CFG_", "AUD_"))
        ]
        for table in tables:
            if conn.exec_driver_sql(f'SELECT 1 FROM "{table}" LIMIT 1').first():
                continue
            payload = {}
            columns = inspector.get_columns(table, schema=schema)
            for column in columns:
                name = column["name"]
                lower = name.lower()
                if (
                    column.get("identity")
                    or column.get("autoincrement")
                    or (column.get("primary_key") and lower not in {"task_id", "target_object"})
                ):
                    continue
                if (
                    column.get("nullable") or column.get("default") is not None
                ) and lower not in values:
                    continue
                value = values.get(lower)
                if value is None:
                    type_name = str(column["type"]).upper()
                    if "TIMESTAMP" in type_name or "DATETIME" in type_name:
                        value = datetime.now(UTC)
                    else:
                        value = 1 if "INT" in type_name or "NUMERIC" in type_name else "sample"
                if table.upper() == "AUD_BUSINESS_RULES_RESULTS" and lower == "status":
                    value = "REPORT"
                payload[name] = value
            if table.upper() != "CFG_BUSINESS_RULES" and "business_rule_id" in [
                c["name"].lower() for c in columns
            ]:
                key = next(c["name"] for c in columns if c["name"].lower() == "business_rule_id")
                payload[key] = conn.execute(
                    text("SELECT BUSINESS_RULE_ID FROM CFG_BUSINESS_RULES")
                ).scalar_one()
            if table.upper() != "AUD_BUSINESS_RULES_RUN_LOG" and "business_rule_run_id" in [
                c["name"].lower() for c in columns
            ]:
                key = next(
                    c["name"] for c in columns if c["name"].lower() == "business_rule_run_id"
                )
                payload[key] = conn.execute(
                    text("SELECT BUSINESS_RULE_RUN_ID FROM AUD_BUSINESS_RULES_RUN_LOG")
                ).scalar_one()
            quoted = ", ".join('"' + name + '"' for name in payload)
            binds = ", ".join(":" + name for name in payload)
            conn.execute(text(f'INSERT INTO "{table}" ({quoted}) VALUES ({binds})'), payload)
        return [(table, inspector.get_columns(table, schema=schema)[1]["name"]) for table in tables]


def test_plain_connections_cannot_mutate_any_protected_table(engine_db):
    tables = seeded_tables(engine_db)
    if engine_db.engine.dialect.name == "sqlite":
        plain = sqlite3.connect(engine_db.engine.url.database)
        expected = r"(?:no such|unknown) function:? etl_craft_actor"
        error = sqlite3.DatabaseError
    else:
        service = require("postgres")
        plain = psycopg.connect(
            host=service.host,
            port=service.port,
            dbname=engine_db.engine.url.database,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            options=f"-c search_path={engine_db.config.engine.active.schema or 'public'}",
        )
        expected = "written only by etl-craft"
        error = psycopg.Error
    try:
        for table, column in tables:
            statements = [
                f'INSERT INTO "{table}" DEFAULT VALUES',
                f'UPDATE "{table}" SET "{column}"="{column}"',
                f'DELETE FROM "{table}"',
            ]
            if engine_db.engine.dialect.name == "postgresql":
                statements.append(f'TRUNCATE "{table}" CASCADE')
            for sql in statements:
                with pytest.raises(error, match=expected):
                    plain.execute(sql)
                plain.rollback()
    finally:
        plain.close()


def test_guarded_upgrade_rolls_back_with_the_ledger(empty_engine_db, monkeypatch):
    from fixtures.catalog import snapshot

    db = empty_engine_db
    install(db, "0.2.0")
    seven = next(
        f
        for f in migrations.migration_streams(db.engine)[0].files
        if f.version == "0007_identity.sql"
    )
    migrations._apply(db.engine, seven)
    with db.engine.begin() as conn:
        add_pipeline(conn, "P")
    before = snapshot(db.engine)
    original = migrations._record

    def refuse(conn, file):
        original(conn, file)
        raise RuntimeError("ledger refused")

    monkeypatch.setattr(migrations, "_record", refuse)
    from etl_craft.core.errors import MigrationError

    with pytest.raises(MigrationError, match="ledger refused"):
        migrations.apply_pending_migrations(db.engine)
    assert snapshot(db.engine) == before
    monkeypatch.setattr(migrations, "_record", original)
    assert migrations.apply_pending_migrations(db.engine) == [
        "0008_actors_and_audit_guards.sql",
        "0009_preserve_request_actors.sql",
        "0010_gate_repairs.sql",
        "0011_target_hash_version.sql",
        "0013_execution_identity_comment.sql",
        "0014_overseers.sql",
        "0015_schedules.sql",
        "0016_gate_waits.sql",
    ]


@pytest.mark.parametrize(
    "empty_engine_db",
    [pytest.param("postgresql", marks=pytest.mark.engine_postgres)],
    indirect=True,
)
def test_postgres_extra_write_grant_is_reported(engine_db):
    role = "etl_craft_test_" + uuid4().hex[:12]
    try:
        with engine_db.engine.begin() as conn:
            conn.exec_driver_sql(f'CREATE ROLE "{role}" LOGIN')
            conn.exec_driver_sql(f'GRANT UPDATE ON CFG_TASKS TO "{role}"')
        assert ("cfg_tasks", role, "UPDATE") in extra_write_grants(engine_db.engine)
        group = role + "_group"
        with engine_db.engine.begin() as conn:
            conn.exec_driver_sql(f'CREATE ROLE "{group}" NOLOGIN')
            conn.exec_driver_sql(f'GRANT "{group}" TO "{role}"')
            conn.exec_driver_sql(f'REVOKE UPDATE ON CFG_TASKS FROM "{role}"')
            conn.exec_driver_sql(f'GRANT UPDATE ON CFG_TASKS TO "{group}"')
        assert ("cfg_tasks", group, "UPDATE") in extra_write_grants(engine_db.engine)
        assert ("cfg_tasks", role, "UPDATE") not in extra_write_grants(engine_db.engine)
        from etl_craft.engine.migrations import mark_packaged_migrations_applied
        from etl_craft.services.doctor import Status, _engine_state

        with engine_db.engine.begin() as conn:
            mark_packaged_migrations_applied(conn)
        checks = _engine_state(engine_db.config, engine_db.engine)
        finding = next(
            check
            for check in checks
            if check.status == Status.FAIL
            and group in check.detail
            and "REVOKE UPDATE" in check.detail
        )
        revoke = finding.detail.split("; ", 1)[1]
        assert f'FROM "{group}"' in revoke
        with engine_db.engine.begin() as conn:
            conn.exec_driver_sql(revoke)
        assert ("cfg_tasks", group, "UPDATE") not in extra_write_grants(engine_db.engine)

    finally:
        with engine_db.engine.begin() as conn:
            conn.exec_driver_sql(f'DROP OWNED BY "{role}"')
            if conn.execute(
                text("SELECT 1 FROM pg_roles WHERE rolname=:group"), {"group": role + "_group"}
            ).first():
                conn.exec_driver_sql(f'DROP OWNED BY "{role}_group"')
                conn.exec_driver_sql(f'DROP ROLE "{role}_group"')
            conn.exec_driver_sql(f'DROP ROLE "{role}"')


@pytest.mark.parametrize("override", [True, False])
def test_cli_actor_requests_and_read_only_audit(engine_db, tmp_path, monkeypatch, capsys, override):
    import yaml

    from etl_craft.cli import main

    profile = engine_db.config.engine.active
    jdbc = profile.jdbc_url
    if profile.auth_mode == "none":
        jdbc = "jdbc:sqlite:" + engine_db.engine.url.database
    metadata = {
        "Secrets": {"Source_type": "environment"},
        "Orchestration": {"Mode": "local"},
        "Engine": {
            "test": {
                "jdbc_url": jdbc,
                "user": profile.user,
                "auth_mode": str(profile.auth_mode),
                "schema": profile.schema or ("main" if profile.auth_mode == "none" else "public"),
                **(
                    {"secret": profile.extra["secret_var"]} if "secret_var" in profile.extra else {}
                ),
            }
        },
    }
    config = tmp_path / "craft-connector.yml"
    config.write_text(yaml.safe_dump(metadata, sort_keys=False))
    with engine_db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        deleted = add_task(conn, pipeline, "deleted", NOTE="deleted_parameter_history")
        unrelated = add_pipeline(conn, "OTHER")
        add_task(conn, unrelated, "other", NOTE="unrelated_parameter_history")
        conn.execute(text("DELETE FROM CFG_TASK_PARAMETERS WHERE TASK_ID=:task"), {"task": deleted})
        conn.execute(text("DELETE FROM CFG_TASKS WHERE TASK_ID=:task"), {"task": deleted})
    actor_name = "alice" if override else "alice@laptop"
    if override:
        monkeypatch.setenv("ETL_CRAFT_ACTOR", actor_name)
    else:
        monkeypatch.delenv("ETL_CRAFT_ACTOR", raising=False)
        monkeypatch.setattr("getpass.getuser", lambda: "alice")
        monkeypatch.setattr("socket.gethostname", lambda: "laptop")
    monkeypatch.delenv("ETL_CRAFT_ACTOR_KIND", raising=False)

    def command(*argv):
        result = main(["--config", str(config), *argv])
        assert result == 0, capsys.readouterr()

    command("run", "--pipeline_code", "P", "--init-only")
    with engine_db.engine.connect() as conn:
        run_id = conn.execute(
            text("SELECT PIPELINE_RUN_ID FROM AUD_PIPELINES_RUN_LOG")
        ).scalar_one()
    command("pause", "--pipeline_code", "P", "--reason", "wait")
    command("resume", "--pipeline_code", "P", "--reason", "ready")
    command("cancel", "--pipeline_code", "P", "--reason", "stop")
    command(
        "mark",
        "--pipeline_code",
        "P",
        "--run-id",
        str(run_id),
        "--status",
        "SUCCESS",
        "--reason",
        "verified",
    )
    capsys.readouterr()
    command("audit", "--pipeline_code", "P", "--since", "2026-01-01")
    shown = capsys.readouterr().out
    assert actor_name in shown and "HUMAN" in shown and "REQUESTED" in shown
    assert "deleted_parameter_history" in shown
    assert "unrelated_parameter_history" not in shown
    command("setup", "--print-grants")
    assert "file permissions" in capsys.readouterr().out if profile.auth_mode == "none" else True
    command("history", "--pipeline_code", "P", "--run-id", str(run_id))
    assert "STARTED_BY" in capsys.readouterr().out
    with engine_db.engine.connect() as conn:
        actions = conn.execute(
            text("SELECT COMMAND, ACTOR, ACTOR_KIND FROM AUD_ACTIONS ORDER BY ACTION_ID")
        ).all()
        assert [tuple(row) for row in actions] == [
            (name, actor_name, "HUMAN") for name in ("run", "pause", "resume", "cancel", "mark")
        ]
        row = conn.execute(
            text(
                "SELECT STARTED_BY, STARTED_BY_KIND, ENDED_BY, ENDED_BY_KIND "
                "FROM AUD_PIPELINES_RUN_LOG"
            )
        ).one()
        assert tuple(row) == (actor_name, "HUMAN", actor_name, "HUMAN")
        row = conn.execute(
            text("SELECT PAUSED_BY_KIND, RESUMED_BY_KIND FROM AUD_PIPELINE_PAUSES")
        ).one()
        assert tuple(row) == ("HUMAN", "HUMAN")
        assert set(
            conn.execute(text("SELECT REQUESTED_BY_KIND FROM AUD_RUN_INTERVENTIONS")).scalars()
        ) == {"HUMAN"}

    monkeypatch.setenv("ETL_CRAFT_ACTOR", "")
    assert main(["--config", str(config), "run", "--pipeline_code", "P", "--init-only"]) == 3
    assert "ETL_CRAFT_ACTOR" in capsys.readouterr().err
    with engine_db.engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM AUD_ACTIONS")).scalar_one() == 5


def test_project_created_tables_keep_guards_and_capture(engine_db, tmp_path):
    project = tmp_path / "migrations"
    project.mkdir()
    (project / "0001_tables.sql").write_text(
        "CREATE TABLE CFG_PROJECT (ID BIGINT PRIMARY KEY, VALUE VARCHAR);\n"
        "INSERT INTO CFG_PROJECT VALUES (1, 'first');\n"
        "UPDATE CFG_PROJECT SET VALUE='second' WHERE ID=1;\n"
        "CREATE TABLE AUD_PROJECT (ID BIGINT PRIMARY KEY, VALUE VARCHAR);\n"
        "INSERT INTO AUD_PROJECT VALUES (1, 'audit');\n"
    )
    with engine_db.engine.begin() as conn:
        migrations.mark_packaged_migrations_applied(conn)
    with acting_as(ALICE):
        migrations.apply_pending_migrations(engine_db.engine, project)
    with engine_db.engine.connect() as conn:
        changes = conn.execute(
            text(
                "SELECT ACTOR, MIGRATION, OPERATION, AFTER_JSON FROM AUD_METADATA_CHANGES "
                "WHERE TABLE_NAME='CFG_PROJECT' ORDER BY CHANGE_ID"
            )
        ).all()
        assert [tuple(row)[:3] for row in changes] == [
            ("alice", "0001_tables.sql", "INSERT"),
            ("alice", "0001_tables.sql", "UPDATE"),
        ]
        after = changes[-1][3]
        assert (after if isinstance(after, dict) else json.loads(after))["value"] == "second"
    if engine_db.engine.dialect.name == "sqlite":
        plain = sqlite3.connect(engine_db.engine.url.database)
        error = sqlite3.DatabaseError
        expected = "etl_craft_actor"
    else:
        service = require("postgres")
        plain = psycopg.connect(
            host=service.host,
            port=service.port,
            dbname=engine_db.engine.url.database,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
        plain.execute(f'SET search_path TO "{engine_db.config.engine.active.schema or "public"}"')
        plain.commit()
        error = psycopg.Error
        expected = "written only by etl-craft"
    try:
        for table in ("CFG_PROJECT", "AUD_PROJECT"):
            with pytest.raises(error, match=expected):
                plain.execute(f"UPDATE {table} SET VALUE='external'")
            plain.rollback()
    finally:
        plain.close()


def test_terminal_attempts_and_append_only_rows_are_immutable(engine_db):
    tables = seeded_tables(engine_db)
    protected = {
        "AUD_TASK_ATTEMPTS",
        "AUD_RUN_INTERVENTIONS",
        "AUD_GATE_DECISIONS",
        "AUD_DEPENDENCY_CONSUMPTION",
        "AUD_METADATA_CHANGES",
        "AUD_ACTIONS",
    }
    for table, column in tables:
        if table.upper() not in protected:
            continue
        for sql in (f'UPDATE "{table}" SET "{column}"="{column}"', f'DELETE FROM "{table}"'):
            with (
                pytest.raises(DBAPIError, match=r"immutable|append-only"),
                engine_db.engine.begin() as conn,
            ):
                conn.exec_driver_sql(sql)


def test_actor_stamps_survive_failed_insert_and_project_column_refresh(engine_db, tmp_path):
    project = tmp_path / "migrations"
    project.mkdir()
    (project / "0001_column.sql").write_text(
        "ALTER TABLE CFG_PIPELINES ADD COLUMN PROJECT_NOTE VARCHAR;"
    )
    with acting_as(ALICE), engine_db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        migrations.mark_packaged_migrations_applied(conn)
    migrations.apply_pending_migrations(engine_db.engine, project)
    bob = Actor("bob", ActorKind.HUMAN)
    with acting_as(bob):
        with pytest.raises(DBAPIError), engine_db.engine.begin() as conn:
            add_pipeline(conn, "P")
        with engine_db.engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE CFG_PIPELINES SET CREATED_BY='forged', "
                    "ACTIVE_FLAG='N' WHERE PIPELINE_ID=:pipeline"
                ),
                {"pipeline": pipeline},
            )
    with engine_db.engine.connect() as conn:
        assert tuple(
            conn.execute(
                text(
                    "SELECT CREATED_BY, UPDATED_BY FROM CFG_PIPELINES WHERE PIPELINE_ID=:pipeline"
                ),
                {"pipeline": pipeline},
            ).one()
        ) == ("alice", "bob")
        conn.execute(
            text("UPDATE CFG_PIPELINES SET CREATED_BY='forged' WHERE PIPELINE_ID=:pipeline"),
            {"pipeline": pipeline},
        )
        assert (
            conn.execute(
                text("SELECT CREATED_BY FROM CFG_PIPELINES WHERE PIPELINE_ID=:pipeline"),
                {"pipeline": pipeline},
            ).scalar_one()
            == "alice"
        )


@pytest.mark.engine_sqlite
@pytest.mark.parametrize("empty_engine_db", ["sqlite"], indirect=True)
def test_doctor_warns_about_writable_sqlite_file(empty_engine_db):
    from pathlib import Path

    from etl_craft.engine.schema import init_db
    from etl_craft.services.doctor import Status, _engine_state

    init_db(empty_engine_db.engine)
    path = Path(empty_engine_db.engine.url.database)
    path.chmod(0o666)
    checks = _engine_state(empty_engine_db.config, empty_engine_db.engine)
    assert any(check.status == Status.WARN and "chmod go-w" in check.detail for check in checks)
