"""Actor attribution, immutable history and ordinary-connection write refusal."""

import json
import logging
import re
import sqlite3
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import psycopg
import pytest
from sqlalchemy import inspect, text
from sqlalchemy.exc import DBAPIError

from etl_craft.core.actor import Actor, ActorKind, acting_as
from etl_craft.core.enums import (
    AccountKind,
    AgentCommand,
    AgentStatus,
    ApiRole,
    AttemptStatus,
    ControlAcknowledgement,
    ControlAction,
    ControlStatus,
    Permission,
    ProcessRole,
    RunStatus,
)
from etl_craft.engine import migrations, transitions
from etl_craft.engine.audit import command_request, register_engine
from fixtures.metadata import add_pipeline, add_pipeline_dependency, add_task
from fixtures.released_schema import install
from fixtures.services import POSTGRES_PASSWORD, POSTGRES_USER, require

ALICE = Actor("alice", ActorKind.USER)


@pytest.fixture(autouse=True)
def restore_logger():
    logger = logging.getLogger("etl_craft")
    handlers, level = list(logger.handlers), logger.level
    yield
    logger.handlers[:] = handlers
    logger.setLevel(level)


def write_grants(db):
    with db.engine.connect() as conn:
        return db.dialect.extra_write_grants(conn)


def test_attribution_and_immutable_command_requests(engine_db):
    engine = engine_db.engine
    with acting_as(ALICE), engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        run = transitions.create_active_run(conn, pipeline)
        row = conn.execute(
            text("SELECT STARTED_BY, STARTED_BY_KIND FROM AUD_PIPELINES_RUN_LOG")
        ).one()
        assert tuple(row) == ("alice", "USER")
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
        assert tuple(row)[:3] == ("alice", "USER", "REQUESTED")
        assert "sensitive" not in str(row[3])
    for sql in ("UPDATE AUD_ACTIONS SET OUTCOME='SUCCESS'", "DELETE FROM AUD_ACTIONS"):
        with pytest.raises(DBAPIError, match=r"immutable|append-only"), engine.begin() as conn:
            conn.execute(text(sql))
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
        assert all(tuple(row)[:2] == ("alice", "USER") for row in rows)
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
        conn.execute(text("DELETE FROM AUD_CFG_PIPELINES_HISTORY"))


def seeded_tables(db):
    engine = db.engine
    from etl_craft.core.actor import purpose

    token = purpose.set("retention")
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO AUD_SOURCE_COLUMNS_BASELINE (SOURCE_COLUMN_ID,TABLE_NAME, "
                    "COLUMN_NAME,DATA_TYPE,COLUMN_POSITION,CREATED_BY,CREATE_DATE,UPDATED_BY, "
                    "UPDATED_DATE) VALUES (1,'raw.orders','id','INT',1,'test',CURRENT_TIMESTAMP, "
                    "'test',CURRENT_TIMESTAMP)"
                )
            )
    finally:
        purpose.reset(token)
    with engine.begin() as conn:
        p1 = add_pipeline(conn, "DOWN")
        p2 = add_pipeline(conn, "UP")
        t1 = add_task(conn, p1, "load")
        t2 = add_task(conn, p2, "load")
        dependency = add_pipeline_dependency(conn, p1, p2)
        r1 = transitions.create_active_run(conn, p1)
        r2 = transitions.create_active_run(conn, p2)
        task_run = transitions.find_or_create_task_run(conn, t1, r1).task_run_id
        from fixtures.metadata import add_table

        contract = add_table(conn, "warehouse.s.t")
        attempt = transitions.queue_attempt(conn, task_run, ALICE)
        transitions.claim_attempt(
            conn,
            attempt,
            ALICE,
            owner="audit-fixture",
            lease_expires_at=datetime.now(UTC) + timedelta(seconds=60),
        )
        transitions.finish_attempt(
            conn, attempt, AttemptStatus.SUCCESS, ALICE, owner="audit-fixture"
        )
        values = {
            "bundle_id": "a" * 64,
            "manifest": "{}",
            "attempt_id": attempt,
            "kind": ControlAcknowledgement.PROCESS,
            "request_key": "audit-guard-fixture",
            "attempt_ids": "[]",
            "requested_by_kind": ActorKind.USER,
            "target_table_id": contract,
            "column_position": 1,
            "operation": "INSERT",
            "current_flag": "Y",
            "table_id": contract,
            "role": "viewer",
            "permission": Permission.CAN_VIEW,
            "grantee_role": ApiRole.VIEWER,
            "account_kind": AccountKind.SERVICE,
            "token_sha256": "0" * 64,
            "pipeline_id": p1,
            "pipeline_code": "DOWN",
            "task_id": t1,
            "depends_on_pipeline_id": p2,
            "depends_on_task_id": t2,
            "pipeline_run_id": r1,
            "task_run_id": task_run,
            "file_path": "/test/event.csv",
            "event_key": "sender-message-id",
            "item_key": "sender-item-id",
            "report_id": "actor-item-report",
            "item_attempt_number": 2,
            "evidence": '{"files":[]}',
            "wait_until": datetime.now(UTC),
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
            "actor_kind": "USER",
            "host": "host",
            "command": "run",
            "arguments": "{}",
            "outcome": "REQUESTED",
            "target_object": "warehouse.s.t",
            "target_catalog_name": "warehouse",
            "target_schema_name": "s",
            "target_table_name": "t",
            "recomputed_at": datetime.now(UTC),
            "reservation_id": "test-process",
            "birth_time": 1.0,
            "pool_connections": 10,
            "session_connections": 1,
            "local_task_connections": 0,
            "backend_pid": 1,
            "backend_started_at": datetime.now(UTC),
            "heartbeat_at": datetime.now(UTC),
            "expires_at": datetime.now(UTC),
            "mirror_id": "actor-mirror",
            "table_name": "AUD_PIPELINE_PAUSES",
            "row_key": "[1]",
            "metadata_snapshot": "{}",
        }
        inspector = inspect(conn)
        schema = None if engine.dialect.name == "sqlite" else inspector.default_schema_name
        live = {name.upper() for name in inspector.get_table_names(schema=schema)}
        declared = re.findall(r"CREATE TABLE (\w+)", db.dialect.schema_path().read_text())
        tables = [
            table if engine.dialect.name == "sqlite" else table.lower()
            for table in declared
            if table.startswith(("CFG_", "AUD_")) and table in live
        ]
        for table in tables:
            if conn.exec_driver_sql(f'SELECT 1 FROM "{table}" LIMIT 1').first():
                continue
            payload = {}
            columns = inspector.get_columns(table, schema=schema)
            for column in columns:
                name = column["name"]
                lower = name.lower()
                if table.upper() == "CFG_API_TOKENS" and lower == "attempt_id":
                    continue
                if (
                    column.get("identity")
                    or column.get("autoincrement")
                    or (column.get("primary_key") and lower not in values)
                ):
                    continue
                if (
                    column.get("nullable") or column.get("default") is not None
                ) and lower not in values:
                    continue
                value = values.get(lower)
                if value is None:
                    type_name = str(column["type"]).upper()
                    if type_name == "DATE":
                        value = datetime.now(UTC).date()
                    elif "TIMESTAMP" in type_name or "DATETIME" in type_name:
                        value = datetime.now(UTC)
                    else:
                        value = 1 if "INT" in type_name or "NUMERIC" in type_name else "sample"
                if table.upper() == "AUD_PIPELINE_PAUSES" and lower == "outcome":
                    value = RunStatus.SUCCESS
                if table.upper() == "AUD_CONNECTION_RESERVATIONS" and lower == "role":
                    value = ProcessRole.UI
                if table.upper() == "AUD_CONTROL_REQUESTS":
                    value = {"status": ControlStatus.REQUESTED, "action": ControlAction.HOLD}.get(
                        lower, value
                    )
                if table.upper() == "AUD_BUSINESS_RULES_RESULTS" and lower == "status":
                    value = "REPORT"
                if table.upper() == "AUD_AGENTS" and lower == "status":
                    value = AgentStatus.ACTIVE
                if table.upper() == "AUD_AGENT_COMMANDS" and lower == "command":
                    value = AgentCommand.DRAIN
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
            if table.upper() == "AUD_ATTEMPT_LOG_CHUNKS":
                # Part of the composite key, which the loop above can leave out.
                name = next(c["name"] for c in columns if c["name"].lower() == "sequence_number")
                payload.setdefault(name, 0)
            # An agent is a token, and an agent's rows name a registered agent.
            for column in columns:
                lower = column["name"].lower()
                if lower == "agent_id" and table.upper() == "AUD_AGENTS":
                    source = "SELECT TOKEN_ID FROM CFG_API_TOKENS"
                elif lower == "agent_id":
                    source = "SELECT AGENT_ID FROM AUD_AGENTS"
                else:
                    continue
                payload[column["name"]] = conn.execute(text(source)).scalars().first()
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
            options=f"-c search_path={engine_db.config.engine.schema or 'public'}",
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
                refusal = expected
                if table.upper() == "CFG_ACCOUNTS" and sql.startswith(("DELETE", "TRUNCATE")):
                    refusal = rf"{expected}|CFG_ACCOUNTS cannot be deleted"
                if table.upper() in (
                    "CFG_GROUPS",
                    "CFG_GROUP_MEMBERS",
                    "CFG_PIPELINE_GRANTS",
                ) and sql.startswith(("DELETE", "TRUNCATE")):
                    refusal = rf"{expected}|{table.upper()}: use ACTIVE_FLAG=N instead of deleting"
                with pytest.raises(error, match=refusal):
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
    applied = migrations.apply_pending_migrations(db.engine)
    assert {
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
        "0020_reject_count.sql",
        "0021_config_history.sql",
        "0022_watermarks.sql",
        "0023_event_waits.sql",
        "0024_source_columns.sql",
        "0025_token_audit_names.sql",
        "0026_config_base_contract.sql",
        "0027_config_identities.sql",
        "0028_source_contracts.sql",
        "0029_rule_target_contracts.sql",
        "0030_task_target_contracts.sql",
        "0031_lineage_versions.sql",
    } <= set(applied)
    assert applied == sorted(applied)
    assert migrations.apply_pending_migrations(db.engine) == []


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
        assert ("cfg_tasks", role, "UPDATE") in write_grants(engine_db)
        with engine_db.engine.begin() as conn:
            conn.exec_driver_sql("CREATE TABLE CFG_TEAM (VALUE VARCHAR)")
            conn.exec_driver_sql(f'GRANT UPDATE ON CFG_TEAM TO "{role}"')
        assert not any(table == "cfg_team" for table, _, _ in write_grants(engine_db))
        group = role + "_group"
        with engine_db.engine.begin() as conn:
            conn.exec_driver_sql(f'CREATE ROLE "{group}" NOLOGIN')
            conn.exec_driver_sql(f'GRANT "{group}" TO "{role}"')
            conn.exec_driver_sql(f'REVOKE UPDATE ON CFG_TASKS FROM "{role}"')
            conn.exec_driver_sql(f'GRANT UPDATE ON CFG_TASKS TO "{group}"')
        assert ("cfg_tasks", group, "UPDATE") in write_grants(engine_db)
        assert ("cfg_tasks", role, "UPDATE") not in write_grants(engine_db)
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
        assert ("cfg_tasks", group, "UPDATE") not in write_grants(engine_db)

    finally:
        with engine_db.engine.begin() as conn:
            conn.exec_driver_sql(f'DROP OWNED BY "{role}"')
            if conn.execute(
                text("SELECT 1 FROM pg_roles WHERE rolname=:group"), {"group": role + "_group"}
            ).first():
                conn.exec_driver_sql(f'DROP OWNED BY "{role}_group"')
                conn.exec_driver_sql(f'DROP ROLE "{role}_group"')
            conn.exec_driver_sql(f'DROP ROLE "{role}"')


@pytest.mark.parametrize("actor_name", ["alice", "alice@laptop"])
def test_cli_actor_requests_and_read_only_audit(
    engine_db, tmp_path, monkeypatch, capsys, actor_name
):
    import yaml

    from etl_craft.cli import main

    profile = engine_db.config.engine
    jdbc = profile.jdbc_url
    if profile.auth_mode == "NONE":
        jdbc = "jdbc:sqlite:" + engine_db.engine.url.database
    metadata = {
        "Secrets": {"Source_type": "ENVIRONMENT"},
        "Orchestration": {"Project": "default"},
        "Engine": {
            "test": {
                "Jdbc_url": jdbc,
                "User": profile.user,
                "Auth_mode": str(profile.auth_mode),
                "Schema": profile.schema or ("main" if profile.auth_mode == "NONE" else "public"),
                **(
                    {"Secret": profile.extra["secret_var"]} if "secret_var" in profile.extra else {}
                ),
            }
        },
    }
    metadata["Artifacts"] = {"Store": (tmp_path / "artifacts").as_uri()}
    config = tmp_path / "craft-connector.yml"
    config.write_text(yaml.safe_dump(metadata, sort_keys=False))
    with engine_db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        deleted = add_task(conn, pipeline, "deleted", NOTE="deleted_parameter_history")
        unrelated = add_pipeline(conn, "OTHER")
        add_task(conn, unrelated, "other", NOTE="unrelated_parameter_history")
        conn.execute(text("DELETE FROM CFG_TASK_PARAMETERS WHERE TASK_ID=:task"), {"task": deleted})
        conn.execute(text("DELETE FROM CFG_TASKS WHERE TASK_ID=:task"), {"task": deleted})
    from fixtures.auth import service_token

    monkeypatch.setenv("ETL_CRAFT_TOKEN", service_token(engine_db.engine, actor_name))

    def command(*argv):
        result = main(["--config", str(config), *argv])
        assert result == 0, capsys.readouterr()

    command("pipelines", "refresh", "--pipeline", "P")
    command("run", "--pipeline-code", "P", "--init-only")
    with engine_db.engine.connect() as conn:
        run_id = conn.execute(
            text("SELECT PIPELINE_RUN_ID FROM AUD_PIPELINES_RUN_LOG")
        ).scalar_one()
    command("pause", "--pipeline-code", "P", "--reason", "wait")
    command("resume", "--pipeline-code", "P", "--reason", "ready")
    command("abort", "--pipeline-code", "P", "--reason", "stop")
    command(
        "mark",
        "--pipeline-code",
        "P",
        "--run-id",
        str(run_id),
        "--status",
        "SUCCESS",
        "--reason",
        "verified",
    )
    capsys.readouterr()
    command("audit", "--pipeline-code", "P", "--since", "2026-01-01")
    shown = capsys.readouterr().out
    assert actor_name in shown and "SERVICE" in shown and "REQUESTED" in shown
    assert "deleted_parameter_history" in shown
    assert "unrelated_parameter_history" not in shown
    command("setup", "--print-grants")
    assert "file permissions" in capsys.readouterr().out if profile.auth_mode == "NONE" else True
    command("history", "--pipeline-code", "P", "--run-id", str(run_id))
    assert "STARTED_BY" in capsys.readouterr().out
    with engine_db.engine.connect() as conn:
        actions = conn.execute(
            text("SELECT COMMAND, ACTOR, ACTOR_KIND FROM AUD_ACTIONS ORDER BY ACTION_ID")
        ).all()
        assert [tuple(row) for row in actions] == [
            (name, actor_name, "SERVICE")
            for name in ("pipelines refresh", "run", "control", "control", "control", "mark")
        ]
        row = conn.execute(
            text(
                "SELECT STARTED_BY, STARTED_BY_KIND, ENDED_BY, ENDED_BY_KIND "
                "FROM AUD_PIPELINES_RUN_LOG"
            )
        ).one()
        assert tuple(row) == (actor_name, "SERVICE", actor_name, "SERVICE")
        row = conn.execute(
            text("SELECT PAUSED_BY_KIND, RESUMED_BY_KIND FROM AUD_PIPELINE_PAUSES")
        ).one()
        assert tuple(row) == ("SERVICE", "SERVICE")
        assert set(
            conn.execute(text("SELECT REQUESTED_BY_KIND FROM AUD_RUN_INTERVENTIONS")).scalars()
        ) == {"SERVICE"}

    monkeypatch.setenv("ETL_CRAFT_TOKEN", "invalid")
    assert main(["--config", str(config), "run", "--pipeline-code", "P", "--init-only"]) == 3
    assert "no signed-in account" in capsys.readouterr().err
    with engine_db.engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM AUD_ACTIONS")).scalar_one() == 6


def test_terminal_attempts_and_append_only_rows_are_immutable(engine_db):
    tables = seeded_tables(engine_db)
    protected = {
        "AUD_TASK_ATTEMPTS",
        "AUD_ATTEMPT_ACKNOWLEDGEMENTS",
        "AUD_TASK_EVENTS",
        "AUD_EVENT_FILES",
        "AUD_EVENT_KEYS",
        "AUD_TASK_ITEMS",
        "AUD_PROJECT_BUNDLES",
        "AUD_RUN_INTERVENTIONS",
        "AUD_GATE_DECISIONS",
        "AUD_DEPENDENCY_CONSUMPTION",
        "AUD_METADATA_CHANGES",
        "AUD_ACTIONS",
        "AUD_PIPELINE_REFRESHES",
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
    bob = Actor("bob", ActorKind.USER)
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


@pytest.mark.parametrize("log_format", ["text", "json"])
def test_nested_action_secrets_never_reach_audit_logs_cli_or_api(engine_db, log_format):
    import io

    from fastapi.testclient import TestClient

    from etl_craft.api.app import create_app
    from etl_craft.cli.output import Output
    from etl_craft.core import log
    from etl_craft.core.enums import ActionName, ApiRole
    from etl_craft.core.errors import ConfigurationError
    from etl_craft.services.operations import OperationContext, tokens
    from etl_craft.services.operations.actions import invoke
    from etl_craft.services.operations.recording import recorded
    from etl_craft.services.operations.requests import AuditInput

    db = engine_db
    ctx = OperationContext(db.engine, db.config, ALICE, role=ApiRole.ADMIN)
    secret = "nested-password-never-public"
    url_secret = "url-password-never-public"
    arguments = {
        "connections": [
            {
                "password": secret,
                "url": f"postgresql://team:{url_secret}@db/project?sslmode=require",
            },
            {
                "nested": [{"API_TOKEN": secret}],
                "url": f"jdbc:postgresql://db/project?pwd={secret}",
                "warehouse": f"jdbc:databricks://db/default;httpPath=/sql/x;PWD={secret}",
            },
        ],
    }
    logs, output = io.StringIO(), io.StringIO()
    log.configure("INFO", log_format, logs)
    with recorded(ctx, ActionName.RUN, arguments):
        logging.getLogger("etl_craft.audit_test").info("request: %s", arguments)
        try:
            raise ConfigurationError(
                f"connection rejected postgresql://team:{url_secret}@db/project"
            )
        except ConfigurationError:
            logging.getLogger("etl_craft.audit_test").exception("connection failure")
    credential = invoke(
        ctx, ActionName.TOKEN_CREATE, tokens.TokenCreate(name="reader", role=ApiRole.ADMIN)
    )
    code = invoke(ctx, ActionName.AUDIT, AuditInput())
    Output(stdout=output).result(code, "json")
    with TestClient(create_app(ctx)) as client:
        response = client.get(
            "/api/v1/projects/default/audit",
            headers={"Authorization": "Bearer " + credential.token},
        )
    assert response.status_code == 200, response.text
    with db.engine.connect() as conn:
        row = conn.execute(
            text("SELECT ARGUMENTS AS arguments FROM AUD_ACTIONS WHERE COMMAND='run'")
        ).scalar_one()
        for table in inspect(conn).get_table_names():
            if table.lower().startswith("aud_"):
                rows = conn.exec_driver_sql('SELECT * FROM "' + table + '"').all()
                assert secret not in str(rows) and url_secret not in str(rows), table
    for visible in (str(row), logs.getvalue(), output.getvalue(), response.text):
        assert secret not in visible and url_secret not in visible
        assert "[REDACTED]" in visible
        assert "sslmode=require" in visible
    assert arguments["connections"][0]["password"] == secret


def test_direct_pipeline_requests_record_the_selected_project(engine_db):
    with engine_db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
    with acting_as(ALICE), command_request("run", {"pipeline_code": "P"}):
        register_engine(engine_db.engine)
    with engine_db.engine.connect() as conn:
        row = conn.execute(
            text("SELECT PROJECT_ID AS project_id,PIPELINE_ID AS pipeline_id FROM AUD_ACTIONS")
        ).one()
    assert row.project_id == 1
    assert row.pipeline_id == pipeline
