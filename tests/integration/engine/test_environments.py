"""Admin governance, PostgreSQL direct changes, and complete package guards."""

import shutil
import sqlite3
from contextvars import Context
from dataclasses import replace

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from etl_craft.core.actor import SYSTEM_ACTOR
from etl_craft.core.enums import ActionName, ApiRole, ControlAction
from etl_craft.core.errors import MigrationError, PermissionDeniedError, UsageError
from etl_craft.dialects.engine.base import EngineDialect
from etl_craft.engine import migrations
from etl_craft.services import doctor
from etl_craft.services.operations import OperationContext
from etl_craft.services.operations.processes import check_package_schema
from fixtures.actions import act
from fixtures.metadata import add_pipeline
from fixtures.released_schema import SCHEMAS, install


def context(db):
    return OperationContext(db.engine, db.config, SYSTEM_ACTOR, role=ApiRole.ADMIN)


def outside(db, sql):
    def execute():
        with db.engine.begin() as conn:
            return (
                conn.execute(text(sql)).all()
                if sql.startswith("SELECT")
                else conn.execute(text(sql))
            )

    return Context().run(execute)


def governance(db, policy):
    return act(
        context(db),
        ActionName.PROJECT_SET,
        project="default",
        reason="test policy",
        governance=policy,
        environment_type="DEV",
    )


def test_environment_settings_require_deployment_admin_and_reason(engine_db):
    ctx = context(engine_db)
    for role in (ApiRole.VIEWER, ApiRole.OPERATOR, ApiRole.DEVELOPER):
        for values in (
            {"governance": "RELAXED"},
            {"environment_type": "DEV"},
            {"environment_name": "perf"},
        ):
            with pytest.raises(PermissionDeniedError):
                act(
                    replace(ctx, role=role),
                    ActionName.PROJECT_SET,
                    project="default",
                    reason="attempt",
                    **values,
                )
    with pytest.raises(UsageError, match="reason"):
        act(ctx, ActionName.PROJECT_SET, project="default", reason="", governance="RELAXED")
    governance(engine_db, "RELAXED")
    with engine_db.engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT ENVIRONMENT_TYPE AS environment, GOVERNANCE AS governance FROM CFG_PROJECTS"
            )
        ).one()
        assert row.environment == "DEV" and row.governance == "RELAXED"
        history = conn.execute(
            text(
                "SELECT ACTOR AS actor, GOVERNANCE AS governance FROM "
                "AUD_CFG_PROJECTS_HISTORY WHERE CURRENT_FLAG='Y'"
            )
        ).one()
        assert history.actor == SYSTEM_ACTOR.name and history.governance == "RELAXED"
    with pytest.raises(DBAPIError, match=r"CFG_PROJECTS|metadata is written only"):
        outside(engine_db, "UPDATE CFG_PROJECTS SET GOVERNANCE='STRICT'")


def test_prod_relaxed_is_refused_without_changing_project(engine_db):
    ctx = context(engine_db)
    with pytest.raises(UsageError, match="PROD requires STRICT"):
        act(
            ctx,
            ActionName.PROJECT_SET,
            project="default",
            reason="test",
            environment_type="PROD",
            governance="RELAXED",
        )
    with pytest.raises(DBAPIError), engine_db.engine.begin() as conn:
        conn.execute(text("UPDATE CFG_PROJECTS SET ENVIRONMENT_TYPE='PROD',GOVERNANCE='RELAXED'"))
    with engine_db.engine.connect() as conn:
        assert (
            conn.execute(text("SELECT GOVERNANCE AS governance FROM CFG_PROJECTS")).scalar_one()
            == "STRICT"
        )


@pytest.mark.parametrize("policy", ("STRICT", "RELAXED"))
def test_governance_pause_and_resume_empty_reasons(engine_db, policy):
    governance(engine_db, policy)
    with engine_db.engine.begin() as conn:
        add_pipeline(conn, "P")
    for action in (ControlAction.PAUSE, ControlAction.RESUME):
        if policy == "STRICT":
            with pytest.raises(UsageError, match=r"default.*DEV.*STRICT.*reason"):
                act(
                    context(engine_db),
                    ActionName.CONTROL,
                    pipeline_code="P",
                    action=action,
                    reason="",
                )
        else:
            act(context(engine_db), ActionName.CONTROL, pipeline_code="P", action=action, reason="")


def test_postgres_direct_config_changes_are_stamped_and_historied(engine_db):
    if not engine_db.dialect.supports_api_replicas:
        with engine_db.engine.begin() as conn:
            migrations.mark_packaged_migrations_applied(conn)
        governance(engine_db, "RELAXED")
        with (
            sqlite3.connect(engine_db.engine.url.database) as conn,
            pytest.raises(sqlite3.OperationalError, match="no such function"),
        ):
            conn.execute("UPDATE CFG_PROJECTS SET GOVERNANCE='STRICT'")
        checks = doctor.run_checks(engine_db.config)
        assert any(
            c.status == doctor.Status.INFO and "direct SQL edits need a PostgreSQL" in c.detail
            for c in checks
        ), [(c.name, c.detail) for c in checks]
        return
    governance(engine_db, "RELAXED")
    outside(
        engine_db,
        "INSERT INTO CFG_PIPELINES (PIPELINE_CODE,PIPELINE_NAME,REFRESH_TYPE)"
        " VALUES ('DIRECT','Direct','FULL')",
    )
    outside(
        engine_db,
        "UPDATE CFG_PIPELINES SET PIPELINE_NAME='Changed',ACTIVE_FLAG='N' "
        "WHERE PIPELINE_CODE='DIRECT'",
    )
    with engine_db.engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT CREATED_BY AS creator,UPDATED_BY AS updater FROM "
                "CFG_PIPELINES WHERE PIPELINE_CODE='DIRECT'"
            )
        ).one()
        assert row.creator.startswith("sql:") and row.updater == row.creator
        history = conn.execute(
            text(
                "SELECT ACTOR AS actor,OPERATION AS operation FROM "
                "AUD_CFG_PIPELINES_HISTORY ORDER BY CHANGE_ID"
            )
        ).all()
        assert [r.operation for r in history] == ["INSERT", "UPDATE"]
        assert all(r.actor == row.creator for r in history)
    for sql, message in (
        ("DELETE FROM CFG_PIPELINES WHERE PIPELINE_CODE='DIRECT'", "direct DELETE"),
        ("UPDATE CFG_PIPELINES SET ACTIVE_FLAG='X'", "ACTIVE_FLAG=X; expected Y or N"),
        ("UPDATE AUD_CFG_PIPELINES_HISTORY SET ACTOR='fake'", "audit and history"),
        ("INSERT INTO CFG_POOLS (POOL_NAME) VALUES ('outside')", "admin, audit"),
    ):
        with pytest.raises(DBAPIError, match=message):
            outside(engine_db, sql)
    governance(engine_db, "STRICT")
    with pytest.raises(DBAPIError, match="governance STRICT"):
        outside(engine_db, "UPDATE CFG_PIPELINES SET PIPELINE_NAME='Forbidden'")


def test_missing_guard_fails_doctor_and_startup(engine_db):
    db = engine_db
    with db.engine.begin() as conn:
        migrations.mark_packaged_migrations_applied(conn)
    trigger = "trg_actor_guard_cfg_pipelines" + (
        "" if db.dialect.supports_api_replicas else "_update"
    )
    with db.engine.begin() as conn:
        conn.exec_driver_sql(
            f"DROP TRIGGER {trigger}"
            + (" ON CFG_PIPELINES" if db.dialect.supports_api_replicas else "")
        )
    with pytest.raises(MigrationError, match=trigger):
        check_package_schema(context(db))
    checks = doctor.run_checks(db.config)
    assert any(
        c.status == doctor.Status.FAIL and trigger in c.detail and "setup" in c.detail
        for c in checks
    )
    with db.engine.begin() as conn:
        db.dialect.refresh_metadata_triggers(conn)
    check_package_schema(context(db))


@pytest.mark.parametrize(
    "release", sorted(path.name for path in SCHEMAS.iterdir() if path.is_dir())
)
def test_environment_upgrade_preserves_real_projects(
    empty_engine_db, release, tmp_path, monkeypatch
):
    db = empty_engine_db
    install(db, release)
    previous = tmp_path / "previous"
    previous.mkdir()
    for file in db.dialect.migrations_dir().glob("*.sql"):
        if file.name < "0066":
            shutil.copyfile(file, previous / file.name)
    with monkeypatch.context() as patch:
        patch.setattr(EngineDialect, "migrations_dir", lambda self: previous)
        migrations.apply_pending_migrations(db.engine)
    with db.engine.begin() as conn:
        identity = conn.execute(
            text(
                "INSERT INTO CFG_PROJECTS (PROJECT_CODE,PROJECT_NAME,DEFAULT_POOL) "
                "VALUES ('legacy','Legacy','default') RETURNING PROJECT_ID AS "
                "project_id"
            )
        ).scalar_one()
    migrations.apply_pending_migrations(db.engine)
    with db.engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT PROJECT_ID AS project_id,ENVIRONMENT_TYPE AS "
                "environment,GOVERNANCE AS governance FROM CFG_PROJECTS WHERE "
                "PROJECT_CODE='legacy'"
            )
        ).one()
        assert row.project_id == identity
        assert row.environment == "CUSTOM" and row.governance == "STRICT"


@pytest.mark.parametrize(
    "empty_engine_db", [pytest.param("sqlite", marks=pytest.mark.engine_sqlite)], indirect=True
)
def test_sqlite_connections_refuse_unattributed_package_ddl(engine_db):
    db = engine_db
    trigger = "trg_actor_guard_cfg_pipelines_update"
    with db.engine.begin() as conn:
        conn.exec_driver_sql(f"DROP TRIGGER {trigger}")
        db.dialect.refresh_metadata_triggers(conn)
    with pytest.raises(DBAPIError, match="not authorized"):
        outside(db, f"DROP TRIGGER {trigger}")
    with db.engine.connect() as conn:
        assert (
            conn.execute(
                text(
                    "SELECT COUNT(*) AS count FROM sqlite_schema WHERE type='trigger' AND"
                    " name=:name"
                ),
                {"name": trigger},
            ).scalar_one()
            == 1
        )


@pytest.mark.parametrize(
    "empty_engine_db",
    [pytest.param("postgresql", marks=pytest.mark.engine_postgres)],
    indirect=True,
)
def test_postgres_direct_edit_cannot_move_or_touch_strict_project(engine_db):
    db = engine_db
    governance(db, "RELAXED")
    act(context(db), ActionName.PROJECT_ADD, project="strict_project", reason="strict project")
    with db.engine.begin() as conn:
        strict = conn.execute(
            text("SELECT PROJECT_ID AS id FROM CFG_PROJECTS WHERE PROJECT_CODE='strict_project'")
        ).scalar_one()
        add_pipeline(conn, "RELAXED_PIPELINE")
        conn.execute(
            text(
                "INSERT INTO CFG_PIPELINES "
                "(PROJECT_ID,PIPELINE_CODE,PIPELINE_NAME,REFRESH_TYPE) VALUES "
                "(:project,'STRICT_PIPELINE','Strict','FULL')"
            ),
            {"project": strict},
        )
        before = conn.execute(
            text(
                "SELECT PIPELINE_CODE AS code,PIPELINE_NAME AS name FROM "
                "CFG_PIPELINES ORDER BY PIPELINE_CODE"
            )
        ).all()
    with pytest.raises(DBAPIError, match="moving PROJECT_ID is refused"):
        outside(
            db,
            f"UPDATE CFG_PIPELINES SET PROJECT_ID={strict} WHERE PIPELINE_CODE='RELAXED_PIPELINE'",
        )
    with pytest.raises(DBAPIError, match="governance STRICT"):
        outside(db, "UPDATE CFG_PIPELINES SET PIPELINE_NAME='outside'")
    with db.engine.connect() as conn:
        assert (
            conn.execute(
                text(
                    "SELECT PIPELINE_CODE AS code,PIPELINE_NAME AS name FROM "
                    "CFG_PIPELINES ORDER BY PIPELINE_CODE"
                )
            ).all()
            == before
        )

    def transaction():
        with db.engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE CFG_PIPELINES SET PIPELINE_NAME='allowed' WHERE "
                    "PIPELINE_CODE='RELAXED_PIPELINE'"
                )
            )
            conn.execute(
                text(
                    "UPDATE CFG_PROJECTS SET GOVERNANCE='RELAXED' WHERE "
                    "PROJECT_CODE='strict_project'"
                )
            )

    with pytest.raises(DBAPIError, match=r"CFG_PROJECTS.*admin, audit"):
        Context().run(transaction)


def test_shared_warehouses_are_informational_and_store_outages_are_reported(
    engine_db, tmp_path, monkeypatch
):
    import base64

    import yaml

    from etl_craft.config.object_models import Artifacts
    from etl_craft.core.errors import ConfigurationError
    from etl_craft.engine.bundles import Bundle
    from etl_craft.services.project_warehouses import shared_warehouses

    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    db = engine_db
    config = replace(db.config, artifacts=Artifacts((tmp_path / "store").as_uri()))
    ctx = replace(context(db), config=config)
    act(ctx, ActionName.PROJECT_ADD, project="second", reason="test")
    for code in ("default", "second"):
        raw = {
            "Secrets": {"Source_type": "ENVIRONMENT"},
            "Orchestration": {"Project": code},
            "Warehouse": {
                "dev": {"Jdbc_url": f"jdbc:duckdb:{tmp_path / 'shared.duckdb'}", "Schema": "main"}
            },
        }
        act(
            replace(ctx, project=code),
            ActionName.BUNDLE_PUBLISH,
            files={
                "craft-connector.yml": base64.b64encode(
                    yaml.safe_dump(raw, sort_keys=False).encode()
                ).decode()
            },
        )
    comparison = shared_warehouses(ctx.engine, config)
    assert comparison.shared == {"default", "second"} and comparison.unavailable == ()
    listed = act(ctx, ActionName.PROJECT_LIST, reason="inspect")
    assert all(p.shared_warehouse for p in listed.projects)

    def unavailable(*args, **kwargs):
        raise ConfigurationError("test store unavailable")

    monkeypatch.setattr(Bundle, "materialize", unavailable)
    changed = act(
        ctx, ActionName.PROJECT_SET, project="second", name="Still changed", reason="admin update"
    )
    assert changed.projects[0].project == "second" and changed.projects[0].name == "Still changed"
    assert any(
        "warehouse comparison unavailable" in note and "restore" in note
        for note in changed.warehouse_notes
    )


def test_project_environment_changes_serialize_without_prod_relaxed(engine_db):
    from fixtures.races import two_at_once

    db = engine_db
    governance(db, "STRICT")
    ctx = context(db)

    def change(**fields):
        try:
            return act(ctx, ActionName.PROJECT_SET, project="default", reason="race", **fields)
        except UsageError as error:
            return error

    results = two_at_once(
        lambda: change(environment_type="PROD"),
        lambda: change(governance="RELAXED"),
        at="etl_craft.services.operations.admin_metadata.change_project",
    )
    assert sum(isinstance(result, UsageError) for result in results) == 1
    with db.engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT ENVIRONMENT_TYPE AS environment,GOVERNANCE AS governance FROM"
                " CFG_PROJECTS WHERE PROJECT_CODE='default'"
            )
        ).one()
        assert (row.environment, row.governance) in (("PROD", "STRICT"), ("DEV", "RELAXED"))


@pytest.mark.parametrize(
    "empty_engine_db",
    [pytest.param("postgresql", marks=pytest.mark.engine_postgres)],
    indirect=True,
)
def test_postgres_direct_primary_key_edits_preserve_history_identity(engine_db):
    from fixtures.metadata import add_task

    db = engine_db
    governance(db, "RELAXED")
    with db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        task = add_task(conn, pipeline, "T", "PYTHON", SCRIPT_FILE="python_scripts/task.py")
        identity = conn.execute(
            text(
                "SELECT TASK_PARAMETER_ID AS id FROM CFG_TASK_PARAMETERS WHERE "
                "TASK_ID=:task AND PARAMETER_NAME='SCRIPT_FILE'"
            ),
            {"task": task},
        ).scalar_one()
    with pytest.raises(DBAPIError, match=r"CFG_TASK_PARAMETERS.TASK_PARAMETER_ID.*primary-key"):
        outside(
            db,
            "UPDATE CFG_TASK_PARAMETERS SET TASK_PARAMETER_ID=DEFAULT "
            f"WHERE TASK_PARAMETER_ID={identity}",
        )
    with db.engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT TASK_PARAMETER_ID AS id,CURRENT_FLAG AS current FROM "
                "AUD_CFG_TASK_PARAMETERS_HISTORY WHERE TASK_ID=:task"
            ),
            {"task": task},
        ).all()
        assert len(rows) == 1 and rows[0].id == identity and rows[0].current == "Y"


def test_malformed_manifest_cannot_block_admin_changes(engine_db, tmp_path, monkeypatch):
    import base64

    import yaml

    from etl_craft.config.object_models import Artifacts
    from etl_craft.core.errors import ConfigurationError
    from etl_craft.engine.bundles import Bundle

    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    db = engine_db
    ctx = replace(
        context(db), config=replace(db.config, artifacts=Artifacts((tmp_path / "store").as_uri()))
    )
    raw = {"Secrets": {"Source_type": "ENVIRONMENT"}, "Orchestration": {"Project": "default"}}
    act(
        ctx,
        ActionName.BUNDLE_PUBLISH,
        files={
            "craft-connector.yml": base64.b64encode(
                yaml.safe_dump(raw, sort_keys=False).encode()
            ).decode()
        },
    )

    def malformed(*args, **kwargs):
        raise ConfigurationError("bundle: invalid manifest; restore its recorded files")

    monkeypatch.setattr(Bundle, "load", malformed)
    result = act(
        ctx,
        ActionName.PROJECT_SET,
        project="default",
        environment_name="applied",
        reason="admin change",
    )
    assert result.projects[0].environment_name == "applied"
    assert (
        len(result.warehouse_notes) == 1 and "comparison unavailable" in result.warehouse_notes[0]
    )


def test_setup_restores_missing_static_guards(engine_db):
    from etl_craft.services.setup import setup

    db = engine_db
    table = "AUD_ATTEMPT_ACKNOWLEDGEMENTS"
    trigger = (
        "trg_attempt_ack_immutable"
        if db.dialect.supports_api_replicas
        else "trg_attempt_ack_delete"
    )
    with db.engine.begin() as conn:
        migrations.mark_packaged_migrations_applied(conn)
        conn.exec_driver_sql(
            f"DROP TRIGGER {trigger}" + (f" ON {table}" if db.dialect.supports_api_replicas else "")
        )
    with pytest.raises(MigrationError, match=trigger):
        check_package_schema(context(db))
    result = setup(db.config)
    assert not result.failed, result.checks
    check_package_schema(context(db))
    if db.dialect.supports_api_replicas:
        with db.engine.begin() as conn:
            conn.exec_driver_sql(f"ALTER TABLE {table} DISABLE TRIGGER {trigger}")
        with pytest.raises(MigrationError, match=trigger):
            check_package_schema(context(db))
        assert not setup(db.config).failed
        check_package_schema(context(db))
