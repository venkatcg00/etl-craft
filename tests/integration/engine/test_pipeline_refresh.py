"""Refresh outcomes retain immutable working definitions without hiding failed pipelines."""

from dataclasses import replace

import pytest
import yaml
from sqlalchemy import text

from etl_craft.config.object_models import Artifacts
from etl_craft.core.actor import SYSTEM_ACTOR
from etl_craft.core.enums import ActionName, ApiRole, RunStatus
from etl_craft.core.errors import RunRefusedError
from etl_craft.engine import transitions
from etl_craft.engine.bundles import Bundle
from etl_craft.engine.refreshes import admitted
from etl_craft.services.operations.context import OperationContext
from fixtures.actions import act
from fixtures.engine_db import profile_block
from fixtures.metadata import add_dependency, add_pipeline, add_task


def project_files(engine_db, tmp_path):
    config = replace(
        engine_db.config,
        config_path=tmp_path / "craft-connector.yml",
        log_dir=tmp_path / "logs",
        artifacts=Artifacts((tmp_path / "store").as_uri()),
    )
    config.config_path.write_text(
        yaml.safe_dump(
            {
                "Secrets": {"Source_type": "ENVIRONMENT"},
                "Orchestration": {"Project": "default"},
                "Engine": {"dev": profile_block(config.engine, config.engine.schema)},
            },
            sort_keys=False,
        )
    )
    (tmp_path / "python_scripts").mkdir()
    script = tmp_path / "python_scripts" / "work.py"
    script.write_text(
        "from etl_craft.scripting import ScriptResult\n"
        "def run():\n    return ScriptResult(row_count=1)\n"
    )
    return config, script


@pytest.fixture
def refresh_project(engine_db, tmp_path):
    config, script = project_files(engine_db, tmp_path)
    with engine_db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        add_task(conn, pipeline, "T", "PYTHON", SCRIPT_FILE="python_scripts/work.py")
    ctx = OperationContext(
        engine_db.engine, config, SYSTEM_ACTOR, role=ApiRole.ADMIN, project="default", project_id=1
    )
    return ctx, pipeline, script


def test_refresh_records_findings_deduplicates_snapshots_and_keeps_the_last_good_bundle(
    refresh_project,
):
    ctx, pipeline, script = refresh_project
    first = act(ctx, ActionName.PIPELINE_REFRESH).refreshes[0]
    assert first.status == RunStatus.SUCCESS, first.findings
    script.write_text("def run(:\n    pass\n")
    failed = act(ctx, ActionName.PIPELINE_REFRESH).refreshes[0]
    assert failed.status == RunStatus.FAILED
    assert any("work.py" in finding.message for finding in failed.findings)
    with ctx.engine.connect() as conn:
        snapshot, version, bundle_id = admitted(conn, pipeline)
        assert bundle_id == first.bundle_id
        assert version == first.metadata_version
        assert "CFG_TASKS" in snapshot
        stored = conn.execute(
            text(
                "SELECT REFRESH_ID AS id,METADATA_SNAPSHOT AS snapshot,"
                "SNAPSHOT_REFRESH_ID AS reference FROM AUD_PIPELINE_REFRESHES "
                "ORDER BY REFRESH_ID"
            )
        ).all()
        assert stored[0].snapshot is not None
        assert stored[1].snapshot is None and stored[1].reference == stored[0].id
    history = act(ctx, ActionName.PIPELINE_REFRESH_HISTORY, pipeline_code="P")
    assert history.refreshes[0].refresh_id == failed.refresh_id
    assert history.refreshes[1].refresh_id == first.refresh_id


def test_new_run_uses_the_last_good_bundle_after_files_change_and_a_restart(refresh_project):
    ctx, pipeline, script = refresh_project
    before = script.read_bytes()
    first = act(ctx, ActionName.PIPELINE_REFRESH).refreshes[0]
    script.write_text("def run(:\n    pass\n")
    act(ctx, ActionName.PIPELINE_REFRESH)
    ctx.engine.dispose()
    ctx.engine.update_execution_options(refresh_required=True)
    with ctx.engine.begin() as conn:
        run = transitions.create_run(conn, pipeline, SYSTEM_ACTOR)
        row = conn.execute(
            text(
                "SELECT BUNDLE_ID AS bundle_id,METADATA_VERSION AS version "
                "FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_RUN_ID=:id"
            ),
            {"id": run},
        ).one()
        assert row.bundle_id == first.bundle_id
        assert row.version == first.metadata_version
        pinned = Bundle.load(conn, row.bundle_id)
    assert pinned.read(ctx.config, "python_scripts/work.py") == before


def test_pinned_tasks_write_relative_files_in_the_project_directory_not_the_bundle(
    refresh_project,
):
    ctx, pipeline, script = refresh_project
    script.write_text(
        "from pathlib import Path\nfrom etl_craft.scripting import ScriptResult\n"
        "def run():\n    Path('scratch.txt').write_text('first')\n"
        "    return ScriptResult(row_count=1)\n"
    )
    with ctx.engine.begin() as conn:
        first = conn.execute(text("SELECT TASK_ID FROM CFG_TASKS")).scalar_one()
        second = add_task(conn, pipeline, "T2", "PYTHON", SCRIPT_FILE="python_scripts/work.py")
        add_dependency(conn, pipeline, second, first)
    bundle = act(ctx, ActionName.PIPELINE_REFRESH).refreshes[0].bundle_id
    ctx.engine.update_execution_options(refresh_required=True)
    outcome = act(ctx, ActionName.RUN, pipeline_code="P")
    assert outcome.status == RunStatus.SUCCESS, outcome
    assert (ctx.config.project_dir / "scratch.txt").read_text() == "first"
    with ctx.engine.connect() as conn:
        root = Bundle.load(conn, bundle).materialize(ctx.config)
    assert not (root / "scratch.txt").exists()


def test_never_passed_and_blocked_refreshes_refuse_with_findings_and_remedy(refresh_project):
    ctx, pipeline, script = refresh_project
    script.unlink()
    failed = act(ctx, ActionName.PIPELINE_REFRESH)
    assert failed.failed
    with (
        ctx.engine.connect() as conn,
        pytest.raises(RunRefusedError, match="no version has passed yet") as error,
    ):
        admitted(conn, pipeline)
    assert "work.py" in str(error.value)
    assert "etl-craft pipelines refresh" in str(error.value)
    script.write_text(
        "from etl_craft.scripting import ScriptResult\n"
        "def run():\n    return ScriptResult(row_count=0)\n"
    )
    assert not act(ctx, ActionName.PIPELINE_REFRESH).failed
    with ctx.engine.begin() as conn:
        conn.execute(
            text("UPDATE CFG_PIPELINES SET BLOCK_ON_REFRESH_ERROR='Y' WHERE PIPELINE_ID=:id"),
            {"id": pipeline},
        )
    script.unlink()
    assert act(ctx, ActionName.PIPELINE_REFRESH).failed
    with (
        ctx.engine.connect() as conn,
        pytest.raises(RunRefusedError, match="BLOCK_ON_REFRESH_ERROR=Y"),
    ):
        admitted(conn, pipeline)


def test_refresh_retention_keeps_latest_and_last_good_snapshots(refresh_project):
    from datetime import UTC, datetime, timedelta

    from etl_craft.services.retention import retaining

    ctx, pipeline, script = refresh_project
    first = act(ctx, ActionName.PIPELINE_REFRESH).refreshes[0]
    expired = act(ctx, ActionName.PIPELINE_REFRESH).refreshes[0]
    second = act(ctx, ActionName.PIPELINE_REFRESH).refreshes[0]
    with ctx.engine.begin() as conn:
        conn.execute(
            text("UPDATE CFG_PIPELINES SET DESCRIPTION='bad version' WHERE PIPELINE_ID=:id"),
            {"id": pipeline},
        )
    script.unlink()
    failed = act(ctx, ActionName.PIPELINE_REFRESH).refreshes[0]
    cutoff = datetime.now(UTC) + timedelta(days=30)
    from etl_craft.engine.queries import statement

    with retaining(), ctx.engine.begin() as conn:
        conn.execute(
            statement(conn, "retention_aud_pipeline_refreshes"), {"cutoff": cutoff, "project_id": 1}
        )
    with ctx.engine.connect() as conn:
        kept = set(
            conn.execute(text("SELECT REFRESH_ID AS id FROM AUD_PIPELINE_REFRESHES")).scalars()
        )
        assert {first.refresh_id, second.refresh_id, failed.refresh_id} <= kept
        assert expired.refresh_id not in kept
        assert admitted(conn, pipeline)[2] == second.bundle_id
    # The first snapshot owns the second result's image and must survive despite its age.
    with ctx.engine.connect() as conn:
        images = conn.execute(
            text(
                "SELECT METADATA_SNAPSHOT AS snapshot FROM AUD_PIPELINE_REFRESHES "
                "WHERE REFRESH_ID=:id"
            ),
            {"id": first.refresh_id},
        ).scalar_one()
    assert images is not None


def test_released_upgrade_refreshes_unseeded_pipelines_without_failing_others(
    empty_engine_db, tmp_path
):
    from etl_craft.engine.migrations import apply_pending_migrations
    from etl_craft.services.doctor import run_checks
    from etl_craft.services.refresh_upgrade import initialize
    from fixtures.released_schema import install

    db = empty_engine_db
    install(db, "0.3.0")
    config, _script = project_files(db, tmp_path)
    with db.engine.begin() as conn:
        working = add_pipeline(conn, "WORKING")
        broken = add_pipeline(conn, "BROKEN")
        add_task(conn, working, "T", "PYTHON", SCRIPT_FILE="python_scripts/work.py")
        add_task(conn, broken, "T", "PYTHON", SCRIPT_FILE="python_scripts/missing.py")
    apply_pending_migrations(
        db.engine, project_default=tmp_path / "migrations", warehouse_dialect="duckdb"
    )
    refreshed = initialize(db.engine, config)
    statuses = {row.pipeline_code: row.status for row in refreshed.refreshes}
    assert statuses == {"WORKING": RunStatus.SUCCESS, "BROKEN": RunStatus.FAILED}
    db.engine.update_execution_options(refresh_required=True)
    with db.engine.begin() as conn:
        created = transitions.create_run(conn, working, SYSTEM_ACTOR)
        assert created > 0
        with pytest.raises(RunRefusedError, match="no version has passed yet") as refused:
            transitions.create_run(conn, broken, SYSTEM_ACTOR)
    assert "missing.py" in str(refused.value)
    assert "etl-craft pipelines refresh" in str(refused.value)
    checks = run_checks(config)
    assert any(
        check.name == "Pipeline refresh BROKEN" and "missing.py" in check.detail for check in checks
    )
    again = initialize(db.engine, config)
    assert all(result.pipeline_code != "WORKING" for result in again.refreshes)


def test_upgrade_seeds_the_last_complete_successful_run_and_preserves_it(
    empty_engine_db, tmp_path, monkeypatch
):
    import json
    import shutil

    from etl_craft.core.text import sha256_hex
    from etl_craft.dialects.engine.base import EngineDialect
    from etl_craft.engine import migrations
    from etl_craft.engine.project_files import collect
    from etl_craft.services.operations.bundles import PublishBundle, publish
    from etl_craft.services.refresh_upgrade import initialize
    from fixtures.released_schema import install

    db = empty_engine_db
    install(db, "0.3.0")
    previous = tmp_path / "before-refresh"
    previous.mkdir()
    for migration in db.dialect.migrations_dir().glob("*.sql"):
        if migration.name < "0067":
            shutil.copy2(migration, previous / migration.name)
    with monkeypatch.context() as patch:
        patch.setattr(EngineDialect, "migrations_dir", lambda self: previous)
        migrations.apply_pending_migrations(db.engine, warehouse_dialect="duckdb")
    config, script = project_files(db, tmp_path)
    ctx = OperationContext(
        db.engine, config, SYSTEM_ACTOR, role=ApiRole.ADMIN, project="default", project_id=1
    )
    with db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "SEEDED")
        add_task(conn, pipeline, "T", "PYTHON", SCRIPT_FILE="python_scripts/work.py")
    publication = publish(ctx, PublishBundle(files=collect(config.project_dir)))
    with db.engine.begin() as conn:
        image = {}
        for name in (
            "CFG_PIPELINES",
            "CFG_TASKS",
            "CFG_TASK_PARAMETERS",
            "CFG_TASK_DEPENDENCY",
            "CFG_PIPELINE_DEPENDENCY",
            "CFG_BUSINESS_RULES",
            "CFG_TABLES",
            "CFG_COLUMNS",
        ):
            selected = conn.execute(text(f"SELECT * FROM {name}"))
            columns = selected.keys()
            image[name] = {
                "columns": [column.lower() for column in columns],
                "rows": [
                    {key.lower(): value for key, value in row.items()}
                    for row in selected.mappings()
                ],
            }
        snapshot = json.dumps(image, default=str)
        conn.execute(
            text(
                "INSERT INTO AUD_PIPELINES_RUN_LOG "
                "(PIPELINE_ID,STATUS,RUN_DATE,BACKFILL,STARTED_BY,STARTED_BY_KIND,"
                "BUNDLE_ID,METADATA_VERSION,METADATA_SNAPSHOT) "
                "VALUES (:pipeline,'SUCCESS',:run_date,'N','system','SYSTEM',"
                ":bundle,:version,:snapshot)"
            ),
            {
                "pipeline": pipeline,
                "run_date": "2026-10-28",
                "bundle": publication.bundle_id,
                "version": sha256_hex(snapshot.encode()),
                "snapshot": snapshot,
            },
        )
    migrations.apply_pending_migrations(db.engine, warehouse_dialect="duckdb")
    script.unlink()
    assert initialize(db.engine, config).refreshes == ()
    with db.engine.connect() as conn:
        assert admitted(conn, pipeline)[2] == publication.bundle_id
    db.engine.update_execution_options(refresh_required=True)
    outcome = act(ctx, ActionName.RUN, pipeline_code="SEEDED")
    assert outcome.status == RunStatus.SUCCESS, outcome


def test_unchanged_refresh_preserves_findings_and_reports_changed_files(refresh_project):
    ctx, _, script = refresh_project
    first = act(ctx, ActionName.PIPELINE_REFRESH).refreshes[0]
    second = act(ctx, ActionName.PIPELINE_REFRESH).refreshes[0]
    assert first.changed
    assert not second.changed
    assert second.bundle_id == first.bundle_id
    assert second.previous_refreshed_at == first.refreshed_at
    script.write_text("def run(:\n    pass\n")
    broken = act(ctx, ActionName.PIPELINE_REFRESH).refreshes[0]
    assert broken.changed
    assert "files: python_scripts/work.py" in broken.changes
    unchanged = act(ctx, ActionName.PIPELINE_REFRESH).refreshes[0]
    assert not unchanged.changed
    assert unchanged.status == RunStatus.FAILED
    assert unchanged.findings == broken.findings


def test_refresh_cli_reports_unchanged_results_in_text_and_json(cli_project):
    import json

    project = cli_project
    code, output = project.run("pipelines", "refresh")
    assert code == 0, output
    assert "P: no changes since the last refresh at" in output
    code, output = project.run("pipelines", "refresh", "--format", "json")
    assert code == 0, output
    assert json.loads(output)["refreshes"][0]["changed"] is False
    (project.config.python_scripts_dir / "load.py").write_text("def run(:\n    pass\n")
    code, output = project.run("pipelines", "refresh")
    assert code != 0
    assert "files: python_scripts/load.py" in output
    code, output = project.run("pipelines", "refresh")
    assert code != 0
    assert "no changes since the last refresh" in output
    assert "load.py" in output


def test_refresh_retention_releases_only_unreferenced_bundle_objects(refresh_project):
    from datetime import UTC, datetime, timedelta
    from pathlib import Path

    from etl_craft.core.text import sha256_hex
    from etl_craft.engine.project_files import collect
    from etl_craft.services.operations.bundles import PublishBundle, publish
    from etl_craft.services.retention import retaining
    from etl_craft.services.retention_bundles import cleanup_bundles

    ctx, _, script = refresh_project
    obsolete_bytes = b"def run(task):\n    return None\n"
    script.write_bytes(obsolete_bytes)
    obsolete = publish(ctx, PublishBundle(files=collect(ctx.config.project_dir)))
    script.write_text("def run(task):\n    return 1\n")
    good = act(ctx, ActionName.PIPELINE_REFRESH).refreshes[0]
    config = replace(ctx.config, retention=replace(ctx.config.retention, config_history_days=1))
    with retaining():
        assert (
            cleanup_bundles(
                ctx.engine, config, datetime.now(UTC) + timedelta(days=2), dry_run=False
            )
            == 1
        )
    with ctx.engine.connect() as conn:
        retained = Bundle.load(conn, good.bundle_id)
        assert retained.read(config, "python_scripts/work.py") == script.read_bytes()
        assert (
            conn.execute(
                text("SELECT COUNT(*) AS count FROM AUD_PROJECT_BUNDLES WHERE BUNDLE_ID=:id"),
                {"id": obsolete.bundle_id},
            ).scalar_one()
            == 0
        )
    assert not (
        Path(config.artifacts.store.removeprefix("file://"))
        / "objects"
        / sha256_hex(obsolete_bytes)
    ).exists()


def test_refresh_api_requires_project_management_and_history_requires_view(refresh_project):
    from fastapi.testclient import TestClient

    from etl_craft.api.app import create_app
    from etl_craft.services.operations.actions import ACTIONS, action_path

    ctx, _, _ = refresh_project
    admin = act(ctx, ActionName.TOKEN_CREATE, name="refresh-admin", role=ApiRole.ADMIN)
    viewer = act(ctx, ActionName.TOKEN_CREATE, name="refresh-viewer", role=ApiRole.VIEWER)
    manager = act(ctx, ActionName.TOKEN_CREATE, name="refresh-manager", role=ApiRole.VIEWER)
    for account, permission in (("refresh-viewer", "CAN_VIEW"), ("refresh-manager", "CAN_MANAGE")):
        act(
            ctx,
            ActionName.ACCESS_GRANT,
            project="default",
            account=account,
            permission=permission,
            reason="refresh test",
        )
    client = TestClient(create_app(ctx))
    route = action_path(ACTIONS[ActionName.PIPELINE_REFRESH]).format(project="default")
    read = action_path(ACTIONS[ActionName.PIPELINE_REFRESH_HISTORY]).format(project="default")
    assert client.post(route, json={}).status_code == 401
    for token, expected in ((viewer.token, 403), (manager.token, 200), (admin.token, 200)):
        response = client.post(route, json={}, headers={"Authorization": "Bearer " + token})
        assert response.status_code == expected, response.text
    response = client.get(read, headers={"Authorization": "Bearer " + viewer.token})
    assert response.status_code == 200, response.text
    assert response.json()["refreshes"]


def test_concurrent_refreshes_serialize_and_share_one_snapshot(refresh_project):
    from etl_craft.services.operations import pipeline_refresh
    from fixtures.races import two_at_once

    ctx, pipeline, _ = refresh_project
    results = two_at_once(
        lambda: act(ctx, ActionName.PIPELINE_REFRESH),
        lambda: act(ctx, ActionName.PIPELINE_REFRESH),
        at=(pipeline_refresh, "refresh"),
    )
    assert all(not result.failed for result in results)
    assert {result.refreshes[0].changed for result in results} == {True, False}
    with ctx.engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT METADATA_SNAPSHOT AS snapshot,SNAPSHOT_REFRESH_ID AS reference "
                "FROM AUD_PIPELINE_REFRESHES WHERE PIPELINE_ID=:id ORDER BY REFRESH_ID"
            ),
            {"id": pipeline},
        ).all()
        assert len(rows) == 2
        assert rows[0].snapshot is not None and rows[0].reference is None
        assert rows[1].snapshot is None and rows[1].reference is not None


@pytest.mark.parametrize("problem", ["cycle", "parameter", "missing script"])
def test_failed_refresh_lists_metadata_findings_without_losing_the_last_good_version(
    refresh_project, problem
):
    from fixtures.metadata import add_dependency

    ctx, pipeline, script = refresh_project
    good = act(ctx, ActionName.PIPELINE_REFRESH).refreshes[0]
    if problem == "missing script":
        script.unlink()
    else:
        with ctx.engine.begin() as conn:
            task = conn.execute(
                text("SELECT TASK_ID AS task_id FROM CFG_TASKS WHERE PIPELINE_ID=:pipeline"),
                {"pipeline": pipeline},
            ).scalar_one()
            if problem == "cycle":
                other = add_task(
                    conn, pipeline, "OTHER", "PYTHON", SCRIPT_FILE="python_scripts/work.py"
                )
                add_dependency(conn, pipeline, task, other)
                add_dependency(conn, pipeline, other, task)
            else:
                conn.execute(
                    text(
                        "INSERT INTO CFG_TASK_PARAMETERS (TASK_ID,PARAMETER_NAME,PARAMETER_VALUE) "
                        "VALUES (:task,'TASK_TIMEOUT_SECONDS','soon')"
                    ),
                    {"task": task},
                )
    failed = act(ctx, ActionName.PIPELINE_REFRESH).refreshes[0]
    assert failed.status == RunStatus.FAILED
    expected = {"cycle": "cycle", "parameter": "soon", "missing script": "work.py"}[problem]
    assert any(expected in finding.message for finding in failed.findings)
    assert all(finding.file and finding.remedy and finding.value for finding in failed.findings)
    with ctx.engine.connect() as conn:
        assert admitted(conn, pipeline)[2] == good.bundle_id
    listed = act(ctx, ActionName.PIPELINES)
    assert any(row.pipeline_code == "P" for row in listed.pipelines)


@pytest.mark.parametrize(
    "empty_engine_db",
    [pytest.param("postgresql", marks=pytest.mark.engine_postgres)],
    indirect=True,
)
def test_controller_refreshes_postgres_direct_edits_without_polling_files(refresh_project):
    import threading
    from contextvars import Context

    from etl_craft.controller.refresh import refresh_due

    ctx, pipeline, _ = refresh_project
    good = act(ctx, ActionName.PIPELINE_REFRESH).refreshes[0]
    act(
        ctx,
        ActionName.PROJECT_SET,
        project="default",
        reason="direct edit test",
        environment_type="DEV",
        governance="RELAXED",
    )

    def edit():
        with ctx.engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE CFG_PIPELINES SET DESCRIPTION='directly changed' WHERE PIPELINE_ID=:id"
                ),
                {"id": pipeline},
            )

    Context().run(edit)
    refresh_due(ctx, threading.Event())
    history = act(ctx, ActionName.PIPELINE_REFRESH_HISTORY, pipeline_code="P")
    assert len(history.refreshes) == 2
    assert history.refreshes[0].metadata_version != good.metadata_version
    assert history.refreshes[0].status == RunStatus.SUCCESS
    refresh_due(ctx, threading.Event())
    assert len(act(ctx, ActionName.PIPELINE_REFRESH_HISTORY, pipeline_code="P").refreshes) == 2


@pytest.mark.parametrize(
    "empty_engine_db", [pytest.param("sqlite", marks=pytest.mark.engine_sqlite)], indirect=True
)
def test_sqlite_default_artifacts_survive_restart_and_execute_the_passing_bundle(refresh_project):
    ctx, _, script = refresh_project
    ctx = replace(ctx, config=replace(ctx.config, artifacts=Artifacts(None)))
    first = act(ctx, ActionName.PIPELINE_REFRESH).refreshes[0]
    with ctx.engine.connect() as conn:
        bundle = Bundle.load(conn, first.bundle_id)
        assert bundle.store == (ctx.config.project_dir / ".etl-craft" / "artifacts").as_uri()
    script.unlink()
    ctx.engine.dispose()
    ctx.engine.update_execution_options(refresh_required=True)
    outcome = act(ctx, ActionName.RUN, pipeline_code="P")
    assert outcome.status == RunStatus.SUCCESS, outcome
    with ctx.engine.connect() as conn:
        assert (
            conn.execute(text("SELECT TARGET_COUNT AS count FROM AUD_TASK_RUN_LOG")).scalar_one()
            == 1
        )


def test_broken_sql_refresh_records_the_file_and_keeps_the_pipeline_listed(cli_project):
    from etl_craft.config import load_config

    project = cli_project
    sql = project.config.project_dir / "sql_scripts" / "bad.sql"
    sql.parent.mkdir(exist_ok=True)
    sql.write_text("SELECT * FROM\n")
    with project.engine.begin() as conn:
        add_task(
            conn,
            project.pipeline_id,
            "BAD_SQL",
            "SQL",
            SQL_ACTION="CREATE_TABLE",
            TARGET_OBJECT="main.bad_refresh",
            SCRIPT_FILE="sql_scripts/bad.sql",
        )
    ctx = OperationContext(
        project.engine,
        load_config(project.config.config_path),
        SYSTEM_ACTOR,
        role=ApiRole.ADMIN,
        project="default",
        project_id=1,
    )
    result = act(ctx, ActionName.PIPELINE_REFRESH).refreshes[0]
    assert result.status == RunStatus.FAILED, result
    assert any(finding.file.endswith("bad.sql") for finding in result.findings), result.findings
    assert any(row.pipeline_code == "P" for row in act(ctx, ActionName.PIPELINES).pipelines)
    from etl_craft.services.validate import validate

    report = validate(ctx.engine, ctx.config)
    assert report.failed
    for required in ("sql_scripts/bad.sql", "line 1", "column", "sqlglot", "duckdb", "SQL_SCRIPT"):
        assert any(required in finding.message for finding in result.findings)
        assert any(required in finding.message for finding in report.findings)
    ctx.engine.update_execution_options(refresh_required=True)
    outcome = act(ctx, ActionName.RUN, pipeline_code="P")
    assert outcome.status == RunStatus.SUCCESS, outcome


def test_sql_script_refresh_does_not_require_sqlglot_to_parse_unmanaged_statements(cli_project):
    project = cli_project
    script = project.config.project_dir / "sql_scripts" / "vendor.sql"
    script.parent.mkdir(exist_ok=True)
    script.write_text("SELECT * FROM\n")
    with project.engine.begin() as conn:
        add_task(
            conn, project.pipeline_id, "VENDOR", "SQL_SCRIPT", SCRIPT_FILE="sql_scripts/vendor.sql"
        )
    ctx = OperationContext(
        project.engine,
        project.config,
        SYSTEM_ACTOR,
        role=ApiRole.ADMIN,
        project="default",
        project_id=1,
    )
    result = act(ctx, ActionName.PIPELINE_REFRESH).refreshes[0]
    assert result.status == RunStatus.SUCCESS, result.findings


def test_bundle_upload_does_not_hold_an_engine_db_write_transaction(refresh_project):
    from etl_craft.engine.artifacts import ArtifactStore
    from etl_craft.engine.project_files import collect
    from etl_craft.services.operations.bundles import PublishBundle, publish
    from fixtures.races import paused_after

    ctx, pipeline, _ = refresh_project
    request = PublishBundle(files=collect(ctx.config.project_dir))
    with (
        paused_after(
            lambda: publish(ctx, request),
            owner=ArtifactStore,
            name="write",
            when=lambda store, *_: store.location == ctx.config.artifacts.store,
        ),
        ctx.engine.begin() as conn,
    ):
        conn.execute(
            text("UPDATE CFG_PIPELINES SET DESCRIPTION='work continues' WHERE PIPELINE_ID=:id"),
            {"id": pipeline},
        )


def test_admission_uses_the_passing_bundles_gate_policy(refresh_project):
    from etl_craft.core.enums import GatePolicy
    from etl_craft.execution.run_start import prepare

    ctx, _, script = refresh_project
    raw = yaml.safe_load(ctx.config.config_path.read_text())
    raw["Orchestration"]["Dependency_gates"] = "WARN"
    ctx.config.config_path.write_text(yaml.safe_dump(raw, sort_keys=False))
    assert not act(ctx, ActionName.PIPELINE_REFRESH).failed
    raw["Orchestration"]["Dependency_gates"] = "OFF"
    ctx.config.config_path.write_text(yaml.safe_dump(raw, sort_keys=False))
    script.write_text("def run(:\n")
    assert act(ctx, ActionName.PIPELINE_REFRESH).failed
    ctx.engine.update_execution_options(refresh_required=True)
    _, _, pinned = prepare(ctx.engine, ctx.config, "P")
    assert pinned.dependency_gates == GatePolicy.WARN


def test_concurrent_admission_pins_the_passing_refresh_once(refresh_project):
    from etl_craft.core.errors import RunStateError
    from etl_craft.engine import runlog
    from etl_craft.execution.run_start import init_pipeline_run
    from fixtures.races import two_at_once

    ctx, _, _ = refresh_project
    good = act(ctx, ActionName.PIPELINE_REFRESH).refreshes[0]
    ctx.engine.update_execution_options(refresh_required=True)

    def initialize(key):
        try:
            return init_pipeline_run(
                ctx.engine, ctx.config, "P", selector=runlog.RunSelector(run_key=key)
            )
        except RunStateError as error:
            return error

    results = two_at_once(
        lambda: initialize("first"),
        lambda: initialize("second"),
        at="etl_craft.execution.run_start.check_gate_now",
    )
    assert sum(isinstance(result, RunStateError) for result in results) == 1
    with ctx.engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT STATUS AS status,METADATA_VERSION AS version,BUNDLE_ID AS bundle_id "
                "FROM AUD_PIPELINES_RUN_LOG"
            )
        ).all()
        assert sum(row.status == RunStatus.IN_PROGRESS for row in rows) == 1
        assert all(
            row.version == good.metadata_version and row.bundle_id == good.bundle_id for row in rows
        )


def test_validate_warehouse_probe_does_not_hold_an_engine_db_writer(cli_project):
    from etl_craft.services.validate import validate
    from fixtures.metadata import add_source_column

    project = cli_project
    with project.engine.begin() as conn:
        add_source_column(conn, "main.source_contract", "id", "INTEGER", 1)
    result = validate(project.engine, project.config)
    assert not any("database is locked" in finding.message for finding in result.findings)
    assert not result.failed, result.findings


def test_bundle_retention_keeps_objects_referenced_through_a_store_alias(refresh_project):
    from datetime import UTC, datetime, timedelta

    from etl_craft.engine.project_files import collect
    from etl_craft.services.operations.bundles import PublishBundle, publish
    from etl_craft.services.retention import retaining
    from etl_craft.services.retention_bundles import cleanup_bundles

    ctx, _, script = refresh_project
    alias = replace(
        ctx,
        config=replace(
            ctx.config,
            artifacts=replace(ctx.config.artifacts, store=ctx.config.artifacts.store + "/"),
        ),
    )
    publish(alias, PublishBundle(files=collect(ctx.config.project_dir)))
    good = act(ctx, ActionName.PIPELINE_REFRESH).refreshes[0]
    config = replace(ctx.config, retention=replace(ctx.config.retention, config_history_days=1))
    with retaining():
        assert (
            cleanup_bundles(
                ctx.engine, config, datetime.now(UTC) + timedelta(days=2), dry_run=False
            )
            == 1
        )
    with ctx.engine.connect() as conn:
        assert (
            Bundle.load(conn, good.bundle_id).read(config, "python_scripts/work.py")
            == script.read_bytes()
        )


def test_refresh_between_preflight_and_admission_uses_the_admitted_gate_policy(
    refresh_project, monkeypatch
):
    from etl_craft.core.enums import GatePolicy
    from etl_craft.execution import run_start

    ctx, _, _ = refresh_project
    raw = yaml.safe_load(ctx.config.config_path.read_text())
    raw["Orchestration"]["Dependency_gates"] = "WARN"
    ctx.config.config_path.write_text(yaml.safe_dump(raw, sort_keys=False))
    assert not act(ctx, ActionName.PIPELINE_REFRESH).failed
    ctx.engine.update_execution_options(refresh_required=True)
    select = run_start._select_run
    check = run_start.check_gate_now
    policies = []

    def publish_after_preflight(*args, **kwargs):
        selected = select(*args, **kwargs)
        raw["Orchestration"]["Dependency_gates"] = "OFF"
        ctx.config.config_path.write_text(yaml.safe_dump(raw, sort_keys=False))
        assert not act(ctx, ActionName.PIPELINE_REFRESH).failed
        return selected

    def observed(*args, **kwargs):
        policies.append(kwargs["policy"])
        return check(*args, **kwargs)

    monkeypatch.setattr(run_start, "_select_run", publish_after_preflight)
    monkeypatch.setattr(run_start, "check_gate_now", observed)
    outcome = run_start.init_pipeline_run(ctx.engine, ctx.config, "P")
    assert outcome.status == RunStatus.IN_PROGRESS
    assert policies == [GatePolicy.OFF]


def test_manual_selection_uses_the_last_passing_refresh_before_creating_a_run(cli_project):
    project = cli_project
    with project.engine.begin() as conn:
        add_task(conn, project.pipeline_id, "NEW", "EMAIL_ALERT")
    code, output = project.run("run", "--pipeline-code", "P", "--task-code", "NEW", "--standalone")
    assert code != 0, output
    assert "NEW" in output
    with project.engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM AUD_PIPELINES_RUN_LOG")).scalar_one() == 0
