"""Renames preserve identity, safely carry references and refuse stale configuration."""

import json
from dataclasses import replace
from datetime import UTC, datetime

import pytest
from sqlalchemy import text

from etl_craft.core.actor import SYSTEM_ACTOR, acting_as
from etl_craft.core.enums import ActionName, ActionOutcome, ApiRole, ControlAction, RunStatus
from etl_craft.core.errors import MetadataFileError, RunRefusedError, UsageError
from etl_craft.engine import transitions
from etl_craft.engine.logs import read_log_attempt
from etl_craft.engine.repository.events import consumed_files, consumed_keys, record_event
from etl_craft.engine.repository.offsets import StoredOffset, fetch_task_offset, save_task_offset
from etl_craft.execution.attempt_records import attempt_log_path, local_attempt_log
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.retention import retaining
from etl_craft.services.retention_files import LogRemoval, retire_log_rows
from fixtures.actions import act
from fixtures.metadata import add_pipeline, add_task, finish_run, start_run, task_run
from fixtures.races import two_at_once


@pytest.fixture
def rename_project(metadata_project):
    project = metadata_project
    script = project.root / "python_scripts/sales/fetch_orders.py"
    script.write_text(
        "from etl_craft.scripting import ScriptResult\ndef run(task):\n"
        "    return ScriptResult(row_count=0 if task.offset else 100, offset=task.offset)\n"
    )
    with project.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        task = add_task(
            conn, pipeline, "T", "PYTHON", SCRIPT_FILE="python_scripts/sales/fetch_orders.py"
        )
    ctx = OperationContext(
        project.engine,
        project.config,
        SYSTEM_ACTOR,
        role=ApiRole.ADMIN,
        project="default",
        project_id=1,
    )
    return ctx, pipeline, task, script


def rename(ctx, **changes):
    return act(
        ctx, ActionName.RENAME, pipeline_code="P", to="NEW", reason="preserve identity", **changes
    )


@pytest.mark.parametrize(
    "kind,value",
    [
        ("NUMBER", "42"),
        ("TIMESTAMP", "2026-01-01T00:00:00+00:00"),
        ("PARTITIONS", '{"east":42,"west":7}'),
    ],
)
def test_rename_preserves_offsets_and_events(rename_project, kind, value):
    ctx, pipeline, task, _ = rename_project
    with ctx.engine.begin() as conn:
        old_run = start_run(conn, pipeline)
        old_task_run = task_run(conn, task, old_run)
        record_event(
            conn, task, old_task_run, {"files": [{"path": "/feed/once.csv"}], "keys": ["once"]}
        )
        save_task_offset(conn, task, StoredOffset(kind, value))
        finish_run(conn, old_run)
    act(
        ctx,
        ActionName.CONTROL,
        pipeline_code="P",
        task_code="T",
        action=ControlAction.PAUSE,
        reason="standing hold",
    )
    first = rename(ctx)
    assert first.pipeline_id == pipeline and first.task_id is None
    second = act(
        ctx, ActionName.RENAME, pipeline_code="NEW", task_code="T", to="NEXT", reason="identity"
    )
    assert second.pipeline_id == pipeline and second.task_id == task
    with ctx.engine.connect() as conn:
        assert fetch_task_offset(conn, task) == StoredOffset(kind, value)
        assert consumed_files(conn, task) == {"/feed/once.csv"}
        assert consumed_keys(conn, task, ["once", "other"]) == {"once"}
        assert (
            conn.execute(
                text("SELECT TASK_ID AS task_id FROM AUD_PIPELINE_PAUSES WHERE RESUMED_AT IS NULL")
            ).scalar_one()
            == task
        )
    act(
        ctx,
        ActionName.CONTROL,
        pipeline_code="NEW",
        task_code="NEXT",
        action=ControlAction.RESUME,
        reason="resume",
    )
    done = act(ctx, ActionName.RUN, pipeline_code="NEW", run_key="after-rename")
    assert done.status == RunStatus.SUCCESS, done
    with ctx.engine.connect() as conn:
        assert fetch_task_offset(conn, task) == StoredOffset(kind, value)
        assert consumed_keys(conn, task, ["once"]) == {"once"}
        assert (
            conn.execute(
                text(
                    "SELECT TARGET_COUNT AS count FROM AUD_TASK_RUN_LOG WHERE PIPELINE_RUN_ID=:id"
                ),
                {"id": done.run.pipeline_run_id},
            ).scalar_one()
            == 0
        )


@pytest.mark.parametrize("task_selection", [False, True])
@pytest.mark.parametrize("state", [RunStatus.QUEUED, RunStatus.IN_PROGRESS])
def test_non_terminal_runs_refuse_rename(rename_project, task_selection, state):
    ctx, pipeline, _, _ = rename_project
    with ctx.engine.begin() as conn:
        run = transitions.create_run(conn, pipeline, SYSTEM_ACTOR, status=state)
    import threading
    from concurrent.futures import ThreadPoolExecutor
    from contextvars import copy_context

    from etl_craft.handlers.resources import LocalLocker
    from etl_craft.warehouse.connection import single_writer_lock

    ready, release = threading.Event(), threading.Event()

    def writer():
        with single_writer_lock(ctx.config, LocalLocker(ctx.engine, ctx.config)):
            ready.set()
            assert release.wait(timeout=10), "rename waited for a busy warehouse before refusing"

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(copy_context().run, writer)
        assert ready.wait(timeout=5)
        try:
            with pytest.raises(RunRefusedError) as error:
                rename(ctx, **({"task_code": "T"} if task_selection else {}))
        finally:
            release.set()
        future.result(timeout=5)
    message = str(error.value)
    for part in ("P", str(run), state, "expected no non-terminal", "finish or cancel"):
        assert part in message
    with ctx.engine.connect() as conn:
        assert (
            conn.execute(
                text("SELECT PIPELINE_CODE AS code FROM CFG_PIPELINES WHERE PIPELINE_ID=:id"),
                {"id": pipeline},
            ).scalar_one()
            == "P"
        )


def test_rename_refuses_pending_window_on_terminal_run(rename_project):
    ctx, pipeline, task, _ = rename_project
    with ctx.engine.begin() as conn:
        run = start_run(conn, pipeline)
        task_id = task_run(conn, task, run)
        finish_run(conn, run, status=RunStatus.FAILED)
        save_task_offset(conn, task, StoredOffset("TIMESTAMP", "2026-01-01T00:00:00+00:00"))
        conn.execute(
            text(
                "UPDATE AUD_TASK_OFFSET_TRACKER SET PENDING_TASK_RUN_ID=:run, "
                "PENDING_UPPER_BOUND=:high WHERE TASK_ID=:task"
            ),
            {"run": task_id, "task": task, "high": datetime(2026, 1, 2, tzinfo=UTC)},
        )
    with pytest.raises(RunRefusedError, match=r"pending window.*explicitly abandon"):
        rename(ctx)


@pytest.mark.parametrize("code", ["T", "P"])
def test_existing_and_invalid_codes_name_the_remedy(rename_project, code):
    ctx, _pipeline, _, _ = rename_project
    with ctx.engine.begin() as conn:
        add_pipeline(conn, "OTHER")
    with pytest.raises(UsageError, match=r"already exists.*unused code"):
        act(
            ctx,
            ActionName.RENAME,
            pipeline_code="P",
            task_code="T" if code == "T" else None,
            to=code,
            reason="collision",
        )
    for invalid in ("../bad", "1bad", "a" * 129, "bad-code", "bad/code"):
        with pytest.raises(UsageError, match=r"expected an ASCII letter.*valid unused code"):
            act(ctx, ActionName.RENAME, pipeline_code="P", to=invalid, reason="invalid")


def test_failed_refresh_rolls_back_rename_and_history(rename_project):
    ctx, pipeline, _, script = rename_project
    script.write_text("def run(:\n")
    with ctx.engine.connect() as conn:
        before = conn.execute(
            text("SELECT COUNT(*) AS n FROM AUD_CFG_PIPELINES_HISTORY")
        ).scalar_one()
    with pytest.raises(RunRefusedError, match="rename refresh failed") as error:
        rename(ctx)
    assert "correct" in str(error.value).lower()
    with ctx.engine.connect() as conn:
        assert (
            conn.execute(
                text("SELECT PIPELINE_CODE AS code FROM CFG_PIPELINES WHERE PIPELINE_ID=:id"),
                {"id": pipeline},
            ).scalar_one()
            == "P"
        )
        assert (
            conn.execute(text("SELECT COUNT(*) AS n FROM AUD_CFG_PIPELINES_HISTORY")).scalar_one()
            == before
        )
        assert (
            conn.execute(text("SELECT COUNT(*) AS n FROM AUD_PIPELINE_REFRESHES")).scalar_one() == 0
        )
        assert conn.execute(text("SELECT COUNT(*) AS n FROM AUD_PROJECT_BUNDLES")).scalar_one() == 0


@pytest.mark.parametrize("task_rename", [False, True])
@pytest.mark.parametrize("minimal", [False, True])
def test_old_config_refuses_renamed_away_code(rename_project, tmp_path, task_rename, minimal):
    ctx, _, _, _ = rename_project
    directory = tmp_path / "stale"
    directory.mkdir()
    filename = "tasks.csv" if task_rename else "pipelines.csv"
    payload = (
        "PIPELINE_CODE,TASK_CODE,TASK_TYPE,HANDLER\nP,T,ETL,PYTHON\n"
        if task_rename
        else "PIPELINE_CODE,PIPELINE_NAME,REFRESH_TYPE\nP,P,FULL\n"
    )
    if minimal:
        payload = "PIPELINE_CODE,TASK_CODE\nP,T\n" if task_rename else "PIPELINE_CODE\nP\n"
    (directory / filename).write_text(payload)
    rename(ctx, **({"task_code": "T"} if task_rename else {}))
    for action in (ActionName.CONFIG_PLAN, ActionName.CONFIG_APPLY):
        with pytest.raises(MetadataFileError) as error:
            act(ctx, action, config_dir=directory)
        for part in (
            filename,
            "line 2",
            "renamed to 'NEW'",
            "etl-craft",
            " at ",
            "update the file, or use a different code",
        ):
            assert part in str(error.value)


@pytest.mark.parametrize("task_rename", [False, True])
def test_logs_and_retention_keep_recorded_paths_after_rename(rename_project, task_rename):
    ctx, pipeline, task, _ = rename_project
    with ctx.engine.begin() as conn:
        run = start_run(conn, pipeline)
        summary = task_run(conn, task, run)
        finish_run(conn, run)
        attempt = conn.execute(
            text(
                "INSERT INTO AUD_TASK_ATTEMPTS (TASK_RUN_ID,ATTEMPT_NUMBER,STATUS) VALUES "
                "(:id,1,'SUCCESS') RETURNING ATTEMPT_ID AS id"
            ),
            {"id": summary},
        ).scalar_one()
        path = attempt_log_path(ctx.config, "P", run, "T", 1, project_id=1)
        path.parent.mkdir(parents=True)
        path.write_text("old log remains readable")
        conn.execute(
            text("UPDATE AUD_TASK_ATTEMPTS SET LOG_PATH=:path WHERE ATTEMPT_ID=:id"),
            {"id": attempt, "path": str(path)},
        )
    rename(ctx, **({"task_code": "T"} if task_rename else {}))
    assert b"".join(act(ctx, ActionName.LOGS, attempt_id=attempt)) == b"old log remains readable"
    from fastapi.testclient import TestClient

    from etl_craft.api.app import create_app
    from etl_craft.services.operations.actions import ACTIONS, action_path

    token = act(ctx, ActionName.TOKEN_CREATE, name="log-reader", role=ApiRole.ADMIN)
    client = TestClient(create_app(ctx))
    route = action_path(ACTIONS[ActionName.LOGS]).format(project="default", attempt_id=attempt)
    response = client.get(
        route, params={"attempt_id": attempt}, headers={"Authorization": "Bearer " + token.token}
    )
    assert response.status_code == 200 and response.content == b"old log remains readable"

    with retaining(), ctx.engine.begin() as conn:
        row = read_log_attempt(conn, attempt)
        assert local_attempt_log(ctx.config, row) == path
        retire_log_rows(
            conn,
            ctx.config,
            [LogRemoval(attempt, str(path), 1, summary, run, "P", "T")],
            datetime.now(UTC),
        )
        assert read_log_attempt(conn, attempt).log_removed_at is not None
        assert conn.execute(
            text("SELECT LOG_PATH AS path FROM AUD_TASK_ATTEMPTS WHERE ATTEMPT_ID=:id"),
            {"id": attempt},
        ).scalar_one() == str(path)
    assert path.exists()


@pytest.mark.parametrize("referencing_parent", [False, True])
def test_rename_racing_run_start_is_serialized(rename_project, referencing_parent):
    ctx, pipeline, _, _ = rename_project
    admitted = pipeline
    if referencing_parent:
        with ctx.engine.begin() as conn:
            admitted = add_pipeline(conn, "PARENT")
            add_task(
                conn,
                admitted,
                "start",
                "RUN_PIPELINE",
                TRIGGER_PIPELINE_CODE="P",
                TRIGGER_MODE="DETACH",
            )

    def renamer():
        try:
            return rename(ctx)
        except RunRefusedError:
            return None

    def starter():
        with acting_as(SYSTEM_ACTOR), ctx.engine.begin() as conn:
            return transitions.create_run(conn, admitted, SYSTEM_ACTOR)

    changed, run = two_at_once(renamer, starter, at=(ctx.engine, "begin"), timeout=30)
    assert run > 0
    with ctx.engine.connect() as conn:
        code = conn.execute(
            text("SELECT PIPELINE_CODE AS code FROM CFG_PIPELINES WHERE PIPELINE_ID=:id"),
            {"id": pipeline},
        ).scalar_one()
    assert code == ("P" if changed is None else "NEW")


def test_grants_follow_identity_without_widening_project_access(rename_project):
    from etl_craft.core.enums_access import Permission
    from etl_craft.core.errors import PermissionDeniedError
    from etl_craft.services.operations.permissions import pipeline_access

    ctx, pipeline, _, _ = rename_project
    act(ctx, ActionName.PROJECT_ADD, project="OTHER", reason="separate scope")
    with ctx.engine.begin() as conn:
        other = conn.execute(
            text("SELECT PROJECT_ID AS id FROM CFG_PROJECTS WHERE PROJECT_CODE='OTHER'")
        ).scalar_one()
        add_pipeline(conn, "P", project_id=other)
    act(
        ctx,
        ActionName.ACCESS_GRANT,
        project="OTHER",
        role=ApiRole.VIEWER,
        pipeline="P",
        permission=Permission.CAN_MANAGE,
        reason="other project only",
    )
    viewer = replace(ctx, role=ApiRole.VIEWER)
    with pytest.raises(PermissionDeniedError, match="CAN_MANAGE"):
        rename(viewer)
    act(
        ctx,
        ActionName.ACCESS_GRANT,
        project="default",
        role=ApiRole.VIEWER,
        pipeline="P",
        permission=Permission.CAN_RUN,
        reason="direct access",
    )
    with ctx.engine.connect() as conn:
        before = conn.execute(
            text(
                "SELECT PIPELINE_GRANT_ID AS id FROM CFG_PIPELINE_GRANTS "
                "WHERE PROJECT_ID=1 AND PIPELINE_CODE='P'"
            )
        ).scalar_one()
        assert pipeline_access(conn, viewer).levels["P"] == Permission.CAN_RUN
    rename(ctx)
    with ctx.engine.connect() as conn:
        assert pipeline_access(conn, viewer).levels["NEW"] == Permission.CAN_RUN
        row = conn.execute(
            text(
                "SELECT PIPELINE_GRANT_ID AS id,PIPELINE_CODE AS code,PERMISSION AS "
                "permission FROM CFG_PIPELINE_GRANTS WHERE PIPELINE_GRANT_ID=:id"
            ),
            {"id": before},
        ).one()
        assert row.id == before and row.code == "NEW" and row.permission == Permission.CAN_RUN
        history = conn.execute(
            text(
                "SELECT PIPELINE_CODE AS code,ACTOR AS actor FROM "
                "AUD_CFG_PIPELINE_GRANTS_HISTORY WHERE PIPELINE_GRANT_ID=:id ORDER BY "
                "CHANGE_ID"
            ),
            {"id": before},
        ).all()
        assert history[-1].code == "NEW" and history[-1].actor == SYSTEM_ACTOR.name
        assert history[-2].code == "P"
        untouched = conn.execute(
            text(
                "SELECT PIPELINE_CODE AS code,PERMISSION AS permission "
                "FROM CFG_PIPELINE_GRANTS WHERE PROJECT_ID=:id"
            ),
            {"id": other},
        ).one()
        assert untouched.code == "P" and untouched.permission == Permission.CAN_MANAGE

    # A pipeline grant never grants project management.
    with pytest.raises(PermissionDeniedError, match="CAN_MANAGE"):
        act(viewer, ActionName.RENAME, pipeline_code="NEW", to="OTHER", reason="refused")
    assert pipeline > 0


def test_rename_requires_management_in_the_selected_project(rename_project):
    from etl_craft.core.enums_access import Permission
    from etl_craft.core.errors import ResourceNotFoundError

    ctx, pipeline, _, _ = rename_project
    act(ctx, ActionName.PROJECT_ADD, project="OTHER", reason="separate project")
    act(
        ctx,
        ActionName.ACCESS_GRANT,
        project="default",
        role=ApiRole.OPERATOR,
        permission=Permission.CAN_MANAGE,
        reason="project manager",
    )
    act(ctx, ActionName.PIPELINE_REFRESH)
    manager = replace(ctx, role=ApiRole.OPERATOR)
    with pytest.raises(ResourceNotFoundError, match=r"not permitted|not found"):
        rename(replace(manager, project="OTHER", project_id=None))
    assert rename(manager).pipeline_id == pipeline


def test_unupdatable_reference_refuses_without_changing_metadata(rename_project):
    ctx, pipeline, task, _ = rename_project
    with ctx.engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO CFG_TASK_PARAMETERS (TASK_ID,PARAMETER_NAME,PARAMETER_VALUE) "
                "VALUES (:id,'WATERMARK_SOURCE_PIPELINES','broken-json')"
            ),
            {"id": task},
        )
    with pytest.raises(RunRefusedError) as error:
        rename(ctx)
    for part in (
        "P.T",
        "WATERMARK_SOURCE_PIPELINES",
        "broken-json",
        "cannot be updated safely",
        "correct the parameter",
    ):
        assert part in str(error.value)
    with ctx.engine.connect() as conn:
        assert (
            conn.execute(
                text("SELECT PIPELINE_CODE AS code FROM CFG_PIPELINES WHERE PIPELINE_ID=:id"),
                {"id": pipeline},
            ).scalar_one()
            == "P"
        )


def test_code_parameters_follow_the_pipeline_with_typed_history(rename_project):
    ctx, _pipeline, _, _ = rename_project
    with ctx.engine.begin() as conn:
        parent = add_pipeline(conn, "PARENT")
        trigger = add_task(
            conn, parent, "start", "RUN_PIPELINE", TRIGGER_PIPELINE_CODE="P", TRIGGER_MODE="DETACH"
        )
        # Inactive definitions still carry references that may later be reactivated.
        source = add_task(conn, parent, "reader", "SQL", WATERMARK_SOURCE_PIPELINES='["P"]')
        conn.execute(text("UPDATE CFG_TASKS SET ACTIVE_FLAG='N' WHERE TASK_ID=:id"), {"id": source})
    rename(ctx)
    with ctx.engine.connect() as conn:
        params = {
            r.task_id: r.value
            for r in conn.execute(
                text(
                    "SELECT TASK_ID AS task_id,PARAMETER_VALUE AS value FROM "
                    "CFG_TASK_PARAMETERS WHERE PARAMETER_NAME IN "
                    "('TRIGGER_PIPELINE_CODE','WATERMARK_SOURCE_PIPELINES')"
                )
            )
        }
        assert params[trigger] == "NEW" and json.loads(params[source]) == ["NEW"]
        assert (
            conn.execute(
                text(
                    "SELECT COUNT(*) AS n FROM AUD_CFG_TASK_PARAMETERS_HISTORY WHERE "
                    "MIGRATION='rename' AND PARAMETER_NAME IN "
                    "('TRIGGER_PIPELINE_CODE','WATERMARK_SOURCE_PIPELINES')"
                )
            ).scalar_one()
            == 2
        )


def test_plan_warns_about_retire_and_insert_without_blocking(rename_project, tmp_path):
    ctx, _, _, _ = rename_project
    directory = tmp_path / "replacement"
    directory.mkdir()
    (directory / "tasks.csv").write_text(
        "PIPELINE_CODE,TASK_CODE,TASK_TYPE,HANDLER,ACTIVE_FLAG\nP,T,ETL,PYTHON,N\nP,NEXT,ETL,PYTHON,Y\n"
    )
    (directory / "task_parameters.csv").write_text(
        "PIPELINE_CODE,TASK_CODE,PARAMETER_NAME,PARAMETER_VALUE\nP,NEXT,SCRIPT_FILE,python_scripts/sales/fetch_orders.py\nP,NEXT,SCRIPT_KIND,INGESTION\n"
    )
    planned = act(ctx, ActionName.CONFIG_PLAN, config_dir=directory)
    assert not planned.failed
    assert any(
        "likely rename" in f.message and "etl-craft rename" in f.message for f in planned.findings
    )
    applied = act(ctx, ActionName.CONFIG_APPLY, config_dir=directory)
    assert applied.applied and not applied.failed


@pytest.mark.parametrize("policy", ["STRICT", "RELAXED"])
def test_generated_api_rename_uses_project_permission_and_reason(rename_project, policy):
    from fastapi.testclient import TestClient

    from etl_craft.api.app import create_app
    from etl_craft.services.operations.actions import ACTIONS, action_path

    ctx, _, _, _ = rename_project
    act(ctx, ActionName.PIPELINE_REFRESH)
    token = act(ctx, ActionName.TOKEN_CREATE, name="api-manager", role=ApiRole.VIEWER)
    client = TestClient(create_app(ctx))
    headers = {"Authorization": "Bearer " + token.token}
    path = action_path(ACTIONS[ActionName.RENAME]).format(project="default")
    payload = {"pipeline_code": "P", "to": "NEW"}
    denied = client.post(path, json={**payload, "reason": "test"}, headers=headers)
    assert denied.status_code == 403
    act(
        ctx,
        ActionName.ACCESS_GRANT,
        project="default",
        account="api-manager",
        permission="CAN_MANAGE",
        reason="delegate",
    )
    act(ctx, ActionName.PROJECT_SET, project="default", governance=policy, reason="policy")
    response = client.post(path, json=payload, headers=headers)
    if policy == "STRICT":
        assert response.status_code >= 400 and "STRICT" in response.text
        response = client.post(path, json={**payload, "reason": "rename"}, headers=headers)
    assert response.status_code == 200, response.text
    assert response.json()["new_code"] == "NEW"
    with ctx.engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT OUTCOME AS outcome FROM AUD_ACTIONS "
                "WHERE COMMAND='rename' ORDER BY ACTION_ID"
            )
        ).all()
        assert (
            rows[0].outcome == ActionOutcome.REFUSED and rows[-1].outcome == ActionOutcome.REQUESTED
        )


def test_pipeline_dependency_consumption_survives_rename(rename_project):
    from etl_craft.engine.repository.trackers import fetch_pipeline_last_consumed
    from fixtures.metadata import add_pipeline_dependency

    ctx, pipeline, _, _ = rename_project
    with ctx.engine.begin() as conn:
        upstream = add_pipeline(conn, "SOURCE")
        dependency = add_pipeline_dependency(conn, pipeline, upstream)
        source_run = start_run(conn, upstream)
        finish_run(conn, source_run)
        run = start_run(conn, pipeline)
        finish_run(conn, run)
        conn.execute(
            text(
                "INSERT INTO AUD_DEPENDENCY_CONSUMPTION "
                "(PIPELINE_DEPENDENCY_ID,PIPELINE_ID,PIPELINE_RUN_ID,"
                "DEPENDS_ON_PIPELINE_ID,CONSUMED_PIPELINE_RUN_ID) "
                "VALUES (:dep,:pipe,:run,:up,:source)"
            ),
            {"dep": dependency, "pipe": pipeline, "run": run, "up": upstream, "source": source_run},
        )
        before = fetch_pipeline_last_consumed(conn, dependency)
    rename(ctx)
    with ctx.engine.connect() as conn:
        assert fetch_pipeline_last_consumed(conn, dependency) == before
        assert (
            conn.execute(
                text(
                    "SELECT PIPELINE_ID AS id FROM CFG_PIPELINE_DEPENDENCY "
                    "WHERE PIPELINE_DEPENDENCY_ID=:id"
                ),
                {"id": dependency},
            ).scalar_one()
            == pipeline
        )


def test_rename_lists_project_files_still_naming_the_old_code(rename_project):
    ctx, _, _, _ = rename_project
    directory = ctx.config.project_dir / "config"
    directory.mkdir()
    (directory / "pipelines.csv").write_text("PIPELINE_CODE,PIPELINE_NAME,REFRESH_TYPE\nP,P,FULL\n")
    result = rename(ctx)
    assert str(directory / "pipelines.csv") in result.config_files
    assert "team-owned CFG_" in result.message and "team SQL" in result.message


def test_stale_code_reservations_survive_metadata_retention(rename_project, tmp_path):
    from etl_craft.config.model import Retention
    from etl_craft.services.retention import plain_history

    ctx, _, _, _ = rename_project
    rename(ctx)
    act(ctx, ActionName.RENAME, pipeline_code="NEW", task_code="T", to="NEXT", reason="task")
    act(ctx, ActionName.RENAME, pipeline_code="NEW", to="FINAL", reason="second pipeline rename")
    config = replace(ctx.config, retention=Retention(config_history_days=1))
    plain_history(ctx.engine, config, datetime(2100, 1, 1, tzinfo=UTC), dry_run=False)
    directory = tmp_path / "stale-after-retention"
    directory.mkdir()
    (directory / "pipelines.csv").write_text("PIPELINE_CODE,PIPELINE_NAME,REFRESH_TYPE\nP,P,FULL\n")
    with pytest.raises(MetadataFileError, match="renamed to 'NEW'"):
        act(ctx, ActionName.CONFIG_PLAN, config_dir=directory)
    (directory / "pipelines.csv").unlink()
    (directory / "tasks.csv").write_text(
        "PIPELINE_CODE,TASK_CODE,TASK_TYPE,HANDLER\nFINAL,T,ETL,PYTHON\n"
    )
    with pytest.raises(MetadataFileError, match="renamed to 'NEXT'"):
        act(ctx, ActionName.CONFIG_PLAN, config_dir=directory)


def test_rename_refuses_unrefreshable_cross_project_reference(rename_project):
    from fixtures.metadata import add_pipeline_dependency

    ctx, pipeline, _, _ = rename_project
    act(ctx, ActionName.PROJECT_ADD, project="OTHER", reason="other project")
    with ctx.engine.begin() as conn:
        other = conn.execute(
            text("SELECT PROJECT_ID AS id FROM CFG_PROJECTS WHERE PROJECT_CODE='OTHER'")
        ).scalar_one()
        dependent = add_pipeline(conn, "DEPENDENT", project_id=other)
        add_pipeline_dependency(conn, dependent, pipeline)
    with pytest.raises(RunRefusedError) as error:
        rename(ctx)
    for part in (
        "P",
        "cross-project",
        "DEPENDENT",
        "cannot be refreshed",
        "removing those dependencies",
    ):
        assert part in str(error.value)


def test_unrelated_failed_pipeline_does_not_block_rename(rename_project):
    ctx, _, _, _ = rename_project
    with ctx.engine.begin() as conn:
        other = add_pipeline(conn, "BROKEN")
        add_task(conn, other, "missing", "PYTHON", SCRIPT_FILE="python_scripts/missing.py")
    result = rename(ctx)
    assert {r.pipeline_code for r in result.refreshes} == {"NEW"}


def test_rename_waits_for_bundle_upload_without_holding_a_writer(rename_project, monkeypatch):
    import threading
    from concurrent.futures import ThreadPoolExecutor
    from contextvars import copy_context

    from etl_craft.engine import locks
    from etl_craft.engine.artifacts import ArtifactStore
    from etl_craft.engine.project_files import collect
    from etl_craft.services.operations.bundles import PublishBundle, publish
    from fixtures.races import paused_after

    ctx, pipeline, _, _ = rename_project
    waiting = threading.Event()
    renamer = []
    original = locks.EngineLock.hold

    def observed(lock, *args, **kwargs):
        if lock == locks.BUNDLES and renamer and threading.get_ident() == renamer[0]:
            waiting.set()
        return original(lock, *args, **kwargs)

    def change():
        renamer.append(threading.get_ident())
        return rename(ctx)

    monkeypatch.setattr(locks.EngineLock, "hold", observed)
    upload = PublishBundle(files=collect(ctx.config.project_dir))
    with ThreadPoolExecutor(max_workers=1) as pool:
        with paused_after(
            lambda: publish(ctx, upload),
            owner=ArtifactStore,
            name="write",
            when=lambda store, *_: store.location == ctx.config.artifacts.store,
        ):
            future = pool.submit(copy_context().run, change)
            assert waiting.wait(timeout=5), "rename did not reach bundle publication"
            with ctx.engine.begin() as conn:
                conn.execute(
                    text(
                        "UPDATE CFG_PIPELINES SET DESCRIPTION='writer proceeds' "
                        "WHERE PIPELINE_ID=:id"
                    ),
                    {"id": pipeline},
                )
        assert future.result(timeout=20).new_code == "NEW"


def test_unreadable_helper_file_refuses_before_renaming(rename_project):
    ctx, pipeline, _, _ = rename_project
    directory = ctx.config.project_dir / "config"
    directory.mkdir()
    path = directory / "pipelines.csv"
    path.write_bytes(b"\xff\xfe")
    with pytest.raises(MetadataFileError) as error:
        rename(ctx)
    for part in (str(path), "cannot inspect", "expected a readable UTF-8", "correct its encoding"):
        assert part in str(error.value)
    with ctx.engine.connect() as conn:
        assert (
            conn.execute(
                text("SELECT PIPELINE_CODE AS code FROM CFG_PIPELINES WHERE PIPELINE_ID=:id"),
                {"id": pipeline},
            ).scalar_one()
            == "P"
        )


def test_rename_refresh_checks_warehouse_source_columns_without_nested_writer(rename_project):
    from etl_craft.warehouse.connection import build_warehouse_engine
    from fixtures.metadata import add_source_column

    ctx, _, _, _ = rename_project
    warehouse = build_warehouse_engine(ctx.config)
    try:
        with warehouse.begin() as conn:
            conn.exec_driver_sql("CREATE TABLE main.source_contract (id INTEGER)")
    finally:
        warehouse.dispose()
    with ctx.engine.begin() as conn:
        add_source_column(conn, "main.source_contract", "id", "INTEGER", 1)
    result = rename(ctx)
    assert result.new_code == "NEW"
    with ctx.engine.begin() as conn:
        add_source_column(conn, "main.source_contract", "missing_column", "INTEGER", 2)
    with pytest.raises(RunRefusedError, match="warehouse lacks column missing_column"):
        act(ctx, ActionName.RENAME, pipeline_code="NEW", task_code="T", to="NEXT", reason="check")
    with ctx.engine.connect() as conn:
        assert conn.execute(text("SELECT TASK_CODE AS code FROM CFG_TASKS")).scalar_one() == "T"
        assert conn.execute(text("SELECT COUNT(*) AS n FROM AUD_RESOURCE_LOCKS")).scalar_one() == 0


def test_api_rename_uses_the_selected_projects_published_files(rename_project):
    import yaml
    from fastapi.testclient import TestClient

    from etl_craft.api.app import create_app
    from etl_craft.config.project import project_config
    from etl_craft.services.operations.actions import ACTIONS, action_path

    ctx, original, _, script = rename_project
    act(ctx, ActionName.PROJECT_ADD, project="OTHER", reason="another project")
    directory = ctx.config.project_dir / "other-project"
    directory.mkdir()
    scripts = directory / "python_scripts"
    scripts.mkdir()
    (scripts / "work.py").write_text(script.read_text())
    raw = yaml.safe_load(ctx.config.config_path.read_text())
    raw["Orchestration"]["Project"] = "OTHER"
    raw["Warehouse"]["dev"]["Jdbc_url"] = f"jdbc:duckdb:{directory / 'analytics.duckdb'}"
    path = directory / "craft-connector.yml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    with ctx.engine.begin() as conn:
        project = conn.execute(
            text("SELECT PROJECT_ID AS id FROM CFG_PROJECTS WHERE PROJECT_CODE='OTHER'")
        ).scalar_one()
        pipeline = add_pipeline(conn, "P", project_id=project)
        add_task(conn, pipeline, "T", "PYTHON", SCRIPT_FILE="python_scripts/work.py")
    other = replace(
        ctx, config=project_config(ctx.config, path), project="OTHER", project_id=project
    )
    assert not act(other, ActionName.PIPELINE_REFRESH).failed
    token = act(ctx, ActionName.TOKEN_CREATE, name="other-manager", role=ApiRole.VIEWER)
    act(
        ctx,
        ActionName.ACCESS_GRANT,
        project="OTHER",
        account="other-manager",
        permission="CAN_MANAGE",
        reason="delegate this project",
    )
    client = TestClient(create_app(ctx))
    route = action_path(ACTIONS[ActionName.RENAME]).format(project="OTHER")
    response = client.post(
        route,
        json={"pipeline_code": "P", "to": "NEW", "reason": "rename"},
        headers={"Authorization": "Bearer " + token.token},
    )
    assert response.status_code == 200, response.text
    assert response.json()["pipeline_id"] == pipeline
    with ctx.engine.connect() as conn:
        assert (
            conn.execute(
                text("SELECT PIPELINE_CODE AS code FROM CFG_PIPELINES WHERE PIPELINE_ID=:id"),
                {"id": original},
            ).scalar_one()
            == "P"
        )


@pytest.mark.parametrize("parameter", ["TRIGGER_PIPELINE_CODE", "WATERMARK_SOURCE_PIPELINES"])
def test_rename_refuses_active_code_reference(rename_project, parameter):
    ctx, pipeline, _, _ = rename_project
    with ctx.engine.begin() as conn:
        parent = add_pipeline(conn, "PARENT")
        add_task(
            conn,
            parent,
            "start",
            "RUN_PIPELINE",
            **{parameter: "P" if parameter == "TRIGGER_PIPELINE_CODE" else '["P"]'},
        )
        run = start_run(conn, parent)
    with pytest.raises(RunRefusedError, match=f"referenced pipeline_id={parent}.*{run}"):
        rename(ctx)
    with ctx.engine.connect() as conn:
        assert (
            conn.execute(
                text("SELECT PIPELINE_CODE AS code FROM CFG_PIPELINES WHERE PIPELINE_ID=:id"),
                {"id": pipeline},
            ).scalar_one()
            == "P"
        )
