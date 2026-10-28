"""Project grants gate every operation, while restricted pipelines keep their own policy."""

from dataclasses import replace

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from etl_craft.api.app import create_app
from etl_craft.core.actor import Actor, ActorKind, acting_as
from etl_craft.core.enums import ActionName, ActionScope, ApiRole, HttpMethod, Permission
from etl_craft.core.errors import PermissionDeniedError, ResourceNotFoundError
from etl_craft.engine.repository.pipelines import selected_project
from etl_craft.services.operations import OperationContext
from etl_craft.services.operations.actions import ACTIONS, action_path, invoke
from etl_craft.services.operations.permissions import pipeline_access
from etl_craft.services.operations.serialization import encode
from fixtures.actions import act, build
from fixtures.metadata import add_pipeline

OWNER = Actor("owner", ActorKind.USER)


@pytest.fixture
def isolation(engine_db):
    admin = OperationContext(engine_db.engine, engine_db.config, OWNER, role=ApiRole.ADMIN)
    tokens = {}
    for name, role in (
        ("reader", ApiRole.VIEWER),
        ("operator", ApiRole.OPERATOR),
        ("manager", ApiRole.VIEWER),
    ):
        token = act(admin, ActionName.TOKEN_CREATE, name=name, role=role)
        tokens[name] = token.token
    for project in ("sales", "finance", "hidden"):
        act(admin, ActionName.PROJECT_ADD, project=project, reason="Isolation fixture")
        with acting_as(OWNER), engine_db.engine.begin() as conn:
            identity = conn.execute(
                text("SELECT PROJECT_ID AS project_id FROM CFG_PROJECTS WHERE PROJECT_CODE=:code"),
                {"code": project},
            ).scalar_one()
            with selected_project(identity):
                for code in ("OPEN", "PRIVATE"):
                    add_pipeline(conn, code, project_id=identity)
    grant(admin, "sales", "reader", Permission.CAN_VIEW)
    grant(admin, "sales", "operator", Permission.CAN_RUN)
    grant(admin, "sales", "manager", Permission.CAN_MANAGE)
    grant(admin, "sales", "reader", Permission.CAN_RUN, pipeline="PRIVATE")
    return admin, tokens


def grant(admin, project, account, permission, **target):
    return act(
        admin,
        ActionName.ACCESS_GRANT,
        project=project,
        account=account,
        permission=permission,
        reason="Test permission",
        **target,
    )


def caller(admin, name, role=ApiRole.VIEWER, project="sales"):
    return replace(
        admin,
        actor=Actor(name, ActorKind.SERVICE),
        role=role,
        project=project,
        project_id=None,
        config=replace(admin.config, project=project),
    )


def test_project_gate_restricted_fallback_and_manager_override(isolation):
    admin, _ = isolation
    with admin.engine.connect() as conn:
        sales = conn.execute(
            text("SELECT PROJECT_ID AS project_id FROM CFG_PROJECTS WHERE PROJECT_CODE='sales'")
        ).scalar_one()

        def levels(name, role=ApiRole.VIEWER):
            ctx = replace(caller(admin, name, role), project_id=sales)
            return pipeline_access(conn, ctx).levels

        assert levels("reader") == {"OPEN": Permission.CAN_VIEW, "PRIVATE": Permission.CAN_RUN}
        assert levels("operator", ApiRole.OPERATOR) == {"OPEN": Permission.CAN_RUN, "PRIVATE": None}
        assert levels("manager") == {
            "OPEN": Permission.CAN_MANAGE,
            "PRIVATE": Permission.CAN_MANAGE,
        }
    grant(admin, "hidden", "reader", Permission.CAN_MANAGE, pipeline="PRIVATE")
    with pytest.raises(ResourceNotFoundError):
        act(caller(admin, "reader", project="hidden"), ActionName.PIPELINES)


@pytest.mark.parametrize("role", [ApiRole.VIEWER, ApiRole.OPERATOR, ApiRole.DEVELOPER])
def test_all_project_actions_mask_hidden_and_missing_projects(isolation, role):
    admin, _ = isolation
    for name, action in ACTIONS.items():
        if action.scope == ActionScope.DEPLOYMENT or action.call is None:
            continue
        request = build(action.request)
        failures = []
        for code in ("hidden", "missing"):
            with pytest.raises(ResourceNotFoundError) as caught:
                invoke(caller(admin, "reader", role, project=code), name, request)
            failures.append(
                (
                    type(caught.value),
                    caught.value.exit_code,
                    str(caught.value).replace(repr(code), "'selected'"),
                )
            )
        assert failures[0] == failures[1], name


def test_all_project_routes_refuse_without_project_grants(isolation):
    admin, tokens = isolation
    api = TestClient(create_app(admin))
    for name, action in ACTIONS.items():
        if action.scope == ActionScope.DEPLOYMENT or action.path is None:
            continue
        request = build(action.request)
        fields = encode(request)
        replies = []
        for project in ("hidden", "missing"):
            values = dict(fields)
            values["project"] = project
            import re

            path = action_path(action).format(
                **{key: values.pop(key) for key in re.findall(r"{([a-z_]+)}", action_path(action))}
            )
            values = {key: value for key, value in values.items() if value is not None}
            headers = {"Authorization": "Bearer " + tokens["reader"]}
            response = (
                api.get(path, params=values, headers=headers)
                if action.method == HttpMethod.GET
                else api.post(path, json=values, headers=headers)
            )
            assert response.status_code == 404, (name, response.text)
            replies.append(response.text.replace(repr(project), "'selected'"))
        assert replies[0] == replies[1], name


def test_project_manager_delegates_inside_its_project_only(isolation):
    admin, _ = isolation
    manager = caller(admin, "manager")
    grant(manager, "sales", "operator", Permission.CAN_VIEW, pipeline="PRIVATE")
    with pytest.raises(ResourceNotFoundError):
        grant(manager, "finance", "reader", Permission.CAN_MANAGE)
    for name in (
        ActionName.PROJECT_ADD,
        ActionName.POOL_ADD,
        ActionName.ACCOUNT_ADD,
        ActionName.GROUP_ADD,
    ):
        with pytest.raises(PermissionDeniedError):
            invoke(manager, name, build(ACTIONS[name].request))
    listed = act(caller(admin, "reader"), ActionName.PROJECT_LIST, reason="List allowed projects")
    assert {p.project for p in listed.projects} == {"default", "sales"}


def test_project_edit_and_dependency_permissions(isolation, tmp_path):
    from etl_craft.config.object_models import Artifacts

    admin, _ = isolation
    admin = replace(
        admin, config=replace(admin.config, artifacts=Artifacts((tmp_path / "artifacts").as_uri()))
    )
    folder = tmp_path / "changes"
    folder.mkdir()
    (folder / "pipeline_dependencies.csv").write_text(
        "PIPELINE_CODE,DEPENDS_ON_PROJECT_CODE,DEPENDS_ON_PIPELINE_CODE,DEPENDENCY_TYPE\n"
        "OPEN,finance,OPEN,SUCCESS\n"
    )
    for name, role in (("reader", ApiRole.VIEWER), ("operator", ApiRole.OPERATOR)):
        ctx = caller(admin, name, role)
        for action in (ActionName.CONFIG_PLAN, ActionName.CONFIG_APPLY):
            with pytest.raises(PermissionDeniedError, match="CAN_MANAGE"):
                act(ctx, action, config_dir=folder)
    manager = caller(admin, "manager")
    with pytest.raises(ResourceNotFoundError) as caught:
        act(manager, ActionName.CONFIG_APPLY, config_dir=folder)
    for part in ("pipeline_dependencies.csv line 2", "sales", "finance", "CAN_VIEW", "manager"):
        assert part in str(caught.value)
    (folder / "pipeline_dependencies.csv").write_text(
        "PIPELINE_CODE,DEPENDS_ON_PROJECT_CODE,DEPENDS_ON_PIPELINE_CODE,DEPENDENCY_TYPE\n"
        "OPEN,missing,OPEN,SUCCESS\n"
    )
    with pytest.raises(ResourceNotFoundError) as missing:
        act(manager, ActionName.CONFIG_APPLY, config_dir=folder)
    assert str(caught.value).replace("'finance'", "'selected'") == str(missing.value).replace(
        "'missing'", "'selected'"
    )
    (folder / "pipeline_dependencies.csv").write_text(
        "PIPELINE_CODE,DEPENDS_ON_PROJECT_CODE,DEPENDS_ON_PIPELINE_CODE,DEPENDENCY_TYPE\n"
        "OPEN,finance,OPEN,SUCCESS\n"
    )
    grant(admin, "finance", "manager", Permission.CAN_VIEW)
    done = act(manager, ActionName.CONFIG_APPLY, config_dir=folder)
    assert done.applied and not done.failed, done.findings
    assert act(manager, ActionName.CONFIG_APPLY, config_dir=folder).changes == ()
    from fixtures.metadata import add_task, start_run

    with acting_as(OWNER), admin.engine.begin() as conn:
        project = conn.execute(
            text("SELECT PROJECT_ID AS project_id FROM CFG_PROJECTS WHERE PROJECT_CODE='finance'")
        ).scalar_one()
        pipeline = conn.execute(
            text(
                "SELECT PIPELINE_ID AS pipeline_id FROM CFG_PIPELINES WHERE PROJECT_ID=:id "
                "AND PIPELINE_CODE='OPEN'"
            ),
            {"id": project},
        ).scalar_one()
        add_task(conn, pipeline, "T", "PYTHON")
        start_run(conn, pipeline)
    upstream = caller(admin, "manager", project="finance")
    assert act(upstream, ActionName.EXPLAIN, pipeline_code="OPEN", task_code="T").dependents == (
        "sales.OPEN",
    )
    act(
        admin,
        ActionName.ACCESS_REVOKE,
        project="sales",
        account="manager",
        reason="Remove project access",
    )
    assert act(upstream, ActionName.EXPLAIN, pipeline_code="OPEN", task_code="T").dependents == ()


def test_project_log_paths(isolation, tmp_path):
    from etl_craft.engine import transitions
    from etl_craft.engine.logs import read_log_attempt
    from etl_craft.execution.attempt_records import attempt_log_path, local_attempt_log
    from fixtures.metadata import add_task, start_run

    admin, _ = isolation
    config = replace(admin.config, log_dir=tmp_path / "logs")
    identities = []
    with acting_as(OWNER), admin.engine.begin() as conn:
        for code in ("sales", "finance"):
            project = conn.execute(
                text("SELECT PROJECT_ID AS project_id FROM CFG_PROJECTS WHERE PROJECT_CODE=:code"),
                {"code": code},
            ).scalar_one()
            pipeline = conn.execute(
                text(
                    "SELECT PIPELINE_ID AS pipeline_id FROM CFG_PIPELINES WHERE "
                    "PROJECT_ID=:id AND PIPELINE_CODE='OPEN'"
                ),
                {"id": project},
            ).scalar_one()
            task = add_task(conn, pipeline, "T", "PYTHON")
            run = start_run(conn, pipeline)
            summary = transitions.find_or_create_task_run(conn, task, run).task_run_id
            attempt = transitions.queue_attempt(conn, summary, OWNER)
            path = attempt_log_path(config, "OPEN", run, "T", 1, project_id=project)
            path.parent.mkdir(parents=True)
            path.write_text(code + " full log")
            conn.execute(
                text("UPDATE AUD_TASK_ATTEMPTS SET LOG_PATH=:path WHERE ATTEMPT_ID=:id"),
                {"id": attempt, "path": str(path)},
            )
            identities.append((code, project, attempt, path))
    assert identities[0][3] != identities[1][3]
    for code, project, attempt, path in identities:
        assert path.parts[-4:] == (
            f"project-{project}",
            "OPEN",
            path.parent.name,
            "T.attempt-1.log",
        )
        ctx = replace(admin, config=replace(config, project=code), project=code)
        assert (
            b"".join(act(ctx, ActionName.LOGS, attempt_id=attempt)) == (code + " full log").encode()
        )
        legacy = config.log_dir / path.relative_to(config.log_dir / f"project-{project}")
        legacy.parent.mkdir(parents=True, exist_ok=True)
        path.rename(legacy)
        with acting_as(OWNER), admin.engine.begin() as conn:
            conn.execute(
                text("UPDATE AUD_TASK_ATTEMPTS SET LOG_PATH=:path WHERE ATTEMPT_ID=:id"),
                {"id": attempt, "path": str(legacy)},
            )
            row = read_log_attempt(conn, attempt)
            assert local_attempt_log(config, row) == legacy
        assert (
            b"".join(act(ctx, ActionName.LOGS, attempt_id=attempt)) == (code + " full log").encode()
        )


def test_project_grant_revocation_stops_admitted_manual_children(isolation):
    from etl_craft.engine import transitions
    from etl_craft.services.operations.child_admission import check_child_start
    from fixtures.metadata import add_task

    admin, _ = isolation
    manager = caller(admin, "manager")
    with acting_as(manager.actor), admin.engine.begin() as conn:
        project = conn.execute(
            text("SELECT PROJECT_ID AS project_id FROM CFG_PROJECTS WHERE PROJECT_CODE='sales'")
        ).scalar_one()
        pipeline = conn.execute(
            text(
                "SELECT PIPELINE_ID AS pipeline_id FROM CFG_PIPELINES WHERE PROJECT_ID=:id "
                "AND PIPELINE_CODE='OPEN'"
            ),
            {"id": project},
        ).scalar_one()
        add_task(conn, pipeline, "T", "PYTHON")
        run = transitions.create_run(conn, pipeline, manager.actor)
        assert check_child_start(admin.engine, manager.config, conn, run) is None
    act(
        admin,
        ActionName.ACCESS_REVOKE,
        project="sales",
        account="manager",
        reason="Revoke admitted project",
    )
    with admin.engine.connect() as conn:
        message = check_child_start(admin.engine, manager.config, conn, run)
    assert message is not None and "sales" in message and "missing or not permitted" in message


def test_revoked_project_refuses_a_local_claim_before_process_dispatch(isolation, monkeypatch):
    from functools import partial

    from etl_craft.engine import transitions
    from etl_craft.execution.attempt_types import ChildOptions
    from etl_craft.execution.pools import AttemptSpec
    from etl_craft.execution.runner import try_execute_attempt
    from etl_craft.services.operations.child_admission import check_child_start
    from fixtures.metadata import add_task

    admin, _ = isolation
    manager = caller(admin, "manager")
    with acting_as(manager.actor), admin.engine.begin() as conn:
        project = conn.execute(
            text("SELECT PROJECT_ID AS project_id FROM CFG_PROJECTS WHERE PROJECT_CODE='sales'")
        ).scalar_one()
        pipeline = conn.execute(
            text(
                "SELECT PIPELINE_ID AS pipeline_id FROM CFG_PIPELINES WHERE PROJECT_ID=:id "
                "AND PIPELINE_CODE='OPEN'"
            ),
            {"id": project},
        ).scalar_one()
        task = add_task(conn, pipeline, "T", "PYTHON")
        run = transitions.create_run(conn, pipeline, manager.actor)
        summary = transitions.find_or_create_task_run(conn, task, run).task_run_id
        attempt = transitions.queue_attempt(conn, summary, manager.actor)
    act(admin, ActionName.ACCESS_REVOKE, project="sales", account="manager", reason="Withdraw")

    def no_process(*args, **kwargs):
        pytest.fail("a revoked account dispatched a task process")

    monkeypatch.setattr("etl_craft.execution.runner.run_child", no_process)
    child = ChildOptions(before_start=partial(check_child_start, admin.engine, admin.config))
    spec = AttemptSpec(attempt, 1, summary, run, "OPEN", "T", "PYTHON", 0, 60)
    with acting_as(OWNER):
        done = try_execute_attempt(admin.engine, admin.config, spec, child)
    assert done is not None and "sales" in done.message and "not permitted" in done.message
    with admin.engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT STATUS AS status,ERROR_MESSAGE AS error_message,TASK_LOG AS task_log "
                "FROM AUD_TASK_ATTEMPTS WHERE ATTEMPT_ID=:id"
            ),
            {"id": attempt},
        ).one()
    assert row.status == "CANCELLED" and row.error_message == done.message
    assert row.task_log == done.message


@pytest.mark.parametrize("principal", ["account", "group", "role"])
@pytest.mark.parametrize("level", list(Permission))
def test_project_principals_and_levels_keep_restricted_rules(isolation, principal, level):
    admin, _ = isolation
    identity = {"account": "reader"} if principal == "account" else {"role": "viewer"}
    if principal == "group":
        act(admin, ActionName.GROUP_ADD, name="project-readers")
        act(admin, ActionName.GROUP_ADD_MEMBER, name="project-readers", account="reader")
        identity = {"group": "project-readers"}
    act(
        admin,
        ActionName.ACCESS_GRANT,
        project="finance",
        permission=level,
        reason="Principal matrix",
        **identity,
    )
    grant(admin, "finance", "operator", Permission.CAN_VIEW, pipeline="PRIVATE")
    ctx = caller(admin, "reader", project="finance")
    listed = {row.pipeline_code for row in act(ctx, ActionName.PIPELINES).pipelines}
    assert ("PRIVATE" in listed) == (level == Permission.CAN_MANAGE)
    assert "OPEN" in listed
    with admin.engine.connect() as conn:
        project = conn.execute(
            text("SELECT PROJECT_ID AS project_id FROM CFG_PROJECTS WHERE PROJECT_CODE='finance'")
        ).scalar_one()
        effective = pipeline_access(conn, replace(ctx, project_id=project)).levels
    assert effective["OPEN"] == level
    assert effective["PRIVATE"] == (level if level == Permission.CAN_MANAGE else None)


def test_project_revoke_and_delegated_grant_race(isolation):
    from fixtures.races import two_at_once

    admin, _ = isolation
    manager = caller(admin, "manager")

    def delegated():
        try:
            return grant(manager, "sales", "operator", Permission.CAN_MANAGE, pipeline="PRIVATE")
        except ResourceNotFoundError:
            return None

    def revoke():
        return act(
            admin,
            ActionName.ACCESS_REVOKE,
            project="sales",
            account="manager",
            reason="Race revoke",
        )

    first, _ = two_at_once(delegated, revoke, at="etl_craft.services.operations.access.inputs")
    with pytest.raises(ResourceNotFoundError):
        act(manager, ActionName.PIPELINES)
    with admin.engine.connect() as conn:
        rows = (
            conn.execute(
                text(
                    "SELECT g.PERMISSION AS permission FROM CFG_PIPELINE_GRANTS g JOIN "
                    "CFG_ACCOUNTS a ON a.ACCOUNT_ID=g.ACCOUNT_ID WHERE "
                    "a.ACCOUNT_NAME='operator' AND g.PIPELINE_CODE='PRIVATE' AND "
                    "g.ACTIVE_FLAG='Y'"
                )
            )
            .scalars()
            .all()
        )
    assert rows == ([Permission.CAN_MANAGE] if first is not None else [])


def test_full_config_export_never_discloses_a_hidden_pipeline(isolation, tmp_path):
    admin, _ = isolation
    target = tmp_path / "export"
    with pytest.raises(PermissionDeniedError, match="CAN_VIEW on every pipeline"):
        act(
            caller(admin, "operator", ApiRole.OPERATOR), ActionName.CONFIG_EXPORT, config_dir=target
        )
    assert not target.exists()
    done = act(caller(admin, "manager"), ActionName.CONFIG_EXPORT, config_dir=target)
    assert done is not None
    assert "PRIVATE" in (target / "pipelines.csv").read_text()


def test_bulk_project_grants_are_atomic_and_new_projects_are_closed(isolation):
    admin, _ = isolation
    assert act(admin, ActionName.ACCESS_LIST, project="finance").grants == ()
    content = (
        "PROJECT,GROUP_NAME,ROLE,PIPELINE_CODE,TAG,PERMISSION,ACTION\n"
        "finance,,viewer,,,CAN_VIEW,GRANT\n"
    )
    preview = act(admin, ActionName.ACCESS_GRANT, content=content, dry_run=True, reason="Review")
    assert preview.dry_run and len(preview.changes) == 1
    with pytest.raises(ResourceNotFoundError):
        act(caller(admin, "reader", project="finance"), ActionName.PIPELINES)
    act(admin, ActionName.ACCESS_GRANT, content=content, reason="Apply")
    assert {
        p.pipeline_code
        for p in act(caller(admin, "reader", project="finance"), ActionName.PIPELINES).pipelines
    } == {"OPEN", "PRIVATE"}
    assert (
        act(admin, ActionName.ACCESS_GRANT, content=content, reason="Again")
        .changes[0]
        .previous_permission
        is not None
    )


def test_project_manage_is_required_for_every_deploy_action(isolation, monkeypatch):
    from etl_craft.services.operations.actions import PROJECT_MANAGE_ACTIONS

    admin, _ = isolation
    for name in PROJECT_MANAGE_ACTIONS:
        action = ACTIONS[name]
        called = []
        monkeypatch.setitem(
            ACTIONS,
            name,
            replace(
                action,
                call=lambda ctx, request, seen=called: seen.append(ctx.actor.name),
                escalate=None,
            ),
        )
        request = build(action.request)
        for identity, role in (("reader", ApiRole.VIEWER), ("operator", ApiRole.OPERATOR)):
            with pytest.raises(PermissionDeniedError, match="CAN_MANAGE"):
                invoke(caller(admin, identity, role), name, request)
        invoke(caller(admin, "manager"), name, request)
        assert called == ["manager"], name


def test_grant_errors_do_not_disclose_restricted_pipeline_names(isolation):
    admin, tokens = isolation
    api = TestClient(create_app(admin))
    for route in ("grants", "revocations"):
        answers = []
        for pipeline in ("PRIVATE", "MISSING"):
            reply = api.post(
                "/api/v1/access/" + route,
                headers={"Authorization": "Bearer " + tokens["operator"]},
                json={
                    "project": "sales",
                    "role": "viewer",
                    "pipeline": pipeline,
                    "permission": "CAN_RUN",
                    "reason": "Probe",
                },
            )
            assert reply.status_code == 404
            answers.append(reply.text.replace(repr(pipeline), "'selected'"))
        assert answers[0] == answers[1]


def test_historical_selectors_hide_foreign_and_restricted_runs(isolation, tmp_path):
    from fixtures.metadata import start_run

    admin, _ = isolation
    runs = {}
    with acting_as(OWNER), admin.engine.begin() as conn:
        for row in conn.execute(
            text(
                "SELECT p.PIPELINE_ID AS pipeline_id,p.PIPELINE_CODE AS pipeline_code,"
                "j.PROJECT_CODE AS project FROM CFG_PIPELINES p JOIN CFG_PROJECTS j "
                "ON j.PROJECT_ID=p.PROJECT_ID WHERE j.PROJECT_CODE IN ('sales','finance')"
            )
        ).all():
            runs[row.project, row.pipeline_code] = start_run(conn, row.pipeline_id)
    for name in (ActionName.CONFIG_EXPORT, ActionName.LINEAGE):
        arguments = {"config_dir": tmp_path / "export"} if name == ActionName.CONFIG_EXPORT else {}
        replies = []
        for run in (runs["finance", "OPEN"], 999999):
            with pytest.raises(ResourceNotFoundError) as error:
                act(caller(admin, "manager"), name, as_of=str(run), **arguments)
            replies.append(
                str(error.value)
                .replace(f"`as_of` {run}:", "`as_of` selected:")
                .replace(f"pipeline_run_id {run};", "pipeline_run_id selected;")
            )
        assert replies[0] == replies[1]
    with pytest.raises(ResourceNotFoundError):
        act(
            caller(admin, "operator", ApiRole.OPERATOR),
            ActionName.LINEAGE,
            as_of=str(runs["sales", "PRIVATE"]),
        )


def test_local_task_dependency_explanations_remain_visible(isolation):
    from fixtures.metadata import add_dependency, add_task, start_run

    admin, _ = isolation
    with acting_as(OWNER), admin.engine.begin() as conn:
        pipeline = conn.execute(
            text(
                "SELECT p.PIPELINE_ID AS pipeline_id FROM CFG_PIPELINES p JOIN CFG_PROJECTS j "
                "ON j.PROJECT_ID=p.PROJECT_ID WHERE j.PROJECT_CODE='sales' AND "
                "p.PIPELINE_CODE='OPEN'"
            )
        ).scalar_one()
        first = add_task(conn, pipeline, "LOAD", "PYTHON")
        next_task = add_task(conn, pipeline, "NEXT", "PYTHON")
        add_dependency(conn, pipeline, next_task, first)
        run = start_run(conn, pipeline)
    explanation = act(
        caller(admin, "reader"),
        ActionName.EXPLAIN,
        pipeline_code="OPEN",
        task_code="NEXT",
        run_id=run,
    )
    assert explanation.dependencies[0].upstream == "LOAD"
    assert explanation.dependencies[0].reason == "Upstream does not satisfy this dependency."


def test_developer_lineage_refresh_keeps_deployment_role_and_visibility(isolation, monkeypatch):
    admin, _ = isolation
    action = ACTIONS[ActionName.LINEAGE_REFRESH]
    called = []
    monkeypatch.setitem(
        ACTIONS,
        action.name,
        replace(action, call=lambda ctx, request: called.append(ctx.actor.name)),
    )
    request = build(action.request)
    invoke(caller(admin, "manager", ApiRole.DEVELOPER), action.name, request)
    assert called == ["manager"]
    with pytest.raises(PermissionDeniedError, match="developer"):
        invoke(caller(admin, "manager"), action.name, request)


def test_generated_catalog_omits_foreign_execution_identities(isolation, tmp_path):
    from etl_craft.engine import transitions
    from fixtures.metadata import add_pipeline_dependency, add_task, start_run

    admin, _ = isolation
    with acting_as(OWNER), admin.engine.begin() as conn:
        pipelines = {
            row.project: row.pipeline_id
            for row in conn.execute(
                text(
                    "SELECT j.PROJECT_CODE AS project,p.PIPELINE_ID AS pipeline_id "
                    "FROM CFG_PIPELINES p "
                    "JOIN CFG_PROJECTS j ON j.PROJECT_ID=p.PROJECT_ID WHERE p.PIPELINE_CODE='OPEN' "
                    "AND j.PROJECT_CODE IN ('sales','finance')"
                )
            )
        }
        upstream = pipelines["finance"]
        downstream = pipelines["sales"]
        secret = add_task(conn, upstream, "SECRET_TASK", "PYTHON")
        add_task(conn, downstream, "LOCAL", "PYTHON")
        upstream_run = start_run(conn, upstream)
        downstream_run = start_run(conn, downstream)
        secret_run = transitions.find_or_create_task_run(conn, secret, upstream_run).task_run_id
        dependency = add_pipeline_dependency(conn, downstream, upstream)
        conn.execute(
            text(
                "INSERT INTO AUD_DEPENDENCY_CONSUMPTION (PIPELINE_DEPENDENCY_ID,PIPELINE_ID,"
                "PIPELINE_RUN_ID,DEPENDS_ON_PIPELINE_ID,"
                "CONSUMED_PIPELINE_RUN_ID,CONSUMED_TASK_RUN_ID) "
                "VALUES (:d,:p,:r,:up,:ur,:ut)"
            ),
            {
                "d": dependency,
                "p": downstream,
                "r": downstream_run,
                "up": upstream,
                "ur": upstream_run,
                "ut": secret_run,
            },
        )
        conn.execute(
            text(
                "UPDATE AUD_PIPELINES_RUN_LOG SET TRIGGERED_BY_TASK_RUN_ID=:task "
                "WHERE PIPELINE_RUN_ID=:run"
            ),
            {"task": secret_run, "run": downstream_run},
        )
    from etl_craft.engine.repository.triggers import fetch_trigger_links

    with admin.engine.connect() as conn:
        assert fetch_trigger_links(conn, [downstream_run])[0].parent_run_id == upstream_run
    folder = tmp_path / "catalog"
    result = act(caller(admin, "manager"), ActionName.GENERATE_DOCS, output=folder)
    assert result.folder == folder
    html = "\n".join(page.read_text() for page in folder.rglob("*.html"))
    assert "SECRET_TASK" not in html
    page = (folder / "pipelines/OPEN.html").read_text()
    assert "finance.OPEN" in page
    assert 'href="finance.OPEN.html"' not in page


def test_aggregate_mutation_refusals_do_not_name_hidden_pipelines(isolation):
    admin, _ = isolation
    for name in (ActionName.LINEAGE_REFRESH, ActionName.RECONCILE):
        with pytest.raises(PermissionDeniedError) as caught:
            invoke(caller(admin, "operator", ApiRole.DEVELOPER), name, build(ACTIONS[name].request))
        message = str(caught.value)
        assert "sales" in message and "project manager" in message
        assert "PRIVATE" not in message and "OPEN" not in message
