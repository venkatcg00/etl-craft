"""Independent selections retain run identity, admission safety and upstream isolation."""

from datetime import date

import pytest
from sqlalchemy import text

from etl_craft.core.actor import current_actor
from etl_craft.core.enums import RunStatus
from etl_craft.core.errors import RunRefusedError, RunStateError, StaleTransitionError
from etl_craft.engine import run_scopes, runlog, transitions
from etl_craft.engine.repository import trackers
from etl_craft.engine.repository.dependencies import fetch_pipeline_graph
from fixtures.metadata import add_dependency, add_pipeline, add_task
from fixtures.races import two_at_once


def begin_admission(engine):
    return engine.begin()


def create(conn, pipeline_id, task_id, key):
    return run_scopes.create_manual_run(
        conn,
        pipeline_id,
        current_actor(),
        run_scopes.TaskSelection((task_id,)),
        key,
        date(2026, 10, 18),
    )


def test_manual_identity_never_replaces_implicit_pipeline_selection(engine_db):
    with engine_db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        task = add_task(conn, pipeline, "mail", "EMAIL_ALERT")
        normal = transitions.create_run(conn, pipeline, current_actor())
        first = create(conn, pipeline, task, "manual-first")
        second = create(conn, pipeline, task, "manual-second")
        assert first != second != normal
        assert runlog.select_run(conn, pipeline).pipeline_run_id == normal
        assert runlog.fetch_active_pipeline_run_id(conn, pipeline) == normal
        assert runlog.select_run(conn, pipeline, runlog.RunSelector(run_id=first)).task_selection
        transitions.finish_run(conn, normal, RunStatus.SUCCESS, current_actor())
        with pytest.raises(RunStateError, match="matched 0 runs"):
            runlog.select_run(conn, pipeline)
        assert runlog.fetch_active_pipeline_run_id(conn, pipeline) is None


def test_manual_run_cannot_bind_an_unselected_task(engine_db):
    with engine_db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        selected = add_task(conn, pipeline, "selected", "EMAIL_ALERT")
        other = add_task(conn, pipeline, "other", "EMAIL_ALERT")
        run_id = create(conn, pipeline, selected, "manual")
        with pytest.raises(RunStateError, match="outside selected tasks"):
            transitions.find_or_create_task_run(conn, other, run_id)
        binding = transitions.find_or_create_task_run(conn, selected, run_id)
        assert binding.created


@pytest.mark.parametrize(
    "handler,params",
    [
        ("PYTHON", {}),
        ("EVENT_WAIT", {}),
        ("BUSINESS_RULES", {}),
        ("SQL", {"WATERMARK_COLUMN": "id"}),
    ],
)
def test_shared_checkpoint_and_mutable_handler_state_are_refused(engine_db, handler, params):
    with engine_db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        task = add_task(conn, pipeline, "T", handler, **params)
        with pytest.raises(RunRefusedError, match="explicit pipeline run"):
            create(conn, pipeline, task, "manual")
        assert runlog.run_candidates(conn, pipeline) == []


@pytest.mark.parametrize("manual_first", [False, True])
def test_sql_selection_and_pipeline_cannot_overlap(engine_db, manual_first):
    with engine_db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        task = add_task(conn, pipeline, "T")
        if manual_first:
            create(conn, pipeline, task, "manual")
            with pytest.raises(StaleTransitionError):
                transitions.create_run(conn, pipeline, current_actor())
        else:
            transitions.create_run(conn, pipeline, current_actor())
            # The caller's transaction rolls back the queued manual row on refusal.
            with pytest.raises(StaleTransitionError), conn.begin_nested():
                create(conn, pipeline, task, "manual")
        active = [
            r for r in runlog.run_candidates(conn, pipeline) if r.status == RunStatus.IN_PROGRESS
        ]
        assert len(active) == 1


@pytest.mark.chaos
def test_manual_and_pipeline_concurrent_admission_has_one_exclusive_winner(engine_db):
    db = engine_db
    with db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        task = add_task(conn, pipeline, "T")

    def start(manual_run):
        try:
            with begin_admission(db.engine) as conn:
                return (
                    create(conn, pipeline, task, "manual")
                    if manual_run
                    else (transitions.create_run(conn, pipeline, current_actor()))
                )
        except StaleTransitionError:
            return None

    result = two_at_once(
        lambda: start(True), lambda: start(False), at=f"{__name__}.begin_admission"
    )
    assert sum(run_id is not None for run_id in result) == 1
    with db.engine.connect() as conn:
        assert len(runlog.run_candidates(conn, pipeline)) == 1


def test_outside_dependencies_require_explicit_current_inputs(engine_db):
    with engine_db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        before = add_task(conn, pipeline, "before")
        selected = add_task(conn, pipeline, "selected")
        after = add_task(conn, pipeline, "after")
        add_dependency(conn, pipeline, selected, before)
        add_dependency(conn, pipeline, after, selected)
        data = fetch_pipeline_graph(conn, pipeline)
        with pytest.raises(RunRefusedError, match="`ignore_dependencies` with `reason`"):
            run_scopes.selected_graph(data, run_scopes.TaskSelection((selected, after)))
        scope = run_scopes.selected_graph(data, run_scopes.TaskSelection((selected, after), True))
        assert {task.task_id for task in scope.tasks} == {selected, after}
        assert [(edge.task_id, edge.depends_on_task_id) for edge in scope.same_pipeline_edges] == [
            (after, selected)
        ]


def test_manual_result_does_not_publish_upstream_success(engine_db):
    with engine_db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        task = add_task(conn, pipeline, "T")
        run_id = create(conn, pipeline, task, "manual")
        binding = transitions.find_or_create_task_run(conn, task, run_id)
        transitions.finish_task_run(conn, binding.task_run_id, status=RunStatus.SUCCESS)
        transitions.finish_run(conn, run_id, RunStatus.SUCCESS, current_actor())
        assert trackers.fetch_latest_pipeline_run(conn, pipeline) is None
        assert trackers.fetch_latest_task_run(conn, task) is None


def test_metadata_change_cannot_turn_overlapping_manual_run_into_a_writer(engine_db):
    with engine_db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        task = add_task(conn, pipeline, "T", "EMAIL_ALERT")
        transitions.create_run(conn, pipeline, current_actor())
        run_id = create(conn, pipeline, task, "manual")
        conn.execute(text("UPDATE CFG_TASKS SET HANDLER='SQL' WHERE TASK_ID=:task"), {"task": task})
        binding = transitions.find_or_create_task_run(conn, task, run_id)
    from etl_craft.execution.context import build_task_context

    context = build_task_context(engine_db.engine, engine_db.config, binding.task_run_id)
    assert context.handler == "EMAIL_ALERT"
    assert context.task_params.get("SQL_ACTION") is None


def test_child_context_keeps_admitted_handler_after_binding(engine_db):
    from etl_craft.execution.context import build_task_context

    db = engine_db
    with db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        task = add_task(conn, pipeline, "T", "EMAIL_ALERT")
        run_id = create(conn, pipeline, task, "manual")
        binding = transitions.find_or_create_task_run(conn, task, run_id)
        conn.execute(
            text("UPDATE CFG_TASKS SET HANDLER='PYTHON' WHERE TASK_ID=:task"), {"task": task}
        )
    context = build_task_context(db.engine, db.config, binding.task_run_id)
    assert context.handler == "EMAIL_ALERT"


def test_overlapping_email_does_not_block_scheduled_dispatch(engine_db):
    from etl_craft.engine.queries import statement

    with engine_db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        task = add_task(conn, pipeline, "T", "EMAIL_ALERT")
        manual_run = create(conn, pipeline, task, "manual")
        queued = transitions.create_run(conn, pipeline, current_actor(), status=RunStatus.QUEUED)
        active = [
            row.pipeline_run_id for row in conn.execute(statement(conn, "controller_active_runs"))
        ]
        assert queued in active and manual_run not in active
