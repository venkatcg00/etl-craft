"""Admission snapshots and repaired upstream revisions through the real command line."""

import pytest
from sqlalchemy import text

from etl_craft.core.actor import current_actor
from etl_craft.engine import transitions as tr
from etl_craft.engine.repository import trackers
from etl_craft.execution import runner
from fixtures.metadata import (
    add_dependency,
    add_pipeline,
    add_pipeline_dependency,
    add_task,
    upstream_run,
)


def upstream(project, *, task_edge=False, pipeline_edge=True, repairs="Y"):
    with project.engine.begin() as conn:
        pipeline = add_pipeline(conn, "UP")
        task = add_task(conn, pipeline, "publish")
        run, rows = upstream_run(conn, pipeline, {task: ("SUCCESS", 5)})
        if pipeline_edge:
            edge = add_pipeline_dependency(conn, project.pipeline_id, pipeline)
            conn.execute(
                text(
                    "UPDATE CFG_PIPELINE_DEPENDENCY SET CONSUME_REPAIRS=:repairs "
                    "WHERE PIPELINE_DEPENDENCY_ID=:edge"
                ),
                {"repairs": repairs, "edge": edge},
            )
        if task_edge:
            edge = add_dependency(
                conn, project.pipeline_id, project.task_id, task, upstream_pipeline=pipeline
            )
            conn.execute(
                text(
                    "UPDATE CFG_TASK_DEPENDENCY SET CONSUME_REPAIRS=:repairs "
                    "WHERE TASK_DEPENDENCY_ID=:edge"
                ),
                {"repairs": repairs, "edge": edge},
            )
    return run, rows[task]


def repair(project, run, *, status="SUCCESS"):
    with project.engine.begin() as conn:
        tr.reopen_run(conn, run, current_actor(), reason="correct upstream output")
        tr.finish_run(conn, run, status, current_actor())


def test_pipeline_consumes_the_admitted_revision_after_upstream_reopens(cli_project):
    project = cli_project
    run, _ = upstream(project)
    code, output = project.run("run", "--pipeline_code", "P", "--init-only")
    assert code == 0, output
    repair(project, run)
    code, output = project.run("run", "--pipeline_code", "P")
    assert code == 0, output
    with project.engine.connect() as conn:
        assert tuple(
            conn.execute(
                text("SELECT SELECTED_PIPELINE_RUN_ID, SELECTED_REVISION FROM AUD_GATE_DECISIONS")
            ).one()
        ) == (run, 1)
        assert tuple(
            conn.execute(
                text(
                    "SELECT CONSUMED_PIPELINE_RUN_ID, CONSUMED_REVISION "
                    "FROM AUD_DEPENDENCY_CONSUMPTION"
                )
            ).one()
        ) == (run, 1)
        assert (
            conn.execute(
                text(
                    "SELECT OUTPUT_REVISION FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_RUN_ID=:run"
                ),
                {"run": run},
            ).scalar_one()
            == 2
        )


@pytest.mark.chaos
def test_task_consumes_its_claim_snapshot_after_upstream_repairs(cli_project):
    project = cli_project
    run, task_run = upstream(project, task_edge=True, pipeline_edge=False)
    (project.config.ingestion_scripts_dir / "load.py").write_text(
        "from pathlib import Path\nimport time\nfrom etl_craft.scripting import ScriptResult\n"
        "def run(task):\n    while not Path('release').exists():\n        time.sleep(0.05)\n"
        "    return ScriptResult(1)\n"
    )
    parent = project.start("run", "--pipeline_code", "P")
    project.wait_for("SELECT STATUS FROM AUD_TASK_ATTEMPTS", expected="RUNNING")
    repair(project, run)
    (project.config.project_dir / "release").touch()
    assert parent.wait() == 0, parent.output
    with project.engine.connect() as conn:
        assert tuple(
            conn.execute(
                text("SELECT SELECTED_TASK_RUN_ID, SELECTED_REVISION FROM AUD_GATE_DECISIONS")
            ).one()
        ) == (task_run, 1)
        assert tuple(
            conn.execute(
                text(
                    "SELECT CONSUMED_TASK_RUN_ID, CONSUMED_REVISION FROM AUD_DEPENDENCY_CONSUMPTION"
                )
            ).one()
        ) == (task_run, 1)
        attempt = conn.execute(text("SELECT ATTEMPT_ID FROM AUD_GATE_DECISIONS")).scalar_one()
        assert attempt is not None


@pytest.mark.parametrize("kind", ["pipeline", "task"])
@pytest.mark.parametrize("repairs", ["Y", "N"])
def test_repaired_upstream_is_new_only_when_the_dependency_consumes_repairs(
    cli_project, kind, repairs
):
    project = cli_project
    run, _ = upstream(
        project, task_edge=kind == "task", pipeline_edge=kind == "pipeline", repairs=repairs
    )
    code, output = project.run("run", "--pipeline_code", "P")
    assert code == 0, output
    repair(project, run)
    code, output = project.run("run", "--pipeline_code", "P")
    assert code == 0, output
    with project.engine.connect() as conn:
        statuses = (
            conn.execute(
                text(
                    "SELECT STATUS FROM AUD_PIPELINES_RUN_LOG "
                    "WHERE PIPELINE_ID=:pipeline ORDER BY PIPELINE_RUN_ID"
                ),
                {"pipeline": project.pipeline_id},
            )
            .scalars()
            .all()
        )
        assert statuses == ["SUCCESS", "SUCCESS" if repairs == "Y" else "SKIPPED"]
        revisions = (
            conn.execute(
                text(
                    "SELECT CONSUMED_REVISION FROM AUD_DEPENDENCY_CONSUMPTION "
                    "ORDER BY CONSUMPTION_ID"
                )
            )
            .scalars()
            .all()
        )
        assert revisions == ([1, 2] if repairs == "Y" else [1])
        decisions = conn.execute(
            text("SELECT SELECTED_REVISION, RESULT FROM AUD_GATE_DECISIONS ORDER BY DECISION_ID")
        ).all()
        assert [tuple(row) for row in decisions] == [
            (1, "SATISFIED"),
            (2, "SATISFIED" if repairs == "Y" else "UNSATISFIED"),
        ]


def test_failed_repair_keeps_revision_until_successful_publication(cli_project):
    project = cli_project
    run, _ = upstream(project)
    repair(project, run, status="FAILED")
    with project.engine.connect() as conn:
        assert tuple(
            conn.execute(
                text(
                    "SELECT OUTPUT_REVISION, REPAIR_PENDING "
                    "FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_RUN_ID=:run"
                ),
                {"run": run},
            ).one()
        ) == (1, "Y")
    repair(project, run)
    with project.engine.connect() as conn:
        assert tuple(
            conn.execute(
                text(
                    "SELECT OUTPUT_REVISION, REPAIR_PENDING "
                    "FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_RUN_ID=:run"
                ),
                {"run": run},
            ).one()
        ) == (2, "N")
    # A second finalizer cannot publish the same output twice.
    with project.engine.begin() as conn:
        assert not tr.finalize_pipeline_run(conn, run, "SUCCESS").ended


def test_pipeline_decisions_roll_back_with_admission(cli_project):
    project = cli_project
    upstream(project)
    code, output = project.run("run", "--pipeline_code", "P", fault="pipeline.after_insert")
    assert code == 19, output
    with project.engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM AUD_GATE_DECISIONS")).scalar_one() == 0
        assert (
            conn.execute(
                text("SELECT STATUS FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_ID=:pipeline"),
                {"pipeline": project.pipeline_id},
            ).scalar_one()
            == "QUEUED"
        )


def test_attempt_decisions_roll_back_with_claim(cli_project, monkeypatch):
    project = cli_project
    upstream(project, task_edge=True, pipeline_edge=False)
    code, output = project.run("run", "--pipeline_code", "P", "--init-only")
    assert code == 0, output
    original = trackers.record_decisions

    def broken(conn, run_id, decisions, *, attempt_id=None):
        original(conn, run_id, decisions, attempt_id=attempt_id)
        raise RuntimeError("cannot record admission")

    monkeypatch.setattr(trackers, "record_decisions", broken)
    with pytest.raises(RuntimeError, match="cannot record admission"):
        runner.run_task(project.engine, project.config, "P", "load")
    with project.engine.connect() as conn:
        for table in ["AUD_GATE_DECISIONS", "AUD_TASK_ATTEMPTS"]:
            assert conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar_one() == 0
        assert (
            conn.execute(
                text("SELECT COUNT(*) FROM AUD_TASK_RUN_LOG WHERE TASK_ID=:task"),
                {"task": project.task_id},
            ).scalar_one()
            == 0
        )


@pytest.mark.parametrize("policy", ["warn", "off"])
def test_bypassed_pipeline_decisions_never_consume_an_upstream(cli_project, policy):
    import yaml

    project = cli_project
    run, _ = upstream(project)
    with project.engine.begin() as conn:
        tr.mark_run(conn, run, "FAILED", current_actor())
    path = project.config.config_path
    raw = yaml.safe_load(path.read_text())
    raw["Orchestration"]["Dependency_gates"] = policy
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    code, output = project.run("run", "--pipeline_code", "P")
    assert code == 0, output
    with project.engine.connect() as conn:
        assert (
            conn.execute(text("SELECT RESULT FROM AUD_GATE_DECISIONS")).scalar_one() == "BYPASSED"
        )
        assert (
            conn.execute(text("SELECT COUNT(*) FROM AUD_DEPENDENCY_CONSUMPTION")).scalar_one() == 0
        )


def test_consumption_from_decisions_is_idempotent(cli_project):
    project = cli_project
    upstream(project, task_edge=True)
    code, output = project.run("run", "--pipeline_code", "P")
    assert code == 0, output
    with project.engine.begin() as conn:
        run = conn.execute(
            text("SELECT PIPELINE_RUN_ID FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_ID=:pipeline"),
            {"pipeline": project.pipeline_id},
        ).scalar_one()
        for _ in range(2):
            trackers.consume_pipeline_decisions(conn, run)
            trackers.consume_task_decisions(conn, project.task_id, run)
        assert (
            conn.execute(text("SELECT COUNT(*) FROM AUD_DEPENDENCY_CONSUMPTION")).scalar_one() == 2
        )


def test_migration_preserves_a_pending_historical_repair(empty_engine_db):
    from etl_craft.engine.migrations import apply_pending_migrations
    from fixtures.released_schema import install

    db = empty_engine_db
    install(db, "0.2.0")
    with db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "UP")
        run = conn.execute(
            text(
                "INSERT INTO AUD_PIPELINES_RUN_LOG (PIPELINE_ID, STATUS) "
                "VALUES (:pipeline, 'IN-PROGRESS') RETURNING PIPELINE_RUN_ID"
            ),
            {"pipeline": pipeline},
        ).scalar_one()
        conn.execute(
            text(
                "INSERT INTO AUD_RUN_INTERVENTIONS (PIPELINE_ID, PIPELINE_RUN_ID, "
                "ACTION, FROM_STATUS, TO_STATUS, REASON, REQUESTED_BY) "
                "VALUES (:pipeline, :run, 'REOPEN', 'SUCCESS', 'IN-PROGRESS', "
                "'historical repair', 'operator')"
            ),
            {"pipeline": pipeline, "run": run},
        )
    apply_pending_migrations(db.engine)
    with db.engine.begin() as conn:
        assert tuple(
            conn.execute(
                text("SELECT OUTPUT_REVISION, REPAIR_PENDING FROM AUD_PIPELINES_RUN_LOG")
            ).one()
        ) == (1, "Y")
        tr.finish_run(conn, run, "SUCCESS", current_actor())
        assert tuple(
            conn.execute(
                text("SELECT OUTPUT_REVISION, REPAIR_PENDING FROM AUD_PIPELINES_RUN_LOG")
            ).one()
        ) == (2, "N")


@pytest.mark.parametrize("point", ["pipeline.before_consumption", "pipeline.after_consumption"])
@pytest.mark.parametrize("crash", [False, True])
@pytest.mark.chaos
def test_run_ending_and_consumption_roll_back_together(cli_project, point, crash):
    project = cli_project
    upstream(project)
    with project.engine.begin() as conn:
        conn.execute(
            text("UPDATE CFG_PIPELINES SET SLA_IN_HOURS=1 WHERE PIPELINE_ID=:id"),
            {"id": project.pipeline_id},
        )
    code, output = project.run(
        "run", "--pipeline_code", "P", fault=point + (":kill" if crash else "")
    )
    assert code == (137 if crash else 19), output
    with project.engine.begin() as conn:
        row = conn.execute(
            text(
                "SELECT STATUS, END_DATE, SLA_STATUS FROM AUD_PIPELINES_RUN_LOG "
                "WHERE PIPELINE_ID=:id"
            ),
            {"id": project.pipeline_id},
        ).one()
        assert tuple(row) == ("IN-PROGRESS", None, None)
        assert (
            conn.execute(text("SELECT COUNT(*) FROM AUD_DEPENDENCY_CONSUMPTION")).scalar_one() == 0
        )
        # The dead overseer's lease is expired before the next command reconciles it.
        conn.execute(
            text("UPDATE AUD_PIPELINES_RUN_LOG SET LEASE_EXPIRES_AT=:past WHERE PIPELINE_ID=:id"),
            {"id": project.pipeline_id, "past": "2000-01-01 00:00:00"},
        )
    code, output = project.run("run", "--pipeline_code", "P")
    assert code == 0, output
    with project.engine.connect() as conn:
        assert tuple(
            conn.execute(
                text("SELECT STATUS, SLA_STATUS FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_ID=:id"),
                {"id": project.pipeline_id},
            ).one()
        ) == ("SUCCESS", "MET")
        assert (
            conn.execute(text("SELECT COUNT(*) FROM AUD_DEPENDENCY_CONSUMPTION")).scalar_one() == 1
        )


@pytest.mark.parametrize(
    "point",
    [
        "attempt.before_summary",
        "attempt.after_status",
        "script.after_offset",
        "runner.before_consumption",
        "attempt.after_consumption",
    ],
)
@pytest.mark.parametrize("crash", [False, True])
@pytest.mark.chaos
def test_attempt_success_offset_and_consumption_roll_back_together(cli_project, point, crash):
    from etl_craft.engine.repository.offsets import (
        StoredOffset,
        fetch_task_offset,
        save_task_offset,
    )

    project = cli_project
    upstream(project, task_edge=True, pipeline_edge=False)
    code, output = project.run("run", "--pipeline_code", "P", "--init-only")
    assert code == 0, output
    (project.config.ingestion_scripts_dir / "load.py").write_text(
        "from etl_craft.scripting import ScriptResult, Offset\n"
        "def run(task):\n    assert task.offset == Offset.number(3)\n"
        "    return ScriptResult(1, Offset.number(9))\n"
    )
    with project.engine.begin() as conn:
        save_task_offset(conn, project.task_id, StoredOffset("NUMBER", "3"))
    code, output = project.run(
        "run",
        "--pipeline_code",
        "P",
        "--task_code",
        "load",
        fault=point + (":kill" if crash else ""),
    )
    assert code == 1, output
    with project.engine.connect() as conn:
        assert fetch_task_offset(conn, project.task_id) == StoredOffset("NUMBER", "3")
        assert conn.execute(text("SELECT STATUS FROM AUD_TASK_ATTEMPTS")).scalar_one() == "FAILED"
        assert (
            conn.execute(text("SELECT COUNT(*) FROM AUD_DEPENDENCY_CONSUMPTION")).scalar_one() == 0
        )
    code, output = project.run("run", "--pipeline_code", "P", "--task_code", "load")
    assert code == 0, output
    with project.engine.connect() as conn:
        assert fetch_task_offset(conn, project.task_id) == StoredOffset("NUMBER", "9")
        assert (
            conn.execute(text("SELECT STATUS FROM AUD_TASK_ATTEMPTS ORDER BY ATTEMPT_ID DESC"))
            .scalars()
            .first()
            == "SUCCESS"
        )
        assert (
            conn.execute(text("SELECT COUNT(*) FROM AUD_DEPENDENCY_CONSUMPTION")).scalar_one() == 1
        )


@pytest.mark.chaos
def test_a_child_crashing_after_commit_keeps_the_complete_success(cli_project):
    from etl_craft.engine.repository.offsets import StoredOffset, fetch_task_offset

    project = cli_project
    upstream(project, task_edge=True, pipeline_edge=False)
    code, output = project.run("run", "--pipeline_code", "P", "--init-only")
    assert code == 0, output
    (project.config.ingestion_scripts_dir / "load.py").write_text(
        "from etl_craft.scripting import ScriptResult, Offset\n"
        "def run(task):\n    return ScriptResult(1, Offset.number(9))\n"
    )
    code, output = project.run(
        "run", "--pipeline_code", "P", "--task_code", "load", fault="child.after_outcome:kill"
    )
    assert code == 0, output
    with project.engine.connect() as conn:
        assert fetch_task_offset(conn, project.task_id) == StoredOffset("NUMBER", "9")
        assert conn.execute(text("SELECT STATUS FROM AUD_TASK_ATTEMPTS")).scalar_one() == "SUCCESS"
        assert (
            conn.execute(text("SELECT COUNT(*) FROM AUD_DEPENDENCY_CONSUMPTION")).scalar_one() == 1
        )


def test_finalized_hook_observes_committed_consumption(cli_project):
    from etl_craft.execution.pipeline import RunHooks, run_pipeline

    project = cli_project
    upstream(project)
    observed = []

    def finalized(outcome):
        with project.engine.connect() as conn:
            observed.append(
                tuple(
                    conn.execute(
                        text(
                            "SELECT STATUS, (SELECT COUNT(*) FROM AUD_DEPENDENCY_CONSUMPTION) "
                            "FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_RUN_ID=:run"
                        ),
                        {"run": outcome.pipeline_run_id},
                    ).one()
                )
            )
        raise RuntimeError("hook failed after commit")

    outcome = run_pipeline(
        project.engine, project.config, "P", hooks=RunHooks(on_finalized=finalized)
    )
    assert outcome.status == "SUCCESS"
    assert observed == [("SUCCESS", 1)]
