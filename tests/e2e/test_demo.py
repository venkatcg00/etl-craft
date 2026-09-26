"""The Support Insights demo, end to end, from the built wheel installed with pip and with uv.

With pip, every Engine DB with every local warehouse; with uv, SQLite and DuckDB, since the
installer changes how the package got there, not what it does. Each case runs in local mode and
under a simulated orchestrator that runs the DAGs ``generate-yml`` writes. The runs cover
ingestion with offsets, the eight SQL actions, business rules in two waves, alerts and a failure
watcher, the SLA email, a task that fails once, a timeout, a stopped run resumed, cross-pipeline
gates and cloning; then the warehouse's data, the audit rows, the emails, and what ``validate``,
``doctor``, ``history``, ``graph``, ``lineage`` and ``generate-docs`` report.
"""

from __future__ import annotations

import signal
import time
from pathlib import Path

import pytest
import yaml
from sqlalchemy import text

from etl_craft.engine.connection import engine_db
from fixtures.demo import Demo, demos, installed_cli
from fixtures.orchestrator import SKIPPED, SUCCESS, run_dag

INSTALLERS = {"pip": pytest.mark.e2e_pip, "uv": pytest.mark.e2e_uv}
CASES = list(demos(INSTALLERS, smoke=frozenset({"uv"})))
"""Everything with pip; with uv, one Engine DB and warehouse, to prove that install works."""

pytestmark = pytest.mark.timeout(1800)


@pytest.fixture(scope="session")
def clis(tmp_path_factory):
    """The installed ``etl-craft`` per installer, installed once per session when first asked."""
    installed: dict[str, Path] = {}

    def cli(installer: str) -> Path:
        if installer not in installed:
            installed[installer] = installed_cli(installer, tmp_path_factory.mktemp("venvs"))
        return installed[installer]

    return cli


@pytest.fixture
def make_demo(clis, tmp_path):
    made: list[Demo] = []

    def make(installer: str, engine: str, warehouse: str, mode: str = "local") -> Demo:
        demo = Demo(clis(installer), tmp_path / "support-insights", engine, warehouse, mode)
        made.append(demo)
        return demo.build()

    yield make
    for demo in made:
        demo.close()


def set_parameter(demo: Demo, pipeline: str, task: str, name: str, value: str) -> None:
    engine = engine_db(demo.config)
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE CFG_TASK_PARAMETERS SET PARAMETER_VALUE = :v WHERE PARAMETER_NAME = :n "
                    "AND TASK_ID = (SELECT t.TASK_ID FROM CFG_TASKS t JOIN CFG_PIPELINES p "
                    "ON p.PIPELINE_ID = t.PIPELINE_ID WHERE p.PIPELINE_CODE = :p "
                    "AND t.TASK_CODE = :t)"
                ),
                {"v": value, "n": name, "p": pipeline, "t": task},
            )
    finally:
        engine.dispose()


def run_clients(demo: Demo) -> None:
    for pipeline in ("CLIENT_ALPHA", "CLIENT_BETA"):
        assert "SUCCESS" in demo.ok("run", "--pipeline_code", pipeline)


@pytest.mark.parametrize(("installer", "engine", "warehouse"), CASES)
def test_the_demo_runs_locally(make_demo, installer, engine, warehouse):
    demo = make_demo(installer, engine, warehouse)
    assert "setup: created the Engine DB tables" in demo.ok("setup")
    demo.seed()
    assert demo.ok("validate").endswith("0 failed, 0 warning(s)\n")
    assert demo.ok("doctor").strip().endswith("0 failed")

    # The mart's first run: the flaky feed fails, what needs it is skipped, and the alerts run.
    run_clients(demo)
    first = demo.run("run", "--pipeline_code", "SUPPORT_DM")
    assert first.returncode == 1, first.stderr[-3000:]
    assert "flaky_feed (FAILED)" in first.stdout
    skipped = first.stdout.split("skipped because of the failure: ", 1)[1].split("\n")[0]
    assert set(skipped.split(", ")) >= {"interactions", "setup_fact", "fact", "quality"}
    # A failed run consumes nothing, so the next one builds on the same client runs; after that
    # the mart waits for both clients to succeed again.
    assert "SUCCESS" in demo.ok("run", "--pipeline_code", "SUPPORT_DM")
    assert "SKIPPED" in demo.ok("run", "--pipeline_code", "SUPPORT_DM")
    run_clients(demo)
    assert "SUCCESS" in demo.ok("run", "--pipeline_code", "SUPPORT_DM")

    # The export overruns its time limit and its SLA.
    export = demo.run("run", "--pipeline_code", "SUPPORT_EXPORT")
    assert export.returncode == 1
    assert "export (FAILED)" in export.stdout and "BREACHED" in export.stdout
    assert "timed out after 2s" in export.stderr

    # A backfill stopped part way is resumed, without repeating what finished.
    running = demo.popen("run", "--pipeline_code", "SUPPORT_BACKFILL")
    deadline = time.monotonic() + 120
    while True:
        rows = demo.engine_rows(
            "SELECT r.STATUS AS status FROM AUD_TASK_RUN_LOG r JOIN CFG_TASKS t "
            "ON t.TASK_ID = r.TASK_ID WHERE t.TASK_CODE = 'backfill'"
        )
        if rows and rows[0][0] == "IN-PROGRESS":
            break
        assert time.monotonic() < deadline, "the backfill never started"
        time.sleep(0.5)
    running.send_signal(signal.SIGTERM)
    running.wait(timeout=120)
    run_id, status = demo.latest_run("SUPPORT_BACKFILL")
    assert status == "IN-PROGRESS"
    assert demo.task_runs("SUPPORT_BACKFILL", run_id) == {
        "prepare": ("SUCCESS", 1),
        "backfill": ("FAILED", 1),
    }
    set_parameter(demo, "SUPPORT_BACKFILL", "backfill", "INPUT_PARAMS", '{"seconds": 0}')
    assert f"pipeline_run_id={run_id} SUCCESS" in demo.ok(
        "run", "--pipeline_code", "SUPPORT_BACKFILL"
    )
    assert demo.task_runs("SUPPORT_BACKFILL", run_id) == {
        "prepare": ("SUCCESS", 1),
        "backfill": ("SUCCESS", 2),
    }

    check_the_warehouse(demo)
    check_the_emails(demo, failed=True)
    check_the_reports(demo)


def check_the_warehouse(demo: Demo) -> None:
    """The data every run wrote, and what the business rules flagged."""
    assert demo.rows("SELECT COUNT(*) FROM {c}.lnd.client_alpha") == [(24,)]
    assert demo.rows("SELECT COUNT(*) FROM {c}.prs.alpha_interactions") == [(22,)]
    assert demo.rows(
        "SELECT COUNT(*) FROM {c}.prs.alpha_interactions WHERE agent_code = 'TEST'"
    ) == [(0,)]
    assert demo.rows(
        "SELECT source_system, COUNT(*) FROM {c}.dm.support_fact GROUP BY source_system "
        "ORDER BY source_system"
    ) == [("ALPHA", 22), ("BETA", 16)]
    assert demo.rows(
        "SELECT agent_code, team, email, DELETE_FLAG FROM {c}.ds.agents ORDER BY agent_code"
    ) == [
        ("A01", "east", "ann@example.com", "N"),
        ("A02", "west", "bo@example.com", "N"),
        ("A03", "west", "cy@example.com", "N"),
        ("A04", "west", "di@example.com", "Y"),
    ]
    assert demo.rows(
        "SELECT agent_code, team, ACTIVE_FLAG FROM {c}.cdc.agent_history "
        "ORDER BY agent_code, ACTIVE_FLAG"
    ) == [
        ("A01", "east", "Y"),
        ("A02", "east", "N"),
        ("A02", "west", "Y"),
        ("A03", "west", "Y"),
        ("A04", "west", "Y"),
    ]
    assert demo.rows("SELECT COUNT(*) FROM {c}.aud.CFG_PIPELINES") == [(5,)]
    flagged = demo.engine_rows(
        "SELECT r.BUSINESS_RULE_NAME AS rule, r.BUSINESS_RULE_TYPE AS kind, COUNT(*) AS n "
        "FROM AUD_BUSINESS_RULES_RESULTS x JOIN CFG_BUSINESS_RULES r "
        "ON r.BUSINESS_RULE_ID = x.BUSINESS_RULE_ID WHERE x.ACTIVE_FLAG = 'Y' "
        "GROUP BY r.BUSINESS_RULE_NAME, r.BUSINESS_RULE_TYPE ORDER BY r.BUSINESS_RULE_NAME"
    )
    assert [(rule, kind, int(n)) for rule, kind, n in flagged] == [
        ("long call", "REPORT", 8),
        ("rating out of range", "REJECT", 6),
        ("unknown agent", "INCOMPLETE", 2),
    ]
    offsets = demo.engine_rows(
        "SELECT p.PIPELINE_CODE AS p, o.OFFSET_TYPE AS kind, o.OFFSET_VALUE AS v "
        "FROM AUD_TASK_OFFSET_TRACKER o JOIN CFG_TASKS t ON t.TASK_ID = o.TASK_ID "
        "JOIN CFG_PIPELINES p ON p.PIPELINE_ID = t.PIPELINE_ID ORDER BY p.PIPELINE_CODE"
    )
    assert offsets == [
        ("CLIENT_ALPHA", "NUMBER", "24"),
        ("CLIENT_BETA", "TIMESTAMP", "2026-09-01T23:00:00"),
        ("SUPPORT_DM", "NUMBER", "3"),
    ]


def check_the_emails(demo: Demo, *, failed: bool, outcome: str = "SUCCESS") -> None:
    subjects = {subject for subject, _ in demo.emails()}
    assert f"SUPPORT_DM: {outcome}" in subjects
    if failed:
        assert {
            "SUPPORT_DM: FAILED",
            "SUPPORT_DM: a feed failed",
            "[etl-craft] SUPPORT_EXPORT: SLA of 0.0002 h missed",
        } <= subjects


def check_the_reports(demo: Demo) -> None:
    history = demo.ok("history", "--pipeline_code", "SUPPORT_DM").splitlines()
    assert [line.split("\t")[1] for line in history[1:]] == [
        "SUCCESS",
        "SKIPPED",
        "SUCCESS",
        "FAILED",
    ]
    assert "wave" in demo.ok("graph", "--pipeline_code", "SUPPORT_DM").lower()
    lineage = demo.ok("lineage", "--table", "dm.support_fact", "--column", "rating")
    assert "lnd.client_alpha.rating" in lineage and "lnd.client_beta.score" in lineage
    assert "generate-docs: wrote" in demo.ok("generate-docs", "--strict", "--with-warehouse")
    fact = (demo.root / "catalog" / "tables" / "dm.support_fact.html").read_text("utf-8")
    assert "unknown agent" in fact and 'data-col="dm.support_fact|rating"' in fact


@pytest.mark.parametrize(("installer", "engine", "warehouse"), CASES)
def test_the_demo_runs_under_an_orchestrator(make_demo, installer, engine, warehouse):
    demo = make_demo(installer, engine, warehouse, mode="remote")
    demo.ok("setup")
    demo.seed()
    refused = demo.run("run", "--pipeline_code", "SUPPORT_DM")
    assert refused.returncode == 10, refused.stderr[-2000:]
    # The orchestrator is the only source of truth, and it has no equivalent of HAS_DATA or of
    # a 2-of-3 run condition: remote mode names each one rather than drop it.
    invalid = demo.run("validate")
    assert invalid.returncode == 1, invalid.stdout
    for rule in (
        "CLIENT_ALPHA.parse: depends on land with DEPENDENCY_TYPE = 'HAS_DATA'",
        "SUPPORT_DM.setup_fact: depends on interactions with DEPENDENCY_TYPE = 'HAS_DATA'",
        "SUPPORT_DM.source_counts: RUN_CONDITION = 'N' (RUN_CONDITION_COUNT = 2)",
    ):
        assert rule in " ".join(invalid.stdout.split()), (rule, invalid.stdout)
    unsupported = demo.run("run", "--pipeline_code", "SUPPORT_DM", "--init-only")
    assert unsupported.returncode == 18, unsupported.stderr[-2000:]
    assert "the remote orchestrator does not support this" in unsupported.stderr
    demo.seed("remote_mode.sql")
    assert demo.ok("validate").endswith("0 failed, 0 warning(s)\n")

    def execute(command: list[str]) -> int:
        assert command[0] == "etl-craft", command
        return demo.run(*command[1:]).returncode

    runs = {}

    def sense(sensor: dict) -> str | None:
        upstream = runs.get(sensor["external_dag_id"])
        if upstream is None:
            return None
        if sensor["external_task_id"] is None:
            return upstream.state
        return upstream.states.get(sensor["external_task_id"])

    for pipeline in ("CLIENT_ALPHA", "CLIENT_BETA", "SUPPORT_DM"):
        dag = yaml.safe_load(demo.ok("generate-yml", "--pipeline_code", pipeline))
        assert dag["max_active_runs"] == 1
        runs[pipeline] = run_dag(dag, execute, sense)
        run_id, status = demo.latest_run(pipeline)
        assert status == "SUCCESS", (pipeline, runs[pipeline].states)
        assert runs[pipeline].state == SUCCESS

    dm = runs["SUPPORT_DM"]
    # The dependencies on both clients' pipelines and tasks were the orchestrator's sensors.
    assert {name for name in dm.states if name.startswith("__wait_for_")} == {
        "__wait_for_CLIENT_ALPHA__",
        "__wait_for_CLIENT_BETA__",
        "__wait_for_CLIENT_ALPHA.purge_test_calls__",
        "__wait_for_CLIENT_BETA.parse__",
    }
    # The flaky feed failed its first try; the orchestrator's retry succeeded, and nothing that
    # finished was run again.
    assert dm.tries["flaky_feed"] == 2
    assert dm.states["on_failure"] == SKIPPED
    assert {
        name: state for name, state in dm.states.items() if name != "on_failure"
    } == dict.fromkeys((n for n in dm.states if n != "on_failure"), SUCCESS)
    run_id, _ = demo.latest_run("SUPPORT_DM")
    attempts = {task: tries for task, (_, tries) in demo.task_runs("SUPPORT_DM", run_id).items()}
    assert attempts["flaky_feed"] == 2 and attempts["interactions"] == 1
    assert demo.rows(
        "SELECT source_system, COUNT(*) FROM {c}.dm.support_fact GROUP BY source_system "
        "ORDER BY source_system"
    ) == [("ALPHA", 11), ("BETA", 8)]
    # A run whose task needed a retry did not run cleanly, and its alert says so.
    check_the_emails(demo, failed=False, outcome="COMPLETED_WITH_ERRORS")
