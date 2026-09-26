"""The Support Insights demo's warehouse work on Databricks and Snowflake, from the built wheel.

The part of the demo a warehouse changes: both clients landed and parsed, then the support mart
built from them (agents with SCD1, SCD2 and a soft delete, the fact, its business rules, the
summary, a scratch table dropped), its first run failing on the flaky feed and its second
succeeding, and the Engine DB cloned into the warehouse. Databricks with Delta and with UniForm;
Snowflake with its own tables and with Iceberg tables. The Engine DB is SQLite, and the emails go
to the local Mailpit.

The demo's schemas get a prefix unique to the run; they are the only schemas it makes, and it
drops them at the end (see ``fixtures.demo``).
"""

from __future__ import annotations

import pytest

from fixtures.demo import Demo, installed_cli

pytestmark = pytest.mark.timeout(3600)


@pytest.fixture(scope="module")
def cli(tmp_path_factory):
    return installed_cli("pip", tmp_path_factory.mktemp("venvs"), extras="databricks,snowflake")


@pytest.fixture
def demo(cli, tmp_path, request):
    made = Demo(cli, tmp_path / "support-insights", "sqlite", request.param)
    try:
        yield made.build()
    finally:
        made.close()


def run_the_demo(demo: Demo) -> None:
    assert "setup: created the Engine DB tables" in demo.ok("setup")
    demo.seed()
    assert demo.ok("validate").endswith("0 failed, 0 warning(s)\n")
    doctor = demo.run("doctor")
    assert "0 failed" in doctor.stdout.splitlines()[-1], doctor.stdout
    for pipeline in ("CLIENT_ALPHA", "CLIENT_BETA"):
        assert "SUCCESS" in demo.ok("run", "--pipeline_code", pipeline)
    first = demo.run("run", "--pipeline_code", "SUPPORT_DM")
    assert first.returncode == 1 and "flaky_feed (FAILED)" in first.stdout, first.stdout
    assert "SUCCESS" in demo.ok("run", "--pipeline_code", "SUPPORT_DM")

    fact = demo.rows(
        "SELECT source_system, COUNT(*) FROM {c}.dm.support_fact GROUP BY source_system "
        "ORDER BY source_system"
    )
    assert [(system, int(n)) for system, n in fact] == [("ALPHA", 11), ("BETA", 8)]
    agents = demo.rows(
        "SELECT agent_code, team, email, DELETE_FLAG FROM {c}.ds.agents ORDER BY agent_code"
    )
    assert [tuple(row) for row in agents] == [
        ("A01", "east", "ann@example.com", "N"),
        ("A02", "west", "bo@example.com", "N"),
        ("A03", "west", "cy@example.com", "N"),
        ("A04", "west", "di@example.com", "Y"),
    ]
    history = demo.rows(
        "SELECT agent_code, team, ACTIVE_FLAG FROM {c}.cdc.agent_history "
        "WHERE agent_code = 'A02' ORDER BY ACTIVE_FLAG"
    )
    assert [tuple(row) for row in history] == [("A02", "east", "N"), ("A02", "west", "Y")]
    flagged = demo.engine_rows(
        "SELECT r.BUSINESS_RULE_TYPE AS kind, COUNT(*) AS n FROM AUD_BUSINESS_RULES_RESULTS x "
        "JOIN CFG_BUSINESS_RULES r ON r.BUSINESS_RULE_ID = x.BUSINESS_RULE_ID "
        "WHERE x.ACTIVE_FLAG = 'Y' GROUP BY r.BUSINESS_RULE_TYPE ORDER BY r.BUSINESS_RULE_TYPE"
    )
    assert [(kind, int(n)) for kind, n in flagged] == [
        ("INCOMPLETE", 1),
        ("REJECT", 2),
        ("REPORT", 3),
    ]
    subjects = {subject for subject, _ in demo.emails()}
    assert {"SUPPORT_DM: FAILED", "SUPPORT_DM: SUCCESS", "SUPPORT_DM: a feed failed"} <= subjects
    if demo.warehouse_kind != "snowflake_iceberg":
        assert [int(n) for (n,) in demo.rows("SELECT COUNT(*) FROM {c}.aud.CFG_PIPELINES")] == [5]


@pytest.mark.cloud_databricks
@pytest.mark.parametrize("demo", ["databricks", "databricks_iceberg"], indirect=True)
def test_the_demo_on_databricks(demo):
    run_the_demo(demo)


@pytest.mark.cloud_snowflake
@pytest.mark.parametrize("demo", ["snowflake", "snowflake_iceberg"], indirect=True)
def test_the_demo_on_snowflake(demo):
    run_the_demo(demo)
