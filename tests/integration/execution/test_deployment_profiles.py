"""Deployment profiles at role startup on both Engine DBs: refusals, pools and the budget."""

from dataclasses import replace

import yaml
from sqlalchemy import text

from etl_craft.cli import main
from etl_craft.config.sizing import start_role
from etl_craft.core.enums import DeploymentProfile
from etl_craft.core.errors import ConfigurationError
from etl_craft.core.resources import GIB, Host
from etl_craft.engine.connection import engine_db
from etl_craft.execution.pools.local import LocalPool
from etl_craft.services.doctor import run_checks
from fixtures.auth import service_token
from fixtures.metadata import add_pipeline, add_task
from fixtures.scoped_controls import ctx as ctx

MACHINE = Host({"/proc/self/cgroup": "0::/\n"}.get, lambda: 8, 8, 16 * GIB)


def test_server_workers_refuses_execution_and_starts_nothing(ctx, capsys, monkeypatch):
    monkeypatch.setenv("ETL_CRAFT_TOKEN", service_token(ctx.engine, "ops"))
    with ctx.engine.begin() as conn:
        add_task(conn, add_pipeline(conn, "P"), "T")
    path = ctx.config.config_path
    raw = yaml.safe_load(path.read_text())
    path.write_text(
        yaml.safe_dump(
            {
                "Secrets": raw["Secrets"],
                "Orchestration": raw["Orchestration"],
                "Deployment": {"Profile": "CONTROLLER_AGENTS"},
                "Engine": raw["Engine"],
            },
            sort_keys=False,
        )
    )
    code = main(["--config", str(path), "run", "--pipeline-code", "P", "--init-only"])
    error = capsys.readouterr().err
    assert code == ConfigurationError.exit_code
    shared = ctx.engine.dialect.name == "postgresql"
    assert ("no agent queue provider" if shared else "share one PostgreSQL") in error
    with ctx.engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM AUD_PIPELINES_RUN_LOG")).scalar() == 0


def test_a_sized_role_builds_its_pool_and_a_server_without_slots_admits_nothing(ctx):
    config = start_role(
        replace(ctx.config, deployment=DeploymentProfile.CONTROLLER), server=True, host=MACHINE
    )
    assert config.sizing is not None
    engine = engine_db(config)
    try:
        assert engine.pool.size() == config.sizing.engine_pool.value
    finally:
        engine.dispose()
    empty = replace(
        config,
        limits=replace(
            config.limits, max_parallel_tasks=0, local_python_slots=0, local_warehouse_slots=0
        ),
    )
    pool = LocalPool(ctx.engine, empty)
    try:
        assert set(pool.capacity().free.values()) == {0}
    finally:
        pool.close()


def test_doctor_counts_the_connection_budget_where_the_server_has_a_limit(ctx):
    checks = {c.name: c for c in run_checks(ctx.config)}
    budget = checks.get("Engine DB connection budget")
    if ctx.engine.dialect.name == "postgresql":
        assert budget is not None, [(c.name, c.detail[:80]) for c in checks.values()]
        assert "of max_connections=" in budget.detail
        assert f"+ one per task process ({ctx.config.limits.max_parallel_tasks})" in budget.detail
    else:
        assert budget is None, budget
    assert "Sizing: task slots" in checks, list(checks)
