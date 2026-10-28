"""Changed data files force the suites Python execution tracking cannot select."""

import pytest

from fixtures.scripts import load

pytestmark = pytest.mark.unit
changed = load("test_changed")


@pytest.mark.parametrize(
    "path",
    [
        "src/etl_craft/dialects/engine/queries/refresh_good.sql",
        "src/etl_craft/dialects/engine/sqlite/schema.sql",
        "src/etl_craft/dialects/engine/postgres/migrations/0067_pipeline_refresh.sql",
        "src/etl_craft/dialects/engine/migration-markers.toml",
        "release/required-suites.toml",
        "examples/demo/config/pipelines.csv",
        "docs/examples/craft-connector.yml",
        "templates/project/config/tasks.csv",
        "tests/fixtures/schemas/0.3.0/postgres.sql",
    ],
)
def test_files_loaded_as_data_force_full_engine_and_warehouse_suites(path):
    assert changed.needs_full_suites([path])


def test_python_only_changes_use_testmon():
    assert not changed.needs_full_suites(["src/etl_craft/core/time.py", "README.md"])


def test_data_changes_run_full_suites_before_the_testmon_selection(monkeypatch):
    import sys
    from types import SimpleNamespace

    calls = []
    monkeypatch.setattr(sys, "argv", ["test_changed", "--workers", "2"])
    monkeypatch.setattr(
        changed.subprocess, "check_output", lambda *_, **__: "release/required-suites.toml\n"
    )
    monkeypatch.setattr(
        changed.subprocess,
        "run",
        lambda command, **_: calls.append(command) or SimpleNamespace(returncode=0),
    )
    assert changed.main() == 0
    assert "--testmon" not in calls[0] and changed.SERVICE_MARKERS in calls[0]
    assert "--testmon" in calls[1]
    assert all(command[command.index("-n") + 1] == "2" for command in calls)


def test_a_failed_full_suite_does_not_continue_with_selection(monkeypatch):
    import sys
    from types import SimpleNamespace

    calls = []
    monkeypatch.setattr(sys, "argv", ["test_changed"])
    monkeypatch.setattr(
        changed.subprocess, "check_output", lambda *_, **__: "examples/demo/config/tasks.csv\n"
    )
    monkeypatch.setattr(
        changed.subprocess,
        "run",
        lambda command, **_: calls.append(command) or SimpleNamespace(returncode=1),
    )
    assert changed.main() == 1
    assert len(calls) == 1
    assert calls[0][calls[0].index("-n") + 1] == "4"
