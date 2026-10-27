"""Generated projects and fixed-number budget evaluation need no deployment."""

import csv
import runpy
from pathlib import Path

import pytest
import yaml

from etl_craft.core.metrics import Metrics
from etl_craft.services.config_files import read_config_files

pytestmark = pytest.mark.unit
ROOT = Path(__file__).parents[2] / "benchmarks"
budgets = runpy.run_path(str(ROOT / "budgets.py"))
generator = runpy.run_path(str(ROOT / "generate.py"))
LIMITS, evaluate, histogram, percentile = (
    budgets[name] for name in ("LIMITS", "evaluate", "histogram", "percentile")
)
Scale, generate, project_rows = (generator[name] for name in ("Scale", "generate", "project_rows"))


def test_generator_writes_complete_config_with_acyclic_edges_and_daily_spread(tmp_path):
    template = tmp_path / "craft-connector.yml"
    template.write_text(yaml.safe_dump({"Engine": {"dev": {"Jdbc_url": "jdbc:sqlite:engine.db"}}}))
    folders = generate(tmp_path / "generated", template, Scale(2, 5, 3, 0.01, 0.2))
    assert len(folders) == 2
    times = set()
    for folder in folders:
        read_config_files(folder / "config")

        def rows(name, folder=folder):
            with (folder / "config" / name).open(encoding="utf-8") as stream:
                return list(csv.DictReader(stream))

        pipelines = rows("pipelines.csv")
        assert len(pipelines) == 5 and len(rows("tasks.csv")) == 15
        for row in pipelines:
            times.add(row["RUN_SCHEDULE"])
        for row in rows("pipeline_dependencies.csv"):
            assert row["DEPENDS_ON_PIPELINE_CODE"] < row["PIPELINE_CODE"]
        for row in rows("task_dependencies.csv"):
            assert row["DEPENDS_ON_TASK_CODE"] < row["TASK_CODE"]
        assert (folder / "python_scripts/work.py").exists()
    assert len(times) == 10
    assert sum(len(project_rows(index, Scale())["tasks.csv"]) for index in range(100)) == 100_000


def test_budgets_pass_fail_at_boundaries_and_fail_missing_or_nonfinite_evidence():
    values = dict.fromkeys(LIMITS, 0)
    assert all(result.passed for result in evaluate(values, 20))
    for name, limit in LIMITS.items():
        failed = {**values, name: 21 if limit is None else limit}
        assert not next(result for result in evaluate(failed, 20) if result.name == name).passed
        for absent in (None, float("nan"), float("inf")):
            assert not next(
                result for result in evaluate({**values, name: absent}, 20) if result.name == name
            ).passed
    assert next(
        r for r in evaluate({**values, "connections": 20}, 20) if r.name == "connections"
    ).passed
    assert percentile([4, 1, 3, 2], 0.95) == 4
    assert percentile([], 0.99) is None


@pytest.mark.parametrize(
    ("name", "good", "bad"),
    [
        ("admission_p99_seconds", 1.9, 2),
        ("queue_free_p99_seconds", 4.9, 5),
        ("postgres_cpu_percent", 59.9, 60),
        ("connections", 20, 21),
        ("controller_memory_bytes", 4 * 1024**3 - 1, 4 * 1024**3),
        ("tick_p95_seconds", 0.999, 1),
    ],
)
def test_fixed_release_budget_numbers(name, good, bad):
    baseline = dict.fromkeys(LIMITS, 0)
    for value, expected in ((good, True), (bad, False)):
        result = next(row for row in evaluate({**baseline, name: value}, 20) if row.name == name)
        assert result.passed is expected


def test_histogram_percentiles_combine_labels_and_fail_without_observations():
    registry = Metrics()
    registry.define("admission_latency_seconds", "Admission.", "histogram")
    assert histogram(registry.render({}), "etl_craft_admission_latency_seconds", 0.99) is None
    for project, duration in (("a", 0.01), ("b", 0.5)):
        registry.add("admission_latency_seconds", (("project", project),), duration)
    assert histogram(registry.render({}), "etl_craft_admission_latency_seconds", 0.99) == 0.5


def test_collector_matches_free_slot_kind_and_rejects_a_missing_measurement(monkeypatch):
    import importlib
    import io
    import json
    from types import SimpleNamespace

    monkeypatch.syspath_prepend(str(ROOT.parent))
    runner = importlib.import_module("benchmarks.run")
    document = (
        "etl_craft_controller_leader 1\n"
        'etl_craft_agent_slots{pool="default",slot_kind="warehouse",status="free"} 1\n'
        'etl_craft_queue_oldest_age_seconds{pool="default",slot_kind="python"} 90\n'
    )
    monkeypatch.setattr(
        runner, "open_request", lambda *args, **kwargs: io.BytesIO(document.encode())
    )
    stats = {
        "postgres_cpu_percent": 10,
        "connections": 8,
        "max_connections": 100,
        "controller_memory_bytes": 1000,
    }
    monkeypatch.setattr(
        runner.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(stdout=json.dumps(stats))
    )
    clock = iter((0, 0, 0, 0, 2))
    monkeypatch.setattr(
        runner, "time", SimpleNamespace(monotonic=lambda: next(clock), sleep=lambda _: None)
    )
    values = runner.collect(
        {"metrics": "http://127.0.0.1/metrics", "probe": ["probe"]},
        [SimpleNamespace(pid=123, poll=lambda: None)],
        1,
        1,
        80,
    )
    assert values["queue_free_p99_seconds"] == 0
    assert values["admission_p99_seconds"] is None
    assert not all(result.passed for result in evaluate(values, 80))


def test_runner_waits_for_leadership_and_expected_agent_registration(monkeypatch):
    import importlib
    import io
    from types import SimpleNamespace

    monkeypatch.syspath_prepend(str(ROOT.parent))
    runner = importlib.import_module("benchmarks.run")
    replies = iter(
        (
            b"etl_craft_controller_leader 1\n",
            b"etl_craft_controller_leader 1\netl_craft_active_agents 2\n",
        )
    )
    waits = []
    monkeypatch.setattr(runner, "open_request", lambda *args, **kwargs: io.BytesIO(next(replies)))
    monkeypatch.setattr(runner, "time", SimpleNamespace(monotonic=lambda: 0, sleep=waits.append))
    runner.ready({"metrics": "http://127.0.0.1/metrics"}, [SimpleNamespace(poll=lambda: None)], 2)
    assert waits == [0.1]


@pytest.mark.parametrize("first_active", [199, 200])
def test_reference_tick_budget_uses_only_intervals_at_200_active_runs(monkeypatch, first_active):
    import importlib
    import io
    import json
    from types import SimpleNamespace

    monkeypatch.syspath_prepend(str(ROOT.parent))
    runner = importlib.import_module("benchmarks.run")
    documents = []
    for count, active in ((1, first_active), (2, 200)):
        documents.append(
            (
                "etl_craft_controller_leader 1\n"
                f'etl_craft_runs{{status="IN-PROGRESS"}} {active}\n'
                f'etl_craft_controller_tick_duration_seconds_bucket{{le="0.5"}} {count}\n'
                f'etl_craft_controller_tick_duration_seconds_bucket{{le="+Inf"}} {count}\n'
            ).encode()
        )
    replies = iter(documents)
    monkeypatch.setattr(runner, "open_request", lambda *args, **kwargs: io.BytesIO(next(replies)))
    stats = {
        "postgres_cpu_percent": 10,
        "connections": 8,
        "max_connections": 100,
        "controller_memory_bytes": 1000,
    }
    monkeypatch.setattr(
        runner.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(stdout=json.dumps(stats))
    )
    clock = iter((0, 0, 0, 0, 0.5, 0.5, 0.5, 2))
    monkeypatch.setattr(
        runner, "time", SimpleNamespace(monotonic=lambda: next(clock), sleep=lambda _: None)
    )
    values = runner.collect(
        {"metrics": "http://127.0.0.1/metrics", "probe": ["probe"]},
        [SimpleNamespace(pid=123, poll=lambda: None)],
        1,
        1,
        80,
        200,
    )
    assert values["tick_p95_seconds"] == (0.5 if first_active == 200 else None)
    assert values["tick_qualified_intervals"] == (1 if first_active == 200 else 0)


def test_probe_emits_normalized_cpu_connections_and_resident_memory(monkeypatch, capsys):
    import importlib
    import json
    import os
    import sys
    from contextlib import nullcontext
    from types import SimpleNamespace

    monkeypatch.syspath_prepend(str(ROOT.parent))
    probe = importlib.import_module("benchmarks.probe")
    monkeypatch.setenv("BENCHMARK_CONTROLLER_PID", str(os.getpid()))
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "probe",
            "--config",
            "test.yml",
            "--postgres-container",
            "test-db",
            "--postgres-cpus",
            "4",
        ],
    )
    monkeypatch.setattr(
        probe, "Path", lambda _: SimpleNamespace(read_text=lambda **kwargs: "VmRSS: 100 kB\n")
    )
    monkeypatch.setattr(probe, "load_config", lambda _: object())
    row = SimpleNamespace(connections=12, maximum=100)
    conn = SimpleNamespace(exec_driver_sql=lambda _: SimpleNamespace(one=lambda: row))
    disposed = []
    engine = SimpleNamespace(
        connect=lambda: nullcontext(conn), dispose=lambda: disposed.append(True)
    )
    monkeypatch.setattr(probe, "engine_db", lambda _: engine)
    monkeypatch.setattr(probe.subprocess, "check_output", lambda *args, **kwargs: "200%\n")
    probe.main()
    result = json.loads(capsys.readouterr().out)
    assert result["postgres_cpu_percent"] == 50
    assert result["connections"] == 12 and result["max_connections"] == 100
    assert result["controller_memory_bytes"] == 102400
    assert disposed == [True]


@pytest.mark.parametrize("url", ["file:///etc/passwd", "http://external.example/metrics"])
def test_metrics_probe_refuses_files_and_external_plain_http(monkeypatch, url):
    import importlib

    from etl_craft.core.errors import UsageError

    monkeypatch.syspath_prepend(str(ROOT.parent))
    runner = importlib.import_module("benchmarks.run")
    opened = []
    monkeypatch.setattr(runner, "open_request", lambda *args, **kwargs: opened.append(args))
    with pytest.raises(UsageError):
        runner.metrics_document(url, 1)
    assert not opened


def test_prepare_provisions_only_missing_admin_projects(tmp_path, monkeypatch):
    runner = runpy.run_path(str(ROOT / "run.py"))
    calls = []
    for project in ("bench_000", "bench_001"):
        folder = tmp_path / project
        folder.mkdir()
        (folder / "craft-connector.yml").write_text(
            yaml.safe_dump(
                {"Secrets": {"Source_type": "ENVIRONMENT"}, "Orchestration": {"Project": project}},
                sort_keys=False,
            )
        )

    def command(config, project, *args):
        calls.append((project, args))
        if args[:2] == ("project", "list"):
            return '{"projects":[{"project":"bench_000"}]}'
        return ""

    monkeypatch.setitem(runner["prepare"].__globals__, "command", command)
    runner["prepare"](tmp_path)
    assert [project for project, args in calls if args[:2] == ("project", "add")] == ["bench_001"]
    for project in ("bench_000", "bench_001"):
        assert (project, ("config", "apply")) in calls
        assert (project, ("bundle", "publish")) in calls
