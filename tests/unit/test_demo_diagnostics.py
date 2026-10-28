"""Demo evidence stays CI-only and within the failing test's catalog namespaces."""

import json
from io import BytesIO

import pytest
import yaml

from plugins import demo_diagnostics

pytestmark = pytest.mark.unit


def test_demo_evidence_is_disabled_without_ci_artifact_directory(monkeypatch):
    monkeypatch.delenv("ETL_CRAFT_CI_DIAGNOSTICS", raising=False)
    assert demo_diagnostics.destination("tests/e2e/test_demo.py::test") is None


def test_demo_evidence_paths_are_independent_and_do_not_embed_node_ids(monkeypatch, tmp_path):
    monkeypatch.setenv("ETL_CRAFT_CI_DIAGNOSTICS", str(tmp_path))
    first = demo_diagnostics.destination("tests/e2e/test_demo.py::test[a]")
    second = demo_diagnostics.destination("tests/e2e/test_demo.py::test[b]")
    assert first.parent == second.parent == tmp_path
    assert first != second
    assert "test" not in first.name


def test_demo_catalog_capture_includes_only_generated_project_namespaces(monkeypatch, tmp_path):
    project = tmp_path / "project"
    project.mkdir()
    target = tmp_path / "evidence"
    target.mkdir()
    (project / "craft-connector.yml").write_text(
        yaml.safe_dump({"Warehouse": {"dev": {"Schema": "ec_0123abcd_aud"}}})
    )
    calls = []

    def read(request, timeout):
        calls.append(request)
        if request.endswith("/tables"):
            return BytesIO(json.dumps({"identifiers": [{"name": "agents"}]}).encode())
        return BytesIO(b'{"metadata":{"current-snapshot-id":17}}')

    monkeypatch.setattr(demo_diagnostics.urllib.request, "urlopen", read)
    demo_diagnostics.catalog(project, target)
    captured = json.loads((target / "catalog.json").read_text())
    assert "ec_0123abcd_ds.agents" in captured
    assert all("/ec_0123abcd_" in url for url in calls)
    assert demo_diagnostics.namespaces("another_project") == ()
    assert demo_diagnostics.namespaces("../../etc/passwd") == ()


def test_failure_capture_saves_attempt_logs_before_catalog_failure(monkeypatch, tmp_path):
    project = tmp_path / "support-insights"
    (project / "logs").mkdir(parents=True)
    (project / "logs" / "attempt.log").write_text("warehouse statement failed\n")
    target = tmp_path / "evidence"

    def unavailable(project, target):
        raise OSError("catalog unavailable")

    monkeypatch.setattr(demo_diagnostics, "catalog", unavailable)
    with pytest.raises(OSError, match="catalog unavailable"):
        demo_diagnostics.failure_artifacts(project, target)
    assert (target / "logs" / "attempt.log").read_text() == "warehouse statement failed\n"


def test_failed_evidence_capture_does_not_replace_the_original_failure(monkeypatch, tmp_path):
    from types import SimpleNamespace

    monkeypatch.setenv("ETL_CRAFT_CI_DIAGNOSTICS", str(tmp_path / "artifacts"))
    temporary = tmp_path / "test"
    project = temporary / "support-insights"
    project.mkdir(parents=True)
    (project / "craft-connector.yml").write_text("Warehouse: {}\n")
    item = SimpleNamespace(
        nodeid="tests/e2e/test_demo.py::failure", funcargs={"tmp_path": temporary}
    )

    def unavailable(project, target):
        raise OSError("catalog unavailable")

    monkeypatch.setattr(demo_diagnostics, "failure_artifacts", unavailable)
    hook = demo_diagnostics.pytest_runtest_makereport(item, None)
    next(hook)
    with pytest.raises(StopIteration):
        hook.send(SimpleNamespace(get_result=lambda: SimpleNamespace(failed=True)))
    evidence = demo_diagnostics.destination(item.nodeid)
    assert (evidence / "capture-error.txt").read_text() == "OSError: catalog unavailable"


def test_stack_capture_refuses_a_reused_pid(monkeypatch, tmp_path):
    from types import SimpleNamespace

    child = SimpleNamespace(
        pid=73,
        create_time=lambda: 1.0,
        cmdline=lambda: ["python", "-m", "etl_craft.execution.child"],
    )
    parent = SimpleNamespace(children=lambda recursive: [child])
    stop = SimpleNamespace(wait=iter((False, True)).__next__)
    monkeypatch.setattr(
        demo_diagnostics.psutil,
        "Process",
        lambda pid=None: parent if pid is None else SimpleNamespace(create_time=lambda: 2.0),
    )
    commands = []
    monkeypatch.setattr(demo_diagnostics.subprocess, "run", lambda *a, **kw: commands.append(a))

    class Stop:
        def wait(self, seconds):
            return stop.wait()

    demo_diagnostics.capture_stacks(tmp_path, Stop())
    assert commands == []
