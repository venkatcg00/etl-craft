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
