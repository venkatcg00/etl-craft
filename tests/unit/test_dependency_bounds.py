"""Every installed dependency has a finite tested compatibility range."""

import importlib.util
import tomllib
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit


def test_dependency_bounds_install_matrix():
    root = Path(__file__).parents[2]
    spec = importlib.util.spec_from_file_location(
        "dependency_bounds", root / "scripts/dependency_bounds.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    project = tomllib.loads((root / "pyproject.toml").read_text())["project"]
    bounds = module.lower_bounds(project)
    assert "sqlglot>=30.22,<30.23" in bounds
    assert "duckdb>=1.5.5,<2" in bounds
    assert any(row.startswith("psycopg[binary]>=") for row in bounds)
    script = (root / "scripts/verify_package.sh").read_text()
    assert "for bounds in lower caps" in script
    assert '"$wheel[all,publish]"' in script
    assert "uv pip check" in script
