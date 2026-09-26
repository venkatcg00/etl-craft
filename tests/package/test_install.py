"""The built wheel and sdist install with pip and with uv, and the installed package works.

The wheel is the one under test (``ETL_CRAFT_TEST_WHEEL``, set by ``scripts/run_suite.py
--wheel``); the sdist is built beside it, with the same version. Each is installed into a clean
virtual environment, outside the source tree, and checked there: the version, the command line,
and the SQL and catalog files that ship inside the package.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

pytestmark = [pytest.mark.package, pytest.mark.timeout(900)]

SHIPPED = (
    "etl_craft/__init__.py",
    "etl_craft/py.typed",
    "etl_craft/cli/__init__.py",
    "etl_craft/dialects/engine/postgres/schema.sql",
    "etl_craft/dialects/engine/sqlite/schema.sql",
    "etl_craft/dialects/engine/queries/applied_migrations.sql",
    "etl_craft/services/catalog_assets/catalog.css",
    "etl_craft/services/catalog_assets/catalog.js",
)

CHECK = """
import sys
import etl_craft
from etl_craft.dialects.engine import all_dialects
assert etl_craft.__version__ == sys.argv[1], etl_craft.__version__
for dialect in all_dialects():
    assert "CREATE TABLE CFG_PIPELINES" in dialect.schema_path().read_text(encoding="utf-8")
    assert dialect.query("existing_tables")
"""


def _artifact(kind: str) -> Path:
    wheel = os.environ.get("ETL_CRAFT_TEST_WHEEL")
    if not wheel:
        message = "the package suite tests the built wheel: make suite SUITE=package WHEEL=..."
        if os.environ.get("ETL_CRAFT_REQUIRE_SERVICES") == "1":
            pytest.fail(message, pytrace=False)
        pytest.skip(message)
    path = Path(wheel)
    if kind == "wheel":
        return path
    version = path.name.split("-")[1]
    sdist = path.with_name(f"etl_craft-{version}.tar.gz")
    assert sdist.is_file(), f"no sdist beside the wheel: expected {sdist}"
    return sdist


def _bin(venv: Path, name: str) -> Path:
    folder = venv / ("Scripts" if os.name == "nt" else "bin")
    return folder / (f"{name}.exe" if os.name == "nt" else name)


def test_the_wheel_holds_the_package_files_and_the_licence():
    names = zipfile.ZipFile(_artifact("wheel")).namelist()
    assert [name for name in SHIPPED if name not in names] == []
    assert any(name.endswith("licenses/LICENSE") for name in names)


@pytest.mark.parametrize("installer", ["pip", "uv"])
@pytest.mark.parametrize("kind", ["wheel", "sdist"])
def test_it_installs_and_runs(tmp_path, installer, kind):
    artifact = _artifact(kind)
    version = _artifact("wheel").name.split("-")[1]
    venv = tmp_path / "venv"
    if installer == "pip":
        subprocess.run([sys.executable, "-m", "venv", str(venv)], check=True)
        install = [str(_bin(venv, "python")), "-m", "pip", "install", "--quiet"]
    else:
        uv = shutil.which("uv")
        assert uv, "uv is not on PATH"
        subprocess.run([uv, "venv", "--quiet", "--python", sys.executable, str(venv)], check=True)
        install = [uv, "pip", "install", "--quiet", "--python", str(_bin(venv, "python"))]
    subprocess.run([*install, str(artifact)], check=True, timeout=600, cwd=tmp_path)

    def run(*argv: str) -> str:
        return subprocess.run(
            argv, check=True, capture_output=True, text=True, cwd=tmp_path
        ).stdout.strip()

    assert run(str(_bin(venv, "etl-craft")), "--version") == f"etl-craft {version}"
    assert run(str(_bin(venv, "python")), "-m", "etl_craft", "--version") == (
        f"etl-craft {version}"
    )
    run(str(_bin(venv, "python")), "-c", CHECK, version)
