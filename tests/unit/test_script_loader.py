"""Ingestion script module registration, names and source reloads."""

import sys

import pytest

from etl_craft.core.errors import HandlerError
from etl_craft.handlers.python_scripts import load_script

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def restore_modules_and_path():
    modules = dict(sys.modules)
    path = list(sys.path)
    yield
    for name in list(sys.modules):
        if name.startswith("etl_craft_script_"):
            if name in modules:
                sys.modules[name] = modules[name]
            else:
                del sys.modules[name]
    sys.path[:] = path


def test_a_nested_script_has_its_relative_name_and_file_spec(tmp_path):
    path = tmp_path / "load.py"
    path.write_text("def run():\n    return 3\n", encoding="utf-8")
    entry = load_script(path, "crm/load.py")
    module = sys.modules["etl_craft_script_crm_load_py"]
    assert module.run is entry
    assert module.__file__ == str(path)
    assert module.__spec__.name == module.__name__
    assert module.__spec__.origin == str(path)


@pytest.mark.parametrize("source", ["raise ValueError('broken')\n", "raise SystemExit(3)\n"])
def test_a_failed_import_removes_its_module(tmp_path, source):
    path = tmp_path / "load.py"
    path.write_text(source, encoding="utf-8")
    with pytest.raises((HandlerError, SystemExit)):
        load_script(path, "load.py")
    assert "etl_craft_script_load_py" not in sys.modules


def test_a_script_is_compiled_from_its_current_source(tmp_path):
    path = tmp_path / "load.py"
    path.write_text("def run():\n    return 3\n", encoding="utf-8")
    assert load_script(path, "load.py")() == 3
    path.write_text("def run():\n    return 4\n", encoding="utf-8")
    assert load_script(path, "load.py")() == 4


def test_scripts_in_different_folders_have_separate_modules(tmp_path):
    for folder, value in [("crm", 3), ("sales", 4)]:
        path = tmp_path / folder / "load.py"
        path.parent.mkdir()
        path.write_text(f"def run():\n    return {value}\n", encoding="utf-8")
        load_script(path, f"{folder}/load.py")
    assert sys.modules["etl_craft_script_crm_load_py"].run() == 3
    assert sys.modules["etl_craft_script_sales_load_py"].run() == 4
