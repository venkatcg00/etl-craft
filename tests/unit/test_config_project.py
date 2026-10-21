"""The project directory, and the SQL files and scripts a task names in it."""

from pathlib import Path

import pytest

from etl_craft.config import ConnectionProfile, ConnectorConfig, SourceConfig
from etl_craft.config.project import python_script, sql_script
from etl_craft.core.errors import MetadataError

pytestmark = pytest.mark.unit


def project_config(tmp_path, config_path=True):
    engine = ConnectionProfile("ENGINE", "dev", "jdbc:sqlite:e.db", "", "NONE")
    return ConnectorConfig(
        source=SourceConfig(type="environment"),
        engine=engine,
        config_path=tmp_path / "etl-craft" / "craft-connector.yml" if config_path else None,
    )


@pytest.fixture
def project(tmp_path):
    root = tmp_path / "etl-craft"
    (root / "sql_scripts" / "sales").mkdir(parents=True)
    (root / "sql_scripts" / "sales" / "orders.sql").write_text("SELECT 1", encoding="utf-8")
    (root / "python_scripts").mkdir()
    (root / "python_scripts" / "load_orders.py").write_text("", encoding="utf-8")
    return project_config(tmp_path)


def test_the_project_directory_holds_everything(tmp_path, monkeypatch):
    config = project_config(tmp_path)
    root = tmp_path / "etl-craft"
    assert config.project_dir == root
    assert (config.sql_scripts_dir, config.python_scripts_dir, config.migrations_dir) == (
        root / "sql_scripts",
        root / "python_scripts",
        root / "migrations",
    )
    monkeypatch.chdir(tmp_path)
    assert project_config(tmp_path, config_path=False).project_dir == Path.cwd()


def test_files_are_found_in_their_folders(project):
    root = project.project_dir
    orders = root / "sql_scripts" / "sales" / "orders.sql"
    assert sql_script(project, "sql_scripts/sales/orders.sql") == orders
    assert sql_script(project, " sql_scripts/sales/orders.sql ") == orders
    assert python_script(project, "python_scripts/load_orders.py") == (
        root / "python_scripts" / "load_orders.py"
    )


@pytest.mark.parametrize(
    ("name", "message"),
    [
        ("", "SOURCE_SQL_FILE='' is empty"),
        ("/etc/passwd.sql", "must be a path relative to"),
        ("../secrets.sql", "without '..'"),
        (
            "sales/orders.sql",
            "must start with sql_scripts/: .* write 'sql_scripts/sales/orders.sql'",
        ),
        ("python_scripts/orders.sql", "must start with sql_scripts/"),
        ("sql_scripts/sales/orders.txt", "must name a .sql file"),
        (
            "sql_scripts/sales/order.sql",
            r"no such file .*order\.sql — did you mean: sql_scripts/sales/orders\.sql",
        ),
        ("sql_scripts/missing/none.sql", r"no such file [^—]*$"),
    ],
)
def test_a_bad_sql_script_name_says_what_is_wrong(project, name, message):
    with pytest.raises(MetadataError, match=message):
        sql_script(project, name)


def test_a_missing_folder_is_named(tmp_path):
    with pytest.raises(
        MetadataError,
        match=r"SCRIPT_FILE='python_scripts/x.py': the project has no python_scripts/",
    ):
        python_script(project_config(tmp_path), "python_scripts/x.py")


@pytest.mark.parametrize(
    ("reader", "folder", "suffix"),
    [
        (sql_script, "sql_scripts", ".sql"),
        (python_script, "python_scripts", ".py"),
    ],
)
def test_symlinks_cannot_read_outside_their_folder(project, reader, folder, suffix):
    external = project.project_dir / ("external" + suffix)
    external.write_text("do not read this", encoding="utf-8")
    link = project.project_dir / folder / ("linked" + suffix)
    link.symlink_to(external)
    with pytest.raises(MetadataError, match="resolves outside"):
        reader(project, f"{folder}/{link.name}")
