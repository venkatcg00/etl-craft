"""The project directory, and the SQL files and scripts a task names in it."""

from pathlib import Path

import pytest

from etl_craft.config import ConnectionProfile, ConnectionSection, ConnectorConfig, SourceConfig
from etl_craft.config.project import ingestion_script, sql_file
from etl_craft.core.enums import Mode
from etl_craft.core.errors import MetadataError

pytestmark = pytest.mark.unit


def project_config(tmp_path, config_path=True):
    engine = ConnectionProfile("ENGINE", "dev", "jdbc:sqlite:e.db", "", "none")
    return ConnectorConfig(
        mode=Mode.LOCAL,
        source=SourceConfig(type="environment"),
        engine=ConnectionSection("dev", {"dev": engine}),
        config_path=tmp_path / "etl-craft" / "craft-connector.yml" if config_path else None,
    )


@pytest.fixture
def project(tmp_path):
    root = tmp_path / "etl-craft"
    (root / "sql_files" / "sales").mkdir(parents=True)
    (root / "sql_files" / "sales" / "orders.sql").write_text("SELECT 1", encoding="utf-8")
    (root / "ingestion_scripts").mkdir()
    (root / "ingestion_scripts" / "load_orders.py").write_text("", encoding="utf-8")
    return project_config(tmp_path)


def test_the_project_directory_holds_everything(tmp_path, monkeypatch):
    config = project_config(tmp_path)
    root = tmp_path / "etl-craft"
    assert config.project_dir == root
    assert (config.sql_files_dir, config.ingestion_scripts_dir, config.migrations_dir) == (
        root / "sql_files",
        root / "ingestion_scripts",
        root / "migrations",
    )
    monkeypatch.chdir(tmp_path)
    assert project_config(tmp_path, config_path=False).project_dir == Path.cwd()


def test_files_are_found_in_their_folders(project):
    root = project.project_dir
    assert sql_file(project, "sales/orders.sql") == root / "sql_files" / "sales" / "orders.sql"
    assert sql_file(project, " sales/orders.sql ") == root / "sql_files" / "sales" / "orders.sql"
    assert ingestion_script(project, "load_orders.py") == (
        root / "ingestion_scripts" / "load_orders.py"
    )


@pytest.mark.parametrize(
    ("name", "message"),
    [
        ("", "SOURCE_SQL_FILE='' is empty"),
        ("/etc/passwd.sql", "must be a path relative to"),
        ("../secrets.sql", "without '..'"),
        ("sales/orders.txt", "must name a .sql file"),
        ("sales/order.sql", r"no such file .*order\.sql — did you mean: sales/orders\.sql"),
        ("missing/none.sql", r"no such file [^—]*$"),
    ],
)
def test_a_bad_sql_file_name_says_what_is_wrong(project, name, message):
    with pytest.raises(MetadataError, match=message):
        sql_file(project, name)


def test_a_missing_folder_is_named(tmp_path):
    with pytest.raises(
        MetadataError, match=r"SCRIPT_NAME='x.py': the project has no ingestion_scripts/"
    ):
        ingestion_script(project_config(tmp_path), "x.py")
