"""The generated reference pages list exactly what the code reads, and every name is explained."""

import re

import pytest

from etl_craft.config.loader import settings_by_section
from fixtures.scripts import load

pytestmark = pytest.mark.unit

pages = load("reference_pages")
GUIDES = pages.REPO / "docs" / "guides"
GUIDE_FILES = {
    "Every task": "pipelines-and-tasks.md",
    "SQL": "sql-tasks.md",
    "PYTHON": "ingestion-scripts.md",
    "EMAIL_ALERT": "email-alerts.md",
}
SCHEMAS = pages.REPO / "src" / "etl_craft" / "dialects" / "engine"


def test_every_setting_is_shown_in_the_annotated_reference_or_an_example():
    unshown = [
        f"{section}.{key}"
        for section, keys in settings_by_section().items()
        for key in keys
        if not pages.setting_sources(key)
    ]
    assert unshown == []
    email = settings_by_section()["Orchestration.Email"]
    annotated = pages.ANNOTATED.read_text(encoding="utf-8")
    assert [key for key in email if not pages.written_in(key, annotated)] == []


def test_every_task_parameter_is_explained_in_its_handlers_guide():
    unexplained = []
    for handler, params in pages.handler_parameters().items():
        guide = (GUIDES / GUIDE_FILES[handler]).read_text(encoding="utf-8")
        for name, _ in params:
            named = re.search(rf"\b{name}\b", guide)
            if not named and f"`{pages.OUTCOME_PATTERNS.get(name)}`" not in guide:
                unexplained.append(f"{handler}: {name}")
    assert unexplained == []


def test_the_parameter_page_lists_what_each_handler_reads():
    page = pages.task_parameters_page()
    assert "| `SCHEMA_EVOLUTION` | `OVERWRITE_TABLE`, `SCD1_MERGE`, `SCD2_MERGE` |" in page
    assert "| `SCRIPT_NAME` |  |" in page
    assert "| `EMAIL_SUBJECT_FAILED` | one outcome: `EMAIL_SUBJECT_<OUTCOME>` |" in page
    assert "| `TASK_TIMEOUT_SECONDS` |  |" in page


@pytest.mark.parametrize("dialect", ["postgres", "sqlite"])
def test_every_engine_db_table_is_described_and_read_whole(dialect):
    tables = pages.schema_tables(SCHEMAS / dialect / "schema.sql")
    text = (SCHEMAS / dialect / "schema.sql").read_text(encoding="utf-8")
    assert [t.name for t in tables] == re.findall(r"^CREATE TABLE (\w+)", text, re.MULTILINE)
    assert [t.name for t in tables if not t.comment] == []
    for table in tables:
        body = re.search(rf"CREATE TABLE {table.name} \((.*?)\n\);", text, re.DOTALL).group(1)
        declared = [
            line.split()[0]
            for line in body.splitlines()
            if re.match(r"^\s+[A-Z_]+\s", line)
            and line.split()[0] not in {"CONSTRAINT", "CHECK", "OR", "AND"}
        ]
        assert [c.name for c in table.columns] == declared, table.name


def test_the_schema_page_shows_types_allowed_values_and_unique_keys():
    page = pages.schema_page()
    assert "## `AUD_RUN_INTERVENTIONS`" in page
    assert "| `REFRESH_TYPE` | `VARCHAR` | no |  | `FULL`, `INCREMENTAL` |" in page
    assert "Unique: `PIPELINE_CODE where ACTIVE_FLAG = 'Y'`." in page
    assert "| `ACTIVE_FLAG` | `VARCHAR` | no | `'Y'` | `Y`, `N` |" in page


def test_the_configuration_page_has_every_section():
    page = pages.configuration_page()
    for section in settings_by_section():
        assert f"## `{section}`" in page
    assert "| `Dependency_gates` | [`craft-connector.example.yml`]" in page
