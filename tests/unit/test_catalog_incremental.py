"""Incremental generation through the public site writer, including damaged caches."""

import json
import os
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from etl_craft.config import parse_config
from etl_craft.core.errors import ConfigurationError
from etl_craft.engine.repository.catalog import (
    Consumption,
    LastRun,
    PipelineRow,
    RunSummary,
    TaskRow,
)
from etl_craft.services.catalog import Catalog, Column, PipelineAsset, TableAsset, TaskAsset
from etl_craft.services.catalog_site import write_site
from etl_craft.services.lineage import Edge
from fixtures.races import paused_after

pytestmark = pytest.mark.unit
AT = datetime(2026, 10, 1, tzinfo=UTC)
STAMP = 1_000_000_000_000_000_000
MANIFEST = ".etl-craft-manifest.json"


@pytest.fixture
def scene(tmp_path):
    config = parse_config(
        {
            "Secrets": {"Source_type": "environment"},
            "Orchestration": {},
            "Engine": {"dev": {"jdbc_url": "jdbc:sqlite:engine.db", "schema": "main"}},
        },
        tmp_path / "craft-connector.yml",
    )
    (tmp_path / "sql_files").mkdir()
    catalog = Catalog(AT, {}, {}, {}, {}, {}, [], [])
    for task_id, code in enumerate(("ALPHA", "BETA"), 1):
        target, label = f"data.{code.lower()}", f"{code}.load"
        row = PipelineRow(task_id, code, code, None, None, None, "FULL", None)
        catalog.pipelines[code] = PipelineAsset(row, tasks=[label])
        task = TaskAsset(
            TaskRow(task_id, code, "load", "ETL", "SQL", None, None),
            {
                "SQL_ACTION": "CREATE_TABLE",
                "TARGET_OBJECT": target,
                "SOURCE_SQL_FILE": f"{code}.sql",
            },
            None,
            None,
            target=target,
        )
        edge = Edge(target, "id", None, None, "1", label)
        task.column_edges.append(edge)
        catalog.column_edges.append(edge)
        catalog.tasks[label] = task
        catalog.tables[target] = TableAsset(target, columns={"id": Column("id")}, writers=[label])
        (tmp_path / f"sql_files/{code}.sql").write_text("SELECT 1 AS id")
    folder = tmp_path / "catalog"
    write_site(catalog, config, folder)
    for page in folder.rglob("*.html"):
        os.utime(page, ns=(STAMP, STAMP))
    catalog.generated_at += timedelta(days=1)
    return catalog, config, folder


def test_unchanged_pipeline_pages_are_reused_and_aggregate_pages_are_fresh(scene):
    catalog, config, folder = scene
    first = (folder / "pipelines/ALPHA.html").read_bytes()
    write_site(catalog, config, folder)
    assert (folder / "pipelines/ALPHA.html").read_bytes() == first
    assert (folder / "pipelines/ALPHA.html").stat().st_mtime_ns == STAMP
    assert (folder / "tasks/ALPHA.load.html").stat().st_mtime_ns == STAMP
    assert (folder / "index.html").stat().st_mtime_ns != STAMP
    assert catalog.generated_at.isoformat() in (folder / "index.html").read_text()


def test_metadata_change_rebuilds_its_pipeline_without_rebuilding_another(scene):
    catalog, config, folder = scene
    catalog.pipelines["BETA"].row = replace(catalog.pipelines["BETA"].row, pipeline_name="New name")
    write_site(catalog, config, folder)
    assert "New name" in (folder / "pipelines/BETA.html").read_text()
    assert (folder / "pipelines/BETA.html").stat().st_mtime_ns != STAMP
    assert (folder / "pipelines/ALPHA.html").stat().st_mtime_ns == STAMP


def test_sql_file_changes_invalidate_the_task_page(scene):
    catalog, config, folder = scene
    (config.project_dir / "sql_files/BETA.sql").write_text("SELECT 2 AS id")
    write_site(catalog, config, folder)
    assert "SELECT 2 AS id" in (folder / "tasks/BETA.load.html").read_text()
    assert (folder / "pipelines/ALPHA.html").stat().st_mtime_ns == STAMP


def test_new_history_rebuilds_only_its_pipeline_and_adds_the_run_page(scene):
    catalog, config, folder = scene
    run = RunSummary(1, "SUCCESS", None, False, AT, AT + timedelta(seconds=1), None)
    catalog.pipelines["BETA"].runs = [run]
    write_site(catalog, config, folder)
    assert (folder / "runs/1.html").is_file()
    assert (folder / "pipelines/BETA.html").stat().st_mtime_ns != STAMP
    assert (folder / "pipelines/ALPHA.html").stat().st_mtime_ns == STAMP


def test_sql_file_changed_during_generation_uses_one_content_snapshot(scene):
    catalog, config, folder = scene
    source = config.project_dir / "sql_files/BETA.sql"
    source.write_text("SELECT 2 AS id")
    with paused_after(
        lambda: write_site(catalog, config, folder),
        owner=Path,
        name="open",
        when=lambda path, *_a, **_k: path == source,
    ):
        replacement = source.with_name("replacement.sql")
        replacement.write_text("SELECT 3 AS id")
        replacement.replace(source)
    assert "SELECT 2 AS id" in (folder / "tasks/BETA.load.html").read_text()
    assert "SELECT 3 AS id" not in (folder / "tasks/BETA.load.html").read_text()
    assert (folder / "pipelines/ALPHA.html").stat().st_mtime_ns == STAMP
    write_site(catalog, config, folder)
    assert "SELECT 3 AS id" in (folder / "tasks/BETA.load.html").read_text()


def test_schema_changes_rebuild_the_table_without_rebuilding_an_unrelated_table(scene):
    catalog, config, folder = scene
    catalog.tables["data.beta"].columns["id"].data_type = "DECIMAL(9,2)"
    catalog.tables["data.beta"].columns["id"].declared_type = "DECIMAL(9,2)"
    write_site(catalog, config, folder)
    assert "DECIMAL(9,2)" in (folder / "tables/data.beta.html").read_text()
    assert (folder / "tables/data.alpha.html").stat().st_mtime_ns == STAMP


def test_consumption_links_refresh_when_a_referenced_run_leaves_the_shown_window(scene):
    catalog, config, folder = scene
    catalog.pipelines["GAMMA"] = PipelineAsset(
        PipelineRow(3, "GAMMA", "GAMMA", None, None, None, "FULL", None)
    )
    for code, run_id in (("ALPHA", 1), ("BETA", 2)):
        pipeline = catalog.pipelines[code]
        pipeline.runs = [RunSummary(run_id, "SUCCESS", None, False, AT, AT, None)]
        pipeline.row = replace(
            pipeline.row, last_run=LastRun("SUCCESS", AT, AT), last_run_id=run_id
        )
    used = Consumption("ALPHA", 1, None, "BETA", 2, None)
    catalog.built_from[1], catalog.consumed_by[2] = [used], [used]
    write_site(catalog, config, folder)
    assert "../runs/2.html" in (folder / "runs/1.html").read_text()
    for page in folder.rglob("*.html"):
        os.utime(page, ns=(STAMP, STAMP))
    catalog.pipelines["BETA"].runs = [RunSummary(3, "SUCCESS", None, False, AT, AT, None)]
    write_site(catalog, config, folder)
    assert "../runs/2.html" not in (folder / "runs/1.html").read_text()
    assert "BETA run 2" in (folder / "runs/1.html").read_text()
    assert not (folder / "runs/2.html").exists()
    assert (folder / "pipelines/GAMMA.html").stat().st_mtime_ns == STAMP


def test_removed_assets_are_not_copied_into_the_new_snapshot(scene):
    catalog, config, folder = scene
    del catalog.pipelines["BETA"], catalog.tasks["BETA.load"], catalog.tables["data.beta"]
    catalog.column_edges = [edge for edge in catalog.column_edges if edge.task != "BETA.load"]
    write_site(catalog, config, folder)
    assert not (folder / "pipelines/BETA.html").exists()
    assert not (folder / "tasks/BETA.load.html").exists()
    assert not (folder / "tables/data.beta.html").exists()
    assert (folder / "pipelines/ALPHA.html").stat().st_mtime_ns == STAMP


@pytest.mark.parametrize(
    "invalid",
    (
        b"{broken",
        b"\xff",
        b'{"version": 9, "pages": {}}',
        b'{"version": true, "pages": {}}',
        b'{"version": 1, "pages": {"x": {"version": 1}}}',
    ),
)
def test_corrupt_manifest_fails_with_remedy_and_keeps_the_published_site(scene, invalid):
    catalog, config, folder = scene
    first = (folder / "index.html").read_bytes()
    (folder / MANIFEST).write_bytes(invalid)
    with pytest.raises(ConfigurationError, match=r"manifest.*remove only.*regenerate"):
        write_site(catalog, config, folder)
    assert (folder / "index.html").read_bytes() == first


def test_changed_cached_html_is_refused_and_removing_manifest_allows_regeneration(scene):
    catalog, config, folder = scene
    (folder / "tasks/BETA.load.html").write_text("changed outside the generator")
    with pytest.raises(ConfigurationError, match=r"BETA\.load\.html.*cached HTML.*remove only"):
        write_site(catalog, config, folder)
    (folder / MANIFEST).unlink()
    write_site(catalog, config, folder)
    assert "changed outside the generator" not in (folder / "tasks/BETA.load.html").read_text()
    assert json.loads((folder / MANIFEST).read_text())["version"] == 1


def test_cache_never_copies_pages_through_an_outside_directory_symlink(scene, tmp_path):
    catalog, config, folder = scene
    outside = tmp_path / "outside"
    (folder / "tables").rename(outside)
    (folder / "tables").symlink_to(outside, target_is_directory=True)
    with pytest.raises(ConfigurationError, match=r"data\.alpha\.html.*cached HTML.*regenerate"):
        write_site(catalog, config, folder)


def test_unreadable_cached_page_names_the_file_and_remedy(scene, monkeypatch):
    catalog, config, folder = scene
    original = Path.read_bytes

    def read(path):
        if path == folder / "tasks/BETA.load.html":
            raise PermissionError("read denied")
        return original(path)

    monkeypatch.setattr(Path, "read_bytes", read)
    with pytest.raises(ConfigurationError, match=r"BETA\.load\.html.*grant.*read access"):
        write_site(catalog, config, folder)


def test_comparison_age_change_rebuilds_coverage_without_changing_the_observation(
    scene, monkeypatch
):
    from etl_craft.services import catalog_gaps

    catalog, config, folder = scene

    class Clock(datetime):
        current = AT + timedelta(hours=1)

        @classmethod
        def now(cls, tz=None):
            return cls.current

    monkeypatch.setattr(catalog_gaps, "datetime", Clock)
    catalog.compared_at = AT
    table = catalog.tables["data.beta"]
    table.warehouse_checked = table.in_warehouse = True
    write_site(catalog, config, folder)
    page = folder / "tables/data.beta.html"
    assert "Comparison from" not in page.read_text()
    for table_page in (folder / "tables").glob("*.html"):
        os.utime(table_page, ns=(STAMP, STAMP))
    Clock.current = AT + timedelta(hours=27)
    write_site(catalog, config, folder)
    assert "Comparison from" in page.read_text()
    assert "is stale" in page.read_text()
    assert page.stat().st_mtime_ns != STAMP
    assert (folder / "tables/data.alpha.html").stat().st_mtime_ns == STAMP


def test_empty_sql_filename_keeps_its_read_error_visible_and_does_not_break_regeneration(scene):
    catalog, config, folder = scene
    catalog.tasks["BETA.load"].params["SOURCE_SQL_FILE"] = ""
    write_site(catalog, config, folder)
    page = (folder / "tasks/BETA.load.html").read_text()
    assert "The file cannot be read" in page
    assert (folder / "pipelines/ALPHA.html").stat().st_mtime_ns == STAMP
