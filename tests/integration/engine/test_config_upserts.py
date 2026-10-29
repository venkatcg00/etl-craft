"""Partial config changes preserve stored metadata and publish only explicit intent."""

import csv
from pathlib import Path

import pytest
from sqlalchemy import text

from etl_craft.core.actor import Actor, ActorKind, acting_as
from etl_craft.core.enums import ActionName, ApiRole
from etl_craft.core.errors import ExitCode, MetadataFileError
from etl_craft.dialects.engine import for_engine
from etl_craft.engine.repository.config_rows import TABLES
from etl_craft.services.operations import OperationContext
from fixtures.actions import act
from fixtures.metadata import add_pipeline

DEPLOYER = Actor("deployer", ActorKind.USER)


@pytest.fixture
def populated(metadata_project):
    metadata_project.load_examples()
    return metadata_project


def write(folder: Path, name: str, header: str, rows: list[tuple]) -> None:
    folder.mkdir(exist_ok=True)
    with (folder / name).open("w", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(header.split(","))
        writer.writerows(rows)


def apply(project, folder, *, plan=False):
    ctx = OperationContext(project.engine, project.config, DEPLOYER, role=ApiRole.ADMIN)
    return act(ctx, ActionName.CONFIG_PLAN if plan else ActionName.CONFIG_APPLY, config_dir=folder)


def snapshot(project):
    with project.engine.connect() as conn:
        return {
            table.file: for_engine(conn).table_rows(conn, table.table, order_by=table.id_column)
            for table in TABLES
        }


def test_partial_files_preserve_all_metadata(populated, tmp_path):
    project = populated
    before = snapshot(project)
    folder = tmp_path / "patch"
    write(
        folder,
        "pipelines.csv",
        "PIPELINE_CODE,PIPELINE_NAME,REFRESH_TYPE",
        [("NEW", "New pipeline", "FULL")],
    )
    plan = apply(project, folder, plan=True)
    assert not plan.failed and [(c.operation, c.row) for c in plan.changes] == [("insert", "NEW")]
    assert snapshot(project) == before
    done = apply(project, folder)
    assert done.applied and done.changes == plan.changes
    after = snapshot(project)
    assert [r for r in after["pipelines.csv"] if r["pipeline_code"] != "NEW"] == before[
        "pipelines.csv"
    ]
    for name, rows in before.items():
        if name != "pipelines.csv":
            assert after[name] == rows
    assert apply(project, folder).changes == ()


def test_header_only_and_missing_files_retire_nothing(populated, tmp_path):
    folder = tmp_path / "patch"
    write(folder, "pipelines.csv", "PIPELINE_CODE", [])
    before = snapshot(populated)
    assert apply(populated, folder).changes == ()
    assert snapshot(populated) == before
    (folder / "pipelines.csv").unlink()
    write(folder, "task_parameters.csv", "PIPELINE_CODE,TASK_CODE,PARAMETER_NAME", [])
    assert apply(populated, folder).changes == ()
    assert snapshot(populated) == before


def test_explicit_flags_preserve_identity_and_omitted_values(populated, tmp_path):
    folder = tmp_path / "patch"
    before = next(
        r for r in snapshot(populated)["pipelines.csv"] if r["pipeline_code"] == "SALES_MART"
    )
    write(folder, "pipelines.csv", "PIPELINE_CODE,PRIORITY", [("SALES_MART", 7)])
    assert apply(populated, folder).applied
    updated = next(
        r for r in snapshot(populated)["pipelines.csv"] if r["pipeline_code"] == "SALES_MART"
    )
    for field in (
        "pipeline_id",
        "pipeline_name",
        "refresh_type",
        "max_parallel_tasks",
        "description",
        "catchup",
    ):
        assert updated[field] == before[field]
    assert updated["priority"] == 7
    for flag in ("N", "Y"):
        write(folder, "pipelines.csv", "PIPELINE_CODE,ACTIVE_FLAG", [("SALES_MART", flag)])
        assert apply(populated, folder).applied
        row = next(
            r for r in snapshot(populated)["pipelines.csv"] if r["pipeline_code"] == "SALES_MART"
        )
        assert row["pipeline_id"] == before["pipeline_id"] and row["active_flag"] == flag
        assert row["priority"] == 7


def test_task_can_reference_pipeline_only_in_engine_db(populated, tmp_path):
    project = populated
    with acting_as(DEPLOYER), project.engine.begin() as conn:
        add_pipeline(conn, "STORED")
    script = project.root / "python_scripts" / "patch.py"
    script.write_text("def run(task):\n    pass\n")
    folder = tmp_path / "patch"
    write(
        folder,
        "tasks.csv",
        "PIPELINE_CODE,TASK_CODE,TASK_TYPE,HANDLER",
        [("STORED", "load", "ETL", "PYTHON")],
    )
    write(
        folder,
        "task_parameters.csv",
        "PIPELINE_CODE,TASK_CODE,PARAMETER_NAME,PARAMETER_VALUE",
        [
            ("STORED", "load", "SCRIPT_FILE", "python_scripts/patch.py"),
            ("STORED", "load", "SCRIPT_KIND", "INGESTION"),
        ],
    )
    assert apply(project, folder).applied
    assert apply(project, folder).changes == ()


@pytest.mark.parametrize("retired", [False, True])
def test_missing_or_retired_references_name_file_line_and_key(populated, tmp_path, retired):
    project = populated
    if retired:
        with acting_as(DEPLOYER), project.engine.begin() as conn:
            add_pipeline(conn, "MISSING")
            conn.execute(
                text("UPDATE CFG_PIPELINES SET ACTIVE_FLAG='N' WHERE PIPELINE_CODE='MISSING'")
            )
    folder = tmp_path / "patch"
    write(
        folder,
        "tasks.csv",
        "PIPELINE_CODE,TASK_CODE,TASK_TYPE,HANDLER",
        [("MISSING", "load", "ETL", "PYTHON")],
    )
    before = snapshot(project)
    with pytest.raises(MetadataFileError) as caught:
        apply(project, folder)
    for part in ("tasks.csv", "line 2", "MISSING", "active reference", "ACTIVE_FLAG=Y"):
        assert part in str(caught.value)
    assert snapshot(project) == before


def test_new_inactive_key_fails_without_writes(populated, tmp_path):
    folder = tmp_path / "patch"
    write(folder, "pipelines.csv", "PIPELINE_CODE,ACTIVE_FLAG", [("NEW", "N")])
    before = snapshot(populated)
    with pytest.raises(MetadataFileError) as caught:
        apply(populated, folder)
    for part in (
        "pipelines.csv",
        "line 2",
        "NEW",
        "ACTIVE_FLAG=N",
        "existing row",
        "ACTIVE_FLAG=Y",
    ):
        assert part in str(caught.value)
    assert snapshot(populated) == before
    assert caught.value.exit_code == ExitCode.METADATA_FILE
    with populated.engine.connect() as conn:
        actors = (
            conn.execute(
                text("SELECT ACTOR AS actor FROM AUD_ACTIONS WHERE COMMAND=:command"),
                {"command": ActionName.CONFIG_APPLY},
            )
            .scalars()
            .all()
        )
    assert actors == [DEPLOYER.name]


def test_unknown_csv_names_the_suggestion(populated, tmp_path):
    folder = tmp_path / "patch"
    write(folder, "pipline.csv", "PIPELINE_CODE", [])
    with pytest.raises(MetadataFileError) as caught:
        apply(populated, folder)
    for part in ("pipline.csv", "unknown config CSV", "pipelines.csv"):
        assert part in str(caught.value)


def test_empty_folder_names_nothing_to_apply(populated, tmp_path):
    folder = tmp_path / "empty"
    folder.mkdir()
    with pytest.raises(MetadataFileError) as caught:
        apply(populated, folder)
    assert str(folder) in str(caught.value) and "nothing to apply" in str(caught.value)


def test_duplicate_keys_name_both_lines_and_write_nothing(populated, tmp_path):
    folder = tmp_path / "patch"
    write(
        folder, "pipelines.csv", "PIPELINE_CODE,PRIORITY", [("SALES_DAILY", 2), ("SALES_DAILY", 3)]
    )
    before = snapshot(populated)
    with pytest.raises(MetadataFileError) as caught:
        apply(populated, folder)
    for part in ("pipelines.csv", "lines 2 and 3", "SALES_DAILY"):
        assert part in str(caught.value)
    assert snapshot(populated) == before


def test_partial_column_positions_and_parent_retirement(populated, tmp_path):
    from fixtures.metadata import add_source_column

    project = populated
    with acting_as(DEPLOYER), project.engine.begin() as conn:
        add_source_column(conn, "patch.orders", "id", "INT", 1)
        add_source_column(conn, "patch.orders", "name", "TEXT", 2)
    folder = tmp_path / "patch"
    header = "SCHEMA_NAME,TABLE_NAME,COLUMN_NAME,COLUMN_POSITION"
    write(folder, "columns.csv", header, [("patch", "orders", "id", 2)])
    before = snapshot(project)
    with pytest.raises(MetadataFileError) as caught:
        apply(project, folder)
    for part in ("columns.csv line 2", "COLUMN_POSITION=2", "name", "explicitly move"):
        assert part in str(caught.value)
    assert snapshot(project) == before
    write(
        folder,
        "columns.csv",
        header,
        [("patch", "orders", "id", 2), ("patch", "orders", "name", 1)],
    )
    assert apply(project, folder).applied
    table_id = next(r["table_id"] for r in before["tables.csv"] if r["schema_name"] == "patch")
    rows = [r for r in snapshot(project)["columns.csv"] if r["table_id"] == table_id]
    assert {r["column_name"]: r["column_position"] for r in rows} == {"id": 2, "name": 1}
    assert {r["column_id"] for r in rows} == {
        r["column_id"] for r in before["columns.csv"] if r["table_id"] == table_id
    }
    (folder / "columns.csv").unlink()
    write(folder, "tables.csv", "SCHEMA_NAME,TABLE_NAME,ACTIVE_FLAG", [("patch", "orders", "N")])
    before_retire = snapshot(project)
    with pytest.raises(MetadataFileError) as caught:
        apply(project, folder)
    for part in ("tables.csv line 2", "active column", "columns retired before", "ACTIVE_FLAG=N"):
        assert part in str(caught.value)
    assert snapshot(project) == before_retire
    write(
        folder,
        "columns.csv",
        "SCHEMA_NAME,TABLE_NAME,COLUMN_NAME,ACTIVE_FLAG",
        [("patch", "orders", "id", "N"), ("patch", "orders", "name", "N")],
    )
    assert apply(project, folder).applied
    assert apply(project, folder).changes == ()


def test_missing_column_parent_is_refused(populated, tmp_path):
    folder = tmp_path / "patch"
    write(
        folder,
        "columns.csv",
        "SCHEMA_NAME,TABLE_NAME,COLUMN_NAME,DATA_TYPE,COLUMN_POSITION",
        [("patch", "missing", "id", "INT", 1)],
    )
    with pytest.raises(MetadataFileError) as caught:
        apply(populated, folder)
    for part in ("columns.csv line 2", "patch.missing", "active reference", "ACTIVE_FLAG=Y"):
        assert part in str(caught.value)


def test_omitted_active_flag_and_present_empty_cell_keep_their_meanings(populated, tmp_path):
    folder = tmp_path / "patch"
    write(folder, "pipelines.csv", "PIPELINE_CODE,ACTIVE_FLAG", [("SALES_MART", "N")])
    assert apply(populated, folder).applied
    write(folder, "pipelines.csv", "PIPELINE_CODE", [("SALES_MART",)])
    assert apply(populated, folder).changes == ()
    write(folder, "pipelines.csv", "PIPELINE_CODE,DESCRIPTION", [("SALES_DAILY", "")])
    assert apply(populated, folder).applied
    row = next(
        r for r in snapshot(populated)["pipelines.csv"] if r["pipeline_code"] == "SALES_DAILY"
    )
    assert row["description"] is None


def test_new_key_requires_insert_values(populated, tmp_path):
    folder = tmp_path / "patch"
    write(folder, "pipelines.csv", "PIPELINE_CODE", [("NEW",)])
    before = snapshot(populated)
    with pytest.raises(MetadataFileError) as caught:
        apply(populated, folder)
    for part in ("pipelines.csv line 2", "NEW", "PIPELINE_NAME", "supply valid values"):
        assert part in str(caught.value)
    assert snapshot(populated) == before


def test_partial_folder_never_changes_another_project(populated, tmp_path):
    project = populated
    with acting_as(DEPLOYER), project.engine.begin() as conn:
        other = conn.execute(
            text(
                "INSERT INTO CFG_PROJECTS (PROJECT_CODE,PROJECT_NAME,DEFAULT_POOL) "
                "VALUES ('other','Other','default') RETURNING PROJECT_ID AS project_id"
            )
        ).scalar_one()
        conn.execute(
            text(
                "INSERT INTO CFG_PIPELINES (PROJECT_ID,PIPELINE_CODE,PIPELINE_NAME,REFRESH_TYPE) "
                "VALUES (:project,'SALES_DAILY','Other sales','FULL')"
            ),
            {"project": other},
        )
    before = snapshot(project)
    folder = tmp_path / "patch"
    write(folder, "pipelines.csv", "PIPELINE_CODE,PRIORITY", [("SALES_DAILY", 17)])
    assert apply(project, folder).applied
    after = snapshot(project)
    assert [r for r in after["pipelines.csv"] if r["project_id"] == other] == [
        r for r in before["pipelines.csv"] if r["project_id"] == other
    ]
    for name in before:
        if name != "pipelines.csv":
            assert after[name] == before[name]


def test_table_identity_resolution_never_updates_absent_tables(populated, tmp_path, monkeypatch):
    from fixtures.metadata import add_source_column

    project = populated
    with acting_as(DEPLOYER), project.engine.begin() as conn:
        add_source_column(conn, "patch.one", "id", "INT", 1)
        add_source_column(conn, "patch.two", "id", "INT", 1)
    monkeypatch.setattr(
        "etl_craft.services.lineage.dialect_and_catalog", lambda config: ("duckdb", "analytics")
    )
    folder = tmp_path / "patch"
    write(folder, "tables.csv", "SCHEMA_NAME,TABLE_NAME", [("patch", "one")])
    before = snapshot(project)
    assert apply(project, folder).changes == ()
    assert snapshot(project) == before
    write(
        folder, "tables.csv", "CATALOG_NAME,SCHEMA_NAME,TABLE_NAME", [("analytics", "patch", "one")]
    )
    done = apply(project, folder)
    assert done.applied and len(done.changes) == 1
    after = snapshot(project)
    assert [r for r in after["tables.csv"] if r["table_name"] != "one"] == [
        r for r in before["tables.csv"] if r["table_name"] != "one"
    ]
    one = next(
        r for r in after["tables.csv"] if r["schema_name"] == "patch" and r["table_name"] == "one"
    )
    assert one["catalog_name"] == "analytics"
    assert apply(project, folder).changes == ()
