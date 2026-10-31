"""Complete stewardship runs use the same reader, scripts and SQL actions on every warehouse."""

from dataclasses import replace

import pytest
import yaml
from sqlalchemy import text

from etl_craft.config import load_config
from etl_craft.core.actor import SYSTEM_ACTOR, Actor, acting_as
from etl_craft.core.enums import ActionName, ActorKind, ApiRole, RunStatus
from etl_craft.engine import transitions
from etl_craft.engine.runlog import RunSelector
from etl_craft.execution.pipeline import run_pipeline
from etl_craft.services.operations import OperationContext
from etl_craft.services.operations.uploads import UploadRequest, receive
from fixtures.actions import act
from fixtures.engine_db import profile_block
from fixtures.metadata import add_dependency, add_task
from fixtures.sql_warehouse import warehouses

VALIDATOR = """
from etl_craft.scripting import PermanentError

def validate(task):
    rows = task.reader.rows()
    header = next(rows, None)
    if header != ("id", "name"):
        raise PermanentError(
            f"row 1 header={header!r}; expected id|name with a pipe separator; "
            "upload a corrected file"
        )
    seen = set()
    for number, row in enumerate(rows, 2):
        if len(row) != 2 or not row[0].isdigit() or int(row[0]) in seen:
            raise PermanentError(
                f"row {number} value={row!r}; expected a unique integer id and name; "
                "upload a corrected file"
            )
        seen.add(int(row[0]))
"""


@pytest.fixture
def upload_world(sql_world, request):
    w = sql_world
    root = w.config.project_dir
    (root / "validation_scripts").mkdir()
    (root / "python_scripts").mkdir()
    (root / "validation_scripts/check.py").write_text(VALIDATOR)
    source, target = w.name("landed"), w.name("uploaded")
    w.execute(
        f"CREATE TABLE {source} (id INTEGER,name VARCHAR,UPLOAD_ID BIGINT,"
        "CREATED_BY VARCHAR,UPDATED_BY VARCHAR)"
    )
    kind = getattr(request, "param", "APPEND_TABLE")
    if kind == "APPEND_TABLE":
        w.execute(
            f"CREATE TABLE {target} (id INTEGER,name VARCHAR,UPLOAD_ID BIGINT,"
            "CREATED_BY VARCHAR,UPDATED_BY VARCHAR,PIPELINE_RUN_ID BIGINT,PIPELINE_ID BIGINT,"
            "TASK_RUN_ID BIGINT,CREATE_DATE TIMESTAMP WITH TIME ZONE,ROW_ID BIGINT)"
        )
    else:
        w.setup("uploaded", f"SELECT id,name,UPLOAD_ID FROM {source}", kind)
        with w.engine_db.begin() as conn:
            conn.execute(text("UPDATE CFG_TASKS SET ACTIVE_FLAG='N'"))
    loader = """
import csv
from sqlalchemy import text
from etl_craft.scripting import ScriptResult

def run(task):
    written = 0
    with task.warehouse() as engine, engine.begin() as conn:
        for upload in task.uploads:
            conn.execute(
                text("DELETE FROM SOURCE_TABLE WHERE UPLOAD_ID=:upload"),
                {"upload": upload.upload_id},
            )
            with upload.path.open(newline="", encoding="utf-8") as stream:
                rows = csv.reader(stream, delimiter="|")
                next(rows)
                for row in rows:
                    conn.execute(text(
                        "INSERT INTO SOURCE_TABLE (id,name,UPLOAD_ID,CREATED_BY,UPDATED_BY) "
                        "VALUES (:id,:name,:upload,:account,:account)"
                    ), {
                        "id": int(row[0]), "name": row[1],
                        "upload": upload.upload_id, "account": upload.account,
                    })
                    written += 1
    return ScriptResult(row_count=written)
""".replace("SOURCE_TABLE", source)
    (root / "python_scripts/load.py").write_text(loader)
    config_file = {
        "Secrets": {"Source_type": "ENVIRONMENT"},
        "Orchestration": {"Project": "default", "Task_timeout_seconds": 60},
        "Engine": {"dev": profile_block(w.config.engine, "main")},
        "Warehouse": {
            "Name": {
                "duckdb": "DuckDB",
                "duckdb_iceberg": "DuckDB",
                "postgres": "Postgres",
                "trino_iceberg": "Trino",
            }[w.kind],
            "Table_format": str(w.config.warehouse_table_format),
            "dev": profile_block(w.config.warehouse, w.schema),
        },
    }
    w.config.config_path.write_text(yaml.safe_dump(config_file, sort_keys=False))
    w.config = load_config(w.config.config_path)
    owner = OperationContext(
        w.engine_db, w.config, SYSTEM_ACTOR, role=ApiRole.ADMIN, project="default", project_id=1
    )
    for name in ("alice", "bob"):
        act(owner, ActionName.ACCOUNT_ADD, name=name, role=ApiRole.ADMIN, oidc_subject=name)
    with w.engine_db.begin() as conn:
        transitions.finalize_pipeline_run(conn, w.pipeline_run_id, RunStatus.SUCCESS)
        validation = add_task(
            conn,
            w.pipeline_id,
            "validate_file",
            "FILE_VALIDATION",
            SCRIPT_FILE="validation_scripts/check.py",
            FILE_FORMAT="CSV",
        )
        load = add_task(
            conn,
            w.pipeline_id,
            "land",
            "PYTHON",
            SCRIPT_FILE="python_scripts/load.py",
            SCRIPT_KIND="INGESTION",
        )
        append = add_task(
            conn,
            w.pipeline_id,
            "apply",
            "SQL",
            SQL_ACTION=kind,
            **(
                {
                    "MERGE_KEY": "id",
                    "MERGE_COMPARE_COLUMNS": "name",
                    "MERGE_DEDUPE_ORDER": "UPLOAD_ID ASC",
                }
                if kind != "APPEND_TABLE"
                else {}
            ),
            TARGET_OBJECT=f"{w.schema}.uploaded",
            SELECT_QUERY=(
                f"SELECT id,name,UPLOAD_ID FROM {source} WHERE UPLOAD_ID IN ($$upload_ids)"
            ),
        )
        add_dependency(conn, w.pipeline_id, load, validation)
        add_dependency(conn, w.pipeline_id, append, load)
        conn.execute(
            text("UPDATE CFG_PIPELINES SET STEWARDABLE_FLAG='Y' WHERE PIPELINE_ID=:id"),
            {"id": w.pipeline_id},
        )
    return w, owner


def submit(owner, account, content):
    caller = replace(owner, actor=Actor(account, ActorKind.USER))
    with receive(caller, UploadRequest(pipeline_code="P")) as receipt:
        receipt.begin_file("corrections.csv", "text/csv")
        receipt.write(content)
    return receipt.result


def drive_upload(w, run_id):
    w.warehouse.dispose()
    with acting_as(SYSTEM_ACTOR):
        return run_pipeline(w.engine_db, w.config, "P", selector=RunSelector(run_id=run_id))


def test_complete_upload_flow_keeps_each_stewards_account(upload_world):
    from etl_craft.controller.uploads import admit_pending

    w, owner = upload_world
    first = submit(owner, "alice", b"id|name\n0|warmup\n")
    alice = submit(owner, "alice", b"id|name\n1|first\n")
    bob = submit(owner, "bob", b"id|name\n2|second\n")
    assert alice["waiting_behind_run_id"] == first["pipeline_run_id"]
    assert bob["waiting_behind_run_id"] == first["pipeline_run_id"]
    warmup = drive_upload(w, first["pipeline_run_id"])
    assert warmup.status == RunStatus.SUCCESS, warmup.message
    admit_pending(owner)
    with w.engine_db.connect() as conn:
        bound = (
            conn.execute(
                text(
                    "SELECT UPLOAD_ID AS upload_id,PIPELINE_RUN_ID AS run_id "
                    "FROM AUD_UPLOADS WHERE UPLOAD_ID IN (:a,:b) ORDER BY UPLOAD_ID"
                ),
                {"a": alice["upload_id"], "b": bob["upload_id"]},
            )
            .mappings()
            .all()
        )
    assert bound[0]["run_id"] == bound[1]["run_id"]
    assert bound[0]["run_id"] != first["pipeline_run_id"]
    result = drive_upload(w, bound[0]["run_id"])
    assert result.status == RunStatus.SUCCESS, result.message
    with w.warehouse.connect() as conn:
        rows = (
            conn.execute(
                text(
                    f"SELECT id AS id,name AS name,CREATED_BY AS creator,"
                    f"UPDATED_BY AS writer FROM {w.name('uploaded')} ORDER BY id"
                )
            )
            .mappings()
            .all()
        )
    with w.warehouse.connect() as conn:
        landed = (
            conn.execute(
                text(
                    f"SELECT id AS id,name AS name,CREATED_BY AS creator,UPDATED_BY AS writer "
                    f"FROM {w.name('landed')} ORDER BY id"
                )
            )
            .mappings()
            .all()
        )
    assert [dict(row) for row in landed] == [dict(row) for row in rows]
    assert [dict(row) for row in rows] == [
        {"id": 0, "name": "warmup", "creator": "alice", "writer": "alice"},
        {"id": 1, "name": "first", "creator": "alice", "writer": "alice"},
        {"id": 2, "name": "second", "creator": "bob", "writer": "bob"},
    ]


def test_bad_stewardship_file_never_reaches_a_project_table(upload_world):
    w, owner = upload_world
    receipt = submit(owner, "alice", b"id,name\n1,bad separator\n")
    result = drive_upload(w, receipt["pipeline_run_id"])
    assert result.status == RunStatus.FAILED
    with w.engine_db.connect() as conn:
        row = (
            conn.execute(
                text(
                    "SELECT STATUS AS status,REASON AS reason FROM AUD_UPLOADS WHERE UPLOAD_ID=:id"
                ),
                {"id": receipt["upload_id"]},
            )
            .mappings()
            .one()
        )
    assert row["status"] == "REFUSED"
    for expected in ("row 1", "header=", "expected id|name", "corrected file"):
        assert expected in row["reason"]
    with w.warehouse.connect() as conn:
        assert (
            conn.execute(text(f"SELECT COUNT(*) AS rows FROM {w.name('uploaded')}"))
            .mappings()
            .one()["rows"]
            == 0
        )


@warehouses("duckdb", "postgres")
@pytest.mark.parametrize("upload_world", ["SCD1_MERGE", "SCD2_MERGE"], indirect=True)
def test_complete_protected_upload_flow_orders_files_and_locks_each_writer(upload_world):
    from etl_craft.controller.uploads import admit_pending

    w, owner = upload_world
    warmup = submit(owner, "alice", b"id|name\n0|warmup\n")
    alice = submit(owner, "alice", b"id|name\n1|first\n2|only alice\n")
    bob = submit(owner, "bob", b"id|name\n1|newest\n3|only bob\n")
    result = drive_upload(w, warmup["pipeline_run_id"])
    assert result.status == RunStatus.SUCCESS, result.message
    admit_pending(owner)
    with w.engine_db.connect() as conn:
        run_id = conn.execute(
            text("SELECT PIPELINE_RUN_ID AS run_id FROM AUD_UPLOADS WHERE UPLOAD_ID=:id"),
            {"id": alice["upload_id"]},
        ).scalar_one()
    result = drive_upload(w, run_id)
    assert result.status == RunStatus.SUCCESS, result.message
    with w.warehouse.connect() as conn:
        rows = (
            conn.execute(
                text(
                    f"SELECT id AS id,name AS name,CREATED_BY AS creator,UPDATED_BY AS writer "
                    f"FROM {w.name('uploaded')} WHERE ACTIVE_FLAG='Y' ORDER BY id"
                )
            )
            .mappings()
            .all()
        )
    assert all(r["creator"] == r["writer"] for r in rows)
    assert [(r["id"], r["name"], r["writer"]) for r in rows] == [
        (0, "warmup", "alice"),
        (1, "newest", "bob"),
        (2, "only alice", "alice"),
        (3, "only bob", "bob"),
    ]
    for key, writer in [(1, "bob"), (2, "alice"), (3, "bob")]:
        evidence = act(
            owner, ActionName.DATA_PROVENANCE, pipeline_code="P", task_code="apply", key={"id": key}
        )
        assert any(row["locked"] and row["writer"] == writer for row in evidence.records)
    with w.engine_db.connect() as conn:
        accounts = (
            conn.execute(
                text(
                    "SELECT ACCOUNT AS account FROM AUD_UPLOADS WHERE UPLOAD_ID IN (:a,:b) "
                    "ORDER BY UPLOAD_ID"
                ),
                {"a": alice["upload_id"], "b": bob["upload_id"]},
            )
            .scalars()
            .all()
        )
    assert accounts == ["alice", "bob"]


@warehouses("duckdb_iceberg", "trino_iceberg")
@pytest.mark.parametrize("upload_world", ["SCD1_MERGE", "SCD2_MERGE"], indirect=True)
def test_validate_and_refresh_refuse_unsupported_protected_upload_targets(upload_world):
    from etl_craft.services.validate import validate

    w, owner = upload_world
    checked = validate(w.engine_db, w.config, "P")
    messages = " ".join(f"{f.where}: {f.message}" for f in checked.findings)
    for expected in ["P.apply", "uploaded", "unsupported", "non-protected target"]:
        assert expected in messages
    refreshed = act(owner, ActionName.PIPELINE_REFRESH, pipeline_code="P")
    assert refreshed.failed
    messages = " ".join(str(row) for row in refreshed.refreshes)
    for expected in ["P.apply", "uploaded", "unsupported", "non-protected target"]:
        assert expected in messages


def test_one_refused_file_does_not_prevent_another_file_loading(upload_world):
    from etl_craft.controller.uploads import admit_pending

    w, owner = upload_world
    warmup = submit(owner, "alice", b"id|name\n0|warmup\n")
    good = submit(owner, "bob", b"id|name\n1|accepted\n")
    bad = submit(owner, "alice", b"id,name\n2,refused\n")
    result = drive_upload(w, warmup["pipeline_run_id"])
    assert result.status == RunStatus.SUCCESS, result.message
    admit_pending(owner)
    with w.engine_db.connect() as conn:
        run_id = conn.execute(
            text("SELECT PIPELINE_RUN_ID AS run_id FROM AUD_UPLOADS WHERE UPLOAD_ID=:id"),
            {"id": good["upload_id"]},
        ).scalar_one()
    result = drive_upload(w, run_id)
    assert result.status == RunStatus.SUCCESS, result.message
    with w.engine_db.connect() as conn:
        records = (
            conn.execute(
                text(
                    "SELECT UPLOAD_ID AS id,STATUS AS status FROM AUD_UPLOADS "
                    "WHERE UPLOAD_ID IN (:good,:bad)"
                ),
                {"good": good["upload_id"], "bad": bad["upload_id"]},
            )
            .mappings()
            .all()
        )
    assert {r["id"]: r["status"] for r in records} == {
        good["upload_id"]: "LOADED",
        bad["upload_id"]: "REFUSED",
    }
    with w.warehouse.connect() as conn:
        rows = (
            conn.execute(
                text(
                    f"SELECT id AS id,CREATED_BY AS creator,UPDATED_BY AS writer "
                    f"FROM {w.name('uploaded')} ORDER BY id"
                )
            )
            .mappings()
            .all()
        )
    assert [(r["id"], r["creator"], r["writer"]) for r in rows] == [
        (0, "alice", "alice"),
        (1, "bob", "bob"),
    ]
