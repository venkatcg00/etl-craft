"""Multipart receipts, permissions and hostile filenames use the shared operation."""

from dataclasses import replace

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from etl_craft.api.app import create_app
from etl_craft.core.actor import SYSTEM_ACTOR
from etl_craft.core.enums import ActionName, ApiRole
from etl_craft.services.operations import OperationContext
from etl_craft.services.operations.actions import invoke
from etl_craft.services.operations.tokens import TokenCreate
from fixtures.metadata import add_pipeline, add_task


@pytest.fixture
def upload_api(engine_db, tmp_path):
    config = replace(engine_db.config, config_path=tmp_path / "craft-connector.yml")
    folder = config.project_dir / "validation_scripts"
    folder.mkdir()
    (folder / "check.py").write_text("def validate(task):\n    list(task.reader.rows())\n")
    ctx = OperationContext(engine_db.engine, config, SYSTEM_ACTOR, role=ApiRole.ADMIN)
    writer = invoke(ctx, ActionName.TOKEN_CREATE, TokenCreate(name="writer", role=ApiRole.ADMIN))
    viewer = invoke(ctx, ActionName.TOKEN_CREATE, TokenCreate(name="viewer", role=ApiRole.VIEWER))
    with ctx.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        add_task(
            conn,
            pipeline,
            "check",
            "FILE_VALIDATION",
            SCRIPT_FILE="validation_scripts/check.py",
            FILE_FORMAT="CSV",
        )
        conn.execute(text("UPDATE CFG_PIPELINES SET STEWARDABLE_FLAG='Y'"))
    with TestClient(create_app(ctx)) as client:
        yield ctx, client, writer, viewer


def authorization(token):
    return {"Authorization": f"Bearer {token.token}"}


def test_multipart_upload_and_history_share_the_receipt(upload_api):
    ctx, client, writer, _ = upload_api
    response = client.post(
        "/api/v1/uploads?pipeline_code=P",
        headers=authorization(writer),
        files={"file": ("data.csv", b"id|name\n1|one\n", "text/csv")},
    )
    assert response.status_code == 200, response.text
    receipt = response.json()
    assert receipt["pipeline_run_id"] is not None
    assert receipt["waiting_behind_run_id"] is None
    history = client.get("/api/v1/uploads?pipeline_code=P", headers=authorization(writer))
    assert history.status_code == 200, history.text
    assert str(receipt["upload_id"]) in history.text
    eligible = client.get("/api/v1/uploads/pipelines", headers=authorization(writer))
    assert eligible.status_code == 200 and "P" in eligible.text
    with ctx.engine.connect() as conn:
        row = conn.execute(
            text("SELECT ACCOUNT AS account,SIZE_BYTES AS size FROM AUD_UPLOADS")
        ).one()
    assert row.account == "writer" and row.size == len(b"id|name\n1|one\n")


@pytest.mark.parametrize(
    "filename",
    [
        "../secret.csv",
        "/tmp/secret.csv",
        r"..\secret.csv",
        "%2fsecret.csv",
        "data.xls",
        "data.xlsm",
    ],
)
def test_hostile_upload_filename_is_refused_and_audited(upload_api, filename):
    ctx, client, writer, _ = upload_api
    response = client.post(
        "/api/v1/uploads?pipeline_code=P",
        headers=authorization(writer),
        files={"file": (filename, b"id|name\n1|one\n", "text/csv")},
    )
    assert response.status_code == 400, response.text
    with ctx.engine.connect() as conn:
        row = conn.execute(text("SELECT STATUS AS status,REASON AS reason FROM AUD_UPLOADS")).one()
        count = conn.execute(
            text("SELECT COUNT(*) AS count FROM AUD_PIPELINES_RUN_LOG")
        ).scalar_one()
    assert row.status == "REFUSED" and "expected" in row.reason and "corrected file" in row.reason
    assert count == 0


@pytest.mark.parametrize("count", [0, 2])
def test_upload_requires_exactly_one_file(upload_api, count):
    ctx, client, writer, _ = upload_api
    files = [("file", (f"{i}.csv", b"id|name\n", "text/csv")) for i in range(count)]
    content = b"--bound--\r\n" if count == 0 else None
    headers = authorization(writer)
    if count == 0:
        headers["Content-Type"] = "multipart/form-data; boundary=bound"
    response = client.post(
        "/api/v1/uploads?pipeline_code=P", headers=headers, files=files or None, content=content
    )
    assert response.status_code == 400, response.text
    with ctx.engine.connect() as conn:
        status = conn.execute(text("SELECT STATUS AS status FROM AUD_UPLOADS")).scalar_one()
    assert status == "REFUSED"


def test_unassigned_pipeline_upload_is_refused(upload_api):
    ctx, client, _, viewer = upload_api
    response = client.post(
        "/api/v1/uploads?pipeline_code=P",
        headers=authorization(viewer),
        files={"file": ("data.csv", b"id|name\n", "text/csv")},
    )
    assert response.status_code == 403, response.text
    with ctx.engine.connect() as conn:
        row = conn.execute(text("SELECT STATUS AS status,REASON AS reason FROM AUD_UPLOADS")).one()
    assert row.status == "REFUSED" and "P" in row.reason


def test_body_ceiling_is_checked_while_streaming(upload_api):
    from etl_craft.config.uploads import Uploads

    ctx, _, writer, _ = upload_api
    ctx = replace(ctx, config=replace(ctx.config, uploads=Uploads(max_file_mb=1)))
    with TestClient(create_app(ctx)) as client:
        response = client.post(
            "/api/v1/uploads?pipeline_code=P",
            headers=authorization(writer),
            files={"file": ("large.csv", b"x" * (1024 * 1024 + 1), "text/csv")},
        )
    assert response.status_code == 400, response.text
    assert "Max_file_mb" in response.text and "corrected file" in response.text


@pytest.mark.parametrize("operation", ["mark", "pause", "item", "standalone", "start", "rerun"])
@pytest.mark.parametrize("outcome", ["SUCCESS", "SKIPPED"])
def test_no_operation_can_bypass_file_validation(upload_api, operation, outcome):
    from etl_craft.core.enums import ControlAction, ItemAction, ItemStatus, RunStatus
    from etl_craft.core.errors import EtlCraftError
    from etl_craft.engine import transitions
    from fixtures.actions import act

    ctx, _, _, _ = upload_api
    with ctx.engine.begin() as conn:
        conn.execute(text("UPDATE CFG_PIPELINES SET STEWARDABLE_FLAG='N'"))
        pipeline = conn.execute(text("SELECT PIPELINE_ID AS id FROM CFG_PIPELINES")).scalar_one()
        legacy = transitions.create_run(conn, pipeline, SYSTEM_ACTOR)
        transitions.finalize_pipeline_run(conn, legacy, RunStatus.SUCCESS)
        conn.execute(text("UPDATE CFG_PIPELINES SET STEWARDABLE_FLAG='Y'"))
    given = {"pipeline_code": "P", "task_code": "check"}
    calls = {
        "mark": lambda: act(
            ctx,
            ActionName.MARK,
            **given,
            status=RunStatus(outcome),
            run_id=legacy,
            reason="test validation boundary",
        ),
        "pause": lambda: act(
            ctx,
            ActionName.CONTROL,
            **given,
            action=ControlAction.PAUSE,
            outcome=RunStatus(outcome),
            reason="test validation boundary",
        ),
        "item": lambda: act(
            ctx,
            ActionName.ITEM_CONTROL,
            **given,
            run_id=legacy,
            action=ItemAction.MARK,
            status=ItemStatus(outcome),
            item_key="one",
            reason="test validation boundary",
        ),
        "standalone": lambda: act(ctx, ActionName.RUN, **given, standalone=True),
        "start": lambda: act(ctx, ActionName.RUN, pipeline_code="P", init_only=True),
        "rerun": lambda: act(
            ctx,
            ActionName.RUN,
            **given,
            run_id=legacy,
            rerun=True,
            reason="test validation boundary",
        ),
    }
    with pytest.raises(EtlCraftError) as failure:
        calls[operation]()
    message = str(failure.value)
    assert "P" in message and "FILE_VALIDATION" in message and "corrected file" in message
    if operation in {"mark", "pause", "item", "standalone"}:
        assert "check" in message


def test_concurrent_uploads_and_restarted_admission_bind_each_file_once(upload_api):
    from etl_craft.controller import uploads as controller_uploads
    from etl_craft.core.actor import Actor
    from etl_craft.core.enums import ActorKind, RunStatus
    from etl_craft.engine import transitions
    from etl_craft.services.operations.uploads import UploadReceipt, UploadRequest, receive
    from fixtures.races import two_at_once

    ctx, _, _, _ = upload_api
    ctx = replace(ctx, actor=Actor("writer", ActorKind.SERVICE))

    def submit(data):
        with receive(ctx, UploadRequest(pipeline_code="P")) as receipt:
            receipt.begin_file("data.csv", "text/csv")
            receipt.write(data)
        return receipt.result

    first = submit(b"id|name\n0|first\n")
    results = two_at_once(
        lambda: submit(b"id|name\n1|alice\n"),
        lambda: submit(b"id|name\n2|bob\n"),
        at=(UploadReceipt, "finish"),
    )
    assert all(row["waiting_behind_run_id"] == first["pipeline_run_id"] for row in results)
    with ctx.engine.begin() as conn:
        transitions.finalize_pipeline_run(conn, first["pipeline_run_id"], RunStatus.SUCCESS)
    ctx.engine.dispose()
    two_at_once(
        lambda: controller_uploads.admit_pending(ctx),
        lambda: controller_uploads.admit_pending(ctx),
        at=(controller_uploads, "admit_pending"),
    )
    with ctx.engine.connect() as conn:
        rows = (
            conn.execute(
                text(
                    "SELECT UPLOAD_ID AS id,PIPELINE_RUN_ID AS run_id "
                    "FROM AUD_UPLOADS ORDER BY UPLOAD_ID"
                )
            )
            .mappings()
            .all()
        )
        runs = conn.execute(
            text("SELECT COUNT(*) AS count FROM AUD_PIPELINES_RUN_LOG")
        ).scalar_one()
    assert runs == 2
    assert len(rows) == 3 and len({row["id"] for row in rows}) == 3
    assert rows[1]["run_id"] == rows[2]["run_id"] != rows[0]["run_id"]


def test_remote_cli_streams_upload_and_reads_its_history(upload_api, monkeypatch, tmp_path, capsys):
    import json
    from urllib.parse import urlsplit

    from etl_craft.cli import main

    _, client, writer, _ = upload_api
    monkeypatch.setenv("ETL_CRAFT_TOKEN", writer.token)
    path = tmp_path / "corrections.csv"
    path.write_bytes(b"id|name\n1|cli\n")

    class Reply:
        def __init__(self, response):
            self.response = response

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def read(self, maximum):
            assert self.response.status_code == 200, self.response.text
            return self.response.content

    class Opener:
        def open(self, request, timeout):
            url = urlsplit(request.full_url)
            data = request.data
            if data is not None and not isinstance(data, bytes):
                data = b"".join(data)
            return Reply(
                client.request(
                    request.get_method(),
                    url.path + "?" + url.query,
                    content=data,
                    headers=dict(request.header_items()),
                )
            )

    monkeypatch.setattr("etl_craft.core.http_client.build_opener", lambda *args: Opener())
    args = ["--api", "http://127.0.0.1:8000", "--project", "default"]
    assert main([*args, "upload", "--pipeline-code", "P", str(path), "--json"]) == 0
    submitted = json.loads(capsys.readouterr().out)
    assert submitted["upload_id"] and submitted["schema"] == "etl-craft/upload/1"
    assert main([*args, "uploads", "--pipeline-code", "P", "--json"]) == 0
    history = json.loads(capsys.readouterr().out)
    assert history["uploads"][0]["upload_id"] == submitted["upload_id"]


def test_can_upload_grant_accepts_files_but_does_not_grant_run(upload_api):
    from fixtures.actions import act

    ctx, client, _, viewer = upload_api
    act(
        ctx,
        ActionName.ACCESS_GRANT,
        project="default",
        account="viewer",
        pipeline="P",
        permission="CAN_UPLOAD",
        reason="assign stewardship",
    )
    response = client.post(
        "/api/v1/uploads?pipeline_code=P",
        headers=authorization(viewer),
        files={"file": ("data.csv", b"id|name\n1|one\n", "text/csv")},
    )
    assert response.status_code == 200, response.text
    with ctx.engine.begin() as conn:
        conn.execute(text("UPDATE CFG_PIPELINES SET STEWARDABLE_FLAG='N'"))
    denied = client.post(
        "/api/v1/projects/default/runs",
        headers=authorization(viewer),
        json={"pipeline_code": "P", "init_only": True},
    )
    assert denied.status_code == 403, denied.text
    eligible = client.get("/api/v1/uploads/pipelines", headers=authorization(viewer))
    assert eligible.json()["pipelines"] == []


def test_unsigned_upload_is_refused_and_recorded_before_reading_body(upload_api):
    ctx, client, _, _ = upload_api
    response = client.post("/api/v1/uploads?pipeline_code=P", content=b"untrusted body")
    assert response.status_code == 401
    with ctx.engine.connect() as conn:
        row = conn.execute(text("SELECT STATUS AS status,REASON AS reason FROM AUD_UPLOADS")).one()
    assert row.status == "REFUSED" and "pipeline P" in row.reason and "CAN_UPLOAD" in row.reason


def test_admitted_validation_cannot_be_bypassed_by_changing_live_metadata(upload_api):
    from etl_craft.core.enums import RunStatus
    from etl_craft.core.errors import RunRefusedError
    from fixtures.actions import act

    ctx, client, writer, _ = upload_api
    response = client.post(
        "/api/v1/uploads?pipeline_code=P",
        headers=authorization(writer),
        files={"file": ("data.csv", b"id|name\n1|one\n", "text/csv")},
    )
    assert response.status_code == 200, response.text
    run_id = response.json()["pipeline_run_id"]
    with ctx.engine.begin() as conn:
        conn.execute(text("UPDATE CFG_PIPELINES SET STEWARDABLE_FLAG='N'"))
        conn.execute(text("UPDATE CFG_TASKS SET HANDLER='PYTHON'"))
    with pytest.raises(RunRefusedError, match=r"FILE_VALIDATION.*corrected file"):
        act(
            ctx,
            ActionName.MARK,
            pipeline_code="P",
            task_code="check",
            run_id=run_id,
            status=RunStatus.SUCCESS,
            reason="try bypass after metadata changed",
        )
