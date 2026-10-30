"""A run's zip: the run log, every attempt log by task, the run's documents and a manifest."""

import io
import json
import tracemalloc
import zipfile
from dataclasses import replace

import pytest
from sqlalchemy import text

from etl_craft.core.enums import ActionName, ApiRole
from fixtures.actions import act
from fixtures.run_scene import seed


@pytest.fixture
def ran(engine_db, tmp_path):
    return seed(engine_db.engine, replace(engine_db.config, log_dir=tmp_path / "logs"))


def place_log(ctx, run, attempt_id, number, chunks):
    """Write an attempt's local log where the engine records it, and record its path."""
    name = f"EXTRACT.attempt-{number}.log"
    path = ctx.config.log_dir / "project-1" / "ORDERS" / f"run-{run}" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as stream:
        for chunk in chunks:
            stream.write(chunk)
    with ctx.engine.begin() as conn:
        conn.execute(
            text("UPDATE AUD_TASK_ATTEMPTS SET LOG_PATH=:path WHERE ATTEMPT_ID=:id"),
            {"path": str(path.absolute()), "id": attempt_id},
        )


def zip_of(ctx, run):
    chunks = act(ctx, ActionName.RUN_ZIP, pipeline_code="ORDERS", run_id=run)
    return zipfile.ZipFile(io.BytesIO(b"".join(chunks)))


def test_zip_holds_the_run_log_documents_and_attempt_logs_with_a_manifest(ran):
    ctx, run, (failed, retried) = ran
    place_log(ctx, run, failed, 1, [b"first try\n"])
    archive = zip_of(ctx, run)
    manifest = json.loads(archive.read("manifest.json"))["files"]
    listed = {entry["path"]: entry for entry in manifest}
    assert set(archive.namelist()) == {
        *(p for p, e in listed.items() if "bytes" in e),
        "manifest.json",
    }
    for path, entry in listed.items():
        if "bytes" in entry:
            assert archive.getinfo(path).file_size == entry["bytes"]
    run_log = b"".join(act(ctx, ActionName.RUN_LOG, pipeline_code="ORDERS", run_id=run))
    assert archive.read("run.log") == run_log
    assert archive.read("EXTRACT/attempt-1.log") == b"first try\n"
    assert json.loads(archive.read("status.json"))["run"]["pipeline_run_id"] == run
    assert json.loads(archive.read("history.json"))["schema"] == "etl-craft/history/1"
    assert json.loads(archive.read("EXTRACT/explain.json"))["schema"].startswith("etl-craft/")
    missing = listed["EXTRACT/attempt-2.log"]
    assert "bytes" not in missing
    assert f"etl-craft logs --attempt-id {retried}" in missing["missing"]


def test_zip_streams_a_large_log_without_holding_it(ran):
    ctx, run, (failed, _) = ran
    block = b"z" * 65536
    place_log(ctx, run, failed, 1, (block for _ in range(512)))
    tracemalloc.start()
    try:
        size = sum(len(c) for c in act(ctx, ActionName.RUN_ZIP, pipeline_code="ORDERS", run_id=run))
        peak = tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()
    assert size > 0 and peak < 8 * 1024 * 1024


def test_cli_writes_the_zip_and_the_api_serves_it_as_an_attachment(
    metadata_project, monkeypatch, capsys, tmp_path
):
    from fastapi.testclient import TestClient

    from etl_craft.api.app import create_app
    from etl_craft.cli import main

    p = metadata_project
    ctx, run, _ = seed(p.engine, p.config)
    token = act(ctx, ActionName.TOKEN_CREATE, name="reader", role=ApiRole.VIEWER).token
    monkeypatch.chdir(p.root)
    monkeypatch.setenv("ETL_CRAFT_TOKEN", token)
    target = tmp_path / "run.zip"
    assert (
        main(["logs", "--pipeline-code", "ORDERS", "--run-id", str(run), "--zip", str(target)]) == 0
    )
    assert f"wrote {target}" in capsys.readouterr().out
    written = zipfile.ZipFile(target)
    assert main(["logs", "--attempt-id", "1", "--zip", str(target)]) != 0
    assert "zips cover whole runs" in capsys.readouterr().err
    response = TestClient(create_app(ctx)).get(
        "/api/v1/projects/default/run-zip",
        params={"pipeline_code": "ORDERS", "run_id": run},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/zip"
    disposition = f'attachment; filename="ORDERS-run-{run}.zip"'
    assert response.headers["content-disposition"] == disposition
    served = zipfile.ZipFile(io.BytesIO(response.content))
    assert served.namelist() == written.namelist()
    paths = [entry["path"] for entry in json.loads(served.read("manifest.json"))["files"]]
    assert paths == [entry["path"] for entry in json.loads(written.read("manifest.json"))["files"]]
    assert served.read("run.log") == written.read("run.log")
