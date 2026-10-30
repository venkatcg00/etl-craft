"""Independent health counts and bounded whole-project catalog reads on both Engine DBs."""

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from etl_craft.api.app import create_app
from etl_craft.core.actor import Actor, ActorKind, acting_as
from etl_craft.core.enums import ActionName, ApiRole
from etl_craft.core.errors import PermissionDeniedError, ResourceNotFoundError, UsageError
from etl_craft.engine import transitions
from etl_craft.services.operations import OperationContext
from etl_craft.services.operations.identity import authenticated_context
from fixtures.actions import act
from fixtures.metadata import add_pipeline, add_task, start_run

OWNER = Actor("owner", ActorKind.USER)


@pytest.fixture
def reads(engine_db, tmp_path):
    ctx = OperationContext(
        engine_db.engine,
        replace(
            engine_db.config,
            docs_site=replace(engine_db.config.docs_site, output=tmp_path / "catalog"),
        ),
        OWNER,
        role=ApiRole.ADMIN,
    )
    token = act(ctx, ActionName.TOKEN_CREATE, name="reader", role=ApiRole.VIEWER)
    return ctx, authenticated_context(ctx, token.metadata), token.token


def test_project_health_matches_independent_audit_counts_and_window_edges(reads, monkeypatch):
    owner, reader, token = reads
    from etl_craft.services.operations import project_health

    now = datetime(2026, 10, 29, 12, tzinfo=UTC)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now

    monkeypatch.setattr(project_health, "datetime", Clock)
    ids = []
    with acting_as(OWNER), owner.engine.begin() as conn:
        for code in ("VISIBLE", "HIDDEN", "ZERO"):
            pipeline = add_pipeline(conn, code)
            ids.append(pipeline)
            if code == "ZERO":
                continue
            task = add_task(conn, pipeline, "T")
            for delta in (
                timedelta(hours=24),
                timedelta(hours=24, microseconds=1),
                timedelta(seconds=1),
                timedelta(0),
            ):
                run = start_run(conn, pipeline)
                conn.execute(
                    text(
                        "UPDATE AUD_PIPELINES_RUN_LOG SET STATUS='FAILED', END_DATE=:end, "
                        "SLA_STATUS='BREACHED' WHERE PIPELINE_RUN_ID=:run"
                    ),
                    {"end": now - delta, "run": run},
                )
            run = start_run(conn, pipeline)
            summary = transitions.find_or_create_task_run(conn, task, run).task_run_id
            transitions.queue_attempt(conn, summary, OWNER)
        conn.execute(
            text(
                "INSERT INTO CFG_PIPELINE_GRANTS (PIPELINE_CODE,GRANTEE_ROLE,PERMISSION) "
                "VALUES ('HIDDEN','operator','CAN_VIEW')"
            )
        )
        conn.execute(
            text(
                "INSERT INTO CFG_PROJECTS (PROJECT_CODE,PROJECT_NAME,DEFAULT_POOL) SELECT "
                "'hidden_project','Hidden project',DEFAULT_POOL FROM CFG_PROJECTS WHERE "
                "PROJECT_CODE='default'"
            )
        )
    result = act(reader, ActionName.PROJECT_HEALTH, project="default")
    assert result.window_start == now - timedelta(hours=24) and result.window_end == now
    assert len(result.projects) == 1
    actual = result.projects[0]
    with owner.engine.connect() as conn:
        expected = conn.execute(
            text(
                "SELECT SUM(CASE WHEN STATUS='IN-PROGRESS' THEN 1 ELSE 0 END) AS running, "
                "SUM(CASE WHEN STATUS='FAILED' AND END_DATE>=:start AND END_DATE<:end THEN "
                "1 ELSE 0 END) AS failed, SUM(CASE WHEN SLA_STATUS='BREACHED' AND "
                "END_DATE>=:start AND END_DATE<:end THEN 1 ELSE 0 END) AS sla FROM "
                "AUD_PIPELINES_RUN_LOG WHERE PIPELINE_ID=:visible"
            ),
            {"start": result.window_start, "end": result.window_end, "visible": ids[0]},
        ).one()
        queued = conn.execute(
            text(
                "SELECT COUNT(DISTINCT a.TASK_RUN_ID) AS count FROM AUD_TASK_ATTEMPTS a "
                "JOIN AUD_TASK_RUN_LOG t ON t.TASK_RUN_ID=a.TASK_RUN_ID JOIN "
                "AUD_PIPELINES_RUN_LOG r ON r.PIPELINE_RUN_ID=t.PIPELINE_RUN_ID WHERE "
                "a.STATUS='QUEUED' AND r.PIPELINE_ID=:visible"
            ),
            {"visible": ids[0]},
        ).scalar_one()
    assert (actual.running, actual.failed, actual.sla_misses, actual.queued_tasks) == (
        expected.running,
        expected.failed,
        expected.sla,
        queued,
    )
    assert (actual.running, actual.failed, actual.sla_misses, actual.queued_tasks) == (1, 2, 2, 1)
    assert {p.project for p in act(reader, ActionName.PROJECT_HEALTH).projects} == {"default"}
    with pytest.raises(ResourceNotFoundError, match="missing or not permitted"):
        act(reader, ActionName.PROJECT_HEALTH, project="hidden_project")
    client = TestClient(create_app(owner))
    response = client.get(
        "/api/v1/project-health?project=default", headers={"Authorization": "Bearer " + token}
    )
    assert response.status_code == 200, response.text
    assert response.json()["projects"][0]["failed"] == expected.failed


def test_health_zero_counts_are_explicit(reads):
    _, reader, _ = reads
    p = act(reader, ActionName.PROJECT_HEALTH, project="default").projects[0]
    assert (p.running, p.failed, p.sla_misses, p.queued_tasks) == (0, 0, 0, 0)


def catalog_files(ctx):
    folder = ctx.config.docs_site.output
    folder.mkdir()
    folder.with_name("." + folder.name + ".publish.lock").touch()
    (folder / ".etl-craft-catalog").write_text(json.dumps({"project": "default"}))
    for name, value in {
        "index.html": "<h1>Résumé</h1>",
        "catalog.css": "body{}",
        "catalog.js": "'use strict';",
        "favicon.svg": '<svg xmlns="http://www.w3.org/2000/svg"/>',
    }.items():
        (folder / name).write_text(value)
    return folder


def test_catalog_stream_bytes_types_missing_and_project_isolation(reads):
    owner, reader, token = reads
    with pytest.raises(ResourceNotFoundError, match="generate-docs"):
        act(reader, ActionName.CATALOG_PAGE)
    folder = catalog_files(owner)
    client = TestClient(create_app(owner))
    for path, content_type in (
        ("index.html", "text/html"),
        ("catalog.css", "text/css"),
        ("catalog.js", "text/javascript"),
        ("favicon.svg", "image/svg+xml"),
    ):
        page = act(reader, ActionName.CATALOG_PAGE, path=path)
        assert b"".join(page) == (folder / path).read_bytes()
        assert page.stream.closed
        api = client.get(
            "/api/v1/projects/default/catalog/page",
            params={"path": path},
            headers={"Authorization": "Bearer " + token},
        )
        assert api.status_code == 200 and api.content == (folder / path).read_bytes(), api.text
        assert api.headers["content-type"].startswith(content_type)
        assert api.headers["x-content-type-options"] == "nosniff"
    (folder / ".etl-craft-catalog").write_text('{"project":"another"}')
    with pytest.raises(ResourceNotFoundError, match="another project"):
        act(reader, ActionName.CATALOG_PAGE)


@pytest.mark.parametrize(
    "path",
    [
        "../index.html",
        "/index.html",
        "C:/index.html",
        "%2e%2e/index.html",
        "%252e%252e/index.html",
        "nested/../index.html",
        "evil.png",
        "x\\index.html",
    ],
)
def test_catalog_refuses_traversal_and_unapproved_types(reads, path):
    _, reader, _ = reads
    with pytest.raises(UsageError, match="relative generated"):
        act(reader, ActionName.CATALOG_PAGE, path=path)


def test_catalog_refuses_symlinks_and_pipeline_limited_accounts(reads, tmp_path):
    owner, reader, _ = reads
    folder = catalog_files(owner)
    secret = tmp_path / "secret.html"
    secret.write_text("secret")
    (folder / "link.html").symlink_to(secret)
    with pytest.raises((ResourceNotFoundError, UsageError), match=r"symlink|escapes"):
        act(reader, ActionName.CATALOG_PAGE, path="link.html")
    (folder / "inside.html").symlink_to(folder / "index.html")
    with pytest.raises(ResourceNotFoundError, match="symlink"):
        act(reader, ActionName.CATALOG_PAGE, path="inside.html")
    with acting_as(OWNER), owner.engine.begin() as conn:
        add_pipeline(conn, "PRIVATE")
        conn.execute(
            text(
                "INSERT INTO CFG_PIPELINE_GRANTS (PIPELINE_CODE,GRANTEE_ROLE,PERMISSION) "
                "VALUES ('PRIVATE','operator','CAN_VIEW')"
            )
        )
    with pytest.raises(PermissionDeniedError, match=r"every pipeline.*limited.*whole-project"):
        act(reader, ActionName.CATALOG_PAGE)


def test_cli_health_about_and_catalog_share_streamed_api_bytes(
    metadata_project, monkeypatch, capsys, tmp_path
):
    from etl_craft.cli import main

    p = metadata_project
    ctx = OperationContext(p.engine, p.config, OWNER, role=ApiRole.ADMIN)
    token = act(ctx, ActionName.TOKEN_CREATE, name="reader", role=ApiRole.VIEWER)
    monkeypatch.chdir(p.root)
    monkeypatch.setenv("ETL_CRAFT_TOKEN", token.token)
    assert main(["project-health", "--json"]) == 0
    health = json.loads(capsys.readouterr().out)
    assert health["projects"][0]["project"] == "default"
    assert health["window_start"] < health["window_end"]
    assert main(["about", "--json"]) == 0
    about = json.loads(capsys.readouterr().out)
    assert about["server_version"]
    release_line = ".".join(about["package_version"].split(".")[:2])
    assert about["docs_url"] == f"https://venkatcg00.github.io/etl-craft/{release_line}/"
    folder = p.root / "catalog"
    folder.mkdir()
    (folder / ".etl-craft-catalog").write_text('{"project":"default"}')
    folder.with_name("." + folder.name + ".publish.lock").touch()
    expected = "<h1>Résumé</h1>".encode()
    (folder / "index.html").write_bytes(expected)
    assert main(["catalog", "page"]) == 0
    assert capsys.readouterr().out.encode() == expected
    output = tmp_path / "page.html"
    assert main(["catalog", "page", "--output", str(output)]) == 0
    assert output.read_bytes() == expected
    api = TestClient(create_app(ctx))
    response = api.get(
        "/api/v1/projects/default/catalog/page", headers={"Authorization": "Bearer " + token.token}
    )
    assert response.content == expected


def test_catalog_stream_releases_file_descriptors_after_io_failure(reads):
    owner, reader, _ = reads
    catalog_files(owner)
    page = act(reader, ActionName.CATALOG_PAGE)
    original = page.stream

    class FailedDisk:
        closed = False

        def read(self, size):
            assert size == 65536
            raise OSError("disk unavailable")

    page.stream = FailedDisk()
    with pytest.raises(OSError, match="disk unavailable"):
        next(page)
    assert original.closed
