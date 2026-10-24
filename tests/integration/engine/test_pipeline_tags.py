"""Exact tag groups through the shared actions, CLI, generated HTTP routes and catalog."""

import json
import re
from html import unescape
from urllib.parse import parse_qs, urlsplit

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from etl_craft.api.app import create_app
from etl_craft.cli import main
from etl_craft.core.actor import current_actor
from etl_craft.core.enums import ActionName, ApiRole, ControlAction
from etl_craft.core.errors import UsageError
from etl_craft.services.catalog.collect import collect_catalog
from etl_craft.services.catalog.site import write_site
from etl_craft.services.operations import OperationContext
from etl_craft.services.operations.requests import BulkControlRequest, ControlRequest
from etl_craft.services.operations.serialization import encode
from fixtures.actions import act
from fixtures.auth import service_token
from fixtures.metadata import add_pipeline, finish_run, start_run


@pytest.fixture
def tagged(metadata_project, monkeypatch):
    project = metadata_project
    with project.engine.begin() as conn:
        for code, tags in (
            ("SALES", ["team:sales", "tier:gold"]),
            ("BRONZE", ["team:sales", "tier:bronze"]),
            ("OPS", ["team:ops", "tier:gold"]),
            ("UNTAGGED", []),
            ("INACTIVE", ["team:sales", "tier:gold"]),
        ):
            pipeline = add_pipeline(conn, code)
            conn.execute(
                text("UPDATE CFG_PIPELINES SET PIPELINE_PARAMETERS=:params WHERE PIPELINE_ID=:id"),
                {"params": json.dumps({"TAGS": tags}), "id": pipeline},
            )
            ended = start_run(conn, pipeline)
            finish_run(conn, ended)
            start_run(conn, pipeline)
        conn.execute(
            text("UPDATE CFG_PIPELINES SET ACTIVE_FLAG='N' WHERE PIPELINE_CODE='INACTIVE'")
        )
    token = service_token(project.engine)
    monkeypatch.setenv("ETL_CRAFT_TOKEN", token)
    ctx = OperationContext(project.engine, project.config, current_actor(), role=ApiRole.ADMIN)
    return project, ctx, token


@pytest.mark.parametrize(
    "command,field", [("pipelines", "pipelines"), ("status", "runs"), ("history", "entries")]
)
def test_cli_and_http_apply_every_tag_and_preserve_unfiltered_reads(tagged, command, field, capsys):
    project, ctx, token = tagged
    assert (
        main(
            [
                "--config",
                str(project.config.config_path),
                command,
                "--tag",
                "team:sales",
                "--tag",
                "tier:gold",
                "--format",
                "json",
            ]
        )
        == 0
    )
    document = json.loads(capsys.readouterr().out)
    entries = document[field]
    codes = [
        e["run"]["pipeline_code"] if command == "status" else e["pipeline_code"] for e in entries
    ]
    assert codes and set(codes) == {"SALES"}
    with TestClient(create_app(ctx)) as client:
        response = client.get(
            "/api/v1/" + command,
            params=[("tag", "team:sales"), ("tag", "tier:gold")],
            headers={"Authorization": "Bearer " + token},
        )
        assert response.status_code == 200, response.text
        actual = response.json()
        if command == "status":
            assert [r["run"]["pipeline_code"] for r in actual[field]] == codes
        else:
            assert actual == document
        assert (
            client.get(
                "/api/v1/" + command,
                params={"tag": "missing"},
                headers={"Authorization": "Bearer " + token},
            ).json()[field]
            == []
        )
    all_definitions = act(ctx, ActionName.PIPELINES).pipelines
    assert {p.pipeline_code for p in all_definitions} == {"SALES", "BRONZE", "OPS", "UNTAGGED"}
    assert next(p for p in all_definitions if p.pipeline_code == "SALES").tags == (
        "team:sales",
        "tier:gold",
    )
    if command == "status":
        assert act(ctx, ActionName.STATUS, pipeline_code="SALES").run.pipeline_code == "SALES"


def test_history_cursor_pages_across_tagged_pipelines_without_losing_runs(tagged):
    _, ctx, _ = tagged
    first = act(ctx, ActionName.HISTORY, tag=("team:sales",), limit=1)
    assert first.before is not None and len(first.entries) == 1
    second = act(ctx, ActionName.HISTORY, tag=("team:sales",), limit=1, before=first.before)
    assert second.entries[0].pipeline_run_id < first.entries[0].pipeline_run_id
    assert first.entries[0].pipeline_code == second.entries[0].pipeline_code == "BRONZE"


def test_tag_pause_requires_confirmation_and_records_fixed_codes(tagged, capsys):
    project, ctx, token = tagged
    argv = [
        "--config",
        str(project.config.config_path),
        "pause",
        "--tag",
        "team:sales",
        "--tag",
        "tier:gold",
        "--reason",
        "source outage",
    ]
    assert main(argv) == 2
    refused = capsys.readouterr()
    assert "Selected pipelines: SALES" in refused.out
    assert "--yes" in refused.err
    assert all(p.paused is None for p in act(ctx, ActionName.PIPELINES).pipelines)
    assert main([*argv, "--yes", "--format", "json"]) == 0
    output = capsys.readouterr()
    done = json.loads(output.out)
    assert [o["pipeline_code"] for o in done["outcomes"]] == ["SALES"]
    assert done["outcomes"][0]["control"]["pipeline"]["paused"] is not None
    # A confirmed API selection remains the same when its original tag no longer matches.
    with project.engine.begin() as conn:
        conn.execute(
            text("UPDATE CFG_PIPELINES SET PIPELINE_PARAMETERS='{}' WHERE PIPELINE_CODE='SALES'")
        )
    request = BulkControlRequest(
        requests=(
            ControlRequest(
                pipeline_code="SALES", action=ControlAction.RESUME, reason="source returned"
            ),
        )
    )
    with TestClient(create_app(ctx)) as client:
        response = client.post(
            "/api/v1/projects/default/controls/bulk",
            json=encode(request),
            headers={"Authorization": "Bearer " + token},
        )
    assert response.status_code == 200, response.text
    assert [o["pipeline_code"] for o in response.json()["outcomes"]] == ["SALES"]
    with project.engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT ARGUMENTS AS arguments FROM AUD_ACTIONS "
                "WHERE COMMAND='control' AND OUTCOME='REQUESTED' ORDER BY ACTION_ID"
            )
        ).all()
        arguments = [
            json.loads(row.arguments) if isinstance(row.arguments, str) else row.arguments
            for row in rows
        ]
        assert len(arguments) == 2
        assert all(row["pipeline_code"] == "SALES" and "tag" not in row for row in arguments)
    assert all(p.paused is None for p in act(ctx, ActionName.PIPELINES).pipelines)


def test_bulk_definition_refusal_does_not_hide_other_outcomes(tagged):
    _, ctx, _ = tagged
    done = act(
        ctx,
        ActionName.CONTROL_BULK,
        requests=tuple(
            ControlRequest(pipeline_code=code, action=ControlAction.PAUSE, reason="maintenance")
            for code in ("SALES", "UNKNOWN", "OPS")
        ),
    )
    assert [outcome.pipeline_code for outcome in done.outcomes] == ["SALES", "UNKNOWN", "OPS"]
    assert [outcome.error is None for outcome in done.outcomes] == [True, False, True]
    assert "UNKNOWN" in done.outcomes[1].error


@pytest.mark.parametrize(
    "value,rule",
    [
        ([], None),
        (["team:sales"], None),
        (["a" * 64], None),
        (None, "list"),
        ("sales", "list"),
        ([""], "1 to 64"),
        (["a" * 65], "1 to 64"),
        (["Sales"], "lowercase"),
        (["sáles"], "ASCII"),
        (["sales team"], "replace"),
        (["sales\n"], "replace"),
        ([1], "replace"),
        (["sales", "sales"], "duplicate"),
        ([f"t{i}" for i in range(21)], "20"),
    ],
)
def test_tag_metadata_validation_names_rule_and_remedy(metadata_project, value, rule):
    project = metadata_project
    with project.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        conn.execute(
            text("UPDATE CFG_PIPELINES SET PIPELINE_PARAMETERS=:params WHERE PIPELINE_ID=:id"),
            {"params": json.dumps({"TAGS": value}), "id": pipeline},
        )
    ctx = OperationContext(project.engine, project.config, current_actor(), role=ApiRole.ADMIN)
    findings = act(ctx, ActionName.VALIDATE).findings
    tag_errors = [f for f in findings if "TAGS" in f.message]
    if rule is None:
        assert not tag_errors
        assert act(ctx, ActionName.PIPELINES).pipelines[0].tags == tuple(value)
    else:
        assert tag_errors and all(f.where == "P" for f in tag_errors)
        assert any(
            rule in f.message
            and any(remedy in f.message for remedy in ("remove", "replace", "write"))
            for f in tag_errors
        )


def test_catalog_tags_link_to_a_filtered_overview_and_invalidate_cached_pages(tagged, tmp_path):
    project, _, _ = tagged
    folder = tmp_path / "catalog"

    def build():
        catalog = collect_catalog(
            project.engine, project.config, with_warehouse=False, record_lineage=False
        )
        return write_site(catalog, project.config, folder)

    build()
    page = (folder / "pipelines/SALES.html").read_text()
    links = re.findall(r'<a class="tag-chip" href="([^"]+)"', page)
    assert {parse_qs(urlsplit(unescape(link)).query)["tag"][0] for link in links} == {
        "team:sales",
        "tier:gold",
    }
    overview = (folder / "dags.html").read_text()
    assert "<fieldset><legend>Filter pipelines by tags</legend>" in overview
    assert set(re.findall(r'name="tag" value="([^"]+)"', overview)) == {
        "team:sales",
        "team:ops",
        "tier:gold",
        "tier:bronze",
    }
    assert 'data-pipeline-tags="team:sales tier:gold"' in overview
    with project.engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE CFG_PIPELINES SET PIPELINE_PARAMETERS=:params WHERE PIPELINE_CODE='SALES'"
            ),
            {"params": json.dumps({"TAGS": ["changed"]})},
        )
    build()
    assert "changed" in (folder / "pipelines/SALES.html").read_text()
    assert 'data-pipeline-tags="changed"' in (folder / "dags.html").read_text()


@pytest.mark.parametrize(
    "action,path",
    [
        (ActionName.AUDIT, "audit"),
        (ActionName.VALIDATE, "validation"),
        (ActionName.CATALOG_GAPS, "catalog/gaps"),
    ],
)
def test_other_project_read_filters_agree_through_generated_routes(tagged, action, path):
    _, ctx, token = tagged
    with TestClient(create_app(ctx)) as client:
        response = client.get(
            "/api/v1/" + path,
            params=[("tag", "missing")],
            headers={"Authorization": "Bearer " + token},
        )
    assert response.status_code == 200, response.text
    document = response.json()
    if action == ActionName.AUDIT:
        assert document["actions"] == document["changes"] == []
    elif action == ActionName.VALIDATE:
        assert document["pipelines"] == document["tasks"] == 0 and not document["findings"]
    else:
        assert document["records"] == []


@pytest.mark.parametrize("value", ["", "Sales", "sales team", "é", "x" * 65])
def test_invalid_filter_is_refused_by_cli_and_http_without_normalizing(tagged, value, capsys):
    project, ctx, token = tagged
    with pytest.raises(UsageError, match="tag"):
        act(ctx, ActionName.PIPELINES, tag=(value,))
    assert main(["--config", str(project.config.config_path), "pipelines", "--tag", value]) == 2
    assert "--tag" in capsys.readouterr().err
    with TestClient(create_app(ctx)) as client:
        response = client.get(
            "/api/v1/projects/default/pipelines",
            params={"tag": value},
            headers={"Authorization": "Bearer " + token},
        )
    assert response.status_code == 400
    assert "tag" in response.text and "replace" in response.text


def test_cli_resolves_a_large_group_before_sending_bounded_batches(tagged, capsys):
    project, _, _ = tagged
    with project.engine.begin() as conn:
        for index in range(101):
            pipeline = add_pipeline(conn, f"BATCH_{index:03}")
            conn.execute(
                text("UPDATE CFG_PIPELINES SET PIPELINE_PARAMETERS=:params WHERE PIPELINE_ID=:id"),
                {"params": json.dumps({"TAGS": ["bulk:large"]}), "id": pipeline},
            )
    assert (
        main(
            [
                "--config",
                str(project.config.config_path),
                "pause",
                "--tag",
                "bulk:large",
                "--reason",
                "maintenance",
                "--yes",
                "--format",
                "json",
            ]
        )
        == 0
    )
    output = capsys.readouterr()
    done = json.loads(output.out)
    assert len(done["outcomes"]) == 101
    assert all(
        outcome["error"] is None and outcome["control"]["pipeline"]["paused"] is not None
        for outcome in done["outcomes"]
    )
    assert "BATCH_000" in output.err and "BATCH_100" in output.err


def test_audit_tag_filter_keeps_selected_actions_and_metadata_captures(tagged):
    project, ctx, _ = tagged
    act(
        ctx,
        ActionName.CONTROL,
        pipeline_code="SALES",
        action=ControlAction.PAUSE,
        reason="maintenance",
    )
    act(
        ctx,
        ActionName.CONTROL,
        pipeline_code="OPS",
        action=ControlAction.PAUSE,
        reason="other maintenance",
    )
    with project.engine.connect() as conn:
        pipeline_id = conn.execute(
            text("SELECT PIPELINE_ID FROM CFG_PIPELINES WHERE PIPELINE_CODE='SALES'")
        ).scalar_one()
    history = act(ctx, ActionName.AUDIT, tag=("team:sales", "tier:gold"))
    assert history.actions and history.changes
    assert all(action.pipeline_id == pipeline_id for action in history.actions)
    assert all(
        any(
            doc and doc.get("pipeline_id") == pipeline_id
            for doc in (change.before_json, change.after_json)
        )
        for change in history.changes
    )


@pytest.mark.parametrize("tag", ["sales", None])
def test_python_filter_requires_a_sequence_instead_of_splitting_text(tagged, tag):
    _, ctx, _ = tagged
    with pytest.raises(UsageError, match=r"tag.*expected a list"):
        act(ctx, ActionName.PIPELINES, tag=tag)


def test_catalog_gaps_keep_only_tasks_of_matching_pipelines(metadata_project):
    project = metadata_project
    project.load_examples()
    ctx = OperationContext(project.engine, project.config, current_actor(), role=ApiRole.ADMIN)
    gaps = act(ctx, ActionName.CATALOG_GAPS, tag=("daily",))
    assert gaps.records
    assert all(
        record.tasks and all(task.startswith("SALES_DAILY.") for task in record.tasks)
        for record in gaps.records
    )


def test_validation_filter_counts_only_matching_active_pipelines(tagged, capsys):
    project, ctx, token = tagged
    assert (
        main(
            [
                "--config",
                str(project.config.config_path),
                "validate",
                "--tag",
                "team:sales",
                "--tag",
                "tier:gold",
                "--format",
                "json",
            ]
        )
        == 0
    )
    result = json.loads(capsys.readouterr().out)
    assert result["pipelines"] == 1 and result["tasks"] == 0
    with TestClient(create_app(ctx)) as client:
        response = client.get(
            "/api/v1/projects/default/validation",
            params=[("tag", "team:sales"), ("tag", "tier:gold")],
            headers={"Authorization": "Bearer " + token},
        )
    assert response.status_code == 200 and response.json() == result
