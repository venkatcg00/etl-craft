"""Apply, validate and refresh reject bad kinds without replacing the last good version."""

import csv

import pytest
from sqlalchemy import text

from etl_craft.core.enums import ActionName
from etl_craft.engine.refreshes import admitted
from etl_craft.services.config_files import read_config_files, sync_config
from etl_craft.services.validate import validate
from fixtures.actions import act
from integration.engine.test_pipeline_refresh import refresh_project as refresh_project


@pytest.mark.parametrize("value", [None, "OTHER"])
def test_python_kind_required_by_apply_validate_and_refresh(refresh_project, tmp_path, value):
    ctx, pipeline, _ = refresh_project
    good = act(ctx, ActionName.PIPELINE_REFRESH, pipeline_code="P")
    assert not good.failed
    with ctx.engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE CFG_TASK_PARAMETERS SET PARAMETER_VALUE=:value,ACTIVE_FLAG=:active "
                "WHERE PARAMETER_NAME='SCRIPT_KIND'"
            ),
            {"value": value or "INGESTION", "active": "N" if value is None else "Y"},
        )
    report = validate(ctx.engine, ctx.config)
    findings = [finding for finding in report.findings if "SCRIPT_KIND" in finding.message]
    assert findings
    assert all(finding.where == "P.T" for finding in findings)
    assert all("INGESTION" in f.message and "UTILITY" in f.message for f in findings)
    assert any(repr(value) in f.message for f in findings)
    failed = act(ctx, ActionName.PIPELINE_REFRESH, pipeline_code="P")
    assert failed.failed and any("SCRIPT_KIND" in f.message for f in failed.refreshes[0].findings)
    with ctx.engine.connect() as conn:
        assert admitted(conn, pipeline)[1] == good.refreshes[0].metadata_version
    with ctx.engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE CFG_TASK_PARAMETERS SET PARAMETER_VALUE='INGESTION',ACTIVE_FLAG='Y' "
                "WHERE PARAMETER_NAME='SCRIPT_KIND'"
            )
        )
    folder = tmp_path / "patch"
    folder.mkdir()
    with (folder / "task_parameters.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            ["PIPELINE_CODE", "TASK_CODE", "PARAMETER_NAME", "PARAMETER_VALUE", "ACTIVE_FLAG"]
        )
        writer.writerow(
            ["P", "T", "SCRIPT_KIND", value or "INGESTION", "N" if value is None else "Y"]
        )
    for apply in (False, True):
        result = sync_config(ctx.engine, ctx.config, read_config_files(folder), apply=apply)
        assert result.failed and not result.applied
        assert any(
            "SCRIPT_KIND" in f.message and "INGESTION" in f.message and "UTILITY" in f.message
            for f in result.findings
        )
    with ctx.engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT PARAMETER_VALUE AS value,ACTIVE_FLAG AS active "
                "FROM CFG_TASK_PARAMETERS WHERE PARAMETER_NAME='SCRIPT_KIND'"
            )
        ).one()
        assert row.value == "INGESTION" and row.active == "Y"


def test_invalid_live_kind_keeps_the_last_passing_script_executable(refresh_project):
    from etl_craft.core.enums import RunStatus

    ctx, pipeline, _ = refresh_project
    good = act(ctx, ActionName.PIPELINE_REFRESH, pipeline_code="P")
    assert not good.failed
    ctx.engine.update_execution_options(refresh_required=True)
    with ctx.engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE CFG_TASK_PARAMETERS SET PARAMETER_VALUE='OTHER' "
                "WHERE PARAMETER_NAME='SCRIPT_KIND'"
            )
        )
    assert act(ctx, ActionName.PIPELINE_REFRESH, pipeline_code="P").failed
    outcome = act(ctx, ActionName.RUN, pipeline_code="P")
    assert outcome.status == RunStatus.SUCCESS, outcome.message
    with ctx.engine.connect() as conn:
        row = conn.execute(
            text("SELECT STATUS AS status,INSERT_COUNT AS inserted FROM AUD_TASK_RUN_LOG")
        ).one()
        assert row.status == RunStatus.SUCCESS and row.inserted == 1
        assert admitted(conn, pipeline)[1] == good.refreshes[0].metadata_version
