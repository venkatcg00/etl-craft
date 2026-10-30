"""Config tag edits report matching grants and policy changes on both interfaces."""

import csv
import json

import pytest
from sqlalchemy import text

from etl_craft.cli import main
from etl_craft.core.actor import SYSTEM_ACTOR, acting_as
from etl_craft.core.enums import ActionName, ApiRole
from etl_craft.core.enums_access import Permission
from etl_craft.services.operations import OperationContext
from fixtures.actions import act
from fixtures.auth import service_token
from fixtures.metadata import add_pipeline


@pytest.mark.parametrize("output_format", ["text", "json"])
@pytest.mark.parametrize("principal", ["group", "role", "account"])
def test_tag_edits_report_grant_matches_and_open_restricted_switches(
    metadata_project, monkeypatch, capsys, output_format, principal
):
    project = metadata_project
    monkeypatch.chdir(project.root)
    monkeypatch.setenv("ETL_CRAFT_TOKEN", service_token(project.engine))
    ctx = OperationContext(project.engine, project.config, SYSTEM_ACTOR, role=ApiRole.ADMIN)
    act(ctx, ActionName.GROUP_ADD, name="auditors")
    with acting_as(SYSTEM_ACTOR), project.engine.begin() as conn:
        add_pipeline(conn, "P")
        holder = add_pipeline(conn, "HOLDER")
        conn.execute(
            text("UPDATE CFG_PIPELINES SET PIPELINE_PARAMETERS=:tags WHERE PIPELINE_ID=:id"),
            {"tags": json.dumps({"TAGS": ["sensitive"]}), "id": holder},
        )
    identity = {
        principal: {"group": "auditors", "role": "viewer", "account": "test-admin"}[principal]
    }
    label = f"{principal}:{identity[principal]}"
    act(
        ctx,
        ActionName.ACCESS_GRANT,
        project="default",
        **identity,
        tag="sensitive",
        permission=Permission.CAN_RUN,
        reason="Review tagged pipelines",
    )
    folder = project.root / "patch"
    folder.mkdir()
    for tags, restricted, expected in (
        (["sensitive"], True, "starts matching"),
        ([], False, "stops matching"),
        (["ungranted"], False, None),
    ):
        with (folder / "pipelines.csv").open("w", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(["PIPELINE_CODE", "PIPELINE_PARAMETERS"])
            writer.writerow(["P", json.dumps({"TAGS": tags})])
        reports = []
        for command in ("plan", "apply"):
            assert (
                main(["config", command, "--config-dir", str(folder), "--format", output_format])
                == 0
            )
            output = capsys.readouterr().out
            if output_format == "json":
                report = json.loads(output)["tag_access_changes"]
                reports.append(report)
                if expected is None:
                    assert report == []
                else:
                    assert report == [
                        {
                            "pipeline": "P",
                            "was_restricted": not restricted,
                            "is_restricted": restricted,
                            "policy_change": (
                                "open -> restricted: project-level access no longer applies; "
                                "only matching "
                                "pipeline and tag grants (plus project CAN_MANAGE) "
                                "reach this pipeline"
                                if restricted
                                else "restricted -> open: the project level now applies"
                            ),
                            "grants": [
                                {
                                    "principal": label,
                                    "tag": "sensitive",
                                    "permission": "CAN_RUN",
                                    "starts_matching": restricted,
                                }
                            ],
                        }
                    ]
            elif expected is None:
                assert "access P:" not in output
            else:
                for part in ("access P:", label, "sensitive", "CAN_RUN", expected):
                    assert part in output
                if restricted:
                    for part in (
                        "open -> restricted",
                        "project-level access no longer applies",
                        "matching pipeline and tag grants",
                        "project CAN_MANAGE",
                    ):
                        assert part in output
                else:
                    assert "restricted -> open: the project level now applies" in output
        if reports:
            assert reports[0] == reports[1]
        with project.engine.connect() as conn:
            active = conn.execute(
                text(
                    "SELECT PERMISSION AS permission FROM CFG_PIPELINE_GRANTS "
                    "WHERE TAG='sensitive' AND ACTIVE_FLAG='Y'"
                )
            ).scalar_one()
            assert active == Permission.CAN_RUN
