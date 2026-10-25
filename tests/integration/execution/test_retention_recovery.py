"""A real purge process can die at either side of a file deletion and resume safely."""

import pytest
import yaml
from sqlalchemy import text

from etl_craft.core.enums import RunStatus
from etl_craft.execution.attempt_records import attempt_log_path
from etl_craft.services.retention import retaining


@pytest.mark.chaos
@pytest.mark.parametrize(
    "point",
    ["retention.after_log_commit", "retention.after_file_delete", "retention.after_batch_commit"],
)
def test_purge_process_loss_resumes_committed_work(cli_project, point):
    p = cli_project
    raw = yaml.safe_load(p.config.config_path.read_text())
    raw["Retention"] = {"Attempt_log_days": 30, "Run_history_days": 90}
    p.config.config_path.write_text(yaml.safe_dump(raw, sort_keys=False))
    code, output = p.run("run", "--pipeline-code", "P")
    assert code == 0, output
    with p.engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT a.ATTEMPT_ID AS attempt_id,a.TASK_RUN_ID AS task_run_id,"
                "r.PIPELINE_RUN_ID AS run_id FROM AUD_TASK_ATTEMPTS a "
                "JOIN AUD_TASK_RUN_LOG t ON t.TASK_RUN_ID=a.TASK_RUN_ID "
                "JOIN AUD_PIPELINES_RUN_LOG r ON r.PIPELINE_RUN_ID=t.PIPELINE_RUN_ID"
            )
        ).one()
    path = attempt_log_path(p.config, "P", row.run_id, "load", 1)
    assert path.is_file()
    with retaining(), p.engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE AUD_PIPELINES_RUN_LOG SET END_DATE='2000-01-01' WHERE PIPELINE_RUN_ID=:id"
            ),
            {"id": row.run_id},
        )
        conn.execute(
            text("UPDATE AUD_TASK_ATTEMPTS SET ENDED_AT='2000-01-01' WHERE ATTEMPT_ID=:id"),
            {"id": row.attempt_id},
        )
    code, output = p.run("purge", fault=point + ":kill")
    assert code == 137, output
    code, output = p.run("purge")
    assert code == 0, output
    assert not path.exists()
    with p.engine.connect() as conn:
        assert (
            conn.execute(text("SELECT COUNT(*) AS count FROM AUD_RETENTION_FILES")).scalar_one()
            == 0
        )
        assert (
            conn.execute(text("SELECT COUNT(*) AS count FROM AUD_TASK_ATTEMPTS")).scalar_one() == 0
        )
    code, output = p.run("run", "--pipeline-code", "P")
    assert code == 0, output
    with p.engine.connect() as conn:
        assert (
            conn.execute(text("SELECT STATUS AS status FROM AUD_TASK_ATTEMPTS")).scalar_one()
            == RunStatus.SUCCESS
        )
