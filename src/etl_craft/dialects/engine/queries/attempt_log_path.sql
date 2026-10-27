-- Bind an engine-built project/run/task log identity in the owner-claim transaction.
UPDATE AUD_TASK_ATTEMPTS SET LOG_PATH = (
    SELECT :log_dir || '/project-' || CAST(p.PROJECT_ID AS varchar) || '/' || p.PIPELINE_CODE
    || '/run-' || CAST(r.PIPELINE_RUN_ID AS varchar) || '/' || t.TASK_CODE
    || '.attempt-' || CAST(AUD_TASK_ATTEMPTS.ATTEMPT_NUMBER AS varchar) || '.log'
    FROM AUD_TASK_RUN_LOG tr JOIN AUD_PIPELINES_RUN_LOG r ON r.PIPELINE_RUN_ID=tr.PIPELINE_RUN_ID
    JOIN CFG_PIPELINES p ON p.PIPELINE_ID=r.PIPELINE_ID JOIN CFG_TASKS t ON t.TASK_ID=tr.TASK_ID
    WHERE tr.TASK_RUN_ID=AUD_TASK_ATTEMPTS.TASK_RUN_ID
)
WHERE ATTEMPT_ID=:attempt_id AND OWNER_ID=:owner AND STATUS='CLAIMED'
RETURNING LOG_PATH AS log_path
