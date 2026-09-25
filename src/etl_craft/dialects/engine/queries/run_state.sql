-- The state of each of :task_ids under :pipeline_run_id.
SELECT TASK_ID AS task_id, STATUS AS status, TARGET_COUNT AS target_count
FROM AUD_TASK_RUN_LOG
WHERE PIPELINE_RUN_ID = :pipeline_run_id AND TASK_ID IN :task_ids
