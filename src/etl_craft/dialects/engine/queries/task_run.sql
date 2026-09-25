-- The row of :task_id under :pipeline_run_id, if any.
SELECT TASK_RUN_ID AS task_run_id, STATUS AS status
FROM AUD_TASK_RUN_LOG
WHERE TASK_ID = :task_id AND PIPELINE_RUN_ID = :pipeline_run_id
