-- The task, pipeline run and attempt a task process was started for, by :task_run_id.
SELECT l.TASK_ID AS task_id, l.PIPELINE_RUN_ID AS pipeline_run_id,
       l.ATTEMPT_COUNT AS attempt_count, l.STATUS AS status
FROM AUD_TASK_RUN_LOG l
WHERE l.TASK_RUN_ID = :task_run_id
