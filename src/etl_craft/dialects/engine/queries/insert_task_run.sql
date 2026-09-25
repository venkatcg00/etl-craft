-- Bind :task_id to :pipeline_run_id. The unique index refuses a second row.
INSERT INTO AUD_TASK_RUN_LOG (TASK_ID, PIPELINE_RUN_ID, STATUS)
VALUES (:task_id, :pipeline_run_id, 'IN-PROGRESS')
RETURNING TASK_RUN_ID AS task_run_id
