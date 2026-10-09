-- Persisted pipeline and task gate waits under one exact run.
SELECT TASK_ID AS task_id, FIRST_CHECK_AT AS first_check_at, NEXT_CHECK_AT AS next_check_at,
       LOOKS AS looks, WAIT_UNTIL AS wait_until
FROM AUD_GATE_WAITS WHERE PIPELINE_RUN_ID=:pipeline_run_id ORDER BY COALESCE(TASK_ID,0)
