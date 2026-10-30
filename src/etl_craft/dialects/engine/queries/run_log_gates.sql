-- Gate and event waits the run's tasks opened, with their budget.
SELECT t.TASK_CODE AS task_code, w.WAIT_KIND AS wait_kind, w.FIRST_CHECK_AT AS first_check_at,
       w.WAIT_UNTIL AS wait_until, w.LOOKS AS looks
FROM AUD_GATE_WAITS w JOIN CFG_TASKS t ON t.TASK_ID = w.TASK_ID
WHERE w.PIPELINE_RUN_ID = :pipeline_run_id
