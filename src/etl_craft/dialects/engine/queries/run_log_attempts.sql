-- Every task run of the run with each of its attempts; a task run without attempts has one row.
SELECT t.TASK_CODE AS task_code, tr.TASK_RUN_ID AS task_run_id, tr.STATUS AS task_status,
       tr.START_DATE AS task_start, tr.END_DATE AS task_end,
       a.ATTEMPT_ID AS attempt_id, a.ATTEMPT_NUMBER AS attempt_number, a.STATUS AS status,
       a.QUEUED_AT AS queued_at, a.NOT_BEFORE AS not_before, a.CLAIMED_AT AS claimed_at,
       a.STARTED_AT AS started_at, a.ENDED_AT AS ended_at, a.OWNER_ID AS owner_id,
       a.HOST AS host, a.PID AS pid, a.POOL AS pool, a.EXIT_CODE AS exit_code,
       a.ROWS_WRITTEN AS rows_written, a.REJECT_COUNT AS reject_count,
       a.ERROR_MESSAGE AS error_message, a.FAILURE_CLASS AS failure_class,
       a.REQUESTED_BY AS requested_by, a.REQUESTED_BY_KIND AS requested_by_kind,
       a.CANCEL_REQUESTED_AT AS cancel_requested_at,
       a.CANCEL_ACKNOWLEDGED_AT AS cancel_acknowledged_at
FROM AUD_TASK_RUN_LOG tr
JOIN CFG_TASKS t ON t.TASK_ID = tr.TASK_ID
LEFT JOIN AUD_TASK_ATTEMPTS a ON a.TASK_RUN_ID = tr.TASK_RUN_ID
WHERE tr.PIPELINE_RUN_ID = :pipeline_run_id
ORDER BY tr.TASK_RUN_ID, a.ATTEMPT_NUMBER
