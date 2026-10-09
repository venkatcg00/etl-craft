-- Attempts retain their order and the identities of their task and pipeline runs.
SELECT p.PIPELINE_ID AS pipeline_id, r.PIPELINE_RUN_ID AS pipeline_run_id,
       r.TASK_ID AS task_id, a.TASK_RUN_ID AS task_run_id, a.ATTEMPT_ID AS attempt_id,
       a.ATTEMPT_NUMBER AS attempt_number, a.STATUS AS status, a.OWNER_ID AS owner_id,
       a.LEASE_EXPIRES_AT AS lease_expires_at, a.HEARTBEAT_AT AS heartbeat_at,
       a.QUEUED_AT AS queued_at, a.CLAIMED_AT AS claimed_at, a.STARTED_AT AS started_at,
       a.ENDED_AT AS ended_at, a.HOST AS host, a.PID AS pid, a.PROCESS_START AS process_start,
       a.EXIT_CODE AS exit_code, a.SOURCE_COUNT AS source_count, a.TARGET_COUNT AS target_count,
       a.INSERT_COUNT AS insert_count, a.UPDATE_COUNT AS update_count,
       a.DELETE_COUNT AS delete_count, a.ROWS_WRITTEN AS rows_written,
       a.ERROR_MESSAGE AS error_message, a.LOG_PATH AS log_path,
       a.REQUESTED_BY AS requested_by, a.REQUESTED_BY_KIND AS requested_by_kind,
       a.NOT_BEFORE AS not_before, a.RETRYABLE AS retryable
FROM AUD_TASK_ATTEMPTS a
JOIN AUD_TASK_RUN_LOG r ON r.TASK_RUN_ID = a.TASK_RUN_ID
JOIN AUD_PIPELINES_RUN_LOG p ON p.PIPELINE_RUN_ID = r.PIPELINE_RUN_ID
WHERE a.ATTEMPT_ID = :attempt_id
