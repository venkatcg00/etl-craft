SELECT ATTEMPT_ID AS row_id, STATUS AS status, OWNER_ID AS owner_id,
TASK_RUN_ID AS task_run_id, ATTEMPT_NUMBER AS attempt_number, LEASE_EXPIRES_AT AS lease_expires_at, PID AS pid, PROCESS_START AS process_start
FROM AUD_TASK_ATTEMPTS WHERE ATTEMPT_ID = :row_id
