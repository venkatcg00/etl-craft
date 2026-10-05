SELECT a.ATTEMPT_ID AS attempt_id, a.ATTEMPT_NUMBER AS attempt_number,
 a.OWNER_ID AS owner_id, a.LEASE_EXPIRES_AT AS lease_expires_at,
 a.HEARTBEAT_AT AS heartbeat_at, a.QUEUED_AT AS queued_at,
 a.HOST AS host, a.PID AS pid, a.PROCESS_START AS process_start,
 t.PIPELINE_RUN_ID AS pipeline_run_id, t.TASK_ID AS task_id
FROM AUD_TASK_ATTEMPTS a JOIN AUD_TASK_RUN_LOG t ON t.TASK_RUN_ID = a.TASK_RUN_ID
JOIN AUD_PIPELINES_RUN_LOG r ON r.PIPELINE_RUN_ID = t.PIPELINE_RUN_ID
WHERE a.STATUS IN ('CLAIMED','RUNNING')
 AND (CAST(:pipeline_id AS bigint) IS NULL OR r.PIPELINE_ID = :pipeline_id)
 AND (CAST(:task_id AS bigint) IS NULL OR t.TASK_ID = :task_id)
