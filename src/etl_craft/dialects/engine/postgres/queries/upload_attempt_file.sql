SELECT u.UPLOAD_ID AS upload_id, u.PIPELINE_RUN_ID AS upload_run_id,
 u.STATUS AS upload_status, u.SHA256 AS sha256, u.SIZE_BYTES AS size_bytes,
 a.OWNER_ID AS owner_id, a.STATUS AS attempt_status,
 a.LEASE_EXPIRES_AT>clock_timestamp() AS lease_live,
 tr.PIPELINE_RUN_ID AS run_id,tr.TASK_ID AS task_id,a.TASK_RUN_ID AS task_run_id,
 r.PIPELINE_ID AS pipeline_id
FROM AUD_UPLOADS u CROSS JOIN AUD_TASK_ATTEMPTS a
JOIN AUD_TASK_RUN_LOG tr ON tr.TASK_RUN_ID=a.TASK_RUN_ID
JOIN AUD_PIPELINES_RUN_LOG r ON r.PIPELINE_RUN_ID=tr.PIPELINE_RUN_ID
WHERE u.UPLOAD_ID=:upload_id AND a.ATTEMPT_ID=:attempt_id
