SELECT c.CONTROL_ID AS control_id, c.REQUEST_KEY AS request_key,
c.PIPELINE_RUN_ID AS pipeline_run_id, r.PIPELINE_ID AS pipeline_id,
p.PIPELINE_CODE AS pipeline_code, c.TASK_ID AS task_id, t.TASK_CODE AS task_code,
c.ACTION AS action, c.STATUS AS status, c.ATTEMPT_IDS AS attempt_ids,
c.REQUESTED_BY AS requested_by, c.REQUESTED_BY_KIND AS requested_by_kind,
c.REASON AS reason, c.REQUESTED_AT AS requested_at, c.ACKNOWLEDGED_AT AS acknowledged_at,
c.COMPLETED_AT AS completed_at, c.MESSAGE AS message, r.STATUS AS run_status
FROM AUD_CONTROL_REQUESTS c JOIN AUD_PIPELINES_RUN_LOG r ON r.PIPELINE_RUN_ID=c.PIPELINE_RUN_ID
JOIN CFG_PIPELINES p ON p.PIPELINE_ID=r.PIPELINE_ID
LEFT JOIN CFG_TASKS t ON t.TASK_ID=c.TASK_ID WHERE c.CONTROL_ID=:control_id
