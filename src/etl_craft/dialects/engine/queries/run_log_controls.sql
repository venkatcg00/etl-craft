-- Operator control requests on the run and their acknowledgement and completion.
SELECT c.CONTROL_ID AS control_id, t.TASK_CODE AS task_code, c.ACTION AS action,
       c.STATUS AS status, c.REQUESTED_BY AS requested_by,
       c.REQUESTED_BY_KIND AS requested_by_kind, c.REASON AS reason,
       c.REQUESTED_AT AS requested_at, c.ACKNOWLEDGED_AT AS acknowledged_at,
       c.COMPLETED_AT AS completed_at, c.MESSAGE AS message
FROM AUD_CONTROL_REQUESTS c LEFT JOIN CFG_TASKS t ON t.TASK_ID = c.TASK_ID
WHERE c.PIPELINE_RUN_ID = :pipeline_run_id
ORDER BY c.CONTROL_ID
