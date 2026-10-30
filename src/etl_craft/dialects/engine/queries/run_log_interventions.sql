-- Marks, reruns, skips and overrides recorded against the run.
SELECT i.INTERVENTION_ID AS intervention_id, t.TASK_CODE AS task_code, i.ACTION AS action,
       i.FROM_STATUS AS from_status, i.TO_STATUS AS to_status, i.REASON AS reason,
       i.REQUESTED_BY AS requested_by, i.REQUESTED_BY_KIND AS requested_by_kind,
       i.REQUESTED_AT AS requested_at
FROM AUD_RUN_INTERVENTIONS i LEFT JOIN CFG_TASKS t ON t.TASK_ID = i.TASK_ID
WHERE i.PIPELINE_RUN_ID = :pipeline_run_id
ORDER BY i.INTERVENTION_ID
