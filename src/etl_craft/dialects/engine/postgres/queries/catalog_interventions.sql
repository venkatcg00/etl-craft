-- Interventions of shown runs only, in audit order.
WITH shown AS (
    SELECT r.PIPELINE_RUN_ID AS pipeline_run_id
    FROM CFG_PIPELINES p
    CROSS JOIN LATERAL (
        SELECT PIPELINE_RUN_ID FROM AUD_PIPELINES_RUN_LOG
        WHERE PIPELINE_ID = p.PIPELINE_ID ORDER BY PIPELINE_RUN_ID DESC LIMIT :limit
    ) r
    WHERE p.ACTIVE_FLAG = 'Y'
)
SELECT i.INTERVENTION_ID AS intervention_id, i.PIPELINE_RUN_ID AS pipeline_run_id,
       t.TASK_CODE AS task_code, i.ACTION AS action, i.FROM_STATUS AS from_status,
       i.TO_STATUS AS to_status, i.TARGET_COUNT AS target_count,
       i.PREVIOUS_MESSAGE AS previous_message, i.REASON AS reason,
       i.REQUESTED_BY AS requested_by, i.REQUESTED_AT AS requested_at
FROM AUD_RUN_INTERVENTIONS i
LEFT JOIN CFG_TASKS t ON t.TASK_ID = i.TASK_ID
WHERE i.PIPELINE_RUN_ID IN (SELECT pipeline_run_id FROM shown)
ORDER BY i.INTERVENTION_ID
