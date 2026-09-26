-- Every change an operator made to the runs of :pipeline_id from :first_run_id on, oldest first.
SELECT i.INTERVENTION_ID AS intervention_id, i.PIPELINE_RUN_ID AS pipeline_run_id,
       t.TASK_CODE AS task_code, i.ACTION AS action, i.FROM_STATUS AS from_status,
       i.TO_STATUS AS to_status, i.TARGET_COUNT AS target_count,
       i.PREVIOUS_MESSAGE AS previous_message, i.REASON AS reason,
       i.REQUESTED_BY AS requested_by, i.REQUESTED_AT AS requested_at
FROM AUD_RUN_INTERVENTIONS i
LEFT JOIN CFG_TASKS t ON t.TASK_ID = i.TASK_ID
WHERE i.PIPELINE_ID = :pipeline_id AND i.PIPELINE_RUN_ID >= :first_run_id
ORDER BY i.INTERVENTION_ID
