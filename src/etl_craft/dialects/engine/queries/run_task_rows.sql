-- Every task row under :pipeline_run_id, with whether an operator marked it.
SELECT r.TASK_RUN_ID AS task_run_id, r.TASK_ID AS task_id, t.TASK_CODE AS task_code,
       r.STATUS AS status, r.ERROR_MESSAGE AS error_message,
       CASE WHEN EXISTS (
           SELECT 1 FROM AUD_RUN_INTERVENTIONS i
           WHERE i.PIPELINE_RUN_ID = r.PIPELINE_RUN_ID AND i.TASK_ID = r.TASK_ID
             AND i.ACTION IN ('MARK', 'NEW_RUN')
       ) THEN 1 ELSE 0 END AS marked,
       CASE WHEN EXISTS (
           SELECT 1 FROM AUD_BUSINESS_RULES_RUN_LOG b WHERE b.TASK_RUN_ID = r.TASK_RUN_ID
       ) THEN 1 ELSE 0 END AS has_rule_runs
FROM AUD_TASK_RUN_LOG r
JOIN CFG_TASKS t ON t.TASK_ID = r.TASK_ID
WHERE r.PIPELINE_RUN_ID = :pipeline_run_id
ORDER BY t.TASK_CODE
