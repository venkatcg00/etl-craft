-- Every task row under :pipeline_run_id, with whether an operator marked it, and the downstream
-- that consumed it through a dependency on another pipeline, if any.
SELECT r.TASK_RUN_ID AS task_run_id, r.TASK_ID AS task_id, t.TASK_CODE AS task_code,
       r.STATUS AS status, r.ERROR_MESSAGE AS error_message,
       CASE WHEN EXISTS (
           SELECT 1 FROM AUD_RUN_INTERVENTIONS i
           WHERE i.PIPELINE_RUN_ID = r.PIPELINE_RUN_ID AND i.TASK_ID = r.TASK_ID
             AND i.ACTION IN ('MARK', 'NEW_RUN')
       ) THEN 1 ELSE 0 END AS marked,
       CASE WHEN EXISTS (
           SELECT 1 FROM AUD_BUSINESS_RULES_RUN_LOG b WHERE b.TASK_RUN_ID = r.TASK_RUN_ID
       ) THEN 1 ELSE 0 END AS has_rule_runs,
       (SELECT MIN(dp.PIPELINE_CODE || '.' || COALESCE(dt.TASK_CODE, '') || ' run ' ||
               c.PIPELINE_RUN_ID)
        FROM AUD_DEPENDENCY_CONSUMPTION c
        JOIN CFG_PIPELINES dp ON dp.PIPELINE_ID = c.PIPELINE_ID
        LEFT JOIN CFG_TASKS dt ON dt.TASK_ID = c.TASK_ID
        WHERE c.CONSUMED_TASK_RUN_ID = r.TASK_RUN_ID) AS consumed_by
FROM AUD_TASK_RUN_LOG r
JOIN CFG_TASKS t ON t.TASK_ID = r.TASK_ID
WHERE r.PIPELINE_RUN_ID = :pipeline_run_id
ORDER BY t.TASK_CODE
