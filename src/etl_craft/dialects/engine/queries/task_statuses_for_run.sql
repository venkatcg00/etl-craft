-- Every active task in :pipeline_id with its row under :pipeline_run_id, if any.
SELECT t.TASK_ID AS task_id, t.TASK_CODE AS task_code, t.HANDLER AS handler,
       COALESCE(l.STATUS, 'PENDING') AS status, l.ERROR_MESSAGE AS error_message,
       COALESCE(l.ATTEMPT_COUNT, 1) AS attempt_count,
       (SELECT CAST(i.ITEM_KEY AS bigint) FROM AUD_RUN_INTERVENTIONS i
        WHERE i.PIPELINE_RUN_ID=:pipeline_run_id AND i.TASK_ID=t.TASK_ID
        AND i.ACTION='PAUSE_APPLIED' AND i.TO_STATUS=l.STATUS AND l.ATTEMPT_COUNT=0 ORDER BY i.INTERVENTION_ID DESC LIMIT 1) AS standing_pause_id
FROM CFG_TASKS t
LEFT JOIN AUD_TASK_RUN_LOG l
  ON l.TASK_ID = t.TASK_ID AND l.PIPELINE_RUN_ID = :pipeline_run_id
WHERE t.PIPELINE_ID = :pipeline_id AND t.ACTIVE_FLAG = 'Y'
ORDER BY t.TASK_CODE
