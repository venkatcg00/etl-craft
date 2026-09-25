-- Every active task in :pipeline_id with its row under :pipeline_run_id, if any.
SELECT t.TASK_ID AS task_id, t.TASK_CODE AS task_code, t.HANDLER AS handler,
       l.STATUS AS status, l.ERROR_MESSAGE AS error_message,
       COALESCE(l.ATTEMPT_COUNT, 1) AS attempt_count
FROM CFG_TASKS t
LEFT JOIN AUD_TASK_RUN_LOG l
  ON l.TASK_ID = t.TASK_ID AND l.PIPELINE_RUN_ID = :pipeline_run_id
WHERE t.PIPELINE_ID = :pipeline_id AND t.ACTIVE_FLAG = 'Y'
ORDER BY t.TASK_CODE
