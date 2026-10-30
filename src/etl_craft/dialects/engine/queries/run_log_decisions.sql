-- Recorded admission decisions for the run, with the task whose attempt asked.
SELECT d.DECISION_ID AS decision_id, t.TASK_CODE AS task_code, d.RESULT AS result,
       d.REASON AS reason, d.DECIDED_AT AS decided_at,
       d.SELECTED_PIPELINE_RUN_ID AS selected_pipeline_run_id
FROM AUD_GATE_DECISIONS d
LEFT JOIN AUD_TASK_ATTEMPTS a ON a.ATTEMPT_ID = d.ATTEMPT_ID
LEFT JOIN AUD_TASK_RUN_LOG tr ON tr.TASK_RUN_ID = a.TASK_RUN_ID
LEFT JOIN CFG_TASKS t ON t.TASK_ID = tr.TASK_ID
WHERE d.PIPELINE_RUN_ID = :pipeline_run_id
ORDER BY d.DECISION_ID
