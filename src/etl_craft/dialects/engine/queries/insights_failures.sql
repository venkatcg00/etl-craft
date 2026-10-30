-- Failed and lost attempts of runs started in the window, per pipeline, task and failure class.
SELECT r.PIPELINE_ID AS pipeline_id, k.TASK_CODE AS task_code, a.STATUS AS status,
       a.FAILURE_CLASS AS failure_class, COUNT(*) AS attempts, MAX(a.ATTEMPT_ID) AS latest_attempt
FROM CFG_PIPELINES p
JOIN AUD_PIPELINES_RUN_LOG r ON r.PIPELINE_ID = p.PIPELINE_ID
 AND r.START_DATE >= :window_start AND r.START_DATE < :window_end
JOIN AUD_TASK_RUN_LOG t ON t.PIPELINE_RUN_ID = r.PIPELINE_RUN_ID
JOIN CFG_TASKS k ON k.TASK_ID = t.TASK_ID
JOIN AUD_TASK_ATTEMPTS a ON a.TASK_RUN_ID = t.TASK_RUN_ID AND a.STATUS IN ('FAILED', 'LOST')
WHERE p.PROJECT_ID = :project_id
GROUP BY r.PIPELINE_ID, k.TASK_CODE, a.STATUS, a.FAILURE_CLASS
