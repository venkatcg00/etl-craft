-- Attempts of runs started in the window, per pipeline: retries, failures, losses, the time
-- spent waiting to start apart from the time spent running, and rows written while measured.
SELECT r.PIPELINE_ID AS pipeline_id, COUNT(*) AS attempts,
       SUM(CASE WHEN a.ATTEMPT_NUMBER > 1 THEN 1 ELSE 0 END) AS retries,
       SUM(CASE WHEN a.STATUS = 'FAILED' THEN 1 ELSE 0 END) AS failed,
       SUM(CASE WHEN a.STATUS = 'LOST' THEN 1 ELSE 0 END) AS lost,
       COUNT(a.STARTED_AT) AS started,
       SUM(CASE WHEN a.STARTED_AT IS NOT NULL THEN ((julianday(a.STARTED_AT) - julianday(a.QUEUED_AT)) * 86400.0) END) AS wait_seconds,
       COUNT(a.ENDED_AT) AS ended,
       SUM(CASE WHEN a.ENDED_AT IS NOT NULL AND a.STARTED_AT IS NOT NULL THEN ((julianday(a.ENDED_AT) - julianday(a.STARTED_AT)) * 86400.0) END) AS run_seconds,
       COUNT(a.ROWS_WRITTEN) AS measured,
       SUM(a.ROWS_WRITTEN) AS rows_written,
       SUM(CASE WHEN a.ROWS_WRITTEN IS NOT NULL AND a.ENDED_AT IS NOT NULL THEN ((julianday(a.ENDED_AT) - julianday(a.STARTED_AT)) * 86400.0) END) AS measured_seconds
FROM CFG_PIPELINES p
JOIN AUD_PIPELINES_RUN_LOG r ON r.PIPELINE_ID = p.PIPELINE_ID
 AND r.START_DATE >= :window_start AND r.START_DATE < :window_end
JOIN AUD_TASK_RUN_LOG t ON t.PIPELINE_RUN_ID = r.PIPELINE_RUN_ID
JOIN AUD_TASK_ATTEMPTS a ON a.TASK_RUN_ID = t.TASK_RUN_ID
WHERE p.PROJECT_ID = :project_id
GROUP BY r.PIPELINE_ID
