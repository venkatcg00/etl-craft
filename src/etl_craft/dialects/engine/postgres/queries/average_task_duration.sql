-- The average length in seconds of the rows of :task_id that really ran: finished SUCCESS or
-- FAILED, longer than zero, and not under a backfill run.
SELECT AVG(EXTRACT(EPOCH FROM (t.END_DATE - t.START_DATE))) AS seconds
FROM AUD_TASK_RUN_LOG t
JOIN AUD_PIPELINES_RUN_LOG r ON r.PIPELINE_RUN_ID = t.PIPELINE_RUN_ID
WHERE t.TASK_ID = :task_id AND t.STATUS IN ('SUCCESS', 'FAILED') AND r.BACKFILL = 'N' AND r.TRIGGER_KIND <> 'STAND_IN'
  AND t.END_DATE > t.START_DATE
