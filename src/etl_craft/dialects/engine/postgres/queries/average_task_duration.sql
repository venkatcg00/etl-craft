-- The average length in seconds of the finished runs of :task_id.
SELECT AVG(EXTRACT(EPOCH FROM (END_DATE - START_DATE))) AS seconds
FROM AUD_TASK_RUN_LOG
WHERE TASK_ID = :task_id AND STATUS IN ('SUCCESS', 'FAILED', 'SKIPPED')
  AND END_DATE IS NOT NULL
