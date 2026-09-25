-- The average length in seconds of the finished runs of :task_id.
SELECT AVG((julianday(END_DATE) - julianday(START_DATE)) * 86400.0) AS seconds
FROM AUD_TASK_RUN_LOG
WHERE TASK_ID = :task_id AND STATUS IN ('SUCCESS', 'FAILED', 'SKIPPED')
  AND END_DATE IS NOT NULL
