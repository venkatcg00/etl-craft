-- The average length in seconds of the finished runs of :pipeline_id.
SELECT AVG(EXTRACT(EPOCH FROM (END_DATE - START_DATE))) AS seconds
FROM AUD_PIPELINES_RUN_LOG
WHERE PIPELINE_ID = :pipeline_id AND STATUS IN ('SUCCESS', 'FAILED', 'SKIPPED')
  AND END_DATE IS NOT NULL
