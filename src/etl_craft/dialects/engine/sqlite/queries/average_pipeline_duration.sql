-- The average length in seconds of the runs of :pipeline_id that really ran: finished SUCCESS or
-- FAILED, longer than zero, and not part of a backfill. Skipped and stand-in runs end at once
-- and would make a gate stop waiting far too soon.
SELECT AVG((julianday(END_DATE) - julianday(START_DATE)) * 86400.0) AS seconds
FROM AUD_PIPELINES_RUN_LOG
WHERE PIPELINE_ID = :pipeline_id AND STATUS IN ('SUCCESS', 'FAILED') AND BACKFILL = 'N'
  AND END_DATE > START_DATE
