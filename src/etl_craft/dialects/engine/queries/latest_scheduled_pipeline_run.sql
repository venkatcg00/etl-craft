-- Prefer the active upstream; later queued or skipped ticks never hide work still running.
-- Queued and backfill runs do not block a gate.
SELECT PIPELINE_RUN_ID AS run_id, STATUS AS status, START_DATE AS start_date
FROM AUD_PIPELINES_RUN_LOG
WHERE PIPELINE_ID = :pipeline_id AND BACKFILL = 'N' AND STATUS <> 'QUEUED'
ORDER BY CASE WHEN STATUS='IN-PROGRESS' THEN 0 ELSE 1 END, START_DATE DESC, PIPELINE_RUN_ID DESC
LIMIT 1
