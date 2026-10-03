-- The most recently started run of :pipeline_id that is not part of a backfill: the run a
-- gate on the pipeline waits for. Backfill runs never satisfy, block or delay a gate.
SELECT PIPELINE_RUN_ID AS pipeline_run_id, STATUS AS status, START_DATE AS start_date,
       END_DATE AS end_date
FROM AUD_PIPELINES_RUN_LOG
WHERE PIPELINE_ID = :pipeline_id AND BACKFILL = 'N'
ORDER BY START_DATE DESC, PIPELINE_RUN_ID DESC
LIMIT 1
