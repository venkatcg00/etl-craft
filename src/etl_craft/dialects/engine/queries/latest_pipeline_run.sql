-- The most recently started run of :pipeline_id.
SELECT PIPELINE_RUN_ID AS pipeline_run_id, STATUS AS status, START_DATE AS start_date,
       END_DATE AS end_date
FROM AUD_PIPELINES_RUN_LOG
WHERE PIPELINE_ID = :pipeline_id
ORDER BY START_DATE DESC, PIPELINE_RUN_ID DESC
LIMIT 1
