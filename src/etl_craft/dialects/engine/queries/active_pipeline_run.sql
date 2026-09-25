-- The IN-PROGRESS run of :pipeline_id, if any.
SELECT PIPELINE_RUN_ID AS pipeline_run_id
FROM AUD_PIPELINES_RUN_LOG
WHERE PIPELINE_ID = :pipeline_id AND STATUS = 'IN-PROGRESS'
