-- The status of :pipeline_run_id.
SELECT STATUS AS status
FROM AUD_PIPELINES_RUN_LOG
WHERE PIPELINE_RUN_ID = :pipeline_run_id
