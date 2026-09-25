-- Stamp :now as the END_DATE of :pipeline_run_id, which a forced task rebinds to.
UPDATE AUD_PIPELINES_RUN_LOG
SET END_DATE = :now
WHERE PIPELINE_RUN_ID = :pipeline_run_id
