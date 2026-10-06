-- The pipeline catalog describes the filter for a pipeline execution.
COMMENT ON TABLE CFG_PIPELINES IS 'One row per pipeline. PIPELINE_CODE is the key the command line uses; REFRESH_TYPE decides whether $$pipeline_run_id_filter reads the run or all rows.';
