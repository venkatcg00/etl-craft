-- Bound catalog history reads by the definition and descending execution identity.
DROP INDEX ix_pipeline_run_pipeline;
CREATE INDEX ix_catalog_pipeline_runs ON AUD_PIPELINES_RUN_LOG (PIPELINE_ID, PIPELINE_RUN_ID DESC);
CREATE INDEX ix_catalog_task_runs ON AUD_TASK_RUN_LOG (TASK_ID, TASK_RUN_ID DESC);
