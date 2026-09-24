-- 0005_pipeline_run_sla_status.sql
--
-- Orchestration.Enforce_sla: AUD_PIPELINES_RUN_LOG.SLA_STATUS, MET or BREACHED
-- against the pipeline's CFG_PIPELINES.SLA_IN_HOURS, written when a run
-- finishes while the setting is on. Mirrors the same change made directly in
-- schema.sql -- see its own POST-SIGNOFF CHANGES block. NULL means not judged:
-- enforcement was off, the pipeline has no SLA, or the run predates this.
--
-- Re-runnable: the column is added only if absent, and the constraint is
-- dropped before it is added (Postgres has no ADD CONSTRAINT IF NOT EXISTS).

ALTER TABLE AUD_PIPELINES_RUN_LOG ADD COLUMN IF NOT EXISTS SLA_STATUS VARCHAR(8);
ALTER TABLE AUD_PIPELINES_RUN_LOG DROP CONSTRAINT IF EXISTS ck_pipeline_run_sla_status;
ALTER TABLE AUD_PIPELINES_RUN_LOG ADD CONSTRAINT ck_pipeline_run_sla_status
    CHECK (SLA_STATUS IN ('MET', 'BREACHED'));
