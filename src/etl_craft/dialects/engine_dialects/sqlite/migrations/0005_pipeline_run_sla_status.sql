-- 0005_pipeline_run_sla_status.sql
--
-- Orchestration.Enforce_sla: AUD_PIPELINES_RUN_LOG.SLA_STATUS, MET or BREACHED
-- against the pipeline's CFG_PIPELINES.SLA_IN_HOURS. The SQLite half of the
-- pair -- ../../postgres/migrations/0005_pipeline_run_sla_status.sql is the
-- other -- and reflected in this dialect's own schema.sql. NULL means not
-- judged: enforcement was off, the pipeline has no SLA, or the run predates it.

ALTER TABLE AUD_PIPELINES_RUN_LOG ADD COLUMN SLA_STATUS VARCHAR(8)
    CONSTRAINT ck_pipeline_run_sla_status CHECK (SLA_STATUS IN ('MET', 'BREACHED'));
