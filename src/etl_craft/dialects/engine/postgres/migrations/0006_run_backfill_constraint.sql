-- Keep the backfill constraint's name consistent with a fresh Engine DB.
DO $constraint$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'AUD_PIPELINES_RUN_LOG'::regclass
          AND conname = 'aud_pipelines_run_log_backfill_check'
    ) THEN
        ALTER TABLE AUD_PIPELINES_RUN_LOG
            RENAME CONSTRAINT aud_pipelines_run_log_backfill_check TO ck_pipeline_run_backfill;
    END IF;
END;
$constraint$;
