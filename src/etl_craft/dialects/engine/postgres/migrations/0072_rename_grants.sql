-- Defer the code reference while the pipeline and its grants change in one transaction.
DO $$
DECLARE grant_fk text;
BEGIN
    FOR grant_fk IN SELECT conname FROM pg_constraint
        WHERE conrelid='cfg_pipeline_grants'::regclass
          AND confrelid='cfg_pipelines'::regclass AND contype='f'
    LOOP
        EXECUTE 'ALTER TABLE CFG_PIPELINE_GRANTS ALTER CONSTRAINT ' || quote_ident(grant_fk)
            || ' DEFERRABLE INITIALLY DEFERRED';
    END LOOP;
END $$;

CREATE INDEX ix_pipeline_rename_history ON AUD_CFG_PIPELINES_HISTORY (PROJECT_ID,PIPELINE_ID,CHANGE_ID)
    WHERE MIGRATION='rename';
CREATE INDEX ix_task_rename_history ON AUD_CFG_TASKS_HISTORY (PIPELINE_ID,TASK_ID,CHANGE_ID)
    WHERE MIGRATION='rename';
