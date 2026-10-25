-- A terminal outcome stays immutable when its external log is acknowledged.
CREATE OR REPLACE FUNCTION etl_craft_guard() RETURNS trigger AS $$
BEGIN
    IF COALESCE(current_setting('etl_craft.actor', true), '') = '' THEN
        RAISE EXCEPTION USING MESSAGE = upper(TG_TABLE_NAME) || ' is written only by etl-craft; change runs with etl-craft mark, cancel or run, and metadata with a project migration (etl-craft migrate)';
    END IF;
    IF COALESCE(current_setting('etl_craft.purpose', true), '') <> 'retention' AND TG_OP IN ('UPDATE','DELETE','TRUNCATE') THEN
        IF TG_TABLE_NAME IN ('aud_run_interventions','aud_actions','aud_dependency_consumption','aud_gate_decisions','aud_task_events','aud_event_files','aud_event_keys','aud_task_items','aud_project_bundles') THEN
            RAISE EXCEPTION USING MESSAGE = upper(TG_TABLE_NAME) || ' is append-only; use etl-craft retention';
        END IF;
        IF TG_TABLE_NAME ~ '^aud_cfg_.*_history$' THEN
            IF TG_OP <> 'UPDATE' THEN
                RAISE EXCEPTION USING MESSAGE = upper(TG_TABLE_NAME) || ' is append-only; use etl-craft retention';
            END IF;
            IF NOT (OLD.current_flag = 'Y' AND NEW.current_flag = 'N'
                    AND to_jsonb(NEW) - ARRAY['valid_to','current_flag'] = to_jsonb(OLD) - ARRAY['valid_to','current_flag']) THEN
                RAISE EXCEPTION USING MESSAGE = upper(TG_TABLE_NAME) || ' is append-only: a version changes only when the next one closes it';
            END IF;
        END IF;
        IF TG_TABLE_NAME = 'aud_task_attempts' THEN
            IF TG_OP = 'TRUNCATE' THEN
                RAISE EXCEPTION 'AUD_TASK_ATTEMPTS terminal rows are immutable; queue a new attempt';
            ELSIF OLD.STATUS NOT IN ('QUEUED','CLAIMED','RUNNING') AND NOT (
                TG_OP = 'UPDATE'
                AND (OLD.LOG_PATH IS NULL AND NEW.LOG_PATH IS NOT NULL)
                AND to_jsonb(NEW) - 'log_path' = to_jsonb(OLD) - 'log_path'
            ) THEN
                RAISE EXCEPTION 'AUD_TASK_ATTEMPTS terminal rows are immutable; queue a new attempt';
            END IF;
        END IF;
    END IF;
    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE INDEX ix_attempt_log_pending ON AUD_TASK_ATTEMPTS (ATTEMPT_ID)
WHERE LOG_PATH IS NULL AND LOG_REMOVED_AT IS NULL
AND STATUS NOT IN ('QUEUED','CLAIMED','RUNNING');
