ALTER TABLE CFG_TASKS DROP CONSTRAINT ck_tasks_handler;
ALTER TABLE CFG_TASKS ADD CONSTRAINT ck_tasks_handler CHECK (HANDLER IN ('PYTHON','SQL','BUSINESS_RULES','EMAIL_ALERT','EVENT_WAIT'));
ALTER TABLE AUD_GATE_WAITS ALTER COLUMN WAIT_UNTIL DROP NOT NULL;
ALTER TABLE AUD_GATE_WAITS ADD COLUMN WAIT_KIND VARCHAR NOT NULL DEFAULT 'DEPENDENCY' CHECK (WAIT_KIND IN ('DEPENDENCY','EVENT'));
ALTER TABLE AUD_GATE_WAITS ADD COLUMN EVENT_OBSERVATIONS TEXT;
ALTER TABLE AUD_GATE_WAITS ADD CONSTRAINT ck_gate_wait_deadline CHECK (WAIT_KIND = 'EVENT' OR WAIT_UNTIL IS NOT NULL);
DROP INDEX ux_gate_wait;
CREATE UNIQUE INDEX ux_gate_wait ON AUD_GATE_WAITS (PIPELINE_RUN_ID, COALESCE(TASK_ID,0), WAIT_KIND);
CREATE TABLE AUD_TASK_EVENTS (
    TASK_RUN_ID BIGINT PRIMARY KEY REFERENCES AUD_TASK_RUN_LOG(TASK_RUN_ID),
    EVIDENCE TEXT NOT NULL
);
CREATE TABLE AUD_EVENT_FILES (
    TASK_ID BIGINT NOT NULL REFERENCES CFG_TASKS(TASK_ID),
    FILE_PATH TEXT NOT NULL,
    TASK_RUN_ID BIGINT NOT NULL REFERENCES AUD_TASK_EVENTS(TASK_RUN_ID),
    CONSTRAINT pk_event_files PRIMARY KEY (TASK_ID, FILE_PATH)
);
CREATE TRIGGER trg_actor_guard_aud_task_events BEFORE INSERT OR UPDATE OR DELETE ON AUD_TASK_EVENTS FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_aud_task_events BEFORE TRUNCATE ON AUD_TASK_EVENTS FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_actor_guard_aud_event_files BEFORE INSERT OR UPDATE OR DELETE ON AUD_EVENT_FILES FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_aud_event_files BEFORE TRUNCATE ON AUD_EVENT_FILES FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();

CREATE OR REPLACE FUNCTION etl_craft_guard() RETURNS trigger AS $$
BEGIN
    IF COALESCE(current_setting('etl_craft.actor', true), '') = '' THEN
        RAISE EXCEPTION USING MESSAGE = upper(TG_TABLE_NAME) || ' is written only by etl-craft; change runs with etl-craft mark, cancel or run, and metadata with a project migration (etl-craft migrate)';
    END IF;
    IF COALESCE(current_setting('etl_craft.purpose', true), '') <> 'retention' AND TG_OP IN ('UPDATE','DELETE','TRUNCATE') THEN
        IF TG_TABLE_NAME IN ('aud_run_interventions','aud_actions','aud_dependency_consumption','aud_gate_decisions','aud_task_events','aud_event_files') THEN
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
            ELSIF OLD.STATUS NOT IN ('QUEUED','CLAIMED','RUNNING') THEN
                RAISE EXCEPTION 'AUD_TASK_ATTEMPTS terminal rows are immutable; queue a new attempt';
            END IF;
        END IF;
    END IF;
    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
