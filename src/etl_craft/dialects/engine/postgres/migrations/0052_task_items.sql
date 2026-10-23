-- etl-craft: rebuild-metadata
-- Immutable item starts, results and operator decisions, scoped to an exact task run.
CREATE TABLE AUD_TASK_ITEMS (
    ITEM_RECORD_ID BIGSERIAL PRIMARY KEY,
    REPORT_ID VARCHAR(36) NOT NULL UNIQUE,
    TASK_RUN_ID BIGINT NOT NULL REFERENCES AUD_TASK_RUN_LOG(TASK_RUN_ID),
    ATTEMPT_ID BIGINT NOT NULL REFERENCES AUD_TASK_ATTEMPTS(ATTEMPT_ID),
    ITEM_KEY VARCHAR(512) NOT NULL CHECK (length(ITEM_KEY) BETWEEN 1 AND 512),
    ITEM_ATTEMPT_NUMBER INTEGER NOT NULL CHECK (ITEM_ATTEMPT_NUMBER >= 0),
    STATUS VARCHAR(24) NOT NULL CHECK (STATUS IN ('QUEUED','RUNNING','SUCCESS','FAILED','SKIPPED','CANCEL_REQUESTED','CANCELLED')),
    STARTED_AT TIMESTAMPTZ,
    ENDED_AT TIMESTAMPTZ,
    ROW_COUNT BIGINT NOT NULL DEFAULT 0 CHECK (ROW_COUNT >= 0),
    ERROR_MESSAGE VARCHAR,
    CREATED_BY VARCHAR NOT NULL,
    CREATED_AT TIMESTAMPTZ NOT NULL
);
CREATE INDEX ix_task_items_latest ON AUD_TASK_ITEMS (TASK_RUN_ID, ITEM_KEY, ITEM_RECORD_ID DESC);
CREATE INDEX ix_task_items_attempt ON AUD_TASK_ITEMS (ATTEMPT_ID, ITEM_KEY, ITEM_RECORD_ID DESC);
ALTER TABLE AUD_RUN_INTERVENTIONS ADD COLUMN ITEM_KEY VARCHAR(512);
CREATE TRIGGER trg_actor_guard_aud_task_items BEFORE INSERT OR UPDATE OR DELETE ON AUD_TASK_ITEMS FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_aud_task_items BEFORE TRUNCATE ON AUD_TASK_ITEMS FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE OR REPLACE FUNCTION etl_craft_guard() RETURNS trigger AS $$
BEGIN
    IF COALESCE(current_setting('etl_craft.actor', true), '') = '' THEN
        RAISE EXCEPTION USING MESSAGE = upper(TG_TABLE_NAME) || ' is written only by etl-craft; change runs with etl-craft mark, cancel or run, and metadata with a project migration (etl-craft migrate)';
    END IF;
    IF COALESCE(current_setting('etl_craft.purpose', true), '') <> 'retention' AND TG_OP IN ('UPDATE','DELETE','TRUNCATE') THEN
        IF TG_TABLE_NAME IN ('aud_run_interventions','aud_actions','aud_dependency_consumption','aud_gate_decisions','aud_task_events','aud_event_files','aud_event_keys','aud_task_items') THEN
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
