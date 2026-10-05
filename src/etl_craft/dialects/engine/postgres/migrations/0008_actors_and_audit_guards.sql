-- Attribute actions without inventing identities for historical rows.
ALTER TABLE AUD_PIPELINES_RUN_LOG ADD COLUMN STARTED_BY VARCHAR;
ALTER TABLE AUD_PIPELINES_RUN_LOG ADD COLUMN STARTED_BY_KIND VARCHAR CONSTRAINT ck_aud_pipelines_run_log_started_by_kind CHECK (STARTED_BY_KIND IN ('HUMAN','SCHEDULE','ORCHESTRATOR','WORKER','SYSTEM'));
ALTER TABLE AUD_PIPELINES_RUN_LOG ADD COLUMN ENDED_BY VARCHAR;
ALTER TABLE AUD_PIPELINES_RUN_LOG ADD COLUMN ENDED_BY_KIND VARCHAR CONSTRAINT ck_aud_pipelines_run_log_ended_by_kind CHECK (ENDED_BY_KIND IN ('HUMAN','SCHEDULE','ORCHESTRATOR','WORKER','SYSTEM'));
ALTER TABLE AUD_TASK_ATTEMPTS ADD COLUMN REQUESTED_BY_KIND VARCHAR CONSTRAINT ck_aud_task_attempts_requested_by_kind CHECK (REQUESTED_BY_KIND IN ('HUMAN','SCHEDULE','ORCHESTRATOR','WORKER','SYSTEM'));
ALTER TABLE AUD_RUN_INTERVENTIONS ADD COLUMN REQUESTED_BY_KIND VARCHAR CONSTRAINT ck_aud_run_interventions_requested_by_kind CHECK (REQUESTED_BY_KIND IN ('HUMAN','SCHEDULE','ORCHESTRATOR','WORKER','SYSTEM'));
ALTER TABLE AUD_PIPELINE_PAUSES ADD COLUMN PAUSED_BY_KIND VARCHAR CONSTRAINT ck_aud_pipeline_pauses_paused_by_kind CHECK (PAUSED_BY_KIND IN ('HUMAN','SCHEDULE','ORCHESTRATOR','WORKER','SYSTEM'));
ALTER TABLE AUD_PIPELINE_PAUSES ADD COLUMN RESUMED_BY_KIND VARCHAR CONSTRAINT ck_aud_pipeline_pauses_resumed_by_kind CHECK (RESUMED_BY_KIND IN ('HUMAN','SCHEDULE','ORCHESTRATOR','WORKER','SYSTEM'));

-- An immutable record of a command request; run and attempt outcomes live in their own logs.
CREATE TABLE AUD_ACTIONS (
    ACTION_ID BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    STARTED_AT TIMESTAMPTZ NOT NULL DEFAULT now(),
    ENDED_AT TIMESTAMPTZ NOT NULL DEFAULT now(),
    ACTOR VARCHAR NOT NULL,
    ACTOR_KIND VARCHAR NOT NULL,
    HOST VARCHAR NOT NULL,
    COMMAND VARCHAR NOT NULL,
    ARGUMENTS JSONB NOT NULL,
    PIPELINE_ID BIGINT REFERENCES CFG_PIPELINES(PIPELINE_ID),
    PIPELINE_RUN_ID BIGINT REFERENCES AUD_PIPELINES_RUN_LOG(PIPELINE_RUN_ID),
    TASK_ID BIGINT REFERENCES CFG_TASKS(TASK_ID),
    OUTCOME VARCHAR NOT NULL,
    EXIT_CODE INT,
    CONSTRAINT ck_actions_actor_kind CHECK (ACTOR_KIND IN ('HUMAN','SCHEDULE','ORCHESTRATOR','WORKER','SYSTEM'))
);
CREATE INDEX ix_actions_pipeline ON AUD_ACTIONS (PIPELINE_ID, STARTED_AT);

-- Every metadata row change with its before/after values and the project migration that made it.
CREATE TABLE AUD_METADATA_CHANGES (
    CHANGE_ID BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    CHANGED_AT TIMESTAMPTZ NOT NULL DEFAULT now(),
    ACTOR VARCHAR NOT NULL,
    ACTOR_KIND VARCHAR NOT NULL,
    TABLE_NAME VARCHAR NOT NULL,
    ROW_KEY VARCHAR NOT NULL,
    OPERATION VARCHAR NOT NULL,
    BEFORE_JSON JSONB,
    AFTER_JSON JSONB,
    MIGRATION VARCHAR,
    CONSTRAINT ck_metadata_operation CHECK (OPERATION IN ('INSERT','UPDATE','DELETE')),
    CONSTRAINT ck_metadata_actor_kind CHECK (ACTOR_KIND IN ('HUMAN','SCHEDULE','ORCHESTRATOR','WORKER','SYSTEM'))
);
CREATE INDEX ix_metadata_changes_table ON AUD_METADATA_CHANGES (TABLE_NAME, CHANGED_AT);
CREATE OR REPLACE FUNCTION trg_set_audit_columns()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        NEW.CREATED_BY   := current_setting('etl_craft.actor');
        NEW.CREATE_DATE  := now();
        NEW.UPDATED_BY   := current_setting('etl_craft.actor');
        NEW.UPDATED_DATE := now();
    ELSIF TG_OP = 'UPDATE' THEN
        NEW.CREATED_BY   := OLD.CREATED_BY;
        NEW.CREATE_DATE  := OLD.CREATE_DATE;
        NEW.UPDATED_BY   := current_setting('etl_craft.actor');
        NEW.UPDATED_DATE := now();
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Require the transaction marker before changing protected rows or truncating a table.
CREATE OR REPLACE FUNCTION etl_craft_guard() RETURNS trigger AS $$
BEGIN
    IF COALESCE(current_setting('etl_craft.actor', true), '') = '' THEN
        RAISE EXCEPTION USING MESSAGE = upper(TG_TABLE_NAME) || ' is written only by etl-craft; change runs with etl-craft mark, cancel or run, and metadata with a project migration (etl-craft migrate)';
    END IF;
    IF COALESCE(current_setting('etl_craft.purpose', true), '') <> 'retention' AND TG_OP IN ('UPDATE','DELETE','TRUNCATE') THEN
        IF TG_TABLE_NAME IN ('aud_run_interventions','aud_actions','aud_metadata_changes','aud_dependency_consumption','aud_gate_decisions') THEN
            RAISE EXCEPTION USING MESSAGE = upper(TG_TABLE_NAME) || ' is append-only; use etl-craft retention';
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

CREATE OR REPLACE FUNCTION etl_craft_metadata_change() RETURNS trigger AS $$
DECLARE
    before_row jsonb;
    after_row jsonb;
    key_value text;
BEGIN
    IF TG_OP <> 'INSERT' THEN before_row := to_jsonb(OLD); END IF;
    IF TG_OP <> 'DELETE' THEN after_row := to_jsonb(NEW); END IF;
    IF TG_OP = 'UPDATE' AND before_row = after_row THEN RETURN NEW; END IF;
    key_value := COALESCE(after_row, before_row)->>TG_ARGV[0];
    INSERT INTO AUD_METADATA_CHANGES (ACTOR, ACTOR_KIND, TABLE_NAME, ROW_KEY, OPERATION, BEFORE_JSON, AFTER_JSON, MIGRATION)
    VALUES (current_setting('etl_craft.actor'), current_setting('etl_craft.actor_kind'), upper(TG_TABLE_NAME), key_value, TG_OP, before_row, after_row, NULLIF(current_setting('etl_craft.migration', true), ''));
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER trg_actor_guard_cfg_pipelines BEFORE INSERT OR UPDATE OR DELETE ON CFG_PIPELINES FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_cfg_pipelines BEFORE TRUNCATE ON CFG_PIPELINES FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_changes_cfg_pipelines AFTER INSERT OR UPDATE OR DELETE ON CFG_PIPELINES FOR EACH ROW EXECUTE FUNCTION etl_craft_metadata_change('pipeline_id');
CREATE TRIGGER trg_actor_guard_cfg_pipeline_dependency BEFORE INSERT OR UPDATE OR DELETE ON CFG_PIPELINE_DEPENDENCY FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_cfg_pipeline_dependency BEFORE TRUNCATE ON CFG_PIPELINE_DEPENDENCY FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_changes_cfg_pipeline_dependency AFTER INSERT OR UPDATE OR DELETE ON CFG_PIPELINE_DEPENDENCY FOR EACH ROW EXECUTE FUNCTION etl_craft_metadata_change('pipeline_dependency_id');
CREATE TRIGGER trg_actor_guard_cfg_tasks BEFORE INSERT OR UPDATE OR DELETE ON CFG_TASKS FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_cfg_tasks BEFORE TRUNCATE ON CFG_TASKS FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_changes_cfg_tasks AFTER INSERT OR UPDATE OR DELETE ON CFG_TASKS FOR EACH ROW EXECUTE FUNCTION etl_craft_metadata_change('task_id');
CREATE TRIGGER trg_actor_guard_cfg_task_dependency BEFORE INSERT OR UPDATE OR DELETE ON CFG_TASK_DEPENDENCY FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_cfg_task_dependency BEFORE TRUNCATE ON CFG_TASK_DEPENDENCY FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_changes_cfg_task_dependency AFTER INSERT OR UPDATE OR DELETE ON CFG_TASK_DEPENDENCY FOR EACH ROW EXECUTE FUNCTION etl_craft_metadata_change('task_dependency_id');
CREATE TRIGGER trg_actor_guard_cfg_task_parameters BEFORE INSERT OR UPDATE OR DELETE ON CFG_TASK_PARAMETERS FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_cfg_task_parameters BEFORE TRUNCATE ON CFG_TASK_PARAMETERS FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_changes_cfg_task_parameters AFTER INSERT OR UPDATE OR DELETE ON CFG_TASK_PARAMETERS FOR EACH ROW EXECUTE FUNCTION etl_craft_metadata_change('task_parameter_id');
CREATE TRIGGER trg_actor_guard_cfg_business_rules BEFORE INSERT OR UPDATE OR DELETE ON CFG_BUSINESS_RULES FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_cfg_business_rules BEFORE TRUNCATE ON CFG_BUSINESS_RULES FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_changes_cfg_business_rules AFTER INSERT OR UPDATE OR DELETE ON CFG_BUSINESS_RULES FOR EACH ROW EXECUTE FUNCTION etl_craft_metadata_change('business_rule_id');
CREATE TRIGGER trg_actor_guard_aud_pipelines_run_log BEFORE INSERT OR UPDATE OR DELETE ON AUD_PIPELINES_RUN_LOG FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_aud_pipelines_run_log BEFORE TRUNCATE ON AUD_PIPELINES_RUN_LOG FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_actor_guard_aud_task_run_log BEFORE INSERT OR UPDATE OR DELETE ON AUD_TASK_RUN_LOG FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_aud_task_run_log BEFORE TRUNCATE ON AUD_TASK_RUN_LOG FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_actor_guard_aud_run_interventions BEFORE INSERT OR UPDATE OR DELETE ON AUD_RUN_INTERVENTIONS FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_aud_run_interventions BEFORE TRUNCATE ON AUD_RUN_INTERVENTIONS FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_actor_guard_aud_pipeline_pauses BEFORE INSERT OR UPDATE OR DELETE ON AUD_PIPELINE_PAUSES FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_aud_pipeline_pauses BEFORE TRUNCATE ON AUD_PIPELINE_PAUSES FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_actor_guard_aud_business_rules_run_log BEFORE INSERT OR UPDATE OR DELETE ON AUD_BUSINESS_RULES_RUN_LOG FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_aud_business_rules_run_log BEFORE TRUNCATE ON AUD_BUSINESS_RULES_RUN_LOG FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_actor_guard_aud_business_rules_results BEFORE INSERT OR UPDATE OR DELETE ON AUD_BUSINESS_RULES_RESULTS FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_aud_business_rules_results BEFORE TRUNCATE ON AUD_BUSINESS_RULES_RESULTS FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_actor_guard_aud_task_offset_tracker BEFORE INSERT OR UPDATE OR DELETE ON AUD_TASK_OFFSET_TRACKER FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_aud_task_offset_tracker BEFORE TRUNCATE ON AUD_TASK_OFFSET_TRACKER FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_actor_guard_aud_column_lineage BEFORE INSERT OR UPDATE OR DELETE ON AUD_COLUMN_LINEAGE FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_aud_column_lineage BEFORE TRUNCATE ON AUD_COLUMN_LINEAGE FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_actor_guard_aud_docs_publication BEFORE INSERT OR UPDATE OR DELETE ON AUD_DOCS_PUBLICATION FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_aud_docs_publication BEFORE TRUNCATE ON AUD_DOCS_PUBLICATION FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_actor_guard_aud_task_documentation BEFORE INSERT OR UPDATE OR DELETE ON AUD_TASK_DOCUMENTATION FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_aud_task_documentation BEFORE TRUNCATE ON AUD_TASK_DOCUMENTATION FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_actor_guard_aud_dependency_consumption BEFORE INSERT OR UPDATE OR DELETE ON AUD_DEPENDENCY_CONSUMPTION FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_aud_dependency_consumption BEFORE TRUNCATE ON AUD_DEPENDENCY_CONSUMPTION FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_actor_guard_aud_task_attempts BEFORE INSERT OR UPDATE OR DELETE ON AUD_TASK_ATTEMPTS FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_aud_task_attempts BEFORE TRUNCATE ON AUD_TASK_ATTEMPTS FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_actor_guard_aud_gate_decisions BEFORE INSERT OR UPDATE OR DELETE ON AUD_GATE_DECISIONS FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_aud_gate_decisions BEFORE TRUNCATE ON AUD_GATE_DECISIONS FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_actor_guard_aud_actions BEFORE INSERT OR UPDATE OR DELETE ON AUD_ACTIONS FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_aud_actions BEFORE TRUNCATE ON AUD_ACTIONS FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_actor_guard_aud_metadata_changes BEFORE INSERT OR UPDATE OR DELETE ON AUD_METADATA_CHANGES FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_aud_metadata_changes BEFORE TRUNCATE ON AUD_METADATA_CHANGES FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();

-- Fill attribution when the engine inserts an action-bearing row.
CREATE OR REPLACE FUNCTION etl_craft_attribute() RETURNS trigger AS $$
BEGIN
    IF TG_TABLE_NAME = 'aud_pipelines_run_log' THEN
        IF TG_OP = 'INSERT' THEN
            NEW.STARTED_BY := COALESCE(NEW.STARTED_BY, current_setting('etl_craft.actor'));
            NEW.STARTED_BY_KIND := COALESCE(NEW.STARTED_BY_KIND, current_setting('etl_craft.actor_kind'));
        END IF;
        IF NEW.END_DATE IS NOT NULL THEN
            NEW.ENDED_BY := COALESCE(NEW.ENDED_BY, current_setting('etl_craft.actor'));
            NEW.ENDED_BY_KIND := COALESCE(NEW.ENDED_BY_KIND, current_setting('etl_craft.actor_kind'));
        END IF;
    ELSIF TG_TABLE_NAME IN ('aud_task_attempts','aud_run_interventions') THEN
        NEW.REQUESTED_BY := COALESCE(NEW.REQUESTED_BY, current_setting('etl_craft.actor'));
        NEW.REQUESTED_BY_KIND := COALESCE(NEW.REQUESTED_BY_KIND, current_setting('etl_craft.actor_kind'));
    ELSIF TG_TABLE_NAME = 'aud_pipeline_pauses' THEN
        NEW.PAUSED_BY_KIND := COALESCE(NEW.PAUSED_BY_KIND, current_setting('etl_craft.actor_kind'));
        IF NEW.RESUMED_AT IS NOT NULL THEN
            NEW.RESUMED_BY_KIND := COALESCE(NEW.RESUMED_BY_KIND, current_setting('etl_craft.actor_kind'));
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER trg_attribute_aud_pipelines_run_log BEFORE INSERT OR UPDATE ON AUD_PIPELINES_RUN_LOG FOR EACH ROW EXECUTE FUNCTION etl_craft_attribute();
CREATE TRIGGER trg_attribute_aud_task_attempts BEFORE INSERT OR UPDATE ON AUD_TASK_ATTEMPTS FOR EACH ROW EXECUTE FUNCTION etl_craft_attribute();
CREATE TRIGGER trg_attribute_aud_run_interventions BEFORE INSERT OR UPDATE ON AUD_RUN_INTERVENTIONS FOR EACH ROW EXECUTE FUNCTION etl_craft_attribute();
CREATE TRIGGER trg_attribute_aud_pipeline_pauses BEFORE INSERT OR UPDATE ON AUD_PIPELINE_PAUSES FOR EACH ROW EXECUTE FUNCTION etl_craft_attribute();
ALTER TABLE AUD_PIPELINES_RUN_LOG ALTER COLUMN STARTED_BY SET DEFAULT current_setting('etl_craft.actor', true);
ALTER TABLE AUD_PIPELINES_RUN_LOG ALTER COLUMN STARTED_BY_KIND SET DEFAULT current_setting('etl_craft.actor_kind', true);
ALTER TABLE AUD_TASK_ATTEMPTS ALTER COLUMN REQUESTED_BY SET DEFAULT current_setting('etl_craft.actor', true);
ALTER TABLE AUD_TASK_ATTEMPTS ALTER COLUMN REQUESTED_BY_KIND SET DEFAULT current_setting('etl_craft.actor_kind', true);
ALTER TABLE AUD_RUN_INTERVENTIONS ALTER COLUMN REQUESTED_BY_KIND SET DEFAULT current_setting('etl_craft.actor_kind', true);
ALTER TABLE AUD_PIPELINE_PAUSES ALTER COLUMN PAUSED_BY_KIND SET DEFAULT current_setting('etl_craft.actor_kind', true);
