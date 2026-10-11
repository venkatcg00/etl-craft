-- Typed change history: one append-only AUD_<table>_HISTORY per CFG_ table. The trigger
-- refresh that runs after this migration moves the AUD_METADATA_CHANGES rows into them and
-- replaces that table with a view over them.
CREATE SEQUENCE etl_craft_change_id;

-- Every version of a CFG_PIPELINES row, valid from VALID_FROM until VALID_TO (the far future while it
-- is the current one, CURRENT_FLAG 'Y'): its values after each insert and update, and its
-- values when deleted, with who changed it and the project migration that did.
CREATE TABLE AUD_CFG_PIPELINES_HISTORY (
    CHANGE_ID    BIGINT PRIMARY KEY,
    VALID_FROM   TIMESTAMPTZ NOT NULL,
    VALID_TO     TIMESTAMPTZ NOT NULL,
    CURRENT_FLAG VARCHAR(1) NOT NULL,
    OPERATION    VARCHAR NOT NULL,
    ACTOR        VARCHAR NOT NULL,
    ACTOR_KIND   VARCHAR NOT NULL,
    MIGRATION    VARCHAR,
    PIPELINE_ID BIGINT,
    PIPELINE_CODE VARCHAR,
    PIPELINE_NAME VARCHAR,
    DESCRIPTION VARCHAR,
    RUN_SCHEDULE VARCHAR,
    SLA_IN_HOURS NUMERIC,
    REFRESH_TYPE VARCHAR,
    ACTIVE_FLAG VARCHAR,
    PIPELINE_PARAMETERS JSONB,
    CREATED_BY VARCHAR,
    CREATE_DATE TIMESTAMPTZ,
    UPDATED_BY VARCHAR,
    UPDATED_DATE TIMESTAMPTZ,
    SCHEDULE_TIMEZONE VARCHAR,
    CATCHUP VARCHAR(1),
    MAX_CATCHUP_RUNS INT,
    OVERLAP_POLICY VARCHAR,
    SCHEDULE_START_DATE DATE,
    CONSTRAINT ck_aud_cfg_pipelines_history_operation CHECK (OPERATION IN ('INSERT','UPDATE','DELETE')),
    CONSTRAINT ck_aud_cfg_pipelines_history_current CHECK (CURRENT_FLAG IN ('Y','N'))
);
CREATE INDEX ix_aud_cfg_pipelines_history_row ON AUD_CFG_PIPELINES_HISTORY (PIPELINE_ID, CHANGE_ID);
CREATE INDEX ix_aud_cfg_pipelines_history_current ON AUD_CFG_PIPELINES_HISTORY (PIPELINE_ID) WHERE CURRENT_FLAG = 'Y';

-- Every version of a CFG_PIPELINE_DEPENDENCY row, valid from VALID_FROM until VALID_TO (the far future while it
-- is the current one, CURRENT_FLAG 'Y'): its values after each insert and update, and its
-- values when deleted, with who changed it and the project migration that did.
CREATE TABLE AUD_CFG_PIPELINE_DEPENDENCY_HISTORY (
    CHANGE_ID    BIGINT PRIMARY KEY,
    VALID_FROM   TIMESTAMPTZ NOT NULL,
    VALID_TO     TIMESTAMPTZ NOT NULL,
    CURRENT_FLAG VARCHAR(1) NOT NULL,
    OPERATION    VARCHAR NOT NULL,
    ACTOR        VARCHAR NOT NULL,
    ACTOR_KIND   VARCHAR NOT NULL,
    MIGRATION    VARCHAR,
    PIPELINE_DEPENDENCY_ID BIGINT,
    PIPELINE_ID BIGINT,
    DEPENDS_ON_PIPELINE_ID BIGINT,
    DEPENDENCY_TYPE VARCHAR,
    ACTIVE_FLAG VARCHAR,
    CREATED_BY VARCHAR,
    CREATE_DATE TIMESTAMPTZ,
    UPDATED_BY VARCHAR,
    UPDATED_DATE TIMESTAMPTZ,
    CONSUME_REPAIRS VARCHAR(1),
    CONSTRAINT ck_aud_cfg_pipeline_dependency_history_operation CHECK (OPERATION IN ('INSERT','UPDATE','DELETE')),
    CONSTRAINT ck_aud_cfg_pipeline_dependency_history_current CHECK (CURRENT_FLAG IN ('Y','N'))
);
CREATE INDEX ix_aud_cfg_pipeline_dependency_history_row ON AUD_CFG_PIPELINE_DEPENDENCY_HISTORY (PIPELINE_DEPENDENCY_ID, CHANGE_ID);
CREATE INDEX ix_aud_cfg_pipeline_dependency_history_current ON AUD_CFG_PIPELINE_DEPENDENCY_HISTORY (PIPELINE_DEPENDENCY_ID) WHERE CURRENT_FLAG = 'Y';

-- Every version of a CFG_TASKS row, valid from VALID_FROM until VALID_TO (the far future while it
-- is the current one, CURRENT_FLAG 'Y'): its values after each insert and update, and its
-- values when deleted, with who changed it and the project migration that did.
CREATE TABLE AUD_CFG_TASKS_HISTORY (
    CHANGE_ID    BIGINT PRIMARY KEY,
    VALID_FROM   TIMESTAMPTZ NOT NULL,
    VALID_TO     TIMESTAMPTZ NOT NULL,
    CURRENT_FLAG VARCHAR(1) NOT NULL,
    OPERATION    VARCHAR NOT NULL,
    ACTOR        VARCHAR NOT NULL,
    ACTOR_KIND   VARCHAR NOT NULL,
    MIGRATION    VARCHAR,
    TASK_ID BIGINT,
    TASK_CODE VARCHAR,
    TASK_TYPE VARCHAR,
    PIPELINE_ID BIGINT,
    HANDLER VARCHAR,
    RUN_CONDITION VARCHAR,
    RUN_CONDITION_COUNT INT,
    ACTIVE_FLAG VARCHAR,
    CREATED_BY VARCHAR,
    CREATE_DATE TIMESTAMPTZ,
    UPDATED_BY VARCHAR,
    UPDATED_DATE TIMESTAMPTZ,
    CONSTRAINT ck_aud_cfg_tasks_history_operation CHECK (OPERATION IN ('INSERT','UPDATE','DELETE')),
    CONSTRAINT ck_aud_cfg_tasks_history_current CHECK (CURRENT_FLAG IN ('Y','N'))
);
CREATE INDEX ix_aud_cfg_tasks_history_row ON AUD_CFG_TASKS_HISTORY (TASK_ID, CHANGE_ID);
CREATE INDEX ix_aud_cfg_tasks_history_current ON AUD_CFG_TASKS_HISTORY (TASK_ID) WHERE CURRENT_FLAG = 'Y';

-- Every version of a CFG_TASK_DEPENDENCY row, valid from VALID_FROM until VALID_TO (the far future while it
-- is the current one, CURRENT_FLAG 'Y'): its values after each insert and update, and its
-- values when deleted, with who changed it and the project migration that did.
CREATE TABLE AUD_CFG_TASK_DEPENDENCY_HISTORY (
    CHANGE_ID    BIGINT PRIMARY KEY,
    VALID_FROM   TIMESTAMPTZ NOT NULL,
    VALID_TO     TIMESTAMPTZ NOT NULL,
    CURRENT_FLAG VARCHAR(1) NOT NULL,
    OPERATION    VARCHAR NOT NULL,
    ACTOR        VARCHAR NOT NULL,
    ACTOR_KIND   VARCHAR NOT NULL,
    MIGRATION    VARCHAR,
    TASK_DEPENDENCY_ID BIGINT,
    PIPELINE_ID BIGINT,
    TASK_ID BIGINT,
    DEPENDS_ON_PIPELINE_ID BIGINT,
    DEPENDS_ON_TASK_ID BIGINT,
    DEPENDENCY_TYPE VARCHAR,
    ACTIVE_FLAG VARCHAR,
    CREATED_BY VARCHAR,
    CREATE_DATE TIMESTAMPTZ,
    UPDATED_BY VARCHAR,
    UPDATED_DATE TIMESTAMPTZ,
    CONSUME_REPAIRS VARCHAR(1),
    CONSTRAINT ck_aud_cfg_task_dependency_history_operation CHECK (OPERATION IN ('INSERT','UPDATE','DELETE')),
    CONSTRAINT ck_aud_cfg_task_dependency_history_current CHECK (CURRENT_FLAG IN ('Y','N'))
);
CREATE INDEX ix_aud_cfg_task_dependency_history_row ON AUD_CFG_TASK_DEPENDENCY_HISTORY (TASK_DEPENDENCY_ID, CHANGE_ID);
CREATE INDEX ix_aud_cfg_task_dependency_history_current ON AUD_CFG_TASK_DEPENDENCY_HISTORY (TASK_DEPENDENCY_ID) WHERE CURRENT_FLAG = 'Y';

-- Every version of a CFG_TASK_PARAMETERS row, valid from VALID_FROM until VALID_TO (the far future while it
-- is the current one, CURRENT_FLAG 'Y'): its values after each insert and update, and its
-- values when deleted, with who changed it and the project migration that did.
CREATE TABLE AUD_CFG_TASK_PARAMETERS_HISTORY (
    CHANGE_ID    BIGINT PRIMARY KEY,
    VALID_FROM   TIMESTAMPTZ NOT NULL,
    VALID_TO     TIMESTAMPTZ NOT NULL,
    CURRENT_FLAG VARCHAR(1) NOT NULL,
    OPERATION    VARCHAR NOT NULL,
    ACTOR        VARCHAR NOT NULL,
    ACTOR_KIND   VARCHAR NOT NULL,
    MIGRATION    VARCHAR,
    TASK_PARAMETER_ID BIGINT,
    TASK_ID BIGINT,
    PARAMETER_NAME VARCHAR,
    PARAMETER_VALUE VARCHAR,
    ACTIVE_FLAG VARCHAR,
    CREATED_BY VARCHAR,
    CREATE_DATE TIMESTAMPTZ,
    UPDATED_BY VARCHAR,
    UPDATED_DATE TIMESTAMPTZ,
    CONSTRAINT ck_aud_cfg_task_parameters_history_operation CHECK (OPERATION IN ('INSERT','UPDATE','DELETE')),
    CONSTRAINT ck_aud_cfg_task_parameters_history_current CHECK (CURRENT_FLAG IN ('Y','N'))
);
CREATE INDEX ix_aud_cfg_task_parameters_history_row ON AUD_CFG_TASK_PARAMETERS_HISTORY (TASK_PARAMETER_ID, CHANGE_ID);
CREATE INDEX ix_aud_cfg_task_parameters_history_current ON AUD_CFG_TASK_PARAMETERS_HISTORY (TASK_PARAMETER_ID) WHERE CURRENT_FLAG = 'Y';

-- Every version of a CFG_BUSINESS_RULES row, valid from VALID_FROM until VALID_TO (the far future while it
-- is the current one, CURRENT_FLAG 'Y'): its values after each insert and update, and its
-- values when deleted, with who changed it and the project migration that did.
CREATE TABLE AUD_CFG_BUSINESS_RULES_HISTORY (
    CHANGE_ID    BIGINT PRIMARY KEY,
    VALID_FROM   TIMESTAMPTZ NOT NULL,
    VALID_TO     TIMESTAMPTZ NOT NULL,
    CURRENT_FLAG VARCHAR(1) NOT NULL,
    OPERATION    VARCHAR NOT NULL,
    ACTOR        VARCHAR NOT NULL,
    ACTOR_KIND   VARCHAR NOT NULL,
    MIGRATION    VARCHAR,
    BUSINESS_RULE_ID BIGINT,
    BUSINESS_RULE_NAME VARCHAR,
    PIPELINE_ID BIGINT,
    TASK_ID BIGINT,
    BUSINESS_RULE_SQL VARCHAR,
    BUSINESS_RULE_TYPE VARCHAR,
    BUSINESS_RULE_KEY_COLUMN VARCHAR,
    TARGET_TABLE VARCHAR,
    SEQUENCE_NUMBER BIGINT,
    ACTIVE_FLAG VARCHAR,
    CREATED_BY VARCHAR,
    CREATE_DATE TIMESTAMPTZ,
    UPDATED_BY VARCHAR,
    UPDATED_DATE TIMESTAMPTZ,
    CONSTRAINT ck_aud_cfg_business_rules_history_operation CHECK (OPERATION IN ('INSERT','UPDATE','DELETE')),
    CONSTRAINT ck_aud_cfg_business_rules_history_current CHECK (CURRENT_FLAG IN ('Y','N'))
);
CREATE INDEX ix_aud_cfg_business_rules_history_row ON AUD_CFG_BUSINESS_RULES_HISTORY (BUSINESS_RULE_ID, CHANGE_ID);
CREATE INDEX ix_aud_cfg_business_rules_history_current ON AUD_CFG_BUSINESS_RULES_HISTORY (BUSINESS_RULE_ID) WHERE CURRENT_FLAG = 'Y';

CREATE OR REPLACE FUNCTION etl_craft_guard() RETURNS trigger AS $$
BEGIN
    IF COALESCE(current_setting('etl_craft.actor', true), '') = '' THEN
        RAISE EXCEPTION USING MESSAGE = upper(TG_TABLE_NAME) || ' is written only by etl-craft; change runs with etl-craft mark, cancel or run, and metadata with a project migration (etl-craft migrate)';
    END IF;
    IF COALESCE(current_setting('etl_craft.purpose', true), '') <> 'retention' AND TG_OP IN ('UPDATE','DELETE','TRUNCATE') THEN
        IF TG_TABLE_NAME IN ('aud_run_interventions','aud_actions','aud_dependency_consumption','aud_gate_decisions') THEN
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

CREATE OR REPLACE FUNCTION etl_craft_metadata_change() RETURNS trigger AS $$
DECLARE
    history text := quote_ident('aud_' || TG_TABLE_NAME || '_history');
    close_sql text := 'UPDATE ' || history || ' SET valid_to = now(), current_flag = ''N'' '
        || 'WHERE current_flag = ''Y'' AND ' || quote_ident(TG_ARGV[0]) || ' = ($1).' || quote_ident(TG_ARGV[0]);
    insert_sql text := 'INSERT INTO ' || history || ' SELECT * FROM jsonb_populate_record(NULL::' || history || ', $1)';
    stamp jsonb := jsonb_build_object(
        'change_id', nextval('etl_craft_change_id'), 'valid_from', now(), 'operation', TG_OP,
        'actor', current_setting('etl_craft.actor'), 'actor_kind', current_setting('etl_craft.actor_kind'),
        'migration', NULLIF(current_setting('etl_craft.migration', true), ''));
BEGIN
    IF TG_OP = 'UPDATE' AND to_jsonb(OLD) = to_jsonb(NEW) THEN RETURN NULL; END IF;
    IF TG_OP = 'DELETE' THEN
        EXECUTE close_sql USING OLD;
        EXECUTE insert_sql USING to_jsonb(OLD) || stamp
            || jsonb_build_object('valid_to', now(), 'current_flag', 'N');
    ELSE
        EXECUTE close_sql USING NEW;
        EXECUTE insert_sql USING to_jsonb(NEW) || stamp
            || jsonb_build_object('valid_to', '9999-12-31 23:59:59+00'::timestamptz, 'current_flag', 'Y');
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_actor_guard_aud_cfg_pipelines_history BEFORE INSERT OR UPDATE OR DELETE ON AUD_CFG_PIPELINES_HISTORY FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_aud_cfg_pipelines_history BEFORE TRUNCATE ON AUD_CFG_PIPELINES_HISTORY FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_actor_guard_aud_cfg_pipeline_dependency_history BEFORE INSERT OR UPDATE OR DELETE ON AUD_CFG_PIPELINE_DEPENDENCY_HISTORY FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_aud_cfg_pipeline_dependency_history BEFORE TRUNCATE ON AUD_CFG_PIPELINE_DEPENDENCY_HISTORY FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_actor_guard_aud_cfg_tasks_history BEFORE INSERT OR UPDATE OR DELETE ON AUD_CFG_TASKS_HISTORY FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_aud_cfg_tasks_history BEFORE TRUNCATE ON AUD_CFG_TASKS_HISTORY FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_actor_guard_aud_cfg_task_dependency_history BEFORE INSERT OR UPDATE OR DELETE ON AUD_CFG_TASK_DEPENDENCY_HISTORY FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_aud_cfg_task_dependency_history BEFORE TRUNCATE ON AUD_CFG_TASK_DEPENDENCY_HISTORY FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_actor_guard_aud_cfg_task_parameters_history BEFORE INSERT OR UPDATE OR DELETE ON AUD_CFG_TASK_PARAMETERS_HISTORY FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_aud_cfg_task_parameters_history BEFORE TRUNCATE ON AUD_CFG_TASK_PARAMETERS_HISTORY FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_actor_guard_aud_cfg_business_rules_history BEFORE INSERT OR UPDATE OR DELETE ON AUD_CFG_BUSINESS_RULES_HISTORY FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_aud_cfg_business_rules_history BEFORE TRUNCATE ON AUD_CFG_BUSINESS_RULES_HISTORY FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
