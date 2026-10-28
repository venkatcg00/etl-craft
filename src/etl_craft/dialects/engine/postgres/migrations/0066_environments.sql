-- etl-craft: rebuild-metadata
ALTER TABLE CFG_PROJECTS ADD COLUMN ENVIRONMENT_TYPE VARCHAR(16) NOT NULL DEFAULT 'CUSTOM'
    CHECK (ENVIRONMENT_TYPE IN ('DEV','SIT','UAT','PROD','SANDBOX','CUSTOM'));
ALTER TABLE CFG_PROJECTS ADD COLUMN ENVIRONMENT_NAME VARCHAR;
ALTER TABLE CFG_PROJECTS ADD COLUMN GOVERNANCE VARCHAR(16) NOT NULL DEFAULT 'STRICT'
    CHECK (GOVERNANCE IN ('STRICT','RELAXED') AND NOT (ENVIRONMENT_TYPE='PROD' AND GOVERNANCE='RELAXED'));

CREATE OR REPLACE FUNCTION etl_craft_config_project(table_name text, row_data jsonb)
RETURNS bigint LANGUAGE plpgsql AS $$
BEGIN
    CASE
        WHEN upper(table_name) IN ('CFG_PIPELINES','CFG_TABLES') THEN
            RETURN (row_data->>'project_id')::bigint;
        WHEN upper(table_name) IN ('CFG_TASKS','CFG_PIPELINE_DEPENDENCY','CFG_TASK_DEPENDENCY','CFG_BUSINESS_RULES') THEN
            RETURN (SELECT p.PROJECT_ID FROM CFG_PIPELINES p WHERE p.PIPELINE_ID=(row_data->>'pipeline_id')::bigint);
        WHEN upper(table_name)='CFG_TASK_PARAMETERS' THEN
            RETURN (SELECT p.PROJECT_ID FROM CFG_TASKS t JOIN CFG_PIPELINES p ON p.PIPELINE_ID=t.PIPELINE_ID WHERE t.TASK_ID=(row_data->>'task_id')::bigint);
        WHEN upper(table_name)='CFG_COLUMNS' THEN
            RETURN (SELECT t.PROJECT_ID FROM CFG_TABLES t WHERE t.TABLE_ID=(row_data->>'table_id')::bigint);
        ELSE RETURN NULL;
    END CASE;
END; $$;

CREATE OR REPLACE FUNCTION etl_craft_guard() RETURNS trigger AS $$
DECLARE
    row_project bigint;
    old_project bigint;
    primary_key text;
    policy text;
BEGIN
    IF COALESCE(current_setting('etl_craft.actor', true), '') = '' THEN
        IF TG_OP NOT IN ('INSERT','UPDATE') THEN
            RAISE EXCEPTION USING MESSAGE=upper(TG_TABLE_NAME) || ': direct ' || TG_OP || ' is refused: written only by etl-craft; use etl-craft commands';
        END IF;
        row_project := etl_craft_config_project(TG_TABLE_NAME, to_jsonb(NEW));
        IF row_project IS NULL THEN
            RAISE EXCEPTION USING MESSAGE=upper(TG_TABLE_NAME) || ': admin, audit and history rows are written only by etl-craft; use the admin commands';
        END IF;
        IF TG_OP = 'UPDATE' THEN
            SELECT a.attname INTO primary_key FROM pg_index i JOIN pg_attribute a
                ON a.attrelid=i.indrelid AND a.attnum=ANY(i.indkey)
                WHERE i.indrelid=TG_RELID AND i.indisprimary ORDER BY a.attnum LIMIT 1;
            IF primary_key IS NULL OR to_jsonb(NEW)->primary_key IS DISTINCT FROM to_jsonb(OLD)->primary_key THEN
                RAISE EXCEPTION USING MESSAGE=upper(TG_TABLE_NAME) || '.' || COALESCE(upper(primary_key), 'primary key')
                    || '=' || COALESCE(to_jsonb(NEW)->>primary_key, 'NULL')
                    || ': direct primary-key change is refused; expected original identity '
                    || COALESCE(to_jsonb(OLD)->>primary_key, 'NULL') || '; keep the id and edit metadata fields';
            END IF;
            old_project := etl_craft_config_project(TG_TABLE_NAME, to_jsonb(OLD));
            IF old_project IS DISTINCT FROM row_project THEN
                RAISE EXCEPTION USING MESSAGE=upper(TG_TABLE_NAME) || ': moving PROJECT_ID is refused; use config apply in the correct project';
            END IF;
        END IF;
        SELECT GOVERNANCE INTO policy FROM CFG_PROJECTS p WHERE p.PROJECT_ID=row_project;
        IF policy IS DISTINCT FROM 'RELAXED' THEN
            RAISE EXCEPTION USING MESSAGE=upper(TG_TABLE_NAME) || ': project ' || row_project || ' governance ' || COALESCE(policy, 'missing') || ' refuses direct SQL: written only by etl-craft; use config apply or ask a deployment admin for RELAXED';
        END IF;
        RETURN NEW;
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
CREATE OR REPLACE FUNCTION trg_set_audit_columns()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.ACTIVE_FLAG IS NULL OR NEW.ACTIVE_FLAG NOT IN ('Y','N') THEN
        RAISE EXCEPTION USING MESSAGE=TG_TABLE_NAME || ': ACTIVE_FLAG=' || COALESCE(NEW.ACTIVE_FLAG, 'NULL') || '; expected Y or N', ERRCODE='23514';
    END IF;
    IF TG_OP = 'INSERT' THEN
        NEW.CREATED_BY   := COALESCE(NULLIF(current_setting('etl_craft.actor', true), ''), 'sql:' || session_user);
        NEW.CREATE_DATE  := now();
        NEW.UPDATED_BY   := COALESCE(NULLIF(current_setting('etl_craft.actor', true), ''), 'sql:' || session_user);
        NEW.UPDATED_DATE := now();
    ELSIF TG_OP = 'UPDATE' THEN
        NEW.CREATED_BY   := OLD.CREATED_BY;
        NEW.CREATE_DATE  := OLD.CREATE_DATE;
        NEW.UPDATED_BY   := COALESCE(NULLIF(current_setting('etl_craft.actor', true), ''), 'sql:' || session_user);
        NEW.UPDATED_DATE := now();
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION etl_craft_metadata_change() RETURNS trigger AS $$
DECLARE
    saved_actor text := current_setting('etl_craft.actor', true);
    saved_kind text := current_setting('etl_craft.actor_kind', true);
    history text := quote_ident('aud_' || TG_TABLE_NAME || '_history');
    close_sql text := 'UPDATE ' || history || ' SET valid_to = now(), current_flag = ''N'' '
        || 'WHERE current_flag = ''Y'' AND ' || quote_ident(TG_ARGV[0]) || ' = ($1).' || quote_ident(TG_ARGV[0]);
    insert_sql text := 'INSERT INTO ' || history || ' SELECT * FROM jsonb_populate_record(NULL::' || history || ', $1)';
    stamp jsonb := jsonb_build_object(
        'change_id', nextval('etl_craft_change_id'), 'valid_from', now(), 'operation', TG_OP,
        'actor', COALESCE(NULLIF(saved_actor, ''), 'sql:' || session_user), 'actor_kind', COALESCE(NULLIF(saved_kind, ''), 'USER'),
        'migration', NULLIF(current_setting('etl_craft.migration', true), ''));
BEGIN
    IF TG_OP = 'UPDATE' AND to_jsonb(OLD) = to_jsonb(NEW) THEN RETURN NULL; END IF;
    PERFORM set_config('etl_craft.actor', COALESCE(NULLIF(saved_actor, ''), 'sql:' || session_user), true);
    PERFORM set_config('etl_craft.actor_kind', COALESCE(NULLIF(saved_kind, ''), 'USER'), true);
    IF TG_OP = 'DELETE' THEN
        EXECUTE close_sql USING OLD;
        EXECUTE insert_sql USING to_jsonb(OLD) || stamp
            || jsonb_build_object('valid_to', now(), 'current_flag', 'N');
    ELSE
        EXECUTE close_sql USING NEW;
        EXECUTE insert_sql USING to_jsonb(NEW) || stamp
            || jsonb_build_object('valid_to', '9999-12-31 23:59:59+00'::timestamptz, 'current_flag', 'Y');
    END IF;
    PERFORM set_config('etl_craft.actor', COALESCE(saved_actor, ''), true);
    PERFORM set_config('etl_craft.actor_kind', COALESCE(saved_kind, ''), true);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

ALTER TABLE AUD_CONTROL_REQUESTS DROP CONSTRAINT aud_control_requests_reason_check;
