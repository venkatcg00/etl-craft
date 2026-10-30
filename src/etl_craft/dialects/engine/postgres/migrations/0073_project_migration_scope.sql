-- Which project owns each team table: claimed by the first project migration that changes it.
CREATE TABLE AUD_TEAM_TABLES (
    TABLE_NAME VARCHAR(128) PRIMARY KEY CHECK (TABLE_NAME = upper(TABLE_NAME)),
    PROJECT_ID BIGINT NOT NULL REFERENCES CFG_PROJECTS(PROJECT_ID),
    MIGRATION VARCHAR NOT NULL,
    CLAIMED_BY VARCHAR NOT NULL,
    CLAIMED_AT TIMESTAMPTZ NOT NULL
);
CREATE INDEX ix_team_tables_project ON AUD_TEAM_TABLES (PROJECT_ID);
CREATE TRIGGER trg_actor_guard_aud_team_tables BEFORE INSERT OR UPDATE OR DELETE ON AUD_TEAM_TABLES FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_aud_team_tables BEFORE TRUNCATE ON AUD_TEAM_TABLES FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();

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
    IF COALESCE(current_setting('etl_craft.migration_project', true), '') <> '' AND pg_trigger_depth() = 1 THEN
        IF TG_OP = 'TRUNCATE' THEN
            RAISE EXCEPTION USING MESSAGE=upper(TG_TABLE_NAME) || ': a project migration cannot TRUNCATE package tables';
        ELSIF TG_OP = 'DELETE' THEN
            row_project := etl_craft_config_project(TG_TABLE_NAME, to_jsonb(OLD));
        ELSE
            row_project := etl_craft_config_project(TG_TABLE_NAME, to_jsonb(NEW));
        END IF;
        IF row_project IS NULL THEN
            RAISE EXCEPTION USING MESSAGE=upper(TG_TABLE_NAME) || ': a project migration cannot write admin, audit or history rows; use the admin commands';
        END IF;
        IF TG_OP = 'UPDATE' THEN
            old_project := etl_craft_config_project(TG_TABLE_NAME, to_jsonb(OLD));
        END IF;
        IF row_project::text <> current_setting('etl_craft.migration_project')
           OR old_project::text <> current_setting('etl_craft.migration_project') THEN
            RAISE EXCEPTION USING MESSAGE=upper(TG_TABLE_NAME) || ': a project migration writes only its own project''s rows; found project '
                || COALESCE(old_project, row_project) || CASE WHEN old_project IS DISTINCT FROM row_project AND old_project IS NOT NULL THEN ' moving to ' || row_project ELSE '' END
                || ', migrating project ' || current_setting('etl_craft.migration_project') || '; filter on (SELECT PROJECT_ID FROM ETL_CRAFT_PROJECT)';
        END IF;
    END IF;
    IF COALESCE(current_setting('etl_craft.purpose', true), '') <> 'retention' AND TG_OP IN ('UPDATE','DELETE','TRUNCATE') THEN
        IF TG_TABLE_NAME IN ('aud_run_interventions','aud_actions','aud_dependency_consumption','aud_gate_decisions','aud_task_events','aud_event_files','aud_event_keys','aud_task_items','aud_project_bundles','aud_team_tables') THEN
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
