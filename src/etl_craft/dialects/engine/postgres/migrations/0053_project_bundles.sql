CREATE TABLE AUD_PROJECT_BUNDLES (
    BUNDLE_ID VARCHAR(64) PRIMARY KEY,
    MANIFEST TEXT NOT NULL,
    CREATED_BY VARCHAR NOT NULL,
    CREATED_AT TIMESTAMPTZ NOT NULL
);
CREATE INDEX ix_project_bundles_latest ON AUD_PROJECT_BUNDLES (CREATED_AT DESC,BUNDLE_ID DESC);
ALTER TABLE AUD_PIPELINES_RUN_LOG ADD COLUMN BUNDLE_ID VARCHAR(64) REFERENCES AUD_PROJECT_BUNDLES(BUNDLE_ID);
ALTER TABLE AUD_PIPELINES_RUN_LOG ADD COLUMN METADATA_VERSION VARCHAR(64);
ALTER TABLE AUD_PIPELINES_RUN_LOG ADD COLUMN METADATA_SNAPSHOT TEXT;
UPDATE AUD_PIPELINES_RUN_LOG AS original
SET (METADATA_SNAPSHOT,METADATA_VERSION) = (WITH image AS (WITH own_tasks AS (SELECT TASK_ID AS task_id FROM CFG_TASKS WHERE PIPELINE_ID=CAST(original.PIPELINE_ID AS bigint)),
wanted_tasks AS (SELECT TASK_ID AS task_id FROM own_tasks UNION SELECT DEPENDS_ON_TASK_ID AS task_id FROM CFG_TASK_DEPENDENCY WHERE TASK_ID IN (SELECT TASK_ID FROM own_tasks)),
wanted_pipelines AS (SELECT CAST(original.PIPELINE_ID AS bigint) AS pipeline_id UNION SELECT PIPELINE_ID AS pipeline_id FROM CFG_TASKS WHERE TASK_ID IN (SELECT TASK_ID FROM wanted_tasks) UNION SELECT DEPENDS_ON_PIPELINE_ID AS pipeline_id FROM CFG_PIPELINE_DEPENDENCY WHERE PIPELINE_ID=CAST(original.PIPELINE_ID AS bigint)),
wanted_tables AS (SELECT TARGET_TABLE_ID AS table_id FROM CFG_TASKS WHERE TASK_ID IN (SELECT TASK_ID FROM wanted_tasks) UNION SELECT TARGET_TABLE_ID AS table_id FROM CFG_BUSINESS_RULES WHERE TASK_ID IN (SELECT TASK_ID FROM own_tasks))
SELECT CAST(jsonb_build_object('CFG_PIPELINES',jsonb_build_object('columns',CAST('["pipeline_id", "pipeline_code", "pipeline_name", "description", "run_schedule", "sla_in_hours", "refresh_type", "active_flag", "pipeline_parameters", "created_by", "create_date", "updated_by", "updated_date", "schedule_timezone", "catchup", "max_catchup_runs", "overlap_policy", "schedule_start_date", "max_parallel_tasks", "priority"]' AS jsonb),'rows',COALESCE((SELECT jsonb_agg(to_jsonb(r)) FROM (SELECT p.* FROM wanted_pipelines w CROSS JOIN LATERAL (SELECT * FROM CFG_PIPELINES p WHERE p.PIPELINE_ID=w.PIPELINE_ID LIMIT 1) p ORDER BY p.pipeline_id) r),'[]'::jsonb)),
'CFG_TASKS',jsonb_build_object('columns',CAST('["task_id", "task_code", "task_type", "pipeline_id", "handler", "run_condition", "run_condition_count", "active_flag", "created_by", "create_date", "updated_by", "updated_date", "target_table_id", "target_active_flag"]' AS jsonb),'rows',COALESCE((SELECT jsonb_agg(to_jsonb(r)) FROM (SELECT * FROM CFG_TASKS WHERE TASK_ID IN (SELECT TASK_ID FROM wanted_tasks) ORDER BY task_id) r),'[]'::jsonb)),
'CFG_TASK_PARAMETERS',jsonb_build_object('columns',CAST('["task_parameter_id", "task_id", "parameter_name", "parameter_value", "active_flag", "created_by", "create_date", "updated_by", "updated_date"]' AS jsonb),'rows',COALESCE((SELECT jsonb_agg(to_jsonb(r)) FROM (SELECT * FROM CFG_TASK_PARAMETERS WHERE TASK_ID IN (SELECT TASK_ID FROM own_tasks) ORDER BY task_parameter_id) r),'[]'::jsonb)),
'CFG_TASK_DEPENDENCY',jsonb_build_object('columns',CAST('["task_dependency_id", "pipeline_id", "task_id", "depends_on_pipeline_id", "depends_on_task_id", "dependency_type", "active_flag", "created_by", "create_date", "updated_by", "updated_date", "consume_repairs"]' AS jsonb),'rows',COALESCE((SELECT jsonb_agg(to_jsonb(r)) FROM (SELECT * FROM CFG_TASK_DEPENDENCY WHERE TASK_ID IN (SELECT TASK_ID FROM own_tasks) ORDER BY task_dependency_id) r),'[]'::jsonb)),
'CFG_PIPELINE_DEPENDENCY',jsonb_build_object('columns',CAST('["pipeline_dependency_id", "pipeline_id", "depends_on_pipeline_id", "dependency_type", "active_flag", "created_by", "create_date", "updated_by", "updated_date", "consume_repairs"]' AS jsonb),'rows',COALESCE((SELECT jsonb_agg(to_jsonb(r)) FROM (SELECT * FROM CFG_PIPELINE_DEPENDENCY WHERE PIPELINE_ID=CAST(original.PIPELINE_ID AS bigint) ORDER BY pipeline_dependency_id) r),'[]'::jsonb)),
'CFG_BUSINESS_RULES',jsonb_build_object('columns',CAST('["business_rule_id", "business_rule_name", "pipeline_id", "task_id", "business_rule_sql", "business_rule_type", "business_rule_key_column", "sequence_number", "active_flag", "created_by", "create_date", "updated_by", "updated_date", "target_table_id"]' AS jsonb),'rows',COALESCE((SELECT jsonb_agg(to_jsonb(r)) FROM (SELECT * FROM CFG_BUSINESS_RULES WHERE TASK_ID IN (SELECT TASK_ID FROM own_tasks) ORDER BY business_rule_id) r),'[]'::jsonb)),
'CFG_TABLES',jsonb_build_object('columns',CAST('["table_id", "catalog_name", "schema_name", "table_name", "object_kind", "active_flag", "created_by", "create_date", "updated_by", "updated_date"]' AS jsonb),'rows',COALESCE((SELECT jsonb_agg(to_jsonb(r)) FROM (SELECT * FROM CFG_TABLES WHERE TABLE_ID IN (SELECT TABLE_ID FROM wanted_tables) ORDER BY table_id) r),'[]'::jsonb)),
'CFG_COLUMNS',jsonb_build_object('columns',CAST('["column_id", "table_id", "column_name", "data_type", "column_position", "is_nullable", "active_flag", "created_by", "create_date", "updated_by", "updated_date"]' AS jsonb),'rows',COALESCE((SELECT jsonb_agg(to_jsonb(r)) FROM (SELECT * FROM CFG_COLUMNS WHERE TABLE_ID IN (SELECT TABLE_ID FROM wanted_tables) ORDER BY column_id) r),'[]'::jsonb))) AS text) AS snapshot) SELECT CAST(snapshot AS text) AS snapshot,encode(sha256(convert_to(CAST(snapshot AS text),'UTF8')),'hex') AS version FROM image)
WHERE STATUS IN ('QUEUED','IN-PROGRESS');
CREATE TRIGGER trg_actor_guard_aud_project_bundles BEFORE INSERT OR UPDATE OR DELETE ON AUD_PROJECT_BUNDLES FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_aud_project_bundles BEFORE TRUNCATE ON AUD_PROJECT_BUNDLES FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
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
            ELSIF OLD.STATUS NOT IN ('QUEUED','CLAIMED','RUNNING') THEN
                RAISE EXCEPTION 'AUD_TASK_ATTEMPTS terminal rows are immutable; queue a new attempt';
            END IF;
        END IF;
    END IF;
    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE FUNCTION etl_craft_guard_run_definition() RETURNS trigger AS $$
BEGIN
 IF OLD.METADATA_SNAPSHOT IS NOT NULL AND (NEW.BUNDLE_ID IS DISTINCT FROM OLD.BUNDLE_ID OR NEW.METADATA_VERSION IS DISTINCT FROM OLD.METADATA_VERSION OR NEW.METADATA_SNAPSHOT IS DISTINCT FROM OLD.METADATA_SNAPSHOT) THEN
  RAISE EXCEPTION 'run definition is immutable; start a new run';
 END IF;
 RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER trg_run_definition_immutable BEFORE UPDATE ON AUD_PIPELINES_RUN_LOG FOR EACH ROW EXECUTE FUNCTION etl_craft_guard_run_definition();
