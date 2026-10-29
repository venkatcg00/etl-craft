-- Which project owns each team table: claimed by the first project migration that changes it.
CREATE TABLE AUD_TEAM_TABLES (
    TABLE_NAME VARCHAR(128) PRIMARY KEY CHECK (TABLE_NAME = upper(TABLE_NAME)),
    PROJECT_ID BIGINT NOT NULL REFERENCES CFG_PROJECTS(PROJECT_ID),
    MIGRATION VARCHAR NOT NULL,
    CLAIMED_BY VARCHAR NOT NULL,
    CLAIMED_AT TIMESTAMP NOT NULL
);
CREATE INDEX ix_team_tables_project ON AUD_TEAM_TABLES (PROJECT_ID);
CREATE TRIGGER trg_actor_guard_aud_team_tables_insert BEFORE INSERT ON AUD_TEAM_TABLES
BEGIN
 SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_TEAM_TABLES is written only by etl-craft migrate') END;
END;
CREATE TRIGGER trg_actor_guard_aud_team_tables_update BEFORE UPDATE ON AUD_TEAM_TABLES
BEGIN
 SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_TEAM_TABLES is written only by etl-craft migrate') END;
 SELECT CASE WHEN etl_craft_purpose() <> 'retention' THEN RAISE(ABORT, 'AUD_TEAM_TABLES is append-only: a team table keeps the project that claimed it') END;
END;
CREATE TRIGGER trg_actor_guard_aud_team_tables_delete BEFORE DELETE ON AUD_TEAM_TABLES
BEGIN
 SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_TEAM_TABLES is written only by etl-craft migrate') END;
 SELECT CASE WHEN etl_craft_purpose() <> 'retention' THEN RAISE(ABORT, 'AUD_TEAM_TABLES is append-only: a team table keeps the project that claimed it') END;
END;
CREATE TRIGGER trg_project_scope_cfg_pipelines_insert BEFORE INSERT ON CFG_PIPELINES
WHEN etl_craft_migration_project() IS NOT NULL AND (NEW.PROJECT_ID IS NOT etl_craft_migration_project())
BEGIN
 SELECT RAISE(ABORT, 'CFG_PIPELINES: a project migration writes only its own project''s rows; filter on (SELECT PROJECT_ID FROM ETL_CRAFT_PROJECT)');
END;
CREATE TRIGGER trg_project_scope_cfg_pipelines_update BEFORE UPDATE ON CFG_PIPELINES
WHEN etl_craft_migration_project() IS NOT NULL AND (NEW.PROJECT_ID IS NOT etl_craft_migration_project() OR OLD.PROJECT_ID IS NOT etl_craft_migration_project())
BEGIN
 SELECT RAISE(ABORT, 'CFG_PIPELINES: a project migration writes only its own project''s rows; filter on (SELECT PROJECT_ID FROM ETL_CRAFT_PROJECT)');
END;
CREATE TRIGGER trg_project_scope_cfg_pipelines_delete BEFORE DELETE ON CFG_PIPELINES
WHEN etl_craft_migration_project() IS NOT NULL AND (OLD.PROJECT_ID IS NOT etl_craft_migration_project())
BEGIN
 SELECT RAISE(ABORT, 'CFG_PIPELINES: a project migration writes only its own project''s rows; filter on (SELECT PROJECT_ID FROM ETL_CRAFT_PROJECT)');
END;
CREATE TRIGGER trg_project_scope_cfg_tables_insert BEFORE INSERT ON CFG_TABLES
WHEN etl_craft_migration_project() IS NOT NULL AND (NEW.PROJECT_ID IS NOT etl_craft_migration_project())
BEGIN
 SELECT RAISE(ABORT, 'CFG_TABLES: a project migration writes only its own project''s rows; filter on (SELECT PROJECT_ID FROM ETL_CRAFT_PROJECT)');
END;
CREATE TRIGGER trg_project_scope_cfg_tables_update BEFORE UPDATE ON CFG_TABLES
WHEN etl_craft_migration_project() IS NOT NULL AND (NEW.PROJECT_ID IS NOT etl_craft_migration_project() OR OLD.PROJECT_ID IS NOT etl_craft_migration_project())
BEGIN
 SELECT RAISE(ABORT, 'CFG_TABLES: a project migration writes only its own project''s rows; filter on (SELECT PROJECT_ID FROM ETL_CRAFT_PROJECT)');
END;
CREATE TRIGGER trg_project_scope_cfg_tables_delete BEFORE DELETE ON CFG_TABLES
WHEN etl_craft_migration_project() IS NOT NULL AND (OLD.PROJECT_ID IS NOT etl_craft_migration_project())
BEGIN
 SELECT RAISE(ABORT, 'CFG_TABLES: a project migration writes only its own project''s rows; filter on (SELECT PROJECT_ID FROM ETL_CRAFT_PROJECT)');
END;
CREATE TRIGGER trg_project_scope_cfg_tasks_insert BEFORE INSERT ON CFG_TASKS
WHEN etl_craft_migration_project() IS NOT NULL AND ((SELECT p.PROJECT_ID FROM CFG_PIPELINES p WHERE p.PIPELINE_ID=NEW.PIPELINE_ID) IS NOT etl_craft_migration_project())
BEGIN
 SELECT RAISE(ABORT, 'CFG_TASKS: a project migration writes only its own project''s rows; filter on (SELECT PROJECT_ID FROM ETL_CRAFT_PROJECT)');
END;
CREATE TRIGGER trg_project_scope_cfg_tasks_update BEFORE UPDATE ON CFG_TASKS
WHEN etl_craft_migration_project() IS NOT NULL AND ((SELECT p.PROJECT_ID FROM CFG_PIPELINES p WHERE p.PIPELINE_ID=NEW.PIPELINE_ID) IS NOT etl_craft_migration_project() OR (SELECT p.PROJECT_ID FROM CFG_PIPELINES p WHERE p.PIPELINE_ID=OLD.PIPELINE_ID) IS NOT etl_craft_migration_project())
BEGIN
 SELECT RAISE(ABORT, 'CFG_TASKS: a project migration writes only its own project''s rows; filter on (SELECT PROJECT_ID FROM ETL_CRAFT_PROJECT)');
END;
CREATE TRIGGER trg_project_scope_cfg_tasks_delete BEFORE DELETE ON CFG_TASKS
WHEN etl_craft_migration_project() IS NOT NULL AND ((SELECT p.PROJECT_ID FROM CFG_PIPELINES p WHERE p.PIPELINE_ID=OLD.PIPELINE_ID) IS NOT etl_craft_migration_project())
BEGIN
 SELECT RAISE(ABORT, 'CFG_TASKS: a project migration writes only its own project''s rows; filter on (SELECT PROJECT_ID FROM ETL_CRAFT_PROJECT)');
END;
CREATE TRIGGER trg_project_scope_cfg_pipeline_dependency_insert BEFORE INSERT ON CFG_PIPELINE_DEPENDENCY
WHEN etl_craft_migration_project() IS NOT NULL AND ((SELECT p.PROJECT_ID FROM CFG_PIPELINES p WHERE p.PIPELINE_ID=NEW.PIPELINE_ID) IS NOT etl_craft_migration_project())
BEGIN
 SELECT RAISE(ABORT, 'CFG_PIPELINE_DEPENDENCY: a project migration writes only its own project''s rows; filter on (SELECT PROJECT_ID FROM ETL_CRAFT_PROJECT)');
END;
CREATE TRIGGER trg_project_scope_cfg_pipeline_dependency_update BEFORE UPDATE ON CFG_PIPELINE_DEPENDENCY
WHEN etl_craft_migration_project() IS NOT NULL AND ((SELECT p.PROJECT_ID FROM CFG_PIPELINES p WHERE p.PIPELINE_ID=NEW.PIPELINE_ID) IS NOT etl_craft_migration_project() OR (SELECT p.PROJECT_ID FROM CFG_PIPELINES p WHERE p.PIPELINE_ID=OLD.PIPELINE_ID) IS NOT etl_craft_migration_project())
BEGIN
 SELECT RAISE(ABORT, 'CFG_PIPELINE_DEPENDENCY: a project migration writes only its own project''s rows; filter on (SELECT PROJECT_ID FROM ETL_CRAFT_PROJECT)');
END;
CREATE TRIGGER trg_project_scope_cfg_pipeline_dependency_delete BEFORE DELETE ON CFG_PIPELINE_DEPENDENCY
WHEN etl_craft_migration_project() IS NOT NULL AND ((SELECT p.PROJECT_ID FROM CFG_PIPELINES p WHERE p.PIPELINE_ID=OLD.PIPELINE_ID) IS NOT etl_craft_migration_project())
BEGIN
 SELECT RAISE(ABORT, 'CFG_PIPELINE_DEPENDENCY: a project migration writes only its own project''s rows; filter on (SELECT PROJECT_ID FROM ETL_CRAFT_PROJECT)');
END;
CREATE TRIGGER trg_project_scope_cfg_task_dependency_insert BEFORE INSERT ON CFG_TASK_DEPENDENCY
WHEN etl_craft_migration_project() IS NOT NULL AND ((SELECT p.PROJECT_ID FROM CFG_PIPELINES p WHERE p.PIPELINE_ID=NEW.PIPELINE_ID) IS NOT etl_craft_migration_project())
BEGIN
 SELECT RAISE(ABORT, 'CFG_TASK_DEPENDENCY: a project migration writes only its own project''s rows; filter on (SELECT PROJECT_ID FROM ETL_CRAFT_PROJECT)');
END;
CREATE TRIGGER trg_project_scope_cfg_task_dependency_update BEFORE UPDATE ON CFG_TASK_DEPENDENCY
WHEN etl_craft_migration_project() IS NOT NULL AND ((SELECT p.PROJECT_ID FROM CFG_PIPELINES p WHERE p.PIPELINE_ID=NEW.PIPELINE_ID) IS NOT etl_craft_migration_project() OR (SELECT p.PROJECT_ID FROM CFG_PIPELINES p WHERE p.PIPELINE_ID=OLD.PIPELINE_ID) IS NOT etl_craft_migration_project())
BEGIN
 SELECT RAISE(ABORT, 'CFG_TASK_DEPENDENCY: a project migration writes only its own project''s rows; filter on (SELECT PROJECT_ID FROM ETL_CRAFT_PROJECT)');
END;
CREATE TRIGGER trg_project_scope_cfg_task_dependency_delete BEFORE DELETE ON CFG_TASK_DEPENDENCY
WHEN etl_craft_migration_project() IS NOT NULL AND ((SELECT p.PROJECT_ID FROM CFG_PIPELINES p WHERE p.PIPELINE_ID=OLD.PIPELINE_ID) IS NOT etl_craft_migration_project())
BEGIN
 SELECT RAISE(ABORT, 'CFG_TASK_DEPENDENCY: a project migration writes only its own project''s rows; filter on (SELECT PROJECT_ID FROM ETL_CRAFT_PROJECT)');
END;
CREATE TRIGGER trg_project_scope_cfg_business_rules_insert BEFORE INSERT ON CFG_BUSINESS_RULES
WHEN etl_craft_migration_project() IS NOT NULL AND ((SELECT p.PROJECT_ID FROM CFG_PIPELINES p WHERE p.PIPELINE_ID=NEW.PIPELINE_ID) IS NOT etl_craft_migration_project())
BEGIN
 SELECT RAISE(ABORT, 'CFG_BUSINESS_RULES: a project migration writes only its own project''s rows; filter on (SELECT PROJECT_ID FROM ETL_CRAFT_PROJECT)');
END;
CREATE TRIGGER trg_project_scope_cfg_business_rules_update BEFORE UPDATE ON CFG_BUSINESS_RULES
WHEN etl_craft_migration_project() IS NOT NULL AND ((SELECT p.PROJECT_ID FROM CFG_PIPELINES p WHERE p.PIPELINE_ID=NEW.PIPELINE_ID) IS NOT etl_craft_migration_project() OR (SELECT p.PROJECT_ID FROM CFG_PIPELINES p WHERE p.PIPELINE_ID=OLD.PIPELINE_ID) IS NOT etl_craft_migration_project())
BEGIN
 SELECT RAISE(ABORT, 'CFG_BUSINESS_RULES: a project migration writes only its own project''s rows; filter on (SELECT PROJECT_ID FROM ETL_CRAFT_PROJECT)');
END;
CREATE TRIGGER trg_project_scope_cfg_business_rules_delete BEFORE DELETE ON CFG_BUSINESS_RULES
WHEN etl_craft_migration_project() IS NOT NULL AND ((SELECT p.PROJECT_ID FROM CFG_PIPELINES p WHERE p.PIPELINE_ID=OLD.PIPELINE_ID) IS NOT etl_craft_migration_project())
BEGIN
 SELECT RAISE(ABORT, 'CFG_BUSINESS_RULES: a project migration writes only its own project''s rows; filter on (SELECT PROJECT_ID FROM ETL_CRAFT_PROJECT)');
END;
CREATE TRIGGER trg_project_scope_cfg_task_parameters_insert BEFORE INSERT ON CFG_TASK_PARAMETERS
WHEN etl_craft_migration_project() IS NOT NULL AND ((SELECT p.PROJECT_ID FROM CFG_TASKS t JOIN CFG_PIPELINES p ON p.PIPELINE_ID=t.PIPELINE_ID WHERE t.TASK_ID=NEW.TASK_ID) IS NOT etl_craft_migration_project())
BEGIN
 SELECT RAISE(ABORT, 'CFG_TASK_PARAMETERS: a project migration writes only its own project''s rows; filter on (SELECT PROJECT_ID FROM ETL_CRAFT_PROJECT)');
END;
CREATE TRIGGER trg_project_scope_cfg_task_parameters_update BEFORE UPDATE ON CFG_TASK_PARAMETERS
WHEN etl_craft_migration_project() IS NOT NULL AND ((SELECT p.PROJECT_ID FROM CFG_TASKS t JOIN CFG_PIPELINES p ON p.PIPELINE_ID=t.PIPELINE_ID WHERE t.TASK_ID=NEW.TASK_ID) IS NOT etl_craft_migration_project() OR (SELECT p.PROJECT_ID FROM CFG_TASKS t JOIN CFG_PIPELINES p ON p.PIPELINE_ID=t.PIPELINE_ID WHERE t.TASK_ID=OLD.TASK_ID) IS NOT etl_craft_migration_project())
BEGIN
 SELECT RAISE(ABORT, 'CFG_TASK_PARAMETERS: a project migration writes only its own project''s rows; filter on (SELECT PROJECT_ID FROM ETL_CRAFT_PROJECT)');
END;
CREATE TRIGGER trg_project_scope_cfg_task_parameters_delete BEFORE DELETE ON CFG_TASK_PARAMETERS
WHEN etl_craft_migration_project() IS NOT NULL AND ((SELECT p.PROJECT_ID FROM CFG_TASKS t JOIN CFG_PIPELINES p ON p.PIPELINE_ID=t.PIPELINE_ID WHERE t.TASK_ID=OLD.TASK_ID) IS NOT etl_craft_migration_project())
BEGIN
 SELECT RAISE(ABORT, 'CFG_TASK_PARAMETERS: a project migration writes only its own project''s rows; filter on (SELECT PROJECT_ID FROM ETL_CRAFT_PROJECT)');
END;
CREATE TRIGGER trg_project_scope_cfg_columns_insert BEFORE INSERT ON CFG_COLUMNS
WHEN etl_craft_migration_project() IS NOT NULL AND ((SELECT t.PROJECT_ID FROM CFG_TABLES t WHERE t.TABLE_ID=NEW.TABLE_ID) IS NOT etl_craft_migration_project())
BEGIN
 SELECT RAISE(ABORT, 'CFG_COLUMNS: a project migration writes only its own project''s rows; filter on (SELECT PROJECT_ID FROM ETL_CRAFT_PROJECT)');
END;
CREATE TRIGGER trg_project_scope_cfg_columns_update BEFORE UPDATE ON CFG_COLUMNS
WHEN etl_craft_migration_project() IS NOT NULL AND ((SELECT t.PROJECT_ID FROM CFG_TABLES t WHERE t.TABLE_ID=NEW.TABLE_ID) IS NOT etl_craft_migration_project() OR (SELECT t.PROJECT_ID FROM CFG_TABLES t WHERE t.TABLE_ID=OLD.TABLE_ID) IS NOT etl_craft_migration_project())
BEGIN
 SELECT RAISE(ABORT, 'CFG_COLUMNS: a project migration writes only its own project''s rows; filter on (SELECT PROJECT_ID FROM ETL_CRAFT_PROJECT)');
END;
CREATE TRIGGER trg_project_scope_cfg_columns_delete BEFORE DELETE ON CFG_COLUMNS
WHEN etl_craft_migration_project() IS NOT NULL AND ((SELECT t.PROJECT_ID FROM CFG_TABLES t WHERE t.TABLE_ID=OLD.TABLE_ID) IS NOT etl_craft_migration_project())
BEGIN
 SELECT RAISE(ABORT, 'CFG_COLUMNS: a project migration writes only its own project''s rows; filter on (SELECT PROJECT_ID FROM ETL_CRAFT_PROJECT)');
END;
