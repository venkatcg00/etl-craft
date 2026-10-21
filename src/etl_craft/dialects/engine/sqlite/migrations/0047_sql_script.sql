-- etl-craft: rebuild-metadata
-- Admit unmanaged SQL tasks without changing their metadata identities.
CREATE TEMP TABLE etl_craft_script_sequence AS SELECT name, seq FROM sqlite_sequence WHERE name='CFG_TASKS';
CREATE TABLE CFG_TASKS_new (
    TASK_ID              INTEGER PRIMARY KEY AUTOINCREMENT,
    TASK_CODE            VARCHAR NOT NULL,
    TASK_TYPE            VARCHAR NOT NULL,
    PIPELINE_ID          BIGINT NOT NULL REFERENCES CFG_PIPELINES(PIPELINE_ID),
    HANDLER              VARCHAR NOT NULL,
    RUN_CONDITION        VARCHAR,
    RUN_CONDITION_COUNT  INT,
    ACTIVE_FLAG          VARCHAR NOT NULL DEFAULT 'Y',
    CREATED_BY           VARCHAR NOT NULL DEFAULT 'etl-craft',
    CREATE_DATE          TIMESTAMP NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'),
    UPDATED_BY           VARCHAR NOT NULL DEFAULT 'etl-craft',
    UPDATED_DATE         TIMESTAMP NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'),
    TARGET_TABLE_ID BIGINT REFERENCES CFG_TABLES(TABLE_ID),
    TARGET_ACTIVE_FLAG VARCHAR(1) NOT NULL DEFAULT 'Y' CONSTRAINT ck_tasks_target_active CHECK (TARGET_ACTIVE_FLAG IN ('Y','N')),
    CONSTRAINT ck_tasks_code CHECK (TASK_CODE GLOB '[A-Za-z]*' AND TASK_CODE NOT GLOB '*[^A-Za-z0-9_]*' AND length(TASK_CODE) <= 128 AND instr(TASK_CODE, char(0)) = 0),
    CONSTRAINT ck_tasks_task_type     CHECK (TASK_TYPE IN ('INGESTION','ETL')),
    CONSTRAINT ck_tasks_handler       CHECK (HANDLER IN ('PYTHON','SQL','SQL_SCRIPT','BUSINESS_RULES','EMAIL_ALERT','EVENT_WAIT','RUN_PIPELINE')),
    CONSTRAINT ck_tasks_active_flag   CHECK (ACTIVE_FLAG IN ('Y','N')),
    CONSTRAINT ck_tasks_run_condition CHECK (RUN_CONDITION IS NULL OR RUN_CONDITION IN ('ALL','ANY','N')),
    CONSTRAINT ck_tasks_run_condition_count CHECK (
        (COALESCE(RUN_CONDITION, '') = 'N' AND RUN_CONDITION_COUNT IS NOT NULL
            AND RUN_CONDITION_COUNT >= 1)
        OR (RUN_CONDITION IS NOT 'N' AND RUN_CONDITION_COUNT IS NULL)
    )
);
INSERT INTO CFG_TASKS_new (TASK_ID, TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER, RUN_CONDITION, RUN_CONDITION_COUNT, ACTIVE_FLAG, CREATED_BY, CREATE_DATE, UPDATED_BY, UPDATED_DATE, TARGET_TABLE_ID, TARGET_ACTIVE_FLAG) SELECT TASK_ID, TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER, RUN_CONDITION, RUN_CONDITION_COUNT, ACTIVE_FLAG, CREATED_BY, CREATE_DATE, UPDATED_BY, UPDATED_DATE, TARGET_TABLE_ID, TARGET_ACTIVE_FLAG FROM CFG_TASKS;
DROP TABLE CFG_TASKS;
ALTER TABLE CFG_TASKS_new RENAME TO CFG_TASKS;
UPDATE sqlite_sequence SET seq=max(seq, coalesce((SELECT seq FROM etl_craft_script_sequence), seq)) WHERE name='CFG_TASKS';
INSERT INTO sqlite_sequence (name, seq) SELECT name, seq FROM etl_craft_script_sequence s WHERE NOT EXISTS (SELECT 1 FROM sqlite_sequence c WHERE c.name=s.name);
DROP TABLE etl_craft_script_sequence;
CREATE INDEX ix_tasks_target ON CFG_TASKS (TARGET_TABLE_ID) WHERE TARGET_TABLE_ID IS NOT NULL;
CREATE UNIQUE INDEX ux_tasks_code_identity
    ON CFG_TASKS (PIPELINE_ID, TASK_CODE);
CREATE TRIGGER trg_actor_guard_cfg_tasks_insert BEFORE INSERT ON CFG_TASKS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'CFG_TASKS is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
    SELECT etl_craft_enter_insert('CFG_TASKS');
END;
CREATE TRIGGER trg_actor_guard_cfg_tasks_update BEFORE UPDATE ON CFG_TASKS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'CFG_TASKS is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_cfg_tasks_delete BEFORE DELETE ON CFG_TASKS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'CFG_TASKS is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;

-- Align Python and SQL script file parameters.
UPDATE CFG_TASK_PARAMETERS SET PARAMETER_NAME = 'SCRIPT_FILE' WHERE PARAMETER_NAME = 'SCRIPT_NAME' AND TASK_ID IN (SELECT TASK_ID FROM CFG_TASKS WHERE HANDLER = 'PYTHON');

-- Script paths are relative to the project directory, starting with their folder; a value that
-- already starts with it is kept, so the folder is never doubled.
UPDATE CFG_TASK_PARAMETERS SET PARAMETER_VALUE = 'python_scripts/' || trim(PARAMETER_VALUE) WHERE PARAMETER_NAME = 'SCRIPT_FILE' AND trim(PARAMETER_VALUE) <> '' AND substr(trim(PARAMETER_VALUE), 1, 15) <> 'python_scripts/' AND TASK_ID IN (SELECT TASK_ID FROM CFG_TASKS WHERE HANDLER = 'PYTHON');
UPDATE CFG_TASK_PARAMETERS SET PARAMETER_VALUE = 'sql_scripts/' || trim(PARAMETER_VALUE) WHERE PARAMETER_NAME = 'SOURCE_SQL_FILE' AND trim(PARAMETER_VALUE) <> '' AND substr(trim(PARAMETER_VALUE), 1, 12) <> 'sql_scripts/';

-- One name per meaning: SCRIPT_FILE names a file a task runs, SELECT_QUERY an inline SELECT.
UPDATE CFG_TASK_PARAMETERS SET PARAMETER_NAME = 'SCRIPT_FILE' WHERE PARAMETER_NAME = 'SOURCE_SQL_FILE';
UPDATE CFG_TASK_PARAMETERS SET PARAMETER_NAME = 'SELECT_QUERY' WHERE PARAMETER_NAME = 'SOURCE_SQL';
