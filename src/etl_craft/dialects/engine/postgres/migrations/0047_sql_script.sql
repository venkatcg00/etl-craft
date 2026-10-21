-- Admit unmanaged SQL tasks.
ALTER TABLE CFG_TASKS DROP CONSTRAINT ck_tasks_handler;
ALTER TABLE CFG_TASKS ADD CONSTRAINT ck_tasks_handler CHECK (HANDLER IN ('PYTHON','SQL','SQL_SCRIPT','BUSINESS_RULES','EMAIL_ALERT','EVENT_WAIT','RUN_PIPELINE'));

-- Align Python and SQL script file parameters.
UPDATE CFG_TASK_PARAMETERS SET PARAMETER_NAME = 'SCRIPT_FILE' WHERE PARAMETER_NAME = 'SCRIPT_NAME' AND TASK_ID IN (SELECT TASK_ID FROM CFG_TASKS WHERE HANDLER = 'PYTHON');

-- Script paths are relative to the project directory, starting with their folder; a value that
-- already starts with it is kept, so the folder is never doubled.
UPDATE CFG_TASK_PARAMETERS SET PARAMETER_VALUE = 'python_scripts/' || trim(PARAMETER_VALUE) WHERE PARAMETER_NAME = 'SCRIPT_FILE' AND trim(PARAMETER_VALUE) <> '' AND substr(trim(PARAMETER_VALUE), 1, 15) <> 'python_scripts/' AND TASK_ID IN (SELECT TASK_ID FROM CFG_TASKS WHERE HANDLER = 'PYTHON');
UPDATE CFG_TASK_PARAMETERS SET PARAMETER_VALUE = 'sql_scripts/' || trim(PARAMETER_VALUE) WHERE PARAMETER_NAME = 'SOURCE_SQL_FILE' AND trim(PARAMETER_VALUE) <> '' AND substr(trim(PARAMETER_VALUE), 1, 12) <> 'sql_scripts/';

-- One name per meaning: SCRIPT_FILE names a file a task runs, SELECT_QUERY an inline SELECT.
UPDATE CFG_TASK_PARAMETERS SET PARAMETER_NAME = 'SCRIPT_FILE' WHERE PARAMETER_NAME = 'SOURCE_SQL_FILE';
UPDATE CFG_TASK_PARAMETERS SET PARAMETER_NAME = 'SELECT_QUERY' WHERE PARAMETER_NAME = 'SOURCE_SQL';
COMMENT ON TABLE CFG_TASK_PARAMETERS IS 'A task''s settings as name and value pairs (SQL_ACTION, SELECT_QUERY, ...); TARGET_OBJECT is derived from the task table reference. Which names each handler reads is documented in the task parameter reference.';

COMMENT ON TABLE AUD_TASK_OFFSET_TRACKER IS 'The watermark an incremental Python script reads and advances.';
