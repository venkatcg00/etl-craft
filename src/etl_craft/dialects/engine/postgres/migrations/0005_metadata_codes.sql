-- Pipeline and task codes are safe in commands and distinct from control steps.
ALTER TABLE CFG_PIPELINES ADD CONSTRAINT ck_pipelines_code
    CHECK (PIPELINE_CODE ~ '^[A-Za-z][A-Za-z0-9_]{0,127}$') NOT VALID;
ALTER TABLE CFG_PIPELINES VALIDATE CONSTRAINT ck_pipelines_code;
ALTER TABLE CFG_TASKS ADD CONSTRAINT ck_tasks_code
    CHECK (TASK_CODE ~ '^[A-Za-z][A-Za-z0-9_]{0,127}$') NOT VALID;
ALTER TABLE CFG_TASKS VALIDATE CONSTRAINT ck_tasks_code;
