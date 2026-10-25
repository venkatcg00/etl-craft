-- Bounded history deletion and recoverable local log cleanup.
ALTER TABLE AUD_TASK_ATTEMPTS ADD COLUMN LOG_REMOVED_AT TIMESTAMP;
-- Pending canonical log deletions, committed before the filesystem is touched.
CREATE TABLE AUD_RETENTION_FILES (
    ATTEMPT_ID BIGINT PRIMARY KEY,
    PROJECT_ID BIGINT NOT NULL REFERENCES CFG_PROJECTS(PROJECT_ID),
    LOG_PATH VARCHAR NOT NULL,
    RECORDED_AT TIMESTAMP NOT NULL
);
CREATE INDEX ix_retention_files_project ON AUD_RETENTION_FILES (PROJECT_ID, ATTEMPT_ID);
-- Latest completed purge day per project, in its configured time zone.
CREATE TABLE AUD_RETENTION_TICKS (
    PROJECT_ID BIGINT PRIMARY KEY REFERENCES CFG_PROJECTS(PROJECT_ID),
    PURGED_ON DATE NOT NULL
);
CREATE INDEX ix_attempt_retention ON AUD_TASK_ATTEMPTS (ENDED_AT, ATTEMPT_ID);
CREATE INDEX ix_gate_attempt_retention ON AUD_GATE_DECISIONS (ATTEMPT_ID);
CREATE INDEX ix_gate_selected_task_retention ON AUD_GATE_DECISIONS (SELECTED_TASK_RUN_ID);
CREATE INDEX ix_gate_selected_run_retention ON AUD_GATE_DECISIONS (SELECTED_PIPELINE_RUN_ID);
CREATE INDEX ix_consumed_task_retention ON AUD_DEPENDENCY_CONSUMPTION (CONSUMED_TASK_RUN_ID);
CREATE INDEX ix_consumed_run_retention ON AUD_DEPENDENCY_CONSUMPTION (CONSUMED_PIPELINE_RUN_ID);
CREATE INDEX ix_actions_run_retention ON AUD_ACTIONS (PIPELINE_RUN_ID);
CREATE TRIGGER trg_actor_guard_aud_retention_files_insert BEFORE INSERT ON AUD_RETENTION_FILES
BEGIN SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_RETENTION_FILES is written only by etl-craft') END; END;
CREATE TRIGGER trg_actor_guard_aud_retention_files_update BEFORE UPDATE ON AUD_RETENTION_FILES
BEGIN SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_RETENTION_FILES is written only by etl-craft') END; END;
CREATE TRIGGER trg_actor_guard_aud_retention_files_delete BEFORE DELETE ON AUD_RETENTION_FILES
BEGIN SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_RETENTION_FILES is written only by etl-craft') END; END;
CREATE TRIGGER trg_actor_guard_aud_retention_ticks_insert BEFORE INSERT ON AUD_RETENTION_TICKS
BEGIN SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_RETENTION_TICKS is written only by etl-craft') END; END;
CREATE TRIGGER trg_actor_guard_aud_retention_ticks_update BEFORE UPDATE ON AUD_RETENTION_TICKS
BEGIN SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_RETENTION_TICKS is written only by etl-craft') END; END;
CREATE TRIGGER trg_actor_guard_aud_retention_ticks_delete BEFORE DELETE ON AUD_RETENTION_TICKS
BEGIN SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_RETENTION_TICKS is written only by etl-craft') END; END;
