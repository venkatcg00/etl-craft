-- Preserve existing identities and unknown historical actors while adding actor defaults.
CREATE TEMP TABLE etl_craft_actor_sequence AS SELECT name, seq FROM sqlite_sequence WHERE name = 'AUD_PIPELINES_RUN_LOG';
CREATE TABLE AUD_PIPELINES_RUN_LOG_new (
    PIPELINE_RUN_ID  INTEGER PRIMARY KEY AUTOINCREMENT,
    PIPELINE_ID      BIGINT NOT NULL REFERENCES CFG_PIPELINES(PIPELINE_ID),
    START_DATE       TIMESTAMP NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'),
    END_DATE         TIMESTAMP,
    STATUS           VARCHAR NOT NULL,
    SLA_STATUS       VARCHAR(8),
    RUN_DATE         DATE,
    BACKFILL         VARCHAR(1) NOT NULL DEFAULT 'N',
    RUN_KEY          VARCHAR NOT NULL DEFAULT ('manual:' || lower(hex(randomblob(16)))),
    TRIGGER_KIND     VARCHAR NOT NULL DEFAULT 'MANUAL',
    OWNER_ID         VARCHAR,
    LEASE_EXPIRES_AT TIMESTAMP,
    OUTPUT_REVISION  INT NOT NULL DEFAULT 1,
    CONFIG_SHA256    VARCHAR(64),
    STARTED_BY VARCHAR DEFAULT (etl_craft_actor()),
    STARTED_BY_KIND VARCHAR DEFAULT (etl_craft_actor_kind()) CONSTRAINT ck_aud_pipelines_run_log_started_by_kind CHECK (STARTED_BY_KIND IN ('HUMAN','SCHEDULE','ORCHESTRATOR','WORKER','SYSTEM')),
    ENDED_BY VARCHAR,
    ENDED_BY_KIND VARCHAR CONSTRAINT ck_aud_pipelines_run_log_ended_by_kind CHECK (ENDED_BY_KIND IN ('HUMAN','SCHEDULE','ORCHESTRATOR','WORKER','SYSTEM')),
    CONSTRAINT ck_pipeline_run_trigger_kind CHECK (TRIGGER_KIND IN ('SCHEDULE','MANUAL','BACKFILL','ORCHESTRATOR','STAND_IN')),
    CONSTRAINT ck_pipeline_run_backfill CHECK (BACKFILL IN ('Y','N')),
    CONSTRAINT ck_pipeline_run_status CHECK (STATUS IN ('IN-PROGRESS','SUCCESS','FAILED','SKIPPED','CANCELLED')),
    CONSTRAINT ck_pipeline_run_sla_status CHECK (SLA_STATUS IN ('MET','BREACHED'))
);
INSERT INTO AUD_PIPELINES_RUN_LOG_new (PIPELINE_RUN_ID, PIPELINE_ID, START_DATE, END_DATE, STATUS, SLA_STATUS, RUN_DATE, BACKFILL, RUN_KEY, TRIGGER_KIND, OWNER_ID, LEASE_EXPIRES_AT, OUTPUT_REVISION, CONFIG_SHA256, STARTED_BY, STARTED_BY_KIND, ENDED_BY, ENDED_BY_KIND) SELECT PIPELINE_RUN_ID, PIPELINE_ID, START_DATE, END_DATE, STATUS, SLA_STATUS, RUN_DATE, BACKFILL, RUN_KEY, TRIGGER_KIND, OWNER_ID, LEASE_EXPIRES_AT, OUTPUT_REVISION, CONFIG_SHA256, NULL, NULL, NULL, NULL FROM AUD_PIPELINES_RUN_LOG;
DROP TABLE AUD_PIPELINES_RUN_LOG;
ALTER TABLE AUD_PIPELINES_RUN_LOG_new RENAME TO AUD_PIPELINES_RUN_LOG;
UPDATE sqlite_sequence SET seq=max(seq, coalesce((SELECT seq FROM etl_craft_actor_sequence), seq)) WHERE name='AUD_PIPELINES_RUN_LOG';
INSERT INTO sqlite_sequence (name, seq) SELECT name, seq FROM etl_craft_actor_sequence s WHERE NOT EXISTS (SELECT 1 FROM sqlite_sequence c WHERE c.name=s.name);
DROP TABLE etl_craft_actor_sequence;
CREATE UNIQUE INDEX ux_pipeline_run_one_active
    ON AUD_PIPELINES_RUN_LOG (PIPELINE_ID) WHERE STATUS = 'IN-PROGRESS';
CREATE INDEX ix_pipeline_run_pipeline ON AUD_PIPELINES_RUN_LOG (PIPELINE_ID);
CREATE UNIQUE INDEX ux_pipeline_run_key ON AUD_PIPELINES_RUN_LOG (PIPELINE_ID, RUN_KEY);
CREATE TEMP TABLE etl_craft_actor_sequence AS SELECT name, seq FROM sqlite_sequence WHERE name = 'AUD_TASK_ATTEMPTS';
CREATE TABLE AUD_TASK_ATTEMPTS_new (
    ATTEMPT_ID       INTEGER PRIMARY KEY AUTOINCREMENT,
    TASK_RUN_ID      BIGINT NOT NULL REFERENCES AUD_TASK_RUN_LOG(TASK_RUN_ID),
    ATTEMPT_NUMBER   INT NOT NULL,
    STATUS           VARCHAR NOT NULL,
    OWNER_ID         VARCHAR,
    LEASE_EXPIRES_AT TIMESTAMP,
    HEARTBEAT_AT     TIMESTAMP,
    QUEUED_AT        TIMESTAMP,
    CLAIMED_AT       TIMESTAMP,
    STARTED_AT       TIMESTAMP,
    ENDED_AT         TIMESTAMP,
    HOST             VARCHAR,
    PID              INT,
    PROCESS_START    VARCHAR,
    EXIT_CODE        INT,
    SOURCE_COUNT     BIGINT,
    TARGET_COUNT     BIGINT,
    INSERT_COUNT     BIGINT,
    UPDATE_COUNT     BIGINT,
    DELETE_COUNT     BIGINT,
    ROWS_WRITTEN     BIGINT,
    ERROR_MESSAGE    VARCHAR,
    TASK_LOG         VARCHAR,
    LOG_PATH         VARCHAR,
    REQUESTED_BY     VARCHAR DEFAULT (etl_craft_actor()),
    REQUESTED_BY_KIND VARCHAR DEFAULT (etl_craft_actor_kind()) CONSTRAINT ck_aud_task_attempts_requested_by_kind CHECK (REQUESTED_BY_KIND IN ('HUMAN','SCHEDULE','ORCHESTRATOR','WORKER','SYSTEM')),
    CONSTRAINT ck_attempt_status CHECK (STATUS IN ('QUEUED','CLAIMED','RUNNING','SUCCESS','FAILED','TIMED_OUT','CANCELLED','LOST'))
);
INSERT INTO AUD_TASK_ATTEMPTS_new (ATTEMPT_ID, TASK_RUN_ID, ATTEMPT_NUMBER, STATUS, OWNER_ID, LEASE_EXPIRES_AT, HEARTBEAT_AT, QUEUED_AT, CLAIMED_AT, STARTED_AT, ENDED_AT, HOST, PID, PROCESS_START, EXIT_CODE, SOURCE_COUNT, TARGET_COUNT, INSERT_COUNT, UPDATE_COUNT, DELETE_COUNT, ROWS_WRITTEN, ERROR_MESSAGE, TASK_LOG, LOG_PATH, REQUESTED_BY, REQUESTED_BY_KIND) SELECT ATTEMPT_ID, TASK_RUN_ID, ATTEMPT_NUMBER, STATUS, OWNER_ID, LEASE_EXPIRES_AT, HEARTBEAT_AT, QUEUED_AT, CLAIMED_AT, STARTED_AT, ENDED_AT, HOST, PID, PROCESS_START, EXIT_CODE, SOURCE_COUNT, TARGET_COUNT, INSERT_COUNT, UPDATE_COUNT, DELETE_COUNT, ROWS_WRITTEN, ERROR_MESSAGE, TASK_LOG, LOG_PATH, REQUESTED_BY, NULL FROM AUD_TASK_ATTEMPTS;
DROP TABLE AUD_TASK_ATTEMPTS;
ALTER TABLE AUD_TASK_ATTEMPTS_new RENAME TO AUD_TASK_ATTEMPTS;
UPDATE sqlite_sequence SET seq=max(seq, coalesce((SELECT seq FROM etl_craft_actor_sequence), seq)) WHERE name='AUD_TASK_ATTEMPTS';
INSERT INTO sqlite_sequence (name, seq) SELECT name, seq FROM etl_craft_actor_sequence s WHERE NOT EXISTS (SELECT 1 FROM sqlite_sequence c WHERE c.name=s.name);
DROP TABLE etl_craft_actor_sequence;
CREATE UNIQUE INDEX ux_attempt_number ON AUD_TASK_ATTEMPTS (TASK_RUN_ID, ATTEMPT_NUMBER);
CREATE UNIQUE INDEX ux_attempt_one_active ON AUD_TASK_ATTEMPTS (TASK_RUN_ID)
    WHERE STATUS IN ('QUEUED','CLAIMED','RUNNING');
CREATE INDEX ix_attempt_status_lease ON AUD_TASK_ATTEMPTS (STATUS, LEASE_EXPIRES_AT);
CREATE TEMP TABLE etl_craft_actor_sequence AS SELECT name, seq FROM sqlite_sequence WHERE name = 'AUD_RUN_INTERVENTIONS';
CREATE TABLE AUD_RUN_INTERVENTIONS_new (
    INTERVENTION_ID   INTEGER PRIMARY KEY AUTOINCREMENT,
    PIPELINE_ID       BIGINT NOT NULL REFERENCES CFG_PIPELINES(PIPELINE_ID),
    PIPELINE_RUN_ID   BIGINT NOT NULL REFERENCES AUD_PIPELINES_RUN_LOG(PIPELINE_RUN_ID),
    TASK_ID           BIGINT REFERENCES CFG_TASKS(TASK_ID),
    ACTION            VARCHAR(24) NOT NULL,
    FROM_STATUS       VARCHAR(16),
    TO_STATUS         VARCHAR(16),
    TARGET_COUNT      BIGINT,
    PREVIOUS_MESSAGE  VARCHAR,
    REASON            VARCHAR NOT NULL,
    REQUESTED_BY      VARCHAR NOT NULL,
    REQUESTED_AT      TIMESTAMP NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'),
    REQUESTED_BY_KIND VARCHAR DEFAULT (etl_craft_actor_kind()) CONSTRAINT ck_aud_run_interventions_requested_by_kind CHECK (REQUESTED_BY_KIND IN ('HUMAN','SCHEDULE','ORCHESTRATOR','WORKER','SYSTEM')),
    CONSTRAINT ck_intervention_action CHECK (ACTION IN ('MARK','NEW_RUN','CANCEL','REOPEN','RESET',
                                                     'GATE_BYPASS','IGNORE_DEPENDENCIES','RERUN'))
);
INSERT INTO AUD_RUN_INTERVENTIONS_new (INTERVENTION_ID, PIPELINE_ID, PIPELINE_RUN_ID, TASK_ID, ACTION, FROM_STATUS, TO_STATUS, TARGET_COUNT, PREVIOUS_MESSAGE, REASON, REQUESTED_BY, REQUESTED_AT, REQUESTED_BY_KIND) SELECT INTERVENTION_ID, PIPELINE_ID, PIPELINE_RUN_ID, TASK_ID, ACTION, FROM_STATUS, TO_STATUS, TARGET_COUNT, PREVIOUS_MESSAGE, REASON, REQUESTED_BY, REQUESTED_AT, NULL FROM AUD_RUN_INTERVENTIONS;
DROP TABLE AUD_RUN_INTERVENTIONS;
ALTER TABLE AUD_RUN_INTERVENTIONS_new RENAME TO AUD_RUN_INTERVENTIONS;
UPDATE sqlite_sequence SET seq=max(seq, coalesce((SELECT seq FROM etl_craft_actor_sequence), seq)) WHERE name='AUD_RUN_INTERVENTIONS';
INSERT INTO sqlite_sequence (name, seq) SELECT name, seq FROM etl_craft_actor_sequence s WHERE NOT EXISTS (SELECT 1 FROM sqlite_sequence c WHERE c.name=s.name);
DROP TABLE etl_craft_actor_sequence;
CREATE INDEX ix_run_interventions_run ON AUD_RUN_INTERVENTIONS (PIPELINE_RUN_ID);
CREATE TEMP TABLE etl_craft_actor_sequence AS SELECT name, seq FROM sqlite_sequence WHERE name = 'AUD_PIPELINE_PAUSES';
CREATE TABLE AUD_PIPELINE_PAUSES_new (
    PIPELINE_PAUSE_ID  INTEGER PRIMARY KEY AUTOINCREMENT,
    PIPELINE_ID        BIGINT NOT NULL REFERENCES CFG_PIPELINES(PIPELINE_ID),
    PAUSED_AT          TIMESTAMP NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'),
    PAUSED_BY          VARCHAR NOT NULL,
    REASON             VARCHAR NOT NULL,
    RESUMED_AT         TIMESTAMP,
    RESUMED_BY         VARCHAR,
    RESUME_REASON      VARCHAR,
    PAUSED_BY_KIND VARCHAR DEFAULT (etl_craft_actor_kind()) CONSTRAINT ck_aud_pipeline_pauses_paused_by_kind CHECK (PAUSED_BY_KIND IN ('HUMAN','SCHEDULE','ORCHESTRATOR','WORKER','SYSTEM')),
    RESUMED_BY_KIND VARCHAR CONSTRAINT ck_aud_pipeline_pauses_resumed_by_kind CHECK (RESUMED_BY_KIND IN ('HUMAN','SCHEDULE','ORCHESTRATOR','WORKER','SYSTEM'))
);
INSERT INTO AUD_PIPELINE_PAUSES_new (PIPELINE_PAUSE_ID, PIPELINE_ID, PAUSED_AT, PAUSED_BY, REASON, RESUMED_AT, RESUMED_BY, RESUME_REASON, PAUSED_BY_KIND, RESUMED_BY_KIND) SELECT PIPELINE_PAUSE_ID, PIPELINE_ID, PAUSED_AT, PAUSED_BY, REASON, RESUMED_AT, RESUMED_BY, RESUME_REASON, NULL, NULL FROM AUD_PIPELINE_PAUSES;
DROP TABLE AUD_PIPELINE_PAUSES;
ALTER TABLE AUD_PIPELINE_PAUSES_new RENAME TO AUD_PIPELINE_PAUSES;
UPDATE sqlite_sequence SET seq=max(seq, coalesce((SELECT seq FROM etl_craft_actor_sequence), seq)) WHERE name='AUD_PIPELINE_PAUSES';
INSERT INTO sqlite_sequence (name, seq) SELECT name, seq FROM etl_craft_actor_sequence s WHERE NOT EXISTS (SELECT 1 FROM sqlite_sequence c WHERE c.name=s.name);
DROP TABLE etl_craft_actor_sequence;
CREATE UNIQUE INDEX ux_pipeline_pauses_open
    ON AUD_PIPELINE_PAUSES (PIPELINE_ID) WHERE RESUMED_AT IS NULL;
-- Attribute actions without inventing identities for historical rows.

-- An immutable record of a command request; run and attempt outcomes live in their own logs.
CREATE TABLE AUD_ACTIONS (
    ACTION_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    STARTED_AT TIMESTAMP NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'),
    ENDED_AT TIMESTAMP NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'),
    ACTOR VARCHAR NOT NULL,
    ACTOR_KIND VARCHAR NOT NULL,
    HOST VARCHAR NOT NULL,
    COMMAND VARCHAR NOT NULL,
    ARGUMENTS TEXT NOT NULL,
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
    CHANGE_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    CHANGED_AT TIMESTAMP NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'),
    ACTOR VARCHAR NOT NULL,
    ACTOR_KIND VARCHAR NOT NULL,
    TABLE_NAME VARCHAR NOT NULL,
    ROW_KEY VARCHAR NOT NULL,
    OPERATION VARCHAR NOT NULL,
    BEFORE_JSON TEXT,
    AFTER_JSON TEXT,
    MIGRATION VARCHAR,
    CONSTRAINT ck_metadata_operation CHECK (OPERATION IN ('INSERT','UPDATE','DELETE')),
    CONSTRAINT ck_metadata_actor_kind CHECK (ACTOR_KIND IN ('HUMAN','SCHEDULE','ORCHESTRATOR','WORKER','SYSTEM'))
);
CREATE INDEX ix_metadata_changes_table ON AUD_METADATA_CHANGES (TABLE_NAME, CHANGED_AT);
DROP TRIGGER trg_audit_cfg_pipelines;
DROP TRIGGER trg_audit_cfg_pipeline_dependency;
DROP TRIGGER trg_audit_cfg_tasks;
DROP TRIGGER trg_audit_cfg_task_dependency;
DROP TRIGGER trg_audit_cfg_task_parameters;
DROP TRIGGER trg_audit_cfg_business_rules;
CREATE TRIGGER trg_actor_guard_cfg_pipelines_insert BEFORE INSERT ON CFG_PIPELINES
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'CFG_PIPELINES is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
    SELECT etl_craft_enter_insert('CFG_PIPELINES');
END;
CREATE TRIGGER trg_actor_guard_cfg_pipelines_update BEFORE UPDATE ON CFG_PIPELINES
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'CFG_PIPELINES is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_cfg_pipelines_delete BEFORE DELETE ON CFG_PIPELINES
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'CFG_PIPELINES is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_cfg_pipeline_dependency_insert BEFORE INSERT ON CFG_PIPELINE_DEPENDENCY
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'CFG_PIPELINE_DEPENDENCY is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
    SELECT etl_craft_enter_insert('CFG_PIPELINE_DEPENDENCY');
END;
CREATE TRIGGER trg_actor_guard_cfg_pipeline_dependency_update BEFORE UPDATE ON CFG_PIPELINE_DEPENDENCY
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'CFG_PIPELINE_DEPENDENCY is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_cfg_pipeline_dependency_delete BEFORE DELETE ON CFG_PIPELINE_DEPENDENCY
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'CFG_PIPELINE_DEPENDENCY is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
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
CREATE TRIGGER trg_actor_guard_cfg_task_dependency_insert BEFORE INSERT ON CFG_TASK_DEPENDENCY
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'CFG_TASK_DEPENDENCY is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
    SELECT etl_craft_enter_insert('CFG_TASK_DEPENDENCY');
END;
CREATE TRIGGER trg_actor_guard_cfg_task_dependency_update BEFORE UPDATE ON CFG_TASK_DEPENDENCY
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'CFG_TASK_DEPENDENCY is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_cfg_task_dependency_delete BEFORE DELETE ON CFG_TASK_DEPENDENCY
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'CFG_TASK_DEPENDENCY is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_cfg_task_parameters_insert BEFORE INSERT ON CFG_TASK_PARAMETERS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'CFG_TASK_PARAMETERS is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
    SELECT etl_craft_enter_insert('CFG_TASK_PARAMETERS');
END;
CREATE TRIGGER trg_actor_guard_cfg_task_parameters_update BEFORE UPDATE ON CFG_TASK_PARAMETERS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'CFG_TASK_PARAMETERS is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_cfg_task_parameters_delete BEFORE DELETE ON CFG_TASK_PARAMETERS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'CFG_TASK_PARAMETERS is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_cfg_business_rules_insert BEFORE INSERT ON CFG_BUSINESS_RULES
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'CFG_BUSINESS_RULES is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
    SELECT etl_craft_enter_insert('CFG_BUSINESS_RULES');
END;
CREATE TRIGGER trg_actor_guard_cfg_business_rules_update BEFORE UPDATE ON CFG_BUSINESS_RULES
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'CFG_BUSINESS_RULES is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_cfg_business_rules_delete BEFORE DELETE ON CFG_BUSINESS_RULES
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'CFG_BUSINESS_RULES is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_pipelines_run_log_insert BEFORE INSERT ON AUD_PIPELINES_RUN_LOG
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_PIPELINES_RUN_LOG is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_pipelines_run_log_update BEFORE UPDATE ON AUD_PIPELINES_RUN_LOG
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_PIPELINES_RUN_LOG is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_pipelines_run_log_delete BEFORE DELETE ON AUD_PIPELINES_RUN_LOG
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_PIPELINES_RUN_LOG is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_task_run_log_insert BEFORE INSERT ON AUD_TASK_RUN_LOG
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_TASK_RUN_LOG is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_task_run_log_update BEFORE UPDATE ON AUD_TASK_RUN_LOG
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_TASK_RUN_LOG is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_task_run_log_delete BEFORE DELETE ON AUD_TASK_RUN_LOG
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_TASK_RUN_LOG is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_run_interventions_insert BEFORE INSERT ON AUD_RUN_INTERVENTIONS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_RUN_INTERVENTIONS is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_run_interventions_update BEFORE UPDATE ON AUD_RUN_INTERVENTIONS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_RUN_INTERVENTIONS is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
    SELECT CASE WHEN etl_craft_purpose() <> 'retention' THEN RAISE(ABORT, 'AUD_RUN_INTERVENTIONS immutable history; use a new action or etl-craft retention') END;
END;
CREATE TRIGGER trg_actor_guard_aud_run_interventions_delete BEFORE DELETE ON AUD_RUN_INTERVENTIONS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_RUN_INTERVENTIONS is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
    SELECT CASE WHEN etl_craft_purpose() <> 'retention' THEN RAISE(ABORT, 'AUD_RUN_INTERVENTIONS immutable history; use a new action or etl-craft retention') END;
END;
CREATE TRIGGER trg_actor_guard_aud_pipeline_pauses_insert BEFORE INSERT ON AUD_PIPELINE_PAUSES
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_PIPELINE_PAUSES is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_pipeline_pauses_update BEFORE UPDATE ON AUD_PIPELINE_PAUSES
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_PIPELINE_PAUSES is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_pipeline_pauses_delete BEFORE DELETE ON AUD_PIPELINE_PAUSES
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_PIPELINE_PAUSES is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_business_rules_run_log_insert BEFORE INSERT ON AUD_BUSINESS_RULES_RUN_LOG
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_BUSINESS_RULES_RUN_LOG is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_business_rules_run_log_update BEFORE UPDATE ON AUD_BUSINESS_RULES_RUN_LOG
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_BUSINESS_RULES_RUN_LOG is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_business_rules_run_log_delete BEFORE DELETE ON AUD_BUSINESS_RULES_RUN_LOG
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_BUSINESS_RULES_RUN_LOG is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_business_rules_results_insert BEFORE INSERT ON AUD_BUSINESS_RULES_RESULTS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_BUSINESS_RULES_RESULTS is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_business_rules_results_update BEFORE UPDATE ON AUD_BUSINESS_RULES_RESULTS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_BUSINESS_RULES_RESULTS is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_business_rules_results_delete BEFORE DELETE ON AUD_BUSINESS_RULES_RESULTS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_BUSINESS_RULES_RESULTS is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_task_offset_tracker_insert BEFORE INSERT ON AUD_TASK_OFFSET_TRACKER
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_TASK_OFFSET_TRACKER is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_task_offset_tracker_update BEFORE UPDATE ON AUD_TASK_OFFSET_TRACKER
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_TASK_OFFSET_TRACKER is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_task_offset_tracker_delete BEFORE DELETE ON AUD_TASK_OFFSET_TRACKER
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_TASK_OFFSET_TRACKER is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_column_lineage_insert BEFORE INSERT ON AUD_COLUMN_LINEAGE
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_COLUMN_LINEAGE is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_column_lineage_update BEFORE UPDATE ON AUD_COLUMN_LINEAGE
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_COLUMN_LINEAGE is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_column_lineage_delete BEFORE DELETE ON AUD_COLUMN_LINEAGE
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_COLUMN_LINEAGE is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_docs_publication_insert BEFORE INSERT ON AUD_DOCS_PUBLICATION
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_DOCS_PUBLICATION is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_docs_publication_update BEFORE UPDATE ON AUD_DOCS_PUBLICATION
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_DOCS_PUBLICATION is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_docs_publication_delete BEFORE DELETE ON AUD_DOCS_PUBLICATION
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_DOCS_PUBLICATION is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_task_documentation_insert BEFORE INSERT ON AUD_TASK_DOCUMENTATION
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_TASK_DOCUMENTATION is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_task_documentation_update BEFORE UPDATE ON AUD_TASK_DOCUMENTATION
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_TASK_DOCUMENTATION is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_task_documentation_delete BEFORE DELETE ON AUD_TASK_DOCUMENTATION
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_TASK_DOCUMENTATION is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_dependency_consumption_insert BEFORE INSERT ON AUD_DEPENDENCY_CONSUMPTION
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_DEPENDENCY_CONSUMPTION is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_dependency_consumption_update BEFORE UPDATE ON AUD_DEPENDENCY_CONSUMPTION
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_DEPENDENCY_CONSUMPTION is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
    SELECT CASE WHEN etl_craft_purpose() <> 'retention' THEN RAISE(ABORT, 'AUD_DEPENDENCY_CONSUMPTION immutable history; use a new action or etl-craft retention') END;
END;
CREATE TRIGGER trg_actor_guard_aud_dependency_consumption_delete BEFORE DELETE ON AUD_DEPENDENCY_CONSUMPTION
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_DEPENDENCY_CONSUMPTION is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
    SELECT CASE WHEN etl_craft_purpose() <> 'retention' THEN RAISE(ABORT, 'AUD_DEPENDENCY_CONSUMPTION immutable history; use a new action or etl-craft retention') END;
END;
CREATE TRIGGER trg_actor_guard_aud_task_attempts_insert BEFORE INSERT ON AUD_TASK_ATTEMPTS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_TASK_ATTEMPTS is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_task_attempts_update BEFORE UPDATE ON AUD_TASK_ATTEMPTS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_TASK_ATTEMPTS is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
    SELECT CASE WHEN etl_craft_purpose() <> 'retention' AND OLD.STATUS NOT IN ('QUEUED','CLAIMED','RUNNING') THEN RAISE(ABORT, 'AUD_TASK_ATTEMPTS immutable history; use a new action or etl-craft retention') END;
END;
CREATE TRIGGER trg_actor_guard_aud_task_attempts_delete BEFORE DELETE ON AUD_TASK_ATTEMPTS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_TASK_ATTEMPTS is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
    SELECT CASE WHEN etl_craft_purpose() <> 'retention' AND OLD.STATUS NOT IN ('QUEUED','CLAIMED','RUNNING') THEN RAISE(ABORT, 'AUD_TASK_ATTEMPTS immutable history; use a new action or etl-craft retention') END;
END;
CREATE TRIGGER trg_actor_guard_aud_gate_decisions_insert BEFORE INSERT ON AUD_GATE_DECISIONS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_GATE_DECISIONS is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_gate_decisions_update BEFORE UPDATE ON AUD_GATE_DECISIONS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_GATE_DECISIONS is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
    SELECT CASE WHEN etl_craft_purpose() <> 'retention' THEN RAISE(ABORT, 'AUD_GATE_DECISIONS immutable history; use a new action or etl-craft retention') END;
END;
CREATE TRIGGER trg_actor_guard_aud_gate_decisions_delete BEFORE DELETE ON AUD_GATE_DECISIONS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_GATE_DECISIONS is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
    SELECT CASE WHEN etl_craft_purpose() <> 'retention' THEN RAISE(ABORT, 'AUD_GATE_DECISIONS immutable history; use a new action or etl-craft retention') END;
END;
CREATE TRIGGER trg_actor_guard_aud_actions_insert BEFORE INSERT ON AUD_ACTIONS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_ACTIONS is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_actions_update BEFORE UPDATE ON AUD_ACTIONS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_ACTIONS is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
    SELECT CASE WHEN etl_craft_purpose() <> 'retention' THEN RAISE(ABORT, 'AUD_ACTIONS immutable history; use a new action or etl-craft retention') END;
END;
CREATE TRIGGER trg_actor_guard_aud_actions_delete BEFORE DELETE ON AUD_ACTIONS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_ACTIONS is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
    SELECT CASE WHEN etl_craft_purpose() <> 'retention' THEN RAISE(ABORT, 'AUD_ACTIONS immutable history; use a new action or etl-craft retention') END;
END;
CREATE TRIGGER trg_actor_guard_aud_metadata_changes_insert BEFORE INSERT ON AUD_METADATA_CHANGES
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_METADATA_CHANGES is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_metadata_changes_update BEFORE UPDATE ON AUD_METADATA_CHANGES
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_METADATA_CHANGES is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
    SELECT CASE WHEN etl_craft_purpose() <> 'retention' THEN RAISE(ABORT, 'AUD_METADATA_CHANGES immutable history; use a new action or etl-craft retention') END;
END;
CREATE TRIGGER trg_actor_guard_aud_metadata_changes_delete BEFORE DELETE ON AUD_METADATA_CHANGES
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_METADATA_CHANGES is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
    SELECT CASE WHEN etl_craft_purpose() <> 'retention' THEN RAISE(ABORT, 'AUD_METADATA_CHANGES immutable history; use a new action or etl-craft retention') END;
END;
CREATE TRIGGER trg_audit_cfg_pipelines_insert AFTER INSERT ON CFG_PIPELINES
BEGIN
    UPDATE CFG_PIPELINES SET CREATED_BY=etl_craft_actor(), CREATE_DATE=(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'), UPDATED_BY=etl_craft_actor(), UPDATED_DATE=(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00') WHERE PIPELINE_ID=NEW.PIPELINE_ID;
    INSERT INTO AUD_METADATA_CHANGES (ACTOR, ACTOR_KIND, TABLE_NAME, ROW_KEY, OPERATION, BEFORE_JSON, AFTER_JSON, MIGRATION) VALUES (etl_craft_actor(), etl_craft_actor_kind(), 'CFG_PIPELINES', CAST(NEW.PIPELINE_ID AS TEXT), 'INSERT', NULL, (SELECT json_object('pipeline_id', row.PIPELINE_ID, 'pipeline_code', row.PIPELINE_CODE, 'pipeline_name', row.PIPELINE_NAME, 'description', row.DESCRIPTION, 'run_schedule', row.RUN_SCHEDULE, 'sla_in_hours', row.SLA_IN_HOURS, 'refresh_type', row.REFRESH_TYPE, 'active_flag', row.ACTIVE_FLAG, 'pipeline_parameters', row.PIPELINE_PARAMETERS, 'created_by', row.CREATED_BY, 'create_date', row.CREATE_DATE, 'updated_by', row.UPDATED_BY, 'updated_date', row.UPDATED_DATE) FROM CFG_PIPELINES row WHERE row.PIPELINE_ID=NEW.PIPELINE_ID), NULLIF(etl_craft_migration(), ''));
    SELECT etl_craft_exit_insert('CFG_PIPELINES');
END;
CREATE TRIGGER trg_audit_cfg_pipelines_update AFTER UPDATE ON CFG_PIPELINES
BEGIN
    UPDATE CFG_PIPELINES SET CREATED_BY=OLD.CREATED_BY, CREATE_DATE=OLD.CREATE_DATE, UPDATED_BY=etl_craft_actor(), UPDATED_DATE=(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00') WHERE PIPELINE_ID=NEW.PIPELINE_ID AND NOT etl_craft_inserting('CFG_PIPELINES');
    INSERT INTO AUD_METADATA_CHANGES (ACTOR, ACTOR_KIND, TABLE_NAME, ROW_KEY, OPERATION, BEFORE_JSON, AFTER_JSON, MIGRATION) SELECT etl_craft_actor(), etl_craft_actor_kind(), 'CFG_PIPELINES', CAST(NEW.PIPELINE_ID AS TEXT), 'UPDATE', json_object('pipeline_id', OLD.PIPELINE_ID, 'pipeline_code', OLD.PIPELINE_CODE, 'pipeline_name', OLD.PIPELINE_NAME, 'description', OLD.DESCRIPTION, 'run_schedule', OLD.RUN_SCHEDULE, 'sla_in_hours', OLD.SLA_IN_HOURS, 'refresh_type', OLD.REFRESH_TYPE, 'active_flag', OLD.ACTIVE_FLAG, 'pipeline_parameters', OLD.PIPELINE_PARAMETERS, 'created_by', OLD.CREATED_BY, 'create_date', OLD.CREATE_DATE, 'updated_by', OLD.UPDATED_BY, 'updated_date', OLD.UPDATED_DATE), (SELECT json_object('pipeline_id', row.PIPELINE_ID, 'pipeline_code', row.PIPELINE_CODE, 'pipeline_name', row.PIPELINE_NAME, 'description', row.DESCRIPTION, 'run_schedule', row.RUN_SCHEDULE, 'sla_in_hours', row.SLA_IN_HOURS, 'refresh_type', row.REFRESH_TYPE, 'active_flag', row.ACTIVE_FLAG, 'pipeline_parameters', row.PIPELINE_PARAMETERS, 'created_by', row.CREATED_BY, 'create_date', row.CREATE_DATE, 'updated_by', row.UPDATED_BY, 'updated_date', row.UPDATED_DATE) FROM CFG_PIPELINES row WHERE row.PIPELINE_ID=NEW.PIPELINE_ID), NULLIF(etl_craft_migration(), '') WHERE (NEW.PIPELINE_ID IS NOT OLD.PIPELINE_ID OR NEW.PIPELINE_CODE IS NOT OLD.PIPELINE_CODE OR NEW.PIPELINE_NAME IS NOT OLD.PIPELINE_NAME OR NEW.DESCRIPTION IS NOT OLD.DESCRIPTION OR NEW.RUN_SCHEDULE IS NOT OLD.RUN_SCHEDULE OR NEW.SLA_IN_HOURS IS NOT OLD.SLA_IN_HOURS OR NEW.REFRESH_TYPE IS NOT OLD.REFRESH_TYPE OR NEW.ACTIVE_FLAG IS NOT OLD.ACTIVE_FLAG OR NEW.PIPELINE_PARAMETERS IS NOT OLD.PIPELINE_PARAMETERS) AND NOT etl_craft_inserting('CFG_PIPELINES');
END;
CREATE TRIGGER trg_audit_cfg_pipelines_delete AFTER DELETE ON CFG_PIPELINES
BEGIN
    INSERT INTO AUD_METADATA_CHANGES (ACTOR, ACTOR_KIND, TABLE_NAME, ROW_KEY, OPERATION, BEFORE_JSON, AFTER_JSON, MIGRATION) VALUES (etl_craft_actor(), etl_craft_actor_kind(), 'CFG_PIPELINES', CAST(OLD.PIPELINE_ID AS TEXT), 'DELETE', json_object('pipeline_id', OLD.PIPELINE_ID, 'pipeline_code', OLD.PIPELINE_CODE, 'pipeline_name', OLD.PIPELINE_NAME, 'description', OLD.DESCRIPTION, 'run_schedule', OLD.RUN_SCHEDULE, 'sla_in_hours', OLD.SLA_IN_HOURS, 'refresh_type', OLD.REFRESH_TYPE, 'active_flag', OLD.ACTIVE_FLAG, 'pipeline_parameters', OLD.PIPELINE_PARAMETERS, 'created_by', OLD.CREATED_BY, 'create_date', OLD.CREATE_DATE, 'updated_by', OLD.UPDATED_BY, 'updated_date', OLD.UPDATED_DATE), NULL, NULLIF(etl_craft_migration(), ''));
END;
CREATE TRIGGER trg_audit_cfg_pipeline_dependency_insert AFTER INSERT ON CFG_PIPELINE_DEPENDENCY
BEGIN
    UPDATE CFG_PIPELINE_DEPENDENCY SET CREATED_BY=etl_craft_actor(), CREATE_DATE=(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'), UPDATED_BY=etl_craft_actor(), UPDATED_DATE=(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00') WHERE PIPELINE_DEPENDENCY_ID=NEW.PIPELINE_DEPENDENCY_ID;
    INSERT INTO AUD_METADATA_CHANGES (ACTOR, ACTOR_KIND, TABLE_NAME, ROW_KEY, OPERATION, BEFORE_JSON, AFTER_JSON, MIGRATION) VALUES (etl_craft_actor(), etl_craft_actor_kind(), 'CFG_PIPELINE_DEPENDENCY', CAST(NEW.PIPELINE_DEPENDENCY_ID AS TEXT), 'INSERT', NULL, (SELECT json_object('pipeline_dependency_id', row.PIPELINE_DEPENDENCY_ID, 'pipeline_id', row.PIPELINE_ID, 'depends_on_pipeline_id', row.DEPENDS_ON_PIPELINE_ID, 'dependency_type', row.DEPENDENCY_TYPE, 'active_flag', row.ACTIVE_FLAG, 'created_by', row.CREATED_BY, 'create_date', row.CREATE_DATE, 'updated_by', row.UPDATED_BY, 'updated_date', row.UPDATED_DATE, 'consume_repairs', row.CONSUME_REPAIRS) FROM CFG_PIPELINE_DEPENDENCY row WHERE row.PIPELINE_DEPENDENCY_ID=NEW.PIPELINE_DEPENDENCY_ID), NULLIF(etl_craft_migration(), ''));
    SELECT etl_craft_exit_insert('CFG_PIPELINE_DEPENDENCY');
END;
CREATE TRIGGER trg_audit_cfg_pipeline_dependency_update AFTER UPDATE ON CFG_PIPELINE_DEPENDENCY
BEGIN
    UPDATE CFG_PIPELINE_DEPENDENCY SET CREATED_BY=OLD.CREATED_BY, CREATE_DATE=OLD.CREATE_DATE, UPDATED_BY=etl_craft_actor(), UPDATED_DATE=(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00') WHERE PIPELINE_DEPENDENCY_ID=NEW.PIPELINE_DEPENDENCY_ID AND NOT etl_craft_inserting('CFG_PIPELINE_DEPENDENCY');
    INSERT INTO AUD_METADATA_CHANGES (ACTOR, ACTOR_KIND, TABLE_NAME, ROW_KEY, OPERATION, BEFORE_JSON, AFTER_JSON, MIGRATION) SELECT etl_craft_actor(), etl_craft_actor_kind(), 'CFG_PIPELINE_DEPENDENCY', CAST(NEW.PIPELINE_DEPENDENCY_ID AS TEXT), 'UPDATE', json_object('pipeline_dependency_id', OLD.PIPELINE_DEPENDENCY_ID, 'pipeline_id', OLD.PIPELINE_ID, 'depends_on_pipeline_id', OLD.DEPENDS_ON_PIPELINE_ID, 'dependency_type', OLD.DEPENDENCY_TYPE, 'active_flag', OLD.ACTIVE_FLAG, 'created_by', OLD.CREATED_BY, 'create_date', OLD.CREATE_DATE, 'updated_by', OLD.UPDATED_BY, 'updated_date', OLD.UPDATED_DATE, 'consume_repairs', OLD.CONSUME_REPAIRS), (SELECT json_object('pipeline_dependency_id', row.PIPELINE_DEPENDENCY_ID, 'pipeline_id', row.PIPELINE_ID, 'depends_on_pipeline_id', row.DEPENDS_ON_PIPELINE_ID, 'dependency_type', row.DEPENDENCY_TYPE, 'active_flag', row.ACTIVE_FLAG, 'created_by', row.CREATED_BY, 'create_date', row.CREATE_DATE, 'updated_by', row.UPDATED_BY, 'updated_date', row.UPDATED_DATE, 'consume_repairs', row.CONSUME_REPAIRS) FROM CFG_PIPELINE_DEPENDENCY row WHERE row.PIPELINE_DEPENDENCY_ID=NEW.PIPELINE_DEPENDENCY_ID), NULLIF(etl_craft_migration(), '') WHERE (NEW.PIPELINE_DEPENDENCY_ID IS NOT OLD.PIPELINE_DEPENDENCY_ID OR NEW.PIPELINE_ID IS NOT OLD.PIPELINE_ID OR NEW.DEPENDS_ON_PIPELINE_ID IS NOT OLD.DEPENDS_ON_PIPELINE_ID OR NEW.DEPENDENCY_TYPE IS NOT OLD.DEPENDENCY_TYPE OR NEW.ACTIVE_FLAG IS NOT OLD.ACTIVE_FLAG OR NEW.CONSUME_REPAIRS IS NOT OLD.CONSUME_REPAIRS) AND NOT etl_craft_inserting('CFG_PIPELINE_DEPENDENCY');
END;
CREATE TRIGGER trg_audit_cfg_pipeline_dependency_delete AFTER DELETE ON CFG_PIPELINE_DEPENDENCY
BEGIN
    INSERT INTO AUD_METADATA_CHANGES (ACTOR, ACTOR_KIND, TABLE_NAME, ROW_KEY, OPERATION, BEFORE_JSON, AFTER_JSON, MIGRATION) VALUES (etl_craft_actor(), etl_craft_actor_kind(), 'CFG_PIPELINE_DEPENDENCY', CAST(OLD.PIPELINE_DEPENDENCY_ID AS TEXT), 'DELETE', json_object('pipeline_dependency_id', OLD.PIPELINE_DEPENDENCY_ID, 'pipeline_id', OLD.PIPELINE_ID, 'depends_on_pipeline_id', OLD.DEPENDS_ON_PIPELINE_ID, 'dependency_type', OLD.DEPENDENCY_TYPE, 'active_flag', OLD.ACTIVE_FLAG, 'created_by', OLD.CREATED_BY, 'create_date', OLD.CREATE_DATE, 'updated_by', OLD.UPDATED_BY, 'updated_date', OLD.UPDATED_DATE, 'consume_repairs', OLD.CONSUME_REPAIRS), NULL, NULLIF(etl_craft_migration(), ''));
END;
CREATE TRIGGER trg_audit_cfg_tasks_insert AFTER INSERT ON CFG_TASKS
BEGIN
    UPDATE CFG_TASKS SET CREATED_BY=etl_craft_actor(), CREATE_DATE=(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'), UPDATED_BY=etl_craft_actor(), UPDATED_DATE=(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00') WHERE TASK_ID=NEW.TASK_ID;
    INSERT INTO AUD_METADATA_CHANGES (ACTOR, ACTOR_KIND, TABLE_NAME, ROW_KEY, OPERATION, BEFORE_JSON, AFTER_JSON, MIGRATION) VALUES (etl_craft_actor(), etl_craft_actor_kind(), 'CFG_TASKS', CAST(NEW.TASK_ID AS TEXT), 'INSERT', NULL, (SELECT json_object('task_id', row.TASK_ID, 'task_code', row.TASK_CODE, 'task_type', row.TASK_TYPE, 'pipeline_id', row.PIPELINE_ID, 'handler', row.HANDLER, 'run_condition', row.RUN_CONDITION, 'run_condition_count', row.RUN_CONDITION_COUNT, 'active_flag', row.ACTIVE_FLAG, 'created_by', row.CREATED_BY, 'create_date', row.CREATE_DATE, 'updated_by', row.UPDATED_BY, 'updated_date', row.UPDATED_DATE) FROM CFG_TASKS row WHERE row.TASK_ID=NEW.TASK_ID), NULLIF(etl_craft_migration(), ''));
    SELECT etl_craft_exit_insert('CFG_TASKS');
END;
CREATE TRIGGER trg_audit_cfg_tasks_update AFTER UPDATE ON CFG_TASKS
BEGIN
    UPDATE CFG_TASKS SET CREATED_BY=OLD.CREATED_BY, CREATE_DATE=OLD.CREATE_DATE, UPDATED_BY=etl_craft_actor(), UPDATED_DATE=(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00') WHERE TASK_ID=NEW.TASK_ID AND NOT etl_craft_inserting('CFG_TASKS');
    INSERT INTO AUD_METADATA_CHANGES (ACTOR, ACTOR_KIND, TABLE_NAME, ROW_KEY, OPERATION, BEFORE_JSON, AFTER_JSON, MIGRATION) SELECT etl_craft_actor(), etl_craft_actor_kind(), 'CFG_TASKS', CAST(NEW.TASK_ID AS TEXT), 'UPDATE', json_object('task_id', OLD.TASK_ID, 'task_code', OLD.TASK_CODE, 'task_type', OLD.TASK_TYPE, 'pipeline_id', OLD.PIPELINE_ID, 'handler', OLD.HANDLER, 'run_condition', OLD.RUN_CONDITION, 'run_condition_count', OLD.RUN_CONDITION_COUNT, 'active_flag', OLD.ACTIVE_FLAG, 'created_by', OLD.CREATED_BY, 'create_date', OLD.CREATE_DATE, 'updated_by', OLD.UPDATED_BY, 'updated_date', OLD.UPDATED_DATE), (SELECT json_object('task_id', row.TASK_ID, 'task_code', row.TASK_CODE, 'task_type', row.TASK_TYPE, 'pipeline_id', row.PIPELINE_ID, 'handler', row.HANDLER, 'run_condition', row.RUN_CONDITION, 'run_condition_count', row.RUN_CONDITION_COUNT, 'active_flag', row.ACTIVE_FLAG, 'created_by', row.CREATED_BY, 'create_date', row.CREATE_DATE, 'updated_by', row.UPDATED_BY, 'updated_date', row.UPDATED_DATE) FROM CFG_TASKS row WHERE row.TASK_ID=NEW.TASK_ID), NULLIF(etl_craft_migration(), '') WHERE (NEW.TASK_ID IS NOT OLD.TASK_ID OR NEW.TASK_CODE IS NOT OLD.TASK_CODE OR NEW.TASK_TYPE IS NOT OLD.TASK_TYPE OR NEW.PIPELINE_ID IS NOT OLD.PIPELINE_ID OR NEW.HANDLER IS NOT OLD.HANDLER OR NEW.RUN_CONDITION IS NOT OLD.RUN_CONDITION OR NEW.RUN_CONDITION_COUNT IS NOT OLD.RUN_CONDITION_COUNT OR NEW.ACTIVE_FLAG IS NOT OLD.ACTIVE_FLAG) AND NOT etl_craft_inserting('CFG_TASKS');
END;
CREATE TRIGGER trg_audit_cfg_tasks_delete AFTER DELETE ON CFG_TASKS
BEGIN
    INSERT INTO AUD_METADATA_CHANGES (ACTOR, ACTOR_KIND, TABLE_NAME, ROW_KEY, OPERATION, BEFORE_JSON, AFTER_JSON, MIGRATION) VALUES (etl_craft_actor(), etl_craft_actor_kind(), 'CFG_TASKS', CAST(OLD.TASK_ID AS TEXT), 'DELETE', json_object('task_id', OLD.TASK_ID, 'task_code', OLD.TASK_CODE, 'task_type', OLD.TASK_TYPE, 'pipeline_id', OLD.PIPELINE_ID, 'handler', OLD.HANDLER, 'run_condition', OLD.RUN_CONDITION, 'run_condition_count', OLD.RUN_CONDITION_COUNT, 'active_flag', OLD.ACTIVE_FLAG, 'created_by', OLD.CREATED_BY, 'create_date', OLD.CREATE_DATE, 'updated_by', OLD.UPDATED_BY, 'updated_date', OLD.UPDATED_DATE), NULL, NULLIF(etl_craft_migration(), ''));
END;
CREATE TRIGGER trg_audit_cfg_task_dependency_insert AFTER INSERT ON CFG_TASK_DEPENDENCY
BEGIN
    UPDATE CFG_TASK_DEPENDENCY SET CREATED_BY=etl_craft_actor(), CREATE_DATE=(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'), UPDATED_BY=etl_craft_actor(), UPDATED_DATE=(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'), DEPENDS_ON_PIPELINE_ID=COALESCE(NEW.DEPENDS_ON_PIPELINE_ID, NEW.PIPELINE_ID) WHERE TASK_DEPENDENCY_ID=NEW.TASK_DEPENDENCY_ID;
    INSERT INTO AUD_METADATA_CHANGES (ACTOR, ACTOR_KIND, TABLE_NAME, ROW_KEY, OPERATION, BEFORE_JSON, AFTER_JSON, MIGRATION) VALUES (etl_craft_actor(), etl_craft_actor_kind(), 'CFG_TASK_DEPENDENCY', CAST(NEW.TASK_DEPENDENCY_ID AS TEXT), 'INSERT', NULL, (SELECT json_object('task_dependency_id', row.TASK_DEPENDENCY_ID, 'pipeline_id', row.PIPELINE_ID, 'task_id', row.TASK_ID, 'depends_on_pipeline_id', row.DEPENDS_ON_PIPELINE_ID, 'depends_on_task_id', row.DEPENDS_ON_TASK_ID, 'dependency_type', row.DEPENDENCY_TYPE, 'active_flag', row.ACTIVE_FLAG, 'created_by', row.CREATED_BY, 'create_date', row.CREATE_DATE, 'updated_by', row.UPDATED_BY, 'updated_date', row.UPDATED_DATE, 'consume_repairs', row.CONSUME_REPAIRS) FROM CFG_TASK_DEPENDENCY row WHERE row.TASK_DEPENDENCY_ID=NEW.TASK_DEPENDENCY_ID), NULLIF(etl_craft_migration(), ''));
    SELECT etl_craft_exit_insert('CFG_TASK_DEPENDENCY');
END;
CREATE TRIGGER trg_audit_cfg_task_dependency_update AFTER UPDATE ON CFG_TASK_DEPENDENCY
BEGIN
    UPDATE CFG_TASK_DEPENDENCY SET CREATED_BY=OLD.CREATED_BY, CREATE_DATE=OLD.CREATE_DATE, UPDATED_BY=etl_craft_actor(), UPDATED_DATE=(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'), DEPENDS_ON_PIPELINE_ID=COALESCE(NEW.DEPENDS_ON_PIPELINE_ID, NEW.PIPELINE_ID) WHERE TASK_DEPENDENCY_ID=NEW.TASK_DEPENDENCY_ID AND NOT etl_craft_inserting('CFG_TASK_DEPENDENCY');
    INSERT INTO AUD_METADATA_CHANGES (ACTOR, ACTOR_KIND, TABLE_NAME, ROW_KEY, OPERATION, BEFORE_JSON, AFTER_JSON, MIGRATION) SELECT etl_craft_actor(), etl_craft_actor_kind(), 'CFG_TASK_DEPENDENCY', CAST(NEW.TASK_DEPENDENCY_ID AS TEXT), 'UPDATE', json_object('task_dependency_id', OLD.TASK_DEPENDENCY_ID, 'pipeline_id', OLD.PIPELINE_ID, 'task_id', OLD.TASK_ID, 'depends_on_pipeline_id', OLD.DEPENDS_ON_PIPELINE_ID, 'depends_on_task_id', OLD.DEPENDS_ON_TASK_ID, 'dependency_type', OLD.DEPENDENCY_TYPE, 'active_flag', OLD.ACTIVE_FLAG, 'created_by', OLD.CREATED_BY, 'create_date', OLD.CREATE_DATE, 'updated_by', OLD.UPDATED_BY, 'updated_date', OLD.UPDATED_DATE, 'consume_repairs', OLD.CONSUME_REPAIRS), (SELECT json_object('task_dependency_id', row.TASK_DEPENDENCY_ID, 'pipeline_id', row.PIPELINE_ID, 'task_id', row.TASK_ID, 'depends_on_pipeline_id', row.DEPENDS_ON_PIPELINE_ID, 'depends_on_task_id', row.DEPENDS_ON_TASK_ID, 'dependency_type', row.DEPENDENCY_TYPE, 'active_flag', row.ACTIVE_FLAG, 'created_by', row.CREATED_BY, 'create_date', row.CREATE_DATE, 'updated_by', row.UPDATED_BY, 'updated_date', row.UPDATED_DATE, 'consume_repairs', row.CONSUME_REPAIRS) FROM CFG_TASK_DEPENDENCY row WHERE row.TASK_DEPENDENCY_ID=NEW.TASK_DEPENDENCY_ID), NULLIF(etl_craft_migration(), '') WHERE (NEW.TASK_DEPENDENCY_ID IS NOT OLD.TASK_DEPENDENCY_ID OR NEW.PIPELINE_ID IS NOT OLD.PIPELINE_ID OR NEW.TASK_ID IS NOT OLD.TASK_ID OR NEW.DEPENDS_ON_PIPELINE_ID IS NOT OLD.DEPENDS_ON_PIPELINE_ID OR NEW.DEPENDS_ON_TASK_ID IS NOT OLD.DEPENDS_ON_TASK_ID OR NEW.DEPENDENCY_TYPE IS NOT OLD.DEPENDENCY_TYPE OR NEW.ACTIVE_FLAG IS NOT OLD.ACTIVE_FLAG OR NEW.CONSUME_REPAIRS IS NOT OLD.CONSUME_REPAIRS) AND NOT etl_craft_inserting('CFG_TASK_DEPENDENCY');
END;
CREATE TRIGGER trg_audit_cfg_task_dependency_delete AFTER DELETE ON CFG_TASK_DEPENDENCY
BEGIN
    INSERT INTO AUD_METADATA_CHANGES (ACTOR, ACTOR_KIND, TABLE_NAME, ROW_KEY, OPERATION, BEFORE_JSON, AFTER_JSON, MIGRATION) VALUES (etl_craft_actor(), etl_craft_actor_kind(), 'CFG_TASK_DEPENDENCY', CAST(OLD.TASK_DEPENDENCY_ID AS TEXT), 'DELETE', json_object('task_dependency_id', OLD.TASK_DEPENDENCY_ID, 'pipeline_id', OLD.PIPELINE_ID, 'task_id', OLD.TASK_ID, 'depends_on_pipeline_id', OLD.DEPENDS_ON_PIPELINE_ID, 'depends_on_task_id', OLD.DEPENDS_ON_TASK_ID, 'dependency_type', OLD.DEPENDENCY_TYPE, 'active_flag', OLD.ACTIVE_FLAG, 'created_by', OLD.CREATED_BY, 'create_date', OLD.CREATE_DATE, 'updated_by', OLD.UPDATED_BY, 'updated_date', OLD.UPDATED_DATE, 'consume_repairs', OLD.CONSUME_REPAIRS), NULL, NULLIF(etl_craft_migration(), ''));
END;
CREATE TRIGGER trg_audit_cfg_task_parameters_insert AFTER INSERT ON CFG_TASK_PARAMETERS
BEGIN
    UPDATE CFG_TASK_PARAMETERS SET CREATED_BY=etl_craft_actor(), CREATE_DATE=(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'), UPDATED_BY=etl_craft_actor(), UPDATED_DATE=(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00') WHERE TASK_PARAMETER_ID=NEW.TASK_PARAMETER_ID;
    INSERT INTO AUD_METADATA_CHANGES (ACTOR, ACTOR_KIND, TABLE_NAME, ROW_KEY, OPERATION, BEFORE_JSON, AFTER_JSON, MIGRATION) VALUES (etl_craft_actor(), etl_craft_actor_kind(), 'CFG_TASK_PARAMETERS', CAST(NEW.TASK_PARAMETER_ID AS TEXT), 'INSERT', NULL, (SELECT json_object('task_parameter_id', row.TASK_PARAMETER_ID, 'task_id', row.TASK_ID, 'parameter_name', row.PARAMETER_NAME, 'parameter_value', row.PARAMETER_VALUE, 'active_flag', row.ACTIVE_FLAG, 'created_by', row.CREATED_BY, 'create_date', row.CREATE_DATE, 'updated_by', row.UPDATED_BY, 'updated_date', row.UPDATED_DATE) FROM CFG_TASK_PARAMETERS row WHERE row.TASK_PARAMETER_ID=NEW.TASK_PARAMETER_ID), NULLIF(etl_craft_migration(), ''));
    SELECT etl_craft_exit_insert('CFG_TASK_PARAMETERS');
END;
CREATE TRIGGER trg_audit_cfg_task_parameters_update AFTER UPDATE ON CFG_TASK_PARAMETERS
BEGIN
    UPDATE CFG_TASK_PARAMETERS SET CREATED_BY=OLD.CREATED_BY, CREATE_DATE=OLD.CREATE_DATE, UPDATED_BY=etl_craft_actor(), UPDATED_DATE=(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00') WHERE TASK_PARAMETER_ID=NEW.TASK_PARAMETER_ID AND NOT etl_craft_inserting('CFG_TASK_PARAMETERS');
    INSERT INTO AUD_METADATA_CHANGES (ACTOR, ACTOR_KIND, TABLE_NAME, ROW_KEY, OPERATION, BEFORE_JSON, AFTER_JSON, MIGRATION) SELECT etl_craft_actor(), etl_craft_actor_kind(), 'CFG_TASK_PARAMETERS', CAST(NEW.TASK_PARAMETER_ID AS TEXT), 'UPDATE', json_object('task_parameter_id', OLD.TASK_PARAMETER_ID, 'task_id', OLD.TASK_ID, 'parameter_name', OLD.PARAMETER_NAME, 'parameter_value', OLD.PARAMETER_VALUE, 'active_flag', OLD.ACTIVE_FLAG, 'created_by', OLD.CREATED_BY, 'create_date', OLD.CREATE_DATE, 'updated_by', OLD.UPDATED_BY, 'updated_date', OLD.UPDATED_DATE), (SELECT json_object('task_parameter_id', row.TASK_PARAMETER_ID, 'task_id', row.TASK_ID, 'parameter_name', row.PARAMETER_NAME, 'parameter_value', row.PARAMETER_VALUE, 'active_flag', row.ACTIVE_FLAG, 'created_by', row.CREATED_BY, 'create_date', row.CREATE_DATE, 'updated_by', row.UPDATED_BY, 'updated_date', row.UPDATED_DATE) FROM CFG_TASK_PARAMETERS row WHERE row.TASK_PARAMETER_ID=NEW.TASK_PARAMETER_ID), NULLIF(etl_craft_migration(), '') WHERE (NEW.TASK_PARAMETER_ID IS NOT OLD.TASK_PARAMETER_ID OR NEW.TASK_ID IS NOT OLD.TASK_ID OR NEW.PARAMETER_NAME IS NOT OLD.PARAMETER_NAME OR NEW.PARAMETER_VALUE IS NOT OLD.PARAMETER_VALUE OR NEW.ACTIVE_FLAG IS NOT OLD.ACTIVE_FLAG) AND NOT etl_craft_inserting('CFG_TASK_PARAMETERS');
END;
CREATE TRIGGER trg_audit_cfg_task_parameters_delete AFTER DELETE ON CFG_TASK_PARAMETERS
BEGIN
    INSERT INTO AUD_METADATA_CHANGES (ACTOR, ACTOR_KIND, TABLE_NAME, ROW_KEY, OPERATION, BEFORE_JSON, AFTER_JSON, MIGRATION) VALUES (etl_craft_actor(), etl_craft_actor_kind(), 'CFG_TASK_PARAMETERS', CAST(OLD.TASK_PARAMETER_ID AS TEXT), 'DELETE', json_object('task_parameter_id', OLD.TASK_PARAMETER_ID, 'task_id', OLD.TASK_ID, 'parameter_name', OLD.PARAMETER_NAME, 'parameter_value', OLD.PARAMETER_VALUE, 'active_flag', OLD.ACTIVE_FLAG, 'created_by', OLD.CREATED_BY, 'create_date', OLD.CREATE_DATE, 'updated_by', OLD.UPDATED_BY, 'updated_date', OLD.UPDATED_DATE), NULL, NULLIF(etl_craft_migration(), ''));
END;
CREATE TRIGGER trg_audit_cfg_business_rules_insert AFTER INSERT ON CFG_BUSINESS_RULES
BEGIN
    UPDATE CFG_BUSINESS_RULES SET CREATED_BY=etl_craft_actor(), CREATE_DATE=(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'), UPDATED_BY=etl_craft_actor(), UPDATED_DATE=(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00') WHERE BUSINESS_RULE_ID=NEW.BUSINESS_RULE_ID;
    INSERT INTO AUD_METADATA_CHANGES (ACTOR, ACTOR_KIND, TABLE_NAME, ROW_KEY, OPERATION, BEFORE_JSON, AFTER_JSON, MIGRATION) VALUES (etl_craft_actor(), etl_craft_actor_kind(), 'CFG_BUSINESS_RULES', CAST(NEW.BUSINESS_RULE_ID AS TEXT), 'INSERT', NULL, (SELECT json_object('business_rule_id', row.BUSINESS_RULE_ID, 'business_rule_name', row.BUSINESS_RULE_NAME, 'pipeline_id', row.PIPELINE_ID, 'task_id', row.TASK_ID, 'business_rule_sql', row.BUSINESS_RULE_SQL, 'business_rule_type', row.BUSINESS_RULE_TYPE, 'business_rule_key_column', row.BUSINESS_RULE_KEY_COLUMN, 'target_table', row.TARGET_TABLE, 'sequence_number', row.SEQUENCE_NUMBER, 'active_flag', row.ACTIVE_FLAG, 'created_by', row.CREATED_BY, 'create_date', row.CREATE_DATE, 'updated_by', row.UPDATED_BY, 'updated_date', row.UPDATED_DATE) FROM CFG_BUSINESS_RULES row WHERE row.BUSINESS_RULE_ID=NEW.BUSINESS_RULE_ID), NULLIF(etl_craft_migration(), ''));
    SELECT etl_craft_exit_insert('CFG_BUSINESS_RULES');
END;
CREATE TRIGGER trg_audit_cfg_business_rules_update AFTER UPDATE ON CFG_BUSINESS_RULES
BEGIN
    UPDATE CFG_BUSINESS_RULES SET CREATED_BY=OLD.CREATED_BY, CREATE_DATE=OLD.CREATE_DATE, UPDATED_BY=etl_craft_actor(), UPDATED_DATE=(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00') WHERE BUSINESS_RULE_ID=NEW.BUSINESS_RULE_ID AND NOT etl_craft_inserting('CFG_BUSINESS_RULES');
    INSERT INTO AUD_METADATA_CHANGES (ACTOR, ACTOR_KIND, TABLE_NAME, ROW_KEY, OPERATION, BEFORE_JSON, AFTER_JSON, MIGRATION) SELECT etl_craft_actor(), etl_craft_actor_kind(), 'CFG_BUSINESS_RULES', CAST(NEW.BUSINESS_RULE_ID AS TEXT), 'UPDATE', json_object('business_rule_id', OLD.BUSINESS_RULE_ID, 'business_rule_name', OLD.BUSINESS_RULE_NAME, 'pipeline_id', OLD.PIPELINE_ID, 'task_id', OLD.TASK_ID, 'business_rule_sql', OLD.BUSINESS_RULE_SQL, 'business_rule_type', OLD.BUSINESS_RULE_TYPE, 'business_rule_key_column', OLD.BUSINESS_RULE_KEY_COLUMN, 'target_table', OLD.TARGET_TABLE, 'sequence_number', OLD.SEQUENCE_NUMBER, 'active_flag', OLD.ACTIVE_FLAG, 'created_by', OLD.CREATED_BY, 'create_date', OLD.CREATE_DATE, 'updated_by', OLD.UPDATED_BY, 'updated_date', OLD.UPDATED_DATE), (SELECT json_object('business_rule_id', row.BUSINESS_RULE_ID, 'business_rule_name', row.BUSINESS_RULE_NAME, 'pipeline_id', row.PIPELINE_ID, 'task_id', row.TASK_ID, 'business_rule_sql', row.BUSINESS_RULE_SQL, 'business_rule_type', row.BUSINESS_RULE_TYPE, 'business_rule_key_column', row.BUSINESS_RULE_KEY_COLUMN, 'target_table', row.TARGET_TABLE, 'sequence_number', row.SEQUENCE_NUMBER, 'active_flag', row.ACTIVE_FLAG, 'created_by', row.CREATED_BY, 'create_date', row.CREATE_DATE, 'updated_by', row.UPDATED_BY, 'updated_date', row.UPDATED_DATE) FROM CFG_BUSINESS_RULES row WHERE row.BUSINESS_RULE_ID=NEW.BUSINESS_RULE_ID), NULLIF(etl_craft_migration(), '') WHERE (NEW.BUSINESS_RULE_ID IS NOT OLD.BUSINESS_RULE_ID OR NEW.BUSINESS_RULE_NAME IS NOT OLD.BUSINESS_RULE_NAME OR NEW.PIPELINE_ID IS NOT OLD.PIPELINE_ID OR NEW.TASK_ID IS NOT OLD.TASK_ID OR NEW.BUSINESS_RULE_SQL IS NOT OLD.BUSINESS_RULE_SQL OR NEW.BUSINESS_RULE_TYPE IS NOT OLD.BUSINESS_RULE_TYPE OR NEW.BUSINESS_RULE_KEY_COLUMN IS NOT OLD.BUSINESS_RULE_KEY_COLUMN OR NEW.TARGET_TABLE IS NOT OLD.TARGET_TABLE OR NEW.SEQUENCE_NUMBER IS NOT OLD.SEQUENCE_NUMBER OR NEW.ACTIVE_FLAG IS NOT OLD.ACTIVE_FLAG) AND NOT etl_craft_inserting('CFG_BUSINESS_RULES');
END;
CREATE TRIGGER trg_audit_cfg_business_rules_delete AFTER DELETE ON CFG_BUSINESS_RULES
BEGIN
    INSERT INTO AUD_METADATA_CHANGES (ACTOR, ACTOR_KIND, TABLE_NAME, ROW_KEY, OPERATION, BEFORE_JSON, AFTER_JSON, MIGRATION) VALUES (etl_craft_actor(), etl_craft_actor_kind(), 'CFG_BUSINESS_RULES', CAST(OLD.BUSINESS_RULE_ID AS TEXT), 'DELETE', json_object('business_rule_id', OLD.BUSINESS_RULE_ID, 'business_rule_name', OLD.BUSINESS_RULE_NAME, 'pipeline_id', OLD.PIPELINE_ID, 'task_id', OLD.TASK_ID, 'business_rule_sql', OLD.BUSINESS_RULE_SQL, 'business_rule_type', OLD.BUSINESS_RULE_TYPE, 'business_rule_key_column', OLD.BUSINESS_RULE_KEY_COLUMN, 'target_table', OLD.TARGET_TABLE, 'sequence_number', OLD.SEQUENCE_NUMBER, 'active_flag', OLD.ACTIVE_FLAG, 'created_by', OLD.CREATED_BY, 'create_date', OLD.CREATE_DATE, 'updated_by', OLD.UPDATED_BY, 'updated_date', OLD.UPDATED_DATE), NULL, NULLIF(etl_craft_migration(), ''));
END;

-- The run log is mutable; stamp its inserting actor before the transaction commits.
CREATE TRIGGER trg_attribute_run_insert AFTER INSERT ON AUD_PIPELINES_RUN_LOG
BEGIN
    UPDATE AUD_PIPELINES_RUN_LOG SET STARTED_BY=COALESCE(NEW.STARTED_BY, etl_craft_actor()), STARTED_BY_KIND=COALESCE(NEW.STARTED_BY_KIND, etl_craft_actor_kind()), ENDED_BY=CASE WHEN NEW.END_DATE IS NOT NULL THEN COALESCE(NEW.ENDED_BY, etl_craft_actor()) ELSE NEW.ENDED_BY END, ENDED_BY_KIND=CASE WHEN NEW.END_DATE IS NOT NULL THEN COALESCE(NEW.ENDED_BY_KIND, etl_craft_actor_kind()) ELSE NEW.ENDED_BY_KIND END WHERE PIPELINE_RUN_ID=NEW.PIPELINE_RUN_ID;
END;
