-- etl-craft: rebuild-metadata
-- Expand account kinds without changing historical attribution or identity sequences.
CREATE TEMP TABLE etl_craft_actor_sequence AS SELECT name, seq FROM sqlite_sequence WHERE name='AUD_PIPELINES_RUN_LOG';
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
    STARTED_BY_KIND VARCHAR DEFAULT (etl_craft_actor_kind()) CONSTRAINT ck_aud_pipelines_run_log_started_by_kind CHECK (STARTED_BY_KIND IN ('USER','SERVICE','SYSTEM','HUMAN','SCHEDULE','ORCHESTRATOR','WORKER')),
    ENDED_BY VARCHAR,
    ENDED_BY_KIND VARCHAR CONSTRAINT ck_aud_pipelines_run_log_ended_by_kind CHECK (ENDED_BY_KIND IN ('USER','SERVICE','SYSTEM','HUMAN','SCHEDULE','ORCHESTRATOR','WORKER')),
    REPAIR_PENDING   VARCHAR NOT NULL DEFAULT 'N' CONSTRAINT ck_run_repair_pending CHECK (REPAIR_PENDING IN ('Y','N')),
    WRITER VARCHAR(128) NOT NULL DEFAULT 'etl-craft' CONSTRAINT ck_run_writer CHECK (length(trim(WRITER)) BETWEEN 1 AND 128),
    CONSTRAINT ck_pipeline_run_trigger_kind CHECK (TRIGGER_KIND IN ('SCHEDULE','MANUAL','BACKFILL','ORCHESTRATOR','STAND_IN')),
    CONSTRAINT ck_pipeline_run_backfill CHECK (BACKFILL IN ('Y','N')),
    CONSTRAINT ck_pipeline_run_status CHECK (STATUS IN ('QUEUED','IN-PROGRESS','SUCCESS','FAILED','SKIPPED','CANCELLED')),
    CONSTRAINT ck_pipeline_run_sla_status CHECK (SLA_STATUS IN ('MET','BREACHED'))
);
INSERT INTO AUD_PIPELINES_RUN_LOG_new (PIPELINE_RUN_ID, PIPELINE_ID, START_DATE, END_DATE, STATUS, SLA_STATUS, RUN_DATE, BACKFILL, RUN_KEY, TRIGGER_KIND, OWNER_ID, LEASE_EXPIRES_AT, OUTPUT_REVISION, CONFIG_SHA256, WRITER, STARTED_BY, STARTED_BY_KIND, ENDED_BY, ENDED_BY_KIND, REPAIR_PENDING) SELECT PIPELINE_RUN_ID, PIPELINE_ID, START_DATE, END_DATE, STATUS, SLA_STATUS, RUN_DATE, BACKFILL, RUN_KEY, TRIGGER_KIND, OWNER_ID, LEASE_EXPIRES_AT, OUTPUT_REVISION, CONFIG_SHA256, 'etl-craft', STARTED_BY, STARTED_BY_KIND, ENDED_BY, ENDED_BY_KIND, REPAIR_PENDING FROM AUD_PIPELINES_RUN_LOG;
DROP TABLE AUD_PIPELINES_RUN_LOG;
ALTER TABLE AUD_PIPELINES_RUN_LOG_new RENAME TO AUD_PIPELINES_RUN_LOG;
UPDATE sqlite_sequence SET seq=max(seq, coalesce((SELECT seq FROM etl_craft_actor_sequence), seq)) WHERE name='AUD_PIPELINES_RUN_LOG';
INSERT INTO sqlite_sequence (name, seq) SELECT name, seq FROM etl_craft_actor_sequence s WHERE NOT EXISTS (SELECT 1 FROM sqlite_sequence c WHERE c.name=s.name);
DROP TABLE etl_craft_actor_sequence;
CREATE UNIQUE INDEX ux_pipeline_run_one_active
    ON AUD_PIPELINES_RUN_LOG (PIPELINE_ID) WHERE STATUS = 'IN-PROGRESS';
CREATE INDEX ix_catalog_pipeline_runs ON AUD_PIPELINES_RUN_LOG (PIPELINE_ID, PIPELINE_RUN_ID DESC);
CREATE UNIQUE INDEX ux_pipeline_run_key ON AUD_PIPELINES_RUN_LOG (PIPELINE_ID, RUN_KEY);
CREATE INDEX ix_pipeline_schedule_key ON AUD_PIPELINES_RUN_LOG (PIPELINE_ID, RUN_KEY) WHERE TRIGGER_KIND = 'SCHEDULE';
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
CREATE TRIGGER trg_attribute_run_insert AFTER INSERT ON AUD_PIPELINES_RUN_LOG
BEGIN
    UPDATE AUD_PIPELINES_RUN_LOG SET STARTED_BY=COALESCE(NEW.STARTED_BY, etl_craft_actor()), STARTED_BY_KIND=COALESCE(NEW.STARTED_BY_KIND, etl_craft_actor_kind()), ENDED_BY=CASE WHEN NEW.END_DATE IS NOT NULL THEN COALESCE(NEW.ENDED_BY, etl_craft_actor()) ELSE NEW.ENDED_BY END, ENDED_BY_KIND=CASE WHEN NEW.END_DATE IS NOT NULL THEN COALESCE(NEW.ENDED_BY_KIND, etl_craft_actor_kind()) ELSE NEW.ENDED_BY_KIND END WHERE PIPELINE_RUN_ID=NEW.PIPELINE_RUN_ID;
END;
CREATE TEMP TABLE etl_craft_actor_sequence AS SELECT name, seq FROM sqlite_sequence WHERE name='AUD_RUN_INTERVENTIONS';
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
    REQUESTED_BY_KIND VARCHAR DEFAULT (etl_craft_actor_kind()) CONSTRAINT ck_aud_run_interventions_requested_by_kind CHECK (REQUESTED_BY_KIND IN ('USER','SERVICE','SYSTEM','HUMAN','SCHEDULE','ORCHESTRATOR','WORKER')),
    CONSTRAINT ck_intervention_action CHECK (ACTION IN ('MARK','NEW_RUN','CANCEL','REOPEN','RESET',
                                                     'GATE_BYPASS','IGNORE_DEPENDENCIES','RERUN'))
);
INSERT INTO AUD_RUN_INTERVENTIONS_new (INTERVENTION_ID, PIPELINE_ID, PIPELINE_RUN_ID, TASK_ID, ACTION, FROM_STATUS, TO_STATUS, TARGET_COUNT, PREVIOUS_MESSAGE, REASON, REQUESTED_BY, REQUESTED_AT, REQUESTED_BY_KIND) SELECT INTERVENTION_ID, PIPELINE_ID, PIPELINE_RUN_ID, TASK_ID, ACTION, FROM_STATUS, TO_STATUS, TARGET_COUNT, PREVIOUS_MESSAGE, REASON, REQUESTED_BY, REQUESTED_AT, REQUESTED_BY_KIND FROM AUD_RUN_INTERVENTIONS;
DROP TABLE AUD_RUN_INTERVENTIONS;
ALTER TABLE AUD_RUN_INTERVENTIONS_new RENAME TO AUD_RUN_INTERVENTIONS;
UPDATE sqlite_sequence SET seq=max(seq, coalesce((SELECT seq FROM etl_craft_actor_sequence), seq)) WHERE name='AUD_RUN_INTERVENTIONS';
INSERT INTO sqlite_sequence (name, seq) SELECT name, seq FROM etl_craft_actor_sequence s WHERE NOT EXISTS (SELECT 1 FROM sqlite_sequence c WHERE c.name=s.name);
DROP TABLE etl_craft_actor_sequence;
CREATE INDEX ix_run_interventions_run ON AUD_RUN_INTERVENTIONS (PIPELINE_RUN_ID);
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
CREATE TEMP TABLE etl_craft_actor_sequence AS SELECT name, seq FROM sqlite_sequence WHERE name='AUD_PIPELINE_PAUSES';
CREATE TABLE AUD_PIPELINE_PAUSES_new (
    PIPELINE_PAUSE_ID  INTEGER PRIMARY KEY AUTOINCREMENT,
    PIPELINE_ID        BIGINT NOT NULL REFERENCES CFG_PIPELINES(PIPELINE_ID),
    PAUSED_AT          TIMESTAMP NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'),
    PAUSED_BY          VARCHAR NOT NULL,
    REASON             VARCHAR NOT NULL,
    RESUMED_AT         TIMESTAMP,
    RESUMED_BY         VARCHAR,
    RESUME_REASON      VARCHAR,
    PAUSED_BY_KIND VARCHAR DEFAULT (etl_craft_actor_kind()) CONSTRAINT ck_aud_pipeline_pauses_paused_by_kind CHECK (PAUSED_BY_KIND IN ('USER','SERVICE','SYSTEM','HUMAN','SCHEDULE','ORCHESTRATOR','WORKER')),
    RESUMED_BY_KIND VARCHAR CONSTRAINT ck_aud_pipeline_pauses_resumed_by_kind CHECK (RESUMED_BY_KIND IN ('USER','SERVICE','SYSTEM','HUMAN','SCHEDULE','ORCHESTRATOR','WORKER'))
);
INSERT INTO AUD_PIPELINE_PAUSES_new (PIPELINE_PAUSE_ID, PIPELINE_ID, PAUSED_AT, PAUSED_BY, REASON, RESUMED_AT, RESUMED_BY, RESUME_REASON, PAUSED_BY_KIND, RESUMED_BY_KIND) SELECT PIPELINE_PAUSE_ID, PIPELINE_ID, PAUSED_AT, PAUSED_BY, REASON, RESUMED_AT, RESUMED_BY, RESUME_REASON, PAUSED_BY_KIND, RESUMED_BY_KIND FROM AUD_PIPELINE_PAUSES;
DROP TABLE AUD_PIPELINE_PAUSES;
ALTER TABLE AUD_PIPELINE_PAUSES_new RENAME TO AUD_PIPELINE_PAUSES;
UPDATE sqlite_sequence SET seq=max(seq, coalesce((SELECT seq FROM etl_craft_actor_sequence), seq)) WHERE name='AUD_PIPELINE_PAUSES';
INSERT INTO sqlite_sequence (name, seq) SELECT name, seq FROM etl_craft_actor_sequence s WHERE NOT EXISTS (SELECT 1 FROM sqlite_sequence c WHERE c.name=s.name);
DROP TABLE etl_craft_actor_sequence;
CREATE UNIQUE INDEX ux_pipeline_pauses_open
    ON AUD_PIPELINE_PAUSES (PIPELINE_ID) WHERE RESUMED_AT IS NULL;
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
CREATE TEMP TABLE etl_craft_actor_sequence AS SELECT name, seq FROM sqlite_sequence WHERE name='AUD_TASK_ATTEMPTS';
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
    REQUESTED_BY_KIND VARCHAR DEFAULT (etl_craft_actor_kind()) CONSTRAINT ck_aud_task_attempts_requested_by_kind CHECK (REQUESTED_BY_KIND IN ('USER','SERVICE','SYSTEM','HUMAN','SCHEDULE','ORCHESTRATOR','WORKER')),
    NOT_BEFORE       TIMESTAMP,
    RETRYABLE        BOOLEAN NOT NULL DEFAULT TRUE CHECK (RETRYABLE IN (TRUE, FALSE)),
    REJECT_COUNT     BIGINT,
    WATERMARK_FROM   TEXT,
    WATERMARK_TO     TEXT,
    CONSTRAINT ck_attempt_status CHECK (STATUS IN ('QUEUED','CLAIMED','RUNNING','SUCCESS','FAILED','TIMED_OUT','CANCELLED','LOST'))
);
INSERT INTO AUD_TASK_ATTEMPTS_new (ATTEMPT_ID, TASK_RUN_ID, ATTEMPT_NUMBER, STATUS, OWNER_ID, LEASE_EXPIRES_AT, HEARTBEAT_AT, QUEUED_AT, CLAIMED_AT, STARTED_AT, ENDED_AT, HOST, PID, PROCESS_START, EXIT_CODE, SOURCE_COUNT, TARGET_COUNT, INSERT_COUNT, UPDATE_COUNT, DELETE_COUNT, ROWS_WRITTEN, ERROR_MESSAGE, TASK_LOG, LOG_PATH, REQUESTED_BY, REQUESTED_BY_KIND, NOT_BEFORE, REJECT_COUNT, WATERMARK_FROM, WATERMARK_TO) SELECT ATTEMPT_ID, TASK_RUN_ID, ATTEMPT_NUMBER, STATUS, OWNER_ID, LEASE_EXPIRES_AT, HEARTBEAT_AT, QUEUED_AT, CLAIMED_AT, STARTED_AT, ENDED_AT, HOST, PID, PROCESS_START, EXIT_CODE, SOURCE_COUNT, TARGET_COUNT, INSERT_COUNT, UPDATE_COUNT, DELETE_COUNT, ROWS_WRITTEN, ERROR_MESSAGE, TASK_LOG, LOG_PATH, REQUESTED_BY, REQUESTED_BY_KIND, NOT_BEFORE, REJECT_COUNT, WATERMARK_FROM, WATERMARK_TO FROM AUD_TASK_ATTEMPTS;
DROP TABLE AUD_TASK_ATTEMPTS;
ALTER TABLE AUD_TASK_ATTEMPTS_new RENAME TO AUD_TASK_ATTEMPTS;
UPDATE sqlite_sequence SET seq=max(seq, coalesce((SELECT seq FROM etl_craft_actor_sequence), seq)) WHERE name='AUD_TASK_ATTEMPTS';
INSERT INTO sqlite_sequence (name, seq) SELECT name, seq FROM etl_craft_actor_sequence s WHERE NOT EXISTS (SELECT 1 FROM sqlite_sequence c WHERE c.name=s.name);
DROP TABLE etl_craft_actor_sequence;
CREATE UNIQUE INDEX ux_attempt_number ON AUD_TASK_ATTEMPTS (TASK_RUN_ID, ATTEMPT_NUMBER);
CREATE UNIQUE INDEX ux_attempt_one_active ON AUD_TASK_ATTEMPTS (TASK_RUN_ID)
    WHERE STATUS IN ('QUEUED','CLAIMED','RUNNING');
CREATE INDEX ix_attempt_status_lease ON AUD_TASK_ATTEMPTS (STATUS, LEASE_EXPIRES_AT);
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
CREATE TEMP TABLE etl_craft_actor_sequence AS SELECT name, seq FROM sqlite_sequence WHERE name='AUD_ACTIONS';
CREATE TABLE AUD_ACTIONS_new (
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
    CONSTRAINT ck_actions_actor_kind CHECK (ACTOR_KIND IN ('USER','SERVICE','SYSTEM','HUMAN','SCHEDULE','ORCHESTRATOR','WORKER'))
);
INSERT INTO AUD_ACTIONS_new (ACTION_ID, STARTED_AT, ENDED_AT, ACTOR, ACTOR_KIND, HOST, COMMAND, ARGUMENTS, PIPELINE_ID, PIPELINE_RUN_ID, TASK_ID, OUTCOME, EXIT_CODE) SELECT ACTION_ID, STARTED_AT, ENDED_AT, ACTOR, ACTOR_KIND, HOST, COMMAND, ARGUMENTS, PIPELINE_ID, PIPELINE_RUN_ID, TASK_ID, OUTCOME, EXIT_CODE FROM AUD_ACTIONS;
DROP TABLE AUD_ACTIONS;
ALTER TABLE AUD_ACTIONS_new RENAME TO AUD_ACTIONS;
UPDATE sqlite_sequence SET seq=max(seq, coalesce((SELECT seq FROM etl_craft_actor_sequence), seq)) WHERE name='AUD_ACTIONS';
INSERT INTO sqlite_sequence (name, seq) SELECT name, seq FROM etl_craft_actor_sequence s WHERE NOT EXISTS (SELECT 1 FROM sqlite_sequence c WHERE c.name=s.name);
DROP TABLE etl_craft_actor_sequence;
CREATE INDEX ix_actions_pipeline ON AUD_ACTIONS (PIPELINE_ID, STARTED_AT);
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
