-- etl-craft: rebuild-metadata
CREATE TEMP TABLE etl_craft_task_sequence AS SELECT seq FROM sqlite_sequence WHERE name='CFG_TASKS';
CREATE TABLE CFG_TASKS_new (
    TASK_ID              INTEGER PRIMARY KEY AUTOINCREMENT,
    TASK_CODE            VARCHAR NOT NULL,
    TASK_TYPE            VARCHAR NOT NULL,
    PIPELINE_ID          BIGINT NOT NULL REFERENCES CFG_PIPELINES(PIPELINE_ID),
    HANDLER              VARCHAR NOT NULL,
    RUN_CONDITION        VARCHAR,
    RUN_CONDITION_COUNT  INT,
    ACTIVE_FLAG          VARCHAR NOT NULL DEFAULT 'Y',
    CREATED_BY           VARCHAR DEFAULT 'etl-craft',
    CREATE_DATE          TIMESTAMP DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'),
    UPDATED_BY           VARCHAR DEFAULT 'etl-craft',
    UPDATED_DATE         TIMESTAMP DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'),
    CONSTRAINT ck_tasks_code CHECK (TASK_CODE GLOB '[A-Za-z]*' AND TASK_CODE NOT GLOB '*[^A-Za-z0-9_]*' AND length(TASK_CODE) <= 128 AND instr(TASK_CODE, char(0)) = 0),
    CONSTRAINT ck_tasks_task_type     CHECK (TASK_TYPE IN ('INGESTION','ETL')),
    CONSTRAINT ck_tasks_handler       CHECK (HANDLER IN ('PYTHON','SQL','BUSINESS_RULES','EMAIL_ALERT','EVENT_WAIT')),
    CONSTRAINT ck_tasks_active_flag   CHECK (ACTIVE_FLAG IN ('Y','N')),
    CONSTRAINT ck_tasks_run_condition CHECK (RUN_CONDITION IS NULL OR RUN_CONDITION IN ('ALL','ANY','N')),
    CONSTRAINT ck_tasks_run_condition_count CHECK (
        (COALESCE(RUN_CONDITION, '') = 'N' AND RUN_CONDITION_COUNT IS NOT NULL
            AND RUN_CONDITION_COUNT >= 1)
        OR (RUN_CONDITION IS NOT 'N' AND RUN_CONDITION_COUNT IS NULL)
    )
);
INSERT INTO CFG_TASKS_new SELECT * FROM CFG_TASKS;
DROP TABLE CFG_TASKS;
ALTER TABLE CFG_TASKS_new RENAME TO CFG_TASKS;
UPDATE sqlite_sequence SET seq=max(seq,coalesce((SELECT seq FROM etl_craft_task_sequence),seq)) WHERE name='CFG_TASKS';
DROP TABLE etl_craft_task_sequence;
CREATE UNIQUE INDEX ux_tasks_code_active
    ON CFG_TASKS (PIPELINE_ID, TASK_CODE) WHERE ACTIVE_FLAG = 'Y';
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
CREATE TABLE AUD_GATE_WAITS_new (
    PIPELINE_RUN_ID BIGINT NOT NULL REFERENCES AUD_PIPELINES_RUN_LOG(PIPELINE_RUN_ID),
    TASK_ID BIGINT REFERENCES CFG_TASKS(TASK_ID),
    FIRST_CHECK_AT TIMESTAMP NOT NULL,
    NEXT_CHECK_AT TIMESTAMP,
    LOOKS INT NOT NULL,
    WAIT_UNTIL TIMESTAMP,
    WAIT_KIND VARCHAR NOT NULL DEFAULT 'DEPENDENCY' CHECK (WAIT_KIND IN ('DEPENDENCY','EVENT')),
    EVENT_OBSERVATIONS TEXT,
    CONSTRAINT ck_gate_wait_deadline CHECK (WAIT_KIND = 'EVENT' OR WAIT_UNTIL IS NOT NULL),
    CONSTRAINT ck_gate_wait_task CHECK (TASK_ID IS NULL OR TASK_ID > 0),
    CONSTRAINT ck_gate_wait_looks CHECK (LOOKS >= 0 AND LOOKS <= 30)
);
INSERT INTO AUD_GATE_WAITS_new (PIPELINE_RUN_ID,TASK_ID,FIRST_CHECK_AT,NEXT_CHECK_AT,LOOKS,WAIT_UNTIL)
SELECT PIPELINE_RUN_ID,TASK_ID,FIRST_CHECK_AT,NEXT_CHECK_AT,LOOKS,WAIT_UNTIL FROM AUD_GATE_WAITS;
DROP TABLE AUD_GATE_WAITS;
ALTER TABLE AUD_GATE_WAITS_new RENAME TO AUD_GATE_WAITS;
CREATE UNIQUE INDEX ux_gate_wait ON AUD_GATE_WAITS (PIPELINE_RUN_ID, COALESCE(TASK_ID,0), WAIT_KIND);
CREATE TRIGGER trg_actor_guard_aud_gate_waits_insert BEFORE INSERT ON AUD_GATE_WAITS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'audit rows are written only by etl-craft; use etl-craft run, mark or cancel') END;
END;
CREATE TRIGGER trg_actor_guard_aud_gate_waits_update BEFORE UPDATE ON AUD_GATE_WAITS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'audit rows are written only by etl-craft; use etl-craft run, mark or cancel') END;
END;
CREATE TRIGGER trg_actor_guard_aud_gate_waits_delete BEFORE DELETE ON AUD_GATE_WAITS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'audit rows are written only by etl-craft; use etl-craft run, mark or cancel') END;
END;

CREATE TABLE AUD_TASK_EVENTS (
    TASK_RUN_ID BIGINT PRIMARY KEY REFERENCES AUD_TASK_RUN_LOG(TASK_RUN_ID),
    EVIDENCE TEXT NOT NULL
);
CREATE TABLE AUD_EVENT_FILES (
    TASK_ID BIGINT NOT NULL REFERENCES CFG_TASKS(TASK_ID),
    FILE_PATH TEXT NOT NULL,
    TASK_RUN_ID BIGINT NOT NULL REFERENCES AUD_TASK_EVENTS(TASK_RUN_ID),
    CONSTRAINT pk_event_files PRIMARY KEY (TASK_ID, FILE_PATH)
);
CREATE TRIGGER trg_actor_guard_aud_task_events_insert BEFORE INSERT ON AUD_TASK_EVENTS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_TASK_EVENTS is written only by etl-craft') END;
END;
CREATE TRIGGER trg_actor_guard_aud_task_events_update BEFORE UPDATE ON AUD_TASK_EVENTS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_TASK_EVENTS is written only by etl-craft') END;
    SELECT CASE WHEN etl_craft_purpose() <> 'retention' THEN RAISE(ABORT, 'AUD_TASK_EVENTS immutable history; use a new action or etl-craft retention') END;
END;
CREATE TRIGGER trg_actor_guard_aud_task_events_delete BEFORE DELETE ON AUD_TASK_EVENTS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_TASK_EVENTS is written only by etl-craft') END;
    SELECT CASE WHEN etl_craft_purpose() <> 'retention' THEN RAISE(ABORT, 'AUD_TASK_EVENTS immutable history; use a new action or etl-craft retention') END;
END;
CREATE TRIGGER trg_actor_guard_aud_event_files_insert BEFORE INSERT ON AUD_EVENT_FILES
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_EVENT_FILES is written only by etl-craft') END;
END;
CREATE TRIGGER trg_actor_guard_aud_event_files_update BEFORE UPDATE ON AUD_EVENT_FILES
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_EVENT_FILES is written only by etl-craft') END;
    SELECT CASE WHEN etl_craft_purpose() <> 'retention' THEN RAISE(ABORT, 'AUD_EVENT_FILES immutable history; use a new action or etl-craft retention') END;
END;
CREATE TRIGGER trg_actor_guard_aud_event_files_delete BEFORE DELETE ON AUD_EVENT_FILES
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_EVENT_FILES is written only by etl-craft') END;
    SELECT CASE WHEN etl_craft_purpose() <> 'retention' THEN RAISE(ABORT, 'AUD_EVENT_FILES immutable history; use a new action or etl-craft retention') END;
END;
