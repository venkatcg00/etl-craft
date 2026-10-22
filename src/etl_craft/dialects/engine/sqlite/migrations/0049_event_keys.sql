-- Signal ids consumed once per event task, committed with immutable successful evidence.
CREATE TABLE AUD_EVENT_KEYS (
    TASK_ID BIGINT NOT NULL REFERENCES CFG_TASKS(TASK_ID),
    EVENT_KEY TEXT NOT NULL,
    TASK_RUN_ID BIGINT NOT NULL REFERENCES AUD_TASK_EVENTS(TASK_RUN_ID),
    CREATED_BY VARCHAR NOT NULL,
    CREATED_AT TEXT NOT NULL,
    CONSTRAINT pk_event_keys PRIMARY KEY (TASK_ID, EVENT_KEY)
);
CREATE TRIGGER trg_actor_guard_aud_event_keys_insert BEFORE INSERT ON AUD_EVENT_KEYS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_EVENT_KEYS is written only by etl-craft') END;
END;
CREATE TRIGGER trg_actor_guard_aud_event_keys_update BEFORE UPDATE ON AUD_EVENT_KEYS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_EVENT_KEYS is written only by etl-craft') END;
    SELECT CASE WHEN etl_craft_purpose() <> 'retention' THEN RAISE(ABORT, 'AUD_EVENT_KEYS immutable history; use a new action or etl-craft retention') END;
END;
CREATE TRIGGER trg_actor_guard_aud_event_keys_delete BEFORE DELETE ON AUD_EVENT_KEYS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_EVENT_KEYS is written only by etl-craft') END;
    SELECT CASE WHEN etl_craft_purpose() <> 'retention' THEN RAISE(ABORT, 'AUD_EVENT_KEYS immutable history; use a new action or etl-craft retention') END;
END;
