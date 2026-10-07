-- Overseer process history; the session lock is the source of leadership.
CREATE TABLE AUD_OVERSEERS (
    OVERSEER_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    HOST VARCHAR NOT NULL,
    PID INTEGER NOT NULL CHECK (PID > 0),
    VERSION VARCHAR NOT NULL,
    STARTED_AT TIMESTAMP NOT NULL,
    HEARTBEAT_AT TIMESTAMP NOT NULL,
    STOPPED_AT TIMESTAMP,
    STARTED_BY VARCHAR NOT NULL,
    STARTED_BY_KIND VARCHAR NOT NULL,
    STOPPED_BY VARCHAR,
    STOPPED_BY_KIND VARCHAR
);
CREATE INDEX ix_overseer_unclosed ON AUD_OVERSEERS (OVERSEER_ID) WHERE STOPPED_AT IS NULL;
CREATE TRIGGER trg_actor_guard_aud_overseers_insert BEFORE INSERT ON AUD_OVERSEERS
WHEN etl_craft_actor() IS NULL OR etl_craft_actor() = ''
BEGIN
    SELECT RAISE(ABORT, 'AUD_OVERSEERS is written only by etl-craft');
END;
CREATE TRIGGER trg_actor_guard_aud_overseers_update BEFORE UPDATE ON AUD_OVERSEERS
WHEN etl_craft_actor() IS NULL OR etl_craft_actor() = ''
BEGIN
    SELECT RAISE(ABORT, 'AUD_OVERSEERS is written only by etl-craft');
END;
CREATE TRIGGER trg_actor_guard_aud_overseers_delete BEFORE DELETE ON AUD_OVERSEERS
WHEN etl_craft_actor() IS NULL OR etl_craft_actor() = ''
BEGIN
    SELECT RAISE(ABORT, 'AUD_OVERSEERS is written only by etl-craft');
END;
