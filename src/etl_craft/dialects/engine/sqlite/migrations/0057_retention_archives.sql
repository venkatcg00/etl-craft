-- Stable deployment identity namespaces archive rows across Engine DBs.
CREATE TABLE AUD_ENGINE_IDENTITY (
    IDENTITY_ID INTEGER PRIMARY KEY CHECK (IDENTITY_ID=1),
    ENGINE_ID VARCHAR(32) NOT NULL
);
INSERT INTO AUD_ENGINE_IDENTITY (IDENTITY_ID,ENGINE_ID) VALUES (1,lower(hex(randomblob(16))));
-- One admitted archive snapshot per project survives remote commit acknowledgement loss.
CREATE TABLE AUD_RETENTION_ARCHIVES (
    PROJECT_ID BIGINT PRIMARY KEY REFERENCES CFG_PROJECTS(PROJECT_ID),
    BATCH_ID VARCHAR(32) NOT NULL,
    DESTINATION VARCHAR(64) NOT NULL,
    SCHEMA_NAME VARCHAR NOT NULL,
    BODY TEXT NOT NULL,
    CREATED_AT TIMESTAMP NOT NULL
);
CREATE TRIGGER trg_actor_guard_aud_engine_identity_insert BEFORE INSERT ON AUD_ENGINE_IDENTITY
BEGIN SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_ENGINE_IDENTITY is written only by etl-craft') END; END;
CREATE TRIGGER trg_actor_guard_aud_engine_identity_update BEFORE UPDATE ON AUD_ENGINE_IDENTITY
BEGIN SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_ENGINE_IDENTITY is written only by etl-craft') END; END;
CREATE TRIGGER trg_actor_guard_aud_engine_identity_delete BEFORE DELETE ON AUD_ENGINE_IDENTITY
BEGIN SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_ENGINE_IDENTITY is written only by etl-craft') END; END;
CREATE TRIGGER trg_actor_guard_aud_retention_archives_insert BEFORE INSERT ON AUD_RETENTION_ARCHIVES
BEGIN SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_RETENTION_ARCHIVES is written only by etl-craft') END; END;
CREATE TRIGGER trg_actor_guard_aud_retention_archives_update BEFORE UPDATE ON AUD_RETENTION_ARCHIVES
BEGIN SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_RETENTION_ARCHIVES is written only by etl-craft') END; END;
CREATE TRIGGER trg_actor_guard_aud_retention_archives_delete BEFORE DELETE ON AUD_RETENTION_ARCHIVES
BEGIN SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_RETENTION_ARCHIVES is written only by etl-craft') END; END;
