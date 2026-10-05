CREATE TABLE AUD_TARGET_HASH_VERSION (
    TARGET_OBJECT VARCHAR PRIMARY KEY,
    HASH_VERSION INTEGER NOT NULL CONSTRAINT ck_target_hash_version CHECK (HASH_VERSION IN (1,2)),
    RECOMPUTED_AT TIMESTAMP NOT NULL
);

CREATE TRIGGER trg_actor_guard_aud_target_hash_version_insert BEFORE INSERT ON AUD_TARGET_HASH_VERSION
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_TARGET_HASH_VERSION is written only by etl-craft; use etl-craft rehash') END;
END;

CREATE TRIGGER trg_actor_guard_aud_target_hash_version_update BEFORE UPDATE ON AUD_TARGET_HASH_VERSION
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_TARGET_HASH_VERSION is written only by etl-craft; use etl-craft rehash') END;
END;

CREATE TRIGGER trg_actor_guard_aud_target_hash_version_delete BEFORE DELETE ON AUD_TARGET_HASH_VERSION
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_TARGET_HASH_VERSION is written only by etl-craft; use etl-craft rehash') END;
END;
