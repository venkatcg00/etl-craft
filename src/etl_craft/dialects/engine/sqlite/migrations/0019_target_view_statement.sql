CREATE TABLE AUD_TARGET_VIEW_STATEMENT (
    TARGET_OBJECT VARCHAR PRIMARY KEY,
    STATEMENT_SHA256 VARCHAR(64) NOT NULL,
    RECORDED_AT TIMESTAMP NOT NULL
);

CREATE TRIGGER trg_actor_guard_aud_target_view_statement_insert BEFORE INSERT ON AUD_TARGET_VIEW_STATEMENT
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_TARGET_VIEW_STATEMENT is written only by etl-craft; use etl-craft run') END;
END;

CREATE TRIGGER trg_actor_guard_aud_target_view_statement_update BEFORE UPDATE ON AUD_TARGET_VIEW_STATEMENT
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_TARGET_VIEW_STATEMENT is written only by etl-craft; use etl-craft run') END;
END;

CREATE TRIGGER trg_actor_guard_aud_target_view_statement_delete BEFORE DELETE ON AUD_TARGET_VIEW_STATEMENT
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_TARGET_VIEW_STATEMENT is written only by etl-craft; use etl-craft run') END;
END;
