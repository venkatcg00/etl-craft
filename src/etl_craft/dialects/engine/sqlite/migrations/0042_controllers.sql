DROP TRIGGER trg_actor_guard_aud_overseers_insert;
DROP TRIGGER trg_actor_guard_aud_overseers_update;
DROP TRIGGER trg_actor_guard_aud_overseers_delete;
ALTER TABLE AUD_OVERSEERS RENAME TO AUD_CONTROLLERS;
ALTER TABLE AUD_CONTROLLERS RENAME COLUMN OVERSEER_ID TO CONTROLLER_ID;
DROP INDEX ix_overseer_unclosed;
CREATE INDEX ix_controller_unclosed ON AUD_CONTROLLERS (CONTROLLER_ID) WHERE STOPPED_AT IS NULL;

CREATE TRIGGER trg_actor_guard_aud_controllers_insert BEFORE INSERT ON AUD_CONTROLLERS
WHEN etl_craft_actor() IS NULL OR etl_craft_actor() = ''
BEGIN
    SELECT RAISE(ABORT, 'AUD_CONTROLLERS is written only by etl-craft');
END;
CREATE TRIGGER trg_actor_guard_aud_controllers_update BEFORE UPDATE ON AUD_CONTROLLERS
WHEN etl_craft_actor() IS NULL OR etl_craft_actor() = ''
BEGIN
    SELECT RAISE(ABORT, 'AUD_CONTROLLERS is written only by etl-craft');
END;
CREATE TRIGGER trg_actor_guard_aud_controllers_delete BEFORE DELETE ON AUD_CONTROLLERS
WHEN etl_craft_actor() IS NULL OR etl_craft_actor() = ''
BEGIN
    SELECT RAISE(ABORT, 'AUD_CONTROLLERS is written only by etl-craft');
END;
