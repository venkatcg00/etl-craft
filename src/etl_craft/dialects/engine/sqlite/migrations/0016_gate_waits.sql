-- Durable gate polling budget; TASK_ID is NULL for pipeline admission and NEXT_CHECK_AT is NULL when settled.
CREATE TABLE AUD_GATE_WAITS (
    PIPELINE_RUN_ID BIGINT NOT NULL REFERENCES AUD_PIPELINES_RUN_LOG(PIPELINE_RUN_ID),
    TASK_ID BIGINT REFERENCES CFG_TASKS(TASK_ID),
    FIRST_CHECK_AT TIMESTAMP NOT NULL,
    NEXT_CHECK_AT TIMESTAMP,
    LOOKS INT NOT NULL,
    WAIT_UNTIL TIMESTAMP NOT NULL,
    CONSTRAINT ck_gate_wait_task CHECK (TASK_ID IS NULL OR TASK_ID > 0),
    CONSTRAINT ck_gate_wait_looks CHECK (LOOKS >= 0 AND LOOKS <= 30)
);
CREATE UNIQUE INDEX ux_gate_wait ON AUD_GATE_WAITS (PIPELINE_RUN_ID, COALESCE(TASK_ID,0));
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
