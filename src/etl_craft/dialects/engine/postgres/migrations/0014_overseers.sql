-- Overseer process history; the session lock is the source of leadership.
CREATE TABLE AUD_OVERSEERS (
    OVERSEER_ID BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    HOST VARCHAR NOT NULL,
    PID INTEGER NOT NULL CHECK (PID > 0),
    VERSION VARCHAR NOT NULL,
    STARTED_AT TIMESTAMPTZ NOT NULL,
    HEARTBEAT_AT TIMESTAMPTZ NOT NULL,
    STOPPED_AT TIMESTAMPTZ,
    STARTED_BY VARCHAR NOT NULL,
    STARTED_BY_KIND VARCHAR NOT NULL,
    STOPPED_BY VARCHAR,
    STOPPED_BY_KIND VARCHAR
);
COMMENT ON TABLE AUD_OVERSEERS IS 'Overseer process history; the session lock owns leadership and unclosed history does not block a replacement.';
CREATE INDEX ix_overseer_unclosed ON AUD_OVERSEERS (OVERSEER_ID) WHERE STOPPED_AT IS NULL;
CREATE TRIGGER trg_actor_guard_aud_overseers BEFORE INSERT OR UPDATE OR DELETE ON AUD_OVERSEERS FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_aud_overseers BEFORE TRUNCATE ON AUD_OVERSEERS FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE OR REPLACE FUNCTION etl_craft_notify_execution() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('etl_craft_events', current_schema());
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER trg_notify_aud_pipelines_run_log AFTER INSERT OR UPDATE OR DELETE ON aud_pipelines_run_log FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_notify_execution();
CREATE TRIGGER trg_notify_aud_task_attempts AFTER INSERT OR UPDATE OR DELETE ON aud_task_attempts FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_notify_execution();
CREATE TRIGGER trg_notify_aud_pipeline_pauses AFTER INSERT OR UPDATE OR DELETE ON aud_pipeline_pauses FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_notify_execution();
