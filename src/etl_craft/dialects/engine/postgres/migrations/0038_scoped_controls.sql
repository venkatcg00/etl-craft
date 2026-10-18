-- Durable scoped requests and append-only observations of worker/query endings.
CREATE TABLE AUD_ATTEMPT_ACKNOWLEDGEMENTS (
    ATTEMPT_ID BIGINT NOT NULL REFERENCES AUD_TASK_ATTEMPTS(ATTEMPT_ID),
    KIND VARCHAR(8) NOT NULL CHECK (KIND IN ('PROCESS','QUERIES')),
    OBSERVED_AT TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (ATTEMPT_ID, KIND)
);
CREATE FUNCTION etl_craft_attempt_ack_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF COALESCE(current_setting('etl_craft.actor', true), '') = '' THEN
        RAISE EXCEPTION USING MESSAGE = upper(TG_TABLE_NAME) || ' is written only by etl-craft; use scoped controls or reconciliation';
    END IF;
    IF TG_OP = 'DELETE' AND current_setting('etl_craft.purpose', true) = 'retention' THEN RETURN OLD; END IF;
    RAISE EXCEPTION 'attempt acknowledgement is immutable; record new evidence or use etl-craft retention';
END; $$;
CREATE TRIGGER trg_attempt_ack_immutable BEFORE UPDATE OR DELETE ON AUD_ATTEMPT_ACKNOWLEDGEMENTS
FOR EACH ROW EXECUTE FUNCTION etl_craft_attempt_ack_guard();
CREATE TABLE AUD_CONTROL_REQUESTS (
    CONTROL_ID BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    REQUEST_KEY VARCHAR(128) NOT NULL UNIQUE,
    PIPELINE_RUN_ID BIGINT NOT NULL REFERENCES AUD_PIPELINES_RUN_LOG(PIPELINE_RUN_ID),
    TASK_ID BIGINT REFERENCES CFG_TASKS(TASK_ID),
    ACTION VARCHAR(8) NOT NULL CHECK (ACTION IN ('HOLD','RELEASE','STOP','ABORT')),
    STATUS VARCHAR(16) NOT NULL CHECK (STATUS IN ('REQUESTED','ACKNOWLEDGED','COMPLETED')),
    ATTEMPT_IDS TEXT NOT NULL,
    REQUESTED_BY VARCHAR(128) NOT NULL,
    REQUESTED_BY_KIND VARCHAR(8) NOT NULL CHECK (REQUESTED_BY_KIND IN ('USER','SERVICE','SYSTEM')),
    REASON TEXT NOT NULL CHECK (length(trim(REASON)) > 0),
    REQUESTED_AT TIMESTAMPTZ NOT NULL,
    ACKNOWLEDGED_AT TIMESTAMPTZ,
    COMPLETED_AT TIMESTAMPTZ,
    MESSAGE TEXT NOT NULL
);
CREATE INDEX ix_control_scope ON AUD_CONTROL_REQUESTS (PIPELINE_RUN_ID, TASK_ID, CONTROL_ID);
CREATE INDEX ix_control_pending ON AUD_CONTROL_REQUESTS (PIPELINE_RUN_ID) WHERE STATUS <> 'COMPLETED';
CREATE FUNCTION etl_craft_control_identity_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF COALESCE(current_setting('etl_craft.actor', true), '') = '' THEN
        RAISE EXCEPTION USING MESSAGE = upper(TG_TABLE_NAME) || ' is written only by etl-craft; use scoped controls or reconciliation';
    END IF;
    IF TG_OP IN ('DELETE','TRUNCATE') THEN
        IF current_setting('etl_craft.purpose', true) = 'retention' THEN RETURN OLD; END IF;
    ELSIF OLD.status <> 'COMPLETED' AND
        to_jsonb(NEW) - ARRAY['status','acknowledged_at','completed_at','message'] =
        to_jsonb(OLD) - ARRAY['status','acknowledged_at','completed_at','message'] THEN RETURN NEW;
    END IF;
    RAISE EXCEPTION 'control request identity and completed outcome are immutable; submit a new control';
END; $$;
CREATE TRIGGER trg_control_identity_immutable BEFORE UPDATE OR DELETE ON AUD_CONTROL_REQUESTS
FOR EACH ROW EXECUTE FUNCTION etl_craft_control_identity_guard();
CREATE TRIGGER trg_attempt_ack_truncate BEFORE TRUNCATE ON AUD_ATTEMPT_ACKNOWLEDGEMENTS
FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_attempt_ack_guard();
CREATE TRIGGER trg_control_identity_truncate BEFORE TRUNCATE ON AUD_CONTROL_REQUESTS
FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_control_identity_guard();
