-- Attribute new requests and endings without inventing historical actors.
CREATE OR REPLACE FUNCTION etl_craft_attribute() RETURNS trigger AS $$
BEGIN
    IF TG_TABLE_NAME = 'aud_pipelines_run_log' THEN
        IF TG_OP = 'INSERT' THEN
            NEW.STARTED_BY := COALESCE(NEW.STARTED_BY, current_setting('etl_craft.actor'));
            NEW.STARTED_BY_KIND := COALESCE(NEW.STARTED_BY_KIND, current_setting('etl_craft.actor_kind'));
        END IF;
        IF NEW.END_DATE IS NOT NULL AND (TG_OP = 'INSERT' OR OLD.END_DATE IS NULL) THEN
            NEW.ENDED_BY := COALESCE(NEW.ENDED_BY, current_setting('etl_craft.actor'));
            NEW.ENDED_BY_KIND := COALESCE(NEW.ENDED_BY_KIND, current_setting('etl_craft.actor_kind'));
        END IF;
    ELSIF TG_TABLE_NAME IN ('aud_task_attempts','aud_run_interventions') AND TG_OP = 'INSERT' THEN
        NEW.REQUESTED_BY := COALESCE(NEW.REQUESTED_BY, current_setting('etl_craft.actor'));
        NEW.REQUESTED_BY_KIND := COALESCE(NEW.REQUESTED_BY_KIND, current_setting('etl_craft.actor_kind'));
    ELSIF TG_TABLE_NAME = 'aud_pipeline_pauses' THEN
        IF TG_OP = 'INSERT' THEN
            NEW.PAUSED_BY_KIND := COALESCE(NEW.PAUSED_BY_KIND, current_setting('etl_craft.actor_kind'));
        END IF;
        IF NEW.RESUMED_AT IS NOT NULL AND (TG_OP = 'INSERT' OR OLD.RESUMED_AT IS NULL) THEN
            NEW.RESUMED_BY_KIND := COALESCE(NEW.RESUMED_BY_KIND, current_setting('etl_craft.actor_kind'));
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
