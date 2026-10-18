-- Recovery, restart and one-run permission to execute while the definition stays paused.
ALTER TABLE AUD_CONTROL_REQUESTS ALTER COLUMN ACTION TYPE VARCHAR(16);
ALTER TABLE AUD_CONTROL_REQUESTS DROP CONSTRAINT aud_control_requests_action_check;
ALTER TABLE AUD_CONTROL_REQUESTS ADD CONSTRAINT ck_control_action CHECK (ACTION IN ('HOLD','RELEASE','STOP','ABORT','RECOVER','RESTART','RUN_PAUSED'));
