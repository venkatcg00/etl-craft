-- Pause :pipeline_id; the partial unique index refuses a second open pause.
INSERT INTO AUD_PIPELINE_PAUSES (PIPELINE_ID, PAUSED_AT, PAUSED_BY, REASON)
VALUES (:pipeline_id, :now, :paused_by, :reason)
