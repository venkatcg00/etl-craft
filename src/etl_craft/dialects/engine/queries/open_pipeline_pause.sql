-- The open pause of :pipeline_id, if it is paused.
SELECT PAUSED_AT AS paused_at, PAUSED_BY AS paused_by, REASON AS reason
FROM AUD_PIPELINE_PAUSES
WHERE PIPELINE_ID = :pipeline_id AND RESUMED_AT IS NULL
