-- Close the open pause of :pipeline_id.
UPDATE AUD_PIPELINE_PAUSES
SET RESUMED_AT = :now, RESUMED_BY = :resumed_by, RESUME_REASON = :reason, RESUMED_BY_KIND = :actor_kind
WHERE PIPELINE_ID = :pipeline_id AND RESUMED_AT IS NULL
