SELECT json_extract(ARGUMENTS, '$.request_sha256') AS request_sha256 FROM AUD_ACTIONS
WHERE COMMAND='agent report' AND json_extract(ARGUMENTS, '$.operation')='END'
AND json_extract(ARGUMENTS, '$.attempt_id')=:attempt_text ORDER BY ACTION_ID DESC LIMIT 1
