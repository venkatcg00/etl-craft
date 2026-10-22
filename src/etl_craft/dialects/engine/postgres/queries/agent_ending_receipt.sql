SELECT ARGUMENTS\:\:jsonb->>'request_sha256' AS request_sha256 FROM AUD_ACTIONS
WHERE COMMAND='agent report' AND ARGUMENTS\:\:jsonb->>'operation'='END'
AND ARGUMENTS\:\:jsonb->>'attempt_id'=:attempt_text ORDER BY ACTION_ID DESC LIMIT 1
