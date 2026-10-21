-- Bind the canonical local log to the attempt in its owner-claim transaction.
UPDATE AUD_TASK_ATTEMPTS SET LOG_PATH = :log_path
WHERE ATTEMPT_ID = :attempt_id AND OWNER_ID = :owner AND STATUS = 'CLAIMED'
