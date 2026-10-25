-- SQLite serializes the transaction with BEGIN IMMEDIATE; PostgreSQL overrides with FOR UPDATE.
SELECT ATTEMPT_ID AS attempt_id FROM AUD_TASK_ATTEMPTS WHERE ATTEMPT_ID=:attempt_id
