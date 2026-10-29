-- Every admitted high is at or before its offset-write clock; source positions may be future-dated.
SELECT MAX(LAST_UPDATED_TIMESTAMP) AS committed, MAX(PENDING_UPPER_BOUND) AS pending
FROM AUD_TASK_OFFSET_TRACKER
