-- Preserve the driver's typed UTC instants for clock rollback checks.
SELECT MAX(LAST_UPDATED_TIMESTAMP) AS committed, MAX(PENDING_UPPER_BOUND) AS pending
FROM AUD_TASK_OFFSET_TRACKER
