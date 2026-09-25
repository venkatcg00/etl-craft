-- The offset :task_id stored after its last successful run.
SELECT OFFSET_TYPE AS offset_type, OFFSET_VALUE AS offset_value
FROM AUD_TASK_OFFSET_TRACKER
WHERE TASK_ID = :task_id
