-- The offset :task_id stored after its last successful run.
SELECT OFFSET_TYPE AS offset_type, OFFSET_VALUE AS offset_value,
PENDING_TASK_RUN_ID AS pending_task_run_id, PENDING_UPPER_BOUND AS pending_upper_bound
FROM AUD_TASK_OFFSET_TRACKER
WHERE TASK_ID = :task_id
