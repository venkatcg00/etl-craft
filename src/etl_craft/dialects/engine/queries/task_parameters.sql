-- The active parameters of :task_id.
SELECT PARAMETER_NAME AS parameter_name, PARAMETER_VALUE AS parameter_value
FROM CFG_TASK_PARAMETERS
WHERE TASK_ID = :task_id AND ACTIVE_FLAG = 'Y'
