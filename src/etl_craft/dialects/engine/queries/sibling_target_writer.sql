-- Another active SQL task in :pipeline_id, other than :task_id, that writes
-- :target_object with an action other than SETUP_TABLE; the lowest task id wins.
SELECT t.TASK_ID AS task_id, a.PARAMETER_VALUE AS sql_action
FROM CFG_TASK_PARAMETERS target_param
JOIN CFG_TASKS t ON t.TASK_ID = target_param.TASK_ID
JOIN CFG_TASK_PARAMETERS a ON a.TASK_ID = t.TASK_ID AND a.PARAMETER_NAME = 'SQL_ACTION'
WHERE t.PIPELINE_ID = :pipeline_id AND t.TASK_ID <> :task_id
  AND t.ACTIVE_FLAG = 'Y' AND t.HANDLER = 'SQL'
  AND target_param.ACTIVE_FLAG = 'Y' AND target_param.PARAMETER_NAME = 'TARGET_OBJECT'
  AND target_param.PARAMETER_VALUE = :target_object
  AND a.ACTIVE_FLAG = 'Y' AND a.PARAMETER_VALUE <> 'SETUP_TABLE'
ORDER BY t.TASK_ID
LIMIT 1
