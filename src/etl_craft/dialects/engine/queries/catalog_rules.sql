-- Every active business rule of an active task, with its task.
SELECT b.BUSINESS_RULE_ID AS business_rule_id, b.BUSINESS_RULE_NAME AS business_rule_name,
       b.BUSINESS_RULE_TYPE AS business_rule_type, b.BUSINESS_RULE_SQL AS business_rule_sql,
       b.BUSINESS_RULE_KEY_COLUMN AS key_column, b.TARGET_TABLE AS target_table,
       b.SEQUENCE_NUMBER AS sequence_number, p.PIPELINE_CODE AS pipeline_code,
       t.TASK_CODE AS task_code
FROM CFG_BUSINESS_RULES b
JOIN CFG_TASKS t ON t.TASK_ID = b.TASK_ID
JOIN CFG_PIPELINES p ON p.PIPELINE_ID = t.PIPELINE_ID
WHERE b.ACTIVE_FLAG = 'Y' AND t.ACTIVE_FLAG = 'Y' AND p.ACTIVE_FLAG = 'Y'
ORDER BY b.BUSINESS_RULE_NAME, b.BUSINESS_RULE_ID
