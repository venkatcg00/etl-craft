-- The active business rules of :task_id, in the order they run.
SELECT BUSINESS_RULE_ID AS business_rule_id, BUSINESS_RULE_NAME AS business_rule_name,
       BUSINESS_RULE_SQL AS business_rule_sql, BUSINESS_RULE_TYPE AS business_rule_type,
       BUSINESS_RULE_KEY_COLUMN AS business_rule_key_column, TARGET_TABLE AS target_table,
       SEQUENCE_NUMBER AS sequence_number
FROM CFG_BUSINESS_RULES
WHERE TASK_ID = :task_id AND ACTIVE_FLAG = 'Y'
ORDER BY SEQUENCE_NUMBER, BUSINESS_RULE_ID
