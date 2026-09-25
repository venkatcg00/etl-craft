-- Every active business rule's warehouse table and key column.
SELECT BUSINESS_RULE_NAME AS business_rule_name, TARGET_TABLE AS target_table,
       BUSINESS_RULE_KEY_COLUMN AS key_column
FROM CFG_BUSINESS_RULES
WHERE ACTIVE_FLAG = 'Y'
ORDER BY BUSINESS_RULE_NAME
