-- The active business rules of :task_id, in execution order.
SELECT b.BUSINESS_RULE_ID AS business_rule_id, b.BUSINESS_RULE_NAME AS business_rule_name,
       b.BUSINESS_RULE_SQL AS business_rule_sql, b.BUSINESS_RULE_TYPE AS business_rule_type,
       b.BUSINESS_RULE_LEVEL AS business_rule_level, b.BUSINESS_RULE_KEY_COLUMN AS business_rule_key_column,
       CASE WHEN o.CATALOG_NAME IS NULL THEN '' ELSE o.CATALOG_NAME || '.' END || o.SCHEMA_NAME || '.' || o.TABLE_NAME AS target_table, b.SEQUENCE_NUMBER AS sequence_number, o.ACTIVE_FLAG AS target_active
FROM CFG_BUSINESS_RULES b JOIN CFG_TABLES o ON o.TABLE_ID=b.TARGET_TABLE_ID
WHERE b.TASK_ID=:task_id AND b.ACTIVE_FLAG='Y'
ORDER BY b.SEQUENCE_NUMBER, b.BUSINESS_RULE_ID
