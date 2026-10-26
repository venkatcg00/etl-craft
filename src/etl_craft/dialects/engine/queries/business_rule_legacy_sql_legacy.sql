SELECT 'default' AS project_code, BUSINESS_RULE_ID AS business_rule_id, BUSINESS_RULE_NAME AS business_rule_name,
       BUSINESS_RULE_KEY_COLUMN AS business_rule_key_column, BUSINESS_RULE_SQL AS business_rule_sql
FROM CFG_BUSINESS_RULES ORDER BY BUSINESS_RULE_ID
