SELECT (SELECT pr.PROJECT_CODE AS project_code FROM CFG_PROJECTS pr
        WHERE pr.PROJECT_ID = (SELECT p.PROJECT_ID AS project_id FROM CFG_PIPELINES p
                              WHERE p.PIPELINE_ID = b.PIPELINE_ID)) AS project_code,
       b.BUSINESS_RULE_ID AS business_rule_id, b.BUSINESS_RULE_NAME AS business_rule_name,
       b.BUSINESS_RULE_KEY_COLUMN AS business_rule_key_column, b.BUSINESS_RULE_SQL AS business_rule_sql
FROM CFG_BUSINESS_RULES b ORDER BY b.BUSINESS_RULE_ID
