-- Retain the computation key on every cached edge; callers must match it before use.
SELECT e.TASK_ID AS task_id, e.SOURCE_SQL_HASH AS source_sql_hash,
       e.TARGET_CATALOG_NAME AS target_catalog_name, e.TARGET_SCHEMA_NAME AS target_schema_name,
       e.TARGET_TABLE_NAME AS target_table_name, e.TARGET_COLUMN AS target_column,
       e.SOURCE_CATALOG_NAME AS source_catalog_name, e.SOURCE_SCHEMA_NAME AS source_schema_name,
       e.SOURCE_TABLE_NAME AS source_table_name, e.SOURCE_COLUMN AS source_column,
       e.TRANSFORMATION AS transformation
FROM AUD_COLUMN_LINEAGE e
JOIN CFG_TASKS t ON t.TASK_ID = e.TASK_ID
JOIN CFG_PIPELINES p ON p.PIPELINE_ID = t.PIPELINE_ID
WHERE t.ACTIVE_FLAG = 'Y' AND p.ACTIVE_FLAG = 'Y' AND t.HANDLER = 'SQL'
ORDER BY e.COLUMN_LINEAGE_ID
