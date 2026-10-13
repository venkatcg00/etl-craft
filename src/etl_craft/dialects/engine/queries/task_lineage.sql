SELECT TARGET_CATALOG_NAME AS target_catalog_name, TARGET_SCHEMA_NAME AS target_schema_name, TARGET_TABLE_NAME AS target_table_name, TARGET_COLUMN AS target_column, SOURCE_CATALOG_NAME AS source_catalog_name, SOURCE_SCHEMA_NAME AS source_schema_name, SOURCE_TABLE_NAME AS source_table_name, SOURCE_COLUMN AS source_column, TRANSFORMATION AS transformation
FROM AUD_COLUMN_LINEAGE
WHERE TASK_ID=:task_id AND SOURCE_SQL_HASH=:source_sql_hash
ORDER BY COLUMN_LINEAGE_ID
