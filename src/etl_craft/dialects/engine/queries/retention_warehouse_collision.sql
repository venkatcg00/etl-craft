SELECT TABLE_ID AS table_id, PROJECT_ID AS project_id FROM CFG_TABLES
WHERE lower(SCHEMA_NAME)=lower(:schema) AND lower(TABLE_NAME)=lower(:table)
AND (CATALOG_NAME IS NULL OR lower(CATALOG_NAME)=lower(:catalog)) AND TABLE_ID<>:id
LIMIT 1
