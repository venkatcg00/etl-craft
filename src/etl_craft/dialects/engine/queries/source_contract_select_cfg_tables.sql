SELECT TABLE_ID AS table_id, CATALOG_NAME AS catalog_name, SCHEMA_NAME AS schema_name, TABLE_NAME AS table_name FROM CFG_TABLES WHERE PROJECT_ID=:project_id
