-- Active project object contracts, including tables without declared columns.
SELECT CATALOG_NAME AS catalog_name, SCHEMA_NAME AS schema_name,
       TABLE_NAME AS table_name, OBJECT_KIND AS object_kind
FROM CFG_TABLES WHERE ACTIVE_FLAG='Y' AND PROJECT_ID=:project_id
ORDER BY CATALOG_NAME, SCHEMA_NAME, TABLE_NAME
