-- Active declared columns of active project objects, in contract order.
SELECT t.CATALOG_NAME AS catalog_name, t.SCHEMA_NAME AS schema_name,
       t.TABLE_NAME AS object_name, t.OBJECT_KIND AS object_kind,
       c.COLUMN_NAME AS column_name, c.DATA_TYPE AS data_type,
       c.COLUMN_POSITION AS column_position, c.IS_NULLABLE AS is_nullable
FROM CFG_COLUMNS c JOIN CFG_TABLES t ON t.TABLE_ID=c.TABLE_ID
WHERE c.ACTIVE_FLAG='Y' AND t.ACTIVE_FLAG='Y'
ORDER BY t.CATALOG_NAME, t.SCHEMA_NAME, t.TABLE_NAME, c.COLUMN_POSITION
