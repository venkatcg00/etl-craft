-- Active source definitions, in table and column order.
SELECT TABLE_NAME AS table_name, COLUMN_NAME AS column_name,
       DATA_TYPE AS data_type, COLUMN_POSITION AS column_position
FROM CFG_SOURCE_COLUMNS WHERE ACTIVE_FLAG = 'Y'
ORDER BY lower(TABLE_NAME), COLUMN_POSITION
