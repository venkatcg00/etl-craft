SELECT SOURCE_COLUMN_ID AS source_column_id, TABLE_NAME AS table_name, COLUMN_NAME AS column_name, DATA_TYPE AS data_type, COLUMN_POSITION AS column_position, ACTIVE_FLAG AS active_flag, CREATED_BY AS created_by, CREATE_DATE AS create_date, UPDATED_BY AS updated_by, UPDATED_DATE AS updated_date
FROM CFG_SOURCE_COLUMNS ORDER BY SOURCE_COLUMN_ID
