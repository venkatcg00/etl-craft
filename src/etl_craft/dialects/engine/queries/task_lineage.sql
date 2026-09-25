-- The lineage of :task_id stored for the SELECT hashed as :source_sql_hash.
SELECT TARGET_OBJECT AS target_object, TARGET_COLUMN AS target_column,
       SOURCE_OBJECT AS source_object, SOURCE_COLUMN AS source_column,
       TRANSFORMATION AS transformation
FROM AUD_COLUMN_LINEAGE
WHERE TASK_ID = :task_id AND SOURCE_SQL_HASH = :source_sql_hash
ORDER BY TARGET_COLUMN, SOURCE_OBJECT, SOURCE_COLUMN
