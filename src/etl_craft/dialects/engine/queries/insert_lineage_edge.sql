-- Store one column's source for :task_id.
INSERT INTO AUD_COLUMN_LINEAGE (
    TASK_ID, SOURCE_SQL_HASH, TARGET_OBJECT, TARGET_COLUMN, SOURCE_OBJECT, SOURCE_COLUMN,
    TRANSFORMATION, COMPUTED_AT
)
VALUES (:task_id, :source_sql_hash, :target_object, :target_column, :source_object,
        :source_column, :transformation, :now)
