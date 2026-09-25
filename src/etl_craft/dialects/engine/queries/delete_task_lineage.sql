-- Forget the lineage stored for :task_id.
DELETE FROM AUD_COLUMN_LINEAGE WHERE TASK_ID = :task_id
