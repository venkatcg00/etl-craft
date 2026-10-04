-- The rows each attempt wrote (inserted, updated or deleted), which a HAS_DATA dependency reads.
-- A row recorded before this has none, and HAS_DATA falls back to its TARGET_COUNT.
ALTER TABLE AUD_TASK_RUN_LOG ADD COLUMN ROWS_WRITTEN BIGINT;
