-- Record version :version of :task_id's documentation.
INSERT INTO AUD_TASK_DOCUMENTATION (TASK_ID, VERSION, DOCUMENTATION_HASH, DOCUMENTATION, RECORDED_AT)
VALUES (:task_id, :version, :documentation_hash, :documentation, :now)
