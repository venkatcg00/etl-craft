-- The latest recorded version of :task_id's documentation.
SELECT VERSION AS version, DOCUMENTATION_HASH AS documentation_hash
FROM AUD_TASK_DOCUMENTATION
WHERE TASK_ID = :task_id
ORDER BY VERSION DESC
LIMIT 1
