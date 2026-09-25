-- The handler of every active task in :pipeline_id.
SELECT DISTINCT HANDLER AS handler
FROM CFG_TASKS
WHERE PIPELINE_ID = :pipeline_id AND ACTIVE_FLAG = 'Y'
