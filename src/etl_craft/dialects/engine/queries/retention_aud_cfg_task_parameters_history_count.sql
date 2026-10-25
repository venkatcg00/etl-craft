SELECT
  COUNT(*) AS count
FROM AUD_CFG_TASK_PARAMETERS_HISTORY AS t
WHERE
  (
    EXISTS(
      SELECT
        1
      FROM CFG_TASKS AS q
      JOIN CFG_PIPELINES AS p
        ON p.PIPELINE_ID = q.PIPELINE_ID
      WHERE
        q.TASK_ID = t.TASK_ID AND p.PROJECT_ID = :project_id
      LIMIT 1
      OFFSET 0
    )
  )
  AND t.CURRENT_FLAG = 'N'
  AND t.VALID_TO < :cutoff
