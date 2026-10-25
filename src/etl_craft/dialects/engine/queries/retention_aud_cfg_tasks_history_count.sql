SELECT
  COUNT(*) AS count
FROM AUD_CFG_TASKS_HISTORY AS t
WHERE
  (
    EXISTS(
      SELECT
        1
      FROM CFG_PIPELINES AS p
      WHERE
        p.PIPELINE_ID = t.PIPELINE_ID AND p.PROJECT_ID = :project_id
      LIMIT 1
      OFFSET 0
    )
  )
  AND t.CURRENT_FLAG = 'N'
  AND t.VALID_TO < :cutoff
