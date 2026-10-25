-- Keep the bounded candidate array as an initplan for indexed target lookups.
DELETE FROM AUD_CFG_PIPELINE_DEPENDENCY_HISTORY WHERE CHANGE_ID = ANY(ARRAY(
SELECT
  t.CHANGE_ID AS row_id
FROM AUD_CFG_PIPELINE_DEPENDENCY_HISTORY AS t
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
ORDER BY
  t.CHANGE_ID
LIMIT 10000
))
