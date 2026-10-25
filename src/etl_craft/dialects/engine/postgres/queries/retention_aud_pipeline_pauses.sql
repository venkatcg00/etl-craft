-- Keep the bounded candidate array as an initplan for indexed target lookups.
DELETE FROM AUD_PIPELINE_PAUSES WHERE PIPELINE_PAUSE_ID = ANY(ARRAY(
SELECT
  t.PIPELINE_PAUSE_ID AS row_id
FROM AUD_PIPELINE_PAUSES AS t
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
  AND t.RESUMED_AT < :cutoff
ORDER BY
  t.PIPELINE_PAUSE_ID
LIMIT 10000
))
