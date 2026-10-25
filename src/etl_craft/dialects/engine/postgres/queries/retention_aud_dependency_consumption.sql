-- Keep the bounded candidate array as an initplan for indexed target lookups.
DELETE FROM AUD_DEPENDENCY_CONSUMPTION WHERE CONSUMPTION_ID = ANY(ARRAY(
SELECT
  t.CONSUMPTION_ID AS row_id
FROM AUD_DEPENDENCY_CONSUMPTION AS t
WHERE
  (
    EXISTS(
      SELECT
        1
      FROM AUD_PIPELINES_RUN_LOG AS r
      JOIN CFG_PIPELINES AS p
        ON p.PIPELINE_ID = r.PIPELINE_ID
      WHERE
        p.PROJECT_ID = :project_id
        AND r.STATUS IN ('SUCCESS', 'FAILED', 'SKIPPED', 'CANCELLED')
        AND r.END_DATE < :cutoff
        AND r.PIPELINE_RUN_ID = t.PIPELINE_RUN_ID
      LIMIT 1
      OFFSET 0
    )
  )
  AND t.CONSUMED_AT < :cutoff
ORDER BY
  t.CONSUMPTION_ID
LIMIT 10000
))
