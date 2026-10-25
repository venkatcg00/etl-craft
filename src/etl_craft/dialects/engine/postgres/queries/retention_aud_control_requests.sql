-- Keep the bounded candidate array as an initplan for indexed target lookups.
DELETE FROM AUD_CONTROL_REQUESTS WHERE CONTROL_ID = ANY(ARRAY(
SELECT
  t.CONTROL_ID AS row_id
FROM AUD_CONTROL_REQUESTS AS t
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
  AND t.COMPLETED_AT < :cutoff
ORDER BY
  t.CONTROL_ID
LIMIT 10000
))
