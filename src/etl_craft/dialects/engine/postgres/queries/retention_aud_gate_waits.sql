-- Keep the bounded candidate array as an initplan for indexed target lookups.
DELETE FROM AUD_GATE_WAITS WHERE ctid = ANY(ARRAY(
SELECT
  t.ctid
FROM AUD_GATE_WAITS AS t
WHERE
  EXISTS(
    SELECT
      1
    FROM AUD_PIPELINES_RUN_LOG AS r
    JOIN CFG_PIPELINES AS p
      ON p.PIPELINE_ID = r.PIPELINE_ID
    WHERE
      r.PIPELINE_RUN_ID = t.PIPELINE_RUN_ID
      AND p.PROJECT_ID = :project_id
      AND r.STATUS IN ('SUCCESS', 'FAILED', 'SKIPPED', 'CANCELLED')
      AND r.END_DATE < :cutoff
    LIMIT 1
    OFFSET 0
  )
ORDER BY
  t.PIPELINE_RUN_ID,
  COALESCE(t.TASK_ID, 0),
  t.WAIT_KIND
LIMIT 10000
))
