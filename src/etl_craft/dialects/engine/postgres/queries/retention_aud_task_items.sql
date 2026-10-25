-- Keep the bounded candidate array as an initplan for indexed target lookups.
DELETE FROM AUD_TASK_ITEMS WHERE ITEM_RECORD_ID = ANY(ARRAY(
SELECT
  t.ITEM_RECORD_ID AS row_id
FROM AUD_TASK_ITEMS AS t
WHERE
  (
    EXISTS(
      SELECT
        1
      FROM AUD_TASK_RUN_LOG AS tr
      JOIN AUD_PIPELINES_RUN_LOG AS r
        ON r.PIPELINE_RUN_ID = tr.PIPELINE_RUN_ID
      JOIN CFG_PIPELINES AS p
        ON p.PIPELINE_ID = r.PIPELINE_ID
      WHERE
        tr.TASK_RUN_ID = t.TASK_RUN_ID
        AND p.PROJECT_ID = :project_id
        AND r.STATUS IN ('SUCCESS', 'FAILED', 'SKIPPED', 'CANCELLED')
        AND r.END_DATE < :cutoff
      LIMIT 1
      OFFSET 0
    )
  )
  AND t.STATUS IN ('SUCCESS', 'FAILED', 'SKIPPED', 'CANCELLED')
  AND t.ENDED_AT < :cutoff
ORDER BY
  t.ITEM_RECORD_ID
LIMIT 10000
))
