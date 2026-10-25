DELETE FROM AUD_GATE_WAITS
WHERE
  (PIPELINE_RUN_ID, COALESCE(TASK_ID, 0), WAIT_KIND) IN (
    SELECT
      t.PIPELINE_RUN_ID AS pipeline_run_id,
      COALESCE(t.TASK_ID, 0) AS task_id,
      t.WAIT_KIND AS wait_kind
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
  )
