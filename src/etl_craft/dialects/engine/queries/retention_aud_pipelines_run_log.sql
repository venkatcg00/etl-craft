DELETE FROM AUD_PIPELINES_RUN_LOG
WHERE
  PIPELINE_RUN_ID IN (
    SELECT
      t.PIPELINE_RUN_ID AS row_id
    FROM AUD_PIPELINES_RUN_LOG AS t
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
      AND (
        t.TRIGGER_KIND <> 'SCHEDULE'
        OR EXISTS(
          SELECT
            1
          FROM AUD_PIPELINES_RUN_LOG AS newer
          WHERE
            newer.PIPELINE_ID = t.PIPELINE_ID
            AND newer.TRIGGER_KIND = 'SCHEDULE'
            AND newer.PIPELINE_RUN_ID > t.PIPELINE_RUN_ID
          LIMIT 1
          OFFSET 0
        )
      )
      AND NOT EXISTS(
        SELECT
          1
        FROM AUD_ACTIONS AS incoming
        WHERE
          incoming.PIPELINE_RUN_ID = t.PIPELINE_RUN_ID
        LIMIT 1
        OFFSET 0
      )
      AND NOT EXISTS(
        SELECT
          1
        FROM AUD_CONTROL_REQUESTS AS incoming
        WHERE
          incoming.PIPELINE_RUN_ID = t.PIPELINE_RUN_ID
        LIMIT 1
        OFFSET 0
      )
      AND NOT EXISTS(
        SELECT
          1
        FROM AUD_DEPENDENCY_CONSUMPTION AS incoming
        WHERE
          incoming.CONSUMED_PIPELINE_RUN_ID = t.PIPELINE_RUN_ID
        LIMIT 1
        OFFSET 0
      )
      AND NOT EXISTS(
        SELECT
          1
        FROM AUD_DEPENDENCY_CONSUMPTION AS incoming
        WHERE
          incoming.PIPELINE_RUN_ID = t.PIPELINE_RUN_ID
        LIMIT 1
        OFFSET 0
      )
      AND NOT EXISTS(
        SELECT
          1
        FROM AUD_GATE_DECISIONS AS incoming
        WHERE
          incoming.SELECTED_PIPELINE_RUN_ID = t.PIPELINE_RUN_ID
        LIMIT 1
        OFFSET 0
      )
      AND NOT EXISTS(
        SELECT
          1
        FROM AUD_GATE_DECISIONS AS incoming
        WHERE
          incoming.PIPELINE_RUN_ID = t.PIPELINE_RUN_ID
        LIMIT 1
        OFFSET 0
      )
      AND NOT EXISTS(
        SELECT
          1
        FROM AUD_GATE_WAITS AS incoming
        WHERE
          incoming.PIPELINE_RUN_ID = t.PIPELINE_RUN_ID
        LIMIT 1
        OFFSET 0
      )
      AND NOT EXISTS(
        SELECT
          1
        FROM AUD_RUN_INTERVENTIONS AS incoming
        WHERE
          incoming.PIPELINE_RUN_ID = t.PIPELINE_RUN_ID
        LIMIT 1
        OFFSET 0
      )
      AND NOT EXISTS(
        SELECT
          1
        FROM AUD_TASK_RUN_LOG AS incoming
        WHERE
          incoming.PIPELINE_RUN_ID = t.PIPELINE_RUN_ID
        LIMIT 1
        OFFSET 0
      )
    ORDER BY
      t.PIPELINE_RUN_ID
    LIMIT 10000
  )
