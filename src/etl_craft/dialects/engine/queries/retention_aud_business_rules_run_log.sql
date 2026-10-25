DELETE FROM AUD_BUSINESS_RULES_RUN_LOG
WHERE
  BUSINESS_RULE_RUN_ID IN (
    SELECT
      t.BUSINESS_RULE_RUN_ID AS row_id
    FROM AUD_BUSINESS_RULES_RUN_LOG AS t
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
      AND NOT EXISTS(
        SELECT
          1
        FROM AUD_BUSINESS_RULES_RESULTS AS incoming
        WHERE
          incoming.BUSINESS_RULE_RUN_ID = t.BUSINESS_RULE_RUN_ID
        LIMIT 1
        OFFSET 0
      )
    ORDER BY
      t.BUSINESS_RULE_RUN_ID
    LIMIT 10000
  )
