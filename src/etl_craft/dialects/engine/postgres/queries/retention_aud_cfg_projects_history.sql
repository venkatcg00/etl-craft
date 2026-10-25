-- Keep the bounded candidate array as an initplan for indexed target lookups.
DELETE FROM AUD_CFG_PROJECTS_HISTORY WHERE CHANGE_ID = ANY(ARRAY(
SELECT
  t.CHANGE_ID AS row_id
FROM AUD_CFG_PROJECTS_HISTORY AS t
WHERE
  (
    t.PROJECT_ID = :project_id
  ) AND t.CURRENT_FLAG = 'N' AND t.VALID_TO < :cutoff
ORDER BY
  t.CHANGE_ID
LIMIT 10000
))
