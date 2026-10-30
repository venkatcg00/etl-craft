SELECT x.PIPELINE_GRANT_ID AS grant_id,x.PROJECT_ID AS project_id,
x.PIPELINE_CODE AS pipeline_code,x.TAG AS tag,x.PERMISSION AS permission,
CASE WHEN x.GROUP_ID IS NOT NULL THEN 'group:' || CAST(x.GROUP_ID AS text)
     WHEN x.ACCOUNT_ID IS NOT NULL THEN 'account:' || a.ACCOUNT_NAME
     ELSE 'role:' || x.GRANTEE_ROLE END AS source
FROM CFG_PIPELINE_GRANTS x LEFT JOIN CFG_ACCOUNTS a ON a.ACCOUNT_ID=x.ACCOUNT_ID
LEFT JOIN CFG_GROUPS g ON g.GROUP_ID=x.GROUP_ID
WHERE x.ACTIVE_FLAG='Y' AND (CAST(:project_id AS bigint) IS NULL OR x.PROJECT_ID=:project_id)
AND ((a.ACCOUNT_NAME=:account AND a.ACTIVE_FLAG='Y')
 OR (x.GRANTEE_ROLE='viewer' AND :role IN ('viewer','operator','developer','admin'))
 OR (x.GRANTEE_ROLE='operator' AND :role IN ('operator','developer','admin'))
 OR (g.ACTIVE_FLAG='Y' AND (EXISTS (
   SELECT 1 AS member FROM CFG_GROUP_MEMBERS m JOIN CFG_ACCOUNTS u ON u.ACCOUNT_ID=m.ACCOUNT_ID
   WHERE m.GROUP_ID=g.GROUP_ID AND m.ACTIVE_FLAG='Y' AND u.ACCOUNT_NAME=:account AND u.ACTIVE_FLAG='Y')
   OR g.GROUP_ID IN (SELECT CAST(value AS bigint) AS group_id FROM json_each(:sso_group_ids)))))
