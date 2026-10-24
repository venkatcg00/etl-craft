WITH origin AS (
    SELECT t.TASK_ID, t.PIPELINE_ID, t.TARGET_TABLE_ID, t.TARGET_ACTIVE_FLAG,
           EXISTS (SELECT 1 AS found FROM CFG_TASK_PARAMETERS wp WHERE wp.TASK_ID=t.TASK_ID
                   AND wp.PARAMETER_NAME='WATERMARK_COLUMN' AND wp.ACTIVE_FLAG='Y') AS watermark_enabled,
           lower(COALESCE(o.CATALOG_NAME, :catalog)) AS catalog_name,
           lower(o.SCHEMA_NAME) AS schema_name, lower(o.TABLE_NAME) AS table_name
    FROM CFG_TASKS t LEFT JOIN CFG_TABLES o ON o.TABLE_ID=t.TARGET_TABLE_ID
    WHERE t.TASK_ID=:task_id
), writers AS (
    SELECT t.TASK_ID AS task_id, t.TASK_CODE AS task_code, t.PIPELINE_ID AS pipeline_id,
           p.ACTIVE_FLAG AS pipeline_active, a.PARAMETER_VALUE AS sql_action
    FROM CFG_TASKS t
    JOIN origin ON origin.TARGET_TABLE_ID=t.TARGET_TABLE_ID
    JOIN CFG_TABLES o ON o.TABLE_ID=t.TARGET_TABLE_ID AND o.ACTIVE_FLAG='Y'
    JOIN CFG_PIPELINES p ON p.PIPELINE_ID=t.PIPELINE_ID
    JOIN CFG_TASK_PARAMETERS a ON a.TASK_ID=t.TASK_ID
      AND a.PARAMETER_NAME='SQL_ACTION' AND a.ACTIVE_FLAG='Y'
    WHERE t.TASK_ID<>:task_id AND t.ACTIVE_FLAG='Y' AND t.HANDLER='SQL'
      AND t.TARGET_ACTIVE_FLAG='Y' AND origin.TARGET_ACTIVE_FLAG='Y'
)
SELECT origin.catalog_name AS catalog_name, origin.schema_name AS schema_name,
       origin.table_name AS table_name, h.HASH_VERSION AS hash_version,
       v.STATEMENT_SHA256 AS statement_sha256,
       off.OFFSET_TYPE AS offset_type, off.OFFSET_VALUE AS offset_value,
       peer.task_id AS task_id, peer.task_code AS task_code, peer.sql_action AS sql_action,
       r.STATUS AS status,
       (SELECT upper(trim(w.sql_action)) FROM writers w WHERE w.pipeline_active='Y'
        AND upper(trim(w.sql_action)) IN ('SCD1_MERGE','SCD2_MERGE')
        AND upper(trim(w.sql_action))<>:sql_action LIMIT 1) AS merge_conflict
FROM origin
LEFT JOIN AUD_TASK_OFFSET_TRACKER off ON off.TASK_ID=origin.TASK_ID
  AND origin.watermark_enabled
LEFT JOIN AUD_TARGET_HASH_VERSION h ON h.TARGET_CATALOG_NAME=origin.catalog_name
  AND h.TARGET_SCHEMA_NAME=origin.schema_name AND h.TARGET_TABLE_NAME=origin.table_name
LEFT JOIN AUD_TARGET_VIEW_STATEMENT v ON v.TARGET_CATALOG_NAME=origin.catalog_name
  AND v.TARGET_SCHEMA_NAME=origin.schema_name AND v.TARGET_TABLE_NAME=origin.table_name
LEFT JOIN writers peer ON peer.pipeline_id=:pipeline_id AND :sql_action IN ('SETUP_TABLE','DROP_TABLE')
LEFT JOIN AUD_TASK_RUN_LOG r ON r.TASK_ID=peer.task_id AND r.PIPELINE_RUN_ID=:pipeline_run_id
ORDER BY peer.task_id
