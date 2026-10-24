SELECT p.PIPELINE_CODE AS pipeline_code,
       (WITH image AS ( {{pipeline_metadata_capture}} ) SELECT etl_craft_sha256(snapshot) AS version FROM image) AS metadata_version
FROM CFG_PIPELINES p WHERE p.ACTIVE_FLAG='Y' AND p.PROJECT_ID=:project_id
