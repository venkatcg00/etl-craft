SELECT p.PIPELINE_CODE AS pipeline_code,
       (WITH image AS ( {{pipeline_metadata_capture}} ) SELECT encode(sha256(convert_to(CAST(snapshot AS text),'UTF8')),'hex') AS version FROM image) AS metadata_version
FROM CFG_PIPELINES p WHERE p.ACTIVE_FLAG='Y'
