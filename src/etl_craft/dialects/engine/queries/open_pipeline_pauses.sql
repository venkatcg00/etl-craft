-- Every paused pipeline, by code, with its open pause.
SELECT p.PIPELINE_CODE AS pipeline_code, a.PAUSED_AT AS paused_at, a.PAUSED_BY AS paused_by,
       a.REASON AS reason
FROM AUD_PIPELINE_PAUSES a
JOIN CFG_PIPELINES p ON p.PIPELINE_ID = a.PIPELINE_ID
WHERE a.RESUMED_AT IS NULL
ORDER BY p.PIPELINE_CODE
