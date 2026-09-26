-- The latest :limit runs of every active pipeline, newest first.
SELECT p.PIPELINE_CODE AS pipeline_code, r.PIPELINE_RUN_ID AS pipeline_run_id,
       r.STATUS AS status, r.RUN_DATE AS run_date, r.BACKFILL AS backfill,
       r.START_DATE AS start_date, r.END_DATE AS end_date, r.SLA_STATUS AS sla_status
FROM (
    SELECT PIPELINE_ID, PIPELINE_RUN_ID, STATUS, RUN_DATE, BACKFILL, START_DATE, END_DATE,
           SLA_STATUS,
           ROW_NUMBER() OVER (PARTITION BY PIPELINE_ID ORDER BY PIPELINE_RUN_ID DESC) AS rn
    FROM AUD_PIPELINES_RUN_LOG
) r
JOIN CFG_PIPELINES p ON p.PIPELINE_ID = r.PIPELINE_ID
WHERE r.rn <= :limit AND p.ACTIVE_FLAG = 'Y'
ORDER BY p.PIPELINE_CODE, r.PIPELINE_RUN_ID DESC
