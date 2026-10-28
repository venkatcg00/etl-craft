-- Replacement closes and opens at one instant; a gap or expiry ends the captured pause.
SELECT p.PIPELINE_PAUSE_ID AS pause_id,p.PIPELINE_ID AS pipeline_id,p.TASK_ID AS task_id,
p.OUTCOME AS outcome,p.EXPIRES_AT AS expires_at,p.PAUSED_AT AS paused_at,
p.PAUSED_BY AS actor,p.REASON AS reason,t.TASK_CODE AS task_code
FROM AUD_RUN_INTERVENTIONS i JOIN AUD_PIPELINE_PAUSES p
ON CAST(p.PIPELINE_PAUSE_ID AS varchar)=i.ITEM_KEY
JOIN CFG_TASKS t ON t.TASK_ID=p.TASK_ID
WHERE i.PIPELINE_RUN_ID=:run_id AND i.ACTION='PAUSE_ADMISSION'
AND NOT EXISTS (
    SELECT 1 FROM AUD_PIPELINE_PAUSES boundary
    WHERE boundary.TASK_ID=p.TASK_ID
      AND boundary.PIPELINE_PAUSE_ID>=p.PIPELINE_PAUSE_ID
      AND boundary.RESUMED_AT IS NOT NULL
      AND (boundary.EXPIRES_AT<=boundary.RESUMED_AT OR NOT EXISTS (
          SELECT 1 FROM AUD_PIPELINE_PAUSES successor
          WHERE successor.TASK_ID=boundary.TASK_ID
            AND successor.PIPELINE_PAUSE_ID>boundary.PIPELINE_PAUSE_ID
            AND successor.PAUSED_AT=boundary.RESUMED_AT
      ))
)
