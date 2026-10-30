-- Rule runs within runs started in the window, with any value a table rule observed.
SELECT r.PIPELINE_ID AS pipeline_id, b.BUSINESS_RULE_ID AS rule_id, r.PIPELINE_RUN_ID AS pipeline_run_id,
       br.BUSINESS_RULE_RUN_ID AS rule_run_id,
       br.END_DATE AS observed_at, br.PASSED AS passed, br.OBSERVED AS observed
FROM CFG_PIPELINES p
JOIN AUD_PIPELINES_RUN_LOG r ON r.PIPELINE_ID = p.PIPELINE_ID
 AND r.START_DATE >= :window_start AND r.START_DATE < :window_end
JOIN AUD_TASK_RUN_LOG t ON t.PIPELINE_RUN_ID = r.PIPELINE_RUN_ID
JOIN AUD_BUSINESS_RULES_RUN_LOG br ON br.TASK_RUN_ID = t.TASK_RUN_ID
JOIN CFG_BUSINESS_RULES b ON b.BUSINESS_RULE_ID = br.BUSINESS_RULE_ID
WHERE p.PROJECT_ID = :project_id
