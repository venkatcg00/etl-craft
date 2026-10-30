-- Business-rule runs within runs started in the window, per pipeline and rule: runs, failures,
-- table-rule passes and the rows each rule flagged.
SELECT r.PIPELINE_ID AS pipeline_id, b.BUSINESS_RULE_ID AS rule_id, b.BUSINESS_RULE_NAME AS rule_name,
       COUNT(*) AS runs, SUM(CASE WHEN br.STATUS = 'FAILED' THEN 1 ELSE 0 END) AS failed,
       SUM(CASE WHEN br.PASSED = 'Y' THEN 1 ELSE 0 END) AS passed,
       SUM(CASE WHEN br.PASSED = 'N' THEN 1 ELSE 0 END) AS not_passed,
       SUM((SELECT COUNT(*) FROM AUD_BUSINESS_RULES_RESULTS x
            WHERE x.BUSINESS_RULE_RUN_ID = br.BUSINESS_RULE_RUN_ID)) AS flagged_rows
FROM CFG_PIPELINES p
JOIN AUD_PIPELINES_RUN_LOG r ON r.PIPELINE_ID = p.PIPELINE_ID
 AND r.START_DATE >= :window_start AND r.START_DATE < :window_end
JOIN AUD_TASK_RUN_LOG t ON t.PIPELINE_RUN_ID = r.PIPELINE_RUN_ID
JOIN AUD_BUSINESS_RULES_RUN_LOG br ON br.TASK_RUN_ID = t.TASK_RUN_ID
JOIN CFG_BUSINESS_RULES b ON b.BUSINESS_RULE_ID = br.BUSINESS_RULE_ID
WHERE p.PROJECT_ID = :project_id
GROUP BY r.PIPELINE_ID, b.BUSINESS_RULE_ID, b.BUSINESS_RULE_NAME
