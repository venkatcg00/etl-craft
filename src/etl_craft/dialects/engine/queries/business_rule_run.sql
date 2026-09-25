-- The run-log row of :business_rule_id under :task_run_id.
SELECT BUSINESS_RULE_RUN_ID AS business_rule_run_id, STATUS AS status
FROM AUD_BUSINESS_RULES_RUN_LOG
WHERE BUSINESS_RULE_ID = :business_rule_id AND TASK_RUN_ID = :task_run_id
