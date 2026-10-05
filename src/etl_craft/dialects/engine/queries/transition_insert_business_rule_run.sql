-- Start :business_rule_id under :task_run_id. The unique index refuses a second row.
INSERT INTO AUD_BUSINESS_RULES_RUN_LOG (BUSINESS_RULE_ID, TASK_RUN_ID, STATUS)
VALUES (:business_rule_id, :task_run_id, 'IN-PROGRESS')
RETURNING BUSINESS_RULE_RUN_ID AS business_rule_run_id
