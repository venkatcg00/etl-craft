-- Start another attempt of :business_rule_run_id.
UPDATE AUD_BUSINESS_RULES_RUN_LOG
SET STATUS = 'IN-PROGRESS', START_DATE = :now, END_DATE = NULL
WHERE BUSINESS_RULE_RUN_ID = :business_rule_run_id
