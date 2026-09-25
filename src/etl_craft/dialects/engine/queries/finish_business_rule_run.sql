-- End :business_rule_run_id with :status at :now.
UPDATE AUD_BUSINESS_RULES_RUN_LOG
SET STATUS = :status, END_DATE = :now
WHERE BUSINESS_RULE_RUN_ID = :business_rule_run_id
