-- Flag :business_rule_key for :business_rule_id, found by :business_rule_run_id.
INSERT INTO AUD_BUSINESS_RULES_RESULTS (
    BUSINESS_RULE_RUN_ID, BUSINESS_RULE_ID, BUSINESS_RULE_KEY, TARGET_TABLE, STATUS,
    ACTIVE_FLAG, START_DATE
)
VALUES (:business_rule_run_id, :business_rule_id, :business_rule_key, :target_table, :status,
        'Y', :now)
