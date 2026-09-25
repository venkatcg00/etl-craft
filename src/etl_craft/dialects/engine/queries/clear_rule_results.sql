-- Clear the active flags of :business_rule_id on :keys.
UPDATE AUD_BUSINESS_RULES_RESULTS
SET ACTIVE_FLAG = 'N', END_DATE = :now
WHERE BUSINESS_RULE_ID = :business_rule_id AND ACTIVE_FLAG = 'Y' AND BUSINESS_RULE_KEY IN :keys
