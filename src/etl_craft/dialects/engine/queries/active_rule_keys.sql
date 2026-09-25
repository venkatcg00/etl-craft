-- The keys :business_rule_id currently flags.
SELECT BUSINESS_RULE_KEY AS business_rule_key
FROM AUD_BUSINESS_RULES_RESULTS
WHERE BUSINESS_RULE_ID = :business_rule_id AND ACTIVE_FLAG = 'Y'
