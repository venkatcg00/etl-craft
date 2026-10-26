-- etl-craft: check-business-rule-flags
CREATE UNIQUE INDEX ux_brresults_active_key ON AUD_BUSINESS_RULES_RESULTS
    (BUSINESS_RULE_ID, BUSINESS_RULE_KEY) WHERE ACTIVE_FLAG = 'Y';
