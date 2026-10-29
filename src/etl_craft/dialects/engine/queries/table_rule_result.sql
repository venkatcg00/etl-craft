-- Publish table evidence on the bound rule run, never to per-row flag storage.
UPDATE AUD_BUSINESS_RULES_RUN_LOG SET PASSED=:passed,OBSERVED=:observed,DETAIL=:detail
WHERE BUSINESS_RULE_RUN_ID=:business_rule_run_id
