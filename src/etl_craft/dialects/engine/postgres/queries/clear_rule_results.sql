-- Resolve each batch key with the active-key index before updating its exact row.
WITH matched AS materialized (
 SELECT flagged.BUSINESS_RULE_RESULT_ID AS business_rule_result_id
 FROM jsonb_array_elements_text(CAST(:keys AS jsonb)) AS keys(value)
 CROSS JOIN LATERAL (
  SELECT r.BUSINESS_RULE_RESULT_ID AS business_rule_result_id
  FROM AUD_BUSINESS_RULES_RESULTS r
  WHERE r.BUSINESS_RULE_ID=:business_rule_id AND r.ACTIVE_FLAG='Y'
  AND r.BUSINESS_RULE_KEY=keys.value
  -- Preserve the per-key lateral probe when table statistics underestimate the rule.
  OFFSET 0
 ) flagged
)
UPDATE AUD_BUSINESS_RULES_RESULTS
SET ACTIVE_FLAG='N',END_DATE=:now
WHERE BUSINESS_RULE_RESULT_ID = ANY(ARRAY(SELECT business_rule_result_id FROM matched))
AND ACTIVE_FLAG='Y'
