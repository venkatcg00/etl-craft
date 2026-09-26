-- Client Alpha's landed interactions, typed, that are newer than the last one parsed.
-- $$pipeline_id stamps the run that parsed them.
SELECT CAST(a.interaction_id AS BIGINT) AS interaction_id,
       a.agent_code,
       a.support_area,
       CAST(a.contact_date AS DATE) AS contact_date,
       a.status,
       CAST(a.duration_seconds AS INTEGER) AS handle_seconds,
       CAST(a.rating AS INTEGER) AS rating,
       CAST($$pipeline_id AS BIGINT) AS parsed_by
FROM lnd.client_alpha a
WHERE a.interaction_id > (SELECT COALESCE(MAX(p.interaction_id), 0) FROM prs.alpha_interactions p)
