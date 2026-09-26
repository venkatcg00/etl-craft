-- Every client's parsed interactions, in one shape, with a key unique across clients.
SELECT 'ALPHA-' || CAST(a.interaction_id AS VARCHAR) AS fact_key,
       'ALPHA' AS source_system,
       a.interaction_id AS record_id,
       a.agent_code,
       a.support_area,
       a.contact_date AS interaction_date,
       a.handle_seconds,
       a.rating
FROM prs.alpha_interactions a
UNION ALL
SELECT 'BETA-' || CAST(b.interaction_id AS VARCHAR) AS fact_key,
       'BETA' AS source_system,
       b.interaction_id AS record_id,
       b.agent_code,
       b.support_area,
       b.contact_date AS interaction_date,
       b.handle_seconds,
       b.rating
FROM prs.beta_interactions b
