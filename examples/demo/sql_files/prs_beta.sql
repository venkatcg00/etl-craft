-- Client Beta's landed events, in the shared names, that are newer than the last one parsed.
SELECT b.support_identifier AS interaction_id,
       b.agent AS agent_code,
       b.regarding AS support_area,
       CAST(b.event_time AS DATE) AS contact_date,
       b.status,
       CAST(b.handle_time AS INTEGER) AS handle_seconds,
       CAST(b.score AS INTEGER) AS rating,
       CAST($$pipeline_id AS BIGINT) AS parsed_by
FROM lnd.client_beta b
WHERE b.support_identifier > (SELECT COALESCE(MAX(p.interaction_id), 0) FROM prs.beta_interactions p)
