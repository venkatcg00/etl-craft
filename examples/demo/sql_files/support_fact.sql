-- The support fact: every interaction with its agent's team and its support area's name.
SELECT i.fact_key,
       i.source_system,
       i.record_id,
       i.agent_code,
       ag.team,
       sa.area_name,
       i.interaction_date,
       i.handle_seconds,
       i.rating
FROM pre_dm.interactions i
LEFT JOIN ds.agents ag ON ag.agent_code = i.agent_code
LEFT JOIN ds.support_areas sa ON sa.area_code = i.support_area
