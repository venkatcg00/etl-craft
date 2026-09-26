-- Interactions, handle time and rating per support area and team.
SELECT f.area_name,
       f.team,
       COUNT(*) AS interactions,
       SUM(f.handle_seconds) AS handle_seconds,
       AVG(CAST(f.rating AS DECIMAL(10, 2))) AS average_rating
FROM dm.support_fact f
GROUP BY f.area_name, f.team
