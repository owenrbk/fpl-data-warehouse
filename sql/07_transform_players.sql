-- 07_transform_players.sql
-- Creates analytics-ready views or tables from players and player_history
-- Example: total points per player, average points per gameweek

CREATE VIEW player_summary AS
SELECT
    p.player_id,
    p.first_name,
    p.last_name,
    p.team_id,
    SUM(ph.total_points) AS total_points,
    AVG(ph.total_points) AS avg_points
FROM players p
JOIN player_history ph ON p.player_id = ph.player_id
GROUP BY p.player_id, p.first_name, p.last_name, p.team_id;
