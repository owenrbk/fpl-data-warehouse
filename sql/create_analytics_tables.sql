CREATE TABLE analytics.all_players AS
SELECT DISTINCT
    player_id,
    CONCAT(p.first_name, ' ', p.last_name) AS full_name,

    t.team_name,
    t.team_short,

    CASE p.element_type
        WHEN 1 THEN 'GKP'
        WHEN 2 THEN 'DEF'
        WHEN 3 THEN 'MID'
        WHEN 4 THEN 'FWD'
    END AS position,

    -- keep all the useful stat columns
    p.now_cost/10,
    p.total_points,
    p.points_per_game,
    p.minutes,
    p.goals_scored,
    p.assists,
    p.clean_sheets,
    p.goals_conceded,
    p.penalties_saved,
    p.penalties_missed,
    p.yellow_cards,
    p.red_cards,
    p.saves,
    p.bonus,
    p.bps,
    p.influence,
    p.creativity,
    p.threat,
    p.ict_index,
    p.starts,
    p.expected_goals,
    p.expected_assists,
    p.expected_goal_involvements,
    p.expected_goals_conceded,
    p.form
FROM core.players p
LEFT JOIN core.teams t
    ON p.team = t.team_id;

