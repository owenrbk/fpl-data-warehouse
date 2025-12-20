DROP TABLE IF EXISTS analytics.all_players;
CREATE TABLE analytics.all_players IF NOT EXISTS AS
SELECT DISTINCT
    player_id,
    CONCAT(p.first_name, ' ', p.last_name) AS full_name,
    t.team_name,
    t.team_short,
    CASE p.position
        WHEN 1 THEN 'GKP'
        WHEN 2 THEN 'DEF'
        WHEN 3 THEN 'MID'
        WHEN 4 THEN 'FWD'
    END AS position,
    p.now_cost/10 AS cost,
    p.form,
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
    p.expected_goals_conceded
FROM core.players p
LEFT JOIN core.teams t
    ON p.team_id = t.team_id;

DROP TABLE IF EXISTS analytics.all_teams;

CREATE TABLE analytics.all_teams AS
SELECT
    t.team_id,
    t.team_name,
    t.position AS fpl_table_position,
    t.strength,
    t.strength_overall_home,
    t.strength_overall_away,
    t.strength_attack_home,
    t.strength_attack_away,
    t.strength_defence_home,
    t.strength_defence_away,
    fm.avg_fotmob_rating,
    fm.matched_fotmob_team,
    fm.similarity_score
FROM core.teams t
LEFT JOIN LATERAL (
    SELECT
        fr.team_name AS matched_fotmob_team,
        AVG(fr.rating) AS avg_fotmob_rating,
        similarity(fr.team_name, t.team_name) AS similarity_score
    FROM core.fotmob_ratings fr
    GROUP BY fr.team_name
    ORDER BY similarity(fr.team_name, t.team_name) DESC
    LIMIT 1
) fm ON TRUE;

DROP TABLE IF EXISTS analytics.gameweeks;
CREATE TABLE analytics.gameweeks IF NOT EXISTS AS
SELECT
    g.gameweek_id AS Gameweek,
    g.average_score AS "Average Score",
    g.highest_score AS "Highest Score",
    g.ranked_count AS "Total # of Managers",
    p_most_selected.full_name AS "Most Selected",
    p_most_transferred_in.full_name AS "Most Transferred In",
    p_most_captained.full_name AS "Most Captained",
    p_most_vice_captained.full_name AS "Most Vice Captained",
    p_top_player.full_name AS "Top Player",
    g.transfers_made AS "Transfers Made",
    COALESCE(SUM(c.num_played) FILTER (WHERE c.chip_name='freehit'),0) AS "Num of Free Hit",
    COALESCE(SUM(c.num_played) FILTER (WHERE c.chip_name='bboost'),0) AS "Num of Bench Boost",
    COALESCE(SUM(c.num_played) FILTER (WHERE c.chip_name='3xc'),0) AS "Num of Triple Captain",
    COALESCE(SUM(c.num_played) FILTER (WHERE c.chip_name='wildcard'),0) AS "Num of Wildcard"
FROM core.gameweeks g
LEFT JOIN analytics.all_players p_most_selected
    ON g.most_selected = p_most_selected.player_id
LEFT JOIN analytics.all_players p_most_transferred_in
    ON g.most_transferred_in = p_most_transferred_in.player_id
LEFT JOIN analytics.all_players p_most_captained
    ON g.most_captained = p_most_captained.player_id
LEFT JOIN analytics.all_players p_most_vice_captained
    ON g.most_vice_captained = p_most_vice_captained.player_id
LEFT JOIN analytics.all_players p_top_player
    ON g.top_player = p_top_player.player_id
LEFT JOIN core.chips c
    ON g.gameweek_id = c.gameweek_id
GROUP BY
    g.gameweek_id,
    g.average_score,
    g.highest_score,
    g.ranked_count,
    g.transfers_made,
    p_most_selected.full_name,
    p_most_transferred_in.full_name,
    p_most_captained.full_name,
    p_most_vice_captained.full_name,
    p_top_player.full_name;
