-- 06_create_player_history.sql
-- Tracks player performance per gameweek, pulled from raw_fpl JSON

CREATE TABLE player_history (
    history_id SERIAL PRIMARY KEY,
    player_id INT,
    gameweek INT,
    minutes INT,
    goals_scored INT,
    assists INT,
    total_points INT
    -- Add more performance stats as needed
);
