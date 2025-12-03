-- 03_create_players_table.sql
-- Parses player information from raw_fpl JSON and creates a structured players table

CREATE TABLE players (
    player_id INT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    team_id INT,
    position TEXT,
    total_points INT,
    cost NUMERIC
    -- Add more columns as needed
);
