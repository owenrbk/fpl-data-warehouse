-- 04_create_teams_table.sql
-- Extracts team information from raw_fpl JSON

CREATE TABLE teams (
    team_id INT PRIMARY KEY,
    team_name TEXT,
    short_name TEXT,
    code INT
);
