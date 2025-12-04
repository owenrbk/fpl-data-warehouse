-- 04_create_teams_table.sql
-- Extracts team information from raw_fpl JSON

CREATE TABLE IF NOT EXISTS staging.teams (
    team_id NUMERIC(3) PRIMARY KEY,
    team_name VARCHAR(50),
    short_name CHAR(3),
    position NUMERIC(2),
    strength NUMERIC(4),
    strength_overall_home NUMERIC(4),
    strength_overall_away NUMERIC(4),
    strength_attack_home NUMERIC(4),
    strength_attack_away NUMERIC(4),
    strength_defense_home NUMERIC(4),
    strength_defense_away NUMERIC(4),
    pulse_id NUMERIC(3),
    code NUMERIC(3)
);
