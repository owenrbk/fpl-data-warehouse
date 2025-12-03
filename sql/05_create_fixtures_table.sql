-- 05_create_fixtures_table.sql
-- Parses fixture data from raw_fpl JSON

CREATE TABLE fixtures (
    fixture_id SERIAL PRIMARY KEY,
    event INT,
    home_team INT,
    away_team INT,
    kickoff_time TIMESTAMP,
    finished BOOLEAN,
    home_score INT,
    away_score INT
);
