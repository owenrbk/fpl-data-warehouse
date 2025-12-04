-- 05_create_fixtures_table.sql
-- Parses fixture data from raw_fpl JSON

CREATE TABLE IF NOT EXISTS staging.gameweeks (
    gameweek_id NUMERIC(2) PRIMARY KEY,
    average_score NUMERIC(3),
    highest_score NUMERIC(3),
    ranked_count NUMERIC(10),
    chip_plays TEXT,
    most_selected INT,
    most_transferred_in INT,
    most_captained INT,
    most_vice_captained INT,
    top_player INT,
    transfers_made INT
);
