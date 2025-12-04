-- 05_create_gameweeks_table.sql
-- Parses gameweek data

CREATE TABLE IF NOT EXISTS staging.gameweeks (
    gameweek_id NUMERIC(2) PRIMARY KEY,
    average_score NUMERIC(3),
    highest_score NUMERIC(3),
    ranked_count NUMERIC(10),
    chip_plays TEXT,
    most_selected NUMERIC(4),
    most_transferred_in NUMERIC(4),
    most_captained NUMERIC(4),
    most_vice_captained NUMERIC(4),
    top_player NUMERIC(4),
    transfers_made NUMERIC(4)
);
