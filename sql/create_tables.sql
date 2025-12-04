-- 03_create_players_table.sql
-- Parses data from raw FPL JSON and creates structured tables

CREATE TABLE IF NOT EXISTS staging.players (
    player_id INT PRIMARY KEY,
    first_name VARCHAR(30),
    second_name VARCHAR(30),
    status CHAR(1),
    team NUMERIC (2),
    team_code NUMERIC(2),
    region VARCHAR(3),
    opta_code CHAR(7),
    element_type NUMERIC(1),
    now_cost NUMERIC(3),
    total_points NUMERIC(3),
    points_per_game NUMERIC(3,2),
    minutes NUMERIC(4),
    goals_scored NUMERIC(3),
    assists NUMERIC(3),
    clean_sheets NUMERIC(2),
    goals_conceded NUMERIC(4),
    own_goals NUMERIC(3),
    penalties_saved NUMERIC(3),
    penalties_missed NUMERIC(3),
    yellow_cards NUMERIC(2),
    red_cards NUMERIC(2),
    saves NUMERIC(4),
    bonus NUMERIC(4),
    bps NUMERIC(5),
    influence NUMERIC(4,1),
    creativity NUMERIC(4,1),
    threat NUMERIC(4,1),
    ict_index NUMERIC(4,1),
    cbi NUMERIC(4),
    recoveries NUMERIC(4),
    tackles NUMERIC(4),
    dc NUMERIC(4),
    starts NUMERIC(2),
    xg NUMERIC(5,2),
    xa NUMERIC(5,2),
    xgi NUMERIC(5,2),
    xgc NUMERIC(5,2),
    form NUMERIC(3,1),
    chance_of_playing_next_round NUMERIC(3),
    chance_of_playing_this_round NUMERIC(3),
    corners_and_indirect_freekicks_order NUMERIC(2),
    direct_freekicks_order NUMERIC(2),
    penalties_order NUMERIC(2)
);

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
    transfers_made NUMERIC(10)
);

CREATE TABLE IF NOT EXISTS staging.chips (
    gameweek_id NUMERIC(2) REFERENCES staging.gameweeks(gameweek_id),
    chip_name VARCHAR(20),
    num_played NUMERIC(10)
);

CREATE TABLE IF NOT EXISTS staging.positions (
    position_id NUMERIC(1) PRIMARY KEY,
    name VARCHAR(15),
    name_short CHAR(3),
    squad_select NUMERIC(1),
    squad_min_play NUMERIC(1),
    squad_max_play NUMERIC(1)
);
