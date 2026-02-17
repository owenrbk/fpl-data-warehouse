-- create_core_tables.sql
-- Parses data from raw FPL JSON and creates unstructured tables

DROP TABLE IF EXISTS core.players;
CREATE TABLE IF NOT EXISTS core.players
(
    player_id INT NOT NULL,
    first_name character varying(50) COLLATE pg_catalog."default",
    last_name character varying(50) COLLATE pg_catalog."default",
    status character(1) COLLATE pg_catalog."default",
    team_id INT,
    opta_code character(7) COLLATE pg_catalog."default",
    "position" INT,
    now_cost numeric(4,1),
    total_points INT,
    points_per_game numeric(3,2),
    minutes INT,
    goals_scored INT,
    assists INT,
    clean_sheets INT,
    goals_conceded INT,
    own_goals INT,
    penalties_saved INT,
    penalties_missed INT,
    yellow_cards INT,
    red_cards INT,
    saves INT,
    bonus INT,
    bps INT,
    influence numeric(5,1),
    creativity numeric(5,1),
    threat numeric(5,1),
    ict_index numeric(5,1),
    clearances_blocks_interceptions INT,
    recoveries INT,
    tackles INT,
    defensive_contributions INT,
    starts INT,
    expected_goals numeric(5,2),
    expected_assists numeric(5,2),
    expected_goal_involvements numeric(5,2),
    expected_goals_conceded numeric(5,2),
    form numeric(3,1),
    chance_of_playing_next_round INT,
    chance_of_playing_this_round INT,
    corners_and_indirect_freekicks_order INT,
    direct_freekicks_order INT,
    penalties_order INT,
    CONSTRAINT players_pkey PRIMARY KEY (player_id)
)

DROP TABLE IF EXISTS core.teams;
CREATE TABLE IF NOT EXISTS core.teams
(
    team_id numeric(3,0) NOT NULL,
    team_name character varying(50) COLLATE pg_catalog."default",
    team_short character(3) COLLATE pg_catalog."default",
    "position" numeric(2,0),
    strength numeric(4,0),
    strength_overall_home numeric(4,0),
    strength_overall_away numeric(4,0),
    strength_attack_home numeric(4,0),
    strength_attack_away numeric(4,0),
    strength_defence_home numeric(4,0),
    strength_defence_away numeric(4,0),
    CONSTRAINT teams_pkey PRIMARY KEY (team_id)
)

DROP TABLE IF EXISTS core.gameweeks;
CREATE TABLE IF NOT EXISTS core.gameweeks
(
    gameweek_id numeric(2,0) NOT NULL,
    average_score numeric(3,0),
    highest_score numeric(3,0),
    ranked_count numeric(10,0),
    most_selected numeric(4,0),
    most_transferred_in numeric(4,0),
    most_captained numeric(4,0),
    most_vice_captained numeric(4,0),
    top_player numeric(4,0),
    transfers_made numeric(10,0),
    CONSTRAINT gameweeks_pkey PRIMARY KEY (gameweek_id)
)

DROP TABLE IF EXISTS core.chips;
CREATE TABLE IF NOT EXISTS core.chips
(
    gameweek_id numeric(2,0) NOT NULL,
    chip_name character varying(20) COLLATE pg_catalog."default" NOT NULL,
    num_played numeric(10,0),
    CONSTRAINT chips_pkey PRIMARY KEY (gameweek_id, chip_name),
    CONSTRAINT chips_gameweek_id_fkey FOREIGN KEY (gameweek_id)
        REFERENCES core.gameweeks (gameweek_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
)

DROP TABLE IF EXISTS core.fotmob_ratings;
CREATE TABLE IF NOT EXISTS core.fotmob_ratings
(
    match_id integer NOT NULL,
    player_id integer NOT NULL,
    player_name character varying(50) COLLATE pg_catalog."default",
    team_id integer,
    team_name character varying(50) COLLATE pg_catalog."default",
    rating numeric(3,1),
    minutes_played integer,
    "position" character varying(6) COLLATE pg_catalog."default",
    opta_id integer,
    CONSTRAINT fotmob_ratings_pkey PRIMARY KEY (match_id, player_id)
);

DROP TABLE IF EXISTS core.fotmob_nations;
CREATE TABLE IF NOT EXISTS core.fotmob_nations
(
    player_id integer NOT NULL,
    player_name character varying(50) COLLATE pg_catalog."default",
    nation character varying(50) COLLATE pg_catalog."default",
    CONSTRAINT fotmob_nations_pkey PRIMARY KEY (player_id)
);
