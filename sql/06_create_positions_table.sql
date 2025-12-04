-- 06_create_positions_table.sql
-- Parses positions and their rules

CREATE TABLE IF NOT EXISTS staging.positions (
    position_id NUMERIC(1) PRIMARY KEY,
    name VARCHAR(15),
    name_short CHAR(3),
    squad_select NUMERIC(1),
    squad_min_play NUMERIC(1),
    squad_max_play NUMERIC(1)
);
