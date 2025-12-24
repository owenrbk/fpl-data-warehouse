import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

import psycopg2
from psycopg2.extras import execute_values
from config.config import PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PW

def get_connection():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PW
    )

def upsert_fotmob_ratings(rows, conn):
    """
    rows tuple order:
    (match_id, player_id, player_name, team_id, team_name,
     rating, minutes_played, position, opta_id)
    """
    if not rows:
        return

    sql = """
    INSERT INTO core.fotmob_ratings
    (match_id, player_id, player_name, team_id, team_name, rating,
     minutes_played, position, opta_id)
    VALUES %s
    ON CONFLICT (match_id, player_id) DO UPDATE
    SET player_name = EXCLUDED.player_name,
        team_id = EXCLUDED.team_id,
        team_name = EXCLUDED.team_name,
        rating = EXCLUDED.rating,
        minutes_played = EXCLUDED.minutes_played,
        position = EXCLUDED.position,
        opta_id = EXCLUDED.opta_id
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=100)
        conn.commit()

def upsert_fotmob_nations(rows, conn):
    """
    rows tuple order: (player_id, player_name, nation)
    """
    if not rows:
        return

    sql = """
    INSERT INTO core.fotmob_nations (player_id, player_name, nation)
    VALUES %s
    ON CONFLICT (player_id) DO UPDATE
    SET player_name = EXCLUDED.player_name,
        nation = EXCLUDED.nation
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=500)
        conn.commit()
