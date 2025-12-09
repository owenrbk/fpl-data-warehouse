import psycopg2
from psycopg2.extras import execute_values
from config import PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PW

def get_connection():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PW
    )

def upsert_ratings(rows, conn):
    if not rows:
        return

    sql = """
    INSERT INTO fotmob.fotmob_ratings
    (match_id, player_id, player_name, team_id, team_name, rating,
     minutes_played, position, rating_source, match_date)
    VALUES %s
    ON CONFLICT (match_id, player_id) DO UPDATE
    SET player_name = EXCLUDED.player_name,
        team_id = EXCLUDED.team_id,
        team_name = EXCLUDED.team_name,
        rating = EXCLUDED.rating,
        minutes_played = EXCLUDED.minutes_played,
        position = EXCLUDED.position,
        rating_source = EXCLUDED.rating_source,
        match_date = EXCLUDED.match_date
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=100)
        conn.commit()