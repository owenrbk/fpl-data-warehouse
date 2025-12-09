import psycopg2
from psycopg2.extras import execute_values

def pg_conn(config):
    return psycopg2.connect(
        host=config["host"],
        port=config["port"],
        dbname=config["db"],
        user=config["user"],
        password=config["password"]
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
