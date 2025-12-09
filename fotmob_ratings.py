#!/usr/bin/env python3
"""
fotmob_ratings_etl.py
Proof-of-concept: pull season matches from FotMob and insert player ratings into Postgres.
"""

import os
import time
import logging
import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

# Config from env
FOTMOB_LEAGUE_ID = os.getenv("FOTMOB_LEAGUE_ID", "47")            # 47 = Premier League
FOTMOB_SEASON = os.getenv("FOTMOB_SEASON", "2025/2026")           # e.g. "2025/2026"
USER_AGENT = os.getenv("FOTMOB_USER_AGENT","Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36")

PG_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB   = os.getenv("POSTGRES_DB", "owen_db")
PG_USER = os.getenv("POSTGRES_USER", "owen")
PG_PW   = os.getenv("POSTGRES_PASSWORD", "")

# Tuning
RATE_LIMIT_SECONDS = float(os.getenv("FOTMOB_RATE_LIMIT_SECONDS", "0.25"))  # 4 requests/sec by default
BATCH_COMMIT_SIZE = int(os.getenv("FOTMOB_BATCH_COMMIT_SIZE", "50"))

logging.basicConfig(
    filename='/home/owen/fpl_updater/logs/fotmob_etl.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})

def pg_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PW
    )

def fetch_season_matches(league_id, season):
    """
    Returns the JSON for season matches. Endpoint used by many wrappers.
    """
    url = f"https://www.fotmob.com/api/matches?leagueId={league_id}&season={season}"
    logging.info("Fetching season matches: %s", url)
    r = session.get(url, timeout=20)
    r.raise_for_status()
    return r.json()

def fetch_match_details(match_id):
    url = f"https://www.fotmob.com/api/matchDetails?matchId={match_id}"
    r = session.get(url, timeout=20)
    r.raise_for_status()
    return r.json()

def normalize_player_rating(match_json):
    """
    Extract player-level ratings from a fetched match JSON.
    Returns a list of tuples matching fotmob.fotmob_ratings fields.
    """
    rows = []
    # fotmob match JSON shape can change. We try common structures observed:
    # - match_json.get('match') or match_json directly contains fields
    mj = match_json.get("match") or match_json
    match_id = mj.get("id") or mj.get("matchId") or mj.get("match_id")
    match_date = mj.get("startTime") or mj.get("utcDate") or mj.get("beginAt")
    # Try to find squads/players and ratings
    # FotMob often includes arrays like 'home'/'away' lineups and 'players' sections.
    # Two common places: mj['lineups'] or mj['players']
    # We'll check for 'lineups' -> each team -> players -> rating
    if "lineups" in mj and isinstance(mj["lineups"], list):
        for team in mj["lineups"]:
            team_id = team.get("teamId") or team.get("id")
            team_name = team.get("team") or team.get("shortName") or team.get("name")
            for pl in team.get("players", []):
                pid = pl.get("id") or pl.get("playerId")
                pname = pl.get("name") or pl.get("playerName")
                rating = pl.get("rating") or pl.get("fotMobRating") or pl.get("ratingValue")
                minutes = pl.get("minutesPlayed") or pl.get("min")
                pos = pl.get("position") or pl.get("role")
                if rating is None:
                    continue
                try:
                    rating_num = float(rating)
                except Exception:
                    # sometimes rating is a string like "7.4"
                    try:
                        rating_num = float(str(rating))
                    except:
                        continue
                rows.append((
                    int(match_id) if match_id else None,
                    int(pid) if pid else None,
                    pname,
                    int(team_id) if team_id else None,
                    team_name,
                    round(rating_num,1),
                    int(minutes) if minutes else None,
                    pos,
                    'fotmob',
                    match_date
                ))
    # fallback: check mj.get('players') top-level
    elif "players" in mj and isinstance(mj["players"], list):
        for pl in mj["players"]:
            pid = pl.get("id")
            pname = pl.get("name")
            team_id = pl.get("teamId") or pl.get("team", {}).get("id")
            team_name = None
            rating = pl.get("rating")
            minutes = pl.get("minutesPlayed")
            pos = pl.get("position")
            if rating is None:
                continue
            try:
                rating_num = float(rating)
            except:
                continue
            rows.append((
                int(match_id) if match_id else None,
                int(pid) if pid else None,
                pname,
                int(team_id) if team_id else None,
                team_name,
                round(rating_num,1),
                int(minutes) if minutes else None,
                pos,
                'fotmob',
                match_date
            ))
    else:
        logging.debug("No lineups/players array found in match %s. Keys: %s", match_id, list(mj.keys())[:30])
    return rows

def upsert_ratings(rows, conn):
    if not rows:
        return
    # columns: match_id, player_id, player_name, team_id, team_name, rating, minutes_played, position, rating_source, match_date
    sql = """
    INSERT INTO fotmob.fotmob_ratings
    (match_id, player_id, player_name, team_id, team_name, rating, minutes_played, position, rating_source, match_date)
    VALUES %s
    ON CONFLICT (match_id, player_id) DO UPDATE
    SET player_name = EXCLUDED.player_name,
        team_id = EXCLUDED.team_id,
        team_name = EXCLUDED.team_name,
        rating = EXCLUDED.rating,
        minutes_played = EXCLUDED.minutes_played,
        position = EXCLUDED.position,
        rating_source = EXCLUDED.rating_source,
        match_date = EXCLUDED.match_date,
        created_at = now()
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=100)
    conn.commit()

def main():
    logging.info("Starting FotMob ETL for league %s season %s", FOTMOB_LEAGUE_ID, FOTMOB_SEASON)
    # 1) fetch season matches list
    try:
        season_json = fetch_season_matches(FOTMOB_LEAGUE_ID, FOTMOB_SEASON)
    except Exception as exc:
        logging.exception("Failed to fetch season matches: %s", exc)
        return

    # The structure: season_json.get('matches') is typical
    matches = season_json.get("matches") or season_json.get("data") or []
    if not matches:
        logging.warning("No matches found in season JSON. Keys: %s", list(season_json.keys())[:50])
        return

    match_ids = []
    for m in matches:
        # try to find match id in multiple common keys
        mid = m.get("id") or m.get("matchId") or m.get("fixtureId")
        if mid:
            match_ids.append(int(mid))

    logging.info("Found %d match IDs", len(match_ids))

    conn = pg_conn()
    processed = 0
    batch_rows = []

    for mid in match_ids:
        try:
            mj = fetch_match_details(mid)
        except requests.HTTPError as e:
            logging.warning("HTTP error fetching match %s: %s", mid, str(e))
            time.sleep(3)
            continue
        except Exception as e:
            logging.exception("Error fetching match %s: %s", mid, e)
            time.sleep(3)
            continue

        rows = normalize_player_rating(mj)
        if rows:
            batch_rows.extend(rows)

        # Commit in batches
        if len(batch_rows) >= BATCH_COMMIT_SIZE:
            try:
                upsert_ratings(batch_rows, conn)
                logging.info("Committed %d rows to DB (up to match %s)", len(batch_rows), mid)
                batch_rows = []
            except Exception:
                logging.exception("DB commit failed. Retrying single-row inserts.")
                # fallback: insert one-by-one
                for r in rows:
                    try:
                        upsert_ratings([r], conn)
                    except Exception:
                        logging.exception("Failed to insert single row: %s", r)

        processed += 1
        # polite rate limit
        time.sleep(RATE_LIMIT_SECONDS)

    # final flush
    if batch_rows:
        try:
            upsert_ratings(batch_rows, conn)
            logging.info("Final commit of %d rows", len(batch_rows))
        except Exception:
            logging.exception("Final commit failed.")
    conn.close()
    logging.info("Done ETL. Processed %d matches", processed)

if __name__ == "__main__":
    main()
