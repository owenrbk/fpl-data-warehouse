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

def require_env(var_name):
    """Crash the script if an env var is missing."""
    value = os.getenv(var_name)
    if value is None or value.strip() == "":
        print(f"ERROR: Missing required environment variable: {var_name}")
        sys.exit(1)
    return value

# Config from env
FOTMOB_LEAGUE_ID = require_env("FOTMOB_LEAGUE_ID")
FOTMOB_SEASON    = require_env("FOTMOB_SEASON")
USER_AGENT       = require_env("FOTMOB_USER_AGENT")

PG_HOST = require_env("POSTGRES_HOST")
PG_PORT = require_env("POSTGRES_PORT")
PG_DB   = require_env("POSTGRES_DB")
PG_USER = require_env("POSTGRES_USER")
PG_PW   = require_env("POSTGRES_PASSWORD")

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
    Universal extractor for FotMob ratings from matchDetails JSON.
    Covers both flat playerStats structures and home/away teamPlayers.
    Returns rows matching fotmob.fotmob_ratings schema.
    """
    rows = []

    # --- Get match metadata ---
    mj = match_json.get("match") or match_json
    match_id = (
        mj.get("id") or
        mj.get("matchId") or
        mj.get("match_id")
    )

    match_date = (
        mj.get("startTime") or
        mj.get("utcDate") or
        mj.get("beginAt")
    )

    # --- Actual playerStats section ---
    playerStats = match_json.get("content", {}).get("playerStats")
    if not playerStats:
        return []

    # ===========================================
    # CASE 1 — Flat dictionary of players
    # ===========================================
    # {"422685": {player_data}, "1234": {...}}
    if all(isinstance(v, dict) and "name" in v for v in playerStats.values()):
        for pid, pdata in playerStats.items():
            row = parse_fotmob_player(pid, pdata, match_id, match_date)
            if row:
                rows.append(row)
        return rows

    # ===========================================
    # CASE 2 — Home/Away -> teamPlayers
    # ===========================================
    if "home" in playerStats and "away" in playerStats:
        for side in ("home", "away"):
            block = playerStats.get(side, {})
            for pdata in block.get("teamPlayers", []):
                pid = pdata.get("id") or pdata.get("playerId")
                row = parse_fotmob_player(pid, pdata, match_id, match_date)
                if row:
                    rows.append(row)
        return rows

    # ===========================================
    # CASE 3 — Deep scan fallback
    # ===========================================
    collected = []

    def scan(obj):
        if isinstance(obj, dict):
            if "name" in obj and "stats" in obj:
                collected.append(obj)
            for v in obj.values():
                scan(v)
        elif isinstance(obj, list):
            for v in obj:
                scan(v)

    scan(playerStats)

    for pdata in collected:
        pid = pdata.get("id") or pdata.get("playerId")
        row = parse_fotmob_player(pid, pdata, match_id, match_date)
        if row:
            rows.append(row)

    return rows


# Helper for single player
def parse_fotmob_player(pid, pdata, match_id, match_date):
    name = pdata.get("name")
    team_id = pdata.get("teamId")
    team_name = pdata.get("teamName")

    # Scan all stats blocks for FotMob rating
    rating = None
    for block in pdata.get("stats", []):
        stats_dict = block.get("stats", {})
        if "FotMob rating" in stats_dict:
            rating = stats_dict["FotMob rating"]["stat"]["value"]

    if rating is None:
        return None

    return (
        int(match_id),
        int(pid),
        name,
        int(team_id) if team_id else None,
        team_name,
        float(rating),
        None,          # minutesPlayed unavailable in your snippet
        None,          # position unavailable in your snippet
        "fotmob",
        match_date
    )

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
