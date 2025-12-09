# etl_fotmob/run_pipeline.py

import time
import traceback
from config import logger, FOTMOB_LEAGUE_ID, FOTMOB_SEASON
from etl_fotmob.extract import fetch_season_matches, fetch_match_details
from etl_fotmob.transform import normalize_player_rating
from etl_fotmob.load import upsert_ratings, get_connection

BATCH_SIZE = 50
RATE_LIMIT_SECONDS = 0.25  # 4 req/sec

def run_pipeline():
    logger.info(f"Starting FotMob ETL for league={FOTMOB_LEAGUE_ID}, season={FOTMOB_SEASON}")

    try:
        season_json = fetch_season_matches(FOTMOB_LEAGUE_ID, FOTMOB_SEASON)
    except Exception as e:
        logger.error("Failed to fetch season match list")
        logger.error(traceback.format_exc())
        return

    matches = season_json.get("matches") or season_json.get("data") or []
    if not matches:
        logger.warning("No matches found in season JSON response")
        return

    match_ids = []
    for match in matches:
        mid = match.get("id") or match.get("matchId") or match.get("fixtureId")
        if mid:
            match_ids.append(int(mid))

    logger.info(f"Found {len(match_ids)} matches")

    conn = get_connection()
    batch = []

    for idx, match_id in enumerate(match_ids, start=1):
        try:
            match_json = fetch_match_details(match_id)
        except Exception:
            logger.error(f"Failed fetch for match {match_id}")
            logger.error(traceback.format_exc())
            time.sleep(3)
            continue

        rows = normalize_player_rating(match_json)
        if rows:
            batch.extend(rows)

        # ---- Batch insert ----
        if len(batch) >= BATCH_SIZE:
            try:
                upsert_ratings(batch, conn)
                logger.info(f"Inserted {len(batch)} rows (through match {match_id})")
                batch = []
            except Exception:
                logger.error("Batch insert failed")
                logger.error(traceback.format_exc())

        # Rate limit so FotMob doesn't block you
        time.sleep(RATE_LIMIT_SECONDS)

    # Final flush of leftovers
    if batch:
        try:
            upsert_ratings(batch, conn)
            logger.info(f"Final commit of {len(batch)} rows")
        except Exception:
            logger.error("Final commit failed")
            logger.error(traceback.format_exc())

    conn.close()
    logger.info("FotMob ETL complete.")

if __name__ == "__main__":
    run_pipeline()
