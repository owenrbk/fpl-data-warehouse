# etl_fotmob/run_pipeline.py

import sys, os
print("PYTHON:", sys.executable)
print("CWD:", os.getcwd())
print("SYS.PATH[0:3]:", sys.path[:3])

from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

import time
from config.config import logger, FOTMOB_START_MATCH_ID, FOTMOB_END_MATCH_ID, FOTMOB_RATE_LIMIT_SECONDS, FOTMOB_BATCH_SIZE
from etl_fotmob.extract import fetch_match_details
from etl_fotmob.transform import normalize_player_rating
from etl_fotmob.load import upsert_fotmob_ratings, upsert_fotmob_nations, get_connection  # <-- see note below

def run_pipeline():
    logger.info("Starting FotMob ETL for Premier League 2025/26")
    logger.info(f"Match ID range: {FOTMOB_START_MATCH_ID} -> {FOTMOB_END_MATCH_ID} (inclusive)")
    logger.info(f"RATE_LIMIT_SECONDS={FOTMOB_RATE_LIMIT_SECONDS}, BATCH_SIZE={FOTMOB_BATCH_SIZE}")

    conn = get_connection()
    ratings_batch = []
    nations_batch = []

    for match_id in range(FOTMOB_START_MATCH_ID, FOTMOB_END_MATCH_ID + 1):
        try:
            match_json = fetch_match_details(match_id)
        except Exception:
            logger.warning(f"Failed to fetch match {match_id}")
            time.sleep(0.5)
            continue

        if not match_json:
            continue

        if match_id % 10 == 0:
            logger.info(f"Processing match_id={match_id}")

        mj = match_json.get("match") or match_json
        content = mj.get("content")
        if not content:
            logger.warning(f"Skipping match {match_id}: no content")
            continue

        transformed = normalize_player_rating(match_json, match_id)

        ratings = transformed.get("ratings", [])
        nations = transformed.get("nations", [])
        logger.info(f"match_id={match_id} extracted ratings={len(ratings)} nations={len(nations)}")

        if ratings:
            ratings_batch.extend(ratings)
        if nations:
            nations_batch.extend(nations)

        # Batch insert
        if len(ratings_batch) >= FOTMOB_BATCH_SIZE:
            try:
                upsert_fotmob_ratings(ratings_batch, conn)
                logger.info(f"Inserted {len(ratings_batch)} ratings rows (through match {match_id})")
                ratings_batch = []
            except Exception:
                logger.exception("Ratings batch insert failed")
                conn.rollback()
                ratings_batch = []

        if len(nations_batch) >= FOTMOB_BATCH_SIZE:
            # Deduplicate by player_id
            deduped = {}
            for pid, pname, nation in nations_batch:
                deduped[pid] = (pid, pname, nation)

            try:
                upsert_fotmob_nations(list(deduped.values()), conn)
                logger.info(f"Inserted {len(deduped)} nation rows")
                nations_batch = []
            except Exception:
                logger.exception("Nations batch insert failed")
                conn.rollback()
                nations_batch = []

        time.sleep(FOTMOB_RATE_LIMIT_SECONDS)

    # Final flush
    if ratings_batch:
        upsert_fotmob_ratings(ratings_batch, conn)
        logger.info(f"Final insert of {len(ratings_batch)} rating rows")

    if nations_batch:
        deduped = {pid: (pid, pname, nation) for pid, pname, nation in nations_batch}
        upsert_fotmob_nations(list(deduped.values()), conn)

    conn.close()
    logger.info("FotMob ETL complete.")

if __name__ == "__main__":
    run_pipeline()
