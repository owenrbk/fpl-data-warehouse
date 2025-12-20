import os
import logging
import psycopg2
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# ----------------------------
# PostgreSQL configuration
# ----------------------------
PG_HOST = os.getenv("POSTGRES_HOST")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB   = os.getenv("POSTGRES_DB")
PG_USER = os.getenv("POSTGRES_USER")
PG_PW   = os.getenv("POSTGRES_PASSWORD")

# Dict-style config for FPL load.py
POSTGRES_CONFIG = {
    "host": PG_HOST,
    "port": PG_PORT,
    "dbname": PG_DB,
    "user": PG_USER,
    "password": PG_PW,
}

def get_connection():
    """Used by FotMob ETL (and anything else) if you want a simple conn helper."""
    return psycopg2.connect(**POSTGRES_CONFIG)

# ----------------------------
# FPL configuration
# ----------------------------
FPL_API_URL = "https://fantasy.premierleague.com/api/bootstrap-static/"

# ----------------------------
# FotMob configuration
# ----------------------------
FOTMOB_START_MATCH_ID = int(os.getenv("FOTMOB_START_MATCH_ID"))
FOTMOB_END_MATCH_ID = int(os.getenv("FOTMOB_END_MATCH_ID"))
FOTMOB_RATE_LIMIT_SECONDS = float(os.getenv("FOTMOB_RATE_LIMIT_SECONDS", "0.25"))
FOTMOB_BATCH_SIZE = int(os.getenv("FOTMOB_BATCH_COMMIT_SIZE", "50"))
USER_AGENT = os.getenv("FOTMOB_USER_AGENT")
HEADERS = {"User-Agent": USER_AGENT} if USER_AGENT else {}

# ----------------------------
# Logging configuration
# ----------------------------
LOG_FILE = os.getenv("ETL_LOG_PATH", "logs/etl.log")

# Ensure logs directory exists
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)
