# config.py
import os
import logging
from dotenv import load_dotenv
import psycopg2

# Load environment variables from .env
load_dotenv()

# ----------------------------
# FotMob / FPL config (from .env)
# ----------------------------
FOTMOB_LEAGUE_ID = os.getenv("FOTMOB_LEAGUE_ID")   # e.g. "47"
FOTMOB_SEASON    = os.getenv("FOTMOB_SEASON")      # e.g. "2025/2026"

FPL_API_BASE_URL = os.getenv("FPL_API_BASE_URL")   # optional; if used
FPL_SEASON       = os.getenv("FPL_SEASON")         # optional; if used

# User agent for scraping (required if your scripts reference it)
USER_AGENT = os.getenv("FOTMOB_USER_AGENT")

# ----------------------------
# Postgres (from .env)
# ----------------------------
PG_HOST = os.getenv("POSTGRES_HOST")
PG_PORT = os.getenv("POSTGRES_PORT")
PG_DB   = os.getenv("POSTGRES_DB")
PG_USER = os.getenv("POSTGRES_USER")
PG_PW   = os.getenv("POSTGRES_PASSWORD")

# ----------------------------
# Request headers
# ----------------------------
# Use this when calling requests.get/post so FotMob sees a proper UA
HEADERS = {"User-Agent": USER_AGENT} if USER_AGENT else {}

# ----------------------------
# Logging
# ----------------------------
# Make sure logs/ directory exists or logging may fail to write.
LOG_FILE_PATH = os.getenv("ETL_LOG_PATH", "logs/etl.log")
os.makedirs(os.path.dirname(LOG_FILE_PATH), exist_ok=True)

logging.basicConfig(
    filename=LOG_FILE_PATH,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ----------------------------
# DB connection helper
# ----------------------------
def get_connection():
    """
    Return a new psycopg2 connection using environment variables.
    Caller is responsible for closing.
    """
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PW
    )
