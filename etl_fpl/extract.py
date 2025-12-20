import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

import requests
import logging
from config.config import FPL_API_URL

def extract_fpl_data():
    logging.info("Extracting data from FPL API...")
    response = requests.get(FPL_API_URL)
    response.raise_for_status()
    return response.json()
