import requests
import logging
from config import FPL_API_URL

def extract_fpl_data():
    logging.info("Extracting data from FPL API...")
    response = requests.get(FPL_API_URL)
    response.raise_for_status()
    return response.json()
