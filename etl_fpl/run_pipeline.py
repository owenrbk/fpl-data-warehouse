import logging
from config import LOG_FILE
from extract import extract_fpl_data
from transform import transform_fpl_data
from load import load_all

logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

if __name__ == "__main__":
    raw = extract_fpl_data()
    transformed = transform_fpl_data(raw)
    load_all(transformed)
