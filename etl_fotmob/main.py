import logging
from extract import extract_fotmob_matches
from transform import transform_fotmob_data
from load import load_fotmob_data

def main():
    logging.info("=== Starting FotMob ETL ===")

    # EXTRACT
    raw = extract_fotmob_matches()
    logging.info("Extract complete")

    # TRANSFORM
    clean = transform_fotmob_data(raw)
    logging.info("Transform complete")

    # LOAD
    load_fotmob_data(clean)
    logging.info("Load complete")

    logging.info("=== FotMob ETL Finished ===")

if __name__ == "__main__":
    main()
