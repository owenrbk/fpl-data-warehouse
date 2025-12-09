import logging
from extract import extract_fpl_data
from transform import transform_fpl_data
from load import load_fpl_data

def main():
    logging.info("=== Starting FPL ETL ===")

    # EXTRACT
    raw = extract_fpl_data()
    logging.info("Extract complete")

    # TRANSFORM
    clean = transform_fpl_data(raw)
    logging.info("Transform complete")

    # LOAD
    load_fpl_data(clean)
    logging.info("Load complete")

    logging.info("=== FPL ETL Finished ===")

if __name__ == "__main__":
    main()
