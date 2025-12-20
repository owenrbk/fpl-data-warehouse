import sys, os
print("PYTHON:", sys.executable)
print("CWD:", os.getcwd())
print("SYS.PATH[0:3]:", sys.path[:3])


from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

import logging
from config.config import LOG_FILE
from etl_fpl.extract import extract_fpl_data
from etl_fpl.transform import transform_fpl_data
from etl_fpl.load import load_all

logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

if __name__ == "__main__":
    raw = extract_fpl_data()
    transformed = transform_fpl_data(raw)
    load_all(transformed)
