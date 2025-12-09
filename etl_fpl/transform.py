import logging

def transform_fpl_data(raw):
    logging.info("Transforming FPL data...")

    return {
        "players": raw.get("elements", []),
        "teams": raw.get("teams", []),
        "gameweeks": raw.get("events", []),
    }
