import os
import json
import psycopg2
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Get Neon credentials from .env
NEON_HOST = os.getenv("NEON_HOST")
NEON_DB = os.getenv("NEON_DB")
NEON_USER = os.getenv("NEON_USER")
NEON_PASSWORD = os.getenv("NEON_PASSWORD")

# Load your local JSON file
with open("raw_fpl.json", "r") as f:
    data = json.load(f)

# Connect to Neon
conn = psycopg2.connect(
    host=NEON_HOST,
    dbname=NEON_DB,
    user=NEON_USER,
    password=NEON_PASSWORD,
    sslmode="require"
)
cur = conn.cursor()

# Insert each player into staging.players
for player in data['elements']:  # 'elements' array contains all players
    cur.execute("""
        INSERT INTO staging.players (
            player_id, first_name, second_name, status, team, team_code, country,
            opta_code, element_type, now_cost, total_points, points_per_game, minutes,
            goals_scored, assists, clean_sheets, goals_conceded, own_goals, penalties_saved,
            penalties_missed, yellow_cards, red_cards, saves, bonus, bps,
            influence, creativity, threat, ict_index, cbi, recoveries, tackles, dc, starts,
            xg, xa, xgi, xgc, form, chance_of_playing_next_round, chance_of_playing_this_round,
            corners_and_indirect_freekicks_order, direct_freekicks_order, penalties_order
        ) VALUES (
            %(id)s, %(first_name)s, %(second_name)s, %(status)s, %(team)s, %(team_code)s, %(region)s,
            %(opta_code)s, %(element_type)s, %(now_cost)s, %(total_points)s, %(points_per_game)s, %(minutes)s,
            %(goals_scored)s, %(assists)s, %(clean_sheets)s, %(goals_conceded)s, %(own_goals)s, %(penalties_saved)s,
            %(penalties_missed)s, %(yellow_cards)s, %(red_cards)s, %(saves)s, %(bonus)s, %(bps)s,
            %(influence)s, %(creativity)s, %(threat)s, %(ict_index)s, %(clearances_blocks_interceptions)s, %(recoveries)s, %(tackles)s, %(defensive_contribution)s, %(starts)s,
            %(expected_goals)s, %(expected_assists)s, %(expected_goal_involvements)s, %(expected_goals_conceded)s, %(form)s, %(chance_of_playing_next_round)s, %(chance_of_playing_this_round)s,
            %(corners_and_indirect_freekicks_order)s, %(direct_freekicks_order)s, %(penalties_order)s
        )
    """, player)

conn.commit()
cur.close()
conn.close()
print("Insert complete!")
