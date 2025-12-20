from pathlib import Path
import sys
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

import psycopg2
import logging
from config.config import POSTGRES_CONFIG

def connect_db():
    return psycopg2.connect(**POSTGRES_CONFIG)


def load_players(cur, players):
    logging.info("Loading players...")

    cur.execute("TRUNCATE core.players RESTART IDENTITY CASCADE;")

    insert_sql = """
        INSERT INTO core.players (
            player_id, first_name, last_name, status, team_id,
            opta_code, position, now_cost, total_points, points_per_game, minutes,
            goals_scored, assists, clean_sheets, goals_conceded, own_goals, penalties_saved,
            penalties_missed, yellow_cards, red_cards, saves, bonus, bps,
            influence, creativity, threat, ict_index, clearances_blocks_interceptions,
            recoveries, tackles, defensive_contributions, starts,
            expected_goals, expected_assists, expected_goal_involvements, expected_goals_conceded,
            form, chance_of_playing_next_round, chance_of_playing_this_round,
            corners_and_indirect_freekicks_order, direct_freekicks_order, penalties_order
        )
        VALUES (
            %(id)s, %(first_name)s, %(second_name)s, %(status)s, %(team)s,
            %(opta_code)s, %(element_type)s, %(now_cost)s, %(total_points)s, %(points_per_game)s,
            %(minutes)s, %(goals_scored)s, %(assists)s, %(clean_sheets)s, %(goals_conceded)s,
            %(own_goals)s, %(penalties_saved)s, %(penalties_missed)s, %(yellow_cards)s,
            %(red_cards)s, %(saves)s, %(bonus)s, %(bps)s, %(influence)s, %(creativity)s,
            %(threat)s, %(ict_index)s, %(clearances_blocks_interceptions)s, %(recoveries)s,
            %(tackles)s, %(defensive_contribution)s, %(starts)s, %(expected_goals)s,
            %(expected_assists)s, %(expected_goal_involvements)s, %(expected_goals_conceded)s,
            %(form)s, %(chance_of_playing_next_round)s, %(chance_of_playing_this_round)s,
            %(corners_and_indirect_freekicks_order)s, %(direct_freekicks_order)s, %(penalties_order)s
        )
        ON CONFLICT (player_id) DO UPDATE SET
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            status = EXCLUDED.status,
            team_id = EXCLUDED.team_id,
            opta_code = EXCLUDED.opta_code,
            position = EXCLUDED.position,
            now_cost = EXCLUDED.now_cost,
            total_points = EXCLUDED.total_points,
            points_per_game = EXCLUDED.points_per_game,
            minutes = EXCLUDED.minutes,
            goals_scored = EXCLUDED.goals_scored,
            assists = EXCLUDED.assists,
            clean_sheets = EXCLUDED.clean_sheets,
            goals_conceded = EXCLUDED.goals_conceded,
            own_goals = EXCLUDED.own_goals,
            penalties_saved = EXCLUDED.penalties_saved,
            penalties_missed = EXCLUDED.penalties_missed,
            yellow_cards = EXCLUDED.yellow_cards,
            red_cards = EXCLUDED.red_cards,
            saves = EXCLUDED.saves,
            bonus = EXCLUDED.bonus,
            bps = EXCLUDED.bps,
            influence = EXCLUDED.influence,
            creativity = EXCLUDED.creativity,
            threat = EXCLUDED.threat,
            ict_index = EXCLUDED.ict_index,
            clearances_blocks_interceptions = EXCLUDED.clearances_blocks_interceptions,
            recoveries = EXCLUDED.recoveries,
            tackles = EXCLUDED.tackles,
            defensive_contributions = EXCLUDED.defensive_contributions,
            starts = EXCLUDED.starts,
            expected_goals = EXCLUDED.expected_goals,
            expected_assists = EXCLUDED.expected_assists,
            expected_goal_involvements = EXCLUDED.expected_goal_involvements,
            expected_goals_conceded = EXCLUDED.expected_goals_conceded,
            form = EXCLUDED.form,
            chance_of_playing_next_round = EXCLUDED.chance_of_playing_next_round,
            chance_of_playing_this_round = EXCLUDED.chance_of_playing_this_round,
            corners_and_indirect_freekicks_order = EXCLUDED.corners_and_indirect_freekicks_order,
            direct_freekicks_order = EXCLUDED.direct_freekicks_order,
            penalties_order = EXCLUDED.penalties_order;
    """

    for p in players:
        cur.execute(insert_sql, p)


def load_teams(cur, teams):
    logging.info("Loading teams...")
    cur.execute("TRUNCATE core.teams RESTART IDENTITY CASCADE;")

    for t in teams:
        cur.execute("""
            INSERT INTO core.teams (team_id, team_name, team_short, position, strength, 
                strength_overall_home, strength_overall_away, strength_attack_home, strength_attack_away,
                strength_defence_home, strength_defence_away)
            VALUES (%(id)s, %(name)s, %(short_name)s, %(position)s, %(strength)s,
                %(strength_overall_home)s, %(strength_overall_away)s,
                %(strength_attack_home)s, %(strength_attack_away)s,
                %(strength_defence_home)s, %(strength_defence_away)s)
            ON CONFLICT (team_id) DO UPDATE SET
                team_name = EXCLUDED.team_name,
                team_short = EXCLUDED.team_short,
                position = EXCLUDED.position,
                strength = EXCLUDED.strength,
                strength_overall_home = EXCLUDED.strength_overall_home,
                strength_overall_away = EXCLUDED.strength_overall_away,
                strength_attack_home = EXCLUDED.strength_attack_home,
                strength_attack_away = EXCLUDED.strength_attack_away,
                strength_defence_home = EXCLUDED.strength_defence_home,
                strength_defence_away = EXCLUDED.strength_defence_away;
        """, t)


def load_gameweeks(cur, gameweeks):
    logging.info("Loading gameweeks...")
    cur.execute("TRUNCATE core.gameweeks RESTART IDENTITY CASCADE;")

    for gw in gameweeks:
        cur.execute("""
            INSERT INTO core.gameweeks (
                gameweek_id, average_score, highest_score, ranked_count, most_selected,
                most_transferred_in, most_captained, most_vice_captained, top_player,
                transfers_made
            )
            VALUES (
                %(id)s, %(average_entry_score)s, %(highest_score)s, %(ranked_count)s,
                %(most_selected)s, %(most_transferred_in)s, %(most_captained)s,
                %(most_vice_captained)s, %(top_element)s, %(transfers_made)s
            )
            ON CONFLICT (gameweek_id) DO UPDATE SET
                average_score = EXCLUDED.average_score,
                highest_score = EXCLUDED.highest_score,
                ranked_count = EXCLUDED.ranked_count,
                most_selected = EXCLUDED.most_selected,
                most_transferred_in = EXCLUDED.most_transferred_in,
                most_captained = EXCLUDED.most_captained,
                most_vice_captained = EXCLUDED.most_vice_captained,
                top_player = EXCLUDED.top_player,
                transfers_made = EXCLUDED.transfers_made;
        """, gw)


def load_all(data):
    try:
        conn = connect_db()
        cur = conn.cursor()

        load_players(cur, data["players"])
        load_teams(cur, data["teams"])
        load_gameweeks(cur, data["gameweeks"])

        conn.commit()
        logging.info("Load complete.")

    except Exception:
        logging.exception("Load failed!")
        raise
    finally:
        if "cur" in locals():
            cur.close()
        if "conn" in locals():
            conn.close()
