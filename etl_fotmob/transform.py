# etl_fotmob/transform.py

from config.config import logger

POSITION_ID_MAP = {
    # Forwards
    "ST":  {104, 105, 106, 115},
    "LW":  {87, 88, 89, 107, 109},
    "RW":  {81, 82, 83, 101, 103},
    "CAM": {84, 85, 86},

    # Midfield
    "LM":  {78},
    "RM":  {72},
    "CM":  {73, 74, 75, 76, 77},
    "CDM": {63, 64, 65, 66, 67},

    # Defense
    "LWB": {59, 68, 79},
    "RWB": {51, 62, 71},
    "LB":  {38},
    "RB":  {32},
    "CB":  {33, 34, 35, 36, 37},

    # Goalkeeper
    "GK":  {11},
}


def resolve_position(position_id):
    if position_id is None:
        return None

    for position, ids in POSITION_ID_MAP.items():
        if position_id in ids:
            return position

    logger.warning(f"Unknown FotMob positionId encountered: {position_id}")
    return None


def _get_top_stats_block(stats_blocks):
    for block in stats_blocks or []:
        if block.get("key") == "top_stats" or block.get("title") == "Top stats":
            return block
    return None


def _extract_stat_value(top_block, label):
    try:
        node = (top_block or {}).get("stats", {}).get(label)
        if not node:
            return None
        return node.get("stat", {}).get("value")
    except Exception:
        return None


def _extract_player_meta(player_obj):
    if isinstance(player_obj.get("stats"), list) and player_obj["stats"]:
        meta0 = player_obj["stats"][0]
        if isinstance(meta0, dict):
            return {
                "player_id": meta0.get("id") or player_obj.get("id"),
                "player_name": meta0.get("name") or player_obj.get("name"),
                "team_id": meta0.get("teamId") or player_obj.get("teamId"),
                "team_name": meta0.get("teamName") or player_obj.get("teamName"),
                "position_id": meta0.get("positionId") or player_obj.get("positionId"),
                "stats_blocks": player_obj.get("stats"),
                "top_container": meta0,
            }

    return {
        "player_id": player_obj.get("id"),
        "player_name": player_obj.get("name"),
        "team_id": player_obj.get("teamId"),
        "team_name": player_obj.get("teamName"),
        "position_id": player_obj.get("positionId"),
        "stats_blocks": player_obj.get("stats", []),
        "top_container": None,
    }


def normalize_player_rating(match_json, match_id):
    """
    Returns:
    {
      "ratings": [...],
      "nations": [...]
    }
    """

    mj = match_json.get("match") or match_json
    content = mj.get("content") or {}

    if match_id is None:
        logger.warning("Skipping match with no match_id")
        return {"ratings": [], "nations": []}
    match_date = mj.get("startTime") or mj.get("utcDate")

    ratings_rows = []

    players_map = content.get("playerStats") or {}

    logger.info(
        f"match_id={match_id} playerStats keys={len(players_map)}"
    )

    for pid_key, player_obj in players_map.items():
        pid = int(pid_key)

        pname = player_obj.get("name")
        opta_id = player_obj.get("optaId")        # ← FIX
        team_id = player_obj.get("teamId")
        team_name = player_obj.get("teamName")
        position_id = player_obj.get("positionId")

        stats_blocks = player_obj.get("stats", [])

        top_block = _get_top_stats_block(stats_blocks)
        rating = _extract_stat_value(top_block, "FotMob rating")
        minutes = _extract_stat_value(top_block, "Minutes played")

        if rating is None:
            continue

        position = resolve_position(position_id)

        ratings_rows.append((
            int(match_id),
            pid,
            pname,
            int(team_id) if team_id else None,
            team_name,
            float(rating),
            int(minutes) if minutes is not None else None,
            position,
            match_date,
            opta_id
        ))

    # ---- Nations (starters + subs + unavailable) ----
    nation_rows = []

    lineup = content.get("lineup", {})

    for side in ("homeTeam", "awayTeam"):
        team_block = lineup.get(side)
        if not team_block:
            continue

        for section in ("starters", "subs", "unavailable"):
            for player in team_block.get(section, []):
                pid = player.get("id")
                pname = player.get("name")
                country = player.get("countryName")

                if not country:
                    logger.warning(
                        f"Missing countryName | match={match_id} | pid={pid} | section={section}"
                    )

                if pid and pname and country:
                    nation_rows.append((int(pid), pname, country))

    return {
        "ratings": ratings_rows,
        "nations": nation_rows
    }
