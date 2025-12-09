def normalize_player_rating(match_json):
    """
    Extract player-level ratings from raw match JSON.
    Returns uniform tuples: (match_id, player_id, player_name, team_id, team_name, rating, minutes, position, source, date)
    """
    rows = []
    mj = match_json.get("match") or match_json

    match_id = mj.get("id") or mj.get("matchId")
    match_date = mj.get("startTime") or mj.get("utcDate")

    if "lineups" in mj:
        for team in mj["lineups"]:
            team_id = team.get("teamId")
            team_name = team.get("team")
            for pl in team.get("players", []):
                pid = pl.get("id")
                pname = pl.get("name")
                rating = pl.get("rating")
                minutes = pl.get("minutesPlayed")
                pos = pl.get("position")

                if rating is None:
                    continue

                rows.append((
                    int(match_id),
                    int(pid),
                    pname,
                    int(team_id),
                    team_name,
                    float(rating),
                    int(minutes) if minutes else None,
                    pos,
                    "fotmob",
                    match_date
                ))

    return rows
