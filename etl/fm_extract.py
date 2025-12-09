import requests

def fetch_season_matches(league_id, season):
    url = f"https://www.fotmob.com/api/matches?leagueId={league_id}&season={season}"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    return r.json()

def fetch_match_details(match_id):
    url = f"https://www.fotmob.com/api/matchDetails?matchId={match_id}"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    return r.json()
