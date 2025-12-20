import requests

def fetch_match_details(match_id):
    url = f"https://www.fotmob.com/api/matchDetails?matchId={match_id}"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    return r.json()