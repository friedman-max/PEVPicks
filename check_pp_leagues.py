import logging
import json
from curl_cffi import requests

def test_leagues():
    logging.basicConfig(level=logging.INFO)
    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Referer": "https://app.prizepicks.com/",
    }
    
    url = "https://api.prizepicks.com/leagues?state_code=&game_mode=pickem"
    
    with requests.Session(impersonate="chrome124") as session:
        try:
            resp = session.get(url, headers=headers)
            print(f"Status: {resp.status_code}")
            if resp.status_code == 200:
                data = resp.json()
                for league in data.get("data", []):
                    attr = league.get("attributes", {})
                    print(f"League: {attr.get('name')} (ID: {league.get('id')})")
            else:
                print(f"Error Body: {resp.text[:500]}")
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    test_leagues()
