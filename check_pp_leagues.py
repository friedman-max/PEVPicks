import logging
import json
import uuid
from curl_cffi import requests

PP_HEADERS = {
    "Accept":          "application/json",
    "Referer":         "https://app.prizepicks.com/",
    "Origin":          "https://app.prizepicks.com",
    "User-Agent":      "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1",
    "x-device-id":     str(uuid.uuid4()),
    "x-device-info":   "{\"anonymousId\":\"\",\"os\":\"ios\",\"osVersion\":\"16.0\",\"platform\":\"web\",\"gameMode\":\"pickem\"}",
}

def test_leagues():
    logging.basicConfig(level=logging.INFO)
    
    url = "https://partner-api.prizepicks.com/leagues?state_code=&game_mode=pickem"
    
    with requests.Session(impersonate="chrome124") as session:
        try:
            resp = session.get(url, headers=PP_HEADERS)
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
