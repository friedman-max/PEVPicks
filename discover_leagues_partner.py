import logging
import json
import uuid
import time
from curl_cffi import requests

PP_BASE = "https://partner-api.prizepicks.com/projections"
PP_HEADERS = {
    "Accept":          "application/json",
    "Referer":         "https://app.prizepicks.com/",
    "Origin":          "https://app.prizepicks.com",
    "User-Agent":      "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1",
    "x-device-id":     str(uuid.uuid4()),
    "x-device-info":   "{\"anonymousId\":\"\",\"os\":\"ios\",\"osVersion\":\"16.0\",\"platform\":\"web\",\"gameMode\":\"pickem\"}",
}

def discover():
    logging.basicConfig(level=logging.INFO)
    
    # Use safari impersonation to match the UA
    with requests.Session(impersonate="safari") as session:
        try:
            print("Fetching PrizePicks projections to discover leagues...")
            # Fetch with no league_id to see what's available
            resp = session.get(PP_BASE, params={"per_page": 100}, headers=PP_HEADERS, timeout=20)
            print(f"Status: {resp.status_code}")
            if resp.status_code == 200:
                data = resp.json()
                included = data.get("included", [])
                
                leagues = {}
                for item in included:
                    if item.get("type") == "league":
                        lid = item.get("id")
                        name = item.get("attributes", {}).get("name")
                        leagues[name] = lid
                
                print("\nDiscovered Leagues:")
                for name, lid in leagues.items():
                    print(f"  {name}: {lid}")
                
                # Also check projections themselves to see what leagues they belong to
                proj_leagues = set()
                for proj in data.get("data", []):
                    if proj.get("type") == "projection":
                        rel = proj.get("relationships", {})
                        league_rel = rel.get("league", {}).get("data", {})
                        if league_rel.get("id"):
                            proj_leagues.add(league_rel.get("id"))
                
                print(f"\nLeagues present in first 100 projections (IDs): {proj_leagues}")
                
            else:
                print(f"Error Body: {resp.text[:500]}")
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    discover()
