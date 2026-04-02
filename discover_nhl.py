import requests
import json
import uuid

PP_BASE = "https://partner-api.prizepicks.com/projections"
PP_HEADERS = {
    "Accept":          "application/json",
    "Referer":         "https://app.prizepicks.com/",
    "Origin":          "https://app.prizepicks.com",
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "x-device-id":     str(uuid.uuid4()),
    "x-device-info":   "{\"anonymousId\":\"\",\"os\":\"windows\",\"osVersion\":\"10\",\"platform\":\"web\",\"gameMode\":\"pickem\"}",
}

def discover_leagues():
    print("Fetching PrizePicks leagues...")
    # Fetch a small page for projection to see the meta / included for leagues
    resp = requests.get(PP_BASE, params={"per_page": 1}, headers=PP_HEADERS)
    if resp.status_code != 200:
        print(f"Error: {resp.status_code}")
        return

    data = resp.json()
    included = data.get("included", [])
    
    leagues = []
    for item in included:
        if item.get("type") == "league":
            leagues.append({
                "id": item.get("id"),
                "name": item.get("attributes", {}).get("name"),
                "display_name": item.get("attributes", {}).get("display_name"),
            })
    
    print("\nAvailable Leagues on PrizePicks:")
    for l in leagues:
        print(f"  [{l['id']}] {l['name']} ({l['display_name']})")

if __name__ == "__main__":
    discover_leagues()
