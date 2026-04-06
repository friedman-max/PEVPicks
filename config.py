"""
App-wide configuration. Edit defaults here or override via .env file.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# Scraping
HEADLESS = os.getenv("HEADLESS", "false").lower() == "true"  # headed by default for Cloudflare bypass
REFRESH_INTERVAL_MINUTES = int(os.getenv("REFRESH_INTERVAL_MINUTES", "15"))

# EV filtering
MIN_INDIVIDUAL_EV_PCT = float(os.getenv("MIN_INDIVIDUAL_EV_PCT", "0.01"))  # 1%

# Leagues to scrape (set specific sports to false to exclude)
SCRAPE_ALL_LEAGUES = os.getenv("SCRAPE_ALL_LEAGUES", "false").lower() == "true"
ACTIVE_LEAGUES = {
    "NBA":   os.getenv("LEAGUE_NBA", "true").lower()   == "true",
    "MLB":   os.getenv("LEAGUE_MLB", "true").lower()   == "true",
    "NHL":   os.getenv("LEAGUE_NHL", "true").lower()   == "true",
    "NCAAB": os.getenv("LEAGUE_NCAAB", "true").lower() == "true",
}

# FanDuel URLs per league
FANDUEL_URLS = {
    "NBA":   "https://sportsbook.fanduel.com/navigation/nba",
    "MLB":   "https://sportsbook.fanduel.com/navigation/mlb",
    "NHL":   "https://sportsbook.fanduel.com/navigation/nhl",
    "NCAAB": "https://sportsbook.fanduel.com/navigation/ncaab",
}

# PrizePicks API league IDs
PRIZEPICKS_LEAGUE_IDS = {
    "NBA":   7,
    "MLB":   2,
    "NHL":   8,
    "NCAAB": 20,   # PrizePicks calls this "CBB" (ID=20); 189 is a defunct alias that returns 0
}

# Fuzzy match threshold (0-100)
FUZZY_THRESHOLD = 91

# Single-sided vig assumption
SINGLE_SIDE_VIG = 0.070

# Server
HOST = os.getenv("HOST", "127.0.0.1")
PORT = int(os.getenv("PORT", "8000"))
