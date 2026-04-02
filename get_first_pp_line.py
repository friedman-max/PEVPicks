import sys
import os
import logging
import time

# Add root to path
sys.path.append(os.getcwd())

from scrapers.prizepicks import scrape_prizepicks
from config import ACTIVE_LEAGUES

# Setup logging
logging.basicConfig(level=logging.INFO)

def get_first_line():
    print("Waiting 30 seconds to bypass rate-limit...")
    time.sleep(30)
    print("Fetching PrizePicks lines (MLB)...")
    # Only fetch MLB for speed during test
    leagues = {"MLB": True}
    lines = scrape_prizepicks(active_leagues=leagues)
    
    if lines:
        first = lines[0]
        print("\nSUCCESS! Found PrizePicks data:")
        print(f"  Player:    {first.player_name}")
        print(f"  League:    {first.league}")
        print(f"  Stat:      {first.stat_type}")
        print(f"  Line Score: {first.line_score}")
        print(f"  Side:      {first.side}")
    else:
        print("\nNo lines found.")

if __name__ == "__main__":
    get_first_line()
