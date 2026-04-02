"""
DraftKings API scraper (Controlled Data strategy).
Fetches lines directly from sportsbook-nash.draftkings.com.
"""
import asyncio
import logging
import json
from typing import Optional, List, Dict
from curl_cffi import requests

from config import ACTIVE_LEAGUES
from engine.constants import PROP_TYPE_MAP
from engine.matcher import FanDuelProp as DraftKingsProp # Reusing FanDuelProp structure for now

logger = logging.getLogger(__name__)

DK_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Origin": "https://sportsbook.draftkings.com",
    "Referer": "https://sportsbook.draftkings.com/",
}

# Subcategory IDs for player props
LEAGUE_CONFIG = {
    "NBA": {
        "id": "42648",
        "subcategories": {
            "Points": "16477",
            "Rebounds": "16479",
            "Assists": "16478",
            "Three Pointers": "16480",
            "Pts+Rebs+Asts": "16481",
        }
    },
    "MLB": {
        "id": "84240",
        "subcategories": {
            "Home Runs": "17319", 
            "Hits": "17320",
            "Total Bases": "17321",
            "RBIs": "17322",
            "Runs": "17316",
            "Pitcher Strikeouts": "17323",
            "Pitching Outs": "20500", # Need to verify this or find correct one
        }
    },
    "NHL": {
        "id": "42133",
        "subcategories": {
            "Goals": "4238",
            "Points": "4240",
            "Shots": "4242",
        }
    }
}

async def _fetch_subcategory(session: requests.AsyncSession, league: str, league_id: str, subcat_id: str) -> List[DraftKingsProp]:
    """Fetch and parse a single subcategory of props."""
    url = "https://sportsbook-nash.draftkings.com/sites/US-SB/api/sportscontent/controldata/league/leagueSubcategory/v1/markets"
    
    # DraftKings sometimes wants just league_id, sometimes league_id,subcat_id
    tvars = league_id if league_id == "42648" else f"{league_id},{subcat_id}"
    
    params = {
        "isBatchable": "false",
        "templateVars": tvars,
        "eventsQuery": f"$filter=leagueId eq '{league_id}' AND clientMetadata/Subcategories/any(s: s/Id eq '{subcat_id}')",
        "marketsQuery": f"$filter=clientMetadata/subCategoryId eq '{subcat_id}' AND tags/all(t: t ne 'SportcastBetBuilder')",
        "include": "Events",
        "entity": "events"
    }
    
    props: List[DraftKingsProp] = []
    try:
        r = await session.get(url, params=params, headers=DK_HEADERS, timeout=15)
        if r.status_code != 200:
            logger.error("DraftKings [%s] HTTP %d: %s", league, r.status_code, r.text[:100])
            return []
        
        data = r.json()
        markets = data.get("markets", [])
        selections = data.get("selections", [])
        logger.debug("Captured %d markets and %d selections for %s", len(markets), len(selections), subcat_id)
        
        # Build market lookup
        market_map = {m["id"]: m for m in markets}
        
        # Group selections by market to find Over/Under pairs
        by_market: Dict[str, Dict] = {}
        
        for sel in selections:
            mkt_id = sel.get("marketId")
            if mkt_id not in market_map:
                continue
            
            mkt = market_map[mkt_id]
            mkt_name = mkt.get("name", "")
            outcome_type = sel.get("outcomeType", "") # Over, Under, Home, Away
            label = sel.get("label", "")
            
            # Extract line (points)
            points = sel.get("points")
            # For milestone markets (1+, 2+), the milestoneValue is used
            milestone = sel.get("milestoneValue")
            
            line = points if points is not None else (milestone - 0.5 if milestone else None)
            if line is None:
                continue
                
            odds = sel.get("displayOdds", {}).get("american")
            if not odds:
                continue
            
            try:
                american_odds = int(odds.replace("\u2212", "-").replace("\u002B", "+"))
            except:
                continue
                
            # DraftKings often puts player name in the market name for player props
            # e.g. "Byron Buxton Home Runs"
            # Or in the participant label
            participants = sel.get("participants", [])
            player_name = ""
            if participants:
                player_name = participants[0].get("name", "")
            
            if not player_name:
                # Fallback: parse from market name
                # "Player Name prop type" -> strip known suffixes
                suffixes = [" Home Runs", " Points", " Rebounds", " Assists", " Three Pointers"]
                player_name = mkt_name
                for s in suffixes:
                    if player_name.endswith(s):
                        player_name = player_name[:-len(s)].strip()
                        break
            
            prop_key = (player_name, mkt_name, line)
            if prop_key not in by_market:
                by_market[prop_key] = {
                    "player_name": player_name,
                    "mkt_name": mkt_name,
                    "line": line,
                    "over_odds": None,
                    "under_odds": None
                }
            
            if outcome_type == "Over" or "+" in label:
                by_market[prop_key]["over_odds"] = american_odds
            elif outcome_type == "Under":
                by_market[prop_key]["under_odds"] = american_odds
            else:
                # For milestone markets, it's just "Over" effectively
                by_market[prop_key]["over_odds"] = american_odds
                
        for key, entry in by_market.items():
            # Normalize prop type
            normalized_type = None
            mkt_lower = entry["mkt_name"].lower()
            
            # Simple mapping
            if "points" in mkt_lower: normalized_type = "Points"
            elif "rebounds" in mkt_lower: normalized_type = "Rebounds"
            elif "assists" in mkt_lower: normalized_type = "Assists"
            elif "threes" in mkt_lower or "three pointers" in mkt_lower: normalized_type = "3-PT Made"
            elif "home run" in mkt_lower: normalized_type = "Home Runs"
            elif "strikeouts" in mkt_lower: normalized_type = "Pitcher Strikeouts"
            elif "hits" in mkt_lower: normalized_type = "Hits"
            elif "rbis" in mkt_lower: normalized_type = "RBIs"
            elif "runs" in mkt_lower: normalized_type = "Runs"
            elif "hits+runs+rbis" in mkt_lower: normalized_type = "Hits+Runs+RBIs"
            
            if not normalized_type:
                continue
                
            props.append(DraftKingsProp(
                league=league,
                player_name=entry["player_name"],
                prop_type=normalized_type,
                line=entry["line"],
                over_odds=entry["over_odds"],
                under_odds=entry["under_odds"],
                both_sided=(entry["over_odds"] is not None and entry["under_odds"] is not None)
            ))
            
    except Exception as e:
        logger.error("DraftKings [%s] subcat %s error: %s", league, subcat_id, e)
        
    return props

async def _scrape_league(session: requests.AsyncSession, league: str) -> List[DraftKingsProp]:
    config = LEAGUE_CONFIG.get(league.upper())
    if not config:
        return []
    
    tasks = []
    for subcat_name, subcat_id in config["subcategories"].items():
        tasks.append(_fetch_subcategory(session, league, config["id"], subcat_id))
        
    results = await asyncio.gather(*tasks)
    all_props = []
    for r in results:
        all_props.extend(r)
        
    # Deduplicate
    seen = set()
    unique = []
    for p in all_props:
        key = (p.player_name, p.prop_type, p.line)
        if key not in seen:
            seen.add(key)
            unique.append(p)
            
    return unique

async def _scrape_all_leagues(active_leagues: dict = None) -> List[DraftKingsProp]:
    leagues = active_leagues if active_leagues is not None else ACTIVE_LEAGUES
    all_props: List[DraftKingsProp] = []
    
    async with requests.AsyncSession(impersonate="chrome") as session:
        for league, active in leagues.items():
            if not active:
                continue
            try:
                props = await _scrape_league(session, league)
                all_props.extend(props)
                logger.info("DraftKings [%s]: %d props captured", league, len(props))
            except Exception as e:
                logger.error("DraftKings [%s]: scraper error - %s", league, e)
                
    return all_props

def scrape_draftkings(active_leagues: dict = None) -> List[DraftKingsProp]:
    """Synchronous entry point."""
    return asyncio.run(_scrape_all_leagues(active_leagues))

if __name__ == "__main__":
    # Test script
    logging.basicConfig(level=logging.INFO)
    test_leagues = {"NBA": True, "MLB": True}
    res = scrape_draftkings(test_leagues)
    print(f"Total props: {len(res)}")
    for p in res[:5]:
        print(p)
