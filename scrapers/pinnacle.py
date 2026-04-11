"""
Pinnacle API scraper.
Fetches sharp player-prop lines from guest.api.arcadia.pinnacle.com.
Pinnacle is widely considered the sharpest sportsbook, so these odds
are used to derive the most accurate "true odds" for EV calculations.
"""
import asyncio
import logging
import re
from typing import List, Dict, Optional, Tuple

from curl_cffi import requests

from config import ACTIVE_LEAGUES
from engine.matcher import FanDuelProp as PinnacleProp  # reuse same dataclass

logger = logging.getLogger(__name__)

PINNACLE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Origin": "https://www.pinnacle.com",
    "Referer": "https://www.pinnacle.com/",
}

# Pinnacle league IDs per sport
LEAGUE_CONFIG = {
    "NBA": {"id": "487"},
    "MLB": {"id": "246"},
    "NHL": {"id": "1456"},
    "NCAAB": {"id": "493"},
    "SOCCER": {"id": "1980,2663,2627,2196,2630,2462,214101"},
}

# Map Pinnacle prop-type labels → our normalized names
_PROP_TYPE_MAP = {
    # Basketball
    "points":          "Points",
    "rebounds":        "Rebounds",
    "assists":         "Assists",
    "3 point fg":      "3-PT Made",
    "pts+rebs+asts":   "Pts+Rebs+Asts",
    "pts+rebs":        "Pts+Rebs",
    "pts+asts":        "Pts+Asts",
    "rebs+asts":       "Rebs+Asts",
    "double+double":   "Double-Double",
    "triple+double":   "Triple-Double",
    "first basket scorer": "First Basket",
    # Baseball
    "home runs":       "Home Runs",
    "total bases":     "Total Bases",
    "hits":            "Hits",
    "rbis":            "RBIs",
    "runs":            "Runs",
    "total strikeouts":"Pitcher Strikeouts",
    "pitching outs":   "Pitching Outs",
    "earned runs":     "Earned Runs Allowed",
    "hits allowed":    "Hits Allowed",
    "walks":           "Walks",
    # Hockey
    "goals":           "Goals",
    "shots on goal":   "Shots on Goal",
    "saves":           "Saves",
    "power play points": "Power Play Points",
    # Soccer
    "shots":           "Shots",
    "shots on target": "Shots On Target",
    "passes":          "Passes Attempted",
    "tackles":         "Tackles",
}

_DESC_RE = re.compile(r"^(.+?)\s*\(([^)]+)\)")


def _parse_description(desc: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Parse 'Player Name (Prop Type)' → (player_name, raw_prop_type).
    Also handles suffixes like '(must start)' that Pinnacle appends for MLB.
    """
    m = _DESC_RE.match(desc)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    
    if desc.endswith(" To Score"):
        return desc[:-9].strip(), "goals"
        
    return None, None


async def _scrape_league_id(session: requests.AsyncSession, base: str, league: str, lid: str) -> List[PinnacleProp]:
    """Fetch matchups + markets for a specific Pinnacle league ID."""
    try:
        matchups_resp, markets_resp = await asyncio.gather(
            session.get(f"{base}/leagues/{lid}/matchups", headers=PINNACLE_HEADERS, timeout=20),
            session.get(f"{base}/leagues/{lid}/markets/straight", headers=PINNACLE_HEADERS, timeout=20),
        )
    except Exception as e:
        logger.error("Pinnacle [%s/%s] fetch error: %s", league, lid, e)
        return []

    if matchups_resp.status_code != 200:
        logger.error("Pinnacle [%s/%s] matchups HTTP %d", league, lid, matchups_resp.status_code)
        return []
    if markets_resp.status_code != 200:
        logger.error("Pinnacle [%s/%s] markets HTTP %d", league, lid, markets_resp.status_code)
        return []

    matchups = matchups_resp.json()
    markets = markets_resp.json()

    # ── Step 1: Build prop-matchup lookup ──────────────────────────────────
    # matchup_id → {player_name, prop_type_normalized, over_pid, under_pid}
    prop_lookup: Dict[int, Dict] = {}

    for item in matchups:
        if item.get("type") != "special":
            continue
        special = item.get("special", {})
        if special.get("category") != "Player Props":
            continue

        desc = special.get("description", "")
        player_name, raw_prop = _parse_description(desc)
        if not player_name or not raw_prop:
            continue

        normalized = _PROP_TYPE_MAP.get(raw_prop.lower())
        if not normalized:
            continue

        over_pid = under_pid = None
        for p in item.get("participants", []):
            p_name = p.get("name", "")
            if p_name in ["Over", "Yes"]:
                over_pid = p.get("id")
            elif p_name in ["Under", "No"]:
                under_pid = p.get("id")

        prop_lookup[item["id"]] = {
            "player_name": player_name,
            "prop_type": normalized,
            "over_pid": over_pid,
            "under_pid": under_pid,
            "start_time": item.get("startTime"),
        }

    # ── Step 2: Join with markets to get odds & lines ──────────────────────
    props: List[PinnacleProp] = []

    for mkt in markets:
        mid = mkt.get("matchupId")
        if mid not in prop_lookup:
            continue
            
        mkt_type = mkt.get("type")
        if mkt_type not in ["total", "moneyline"]:
            continue
            
        if mkt.get("period") != 0:
            continue

        info = prop_lookup[mid]
        prices = mkt.get("prices", [])

        over_odds = under_odds = None
        line = 0.5 if mkt_type == "moneyline" else None

        for price in prices:
            pid = price.get("participantId")
            if pid == info["over_pid"]:
                over_odds = price.get("price")
                if mkt_type == "total":
                    line = price.get("points")
            elif pid == info["under_pid"]:
                under_odds = price.get("price")
                if mkt_type == "total" and line is None:
                    line = price.get("points")

        if line is None:
            continue

        both_sided = over_odds is not None and under_odds is not None
        props.append(PinnacleProp(
            league=league,
            player_name=info["player_name"],
            prop_type=info["prop_type"],
            line=line,
            over_odds=over_odds,
            under_odds=under_odds,
            both_sided=both_sided,
            start_time=info.get("start_time", ""),
        ))

    return props


async def _scrape_league(session: requests.AsyncSession, league: str) -> List[PinnacleProp]:
    """Fetch matchups + markets for one Pinnacle league and return parsed props."""
    config = LEAGUE_CONFIG.get(league.upper())
    if not config:
        return []

    league_ids = str(config["id"]).split(",")
    base = "https://guest.api.arcadia.pinnacle.com/0.1"

    tasks = [_scrape_league_id(session, base, league, lid) for lid in league_ids]
    results = await asyncio.gather(*tasks)

    all_props = []
    for props in results:
        all_props.extend(props)

    logger.info("Pinnacle [%s]: %d props captured", league, len(all_props))
    return all_props


async def _scrape_all_leagues(active_leagues: dict = None) -> List[PinnacleProp]:
    leagues = active_leagues if active_leagues is not None else ACTIVE_LEAGUES
    all_props: List[PinnacleProp] = []

    async with requests.AsyncSession(impersonate="chrome") as session:
        for league, active in leagues.items():
            if not active:
                continue
            try:
                props = await _scrape_league(session, league)
                all_props.extend(props)
            except Exception as e:
                logger.error("Pinnacle [%s]: scraper error - %s", league, e)

    return all_props


def scrape_pinnacle(active_leagues: dict = None) -> List[PinnacleProp]:
    """Synchronous entry point."""
    return asyncio.run(_scrape_all_leagues(active_leagues))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_leagues = {"NBA": True, "MLB": True, "NHL": True, "NCAAB": True}
    res = scrape_pinnacle(test_leagues)
    print(f"Total props: {len(res)}")
    from collections import Counter
    types = Counter(f"{p.league}/{p.prop_type}" for p in res)
    for t, c in types.most_common():
        print(f"  {t}: {c}")
    print()
    for p in res[:10]:
        print(f"  {p.player_name:25s} | {p.league:5s} | {p.prop_type:20s} | "
              f"line={p.line} | over={p.over_odds} | under={p.under_odds}")
