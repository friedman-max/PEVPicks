"""
DraftKings API scraper (Controlled Data strategy).
Fetches lines directly from sportsbook-nash.draftkings.com.

Subcategory IDs discovered via API enumeration (April 2026).
"""
import asyncio
import logging
import json
from typing import Optional, List, Dict
from curl_cffi import requests

from config import ACTIVE_LEAGUES
from engine.constants import PROP_TYPE_MAP
from engine.matcher import FanDuelProp as DraftKingsProp  # Reusing FanDuelProp structure

logger = logging.getLogger(__name__)

DK_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Origin": "https://sportsbook.draftkings.com",
    "Referer": "https://sportsbook.draftkings.com/",
}

# ---------------------------------------------------------------------------
# Subcategory IDs for player props, discovered via API enumeration.
# Each subcategory ID maps to a specific market type.
# ---------------------------------------------------------------------------
LEAGUE_CONFIG = {
    "NBA": {
        "id": "42648",
        "subcategories": {
            # ── Individual stats ──
            "Points":           "16477",
            "Assists":          "16478",
            "Rebounds":         "16479",
            "Three Pointers":   "16480",
            # ── Combo stats ──
            "Pts+Asts":         "16481",
            "Pts+Rebs":         "16482",
            "Pts+Rebs+Asts":    "16483",
        }
    },
    "MLB": {
        "id": "84240",
        "subcategories": {
            # ── Batter props ──
            "Home Runs":            "17319",
            "Hits":                 "17320",
            "Total Bases":          "17321",
            "RBIs":                 "17322",
            "Hits+Runs+RBIs":       "17843",
            "Runs":                 "17844",
            "Singles":              "17845",
            "Doubles":              "17846",
            "Triples":              "17847",
            "Stolen Bases":         "18726",
            "Extra Base Hits":      "19451",
            "Runs+RBIs":            "19453",
            # ── Pitcher props ──
            "Pitcher Strikeouts":   "17323",
            "Pitching Outs":        "17413",
        }
    },
    "NHL": {
        "id": "42133",
        "subcategories": {
            "Goals":            "14495",
            "Points":           "16545",
            "Assists":          "16546",
            "Shots on Goal":    "16544",
            "Saves":            "16550",
        }
    }
}

# ---------------------------------------------------------------------------
# Subcategory name → canonical prop type.
# This is the PRIMARY mapping: since we fetch by subcategory, we already
# know what kind of prop we're looking at.
# ---------------------------------------------------------------------------
SUBCAT_TO_PROP_TYPE = {
    # NBA
    "Points":           "Points",
    "Assists":          "Assists",
    "Rebounds":         "Rebounds",
    "Three Pointers":   "3-PT Made",
    "Pts+Asts":         "Pts+Asts",
    "Pts+Rebs":         "Pts+Rebs",
    "Pts+Rebs+Asts":    "Pts+Rebs+Asts",
    # MLB batter
    "Home Runs":        "Home Runs",
    "Hits":             "Hits",
    "Total Bases":      "Total Bases",
    "RBIs":             "RBIs",
    "Hits+Runs+RBIs":   "Hits+Runs+RBIs",
    "Runs":             "Runs",
    "Singles":          "Singles",
    "Doubles":          "Doubles",
    "Triples":          "Triples",
    "Stolen Bases":     "Stolen Bases",
    "Extra Base Hits":  "Extra Base Hits",
    "Runs+RBIs":        "Runs+RBIs",
    # MLB pitcher
    "Pitcher Strikeouts": "Pitcher Strikeouts",
    "Pitching Outs":    "Pitching Outs",
    # NHL
    "Goals":            "Goals",
    "Shots on Goal":    "Shots on Goal",
    "Saves":            "Saves",
}


def _resolve_prop_type(subcat_name: str) -> Optional[str]:
    """Resolve the canonical prop type from the subcategory name.
    
    Since we fetch by subcategory, the subcategory name is the
    authoritative source of what prop type we're dealing with.
    """
    return SUBCAT_TO_PROP_TYPE.get(subcat_name)


def _extract_player_name(market_name: str, participants: list, prop_type: Optional[str]) -> str:
    """Extract the player name from selection participants or market name."""
    # Prefer participant name from the selection
    if participants:
        name = participants[0].get("name", "")
        if name:
            return name
    
    if not prop_type:
        return market_name
    
    # Fallback: strip known DK market suffixes from the market name
    # e.g. "Aaron Judge Home Runs" → "Aaron Judge"
    _SUFFIXES = [
        " Home Runs", " Hits + Runs + RBIs", " Runs + RBIs",
        " Extra Base Hits", " Total Bases", " Stolen Bases",
        " Strikeouts Thrown", " Outs O/U",
        " Singles", " Doubles", " Triples",
        " RBIs", " Hits", " Runs",
        " Points + Rebounds + Assists", " Points + Assists",
        " Points + Rebounds",
        " Three Pointers Made", " Three Pointers",
        " Rebounds", " Assists", " Points",
        " Shots on Goal", " Saves", " Goals",
    ]
    for suffix in _SUFFIXES:
        if market_name.endswith(suffix):
            return market_name[: -len(suffix)].strip()
    
    return market_name


async def _fetch_subcategory(
    session: requests.AsyncSession,
    league: str,
    league_id: str,
    subcat_name: str,
    subcat_id: str,
) -> List[DraftKingsProp]:
    """Fetch and parse a single subcategory of props."""
    url = (
        "https://sportsbook-nash.draftkings.com/sites/US-SB/api/sportscontent/"
        "controldata/league/leagueSubcategory/v1/markets"
    )

    params = {
        "isBatchable": "false",
        "templateVars": f"{league_id},{subcat_id}",
        "eventsQuery": (
            f"$filter=leagueId eq '{league_id}' "
            f"AND clientMetadata/Subcategories/any(s: s/Id eq '{subcat_id}')"
        ),
        "marketsQuery": (
            f"$filter=clientMetadata/subCategoryId eq '{subcat_id}' "
            "AND tags/all(t: t ne 'SportcastBetBuilder')"
        ),
        "include": "Events",
        "entity": "events",
    }

    props: List[DraftKingsProp] = []
    try:
        r = await session.get(url, params=params, headers=DK_HEADERS, timeout=15)
        if r.status_code != 200:
            logger.warning(
                "DraftKings [%s/%s] HTTP %d", league, subcat_name, r.status_code
            )
            return []

        data = r.json()
        markets = data.get("markets", [])
        selections = data.get("selections", [])
        logger.debug(
            "DK [%s/%s] %d markets, %d selections",
            league, subcat_name, len(markets), len(selections),
        )

        # Build market lookup
        market_map = {m["id"]: m for m in markets}

        # Group selections by market to find Over/Under pairs
        by_market: Dict[tuple, Dict] = {}

        for sel in selections:
            mkt_id = sel.get("marketId")
            if mkt_id not in market_map:
                continue

            mkt = market_map[mkt_id]
            mkt_name = mkt.get("name", "")
            outcome_type = sel.get("outcomeType", "")  # Over, Under, Home, Away
            label = sel.get("label", "")

            # Extract line (points)
            points = sel.get("points")
            milestone = sel.get("milestoneValue")
            line = points if points is not None else (
                milestone - 0.5 if milestone else None
            )
            if line is None:
                continue

            # Extract American odds
            odds = sel.get("displayOdds", {}).get("american")
            if not odds:
                continue
            try:
                american_odds = int(
                    odds.replace("\u2212", "-").replace("\u002B", "+")
                )
            except (ValueError, AttributeError):
                continue

            # Player name
            participants = sel.get("participants", [])
            # Use the subcategory name as the authoritative prop type
            prop_type = _resolve_prop_type(subcat_name)
            player_name = _extract_player_name(mkt_name, participants, prop_type)

            if not player_name or not prop_type:
                continue

            prop_key = (player_name, prop_type, line)
            if prop_key not in by_market:
                by_market[prop_key] = {
                    "player_name": player_name,
                    "prop_type": prop_type,
                    "line": line,
                    "over_odds": None,
                    "under_odds": None,
                }

            if outcome_type == "Over" or "+" in label:
                by_market[prop_key]["over_odds"] = american_odds
            elif outcome_type == "Under":
                by_market[prop_key]["under_odds"] = american_odds
            else:
                # Milestone markets (1+, 2+) are effectively "Over"
                by_market[prop_key]["over_odds"] = american_odds

        for entry in by_market.values():
            props.append(
                DraftKingsProp(
                    league=league,
                    player_name=entry["player_name"],
                    prop_type=entry["prop_type"],
                    line=entry["line"],
                    over_odds=entry["over_odds"],
                    under_odds=entry["under_odds"],
                    both_sided=(
                        entry["over_odds"] is not None
                        and entry["under_odds"] is not None
                    ),
                )
            )

    except Exception as e:
        logger.error("DraftKings [%s/%s] error: %s", league, subcat_name, e)

    return props


async def _scrape_league(
    session: requests.AsyncSession, league: str
) -> List[DraftKingsProp]:
    config = LEAGUE_CONFIG.get(league.upper())
    if not config:
        return []

    tasks = []
    for subcat_name, subcat_id in config["subcategories"].items():
        tasks.append(
            _fetch_subcategory(session, league, config["id"], subcat_name, subcat_id)
        )

    results = await asyncio.gather(*tasks)
    all_props: List[DraftKingsProp] = []
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
    test_leagues = {"NBA": True, "MLB": True, "NHL": True}
    res = scrape_draftkings(test_leagues)

    # Group by league and prop type for summary
    from collections import Counter
    by_league: Dict[str, Counter] = {}
    for p in res:
        by_league.setdefault(p.league, Counter())[p.prop_type] += 1

    print(f"\nTotal props: {len(res)}")
    for league in sorted(by_league):
        print(f"\n{league}:")
        for prop_type, count in by_league[league].most_common():
            print(f"  {prop_type}: {count}")

    # Show a few examples per prop type
    print("\n--- Sample props ---")
    shown_types = set()
    for p in res:
        key = (p.league, p.prop_type)
        if key not in shown_types:
            shown_types.add(key)
            print(
                f"  {p.league} | {p.player_name} | {p.prop_type} | "
                f"Line: {p.line} | Over: {p.over_odds} | Under: {p.under_odds}"
            )
