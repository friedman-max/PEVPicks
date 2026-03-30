"""
FanDuel API scraper (Two-Step HTTP strategy).

Strategy:
  1. Discovery: Fetch `content-managed-page` API to get the active `eventId`s for the league.
  2. Enumeration: Loop through each `eventId` and fetch `event-page?tab=player-props` to intercept the prop JSON directly.
  3. Parse the raw JSON — immune to UI/CSS changes and entirely bypasses PerimeterX bots headers blocks.
"""
import asyncio
import httpx
import logging
import urllib.parse
from typing import Optional

from config import ACTIVE_LEAGUES
from engine.constants import PROP_TYPE_MAP
from engine.matcher import FanDuelProp

logger = logging.getLogger(__name__)

FD_AK_TOKEN = "FhMFpcPWXMeyZxOx"
FD_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json"
}

def _normalize_prop_type(raw: str) -> Optional[str]:
    raw = raw.lower().strip()
    res = PROP_TYPE_MAP.get(raw)
    if res: return res
    
    raw = raw.replace("_", " ")

    if "made 3 point field goals" in raw or "made threes" in raw or " threes" in raw: return "3-PT Made"
    if "points + rebounds + assists" in raw or "pts + reb + ast" in raw or "pts+reb+ast" in raw: return "Pts+Rebs+Asts"
    if "points + rebounds" in raw or "pts + reb" in raw: return "Pts+Rebs"
    if "points + assists" in raw or "pts + ast" in raw: return "Pts+Asts"
    if "rebounds + assists" in raw or "reb + ast" in raw: return "Rebs+Asts"
    if "blocks + steals" in raw or "steals + blocks" in raw: return "Blks+Stls"
    
    if "total points" in raw or raw.endswith(" - points") or "_points" in raw:
        if any(x in raw for x in ["1st", "2nd", "3rd", "4th", "quarter", "1q", "2q", "3q", "4q", "half"]): return None
        return "Points"
    if "total rebounds" in raw or raw.endswith(" - rebounds") or "_rebounds" in raw: return "Rebounds"
    if "total assists" in raw or raw.endswith(" - assists") or "_assists" in raw: return "Assists"
    if "steals" in raw: return "Steals"
    if "blocks" in raw or "blocked shots" in raw: return "Blocked Shots"

    return None

def _parse_american(price_str) -> Optional[int]:
    """Parse an American odds value from various FanDuel response formats."""
    if price_str is None:
        return None
    try:
        return int(price_str)
    except (ValueError, TypeError):
        pass
    if isinstance(price_str, str):
        price_str = price_str.replace("+", "").strip()
        try:
            return int(price_str)
        except ValueError:
            pass
    return None

def _extract_props_from_json(data: dict, league: str) -> list[FanDuelProp]:
    """
    Parse FanDuel API JSON response.
    Extracts marketName, runnerName, handicap, and americanDisplayOdds for Player Props.
    """
    props: list[FanDuelProp] = []

    try:
        attachments = data.get("attachments", {})
        markets_raw = attachments.get("markets", {})
        events_raw  = attachments.get("events",  {})

        # Build event-id → player name mapping from events
        player_by_event: dict[str, str] = {}
        for ev_id, ev in events_raw.items():
            name = ev.get("name", "") or ev.get("teamName", "")
            player_by_event[str(ev_id)] = name

        for mkt_id, mkt in markets_raw.items():
            mkt_name     = mkt.get("marketName", "") or mkt.get("marketType", "")
            market_type  = mkt.get("marketType", "")

            normalized = _normalize_prop_type(mkt_name) or _normalize_prop_type(market_type)
            if not normalized:
                continue

            runners = mkt.get("runners", [])
            # Group runners by handicap (line) to find over/under pairs
            by_line: dict[float, dict] = {}

            for runner in runners:
                handicap_raw = runner.get("handicap")
                if handicap_raw is None:
                     handicap_raw = runner.get("runnerName", "")
                     
                try:
                    handicap = float(handicap_raw)
                except (ValueError, TypeError):
                    continue

                win_odds = runner.get("winRunnerOdds", {})
                american = (
                    _parse_american(win_odds.get("americanDisplayOdds", {}).get("americanOdds"))
                    or _parse_american(runner.get("currentPrice"))
                )

                runner_name = runner.get("runnerName", "").lower()
                is_over  = "over"  in runner_name or "+" in runner_name
                is_under = "under" in runner_name

                # Extract player name from market name (e.g. "Bam Adebayo - Points")
                if " - " in mkt_name:
                    player_name = mkt_name.split(" - ")[0].strip()
                else:
                    player_name = (
                        runner.get("selectionName")
                        or runner.get("runnerName", "").split("(")[0].strip()
                    )
                    event_id = str(mkt.get("eventId", ""))
                    if event_id and event_id in player_by_event:
                        # Fallback to event name only if we can't find a player
                        if not player_name or player_name.lower() in ["over", "under"]:
                             player_name = player_by_event[event_id]

                if not player_name or handicap == 0:
                    continue

                entry = by_line.setdefault(handicap, {
                    "player_name": player_name,
                    "over_odds":   None,
                    "under_odds":  None,
                })
                if is_over:
                    entry["over_odds"] = american
                elif is_under:
                    entry["under_odds"] = american
                else:
                    # Fallback: treat as over if it's the first runner
                    if entry["over_odds"] is None:
                        entry["over_odds"] = american

            for line, entry in by_line.items():
                over_odds  = entry["over_odds"]
                under_odds = entry["under_odds"]
                if over_odds is None and under_odds is None:
                    continue

                both_sided = over_odds is not None and under_odds is not None
                props.append(FanDuelProp(
                    league=league,
                    player_name=entry["player_name"],
                    prop_type=normalized,
                    line=line,
                    over_odds=over_odds,
                    under_odds=under_odds,
                    both_sided=both_sided,
                ))
    except Exception as e:
        logger.debug("JSON parse error: %s", e)

    return props

async def _fetch_event_tab(client: httpx.AsyncClient, league: str, eid: str, tab: str) -> list[FanDuelProp]:
    url = f"https://sbapi.nj.sportsbook.fanduel.com/api/event-page?_ak={FD_AK_TOKEN}&eventId={eid}&tab={tab}"
    try:
         r = await client.get(url, headers=FD_HEADERS, timeout=15)
         if r.status_code == 200:
              return _extract_props_from_json(r.json(), league)
    except Exception as e:
         logger.debug("FanDuel [%s]: event %s tab %s error: %s", league, eid, tab, e)
    return []

async def _scrape_league(client: httpx.AsyncClient, league: str) -> list[FanDuelProp]:
    all_props = []
    
    logger.info("FanDuel [%s]: fetching events (Phase 1)", league)
    nav_url = f"https://sbapi.nj.sportsbook.fanduel.com/api/content-managed-page?page=CUSTOM&customPageId={league.lower()}&_ak={FD_AK_TOKEN}"
    
    try:
        r = await client.get(nav_url, headers=FD_HEADERS, timeout=15)
        if r.status_code != 200:
            logger.error("FanDuel [%s]: phase 1 returned %d", league, r.status_code)
            return []
        data = r.json()
        events = data.get("attachments", {}).get("events", {})
        event_ids = list(events.keys())
        logger.info("FanDuel [%s]: found %d active events", league, len(event_ids))
    except Exception as e:
        logger.error("FanDuel [%s]: phase 1 error: %s", league, e)
        return []
        
    TABS = [
        "player-props", "player-points", "player-rebounds", 
        "player-assists", "player-threes", "pitcher-props", 
        "batter-props", "home-runs", "player-passing", 
        "player-receiving", "player-rushing"
    ]
    
    sem = asyncio.Semaphore(5)
    
    async def _safe_fetch(eid: str, tab: str):
        async with sem:
            return await _fetch_event_tab(client, league, eid, tab)

    logger.info("FanDuel [%s]: fetching %d tabs for %d events (Phase 2)", league, len(TABS), len(event_ids))
    tasks = []
    for eid in event_ids:
        for tab in TABS:
            tasks.append(_safe_fetch(eid, tab))

    results = await asyncio.gather(*tasks, return_exceptions=True)
    for res in results:
        if isinstance(res, list):
            all_props.extend(res)
             
    # Deduplicate by (player_name, prop_type, line, side)
    seen = set()
    unique: list[FanDuelProp] = []
    for p in all_props:
        key = (p.player_name, p.prop_type, p.line)
        if key not in seen:
            seen.add(key)
            unique.append(p)
            
    logger.info("FanDuel [%s]: %d unique props captured", league, len(unique))
    return unique


async def _scrape_all_leagues(active_leagues: dict | None = None) -> list[FanDuelProp]:
    leagues = active_leagues if active_leagues is not None else ACTIVE_LEAGUES
    all_props: list[FanDuelProp] = []

    async with httpx.AsyncClient(verify=False) as client:
        for league, active in leagues.items():
            if not active:
                continue

            try:
                props = await _scrape_league(client, league)
                all_props.extend(props)
            except Exception as e:
                logger.error("FanDuel [%s]: uncaught scraper error - %s", league, e)
                
    return all_props


def scrape_fanduel(active_leagues: dict | None = None) -> list[FanDuelProp]:
    """Synchronous entry point — runs the async httpx scraper."""
    return asyncio.run(_scrape_all_leagues(active_leagues))
