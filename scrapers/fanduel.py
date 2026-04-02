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
import re
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

    # If market name has "Player - PropType" format, try just the prop part
    if " - " in raw:
        prop_part = raw.split(" - ", 1)[1].strip()
        res = PROP_TYPE_MAP.get(prop_part)
        if res: return res

    raw_norm = raw.replace("_", " ")

    # ── NBA / NCAAB combo stats ──
    if "made 3 point field goals" in raw_norm or "made threes" in raw_norm or " threes" in raw_norm: return "3-PT Made"
    if "points + rebounds + assists" in raw_norm or "pts + reb + ast" in raw_norm or "pts+reb+ast" in raw_norm: return "Pts+Rebs+Asts"
    if "points + rebounds" in raw_norm or "pts + reb" in raw_norm: return "Pts+Rebs"
    if "points + assists" in raw_norm or "pts + ast" in raw_norm: return "Pts+Asts"
    if "rebounds + assists" in raw_norm or "reb + ast" in raw_norm: return "Rebs+Asts"
    if "blocks + steals" in raw_norm or "steals + blocks" in raw_norm: return "Blks+Stls"

    # ── NBA / NCAAB individual stats ──
    if "total points" in raw_norm or raw_norm.endswith(" - points") or raw_norm.endswith(" points"):
        if any(x in raw_norm for x in ["1st", "2nd", "3rd", "4th", "quarter", "1q", "2q", "3q", "4q", "half"]): return None
        return "Points"
    if "total rebounds" in raw_norm or raw_norm.endswith(" - rebounds") or raw_norm.endswith(" rebounds"): return "Rebounds"
    if "total assists" in raw_norm or raw_norm.endswith(" - assists") or raw_norm.endswith(" assists"): return "Assists"
    if raw_norm == "steals" or raw_norm.endswith(" - steals"): return "Steals"
    if "blocked shots" in raw_norm or raw_norm == "blocks" or raw_norm.endswith(" - blocks"): return "Blocked Shots"

    # ── MLB pitcher props (market types like PITCHER_C_TOTAL_STRIKEOUTS) ──
    if "total strikeouts" in raw_norm or raw_norm.endswith(" strikeouts") or raw_norm.endswith(" - strikeouts"):
        return "Pitcher Strikeouts"
    if "outs recorded" in raw_norm: return "Pitching Outs"
    if "earned runs" in raw_norm: return "Earned Runs Allowed"
    if "hits allowed" in raw_norm: return "Hits Allowed"
    if "walks allowed" in raw_norm or "walks issued" in raw_norm: return "Walks"
    if "total bases" in raw_norm and "record" not in raw_norm: return "Total Bases"
    if raw_norm.endswith(" - hits") or raw_norm == "hits": return "Hits"
    if raw_norm.endswith(" - runs") or raw_norm == "runs" or raw_norm == "batting runs": return "Runs"
    if raw_norm.endswith(" - rbis") or raw_norm == "rbis": return "RBIs"

    # ── NHL player props (market types like PLAYER_TOTAL_SHOTS, PLAYER_TOTAL_SAVES) ──
    if "shots on goal" in raw_norm or "total shots" in raw_norm or "player total shots" in raw_norm or "shots" in raw_norm: return "Shots on Goal"
    if "total saves" in raw_norm or "player total saves" in raw_norm or "saves" in raw_norm: return "Saves"
    if "total goals" in raw_norm or "player total goals" in raw_norm or "goal" in raw_norm: return "Goals"
    if "total assists" in raw_norm or "player total assists" in raw_norm or "assist" in raw_norm: return "Assists"
    if "time on ice" in raw_norm: return "Time On Ice"
    if "points" in raw_norm and league == "NHL": return "Points"

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

# Game-level market types to skip (not player props)
_GAME_LEVEL_TYPES = {
    "MONEY_LINE", "MATCH_HANDICAP_(2-WAY)", "TOTAL_POINTS_(OVER/UNDER)",
    "MATCH_RESULT", "BOTH_TEAMS_TO_SCORE", "DRAW_NO_BET",
}

# ── Multi-runner "To Record X+" markets (MLB, NBA, NHL props) ──
_MULTI_RUNNER_RE = re.compile(
    r"^(?:to record|player to record|player|to hit)\s+"
    r"(?:a |an )?(\d+\+\s*)?(.+)$",
    re.IGNORECASE,
)

# Market name fragment → PrizePicks stat type
_MULTI_RUNNER_MAP = {
    # -- MLB Batter milestones --
    "hit":                "Hits",
    "hits":               "Hits",
    "single":             "Singles",
    "doubles":            "Doubles",
    "triples":            "Triples",
    "home run":           "Home Runs",
    "home runs":          "Home Runs",
    "rbi":                "RBIs",
    "rbis":               "RBIs",
    "run":                "Runs",
    "runs":               "Runs",
    "total bases":        "Total Bases",
    "stolen base":        "Stolen Bases",
    "stolen bases":       "Stolen Bases",
    "hits + runs + rbis": "Hits+Runs+RBIs",
    "walks":              "Walks",
    "strikeouts":         "Hitter Strikeouts",
    
    # -- NBA milestones --
    "points":             "Points",
    "rebounds":           "Rebounds",
    "assists":            "Assists",
    "made threes":        "3-PT Made",
    "three point field goals": "3-PT Made",
    "threes":             "3-PT Made",
    "steals":             "Steals",
    "blocks":             "Blocked Shots",
    "blocked shots":      "Blocked Shots",
    
    # -- NHL milestones --
    "shots on goal":      "Shots on Goal",
    "shots":              "Shots on Goal",
    "saves":              "Saves",
    "goals":              "Goals",
    "points-assists":     "Pts+Asts",
    "points":             "Points",
}

def _parse_multi_runner_market(mkt_name: str) -> Optional[tuple[str, float]]:
    """
    Parse a multi-runner milestone market name.
    """
    m = _MULTI_RUNNER_RE.match(mkt_name.strip())
    if not m:
        # Fallback: "[Stat] X+" or "X+ [Stat]"
        m2 = re.match(r"^(.+?)\s+(\d+\+)$", mkt_name.strip(), re.IGNORECASE)
        if m2:
             stat_part = m2.group(1).strip()
             threshold_str = m2.group(2).rstrip("+")
        else:
             return None
    else:
        threshold_str = (m.group(1) or "").strip().rstrip("+").strip()
        stat_part = m.group(2).strip().lower()

    threshold = int(threshold_str) if threshold_str else 1
    stat_raw = stat_part.lower()
    stat_norm = stat_raw.rstrip("s")

    # Try mapping
    pp_stat = _MULTI_RUNNER_MAP.get(stat_raw) or _MULTI_RUNNER_MAP.get(stat_norm)
    if not pp_stat:
        # Partial match
        for key, val in _MULTI_RUNNER_MAP.items():
            if key in stat_raw:
                pp_stat = val
                break
    if not pp_stat:
        return None

    line = threshold - 0.5
    return (pp_stat, line)

def _extract_props_from_json(data: dict, league: str) -> list[FanDuelProp]:
    """Parse FanDuel API JSON response."""
    props: list[FanDuelProp] = []

    try:
        attachments = data.get("attachments", {})
        markets_raw = attachments.get("markets", {})
        events_raw  = attachments.get("events",  {})

        player_by_event: dict[str, str] = {}
        for ev_id, ev in events_raw.items():
            name = ev.get("name", "") or ev.get("teamName", "")
            player_by_event[str(ev_id)] = name

        for mkt_id, mkt in markets_raw.items():
            mkt_name     = mkt.get("marketName", "") or mkt.get("marketType", "")
            market_type  = mkt.get("marketType", "")

            if market_type in _GAME_LEVEL_TYPES:
                continue
                
            mkt_lower = mkt_name.lower()
            if any(x in mkt_lower for x in [
                "1st period", "2nd period", "3rd period",
                "1st quarter", "2nd quarter", "3rd quarter", "4th quarter",
                "1st half", "2nd half",
                "inning", "first pitch",
                "game specials", "team total",
                "puck line", "run line", "spread betting",
                "will there be", "moneyline",
                "any time goal scorer", "anytime goal scorer",
                "first goal scorer", "last goal scorer",
            ]):
                continue

            # ── Multi-runner / Milestone markets ──
            multi = _parse_multi_runner_market(mkt_name)
            is_alt = " - alt" in mkt_name.lower() or "alternative" in mkt_name.lower() 
            
            if multi or is_alt:
                runners = mkt.get("runners", [])
                for runner in runners:
                    runner_raw = runner.get("runnerName", "").strip()
                    if not runner_raw: continue
                    if "/" in runner_raw: continue

                    if multi:
                        pp_stat, line = multi
                        player_name = runner_raw
                    else:
                        m_th = re.search(r"(\d+)\+", runner_raw)
                        if not m_th: continue
                        threshold = int(m_th.group(1))
                        line = threshold - 0.5
                        
                        pp_stat = _normalize_prop_type(mkt_name)
                        if not pp_stat: continue
                        
                        player_name = runner_raw.split(" - ")[0].strip()
                        if player_name.lower() in ["over", "under"]:
                             player_name = mkt_name.split(" - ")[0].strip()

                    win_odds = runner.get("winRunnerOdds", {})
                    american = (
                        _parse_american(win_odds.get("americanDisplayOdds", {}).get("americanOdds"))
                        or _parse_american(runner.get("currentPrice"))
                    )
                    if american is None: continue
                    
                    props.append(FanDuelProp(
                        league=league,
                        player_name=player_name,
                        prop_type=pp_stat,
                        line=line,
                        over_odds=american,
                        under_odds=None,
                        both_sided=False,
                    ))
                continue

            normalized = _normalize_prop_type(mkt_name) or _normalize_prop_type(market_type)
            if not normalized:
                continue

            runners = mkt.get("runners", [])
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

                if " - " in mkt_name:
                    player_name = mkt_name.split(" - ")[0].strip()
                else:
                    runner_raw = runner.get("runnerName", "")
                    clean_runner = runner_raw
                    for suffix in [" - Over", " - Under", " Over", " Under"]:
                        if clean_runner.endswith(suffix):
                            clean_runner = clean_runner[:-len(suffix)].strip()
                            break
                    clean_runner = clean_runner.split("(")[0].strip()

                    market_player = ""
                    _PROP_SUFFIXES = [
                        "shots on goal", "total saves", "total goals",
                        "total assists", "total points", "outs recorded",
                        "strikeouts",
                    ]
                    mkt_name_lower = mkt_name.lower()
                    for suffix in _PROP_SUFFIXES:
                        if mkt_name_lower.endswith(suffix):
                            market_player = mkt_name[:-(len(suffix))].strip()
                            break

                    player_name = (
                        runner.get("selectionName")
                        or market_player
                        or clean_runner
                    )

                    event_id = str(mkt.get("eventId", ""))
                    if event_id and event_id in player_by_event:
                        if not player_name or player_name.lower() in ["over", "under", ""]:
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

LEAGUE_TABS = {
    "NBA": [
        "player-points", "player-rebounds", "player-assists",
        "player-threes", "player-props", "player-combos",
        "player-defense", "alternative-handicaps",
    ],
    "NCAAB": [
        "player-points", "player-rebounds", "player-assists",
        "player-threes", "player-props", "player-combos",
    ],
    "NHL": [
        "shots", "goalies", "goals", "points-assists",
        "player-props", "alternative-handicaps",
    ],
    "MLB": [
        "pitcher-props", "batter-props", "player-props", 
        "home-runs", "strikeouts", "hits", "runs", "rbis", 
        "total-bases", "stolen-bases", "outs-recorded", 
        "earned-runs-allowed", "earned-runs", "walks-allowed", 
        "walks-issued", "walks", "pitcher-strikeouts", 
        "hits-+-runs-+-rbis", "hits-runs-rbis", "to-record-a-hit",
        "hits-allowed", "singles", "doubles", "triples",
        "alternative-run-lines",
    ],
}

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

    TABS = LEAGUE_TABS.get(league.upper(), [
        "player-props", "player-points", "player-rebounds",
        "player-assists", "player-threes",
    ])
    
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
             
    # Deduplicate by (player_name, prop_type, line)
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
