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

def _normalize_prop_type(raw: str, league: str = "") -> Optional[str]:
    """
    Map FanDuel market names or types to canonical PrizePicks stat_type labels.
    """
    raw = raw.lower().strip()
    raw_norm = raw.replace("_", " ")

    # 1. Direct lookup in league-aware PROP_TYPE_MAP
    res = PROP_TYPE_MAP.get(league.upper(), {}).get(raw)
    if res: return res

    # Check normalized string too
    res = PROP_TYPE_MAP.get(league.upper(), {}).get(raw_norm)
    if res: return res

    # Reject split-period markets up front (1st quarter/half/period, innings, first pitch, etc.)
    _PERIOD_TOKENS = (
        "1st quarter", "2nd quarter", "3rd quarter", "4th quarter",
        "1st qtr", "2nd qtr", "3rd qtr", "4th qtr",
        "1st half", "2nd half", "first half", "second half",
        "1st period", "2nd period", "3rd period",
        "1q ", "2q ", "3q ", "4q ",
        " inning", "first pitch",
    )
    if any(t in raw_norm for t in _PERIOD_TOKENS):
        return None

    # 2. League-Specific substring matching (handles FanDuel's verbose market names)
    lkey = league.upper()

    if lkey == "SOCCER":
        if "shots on target" in raw_norm or "shots on goal" in raw_norm: return "Shots On Target"
        if "total shots" in raw_norm or raw_norm.endswith(" - shots") or raw_norm.endswith(" shots"): return "Shots"
        if "shots" in raw_norm: return "Shots"
        if raw_norm.endswith(" - passes") or "passes completed" in raw_norm or "passes attempted" in raw_norm or "passes" in raw_norm: return "Passes Attempted"
        if raw_norm.endswith(" - tackles") or "tackles" in raw_norm: return "Tackles"
        if raw_norm.endswith(" - crosses") or "crosses" in raw_norm: return "Crosses"
        if raw_norm.endswith(" - clearances") or "clearances" in raw_norm: return "Clearances"
        if raw_norm.endswith(" - assists") or " assists" in raw_norm: return "Assists"
        if raw_norm.endswith(" - saves") or "goalie saves" in raw_norm or "goalkeeper saves" in raw_norm or "saves" in raw_norm: return "Goalie Saves"
        if "goals" in raw_norm or "to score" in raw_norm or "goal-scorer" in raw_norm or "goalscorer" in raw_norm: return "Goals"

    elif lkey in ("NBA", "NCAAB"):
        if "made 3 point field goals" in raw_norm or "made threes" in raw_norm or " threes" in raw_norm: return "3-PT Made"
        if "points + rebounds + assists" in raw_norm or "pts + reb + ast" in raw_norm or "pts+reb+ast" in raw_norm: return "Pts+Rebs+Asts"
        if "points + rebounds" in raw_norm or "pts + reb" in raw_norm: return "Pts+Rebs"
        if "points + assists" in raw_norm or "pts + ast" in raw_norm: return "Pts+Asts"
        if "rebounds + assists" in raw_norm or "reb + ast" in raw_norm: return "Rebs+Asts"
        if "blocks + steals" in raw_norm or "steals + blocks" in raw_norm: return "Blks+Stls"
        if "total points" in raw_norm or raw_norm.endswith(" - points") or raw_norm.endswith(" points"):
            return "Points"
        if "total rebounds" in raw_norm or raw_norm.endswith(" - rebounds") or raw_norm.endswith(" rebounds"): return "Rebounds"
        if "total assists" in raw_norm or raw_norm.endswith(" - assists") or raw_norm.endswith(" assists"): return "Assists"
        if "blocked shots" in raw_norm or raw_norm == "blocks" or raw_norm.endswith(" - blocks") or raw_norm.endswith(" blocks"): return "Blocked Shots"
        if raw_norm.endswith(" - steals") or raw_norm.endswith(" steals") or raw_norm == "steals": return "Steals"
        if raw_norm.endswith(" - turnovers") or raw_norm.endswith(" turnovers"): return "Turnovers"
        if "fantasy score" in raw_norm or "fantasy points" in raw_norm: return "Fantasy Score"

    elif lkey == "MLB":
        # Pitcher props (suffix-based, pitcher is assumed)
        if "total strikeouts" in raw_norm or "strikeouts thrown" in raw_norm: return "Pitcher Strikeouts"
        if raw_norm.endswith(" - strikeouts") or raw_norm.endswith(" - alt strikeouts") or raw_norm.endswith(" - pitcher strikeouts"): return "Pitcher Strikeouts"
        if "outs recorded" in raw_norm or "pitching outs" in raw_norm or raw_norm.endswith(" - outs recorded") or raw_norm.endswith(" - pitcher outs"): return "Pitching Outs"
        if raw_norm.endswith(" - earned runs") or raw_norm.endswith(" - alt earned runs") or "earned runs allowed" in raw_norm: return "Earned Runs Allowed"
        if raw_norm.endswith(" - hits allowed") or raw_norm.endswith(" - alt hits allowed") or "hits allowed" in raw_norm: return "Hits Allowed"
        if raw_norm.endswith(" - walks issued") or raw_norm.endswith(" - alt walks issued") or "walks issued" in raw_norm or "walks allowed" in raw_norm or raw_norm.endswith(" - pitcher walks"): return "Pitcher Walks"

        # Batter props
        if "total bases" in raw_norm and "record" not in raw_norm: return "Total Bases"
        if raw_norm.endswith(" - total bases") or raw_norm.endswith(" - alt total bases"): return "Total Bases"
        if raw_norm.endswith(" - hits") or raw_norm.endswith(" - alt hits") or raw_norm == "hits": return "Hits"
        if raw_norm.endswith(" - runs") or raw_norm.endswith(" - alt runs") or raw_norm.endswith(" - runs scored") or raw_norm == "runs" or raw_norm == "batting runs": return "Runs"
        if raw_norm.endswith(" - rbis") or raw_norm.endswith(" - alt rbis") or raw_norm == "rbis": return "RBIs"
        if raw_norm.endswith(" - home runs") or raw_norm.endswith(" - alt home runs"): return "Home Runs"
        if raw_norm.endswith(" - stolen bases"): return "Stolen Bases"
        if raw_norm.endswith(" - singles"): return "Singles"
        if raw_norm.endswith(" - doubles"): return "Doubles"
        if raw_norm.endswith(" - triples"): return "Triples"
        if raw_norm.endswith(" - walks"): return "Walks"
        if "hits + runs + rbis" in raw_norm or "hits+runs+rbis" in raw_norm: return "Hits+Runs+RBIs"

    elif lkey == "NHL":
        # Player-prefixed suffix form first, then general substring
        if raw_norm.endswith(" - shots on goal") or "shots on goal" in raw_norm or raw_norm.endswith(" shots on goal"): return "Shots on Goal"
        if raw_norm.endswith(" - total shots") or "total shots" in raw_norm: return "Shots on Goal"
        if raw_norm.endswith(" - saves") or "total saves" in raw_norm or raw_norm.endswith(" saves"): return "Saves"
        if raw_norm.endswith(" - power play points") or "power play points" in raw_norm: return "Power Play Points"
        if raw_norm.endswith(" - points") or raw_norm.endswith(" points"): return "Points"
        if raw_norm.endswith(" - assists") or "total assists" in raw_norm or raw_norm.endswith(" assists"): return "Assists"
        if raw_norm.endswith(" - goals") or "total goals" in raw_norm or raw_norm.endswith(" goals"): return "Goals"
        if raw_norm.endswith(" - blocked shots") or "blocked shots" in raw_norm: return "Blocked Shots"
        if raw_norm.endswith(" - hits"): return "Hits"

    # Global/Generic fallbacks (use with caution)
    if "double double" in raw_norm: return "Double-Double"
    if "triple double" in raw_norm: return "Triple-Double"
    if "first basket" in raw_norm: return "First Basket"

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

# ── Multi-runner "milestone" markets (e.g. "To Score 30+ Points") ──
# Primary: "[Player] To [Record|Score|Hit|Get|Make] [N+] [stat]"
_MULTI_RUNNER_RE = re.compile(
    r"^(?:(?:a\s+|the\s+)?player\s+)?"
    r"to\s+(?:record|hit|score|get|achieve|reach|make|notch)\s+"
    r"(?:a\s+|an\s+)?(\d+\+\s*)?(.+?)$",
    re.IGNORECASE,
)
# Secondary: "N+ Stat" (e.g. "1+ Made Threes", "10+ Made Threes")
_MILESTONE_NUM_FIRST_RE = re.compile(r"^(\d+)\+\s+(.+)$", re.IGNORECASE)
# Tertiary: "Stat N+" (digit at end)
_MILESTONE_NUM_LAST_RE = re.compile(r"^(.+?)\s+(\d+)\+$", re.IGNORECASE)

# Market stat → PrizePicks stat type
_MULTI_RUNNER_MAP = {
    # NBA combos (check longest keys first via sorted-by-length partial match)
    "pts + reb + ast":    "Pts+Rebs+Asts",
    "pts+reb+ast":        "Pts+Rebs+Asts",
    "pts + reb":          "Pts+Rebs",
    "pts+reb":            "Pts+Rebs",
    "pts + ast":          "Pts+Asts",
    "pts+ast":            "Pts+Asts",
    "reb + ast":          "Rebs+Asts",
    "reb+ast":            "Rebs+Asts",
    "double double":      "Double-Double",
    "triple double":      "Triple-Double",

    # NBA individual
    "point":              "Points",
    "points":             "Points",
    "rebound":            "Rebounds",
    "rebounds":           "Rebounds",
    "assist":             "Assists",
    "assists":            "Assists",
    "made three":         "3-PT Made",
    "made threes":        "3-PT Made",
    "three":              "3-PT Made",
    "threes":             "3-PT Made",
    "three point field goals": "3-PT Made",
    "steal":              "Steals",
    "steals":             "Steals",
    "block":              "Blocked Shots",
    "blocks":             "Blocked Shots",
    "blocked shot":       "Blocked Shots",
    "blocked shots":      "Blocked Shots",
    "turnover":           "Turnovers",
    "turnovers":          "Turnovers",
    "first basket":       "First Basket",

    # MLB batter
    "hit":                "Hits",
    "hits":               "Hits",
    "single":             "Singles",
    "singles":            "Singles",
    "double":             "Doubles",
    "doubles":            "Doubles",
    "triple":             "Triples",
    "triples":            "Triples",
    "home run":           "Home Runs",
    "home runs":          "Home Runs",
    "rbi":                "RBIs",
    "rbis":               "RBIs",
    "run":                "Runs",
    "runs":               "Runs",
    "run scored":         "Runs",
    "runs scored":        "Runs",
    "total base":         "Total Bases",
    "total bases":        "Total Bases",
    "stolen base":        "Stolen Bases",
    "stolen bases":       "Stolen Bases",
    "hits + runs + rbis": "Hits+Runs+RBIs",
    "hits+runs+rbis":     "Hits+Runs+RBIs",
    "walk":               "Walks",
    "walks":              "Walks",
    "strikeout":          "Hitter Strikeouts",
    "strikeouts":         "Hitter Strikeouts",
    "extra base hit":     "Extra Base Hits",
    "extra base hits":    "Extra Base Hits",

    # NHL
    "shot on goal":       "Shots on Goal",
    "shots on goal":      "Shots on Goal",
    "shot":               "Shots on Goal",
    "shots":              "Shots on Goal",
    "save":               "Saves",
    "saves":              "Saves",
    "power play point":   "Power Play Points",
    "power play points":  "Power Play Points",
    "goal":               "Goals",
    "goals":              "Goals",
    "anytime goalscorer": "Goals",
    "goalscorer":         "Goals",
    "goal scorer":        "Goals",

    # Soccer
    "shots on target":    "Shots On Target",
    "shot on target":     "Shots On Target",
    "sot":                "Shots On Target",
    "total shots":        "Shots",
    "total shot":         "Shots",
    "goalie save":        "Goalie Saves",
    "goalie saves":       "Goalie Saves",
    "goalkeeper saves":   "Goalie Saves",
    "pass":               "Passes Attempted",
    "passes":             "Passes Attempted",
    "tackle":             "Tackles",
    "tackles":            "Tackles",
    "cross":              "Crosses",
    "crosses":            "Crosses",
    "clearance":          "Clearances",
    "clearances":         "Clearances",

    # Generic
    "points-assists":     "Pts+Asts",
}

# Detects whether the suffix after "Player - " looks like a milestone.
_MILESTONE_SUFFIX_RE = re.compile(
    r"^(?:to\s+(?:score|record|hit|get|make|achieve|reach)|\d+\+\s)",
    re.IGNORECASE,
)

def _split_player_milestone(mkt_name: str) -> tuple[Optional[str], str]:
    """If mkt_name is "Player Name - <milestone>", return (player, milestone);
    otherwise (None, mkt_name)."""
    if " - " not in mkt_name:
        return (None, mkt_name)
    player, suffix = mkt_name.split(" - ", 1)
    suffix = suffix.strip()
    if _MILESTONE_SUFFIX_RE.match(suffix):
        return (player.strip(), suffix)
    return (None, mkt_name)

def _parse_multi_runner_market(mkt_name: str, league: str = "") -> Optional[tuple[str, float]]:
    """Parse a multi-runner milestone market name into (stat, line)."""
    s = mkt_name.strip()
    # Strip optional "Player Name - " prefix if suffix looks milestone-y
    _, s = _split_player_milestone(s)

    m = _MULTI_RUNNER_RE.match(s)
    if m:
        threshold_str = (m.group(1) or "").strip().rstrip("+").strip()
        stat_part = m.group(2).strip()
    else:
        m2 = _MILESTONE_NUM_FIRST_RE.match(s)
        if m2:
            threshold_str = m2.group(1)
            stat_part = m2.group(2).strip()
        else:
            m3 = _MILESTONE_NUM_LAST_RE.match(s)
            if m3:
                stat_part = m3.group(1).strip()
                threshold_str = m3.group(2)
            else:
                # Binary soccer props (no threshold)
                low = s.lower()
                if low in ("anytime goalscorer", "anytime goal scorer",
                           "goalscorer", "goal scorer", "player to score",
                           "to score", "to score a goal"):
                    return ("Goals", 0.5)
                return None

    threshold = int(threshold_str) if threshold_str else 1
    stat_raw = stat_part.lower()
    stat_norm = stat_raw.rstrip("s")

    pp_stat = _MULTI_RUNNER_MAP.get(stat_raw) or _MULTI_RUNNER_MAP.get(stat_norm)
    if not pp_stat:
        sorted_keys = sorted(_MULTI_RUNNER_MAP.keys(), key=len, reverse=True)
        for key in sorted_keys:
            if key in stat_raw:
                pp_stat = _MULTI_RUNNER_MAP[key]
                break

    # Soccer overrides: "Shots" means open shots, not on-goal; "Saves" means goalie saves
    if league == "SOCCER" and pp_stat == "Shots on Goal":
        pp_stat = "Shots"
    if league == "SOCCER" and pp_stat == "Saves":
        pp_stat = "Goalie Saves"

    if not pp_stat:
        return None

    return (pp_stat, threshold - 0.5)

def _extract_props_from_json(data: dict, league: str) -> list[FanDuelProp]:
    """Parse FanDuel API JSON response."""
    props: list[FanDuelProp] = []

    try:
        attachments = data.get("attachments", {})
        markets_raw = attachments.get("markets", {})
        events_raw  = attachments.get("events",  {})

        player_by_event: dict[str, str] = {}
        time_by_event: dict[str, str] = {}
        for ev_id, ev in events_raw.items():
            name = ev.get("name", "") or ev.get("teamName", "")
            player_by_event[str(ev_id)] = name
            time_by_event[str(ev_id)] = ev.get("openDate", "")

        for mkt_id, mkt in markets_raw.items():
            mkt_name     = mkt.get("marketName", "") or mkt.get("marketType", "")
            market_type  = mkt.get("marketType", "")
            
            event_id = str(mkt.get("eventId", ""))
            start_time = time_by_event.get(event_id, "")

            if market_type in _GAME_LEVEL_TYPES:
                continue
                
            mkt_lower = mkt_name.lower()
            if any(x in mkt_lower for x in [
                "1st period", "2nd period", "3rd period",
                "1st quarter", "2nd quarter", "3rd quarter", "4th quarter",
                "1st qtr", "2nd qtr", "3rd qtr", "4th qtr",
                "1st half", "2nd half",
                " inning", "first pitch",
                "game specials", "team total",
                "puck line", "run line", "spread betting",
                "will there be", "moneyline",
                "first goal scorer", "last goal scorer",
                "correct score", "odd / even", "odd/even",
                "both teams to score",
            ]):
                continue

            # ── Multi-runner / Milestone markets ──
            multi = _parse_multi_runner_market(mkt_name, league)
            is_alt = " - alt" in mkt_name.lower() or "alternative" in mkt_name.lower()

            # Detect player-prefixed milestones: "Adem Bona - To Score 10+ Points"
            player_prefix, _milestone_suffix = _split_player_milestone(mkt_name)

            if multi or is_alt:
                runners = mkt.get("runners", [])
                for runner in runners:
                    runner_raw = runner.get("runnerName", "").strip()
                    if not runner_raw: continue
                    if "/" in runner_raw: continue

                    if multi:
                        pp_stat, line = multi
                        if player_prefix:
                            # Player fixed in market name; runners are Yes/No / Over/Under — keep only positive side
                            if runner_raw.lower() in ("no", "under"):
                                continue
                            player_name = player_prefix
                        else:
                            player_name = runner_raw
                    else:
                        m_th = re.search(r"(\d+)\+", runner_raw)
                        if not m_th: continue
                        threshold = int(m_th.group(1))
                        line = threshold - 0.5

                        pp_stat = _normalize_prop_type(mkt_name, league)
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
                        start_time=start_time,
                    ))
                continue

            normalized = _normalize_prop_type(mkt_name, league) or _normalize_prop_type(market_type, league)
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
                    start_time=start_time,
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
        "player-performance-doubles", "first-basket",
        "player-steals", "player-blocks", "player-turnovers",
        "player-scoring-combos", "player-rebounds-assists",
        "player-points-rebounds", "player-points-assists",
        "player-steals-blocks", "player-double-double",
        "player-triple-double", "player-fantasy-score",
    ],
    "NCAAB": [
        "player-points", "player-rebounds", "player-assists",
        "player-threes", "player-props", "player-combos",
        "player-defense",
    ],
    "NHL": [
        "shots", "goalies", "goals", "points-assists",
        "player-props", "alternative-handicaps",
        "power-play-points", "player-shots", "player-saves",
        "player-goals", "player-assists", "player-points",
        "goalscorer", "goal-scorer",
    ],
    "MLB": [
        "pitcher-props", "batter-props", "player-props",
        "home-runs", "strikeouts", "hits", "runs", "rbis",
        "total-bases", "stolen-bases", "outs-recorded",
        "earned-runs-allowed", "earned-runs", "walks-allowed",
        "walks-issued", "walks", "pitcher-strikeouts",
        "hits-+-runs-+-rbis", "hits-runs-rbis", "to-record-a-hit",
        "hits-allowed", "singles", "doubles", "triples",
        "alternative-run-lines", "extra-base-hits",
        "batter-home-runs", "batter-hits", "batter-runs",
        "batter-rbis", "batter-total-bases", "batter-strikeouts",
        "pitcher-outs", "pitcher-hits-allowed", "pitcher-walks",
        "pitcher-earned-runs",
    ],
    "SOCCER": [
        "player-props", "goalscorer", "shots", "cards", "assists", "passes",
        "shots-on-target", "tackles", "crosses", "clearances", "saves",
        "player-shots", "player-goals", "player-assists",
        "player-shots-on-target", "player-passes", "player-tackles",
        "player-crosses", "player-saves", "to-score", "first-goalscorer",
    ],
}

async def _scrape_league(client: httpx.AsyncClient, league: str) -> list[FanDuelProp]:
    all_props = []

    logger.info("FanDuel [%s]: fetching events (Phase 1)", league)
    if league == "SOCCER":
        nav_urls = [
            f"https://sbapi.nj.sportsbook.fanduel.com/api/content-managed-page?page=SPORT&eventTypeId=1&_ak={FD_AK_TOKEN}"
        ]
    else:
        nav_urls = [f"https://sbapi.nj.sportsbook.fanduel.com/api/content-managed-page?page=CUSTOM&customPageId={league.lower()}&_ak={FD_AK_TOKEN}"]

    event_ids = set()
    for nav_url in nav_urls:
        try:
            r = await client.get(nav_url, headers=FD_HEADERS, timeout=15)
            if r.status_code == 200:
                data = r.json()
                events = data.get("attachments", {}).get("events", {})
                event_ids.update(events.keys())
        except Exception as e:
            logger.error("FanDuel [%s]: phase 1 error on %s: %s", league, nav_url, e)

    event_ids = list(event_ids)
    if not event_ids:
        logger.error("FanDuel [%s]: phase 1 returned 0 events", league)
        return []

    logger.info("FanDuel [%s]: found %d active events", league, len(event_ids))

    TABS = LEAGUE_TABS.get(league.upper(), [
        "player-props", "player-points", "player-rebounds",
        "player-assists", "player-threes",
    ])
    
    # Lower concurrency cuts peak memory — each in-flight request holds a
    # response JSON (tens of KB each) in RAM until parsed. 2 gives us enough
    # throughput for the free tier without stacking up response buffers.
    sem = asyncio.Semaphore(2)

    async def _safe_fetch(eid: str, tab: str):
        async with sem:
            return await _fetch_event_tab(client, league, eid, tab)

    logger.info("FanDuel [%s]: fetching %d tabs for %d events (Phase 2)", league, len(TABS), len(event_ids))
    tasks = [
        asyncio.create_task(_safe_fetch(eid, tab))
        for eid in event_ids
        for tab in TABS
    ]

    # Deduplicate as we stream — avoids holding N intermediate lists and the
    # final `all_props` concatenation at the same time.
    seen: set = set()
    unique: list[FanDuelProp] = []
    for coro in asyncio.as_completed(tasks):
        try:
            res = await coro
        except Exception:
            continue
        if not isinstance(res, list):
            continue
        for p in res:
            key = (p.player_name, p.prop_type, p.line)
            if key in seen:
                continue
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


if __name__ == "__main__":
    # Test script
    logging.basicConfig(level=logging.INFO)
    test_leagues = {"NBA": True, "MLB": True, "NHL": True, "NCAAB": True}
    res = scrape_fanduel(test_leagues)

    # Group by league and prop type for summary
    from collections import Counter
    by_league: dict[str, Counter] = {}
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
