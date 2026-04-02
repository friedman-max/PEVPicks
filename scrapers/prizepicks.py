"""
PrizePicks scraper using their public (undocumented) API.
No authentication required.
"""
import logging
import time
import uuid
from typing import Optional

from curl_cffi import requests

from config import PRIZEPICKS_LEAGUE_IDS, ACTIVE_LEAGUES, SCRAPE_ALL_LEAGUES
from engine.matcher import PrizePickLine

logger = logging.getLogger(__name__)

PP_BASE = "https://partner-api.prizepicks.com/projections"
PP_HEADERS = {
    "Accept":          "application/json",
    "Referer":         "https://app.prizepicks.com/",
    "Origin":          "https://app.prizepicks.com",
    "User-Agent":      "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1",
    "x-device-id":     "73d6f789-53b1-4b13-97cc-f91cc6d11111",
}

def _request_with_retry(session: requests.Session, method: str, url: str, **kwargs) -> requests.Response:
    """Make an HTTP request with retries for status 429/403."""
    max_retries = 3
    base_delay = 10
    for attempt in range(max_retries):
        try:
            resp = session.request(method, url, **kwargs)
            if resp.status_code in [429, 403]:
                delay = base_delay * (3 ** attempt)
                logger.warning("PrizePicks %d Error - retrying in %d seconds...", resp.status_code, delay)
                time.sleep(delay)
                continue
            resp.raise_for_status()
            return resp
        except Exception as e:
            if attempt == max_retries - 1:
                raise e
            time.sleep(base_delay)
    raise Exception("Max retries reached")


def _fetch_league(session: requests.Session, league: str, league_id: int) -> list[PrizePickLine]:
    """Fetch all projections for a single league."""
    lines: list[PrizePickLine] = []
    page = 1

    while True:
        try:
            resp = _request_with_retry(
                session, 
                "GET",
                PP_BASE,
                params={"league_id": league_id, "per_page": 250, "page": page},
                headers=PP_HEADERS,
                timeout=20,
            )
        except Exception as e:
            logger.error("PrizePicks HTTP error for %s page %d: %s", league, page, e)
            break

        data = resp.json()
        projections = data.get("data", [])
        included   = data.get("included", [])

        # Build player_id → player_name lookup from included resources
        player_map: dict[str, str] = {}
        for item in included:
            if item.get("type") == "new_player":
                pid = item.get("id", "")
                name = item.get("attributes", {}).get("display_name", "")
                if pid and name:
                    player_map[pid] = name

        for proj in projections:
            if proj.get("type") != "projection":
                continue
            attrs = proj.get("attributes", {})
            # Resolve player name
            rel = proj.get("relationships", {})
            player_rel = rel.get("new_player", {}).get("data", {})
            player_id  = player_rel.get("id", proj.get("id", ""))
            player_name = player_map.get(player_id, attrs.get("description", ""))

            stat_type  = attrs.get("stat_type", "")
            line_score_raw = attrs.get("line_score")
            odds_type  = attrs.get("odds_type", "standard")
            start_time = attrs.get("start_time", "")
            if not player_name or not stat_type or line_score_raw is None:
                continue
            # Only keep standard lines (filter out demons and goblins)
            if odds_type != "standard":
                continue

            try:
                line_score = float(line_score_raw)
            except (ValueError, TypeError):
                continue

            if line_score % 1 == 0:
                # Whole number -> split into restrictive Over and Under lines to penalize pushes
                lines.append(PrizePickLine(
                    league=league,
                    player_name=player_name,
                    stat_type=stat_type,
                    line_score=line_score + 0.5,
                    player_id=player_id,
                    start_time=start_time or "",
                    side="over",
                ))
                lines.append(PrizePickLine(
                    league=league,
                    player_name=player_name,
                    stat_type=stat_type,
                    line_score=line_score - 0.5,
                    player_id=player_id,
                    start_time=start_time or "",
                    side="under",
                ))
            else:
                lines.append(PrizePickLine(
                    league=league,
                    player_name=player_name,
                    stat_type=stat_type,
                    line_score=line_score,
                    player_id=player_id,
                    start_time=start_time or "",
                    side="both",
                ))

        # Pagination
        meta = data.get("meta", {})
        total_pages = meta.get("last_page") or meta.get("total_pages") or 1
        if page >= total_pages or not projections:
            break
        page += 1
        time.sleep(3.0)  # Moderate intra-league pagination delay

    logger.info("PrizePicks [%s]: %d lines fetched", league, len(lines))
    return lines


def scrape_prizepicks(active_leagues: dict | None = None) -> list[PrizePickLine]:
    """Scrape specific active leagues from PrizePicks API."""
    all_lines: list[PrizePickLine] = []
    
    # Use the 4 core leagues by default
    target_leagues = active_leagues if active_leagues is not None else ACTIVE_LEAGUES
    
    with requests.Session(impersonate="safari17_2_ios") as session:
        for league_name, is_active in target_leagues.items():
            if not is_active:
                continue
            
            league_id = PRIZEPICKS_LEAGUE_IDS.get(league_name)
            if not league_id:
                continue
                
            lines = _fetch_league(session, league_name, league_id)
            all_lines.extend(lines)
            time.sleep(5.0)

    return all_lines
