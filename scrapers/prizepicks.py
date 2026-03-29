"""
PrizePicks scraper using their public (undocumented) API.
No authentication required.
"""
import logging
import time
from typing import Optional

import httpx

from config import PRIZEPICKS_LEAGUE_IDS, ACTIVE_LEAGUES
from engine.matcher import PrizePickLine

logger = logging.getLogger(__name__)

PP_BASE = "https://api.prizepicks.com/projections"
PP_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept":          "application/json",
    "Referer":         "https://app.prizepicks.com/",
    "Origin":          "https://app.prizepicks.com",
}


def _fetch_league(client: httpx.Client, league: str, league_id: int) -> list[PrizePickLine]:
    """Fetch all projections for a single league."""
    lines: list[PrizePickLine] = []
    page = 1

    while True:
        try:
            resp = client.get(
                PP_BASE,
                params={"league_id": league_id, "per_page": 250, "page": page},
                headers=PP_HEADERS,
                timeout=20,
            )
            resp.raise_for_status()
        except httpx.HTTPError as e:
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

            lines.append(PrizePickLine(
                league=league,
                player_name=player_name,
                stat_type=stat_type,
                line_score=line_score,
                player_id=player_id,
                start_time=start_time or "",
            ))

        # Pagination
        meta = data.get("meta", {})
        total_pages = meta.get("last_page") or meta.get("total_pages") or 1
        if page >= total_pages or not projections:
            break
        page += 1

    logger.info("PrizePicks [%s]: %d lines fetched", league, len(lines))
    return lines


def scrape_prizepicks(active_leagues: dict | None = None) -> list[PrizePickLine]:
    """Scrape all active leagues from PrizePicks API."""
    leagues = active_leagues if active_leagues is not None else ACTIVE_LEAGUES
    all_lines: list[PrizePickLine] = []

    with httpx.Client() as client:
        for league, active in leagues.items():
            if not active:
                continue
            league_id = PRIZEPICKS_LEAGUE_IDS.get(league)
            if league_id is None:
                continue
            lines = _fetch_league(client, league, league_id)
            all_lines.extend(lines)
            time.sleep(1.5)  # rate-limit buffer between leagues

    return all_lines
