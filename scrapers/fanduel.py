"""
FanDuel scraper using Playwright with network interception.

Strategy:
  1. Open FanDuel in a headed Chromium browser (bypasses Cloudflare).
  2. Intercept XHR/Fetch API responses from FanDuel's internal odds API.
  3. Parse the raw JSON directly — immune to UI/CSS changes.
  4. Navigate to each league's Player Props tab and collect intercepted data.
"""
import asyncio
import json
import logging
import pathlib
import re
from typing import Optional

from playwright.async_api import async_playwright, Page, Response
from playwright_stealth import Stealth

from config import HEADLESS, FANDUEL_URLS, ACTIVE_LEAGUES
from engine.constants import PROP_TYPE_MAP
from engine.matcher import FanDuelProp

logger = logging.getLogger(__name__)

# FanDuel internal API URL patterns to intercept
FD_API_PATTERNS = [
    re.compile(r"sportsbook-api.*events", re.IGNORECASE),
    re.compile(r"sbapi.*markets",         re.IGNORECASE),
    re.compile(r"api\.fanduel\.com",      re.IGNORECASE),
    re.compile(r"/api/content/",          re.IGNORECASE),
    re.compile(r"sb-content",             re.IGNORECASE),
    re.compile(r"fixture-markets",        re.IGNORECASE),
]

# How long to wait for network responses after navigating to the props tab (ms)
NETWORK_IDLE_TIMEOUT = 12_000
NAV_TIMEOUT = 30_000


def _normalize_prop_type(raw: str) -> Optional[str]:
    return PROP_TYPE_MAP.get(raw.lower().strip())


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
    FanDuel responses vary by endpoint; this handles the common nested structure:
      attachments → markets → [] → runners → [] → handicap / winRunnerOdds
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
                handicap_raw = runner.get("handicap") or runner.get("runnerName", "")
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

                player_name = (
                    runner.get("selectionName")
                    or runner.get("runnerName", "").split("(")[0].strip()
                )
                # Try resolving from event
                event_id = str(mkt.get("eventId", ""))
                if event_id and event_id in player_by_event:
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


async def _scrape_league(page: Page, league: str, url: str) -> list[FanDuelProp]:
    """Navigate to a league's player props page and collect intercepted API data."""
    collected_props: list[FanDuelProp] = []
    intercepted_jsons: list[dict] = []

    async def handle_response(response: Response):
        resp_url = response.url
        content_type = response.headers.get("content-type", "")
        # Capture all JSON responses (not just pattern-matched) for debug discovery
        if "json" not in content_type and "javascript" not in content_type:
            return
        try:
            body = await response.body()
            if not body or len(body) < 100:
                return
            data = json.loads(body)
            # Log all intercepted API URLs for debugging
            logger.info("FanDuel [%s] intercepted: %s (%d bytes)", league, resp_url[:120], len(body))

            # Save to data/ dir for offline analysis
            import hashlib
            slug = hashlib.md5(resp_url.encode()).hexdigest()[:8]
            dump_path = pathlib.Path(__file__).parent.parent / "data" / f"fd_{league}_{slug}.json"
            dump_path.parent.mkdir(exist_ok=True)
            dump_path.write_text(json.dumps(data, indent=2)[:500_000])  # cap at 500KB

            intercepted_jsons.append(data)
        except Exception:
            pass

    page.on("response", handle_response)

    try:
        logger.info("FanDuel [%s]: navigating to %s", league, url)
        await page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)

        # Try to click "Player Props" tab if present
        try:
            props_tab = page.get_by_text(re.compile(r"player props", re.IGNORECASE))
            if await props_tab.count() > 0:
                await props_tab.first.click()
                logger.info("FanDuel [%s]: clicked Player Props tab", league)
        except Exception:
            pass

        # Wait for network to settle and API calls to fire
        try:
            await page.wait_for_load_state("networkidle", timeout=NETWORK_IDLE_TIMEOUT)
        except Exception:
            pass

        # Small extra delay to catch any lazy-loaded API calls
        await asyncio.sleep(3)

    except Exception as e:
        logger.error("FanDuel [%s]: navigation error: %s", league, e)
    finally:
        page.remove_listener("response", handle_response)

    # Parse all intercepted JSON blobs
    for data in intercepted_jsons:
        props = _extract_props_from_json(data, league)
        collected_props.extend(props)

    # Deduplicate by (player_name, prop_type, line, side)
    seen = set()
    unique: list[FanDuelProp] = []
    for p in collected_props:
        key = (p.player_name, p.prop_type, p.line)
        if key not in seen:
            seen.add(key)
            unique.append(p)

    logger.info("FanDuel [%s]: %d unique props captured", league, len(unique))
    return unique


async def _scrape_all_leagues(active_leagues: dict | None = None) -> list[FanDuelProp]:
    leagues = active_leagues if active_leagues is not None else ACTIVE_LEAGUES
    all_props: list[FanDuelProp] = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=HEADLESS,
            args=[
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-infobars",
            ],
        )
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1440, "height": 900},
            locale="en-US",
        )

        for league, active in leagues.items():
            if not active:
                continue
            url = FANDUEL_URLS.get(league)
            if not url:
                continue

            page = await context.new_page()
            await Stealth().apply_stealth_async(page)

            try:
                props = await _scrape_league(page, league, url)
                all_props.extend(props)
            finally:
                await page.close()

        await browser.close()

    return all_props


def scrape_fanduel(active_leagues: dict | None = None) -> list[FanDuelProp]:
    """Synchronous entry point — runs the async Playwright scraper."""
    return asyncio.run(_scrape_all_leagues(active_leagues))
