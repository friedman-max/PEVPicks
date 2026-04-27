"""
FastAPI backend with APScheduler auto-refresh.

Endpoints:
  GET  /api/bets        - All current +EV bets (sorted by ind_ev_pct desc)
  GET  /api/status      - Scrape status, last/next refresh time
  POST /api/slip        - Calculate slip EV for selected bet IDs
  GET  /api/config      - Current runtime config
  POST /api/config      - Update config (interval, min_ev, leagues)
"""
import gc
import hashlib
import json
import logging
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

# Aggressive GC thresholds — scrape cycles create lots of short-lived objects.
# Lower thresholds trigger collection earlier, keeping RSS closer to working-set
# size on the 512MB Render free tier.
gc.set_threshold(500, 5, 5)


def _intern(s):
    """Intern strings so repeated league/stat_type/side values share storage."""
    return sys.intern(s) if isinstance(s, str) else s

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.jobstores.base import JobLookupError
from fastapi import FastAPI, HTTPException, Depends, Request, Response
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from engine.ev_calculator import reload_calibration
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import statistics
from web.auth import get_current_user, get_current_user_optional

import config as cfg
from engine.ev_calculator import BetResult, calculate_slip, evaluate_match
from engine.matcher import match_props
from engine.backtest import BacktestLogger, make_bet_key
from engine.results_checker import ESPNResultsChecker
from engine.clv_checker import CLVTracker
from engine.persistence import sync_state_to_supabase, load_state_from_supabase
from engine.devig import (
    american_to_implied as _american_to_implied,
    devig_single_sided_scaled,
    prob_to_american as _prob_to_american,
    revigg_power,
)
from scrapers.fanduel import scrape_fanduel
from scrapers.prizepicks import scrape_prizepicks
from scrapers.draftkings import scrape_draftkings
from scrapers.pinnacle import scrape_pinnacle

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="CoreProp")

# Compress JSON responses >= 1KB. Typical line payloads are 100-500KB raw and
# compress 6-10x with gzip, dramatically reducing network time and response-
# buffer memory on the 512MB tier.
app.add_middleware(GZipMiddleware, minimum_size=1024)

# Results checker / CLV singletons (stateless, run in background via service role)
_results_checker  = ESPNResultsChecker()
_clv_tracker      = CLVTracker()

# ---------------------------------------------------------------------------
# In-memory state
# ---------------------------------------------------------------------------

_lock = threading.RLock()

_state = {
    "bets":          [],        # list[dict] — serialized BetResult
    "bet_map":       {},        # bet_id -> BetResult (for slip calc)
    "matches":       [],        # list[dict] — unfiltered combined lines
    "pp_lines":      [],        # list[dict] — raw PrizePicks lines
    "fd_lines":      [],        # list[dict] — raw FanDuel lines
    "dk_lines":      [],        # list[dict] — raw DraftKings lines
    "pin_lines":     [],        # list[dict] — raw Pinnacle lines
    "last_refresh":  None,      # datetime | None
    "next_refresh":  None,      # datetime | None
    "is_scraping":   False,
    "is_scraping_pp": False,
    "is_scraping_fd": False,
    "is_scraping_dk": False,
    "is_scraping_pin": False,
    "scrape_errors": {},        # league -> error str | None
    "interval_min":  5,
    "min_ev_pct":    -10.0,         # fallback default
    "active_leagues": dict(cfg.ACTIVE_LEAGUES), # fallback default
}

# Pre-serialized JSON response cache. Populated once at the end of each scrape
# cycle so GET endpoints can return bytes directly without a per-request
# json.dumps (which on the 512MB tier was the largest transient allocation).
#
# Values are bytes keyed by dataset. `etag` is a weak hash of last_refresh so
# clients can 304-short-circuit unchanged polls.
_payload_cache = {
    "bets":      None,   # bytes | None
    "matches":   None,
    "pp_lines":  None,
    "fd_lines":  None,
    "dk_lines":  None,
    "pin_lines": None,
    "core":      None,   # bootstrap/core — bets + meta
    "etag":      None,   # str | None
}
_payload_lock = threading.Lock()

# Per-user analytics cache. /api/analytics is the slowest endpoint — it does
# 3 legs-table scans + 1 slips scan, all unbounded. But the underlying data
# only changes when a slip is added/resolved/deleted, which is rare relative
# to tab clicks. A short TTL makes repeat access essentially free.
_analytics_cache: dict = {}       # user_id -> (monotonic_ts, data_dict)
_analytics_cache_lock = threading.Lock()
_ANALYTICS_TTL_SEC = 300.0


def _invalidate_analytics_cache(user_id: Optional[str] = None):
    """Drop a specific user's cached analytics (after add/delete slip), or
    everyone's (pass None) if global state changed."""
    with _analytics_cache_lock:
        if user_id is None:
            _analytics_cache.clear()
        else:
            _analytics_cache.pop(user_id, None)


def _build_etag(last_refresh) -> str:
    """Stable ETag derived from the last_refresh timestamp. Weak (W/) because
    gzip compression can mutate bytes at the transport layer."""
    seed = last_refresh.isoformat() if isinstance(last_refresh, datetime) else str(last_refresh)
    h = hashlib.md5(seed.encode("utf-8")).hexdigest()[:16]
    return f'W/"{h}"'


def _json_bytes(obj) -> bytes:
    """Compact JSON encoding. separators=(',',':') trims ~15% off list payloads."""
    return json.dumps(obj, separators=(",", ":"), default=str).encode("utf-8")


def _serialize_one(key: str, data, last_iso: str, interval_min=None) -> bytes:
    """Encode a single dataset's response body as JSON bytes."""
    if key == "bets":
        payload = {"bets": data, "total": len(data), "is_scraping": False, "last_refresh": last_iso}
    elif key == "matches":
        payload = {"matches": data, "total": len(data), "is_scraping": False, "last_refresh": last_iso}
    elif key == "core":
        payload = {"bets": data, "total": len(data), "is_scraping": False, "last_refresh": last_iso, "interval_min": interval_min}
    else:  # pp_lines / fd_lines / dk_lines / pin_lines — line datasets
        payload = {"lines": data, "total": len(data), "is_scraping": False, "last_refresh": last_iso}
    return _json_bytes(payload)


def _refresh_payload_cache(
    serialized_bets,
    serialized_matches,
    serialized_pp,
    serialized_fd,
    serialized_dk,
    serialized_pin,
    last_refresh,
    interval_min,
):
    """Build all pre-serialized response bytes in one pass. Called from the
    pipeline with the just-built state so we never dumps() per-request."""
    last_iso = last_refresh.isoformat() if isinstance(last_refresh, datetime) else last_refresh
    etag = _build_etag(last_refresh)

    # Serialize outside the lock — encoding is CPU-bound and doesn't touch
    # shared state. Holding _payload_lock across six dumps() on the 512MB tier
    # was serializing concurrent GETs against the cache swap.
    bets_bytes    = _serialize_one("bets", serialized_bets, last_iso)
    matches_bytes = _serialize_one("matches", serialized_matches, last_iso)
    pp_bytes      = _serialize_one("pp_lines", serialized_pp, last_iso)
    fd_bytes      = _serialize_one("fd_lines", serialized_fd, last_iso)
    dk_bytes      = _serialize_one("dk_lines", serialized_dk, last_iso)
    pin_bytes     = _serialize_one("pin_lines", serialized_pin, last_iso)
    core_bytes    = _serialize_one("core", serialized_bets, last_iso, interval_min)

    with _payload_lock:
        _payload_cache["bets"]      = bets_bytes
        _payload_cache["matches"]   = matches_bytes
        _payload_cache["pp_lines"]  = pp_bytes
        _payload_cache["fd_lines"]  = fd_bytes
        _payload_cache["dk_lines"]  = dk_bytes
        _payload_cache["pin_lines"] = pin_bytes
        _payload_cache["core"]      = core_bytes
        _payload_cache["etag"]      = etag


def _update_one_payload(key: str, data, last_refresh):
    """Serialize and swap a single cache entry. Used by per-book refresh
    endpoints so they don't re-encode the other five datasets (which was
    allocating ~3–5MB transient buffers per unrelated book refresh)."""
    last_iso = last_refresh.isoformat() if isinstance(last_refresh, datetime) else last_refresh
    body = _serialize_one(key, data, last_iso)
    etag = _build_etag(last_refresh)
    with _payload_lock:
        _payload_cache[key] = body
        _payload_cache["etag"] = etag


def _rebuild_cache_from_state():
    """Rebuild the payload cache from whatever's currently in _state. Used
    at startup after seeding from Supabase."""
    with _lock:
        _refresh_payload_cache(
            _state["bets"],
            _state["matches"],
            _state["pp_lines"],
            _state["fd_lines"],
            _state["dk_lines"],
            _state["pin_lines"],
            _state["last_refresh"],
            _state["interval_min"],
        )


def _cached_response(key: str, request: Request) -> Response:
    """Serve a pre-serialized JSON payload with ETag/304 short-circuit. If the
    cache is empty (pre-first-scrape), returns an empty-shape JSON response."""
    with _payload_lock:
        body = _payload_cache.get(key)
        etag = _payload_cache.get("etag")

    if body is None:
        # Cold start — minimal empty shape, but still fast and cacheable.
        shape = {"bets": []} if key in ("bets", "core") else {"lines": [] if key != "matches" else None, "matches": []}
        empty = {
            "total": 0,
            "is_scraping": _state["is_scraping"],
            "last_refresh": _last_refresh_iso(),
        }
        if key == "core":
            empty["bets"] = []
            empty["interval_min"] = _state["interval_min"]
        elif key == "bets":
            empty["bets"] = []
        elif key == "matches":
            empty["matches"] = []
        else:
            empty["lines"] = []
        return JSONResponse(empty)

    if etag and request.headers.get("if-none-match") == etag:
        return Response(status_code=304, headers={"ETag": etag})

    headers = {"Cache-Control": "no-cache"}
    if etag:
        headers["ETag"] = etag
    return Response(content=body, media_type="application/json", headers=headers)


scheduler = BackgroundScheduler()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _compute_book_overround(props: list) -> float:
    """
    Compute the median overround (margin) for a book from its own both-sided
    props.  Returns the overround as a fraction (e.g. 0.07 for 7%).
    Falls back to 0.07 if there aren't enough both-sided lines.
    """
    _DEFAULT_OVERROUND = 0.07
    margins = []
    for p in props:
        if getattr(p, "both_sided", False) and p.over_odds is not None and p.under_odds is not None:
            impl_o = _american_to_implied(p.over_odds)
            impl_u = _american_to_implied(p.under_odds)
            margin = impl_o + impl_u - 1.0
            if 0 < margin < 0.25:  # sanity: ignore bad data
                margins.append(margin)
    if len(margins) >= 3:
        return statistics.median(margins)
    return _DEFAULT_OVERROUND


def _display_odds(book, side: str, book_overround: float = 0.07):
    """
    Return the American odds to display for a book on a given side.

    If the book only has the opposite side, derive the missing side using:
      1. Devig the available side → true probability
      2. Complement → true probability for missing side
      3. Re-vig BOTH sides using the inverse Power Method with the book's
         own median overround, producing realistic vigged odds that honor
         the favorite-longshot bias.

    Returns None if the book is None or has no odds at all.
    """
    if book is None:
        return None
    direct = book.over_odds if side == "over" else book.under_odds
    if direct is not None:
        return direct
    # Derive from opposite side using the book's own overround
    opposite = book.under_odds if side == "over" else book.over_odds
    if opposite is not None:
        # Step 1-2: devig available side, complement for missing side
        available_true = devig_single_sided_scaled(opposite)
        missing_true = 1.0 - available_true
        if missing_true <= 0 or missing_true >= 1:
            return None
        # Step 3: re-vig both sides with the book's observed overround
        if side == "over":
            vigged_over, vigged_under = revigg_power(missing_true, available_true, book_overround)
            return round(_prob_to_american(vigged_over), 2)
        else:
            vigged_over, vigged_under = revigg_power(available_true, missing_true, book_overround)
            return round(_prob_to_american(vigged_under), 2)
    return None


# ---------------------------------------------------------------------------
# Core pipeline
# ---------------------------------------------------------------------------

# Auto-logging has been removed to support multi-tenancy.
# Users must manually log slips from the UI.



def run_pipeline():
    """Public pipeline entry. Delegates to _run_pipeline_body so that all
    large per-cycle locals (tens of thousands of dicts) go out of scope and
    can be reclaimed before we force a GC."""
    with _lock:
        if _state["is_scraping"]:
            logger.info("Scrape already in progress, skipping.")
            return
        _state["is_scraping"] = True
    try:
        _run_pipeline_body()
    finally:
        with _lock:
            _state["is_scraping"] = False
        # Large locals from _run_pipeline_body are now unreachable. Force a
        # full GC so the memory is released to the OS (critical on 512MB tier).
        gc.collect()


def _run_pipeline_body():
    errors = {}
    try:
        with _lock:
            leagues = dict(_state["active_leagues"])

        import concurrent.futures
        
        logger.info("Pipeline: kicking off scrapers concurrently...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            future_pp = executor.submit(scrape_prizepicks, active_leagues=leagues)
            future_fd = executor.submit(scrape_fanduel, active_leagues=leagues)
            future_dk = executor.submit(scrape_draftkings, active_leagues=leagues)
            future_pin = executor.submit(scrape_pinnacle, active_leagues=leagues)

            try:
                pp_lines = future_pp.result()
                if len(pp_lines) == 0: errors["prizepicks"] = "Empty response"
            except Exception as e:
                logger.error(f"PrizePicks scraper failed: {e}")
                pp_lines, errors["prizepicks"] = [], "Exception"

            try:
                fd_props = future_fd.result()
                if len(fd_props) == 0: errors["fanduel"] = "Empty response"
            except Exception as e:
                logger.error(f"FanDuel scraper failed: {e}")
                fd_props, errors["fanduel"] = [], "Exception"

            try:
                dk_props = future_dk.result()
                if len(dk_props) == 0: errors["draftkings"] = "Empty response"
            except Exception as e:
                logger.error(f"DraftKings scraper failed: {e}")
                dk_props, errors["draftkings"] = [], "Exception"

            try:
                pin_props = future_pin.result()
                if len(pin_props) == 0: errors["pinnacle"] = "Empty response"
            except Exception as e:
                logger.error(f"Pinnacle scraper failed: {e}")
                pin_props, errors["pinnacle"] = [], "Exception"

        # If every source failed, skip the state update entirely so the UI
        # keeps the last good snapshot (avoids clearing screens on a bad cycle).
        if not pp_lines and not fd_props and not dk_props and not pin_props:
            logger.warning("Pipeline: all scrapers returned 0 — preserving previous state.")
            with _lock:
                _state["scrape_errors"] = errors
            return

        serialized_pp = []
        for l in pp_lines:
            lg = _intern(l.league)
            st = _intern(l.stat_type)
            if l.side == "both":
                common = {
                    "league": lg,
                    "player_name": l.player_name,
                    "stat_type": st,
                    "line_score": l.line_score,
                    "start_time": l.start_time,
                }
                serialized_pp.append({**common, "side": "over"})
                serialized_pp.append({**common, "side": "under"})
            else:
                serialized_pp.append({
                    "league": lg,
                    "player_name": l.player_name,
                    "stat_type": st,
                    "line_score": l.line_score,
                    "side": _intern(l.side),
                    "start_time": l.start_time,
                })

        from engine.devig import devig_power, devig_single_sided, prob_to_american

        def _serialize_book(props):
            out = []
            _OVER = sys.intern("over")
            _UNDER = sys.intern("under")
            for p in props:
                true_over, true_under = None, None
                if p.both_sided and p.over_odds is not None and p.under_odds is not None:
                    true_over, true_under = devig_power(p.over_odds, p.under_odds)
                else:
                    if p.over_odds is not None:
                        true_over = devig_single_sided(p.over_odds)
                    if p.under_odds is not None:
                        true_under = devig_single_sided(p.under_odds)
                lg = _intern(p.league)
                st = _intern(p.prop_type)
                start = getattr(p, "start_time", None)
                if p.over_odds is not None:
                    out.append({
                        "league": lg, "player_name": p.player_name, "stat_type": st,
                        "line_score": p.line, "side": _OVER, "line_odds": p.over_odds,
                        "true_odds": prob_to_american(true_over) if true_over else None,
                        "start_time": start,
                    })
                if p.under_odds is not None:
                    out.append({
                        "league": lg, "player_name": p.player_name, "stat_type": st,
                        "line_score": p.line, "side": _UNDER, "line_odds": p.under_odds,
                        "true_odds": prob_to_american(true_under) if true_under else None,
                        "start_time": start,
                    })
            return out

        serialized_fd = _serialize_book(fd_props)
        serialized_dk = _serialize_book(dk_props)
        serialized_pin = _serialize_book(pin_props)

        logger.info("Pipeline: matching %d PP lines vs %d FD, %d DK, %d Pinnacle props...", len(pp_lines), len(fd_props), len(dk_props), len(pin_props))
        matches = match_props(fd_props, dk_props, pp_lines, pin_props)

        # Compute each book's median overround from its own both-sided lines
        fd_margin = _compute_book_overround(fd_props)
        dk_margin = _compute_book_overround(dk_props)
        pin_margin = _compute_book_overround(pin_props)
        logger.info("Book overrounds: FD=%.2f%% DK=%.2f%% PIN=%.2f%%",
                     fd_margin * 100, dk_margin * 100, pin_margin * 100)

        from engine.devig import prob_to_american
        from engine.ev_calculator import BetResult
        from engine.consensus import compute_true_probability, books_from_match
        
        with _lock:
            min_ev = _state["min_ev_pct"]
        bets: list[BetResult] = []
        bet_book_odds: dict[str, dict] = {}  # bet_id -> {fd_odds, dk_odds, pin_odds}
        serialized_matches = []
        for m in matches:
            # At least one book must be present. We check line equality for the books that exist.
            # Nullify any books that don't match the PrizePicks line score exactly,
            # but don't discard the whole match if at least one book matches.
            if m.fd and m.pp.line_score != m.fd.line:
                m.fd = None
            if m.dk and m.pp.line_score != m.dk.line:
                m.dk = None
            if m.pin and m.pp.line_score != m.pin.line:
                m.pin = None

            if not m.fd and not m.dk and not m.pin:
                continue

            base = {
                "player_name": m.pp.player_name,
                "league": m.pp.league,
                "stat_type": m.pp.stat_type,
                "pp_line": m.pp.line_score,
                "fd_line": m.fd.line if m.fd else None,
                "dk_line": m.dk.line if m.dk else None,
                "pin_line": m.pin.line if m.pin else None,
                "start_time": m.pp.start_time,
            }

            pp_side = getattr(m.pp, "side", "both")

            # Build book odds list for the consensus engine
            match_books = books_from_match(m.fd, m.dk, m.pin)

            def _per_book_probs(side: str) -> dict:
                """Snapshot each book's devigged probability for `side` so the
                sharpness fitter (engine/sharpness_calibration.py) can later
                compare each book's price to the eventual closing line.
                Returns e.g. {"fanduel": 0.62, "draftkings": 0.61}."""
                from engine.devig import devig_power as _dp, devig_single_sided_scaled as _dss
                out: dict[str, float] = {}
                for name, bk in (("fanduel", m.fd), ("draftkings", m.dk), ("pinnacle", m.pin)):
                    if bk is None:
                        continue
                    prob = None
                    if bk.both_sided and bk.over_odds is not None and bk.under_odds is not None:
                        t_o, t_u = _dp(bk.over_odds, bk.under_odds)
                        prob = t_o if side == "over" else t_u
                    elif side == "over" and bk.over_odds is not None:
                        prob = _dss(bk.over_odds)
                    elif side == "under" and bk.under_odds is not None:
                        prob = _dss(bk.under_odds)
                    if prob is not None and 0.0 < prob < 1.0:
                        out[name] = round(float(prob), 4)
                return out

            def get_combined_true_odds(side):
                """Compute consensus true probability via the VWAP engine."""
                consensus_prob, worst_case_prob, meta = compute_true_probability(match_books, side)

                if consensus_prob is None:
                    return None, None, None

                # Find the best odds for display (includes derived complement odds)
                odds_list = [
                    o for o in [
                        _display_odds(m.fd, side, fd_margin),
                        _display_odds(m.dk, side, dk_margin),
                        _display_odds(m.pin, side, pin_margin),
                    ] if o is not None
                ]
                best_odds = max(odds_list) if odds_list else None

                # Use worst-case probability for EV decisions (most conservative)
                final_true_prob = worst_case_prob
                return best_odds, final_true_prob, prob_to_american(final_true_prob) if final_true_prob else None

            # Pick the first available book for BetResult fields
            def _first_book():
                for bk in [m.pin, m.fd, m.dk]:
                    if bk:
                        return bk
                return None

            first_bk = _first_book()

            # Process Over side
            if pp_side in ("both", "over"):
                best, prob, true = get_combined_true_odds("over")
                if best is not None:
                    serialized_matches.append({
                        **base,
                        "side": "over",
                        "best_odds": best,
                        "fd_odds": _display_odds(m.fd, "over", fd_margin),
                        "dk_odds": _display_odds(m.dk, "over", dk_margin),
                        "pin_odds": _display_odds(m.pin, "over", pin_margin),
                        "true_odds": true
                    })

                    # Also create +EV bet if applicable
                    if prob is not None and first_bk:
                        bet_id = f"{m.pp.player_id}_{m.pp.stat_type}_over"
                        res = BetResult(
                            bet_id=bet_id,
                            player_name=m.pp.player_name,
                            league=m.pp.league,
                            prop_type=m.pp.stat_type,
                            pp_line=m.pp.line_score,
                            fd_line=base["fd_line"] or base["dk_line"] or base["pin_line"],
                            side="over",
                            true_prob=prob,
                            over_odds=first_bk.over_odds,
                            under_odds=first_bk.under_odds,
                            both_sided=first_bk.both_sided,
                            pp_player_id=m.pp.player_id
                        )
                        if res.individual_ev_pct >= min_ev:
                            bets.append(res)
                            bet_book_odds[bet_id] = {
                                "fd_odds":    _display_odds(m.fd, "over", fd_margin),
                                "dk_odds":    _display_odds(m.dk, "over", dk_margin),
                                "pin_odds":   _display_odds(m.pin, "over", pin_margin),
                                "start_time": base.get("start_time", ""),
                                "books_probs": _per_book_probs("over"),
                            }

            # Process Under side
            if pp_side in ("both", "under"):
                best, prob, true = get_combined_true_odds("under")
                if best is not None:
                    serialized_matches.append({
                        **base,
                        "side": "under",
                        "best_odds": best,
                        "fd_odds": _display_odds(m.fd, "under", fd_margin),
                        "dk_odds": _display_odds(m.dk, "under", dk_margin),
                        "pin_odds": _display_odds(m.pin, "under", pin_margin),
                        "true_odds": true
                    })

                    # Also create +EV bet if applicable
                    if prob is not None and first_bk:
                        bet_id = f"{m.pp.player_id}_{m.pp.stat_type}_under"
                        res = BetResult(
                            bet_id=bet_id,
                            player_name=m.pp.player_name,
                            league=m.pp.league,
                            prop_type=m.pp.stat_type,
                            pp_line=m.pp.line_score,
                            fd_line=base["fd_line"] or base["dk_line"] or base["pin_line"],
                            side="under",
                            true_prob=prob,
                            over_odds=first_bk.over_odds,
                            under_odds=first_bk.under_odds,
                            both_sided=first_bk.both_sided,
                            pp_player_id=m.pp.player_id
                        )
                        if res.individual_ev_pct >= min_ev:
                            bets.append(res)
                            bet_book_odds[bet_id] = {
                                "fd_odds":    _display_odds(m.fd, "under", fd_margin),
                                "dk_odds":    _display_odds(m.dk, "under", dk_margin),
                                "pin_odds":   _display_odds(m.pin, "under", pin_margin),
                                "start_time": base.get("start_time", ""),
                                "books_probs": _per_book_probs("under"),
                            }

        # Deduplicate bets based on bet_id, keeping the one with highest EV
        unique_bets = {}
        for b in bets:
            if b.bet_id not in unique_bets or b.individual_ev_pct > unique_bets[b.bet_id].individual_ev_pct:
                unique_bets[b.bet_id] = b
        bets = list(unique_bets.values())

        # Sort by individual EV% descending
        bets.sort(key=lambda b: b.individual_ev_pct, reverse=True)

        # Sidecar map (not serialized to the client) so the market_observatory
        # background worker can attach per-book devigged probs to each row.
        # The empirical sharpness fitter uses these vs the eventual closing
        # line to refit consensus weights.
        bet_books_probs: dict[str, dict] = {}

        serialized_bets = []
        for b in bets:
            d = b.to_dict()
            extras = bet_book_odds.get(b.bet_id, {})
            d["fd_odds_book"] = extras.get("fd_odds")
            d["dk_odds_book"] = extras.get("dk_odds")
            d["pin_odds_book"] = extras.get("pin_odds")
            bet_books_probs[b.bet_id] = extras.get("books_probs") or {}
            start_time = extras.get("start_time", "")
            d["start_time"] = start_time
            # Precompute the backtest-dedup key so the client can join
            # in_backtest flags locally against /api/backtest/keys, removing
            # per-request copies and a Supabase round-trip off the hot path.
            player_key, time_key = make_bet_key(d.get("player_name", ""), start_time)
            d["bet_key"] = f"{player_key}|{time_key}"
            serialized_bets.append(d)

        # Free intermediate mapping before we hand off to Supabase + background
        # workers. On a full scrape this dict contains one entry per bet and
        # holds references the GC would otherwise retain until cycle end.
        bet_book_odds.clear()
        del bet_book_odds

        # Precompute what CLV actually needs from `matches` so we can drop the
        # heavy MatchedProp list (and its transitive references to every raw
        # FanDuel/DK/Pinnacle prop) before launching the background threads.
        try:
            clv_current_probs = _clv_tracker._build_current_probs(matches)
        except Exception as clv_pre_exc:
            logger.warning("CLV precompute error: %s", clv_pre_exc)
            clv_current_probs = {}

        with _lock:
            _state["bets"]         = serialized_bets
            _state["bet_map"]      = {b.bet_id: b for b in bets}
            _state["matches"]      = serialized_matches
            _state["pp_lines"]     = serialized_pp
            _state["fd_lines"]     = serialized_fd
            _state["dk_lines"]     = serialized_dk
            _state["pin_lines"]    = serialized_pin
            _state["last_refresh"] = datetime.now()
            _state["next_refresh"] = datetime.now() + timedelta(minutes=_state["interval_min"])
            _state["scrape_errors"] = errors
            interval_min_snapshot = _state["interval_min"]
            last_refresh_snapshot = _state["last_refresh"]

        # Rebuild the pre-serialized payload cache so subsequent GETs avoid
        # per-request json.dumps. Done outside the state lock because json
        # encoding is CPU-bound and doesn't touch _state.
        _refresh_payload_cache(
            serialized_bets,
            serialized_matches,
            serialized_pp,
            serialized_fd,
            serialized_dk,
            serialized_pin,
            last_refresh_snapshot,
            interval_min_snapshot,
        )
        # Books bytes are now in the payload cache — the Python lists in
        # _state are redundant (never read elsewhere). Releasing them here
        # halves the resident memory footprint of book datasets, which on a
        # full scrape is ~3-5MB per book.
        with _lock:
            _state["fd_lines"]  = []
            _state["dk_lines"]  = []
            _state["pin_lines"] = []
        logger.info("Pipeline complete: %d +EV bets found.", len(bets))

        # ── Supabase Sync: Persist the new state for instant load on restart ──
        # sync_state_to_supabase auto-gzips payloads over 256KB, so the books
        # (fd/dk/pin_lines, 2-5MB raw) compress to ~300-500KB — well under any
        # PostgREST request cap.
        def _sync_all():
            sync_state_to_supabase("bets", serialized_bets)
            sync_state_to_supabase("matches", serialized_matches)
            sync_state_to_supabase("pp_lines", serialized_pp)
            sync_state_to_supabase("fd_lines", serialized_fd)
            sync_state_to_supabase("dk_lines", serialized_dk)
            sync_state_to_supabase("pin_lines", serialized_pin)
            if _state["last_refresh"]:
                sync_state_to_supabase("last_refresh", _state["last_refresh"].isoformat())
        
        threading.Thread(target=_sync_all, daemon=True).start()

        # Auto-backtest logging for opted-in users
        def _auto_log_bg(bets=serialized_bets):
            try:
                from engine.database import get_db
                db = get_db()
                if not db:
                    return
                # Fetch users who explicitly opted in
                users_res = db.table("user_config").select("user_id").eq("auto_backtest", True).execute()
                for row in (users_res.data or []):
                    uid = row.get("user_id")
                    if not uid: 
                        continue
                        
                    from engine.backtest import BacktestLogger
                    bl = BacktestLogger(user_id=uid, db_client=db)
                    
                    # Pass the top ~40 bets so it has enough pool to select from
                    bl.try_log_slip(bets[:40], slip_type="Power", n_legs=6)
            except Exception as e:
                logger.error("Auto-backtest background worker error: %s", e)

        threading.Thread(target=_auto_log_bg, daemon=True).start()

        # ── Market Observatory: log lines for global calibration ──
        # Threshold at 0.30 (not 0.50) so the calibration sees both winners and
        # losers. Restricting to >0.50 made every observation an "expected hit",
        # which can only push calibration down. Including the 0.30–0.50 band
        # gives bidirectional signal — leagues whose underdogs over-perform
        # adjust up, those whose favorites under-perform adjust down.
        def _log_observatory_bg(bets=serialized_bets, books_probs_map=bet_books_probs):
            try:
                from engine.database import get_db as _get_db
                obs_db = _get_db()
                if not obs_db:
                    return
                rows_to_upsert = []
                # Track whether the `books` column exists; if a write fails we
                # retry once without it for older schemas (pre-migration_003).
                _books_supported = True
                for b in bets:
                    tp = float(b.get("true_prob") or 0)
                    if tp < 0.30:
                        continue
                    player = b.get("player_name", "")
                    league = b.get("league", "")
                    prop   = b.get("prop_type", "")
                    line   = b.get("pp_line", "")
                    side   = b.get("side", "")
                    start  = b.get("start_time", "")
                    market_key = f"{player}|{league}|{prop}|{line}|{side}|{start}"
                    row = {
                        "market_key":  market_key,
                        "player":      player,
                        "league":      league,
                        "prop":        prop,
                        "line":        float(line) if line != "" else 0,
                        "side":        side,
                        "true_prob":   round(tp, 4),
                        "game_start":  start if start else None,
                        "result":      "pending",
                    }
                    bp = books_probs_map.get(b.get("bet_id")) if books_probs_map else None
                    if bp:
                        row["books"] = bp
                    rows_to_upsert.append(row)
                if rows_to_upsert:
                    try:
                        # Batch upsert, skip duplicates via market_key unique constraint
                        obs_db.table("market_observatory").upsert(
                            rows_to_upsert,
                            on_conflict="market_key",
                            ignore_duplicates=True
                        ).execute()
                        logger.info("Observatory: logged %d observations", len(rows_to_upsert))
                    except Exception as upsert_exc:
                        # Pre-migration_003: the `books` column doesn't exist.
                        # Strip it and retry once. This keeps the pipeline
                        # working for users who haven't applied the migration.
                        if any("books" in r for r in rows_to_upsert):
                            for r in rows_to_upsert:
                                r.pop("books", None)
                            try:
                                obs_db.table("market_observatory").upsert(
                                    rows_to_upsert,
                                    on_conflict="market_key",
                                    ignore_duplicates=True
                                ).execute()
                                logger.info(
                                    "Observatory: logged %d observations (without per-book data — apply migration_003.sql to enable sharpness fitting)",
                                    len(rows_to_upsert),
                                )
                            except Exception as exc2:
                                logger.error("Observatory logging retry failed: %s", exc2)
                        else:
                            logger.error("Observatory logging error: %s", upsert_exc)
            except Exception as e:
                logger.error("Observatory logging error: %s", e)

        threading.Thread(target=_log_observatory_bg, daemon=True).start()

        # ── CLV Tracker: update closing lines for pending bets non-blocking ──
        # Pass the precomputed probs dict only — not the full matches list —
        # so matches can be freed as soon as this scope exits.
        def _update_clv_bg(current_probs=clv_current_probs):
            try:
                updated = _clv_tracker.update_closing_lines_from_probs(current_probs)
                finalized = _clv_tracker.finalize_missed()
                if updated or finalized:
                    logger.info("CLVTracker: %d updated, %d finalized in background", updated, finalized)
            except Exception as clv_exc:
                logger.warning("CLVTracker background error: %s", clv_exc)

        threading.Thread(target=_update_clv_bg, daemon=True).start()

        # ── Results checker: back-fill any pending rows non-blocking ──
        def _check_results_bg():
            try:
                updated = _results_checker.check_pending_results()
                if updated:
                    logger.info("ResultsChecker: %d rows updated in background", updated)
            except Exception as rc_exc:
                logger.warning("ResultsChecker background error: %s", rc_exc)
            # Also resolve observatory rows (reuses ESPN cache from above)
            try:
                obs_updated = _results_checker.check_observatory_results()
                if obs_updated:
                    logger.info("Observatory: %d observations resolved in background", obs_updated)
            except Exception as obs_exc:
                logger.warning("Observatory resolution error: %s", obs_exc)

        threading.Thread(target=_check_results_bg, daemon=True).start()

    except Exception as e:
        logger.exception("Pipeline error: %s", e)
        errors["pipeline"] = str(e)
        with _lock:
            _state["scrape_errors"] = errors


def _reschedule(interval_min: int):
    """Reschedule the auto-refresh job with a new interval.

    Only touches the auto_refresh job; leaves other jobs (e.g. daily_calibration)
    intact so a user config change doesn't silently cancel the calibration cron.
    """
    try:
        scheduler.remove_job("auto_refresh")
    except JobLookupError:
        pass
    scheduler.add_job(
        run_pipeline,
        trigger="interval",
        minutes=interval_min,
        id="auto_refresh",
        next_run_time=datetime.now() + timedelta(minutes=interval_min),
    )
    with _lock:
        _state["next_refresh"] = datetime.now() + timedelta(minutes=interval_min)


# ---------------------------------------------------------------------------
# Startup / shutdown
# ---------------------------------------------------------------------------

@app.on_event("startup")
def startup():
    # ── Seed state from Supabase SYNCHRONOUSLY first ──
    # This must happen before the pipeline starts so the fresh scrape can't
    # be overwritten by a late-arriving stale seed. It also guarantees any
    # request that hits /api/bets, /api/bootstrap etc. during warm-up gets
    # the most recent persisted snapshot instead of empty arrays.
    _seed_state_from_db_sync()

    # Seed the payload cache from whatever we just loaded so warm-start GETs
    # don't fall through to the cold-start empty shape. Safe to call even if
    # some datasets are empty — empty arrays serialize fine.
    try:
        _rebuild_cache_from_state()
        # Books are authoritative as pre-serialized bytes now. Drop the
        # duplicate Python lists to cut ~10MB of RSS before the pipeline runs.
        with _lock:
            _state["fd_lines"]  = []
            _state["dk_lines"]  = []
            _state["pin_lines"] = []
    except Exception as exc:
        logger.warning("Initial cache seed failed: %s", exc)

    scheduler.start()
    _reschedule(_state["interval_min"])
    logger.info("Scheduler started. Auto-refresh every %d min.", _state["interval_min"])

    # ── Startup recovery: finalize any missed CLV rows from when the app was down ──
    def _startup_clv_recovery():
        try:
            finalized = _clv_tracker.finalize_missed()
            if finalized:
                logger.info("Startup CLV recovery: finalized %d missed rows", finalized)
        except Exception as exc:
            logger.warning("Startup CLV recovery error: %s", exc)

    threading.Thread(target=_startup_clv_recovery, daemon=True).start()

    # ── Hourly calibration + correlation refit ──
    # Updated from daily → hourly: calibration multipliers and leg-pair
    # correlations both benefit from faster feedback as the backtest log
    # grows. Each run is a single aggregation query on resolved observations
    # (no scraping), so the load is negligible.
    def _run_periodic_models():
        try:
            from engine.isotonic_calibration import update_isotonic_calibration
            from engine.ev_calculator import reload_calibration
            curves = update_isotonic_calibration()
            if curves:
                reload_calibration()
                logger.info(
                    "Hourly refit: isotonic curves reloaded — %d leagues, %d (league,prop) buckets",
                    len(curves.get("leagues") or {}), len(curves.get("props") or {}),
                )
            else:
                logger.info("Hourly refit: calibration — no mature data yet")
        except Exception as exc:
            logger.error("Hourly refit: calibration error: %s", exc)

        try:
            from engine.sharpness_calibration import update_sharpness_weights
            from engine.consensus import reload_sharpness
            sharp = update_sharpness_weights()
            if sharp:
                n_books = reload_sharpness()
                logger.info("Hourly refit: sharpness weights refit for %d books", n_books)
            else:
                logger.info(
                    "Hourly refit: sharpness — no per-book CLV data yet "
                    "(apply migration_003.sql once observations accumulate)",
                )
        except Exception as exc:
            logger.error("Hourly refit: sharpness error: %s", exc)

        try:
            from engine.correlation import update_correlation_map, reload_correlation, MIN_PAIR_OBS
            corr = update_correlation_map()
            if corr:
                n_trusted = reload_correlation()
                logger.info(
                    "Hourly refit: %d correlation buckets total, %d above %d-pair threshold",
                    len(corr.get("buckets", {})), n_trusted, MIN_PAIR_OBS,
                )
            else:
                logger.info("Hourly refit: correlation — no data yet")
        except Exception as exc:
            logger.error("Hourly refit: correlation error: %s", exc)

    scheduler.add_job(
        _run_periodic_models,
        trigger="interval",
        hours=1,
        id="periodic_models",
        next_run_time=datetime.now() + timedelta(minutes=1),
        replace_existing=True,
    )
    # Also run once on startup (off-thread so we don't block boot).
    threading.Thread(target=_run_periodic_models, daemon=True).start()

    # Run pipeline immediately on startup so data is ready
    threading.Thread(target=run_pipeline, daemon=True).start()


# Any cached snapshot older than this is considered stale and ignored on
# startup (and proactively purged from Supabase). 
# Set to 1440 (24h) so users can instantly load the UI with yesterday's lines 
# while the background scrape runs, rather than facing a 60s loading screen.
_SEED_MAX_AGE_MIN = 1440


def _parse_updated_at(s: str | None):
    if not s:
        return None
    try:
        # Supabase returns UTC ISO with trailing '+00:00' or 'Z'
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def _seed_state_from_db_sync():
    """
    Load the last persisted snapshot from Supabase into in-memory state.

    Only seeds keys that are still empty AND whose cached value is newer
    than _SEED_MAX_AGE_MIN. Stale rows are deleted from app_state_cache so
    they can't resurface on the next restart.
    """
    from datetime import timezone
    from engine.database import get_db
    from engine.persistence import load_multiple_states_from_supabase
    
    logger.info("Startup: Seeding state from Supabase cache...")
    keys = ["bets", "matches", "pp_lines", "fd_lines", "dk_lines", "pin_lines", "last_refresh"]
    now = datetime.now(timezone.utc)
    db = get_db()

    try:
        states = load_multiple_states_from_supabase(keys)
    except Exception as exc:
        logger.warning(f"Seed: failed to load states: {exc}")
        states = {}

    purged = []
    
    for k in keys:
        if k not in states:
            # Maybe log or just continue
            continue
            
        data, updated_at = states[k]
        if data is None:
            continue

        ts = _parse_updated_at(updated_at)
        age_min = None
        if ts is not None:
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            age_min = (now - ts).total_seconds() / 60.0

        # Stale (or un-timestamped) → skip and delete so we never see it again.
        if ts is None or age_min > _SEED_MAX_AGE_MIN:
            purged.append((k, age_min))
            if db:
                try:
                    db.table("app_state_cache").delete().eq("key", k).execute()
                except Exception as exc:
                    logger.warning("Seed: failed to purge stale '%s': %s", k, exc)
            continue

        with _lock:
            current = _state.get(k)
            if k == "last_refresh":
                if current is not None:
                    continue
                try:
                    _state[k] = datetime.fromisoformat(data)
                except Exception:
                    _state[k] = None
            else:
                if current:  # non-empty list already — keep it
                    continue
                _state[k] = data

    if purged:
        logger.info(
            "Startup: purged %d stale cache key(s) (> %dmin old): %s",
            len(purged), _SEED_MAX_AGE_MIN,
            ", ".join(f"{k}({'?' if a is None else f'{a:.0f}m'})" for k, a in purged),
        )
    logger.info("Startup: Seeding complete.")


@app.on_event("shutdown")
def shutdown():
    scheduler.shutdown(wait=False)


# ---------------------------------------------------------------------------
# Static files + root
# ---------------------------------------------------------------------------

import pathlib
STATIC_DIR = pathlib.Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/health")
@app.head("/health")
def health():
    return {"status": "ok"}


@app.get("/")
def root():
    from engine.database import SUPABASE_URL, SUPABASE_ANON_KEY
    html = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    # Inject runtime config so the frontend doesn't need to fetch /api/ui-config
    config_script = (
        f'<script>window.__COREPROP_CONFIG='
        f'{{"supabase_url":"{SUPABASE_URL}","supabase_anon_key":"{SUPABASE_ANON_KEY}"}}'
        f'</script>'
    )
    html = html.replace("</head>", config_script + "\n</head>", 1)
    from fastapi.responses import HTMLResponse
    return HTMLResponse(content=html)


# ---------------------------------------------------------------------------
# API routes
# ---------------------------------------------------------------------------

@app.get("/api/auth/me")
def get_auth_me(user: dict = Depends(get_current_user)):
    """Return current verified user metadata."""
    return user


@app.get("/api/auth/check-username")
def check_username(username: str):
    """Check if a username is already taken. Public endpoint."""
    import requests as _req
    from engine.database import SUPABASE_URL, SUPABASE_KEY
    if not username or len(username) < 2 or len(username) > 20:
        raise HTTPException(status_code=400, detail="Username must be 2-20 characters.")
    if not username.replace("_", "").isalnum():
        raise HTTPException(status_code=400, detail="Username may only contain letters, numbers, and underscores.")

    target = username.lower()
    # Page through the admin API. Default page size is 50; without paging,
    # users beyond page 1 silently pass as "available". We cap at 20 pages
    # (1000 users) — well above current scale, with a hard exit so a future
    # 10k-user database can't pin this endpoint.
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    PER_PAGE = 200
    for page in range(1, 21):
        try:
            r = _req.get(
                f"{SUPABASE_URL}/auth/v1/admin/users",
                params={"page": page, "per_page": PER_PAGE},
                headers=headers,
                timeout=5,
            )
        except Exception as exc:
            logger.warning("check_username request failed: %s", exc)
            raise HTTPException(status_code=500, detail="Could not verify username.")
        if r.status_code != 200:
            logger.warning("check_username admin API returned %s: %s", r.status_code, r.text[:200])
            raise HTTPException(status_code=500, detail="Could not verify username.")
        users = r.json().get("users", []) or []
        for u in users:
            meta = u.get("user_metadata") or {}
            if (meta.get("username", "") or "").lower() == target:
                return {"available": False}
        if len(users) < PER_PAGE:
            break
    return {"available": True}


def _last_refresh_iso():
    """Return last_refresh as ISO string or None (callers already hold no lock)."""
    ts = _state.get("last_refresh")
    if isinstance(ts, datetime):
        return ts.isoformat()
    if isinstance(ts, str):
        return ts
    return None


def _get_user_config(user: Optional[dict]) -> dict:
    import config as cfg
    base = {
        "min_ev_pct": -10.0,
        "active_leagues": dict(cfg.ACTIVE_LEAGUES),
        "refresh_interval_min": 5,
        "auto_backtest": False
    }
    if not user:
        return base
        
    try:
        from engine.database import get_user_db
        db = get_user_db(user["jwt"])
        if db:
            res = db.table("user_config").select("*").eq("user_id", user["id"]).execute()
            if res.data:
                row = res.data[0]
                base["min_ev_pct"] = float(row.get("min_ev_pct", -10.0))
                if row.get("active_leagues"):
                    base["active_leagues"] = row["active_leagues"]
                base["refresh_interval_min"] = int(row.get("refresh_interval_min", 5))
                base["auto_backtest"] = bool(row.get("auto_backtest", False))
    except Exception as e:
        logger.warning(f"Failed to fetch user config for {user['id']}: {e}")
        
    return base

@app.get("/api/bets")
def get_bets(request: Request):
    """Serve the pre-serialized bets payload. Per-user filtering (min_ev_pct,
    active_leagues, in_backtest) now happens client-side via `/api/backtest/keys`
    — this removes the per-request dict-copy (~one full payload of allocations)
    and the Supabase round-trip that used to block the hot path."""
    return _cached_response("bets", request)


@app.get("/api/ui-config")
def get_ui_config():
    from engine.database import SUPABASE_URL, SUPABASE_ANON_KEY
    return {
        "supabase_url": SUPABASE_URL,
        "supabase_anon_key": SUPABASE_ANON_KEY
    }


@app.get("/api/matched")
def get_matched(request: Request):
    return _cached_response("matches", request)


@app.get("/api/bootstrap/core")
def get_bootstrap_core(request: Request):
    """Critical-path payload for first paint: bets + meta only.

    Intentionally does NOT include matches/pp/fd/dk/pin — those load lazily
    when their tab is activated. This trims the initial payload by ~80% and
    removes 5 heavy allocations from the common request path on the 512MB tier.
    """
    return _cached_response("core", request)


@app.get("/api/backtest/keys")
def get_backtest_keys(user: dict = Depends(get_current_user)):
    """Lightweight endpoint: returns the set of `bet_key` strings already
    logged by this user, so the client can join in_backtest flags locally.

    Replaces the full per-request dict-copy + Supabase round-trip that used
    to live inside /api/bets and /api/bootstrap."""
    from engine.backtest import BacktestLogger
    logger_inst = BacktestLogger(user["id"], user["jwt"])
    used = logger_inst._load_used_keys_from_db()
    # used is a set of (normalized_player, yyyy-mm-dd) tuples — flatten to
    # the same "player|date" string we stamp on each bet during pipeline.
    keys = [f"{p}|{d}" for (p, d) in used]
    return {"keys": keys}


@app.get("/api/bootstrap")
def get_bootstrap_legacy(request: Request):
    """Back-compat shim: old clients (or stale cached HTML) may still call
    this. Redirect to the lean core endpoint — the extra datasets load lazily
    on tab activation now."""
    return _cached_response("core", request)


@app.get("/api/status")
def get_status():
    with _lock:
        return {
            "is_scraping":   _state["is_scraping"],
            "last_refresh":  _state["last_refresh"].isoformat() if _state["last_refresh"] else None,
            "next_refresh":  _state["next_refresh"].isoformat() if _state["next_refresh"] else None,
            "scrape_errors": _state["scrape_errors"],
            "interval_min":  _state["interval_min"],
            "total_bets":    len(_state["bets"]),
        }


class SlipRequest(BaseModel):
    bet_ids: list[str]
    bankroll: float = 100.0


@app.post("/api/slip")
def build_slip(req: SlipRequest, user: Optional[dict] = Depends(get_current_user_optional)):
    if not req.bet_ids:
        raise HTTPException(status_code=400, detail="No bet IDs provided.")
    if len(req.bet_ids) < 2 or len(req.bet_ids) > 6:
        raise HTTPException(status_code=400, detail="Slip must have 2-6 picks.")

    with _lock:
        bet_map = _state["bet_map"]

    selected: list[BetResult] = []
    missing = []
    for bid in req.bet_ids:
        bet = bet_map.get(bid)
        if bet is None:
            missing.append(bid)
        else:
            selected.append(bet)

    if missing:
        raise HTTPException(status_code=404, detail=f"Bet IDs not found: {missing}")

    result = calculate_slip(selected, req.bankroll)
    return result


#  Candidate pool cap for /api/slip/auto. Limits combinatorial blow-up of the
#  subset search: Σ C(12, k) for k=2..6 is 2,497 subsets — each scored via the
#  exact independence-based EV formulas (microseconds each) before the single
#  winning subset gets the full correlation-aware Monte Carlo.
_AUTO_SLIP_MAX_CANDIDATES = 12


@app.post("/api/slip/auto")
def auto_build_slip(req: SlipRequest, user: Optional[dict] = Depends(get_current_user_optional)):
    """Pick the best 2–6-leg slip from a candidate pool.

    Unlike the prior implementation (which only tried EV-sorted prefixes),
    this enumerates *every* subset of size K ∈ [2, 6] from the deduplicated
    candidate pool and scores each under the exact independence formulas —
    significantly more accurate for Flex slips where a low-probability leg
    can hurt despite having positive individual EV. The winner is then
    re-scored with correlation-aware Monte Carlo so same-game stacks aren't
    penalised for picking up latent positive correlation.
    """
    if not req.bet_ids:
        raise HTTPException(status_code=400, detail="No bet IDs provided.")
    if len(req.bet_ids) < 2:
        raise HTTPException(status_code=400, detail="Must provide at least 2 bets.")

    with _lock:
        bet_map = _state["bet_map"]

    # Dedupe by (player, game_day). Input order is preserved — the client sorts
    # by EV before sending, so the cap keeps the most promising candidates.
    candidates: list[BetResult] = []
    seen: set[tuple[str, str]] = set()
    for bid in req.bet_ids:
        bet = bet_map.get(bid)
        if bet is None:
            continue
        key = make_bet_key(bet.player_name, bet.start_time)
        if key in seen:
            continue
        candidates.append(bet)
        seen.add(key)
        if len(candidates) >= _AUTO_SLIP_MAX_CANDIDATES:
            break

    if len(candidates) < 2:
        raise HTTPException(status_code=400, detail="Not enough unique bets after dedup.")

    from itertools import combinations
    from engine.ev_calculator import power_slip_ev, flex_slip_ev

    best_ev = -float("inf")
    best_subset: list[BetResult] = []
    max_k = min(6, len(candidates))
    for k in range(2, max_k + 1):
        for combo in combinations(candidates, k):
            probs = [b.true_prob for b in combo]
            p_ev = power_slip_ev(probs)
            f_ev = flex_slip_ev(probs)
            evs = [ev for ev in (p_ev, f_ev) if ev is not None]
            if not evs:
                continue
            ev = max(evs)
            if ev > best_ev + 1e-9:
                best_ev = ev
                best_subset = list(combo)

    if not best_subset:
        raise HTTPException(status_code=400, detail="Could not calculate any valid slip.")

    # Final evaluation with correlation-aware Monte Carlo for the returned metrics.
    final = calculate_slip(best_subset, req.bankroll)
    return {
        **final,
        "optimal_k":       len(best_subset),
        "optimal_bet_ids": [b.bet_id for b in best_subset],
    }


class SandboxRequest(BaseModel):
    leagues:        list[str] = []
    min_prob:       float     = 0.5408
    slip_size:      int       = 6
    slip_type:      str       = "flex"
    bet_size:       float     = 1.0
    use_kelly:      bool      = False
    included_props: list[str] = []

@app.get("/api/sandbox/stat-types")
def list_sandbox_stat_types(user: dict = Depends(get_current_user)):
    """Return distinct (league, stat_type) pairs from market_observatory so
    the Sandbox UI shows only filter chips that will actually match data.
    Pages through results because the supabase client caps a single
    response at 1000 rows."""
    from engine.database import get_db
    db = get_db()
    if not db:
        raise HTTPException(status_code=503, detail="Database not connected")

    grouped: dict[str, set] = {}
    PAGE = 1000
    offset = 0
    try:
        while True:
            res = (
                db.table("market_observatory")
                .select("league,prop")
                .range(offset, offset + PAGE - 1)
                .execute()
            )
            rows = res.data or []
            if not rows:
                break
            for r in rows:
                lg = (r.get("league") or "").strip() or "Unknown"
                prop = (r.get("prop") or "").strip()
                if not prop:
                    continue
                grouped.setdefault(lg, set()).add(prop)
            if len(rows) < PAGE:
                break
            offset += PAGE
            # Hard cap to avoid pathological loops on a runaway table.
            if offset > 200000:
                break
    except Exception as e:
        logger.exception("stat-types endpoint failed")
        raise HTTPException(status_code=500, detail=f"DB query failed: {e}")

    return {lg: sorted(props) for lg, props in sorted(grouped.items())}


@app.post("/api/sandbox/run")
def run_sandbox_simulation(req: SandboxRequest, user: dict = Depends(get_current_user)):
    from engine.strategy_tester import StrategyTester, StrategyConfig
    tester = StrategyTester()
    config = StrategyConfig(
        leagues=req.leagues,
        min_prob=req.min_prob,
        slip_size=req.slip_size,
        slip_type=req.slip_type,
        bet_size=req.bet_size,
        use_kelly=req.use_kelly,
        included_props=req.included_props
    )
    result = tester.run_simulation(config)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result

@app.post("/api/sandbox/optimize")
def optimize_sandbox_threshold(req: SandboxRequest, user: dict = Depends(get_current_user)):
    from engine.strategy_tester import StrategyTester, StrategyConfig
    tester = StrategyTester()
    config = StrategyConfig(
        leagues=req.leagues,
        slip_size=req.slip_size,
        slip_type=req.slip_type,
        bet_size=req.bet_size,
        use_kelly=req.use_kelly,
        included_props=req.included_props
    )
    result = tester.optimize_threshold(config)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


class ConfigUpdate(BaseModel):
    interval_min:    Optional[int]   = None
    min_ev_pct:      Optional[float] = None
    active_leagues:  Optional[dict]  = None


@app.get("/api/config")
def get_config(user: dict = Depends(get_current_user)):
    cfg = _get_user_config(user)
    return {
        "interval_min":   cfg["refresh_interval_min"],
        "min_ev_pct":     cfg["min_ev_pct"],
        "active_leagues": cfg["active_leagues"],
        "auto_backtest":  cfg.get("auto_backtest", False)
    }


@app.post("/api/config")
def update_config(update: ConfigUpdate, user: dict = Depends(get_current_user)):
    with _lock:
        if update.interval_min is not None:
            if update.interval_min < 1:
                raise HTTPException(status_code=400, detail="interval_min must be >= 1")
            _state["interval_min"] = update.interval_min
            _reschedule(update.interval_min)

        if update.min_ev_pct is not None:
            _state["min_ev_pct"] = update.min_ev_pct

        if update.active_leagues is not None:
            _state["active_leagues"].update(update.active_leagues)

    return {"status": "config updated"}


class AutoBacktestUpdate(BaseModel):
    auto_backtest: bool

@app.post("/api/user/auto-backtest")
def update_auto_backtest(update: AutoBacktestUpdate, user: dict = Depends(get_current_user)):
    from engine.database import get_user_db
    db = get_user_db(user["jwt"])
    if not db:
        raise HTTPException(status_code=500, detail="Database not reachable.")
    
    # Upsert the user_config row. (Requires an ON CONFLICT clause in raw sql, but we'll try a basic upsert via PostgREST)
    try:
        db.table("user_config").upsert({
            "user_id": user["id"],
            "auto_backtest": update.auto_backtest,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }).execute()
        return {"status": "success", "auto_backtest": update.auto_backtest}
    except Exception as e:
        logger.error(f"Failed to update auto_backtest for user {user['id']}: {e}")
        raise HTTPException(status_code=500, detail="Failed to update configuration.")

# ---------------------------------------------------------------------------
# PrizePicks-only endpoints
# ---------------------------------------------------------------------------

@app.get("/api/prizepicks")
def get_prizepicks(request: Request):
    return _cached_response("pp_lines", request)


@app.post("/api/prizepicks/refresh")
def refresh_prizepicks():
    with _lock:
        if _state["is_scraping_pp"]:
            raise HTTPException(status_code=409, detail="PrizePicks scrape already in progress.")
    threading.Thread(target=_run_pp_scrape, daemon=True).start()
    return {"status": "prizepicks refresh started"}


def _run_pp_scrape():
    with _lock:
        if _state["is_scraping_pp"]:
            return
        _state["is_scraping_pp"] = True

    try:
        with _lock:
            leagues = dict(_state["active_leagues"])

        logger.info("PrizePicks-only scrape starting...")
        pp_lines = scrape_prizepicks(active_leagues=leagues)
        if len(pp_lines) == 0:
            logger.warning("PrizePicks returned 0 lines — keeping previous serialized state")
            return
        serialized = []
        for l in pp_lines:
            if l.side == "both":
                common = {
                    "league": l.league,
                    "player_name": l.player_name,
                    "stat_type": l.stat_type,
                    "line_score": l.line_score,
                    "start_time": l.start_time,
                }
                serialized.append({**common, "side": "over"})
                serialized.append({**common, "side": "under"})
            else:
                serialized.append({
                    "league": l.league,
                    "player_name": l.player_name,
                    "stat_type": l.stat_type,
                    "line_score": l.line_score,
                    "side": l.side,
                    "start_time": l.start_time,
                })
        with _lock:
            _state["last_refresh"] = datetime.now()
            last_ref = _state["last_refresh"]
        _update_one_payload("pp_lines", serialized, last_ref)
        sync_state_to_supabase("pp_lines", serialized)
        with _lock:
            _state["pp_lines"] = []
        logger.info("PrizePicks-only scrape complete: %d lines.", len(serialized))
    except Exception as e:
        logger.exception("PrizePicks scrape error: %s", e)
    finally:
        with _lock:
            _state["is_scraping_pp"] = False

# ---------------------------------------------------------------------------
# FanDuel-only endpoints
# ---------------------------------------------------------------------------

@app.get("/api/fanduel")
def get_fanduel(request: Request):
    return _cached_response("fd_lines", request)


@app.post("/api/fanduel/refresh")
def refresh_fanduel():
    with _lock:
        if _state["is_scraping_fd"]:
            raise HTTPException(status_code=409, detail="FanDuel scrape already in progress.")
    threading.Thread(target=_run_fd_scrape, daemon=True).start()
    return {"status": "fanduel refresh started"}


def _run_fd_scrape():
    with _lock:
        if _state["is_scraping_fd"]:
            return
        _state["is_scraping_fd"] = True

    try:
        with _lock:
            leagues = dict(_state["active_leagues"])

        logger.info("FanDuel scrape starting...")
        fd_props = scrape_fanduel(active_leagues=leagues)
        if len(fd_props) == 0:
            logger.warning("FanDuel returned 0 props — keeping previous serialized state")
            return
        from engine.devig import devig_power, devig_single_sided, prob_to_american
        serialized = []
        for p in fd_props:
            true_over, true_under = None, None
            if p.both_sided and p.over_odds is not None and p.under_odds is not None:
                true_over, true_under = devig_power(p.over_odds, p.under_odds)
            else:
                if p.over_odds is not None:
                    true_over = devig_single_sided(p.over_odds)
                if p.under_odds is not None:
                    true_under = devig_single_sided(p.under_odds)

            if p.over_odds is not None:
                serialized.append({
                    "league": p.league,
                    "player_name": p.player_name,
                    "stat_type": p.prop_type,
                    "line_score": p.line,
                    "side": "over",
                    "line_odds": p.over_odds,
                    "true_odds": prob_to_american(true_over) if true_over else None,
                    "start_time": getattr(p, "start_time", None),
                })
            if p.under_odds is not None:
                serialized.append({
                    "league": p.league,
                    "player_name": p.player_name,
                    "stat_type": p.prop_type,
                    "line_score": p.line,
                    "side": "under",
                    "line_odds": p.under_odds,
                    "true_odds": prob_to_american(true_under) if true_under else None,
                    "start_time": getattr(p, "start_time", None),
                })
        with _lock:
            _state["last_refresh"] = datetime.now()
            last_ref = _state["last_refresh"]
        # Only swap the fd_lines cache slot — the other five (bets, matches,
        # pp_lines, dk_lines, pin_lines, core) are untouched, so re-encoding
        # them would just churn memory.
        _update_one_payload("fd_lines", serialized, last_ref)
        # Persist so a cold start can serve data before the next scrape.
        # sync_state_to_supabase auto-gzips payloads over 256KB.
        threading.Thread(target=sync_state_to_supabase, args=("fd_lines", serialized), daemon=True).start()
        # Drop the Python list: the bytes cache is now authoritative for GETs,
        # and _state[fd_lines] isn't read anywhere else.
        with _lock:
            _state["fd_lines"] = []
        logger.info("FanDuel scrape complete: %d lines.", len(serialized))
    except Exception as e:
        logger.exception("FanDuel scrape error: %s", e)
    finally:
        with _lock:
            _state["is_scraping_fd"] = False


# ---------------------------------------------------------------------------
# DraftKings-only endpoints
# ---------------------------------------------------------------------------

@app.get("/api/draftkings")
def get_draftkings(request: Request):
    return _cached_response("dk_lines", request)


@app.post("/api/draftkings/refresh")
def refresh_draftkings():
    with _lock:
        if _state["is_scraping_dk"]:
            raise HTTPException(status_code=409, detail="DraftKings scrape already in progress.")
    threading.Thread(target=_run_dk_scrape, daemon=True).start()
    return {"status": "draftkings refresh started"}


def _run_dk_scrape():
    with _lock:
        if _state["is_scraping_dk"]:
            return
        _state["is_scraping_dk"] = True

    try:
        with _lock:
            leagues = dict(_state["active_leagues"])

        logger.info("DraftKings scrape starting...")
        dk_props = scrape_draftkings(active_leagues=leagues)
        if len(dk_props) == 0:
            logger.warning("DraftKings returned 0 props — keeping previous serialized state")
            return
        from engine.devig import devig_power, devig_single_sided, prob_to_american
        serialized = []
        for p in dk_props:
            true_over, true_under = None, None
            if p.both_sided and p.over_odds is not None and p.under_odds is not None:
                true_over, true_under = devig_power(p.over_odds, p.under_odds)
            else:
                if p.over_odds is not None:
                    true_over = devig_single_sided(p.over_odds)
                if p.under_odds is not None:
                    true_under = devig_single_sided(p.under_odds)

            if p.over_odds is not None:
                serialized.append({
                    "league": p.league,
                    "player_name": p.player_name,
                    "stat_type": p.prop_type,
                    "line_score": p.line,
                    "side": "over",
                    "line_odds": p.over_odds,
                    "true_odds": prob_to_american(true_over) if true_over else None,
                    "start_time": getattr(p, "start_time", None),
                })
            if p.under_odds is not None:
                serialized.append({
                    "league": p.league,
                    "player_name": p.player_name,
                    "stat_type": p.prop_type,
                    "line_score": p.line,
                    "side": "under",
                    "line_odds": p.under_odds,
                    "true_odds": prob_to_american(true_under) if true_under else None,
                    "start_time": getattr(p, "start_time", None),
                })
        with _lock:
            _state["last_refresh"] = datetime.now()
            last_ref = _state["last_refresh"]
        _update_one_payload("dk_lines", serialized, last_ref)
        threading.Thread(target=sync_state_to_supabase, args=("dk_lines", serialized), daemon=True).start()
        with _lock:
            _state["dk_lines"] = []
        logger.info("DraftKings scrape complete: %d lines.", len(serialized))
    except Exception as e:
        logger.exception("DraftKings scrape error: %s", e)
    finally:
        with _lock:
            _state["is_scraping_dk"] = False


# ---------------------------------------------------------------------------
# Pinnacle-only endpoints
# ---------------------------------------------------------------------------

@app.get("/api/pinnacle")
def get_pinnacle(request: Request):
    return _cached_response("pin_lines", request)


@app.post("/api/pinnacle/refresh")
def refresh_pinnacle():
    with _lock:
        if _state["is_scraping_pin"]:
            raise HTTPException(status_code=409, detail="Pinnacle scrape already in progress.")
    threading.Thread(target=_run_pin_scrape, daemon=True).start()
    return {"status": "pinnacle refresh started"}


def _run_pin_scrape():
    with _lock:
        if _state["is_scraping_pin"]:
            return
        _state["is_scraping_pin"] = True

    try:
        with _lock:
            leagues = dict(_state["active_leagues"])

        logger.info("Pinnacle scrape starting...")
        pin_props = scrape_pinnacle(active_leagues=leagues)
        if len(pin_props) == 0:
            logger.warning("Pinnacle returned 0 props — keeping previous serialized state")
            return
        from engine.devig import devig_power, devig_single_sided, prob_to_american
        serialized = []
        for p in pin_props:
            true_over, true_under = None, None
            if p.both_sided and p.over_odds is not None and p.under_odds is not None:
                true_over, true_under = devig_power(p.over_odds, p.under_odds)
            else:
                if p.over_odds is not None:
                    true_over = devig_single_sided(p.over_odds)
                if p.under_odds is not None:
                    true_under = devig_single_sided(p.under_odds)

            if p.over_odds is not None:
                serialized.append({
                    "league": p.league,
                    "player_name": p.player_name,
                    "stat_type": p.prop_type,
                    "line_score": p.line,
                    "side": "over",
                    "line_odds": p.over_odds,
                    "true_odds": prob_to_american(true_over) if true_over else None,
                    "start_time": getattr(p, "start_time", None),
                })
            if p.under_odds is not None:
                serialized.append({
                    "league": p.league,
                    "player_name": p.player_name,
                    "stat_type": p.prop_type,
                    "line_score": p.line,
                    "side": "under",
                    "line_odds": p.under_odds,
                    "true_odds": prob_to_american(true_under) if true_under else None,
                    "start_time": getattr(p, "start_time", None),
                })
        with _lock:
            _state["last_refresh"] = datetime.now()
            last_ref = _state["last_refresh"]
        _update_one_payload("pin_lines", serialized, last_ref)
        threading.Thread(target=sync_state_to_supabase, args=("pin_lines", serialized), daemon=True).start()
        with _lock:
            _state["pin_lines"] = []
        logger.info("Pinnacle scrape complete: %d lines.", len(serialized))
    except Exception as e:
        logger.exception("Pinnacle scrape error: %s", e)
    finally:
        with _lock:
            _state["is_scraping_pin"] = False


# ---------------------------------------------------------------------------
# Calibration metrics endpoint
# ---------------------------------------------------------------------------

@app.get("/api/calibration")
def get_calibration(user: dict = Depends(get_current_user)):
    """Return Brier Score, Log-Loss, and calibration buckets from resolved backtest data."""
    from engine.calibration import evaluate_calibration
    return evaluate_calibration(user_jwt=user["jwt"])


@app.get("/api/analytics")
def get_analytics(user: dict = Depends(get_current_user)):
    """
    Richer analytics payload: calibration + per-league / per-prop performance,
    cumulative P&L timeline, and slip outcome mix.

    Per-user TTL cache (30s) because the frontend re-hits this on every tab
    activation and status-poll refresh, but the underlying backtest state
    rarely changes between those calls. Invalidated on add/delete slip.
    """
    uid = user["id"]
    now = time.monotonic()
    with _analytics_cache_lock:
        cached = _analytics_cache.get(uid)
        if cached and (now - cached[0]) < _ANALYTICS_TTL_SEC:
            return cached[1]

    from engine.calibration import evaluate_analytics
    data = evaluate_analytics(user_jwt=user["jwt"])
    with _analytics_cache_lock:
        _analytics_cache[uid] = (now, data)
    return data


# ---------------------------------------------------------------------------
# Backtest endpoints
# ---------------------------------------------------------------------------


@app.get("/api/backtest/slips")
def get_backtest_slips(user: dict = Depends(get_current_user)):
    """Return the last 100 logged slips from Supabase."""
    from engine.database import get_user_db

    db = get_user_db(user["jwt"])
    all_slips = []

    if not db:
        return {"slips": [], "total": 0}

    try:
        # 1. Fetch the latest 100 slips
        slips_res = db.table("slips").select("*").order("timestamp", desc=True).limit(100).execute()
        slip_data = slips_res.data
        if not slip_data:
            return {"slips": [], "total": 0}

        sids = [s["id"] for s in slip_data]
        # 2. Fetch all legs for these slips
        legs_res = db.table("legs").select("*").in_("slip_id", sids).execute()
        legs_by_slip = {}
        for l in legs_res.data:
            sid = l["slip_id"]
            if sid not in legs_by_slip:
                legs_by_slip[sid] = []
            legs_by_slip[sid].append(l)
        
        for s in slip_data:
            s["slip_id"] = s["id"]
            s["legs"] = sorted(legs_by_slip.get(s["id"], []), key=lambda x: x["leg_num"])
        all_slips = slip_data
    except Exception as db_err:
        logger.error("Backtest API: Supabase fetch failed: %s", db_err)
        return {"slips": [], "total": 0}

    # Compute payout per slip (Common Logic)
    from engine.constants import POWER_PAYOUTS, FLEX_PAYOUTS

    for slip in all_slips:
        legs = slip.get("legs", [])
        n_legs = int(slip.get("n_legs") or len(legs))
        slip_type = (slip.get("slip_type") or "").lower()
        results = [l.get("result", "pending") for l in legs]

        completed = all(r in ("hit", "miss", "push", "dnp") for r in results)
        slip["completed"] = completed

        if completed:
            effective_results = [r for r in results if r not in ("push", "dnp")]
            n_eff = len(effective_results)
            hits_eff = sum(1 for r in effective_results if r == "hit")

            if n_eff < 2:
                payout = 1.0 if (n_eff == 0 or (n_eff == 1 and hits_eff == 1)) else 0
            elif slip_type == "power":
                payout = POWER_PAYOUTS.get(n_eff, 0) if hits_eff == n_eff else 0
            else:  # flex
                if n_eff == 2:
                    payout = POWER_PAYOUTS.get(2, 0) if hits_eff == 2 else 0
                else:
                    payout = FLEX_PAYOUTS.get(n_eff, {}).get(hits_eff, 0)

            slip["payout"] = payout
            slip["hits"] = hits_eff
        else:
            slip["payout"] = None
            slip["hits"] = None

    return {"slips": all_slips, "total": len(all_slips)}


class BacktestAddSlipRequest(BaseModel):
    bet_ids: list[str]


@app.post("/api/backtest/add-slip")
def add_slip_to_backtest(req: BacktestAddSlipRequest, user: dict = Depends(get_current_user)):
    """
    Manually log a slip from the currently selected +EV bets.
    bet_ids must refer to bets currently in _state['bet_map'].
    """
    if not req.bet_ids or len(req.bet_ids) < 2 or len(req.bet_ids) > 6:
        raise HTTPException(status_code=400, detail="Slip must have 2-6 legs.")

    with _lock:
        bet_map      = _state["bet_map"]
        serialized   = _state["bets"]

    # Build a lookup of full serialized dicts (includes start_time)
    ser_map = {d["bet_id"]: d for d in serialized}

    backtest_bets = []
    missing = []
    for bid in req.bet_ids:
        d = ser_map.get(bid)
        b = bet_map.get(bid)
        if d is None or b is None:
            missing.append(bid)
            continue
        backtest_bets.append({
            "player_name":       d.get("player_name", ""),
            "league":            d.get("league", ""),
            "prop_type":         d.get("prop_type", ""),
            "pp_line":           d.get("pp_line"),
            "side":              d.get("side", "over"),
            "true_prob":         d.get("true_prob"),
            "individual_ev_pct": d.get("individual_ev_pct"),
            "start_time":        d.get("start_time", ""),
        })

    if missing:
        raise HTTPException(status_code=404, detail=f"Bet IDs not found: {missing}")

    # Within-slip dedup: a single slip cannot contain two legs on the
    # same (player, start_time) — e.g. selecting both "Player A points
    # over" and "Player A assists over" for the same game.
    seen_keys: set[tuple[str, str]] = set()
    dup_within: list[str] = []
    for b in backtest_bets:
        k = make_bet_key(b.get("player_name", ""), b.get("start_time", ""))
        if k in seen_keys:
            dup_within.append(b.get("player_name", "") or "?")
        seen_keys.add(k)
    if dup_within:
        raise HTTPException(
            status_code=400,
            detail=(
                "A slip cannot contain multiple legs on the same player+game: "
                + ", ".join(dup_within)
            ),
        )

    from engine.backtest import BacktestLogger
    _logger = BacktestLogger(user["id"], user["jwt"])

    # Cross-slip dedup: any (player, start_time) already used in another
    # slip within the last 48h blocks this whole slip. Same player in a
    # different game (different start_time) is fine.
    
    used_keys = _logger._load_used_keys_from_db()
    conflicts = _logger.find_conflicting_legs(backtest_bets, used_keys=used_keys)
    if conflicts:
        detail_parts = [
            f"{c.get('player_name', '')} @ {c.get('start_time', '')}"
            for c in conflicts
        ]
        raise HTTPException(
            status_code=409,
            detail=(
                f"{len(conflicts)} leg(s) already used in another slip "
                f"within the last 48 hours: " + ", ".join(detail_parts)
            ),
        )

    # Force the slip through — bypass the "enough bets" / EV gate by
    # calling try_log_slip with only these bets (already in correct format).
    new_slip = _logger.try_log_slip(backtest_bets, slip_type="Manual")
    if new_slip is None:
        # try_log_slip may reject due to EV or already-used bets;
        # for manual adds we force-log it anyway.
        #
        # Re-check for player conflicts one more time: if another process
        # inserted a conflicting leg between our top-of-endpoint check and
        # now, we want to surface that rather than silently force-insert.
        late_used = _logger._load_used_keys_from_db()
        late_conflicts = _logger.find_conflicting_legs(backtest_bets, used_keys=late_used)
        if late_conflicts:
            detail_parts = [
                f"{c.get('player_name', '')} @ {c.get('start_time', '')}"
                for c in late_conflicts
            ]
            raise HTTPException(
                status_code=409,
                detail=(
                    f"{len(late_conflicts)} leg(s) already used in another slip "
                    f"within the last 48 hours: " + ", ".join(detail_parts)
                ),
            )

        import uuid
        from datetime import datetime as _dt
        from engine.database import get_db

        true_probs = [float(b.get("true_prob") or 0) for b in backtest_bets]
        k = len(backtest_bets)
        # Compute the better of Power/Flex for the projected EV, but always
        # tag the row as "Manual" so the backtest view reflects user intent.
        try:
            import numpy as _np
            from engine.ev_calculator import (
                power_slip_ev, flex_slip_ev,
                power_slip_ev_corr, flex_slip_ev_corr,
            )
            from engine.correlation import build_correlation_matrix, legs_metadata_from_bets
            corr = build_correlation_matrix(legs_metadata_from_bets(backtest_bets))
            if k >= 2 and not _np.allclose(corr, _np.eye(k), atol=1e-8):
                power_ev = power_slip_ev_corr(true_probs, corr)
                flex_ev  = flex_slip_ev_corr(true_probs,  corr)
            else:
                power_ev = power_slip_ev(true_probs)
                flex_ev  = flex_slip_ev(true_probs)
            candidates = [ev for ev in (power_ev, flex_ev) if ev is not None]
            best_ev = max(candidates) if candidates else 0.0
        except Exception:
            best_ev = 0.0
        best_type = "Manual"

        slip_id   = str(uuid.uuid4())[:8].upper()
        timestamp = _dt.now().isoformat(timespec="seconds")
        proj_ev   = round(best_ev, 4)

        rows = []
        for i, b in enumerate(backtest_bets, start=1):
            true_p = round(float(b.get("true_prob") or 0), 4)
            rows.append({
                "leg_num":          i,
                "player":           b.get("player_name", ""),
                "league":           b.get("league", ""),
                "prop":             b.get("prop_type", ""),
                "line":             b.get("pp_line", ""),
                "side":             b.get("side", ""),
                "true_prob":        true_p,
                "ind_ev_pct":       round(float(b.get("individual_ev_pct") or 0), 4),
                "game_start":       b.get("start_time", ""),
                "closing_prob":     true_p,
                "clv_pct":          0.0,
                "result":           "pending",
                "stat_actual":      None,
            })

        # Write to Supabase
        db_client = get_db()
        if not db_client:
            raise HTTPException(status_code=500, detail="No database connection available.")

        try:
            # 1. Insert slip header
            db_client.table("slips").insert({
                "id":               slip_id,
                "timestamp":        timestamp,
                "slip_type":        best_type,
                "n_legs":           k,
                "proj_slip_ev_pct": proj_ev
            }).execute()
            # 2. Insert legs
            db_legs = []
            for r in rows:
                db_legs.append({
                    "slip_id":      slip_id,
                    "leg_num":      r["leg_num"],
                    "player":       r["player"],
                    "league":       r["league"],
                    "prop":         r["prop"],
                    "line":         r["line"],
                    "side":         r["side"],
                    "true_prob":    r["true_prob"],
                    "ind_ev_pct":   r["ind_ev_pct"],
                    "game_start":   r["game_start"] if r["game_start"] else None,
                    "closing_prob": r["closing_prob"],
                    "clv_pct":      r["clv_pct"],
                    "result":       r["result"],
                    "stat_actual":  r["stat_actual"]
                })
            db_client.table("legs").insert(db_legs).execute()
            logger.info("Backtest: manually added slip %s to Supabase", slip_id)
        except Exception as db_exc:
            logger.error("Backtest: manual slip write failed: %s", db_exc)
            raise HTTPException(status_code=500, detail=f"Database write failed: {db_exc}")

        # No in-memory "used" set to update — BacktestLogger reads fresh
        # conflicts from Supabase on every call via _load_used_keys_from_db(),
        # so the newly-inserted legs are picked up automatically on the next
        # dedup check.

        new_slip = {
            "slip_id":          slip_id,
            "timestamp":        timestamp,
            "slip_type":        best_type,
            "n_legs":           k,
            "proj_slip_ev_pct": proj_ev,
            "legs": [
                {
                    "player":     r["player"],
                    "league":     r["league"],
                    "prop":       r["prop"],
                    "line":       r["line"],
                    "side":       r["side"],
                    "true_prob":  r["true_prob"],
                    "ind_ev_pct": r["ind_ev_pct"],
                    "game_start": r["game_start"],
                    "closing_prob": r["closing_prob"],
                    "clv_pct":      r["clv_pct"],
                }
                for r in rows
            ],
        }

    with _lock:
        _state["latest_slip"] = new_slip

    _invalidate_analytics_cache(user["id"])
    logger.info("Manual backtest slip logged: %s (%d legs)", new_slip["slip_id"], new_slip["n_legs"])
    return {"slip": new_slip}


@app.delete("/api/backtest/slip/{slip_id}")
def delete_backtest_slip(slip_id: str, user: dict = Depends(get_current_user)):
    """Delete a slip and its legs from the user's backtest history."""
    from engine.database import get_user_db
    db = get_user_db(user["jwt"])
    if not db:
        raise HTTPException(status_code=500, detail="No database connection.")

    try:
        # Verify the slip belongs to this user
        check = db.table("slips").select("id").eq("id", slip_id).execute()
        if not check.data:
            raise HTTPException(status_code=404, detail="Slip not found.")

        # Delete legs first (foreign key), then slip header
        db.table("legs").delete().eq("slip_id", slip_id).execute()
        db.table("slips").delete().eq("id", slip_id).execute()

        _invalidate_analytics_cache(user["id"])
        logger.info("Backtest: deleted slip %s for user %s", slip_id, user["id"])
        return {"status": "deleted", "slip_id": slip_id}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Backtest: delete slip failed: %s", e)
        raise HTTPException(status_code=500, detail=f"Delete failed: {e}")


# ── Market Observatory Endpoints ───────────────────────────────────────────

@app.get("/api/observatory")
def get_observatory_data():
    """Returns the latest observations from the market_observatory table.
    Resolved and pending rows are fetched separately so a flood of recent
    pending rows can't push all the hit/miss rows past the row cap."""
    try:
        from engine.database import get_db
        db = get_db()
        if not db:
            return []
        resolved = db.table("market_observatory") \
            .select("*") \
            .neq("result", "pending") \
            .order("created_at", desc=True) \
            .limit(100) \
            .execute().data or []
        pending = db.table("market_observatory") \
            .select("*") \
            .eq("result", "pending") \
            .order("created_at", desc=True) \
            .limit(100) \
            .execute().data or []
        combined = resolved + pending
        combined.sort(key=lambda r: r.get("created_at") or "", reverse=True)
        return combined
    except Exception as e:
        logger.error("API: observatory fetch error: %s", e)
        return []

@app.get("/api/observatory/multipliers")
def get_calibration_map_api():
    """Per-league calibration summary for the Observatory tab.

    Each entry reports the *effective* shrinkage applied at the p=0.60
    anchor — i.e. the post-Bayesian-shrinkage calibrated probability divided
    by 0.60. A value of 1.00 means no shrinkage; 0.95 means a 5% haircut,
    etc. Leagues without any data report `calibrated: false`.

    The shape matches the legacy multiplier endpoint
    (`{key: {value, calibrated}}`) so the existing UI renderer keeps working
    without changes.
    """
    try:
        from engine.isotonic_calibration import load_isotonic_calibration, calibrate, DISPLAY_ANCHOR
        from engine.constants import PROP_TYPE_MAP
        curves = load_isotonic_calibration()

        out: dict = {}
        for league in sorted(PROP_TYPE_MAP.keys()):
            key = f"{league}|Calibration @ p={DISPLAY_ANCHOR:.2f}"
            calibrated_at_anchor = calibrate(curves, league, None, DISPLAY_ANCHOR)
            # A league with NO fitted curve passes through identity (== anchor),
            # which we surface as "Awaiting data" rather than "1.00x".
            has_data = curves.get("leagues", {}).get(league) is not None or curves.get("global") is not None
            ratio = calibrated_at_anchor / DISPLAY_ANCHOR if DISPLAY_ANCHOR > 0 else 1.0
            out[key] = {"value": round(ratio, 4), "calibrated": bool(has_data)}

        # Surface any fitted league not in PROP_TYPE_MAP (e.g. NCAAF).
        for league in (curves.get("leagues") or {}).keys():
            key = f"{league}|Calibration @ p={DISPLAY_ANCHOR:.2f}"
            if key in out:
                continue
            calibrated_at_anchor = calibrate(curves, league, None, DISPLAY_ANCHOR)
            out[key] = {"value": round(calibrated_at_anchor / DISPLAY_ANCHOR, 4), "calibrated": True}

        return out
    except Exception as e:
        logger.error("API: calibration fetch error: %s", e)
        return {}


@app.get("/api/calibration/curves")
def get_calibration_curves_api():
    """Full hierarchical calibration state for diagnostics / debugging.

    Returns the global, per-league, and per-(league, prop) curves with their
    effective sample sizes, plus the empirical book sharpness weights when
    available. Intended for power users; not currently surfaced in the UI.
    """
    try:
        from engine.isotonic_calibration import load_isotonic_calibration
        from engine.sharpness_calibration import load_sharpness_weights
        return {
            "isotonic":  load_isotonic_calibration(),
            "sharpness": load_sharpness_weights(),
        }
    except Exception as e:
        logger.error("API: calibration curves fetch error: %s", e)
        return {"isotonic": {}, "sharpness": {}}





