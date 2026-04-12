"""
FastAPI backend with APScheduler auto-refresh.

Endpoints:
  GET  /api/bets        - All current +EV bets (sorted by ind_ev_pct desc)
  GET  /api/status      - Scrape status, last/next refresh time
  POST /api/refresh     - Manually trigger a re-scrape
  POST /api/slip        - Calculate slip EV for selected bet IDs
  GET  /api/config      - Current runtime config
  POST /api/config      - Update config (interval, min_ev, leagues)
"""
import csv
import logging
import threading
from datetime import datetime, timedelta
from typing import Optional

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import statistics

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

# Backtest / results-checker singletons
_backtest         = BacktestLogger()
_results_checker  = ESPNResultsChecker()
_clv_tracker      = CLVTracker()

# If a scraper returns 0 results but previous had at least this many,
# reuse the previous data instead of wiping the state.
_MIN_LINES_FOR_FALLBACK = 5

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
    "min_ev_pct":    -10.0,
    "active_leagues": dict(cfg.ACTIVE_LEAGUES),
    # Raw prop objects from last successful scrape (for fallback)
    "_prev_pp_raw":  [],        # list[PrizePickLine]
    "_prev_fd_raw":  [],        # list[FanDuelProp]
    "_prev_dk_raw":  [],        # list[FanDuelProp]
    "_prev_pin_raw": [],        # list[FanDuelProp]
    # Backtest: latest logged slip (for frontend notification)
    "latest_slip":   None,
}

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

def run_pipeline():
    with _lock:
        if _state["is_scraping"]:
            logger.info("Scrape already in progress, skipping.")
            return
        _state["is_scraping"] = True
        errors = {}

    try:
        # Read runtime league config and snapshot previous raw data for fallback
        with _lock:
            leagues = dict(_state["active_leagues"])
            prev_pp_raw = list(_state["_prev_pp_raw"])
            prev_fd_raw = list(_state["_prev_fd_raw"])
            prev_dk_raw = list(_state["_prev_dk_raw"])
            prev_pin_raw = list(_state["_prev_pin_raw"])

        logger.info("Pipeline: scraping PrizePicks...")
        pp_lines = scrape_prizepicks(active_leagues=leagues)
        if len(pp_lines) == 0 and len(prev_pp_raw) >= _MIN_LINES_FOR_FALLBACK:
            logger.warning("PrizePicks returned 0 lines, reusing %d cached lines", len(prev_pp_raw))
            pp_lines = prev_pp_raw
            errors["prizepicks"] = "Empty response — using cached data"

        logger.info("Pipeline: scraping FanDuel...")
        fd_props = scrape_fanduel(active_leagues=leagues)
        if len(fd_props) == 0 and len(prev_fd_raw) >= _MIN_LINES_FOR_FALLBACK:
            logger.warning("FanDuel returned 0 props, reusing %d cached props", len(prev_fd_raw))
            fd_props = prev_fd_raw
            errors["fanduel"] = "Empty response — using cached data"

        logger.info("Pipeline: scraping DraftKings...")
        dk_props = scrape_draftkings(active_leagues=leagues)
        if len(dk_props) == 0 and len(prev_dk_raw) >= _MIN_LINES_FOR_FALLBACK:
            logger.warning("DraftKings returned 0 props, reusing %d cached props", len(prev_dk_raw))
            dk_props = prev_dk_raw
            errors["draftkings"] = "Empty response — using cached data"

        logger.info("Pipeline: scraping Pinnacle...")
        pin_props = scrape_pinnacle(active_leagues=leagues)
        if len(pin_props) == 0 and len(prev_pin_raw) >= _MIN_LINES_FOR_FALLBACK:
            logger.warning("Pinnacle returned 0 props, reusing %d cached props", len(prev_pin_raw))
            pin_props = prev_pin_raw
            errors["pinnacle"] = "Empty response — using cached data"

        serialized_pp = []
        for l in pp_lines:
            if l.side == "both":
                common = {
                    "league": l.league,
                    "player_name": l.player_name,
                    "stat_type": l.stat_type,
                    "line_score": l.line_score,
                    "start_time": l.start_time,
                }
                serialized_pp.append({**common, "side": "over"})
                serialized_pp.append({**common, "side": "under"})
            else:
                serialized_pp.append({
                    "league": l.league,
                    "player_name": l.player_name,
                    "stat_type": l.stat_type,
                    "line_score": l.line_score,
                    "side": l.side,
                    "start_time": l.start_time,
                })

        from engine.devig import devig_power, devig_single_sided, prob_to_american
        serialized_fd = []
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
                serialized_fd.append({
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
                serialized_fd.append({
                    "league": p.league,
                    "player_name": p.player_name,
                    "stat_type": p.prop_type,
                    "line_score": p.line,
                    "side": "under",
                    "line_odds": p.under_odds,
                    "true_odds": prob_to_american(true_under) if true_under else None,
                    "start_time": getattr(p, "start_time", None),
                })

        serialized_dk = []
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
                serialized_dk.append({
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
                serialized_dk.append({
                    "league": p.league,
                    "player_name": p.player_name,
                    "stat_type": p.prop_type,
                    "line_score": p.line,
                    "side": "under",
                    "line_odds": p.under_odds,
                    "true_odds": prob_to_american(true_under) if true_under else None,
                    "start_time": getattr(p, "start_time", None),
                })

        serialized_pin = []
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
                serialized_pin.append({
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
                serialized_pin.append({
                    "league": p.league,
                    "player_name": p.player_name,
                    "stat_type": p.prop_type,
                    "line_score": p.line,
                    "side": "under",
                    "line_odds": p.under_odds,
                    "true_odds": prob_to_american(true_under) if true_under else None,
                    "start_time": getattr(p, "start_time", None),
                })

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
            is_valid = True
            if m.fd and m.pp.line_score != m.fd.line:
                is_valid = False
            if m.dk and m.pp.line_score != m.dk.line:
                is_valid = False
            if m.pin and m.pp.line_score != m.pin.line:
                is_valid = False

            if not is_valid:
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
                            }

        # Deduplicate bets based on bet_id, keeping the one with highest EV
        unique_bets = {}
        for b in bets:
            if b.bet_id not in unique_bets or b.individual_ev_pct > unique_bets[b.bet_id].individual_ev_pct:
                unique_bets[b.bet_id] = b
        bets = list(unique_bets.values())

        # Sort by individual EV% descending
        bets.sort(key=lambda b: b.individual_ev_pct, reverse=True)

        # Snapshot used_bets keys so we can flag already-logged legs in the UI
        used_keys = _backtest.used_bet_keys()

        serialized_bets = []
        for b in bets:
            d = b.to_dict()
            extras = bet_book_odds.get(b.bet_id, {})
            d["fd_odds_book"] = extras.get("fd_odds")
            d["dk_odds_book"] = extras.get("dk_odds")
            d["pin_odds_book"] = extras.get("pin_odds")
            d["start_time"]   = extras.get("start_time", "")
            # Flag players already logged for this specific game
            start_time = extras.get("start_time", "")
            bet_key = make_bet_key(b.player_name, start_time)
            d["in_backtest"] = bet_key in used_keys
            serialized_bets.append(d)

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
            # Persist raw objects for fallback on next cycle
            _state["_prev_pp_raw"]  = pp_lines
            _state["_prev_fd_raw"]  = fd_props
            _state["_prev_dk_raw"]  = dk_props
            _state["_prev_pin_raw"] = pin_props
        logger.info("Pipeline complete: %d +EV bets found.", len(bets))

        # ── Supabase Sync: Persist the new state for instant load on restart ──
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

        # ── Backtest: try to log a new slip from the freshly computed bets ──
        if len(serialized_bets) >= 3:
            try:
                # Build the bet dicts expected by BacktestLogger
                backtest_bets = []
                for d in serialized_bets:
                    backtest_bets.append({
                        "player_name":      d.get("player_name", ""),
                        "league":           d.get("league", ""),
                        "prop_type":        d.get("prop_type", ""),
                        "pp_line":          d.get("pp_line"),
                        "side":             d.get("side", "over"),
                        "true_prob":        d.get("true_prob"),
                        "individual_ev_pct": d.get("individual_ev_pct"),
                        "start_time":       d.get("start_time", ""),
                    })
                new_slip = _backtest.try_log_slip(backtest_bets)
                if new_slip:
                    with _lock:
                        _state["latest_slip"] = new_slip
                    logger.info("Backtest: new slip logged — %s", new_slip.get("slip_id"))
            except Exception as bt_exc:
                logger.warning("Backtest try_log_slip error: %s", bt_exc)

        # ── CLV Tracker: update closing lines for pending bets non-blocking ──
        def _update_clv_bg():
            try:
                updated = _clv_tracker.update_closing_lines(matches)
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

        threading.Thread(target=_check_results_bg, daemon=True).start()

    except Exception as e:
        logger.exception("Pipeline error: %s", e)
        errors["pipeline"] = str(e)
        with _lock:
            _state["scrape_errors"] = errors
    finally:
        with _lock:
            _state["is_scraping"] = False


def _reschedule(interval_min: int):
    """Reschedule the auto-refresh job with a new interval."""
    scheduler.remove_all_jobs()
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
    scheduler.start()
    _reschedule(_state["interval_min"])
    # Midnight reset: clear used-bets pool every day at 00:00
    scheduler.add_job(
        _backtest.reset_daily,
        trigger=CronTrigger(hour=0, minute=0),
        id="midnight_reset",
        replace_existing=True,
    )
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

    # Run pipeline immediately on startup so data is ready
    threading.Thread(target=run_pipeline, daemon=True).start()

    # ── Startup recovery: load cached state from Supabase ──
    def _seed_state_from_db():
        logger.info("Startup: Seeding state from Supabase cache...")
        keys = ["bets", "matches", "pp_lines", "fd_lines", "dk_lines", "pin_lines", "last_refresh"]
        for k in keys:
            data, updated_at = load_state_from_supabase(k)
            if data is not None:
                with _lock:
                    if k == "last_refresh":
                        try:
                            _state[k] = datetime.fromisoformat(data)
                        except:
                            _state[k] = None
                    else:
                        _state[k] = data
        logger.info("Startup: Seeding complete.")

    threading.Thread(target=_seed_state_from_db, daemon=True).start()


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
    return FileResponse(str(STATIC_DIR / "index.html"))


# ---------------------------------------------------------------------------
# API routes
# ---------------------------------------------------------------------------

@app.get("/api/bets")
def get_bets():
    with _lock:
        return {
            "bets":         _state["bets"],
            "total":        len(_state["bets"]),
            "is_scraping":  _state["is_scraping"],
        }


@app.get("/api/matched")
def get_matched():
    with _lock:
        return {
            "matches":      _state.get("matches", []),
            "total":        len(_state.get("matches", [])),
            "is_scraping":  _state["is_scraping"],
        }


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


@app.post("/api/refresh")
def manual_refresh():
    with _lock:
        if _state["is_scraping"]:
            raise HTTPException(status_code=409, detail="Scrape already in progress.")
    threading.Thread(target=run_pipeline, daemon=True).start()
    return {"status": "refresh started"}


class SlipRequest(BaseModel):
    bet_ids: list[str]
    bankroll: float = 100.0


@app.post("/api/slip")
def build_slip(req: SlipRequest):
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


@app.post("/api/slip/auto")
def auto_build_slip(req: SlipRequest):
    if not req.bet_ids:
        raise HTTPException(status_code=400, detail="No bet IDs provided.")
    if len(req.bet_ids) < 2:
        raise HTTPException(status_code=400, detail="Must provide at least 2 bets.")

    with _lock:
        bet_map = _state["bet_map"]

    selected = []
    for bid in req.bet_ids[:6]:
        if bid in bet_map:
            selected.append(bet_map[bid])
            
    if len(selected) < 2:
        raise HTTPException(status_code=400, detail="Not enough valid bets found.")

    best_ev = -float('inf')
    best_k = 0
    best_result = None
    best_subset = []
    
    for k in range(2, len(selected) + 1):
        subset = selected[:k]
        result = calculate_slip(subset, req.bankroll)
        
        ev = result.get("best_ev_pct")
        if ev is None: 
            continue
        
        if ev > best_ev + 0.00001:
            best_ev = ev
            best_k = k
            best_result = result
            best_subset = [b.bet_id for b in subset]

    if not best_result:
        raise HTTPException(status_code=400, detail="Could not calculate any valid slip.")
        
    best_result["optimal_bet_ids"] = best_subset
    return best_result


class ConfigUpdate(BaseModel):
    interval_min:    Optional[int]   = None
    min_ev_pct:      Optional[float] = None
    active_leagues:  Optional[dict]  = None


@app.get("/api/config")
def get_config():
    with _lock:
        return {
            "interval_min":   _state["interval_min"],
            "min_ev_pct":     _state["min_ev_pct"],
            "active_leagues": _state["active_leagues"],
        }


@app.post("/api/config")
def update_config(update: ConfigUpdate):
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


# ---------------------------------------------------------------------------
# PrizePicks-only endpoints
# ---------------------------------------------------------------------------

@app.get("/api/prizepicks")
def get_prizepicks():
    with _lock:
        return {
            "lines": _state["pp_lines"],
            "total": len(_state["pp_lines"]),
            "is_scraping": _state["is_scraping_pp"],
        }


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
            prev_pp_raw = list(_state["_prev_pp_raw"])

        logger.info("PrizePicks-only scrape starting...")
        pp_lines = scrape_prizepicks(active_leagues=leagues)
        if len(pp_lines) == 0 and len(prev_pp_raw) >= _MIN_LINES_FOR_FALLBACK:
            logger.warning("PrizePicks returned 0 lines, reusing %d cached lines", len(prev_pp_raw))
            pp_lines = prev_pp_raw
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
            _state["pp_lines"] = serialized
            _state["_prev_pp_raw"] = pp_lines
        sync_state_to_supabase("pp_lines", serialized)
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
def get_fanduel():
    with _lock:
        return {
            "lines": _state["fd_lines"],
            "total": len(_state["fd_lines"]),
            "is_scraping": _state["is_scraping_fd"],
        }


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
            prev_fd_raw = list(_state["_prev_fd_raw"])

        logger.info("FanDuel scrape starting...")
        fd_props = scrape_fanduel(active_leagues=leagues)
        if len(fd_props) == 0 and len(prev_fd_raw) >= _MIN_LINES_FOR_FALLBACK:
            logger.warning("FanDuel returned 0 props, reusing %d cached props", len(prev_fd_raw))
            fd_props = prev_fd_raw
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
            _state["fd_lines"] = serialized
            _state["_prev_fd_raw"] = fd_props
        sync_state_to_supabase("fd_lines", serialized)
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
def get_draftkings():
    with _lock:
        return {
            "lines": _state["dk_lines"],
            "total": len(_state["dk_lines"]),
            "is_scraping": _state["is_scraping_dk"],
        }


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
            prev_dk_raw = list(_state["_prev_dk_raw"])

        logger.info("DraftKings scrape starting...")
        dk_props = scrape_draftkings(active_leagues=leagues)
        if len(dk_props) == 0 and len(prev_dk_raw) >= _MIN_LINES_FOR_FALLBACK:
            logger.warning("DraftKings returned 0 props, reusing %d cached props", len(prev_dk_raw))
            dk_props = prev_dk_raw
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
            _state["dk_lines"] = serialized
            _state["_prev_dk_raw"] = dk_props
        sync_state_to_supabase("dk_lines", serialized)
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
def get_pinnacle():
    with _lock:
        return {
            "lines": _state["pin_lines"],
            "total": len(_state["pin_lines"]),
            "is_scraping": _state["is_scraping_pin"],
        }


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
            prev_pin_raw = list(_state["_prev_pin_raw"])

        logger.info("Pinnacle scrape starting...")
        pin_props = scrape_pinnacle(active_leagues=leagues)
        if len(pin_props) == 0 and len(prev_pin_raw) >= _MIN_LINES_FOR_FALLBACK:
            logger.warning("Pinnacle returned 0 props, reusing %d cached props", len(prev_pin_raw))
            pin_props = prev_pin_raw
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
            _state["pin_lines"] = serialized
            _state["_prev_pin_raw"] = pin_props
        sync_state_to_supabase("pin_lines", serialized)
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
def get_calibration():
    """Return Brier Score, Log-Loss, and calibration buckets from resolved backtest data."""
    from engine.calibration import evaluate_calibration
    return evaluate_calibration()


# ---------------------------------------------------------------------------
# Backtest endpoints
# ---------------------------------------------------------------------------

@app.get("/api/backtest/latest-slip")
def get_latest_slip():
    """Return the most recently logged slip (for frontend polling / notification)."""
    with _lock:
        return {"slip": _state.get("latest_slip")}


@app.get("/api/backtest/slips")
def get_backtest_slips():
    """Return the last 50 logged slips, prioritizing Supabase data with a CSV fallback."""
    import csv as _csv
    from engine.backtest import CSV_PATH
    from engine.database import get_db

    db = get_db()
    use_csv = False
    all_slips = []

    if db:
        try:
            # 1. Fetch the latest 50 slips
            slips_res = db.table("slips").select("*").order("timestamp", desc=True).limit(50).execute()
            slip_data = slips_res.data
            if not slip_data:
                use_csv = True
            else:
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
                    # Rename 'id' to 'slip_id' for frontend compatibility if needed, 
                    # but our CSV format uses 'slip_id'.
                    s["slip_id"] = s["id"]
                    s["legs"] = sorted(legs_by_slip.get(s["id"], []), key=lambda x: x["leg_num"])
                all_slips = slip_data
        except Exception as db_err:
            _logger.warning("Backtest API: Supabase fetch failed, falling back to CSV: %s", db_err)
            use_csv = True
    else:
        use_csv = True

    if use_csv:
        if not CSV_PATH.exists():
            return {"slips": [], "total": 0}
        try:
            with open(CSV_PATH, "r", encoding="utf-8-sig") as f:
                rows = list(_csv.DictReader(f))
            
            slips_by_id = {}
            for row in rows:
                sid = row.get("slip_id", "")
                if sid not in slips_by_id:
                    slips_by_id[sid] = {
                        "slip_id":          sid,
                        "timestamp":        row.get("timestamp"),
                        "slip_type":        row.get("slip_type"),
                        "n_legs":           row.get("n_legs"),
                        "proj_slip_ev_pct": row.get("proj_slip_ev_pct"),
                        "legs":             [],
                    }
                slips_by_id[sid]["legs"].append({
                    "leg_num":    row.get("leg_num"),
                    "player":     row.get("player"),
                    "league":     row.get("league"),
                    "prop":       row.get("prop"),
                    "line":       row.get("line"),
                    "side":       row.get("side"),
                    "true_prob":  row.get("true_prob"),
                    "ind_ev_pct": row.get("ind_ev_pct"),
                    "game_start": row.get("game_start"),
                    "closing_prob": row.get("closing_prob"),
                    "clv_pct":      row.get("clv_pct"),
                    "result":     row.get("result"),
                    "stat_actual":row.get("stat_actual"),
                })
            # Convert to list and sort
            all_slips = sorted(slips_by_id.values(), key=lambda s: s.get("timestamp") or "", reverse=True)[:50]
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Cannot read backtest CSV: {exc}")

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
def add_slip_to_backtest(req: BacktestAddSlipRequest):
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

    # Force the slip through — bypass the "enough bets" / dedup gate by
    # calling try_log_slip with only these bets (already in correct format)
    new_slip = _backtest.try_log_slip(backtest_bets)
    if new_slip is None:
        # try_log_slip may reject due to EV or already-used bets;
        # for manual adds we force-log it anyway
        import uuid, csv as _csv
        from datetime import datetime as _dt
        from engine.backtest import CSV_PATH, CSV_COLUMNS, URGENCY_MINUTES
        from engine.ev_calculator import power_slip_ev
        from engine.constants import BREAK_EVEN

        true_probs = [float(b.get("true_prob") or 0) for b in backtest_bets]
        k = len(backtest_bets)
        try:
            from engine.ev_calculator import power_slip_ev, flex_slip_ev
            power_ev = power_slip_ev(true_probs)
            flex_ev  = flex_slip_ev(true_probs)
            best_ev   = max(power_ev, flex_ev) if flex_ev is not None else power_ev
            best_type = "Power" if power_ev >= (flex_ev or -999) else "Flex"
        except Exception:
            best_ev, best_type = 0.0, "Power"

        slip_id   = str(uuid.uuid4())[:8].upper()
        timestamp = _dt.now().isoformat(timespec="seconds")
        proj_ev   = round(best_ev, 4)

        from datetime import timezone, timedelta
        def _is_urgent(gs_str):
            if not gs_str: return False
            try:
                gs = _dt.fromisoformat(gs_str.replace("Z", "+00:00"))
                now = _dt.now(tz=timezone.utc)
                mins = (gs - now).total_seconds() / 60
                return 0 < mins <= URGENCY_MINUTES
            except Exception:
                return False

        rows = []
        for i, b in enumerate(backtest_bets, start=1):
            true_p = round(float(b.get("true_prob") or 0), 4)
            rows.append({
                "slip_id":          slip_id,
                "timestamp":        timestamp,
                "slip_type":        best_type,
                "n_legs":           k,
                "proj_slip_ev_pct": proj_ev,
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
                "stat_actual":      "",
            })

        try:
            with open(CSV_PATH, "a", newline="", encoding="utf-8-sig") as f:
                _csv.DictWriter(f, fieldnames=CSV_COLUMNS).writerows(rows)
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"CSV write failed: {exc}")

        # Supabase Dual-Write
        from engine.database import get_db
        db_client = get_db()
        if db_client:
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
                        "stat_actual":  None if r["stat_actual"] == "" else r["stat_actual"]
                    })
                db_client.table("legs").insert(db_legs).execute()
                _logger.info("Backtest: manually added slip %s sync'd to Supabase", slip_id)
            except Exception as db_exc:
                _logger.error("Backtest: manual slip Supabase sync failed: %s", db_exc)

        # Mark legs as used
        for b in backtest_bets:
            key = (b.get("player_name","").lower(), b.get("prop_type","").lower(), b.get("side",""))
            _backtest.used_bets.add(key)

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

    logger.info("Manual backtest slip logged: %s (%d legs)", new_slip["slip_id"], new_slip["n_legs"])
    return {"slip": new_slip}




@app.get("/api/backtest/download-csv")
def download_backtest_csv():
    """Download the backtest CSV file directly."""
    from engine.backtest import CSV_PATH
    if not CSV_PATH.exists():
        raise HTTPException(status_code=404, detail="No backtest CSV file yet.")
    return FileResponse(
        str(CSV_PATH),
        media_type="text/csv",
        filename="backtest.csv",
        headers={"Content-Disposition": "attachment; filename=backtest.csv"},
    )


@app.post("/api/backtest/check-results")
def trigger_result_check():
    """Manually trigger ESPN result checking for pending backtest rows."""
    def _run():
        try:
            updated = _results_checker.check_pending_results()
            logger.info("Manual result check: %d rows updated", updated)
        except Exception as exc:
            logger.error("Manual result check error: %s", exc)

    threading.Thread(target=_run, daemon=True).start()
    return {"status": "result check started"}
