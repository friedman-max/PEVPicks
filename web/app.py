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

import config as cfg
from engine.ev_calculator import BetResult, calculate_slip, evaluate_match
from engine.matcher import match_props
from engine.backtest import BacktestLogger
from engine.results_checker import ESPNResultsChecker
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
    "interval_min":  1,
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

        from engine.devig import devig_multiplicative, devig_single_sided, prob_to_american
        serialized_fd = []
        for p in fd_props:
            true_over, true_under = None, None
            if p.both_sided and p.over_odds is not None and p.under_odds is not None:
                true_over, true_under = devig_multiplicative(p.over_odds, p.under_odds)
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
                true_over, true_under = devig_multiplicative(p.over_odds, p.under_odds)
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
                true_over, true_under = devig_multiplicative(p.over_odds, p.under_odds)
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
        
        from engine.devig import devig_multiplicative, devig_single_sided, prob_to_american
        from engine.ev_calculator import BetResult
        
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

            # Helper to calculate conservative true odds based on available books
            def get_combined_true_odds(side):
                fd_o, fd_u = (m.fd.over_odds, m.fd.under_odds) if m.fd else (None, None)
                dk_o, dk_u = (m.dk.over_odds, m.dk.under_odds) if m.dk else (None, None)
                pin_o, pin_u = (m.pin.over_odds, m.pin.under_odds) if m.pin else (None, None)

                if side == "over":
                    odds_list = [fd_o, dk_o, pin_o]
                else:
                    odds_list = [fd_u, dk_u, pin_u]

                # If no book has odds for this side, return None
                available_odds = [o for o in odds_list if o is not None]
                if not available_odds: return None, None, None

                # Best odds is the one that pays out MORE (higher American number)
                best_odds = max(available_odds)

                # Use all available books to find the most conservative (lowest) true probability
                probs = []
                for book, b_o, b_u in [(m.fd, fd_o, fd_u), (m.dk, dk_o, dk_u), (m.pin, pin_o, pin_u)]:
                    if not book:
                        continue
                    if side == "over" and b_o is None:
                        continue
                    if side == "under" and b_u is None:
                        continue
                    if book.both_sided and b_o is not None and b_u is not None:
                        t_o, t_u = devig_multiplicative(b_o, b_u)
                    else:
                        t_o = devig_single_sided(b_o) if b_o is not None else None
                        t_u = devig_single_sided(b_u) if b_u is not None else None
                    probs.append(t_o if side == "over" else t_u)

                # Filter out Nones
                probs = [p for p in probs if p is not None]
                final_true_prob = min(probs) if probs else None
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
                        "fd_odds": m.fd.over_odds if (m.fd and m.fd.over_odds is not None) else None,
                        "dk_odds": m.dk.over_odds if (m.dk and m.dk.over_odds is not None) else None,
                        "pin_odds": m.pin.over_odds if (m.pin and m.pin.over_odds is not None) else None,
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
                                "fd_odds":    m.fd.over_odds if m.fd else None,
                                "dk_odds":    m.dk.over_odds if m.dk else None,
                                "pin_odds":   m.pin.over_odds if m.pin else None,
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
                        "fd_odds": m.fd.under_odds if (m.fd and m.fd.under_odds is not None) else None,
                        "dk_odds": m.dk.under_odds if (m.dk and m.dk.under_odds is not None) else None,
                        "pin_odds": m.pin.under_odds if (m.pin and m.pin.under_odds is not None) else None,
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
                                "fd_odds":    m.fd.under_odds if m.fd else None,
                                "dk_odds":    m.dk.under_odds if m.dk else None,
                                "pin_odds":   m.pin.under_odds if m.pin else None,
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
            # Flag bets already logged in today's backtest slips
            bet_key = (
                b.player_name.lower(),
                b.prop_type.lower(),
                b.side,
            )
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
    # Run pipeline immediately on startup so data is ready
    threading.Thread(target=run_pipeline, daemon=True).start()


@app.on_event("shutdown")
def shutdown():
    scheduler.shutdown(wait=False)


# ---------------------------------------------------------------------------
# Static files + root
# ---------------------------------------------------------------------------

import pathlib
STATIC_DIR = pathlib.Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


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
        from engine.devig import devig_multiplicative, devig_single_sided, prob_to_american
        serialized = []
        for p in fd_props:
            true_over, true_under = None, None
            if p.both_sided and p.over_odds is not None and p.under_odds is not None:
                true_over, true_under = devig_multiplicative(p.over_odds, p.under_odds)
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
        from engine.devig import devig_multiplicative, devig_single_sided, prob_to_american
        serialized = []
        for p in dk_props:
            true_over, true_under = None, None
            if p.both_sided and p.over_odds is not None and p.under_odds is not None:
                true_over, true_under = devig_multiplicative(p.over_odds, p.under_odds)
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
        from engine.devig import devig_multiplicative, devig_single_sided, prob_to_american
        serialized = []
        for p in pin_props:
            true_over, true_under = None, None
            if p.both_sided and p.over_odds is not None and p.under_odds is not None:
                true_over, true_under = devig_multiplicative(p.over_odds, p.under_odds)
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
        logger.info("Pinnacle scrape complete: %d lines.", len(serialized))
    except Exception as e:
        logger.exception("Pinnacle scrape error: %s", e)
    finally:
        with _lock:
            _state["is_scraping_pin"] = False


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
    """Return the last 50 logged slips from the backtest CSV."""
    import pathlib
    from engine.backtest import CSV_PATH

    if not CSV_PATH.exists():
        return {"slips": [], "total": 0}

    try:
        with open(CSV_PATH, "r", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Cannot read backtest CSV: {exc}")

    # Group rows by slip_id to build slip summaries
    slips_by_id: dict = {}
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
            "urgency":    row.get("urgency"),
            "game_start": row.get("game_start"),
            "result":     row.get("result"),
            "stat_actual":row.get("stat_actual"),
        })

    # Compute payout per slip
    from engine.constants import POWER_PAYOUTS, FLEX_PAYOUTS

    all_slips = list(slips_by_id.values())
    for slip in all_slips:
        legs = slip.get("legs", [])
        n_legs = int(slip.get("n_legs") or len(legs))
        slip_type = (slip.get("slip_type") or "").lower()
        results = [l.get("result", "pending") for l in legs]

        # A slip is "completed" when all legs have a result
        completed = all(r in ("hit", "miss") for r in results)
        slip["completed"] = completed

        if completed:
            hits = sum(1 for r in results if r == "hit")
            if slip_type == "power":
                payout = POWER_PAYOUTS.get(n_legs, 0) if hits == n_legs else 0
            else:  # flex
                payout = FLEX_PAYOUTS.get(n_legs, {}).get(hits, 0)
            slip["payout"] = payout
            slip["hits"] = hits
        else:
            slip["payout"] = None
            slip["hits"] = None

    # Sort by timestamp descending, return last 50
    all_slips.sort(key=lambda s: s.get("timestamp") or "", reverse=True)
    return {"slips": all_slips[:50], "total": len(all_slips)}


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
                "true_prob":        round(float(b.get("true_prob") or 0), 4),
                "ind_ev_pct":       round(float(b.get("individual_ev_pct") or 0), 4),
                "urgency":          "HIGH" if _is_urgent(b.get("start_time")) else "NORMAL",
                "game_start":       b.get("start_time", ""),
                "result":           "pending",
                "stat_actual":      "",
            })

        try:
            with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
                _csv.DictWriter(f, fieldnames=CSV_COLUMNS).writerows(rows)
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"CSV write failed: {exc}")

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
                    "urgency":    r["urgency"],
                    "game_start": r["game_start"],
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
