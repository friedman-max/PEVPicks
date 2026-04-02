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
import logging
import threading
from datetime import datetime, timedelta
from typing import Optional

from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import config as cfg
from engine.ev_calculator import BetResult, calculate_slip, evaluate_match
from engine.matcher import match_props
from scrapers.fanduel import scrape_fanduel
from scrapers.prizepicks import scrape_prizepicks
from scrapers.draftkings import scrape_draftkings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="PrizePicks +EV Finder")

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
    "last_refresh":  None,      # datetime | None
    "next_refresh":  None,      # datetime | None
    "is_scraping":   False,
    "is_scraping_pp": False,
    "is_scraping_fd": False,
    "is_scraping_dk": False,
    "scrape_errors": {},        # league -> error str | None
    "interval_min":  1,
    "min_ev_pct":    -10.0,
    "active_leagues": dict(cfg.ACTIVE_LEAGUES),
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
        # Read runtime league config (UI-togglable)
        with _lock:
            leagues = dict(_state["active_leagues"])

        logger.info("Pipeline: scraping PrizePicks...")
        pp_lines = scrape_prizepicks(active_leagues=leagues)
        logger.info("Pipeline: scraping FanDuel...")
        fd_props = scrape_fanduel(active_leagues=leagues)
        logger.info("Pipeline: scraping DraftKings...")
        dk_props = scrape_draftkings(active_leagues=leagues)
        
        serialized_pp = [
            {
                "league": l.league,
                "player_name": l.player_name,
                "stat_type": l.stat_type,
                "line_score": l.line_score,
                "start_time": l.start_time,
            }
            for l in pp_lines
        ]

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
                    "stat_type": p.prop_type + " (O)",
                    "line_score": p.line,
                    "line_odds": p.over_odds,
                    "true_odds": prob_to_american(true_over) if true_over else None,
                    "start_time": None,
                })
            if p.under_odds is not None:
                serialized_fd.append({
                    "league": p.league,
                    "player_name": p.player_name,
                    "stat_type": p.prop_type + " (U)",
                    "line_score": p.line,
                    "line_odds": p.under_odds,
                    "true_odds": prob_to_american(true_under) if true_under else None,
                    "start_time": None,
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
                    "stat_type": p.prop_type + " (O)",
                    "line_score": p.line,
                    "line_odds": p.over_odds,
                    "true_odds": prob_to_american(true_over) if true_over else None,
                    "start_time": None,
                })
            if p.under_odds is not None:
                serialized_dk.append({
                    "league": p.league,
                    "player_name": p.player_name,
                    "stat_type": p.prop_type + " (U)",
                    "line_score": p.line,
                    "line_odds": p.under_odds,
                    "true_odds": prob_to_american(true_under) if true_under else None,
                    "start_time": None,
                })

        logger.info("Pipeline: matching %d PP lines vs %d FD and %d DK props...", len(pp_lines), len(fd_props), len(dk_props))
        matches = match_props(fd_props, dk_props, pp_lines)
        
        from engine.devig import devig_multiplicative, devig_single_sided, prob_to_american
        
        serialized_matches = []
        for m in matches:
            # We already filter for both books in the matcher, but we check line equality here
            # Both books must match the PP line for it to be a clean 'Combined Line' comparison
            if m.pp.line_score != m.fd.line or m.pp.line_score != m.dk.line:
                continue
                
            base = {
                "player_name": m.pp.player_name,
                "league": m.pp.league,
                "stat_type": m.pp.stat_type,
                "pp_line": m.pp.line_score,
                "fd_line": m.fd.line,
                "dk_line": m.dk.line,
                "start_time": m.pp.start_time,
            }
            
            pp_side = getattr(m.pp, "side", "both")
            
            # Helper to calculate conservative true odds based on two books
            def get_combined_true_odds(fd_o, dk_o, fd_u, dk_u, side):
                if side == "over":
                    o1, o2 = fd_o, dk_o
                    u1, u2 = fd_u, dk_u
                else:
                    o1, o2 = fd_u, dk_u
                    u1, u2 = fd_o, dk_o

                if o1 is None or o2 is None: return None, None
                
                # Best odds is the one that pays out MORE (higher American number)
                best_odds = max(o1, o2)
                
                # True odds based on the book that provided the best_odds (conservative approach)
                # If we have both sides for that book, use multiplicative devig
                target_book = "fd" if o1 >= o2 else "dk"
                
                if target_book == "fd":
                    if m.fd.both_sided and fd_o is not None and fd_u is not None:
                        t_o, t_u = devig_multiplicative(fd_o, fd_u)
                    else:
                        t_o, t_u = devig_single_sided(fd_o), devig_single_sided(fd_u) if fd_u else None
                else:
                    if m.dk.both_sided and dk_o is not None and dk_u is not None:
                        t_o, t_u = devig_multiplicative(dk_o, dk_u)
                    else:
                        t_o, t_u = devig_single_sided(dk_o), devig_single_sided(dk_u) if dk_u else None
                
                final_true_prob = t_o if side == "over" else t_u
                return best_odds, prob_to_american(final_true_prob) if final_true_prob else None

            # Process Over side
            if pp_side in ("both", "over") and m.fd.over_odds is not None and m.dk.over_odds is not None:
                best, true = get_combined_true_odds(m.fd.over_odds, m.dk.over_odds, m.fd.under_odds, m.dk.under_odds, "over")
                if best is not None:
                    serialized_matches.append({
                        **base, 
                        "side": "over", 
                        "best_odds": best,
                        "fd_odds": m.fd.over_odds,
                        "dk_odds": m.dk.over_odds,
                        "true_odds": true
                    })

            # Process Under side
            if pp_side in ("both", "under") and m.fd.under_odds is not None and m.dk.under_odds is not None:
                best, true = get_combined_true_odds(m.fd.over_odds, m.dk.over_odds, m.fd.under_odds, m.dk.under_odds, "under")
                if best is not None:
                    serialized_matches.append({
                        **base, 
                        "side": "under", 
                        "best_odds": best,
                        "fd_odds": m.fd.under_odds,
                        "dk_odds": m.dk.under_odds,
                        "true_odds": true
                    })

        bets: list[BetResult] = []
        with _lock:
            min_ev = _state["min_ev_pct"]

        for match in matches:
            if match.pp.line_score != match.fd.line:
                continue
            results = evaluate_match(match, min_ev_pct=min_ev)
            bets.extend(results)

        # Deduplicate bets based on bet_id, keeping the one with highest EV
        unique_bets = {}
        for b in bets:
            if b.bet_id not in unique_bets or b.individual_ev_pct > unique_bets[b.bet_id].individual_ev_pct:
                unique_bets[b.bet_id] = b
        bets = list(unique_bets.values())

        # Sort by individual EV% descending
        bets.sort(key=lambda b: b.individual_ev_pct, reverse=True)

        with _lock:
            _state["bets"]         = [b.to_dict() for b in bets]
            _state["bet_map"]      = {b.bet_id: b for b in bets}
            _state["matches"]      = serialized_matches
            _state["pp_lines"]     = serialized_pp
            _state["fd_lines"]     = serialized_fd
            _state["dk_lines"]     = serialized_dk
            _state["last_refresh"] = datetime.now()
            _state["next_refresh"] = datetime.now() + timedelta(minutes=_state["interval_min"])
            _state["scrape_errors"] = errors
        logger.info("Pipeline complete: %d +EV bets found.", len(bets))

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

        logger.info("PrizePicks-only scrape starting...")
        pp_lines = scrape_prizepicks(active_leagues=leagues)
        serialized = [
            {
                "league": l.league,
                "player_name": l.player_name,
                "stat_type": l.stat_type,
                "line_score": l.line_score,
                "start_time": l.start_time,
            }
            for l in pp_lines
        ]
        with _lock:
            _state["pp_lines"] = serialized
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

        logger.info("FanDuel scrape starting...")
        fd_props = scrape_fanduel(active_leagues=leagues)
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
                    "stat_type": p.prop_type + " (O)",
                    "line_score": p.line,
                    "line_odds": p.over_odds,
                    "true_odds": prob_to_american(true_over) if true_over else None,
                    "start_time": None,
                })
            if p.under_odds is not None:
                serialized.append({
                    "league": p.league,
                    "player_name": p.player_name,
                    "stat_type": p.prop_type + " (U)",
                    "line_score": p.line,
                    "line_odds": p.under_odds,
                    "true_odds": prob_to_american(true_under) if true_under else None,
                    "start_time": None,
                })
        with _lock:
            _state["fd_lines"] = serialized
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

        logger.info("DraftKings scrape starting...")
        dk_props = scrape_draftkings(active_leagues=leagues)
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
                    "stat_type": p.prop_type + " (O)",
                    "line_score": p.line,
                    "line_odds": p.over_odds,
                    "true_odds": prob_to_american(true_over) if true_over else None,
                    "start_time": None,
                })
            if p.under_odds is not None:
                serialized.append({
                    "league": p.league,
                    "player_name": p.player_name,
                    "stat_type": p.prop_type + " (U)",
                    "line_score": p.line,
                    "line_odds": p.under_odds,
                    "true_odds": prob_to_american(true_under) if true_under else None,
                    "start_time": None,
                })
        with _lock:
            _state["dk_lines"] = serialized
        logger.info("DraftKings scrape complete: %d lines.", len(serialized))
    except Exception as e:
        logger.exception("DraftKings scrape error: %s", e)
    finally:
        with _lock:
            _state["is_scraping_dk"] = False

