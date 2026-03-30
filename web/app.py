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

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="PrizePicks +EV Finder")

# ---------------------------------------------------------------------------
# In-memory state
# ---------------------------------------------------------------------------

_lock = threading.Lock()

_state = {
    "bets":          [],        # list[dict] — serialized BetResult
    "bet_map":       {},        # bet_id -> BetResult (for slip calc)
    "matches":       [],        # list[dict] — unfiltered combined lines
    "pp_lines":      [],        # list[dict] — raw PrizePicks lines
    "fd_lines":      [],        # list[dict] — raw FanDuel lines
    "last_refresh":  None,      # datetime | None
    "next_refresh":  None,      # datetime | None
    "is_scraping":   False,
    "is_scraping_pp": False,
    "is_scraping_fd": False,
    "scrape_errors": {},        # league -> error str | None
    "interval_min":  cfg.REFRESH_INTERVAL_MINUTES,
    "min_ev_pct":    cfg.MIN_INDIVIDUAL_EV_PCT,
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
        logger.info("Pipeline: matching %d PP lines vs %d FD props...", len(pp_lines), len(fd_props))
        matches = match_props(fd_props, pp_lines)
        
        serialized_matches = []
        for m in matches:
            if m.pp.line_score != m.fd.line:
                continue
            base = {
                "player_name": m.pp.player_name,
                "league": m.pp.league,
                "stat_type": m.pp.stat_type,
                "pp_line": m.pp.line_score,
                "fd_line": m.fd.line,
                "start_time": m.pp.start_time,
            }
            pp_side = getattr(m.pp, "side", "both")
            if pp_side in ("both", "over") and m.fd.over_odds is not None:
                serialized_matches.append({**base, "side": "over", "odds": m.fd.over_odds})
            if pp_side in ("both", "under") and m.fd.under_odds is not None:
                serialized_matches.append({**base, "side": "under", "odds": m.fd.under_odds})

        bets: list[BetResult] = []
        with _lock:
            min_ev = _state["min_ev_pct"]

        for match in matches:
            results = evaluate_match(match, min_ev_pct=min_ev)
            bets.extend(results)

        # Sort by individual EV% descending
        bets.sort(key=lambda b: b.individual_ev_pct, reverse=True)

        with _lock:
            _state["bets"]         = [b.to_dict() for b in bets]
            _state["bet_map"]      = {b.bet_id: b for b in bets}
            _state["matches"]      = serialized_matches
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
        serialized = []
        for p in fd_props:
            if p.over_odds is not None:
                serialized.append({
                    "league": p.league,
                    "player_name": p.player_name,
                    "stat_type": p.prop_type + " (O)",
                    "line_score": p.line,
                    "line_odds": p.over_odds,
                    "start_time": None,
                })
            if p.under_odds is not None:
                serialized.append({
                    "league": p.league,
                    "player_name": p.player_name,
                    "stat_type": p.prop_type + " (U)",
                    "line_score": p.line,
                    "line_odds": p.under_odds,
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

