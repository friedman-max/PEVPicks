"""
Backtest and slip logger for CoreProp.

Automatically documents the best +EV slip combinations as they appear
throughout the day. Logs to Supabase with one row per leg.
Tracks which players have been used to avoid repeats in the same betting day.
Resets daily at midnight.
"""
import logging
import uuid
from datetime import date, datetime, timezone, timedelta
from typing import Optional
import unidecode

from engine.constants import BREAK_EVEN
from engine.ev_calculator import power_slip_ev, flex_slip_ev
from engine.database import get_db

logger = logging.getLogger(__name__)

# Hard floor: legs with individual EV below this are never included
MIN_LEG_EV_PCT = -0.01   # -1%

def _normalize(s: str) -> str:
    """Standardize strings: unidecode, lowercase, and strip whitespace."""
    if not s:
        return ""
    s = unidecode.unidecode(s).lower().strip()
    return s

def make_bet_key(player: str, start_time: str) -> tuple[str, str]:
    """Build a unique signature for a player in a specific game (UTC-normalized)."""
    time_key = "no_time"
    if start_time:
        try:
            # Handle standard ISO formats (with or without offset)
            # fromisoformat handles '2023-10-27T19:45:00-04:00' and '2023-10-27T23:45:00+00:00'
            # We replace ' ' with 'T' for consistent ISO parsing
            clean_ts = start_time.replace(" ", "T")
            
            # Python 3.11+ handles the 'Z' suffix, but for older versions we replace it
            if clean_ts.endswith("Z"):
                clean_ts = clean_ts[:-1] + "+00:00"
                
            dt = datetime.fromisoformat(clean_ts)
            
            # If naive, assume it's already UTC (common for scrapers) OR Eastern if we wanted to be specific,
            # but usually scrapers without offsets are UTC or intended to be.
            # Convert to UTC
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            
            # Format back to a string key: YYYY-MM-DDTHH:MM
            time_key = dt.strftime("%Y-%m-%dT%H:%M")
        except Exception as e:
            # Fallback for malformed strings: take first 10-16 chars if parsing fails
            logger.warning("Backtest: _make_key failed to parse '%s': %s", start_time, e)
            time_key = start_time[:16] if start_time else "no_time"

    return (_normalize(player), time_key)


class BacktestLogger:
    """
    Builds and logs the best available +EV slips to Supabase.

    Selection logic:
      - Filter out already-used PLAYERS (Hard Deduplication)
      - Hard floor: individual_ev_pct >= -1%
      - Sort greedily by individual EV descending
      - Pick top 6 legs (must be 6 unique players)
      - Write to Supabase; lock players for the day
    """

    def __init__(self):
        self.used_bets: set[tuple[str, str]] = set()
        self.last_reset_date: date = date.today()
        self._rebuild_used_bets()

    def _rebuild_used_bets(self) -> None:
        """Query Supabase for recent legs (last 48h) and repopulate used_bets."""
        db = get_db()
        if not db:
            return

        today = date.today()
        yesterday = today - timedelta(days=1)
        target_dates = [today.isoformat(), yesterday.isoformat()]

        try:
            # Fetch recent slips by timestamp
            res = db.table("slips").select("id, timestamp").order("timestamp", desc=True).limit(200).execute()
            if not res.data:
                return

            recent_sids = []
            for s in res.data:
                ts = s.get("timestamp", "")
                if any(ts.startswith(d) for d in target_dates):
                    recent_sids.append(s["id"])

            if not recent_sids:
                return

            # Fetch legs for those slips
            legs_res = db.table("legs").select("player, game_start").in_("slip_id", recent_sids).execute()
            for leg in legs_res.data:
                self.used_bets.add(make_bet_key(
                    leg.get("player", ""),
                    leg.get("game_start", "") or ""
                ))

            if self.used_bets:
                logger.debug("Backtest: rebuilt %d used-player-game keys from Supabase", len(self.used_bets))

        except Exception as exc:
            logger.warning("Backtest: could not rebuild used_bets from Supabase: %s", exc)

    def _midnight_reset(self) -> None:
        today = date.today()
        if self.last_reset_date != today:
            if self.last_reset_date is not None:
                logger.info("Midnight reset: clearing %d used-bet keys", len(self.used_bets))
            self.used_bets = set()
            self.last_reset_date = today

    def reset_daily(self) -> None:
        logger.info("Daily reset: clearing %d used-bet keys", len(self.used_bets))
        self.used_bets = set()
        self.last_reset_date = date.today()

    def used_bet_keys(self) -> set[tuple[str, str]]:
        return set(self.used_bets)


    def try_log_slip(self, bets: list[dict]) -> Optional[dict]:
        """Build and log the best available 6-leg power slip with unique players."""
        self._midnight_reset()

        def _ev(b: dict) -> float:
            return float(b.get("individual_ev_pct") or 0.0)

        valid = [b for b in bets if _ev(b) >= MIN_LEG_EV_PCT]
        pool = sorted(valid, key=_ev, reverse=True)

        best_legs = []
        seen_in_this_slip = set()
        
        for bet in pool:
            p_key = make_bet_key(
                bet.get("player_name", ""),
                bet.get("start_time", "")
            )
            if p_key in self.used_bets or p_key in seen_in_this_slip:
                continue
            best_legs.append(bet)
            seen_in_this_slip.add(p_key)
            if len(best_legs) == 6:
                break

        if len(best_legs) < 6:
            return None

        true_probs = [float(b.get("true_prob") or 0.0) for b in best_legs]
        avg_prob = sum(true_probs) / 6
        power_be = BREAK_EVEN.get(("6", "power"))

        if power_be is None or avg_prob < power_be:
            return None

        best_ev = power_slip_ev(true_probs)
        if best_ev is None or best_ev <= 0:
            return None

        slip_id = str(uuid.uuid4())[:8].upper()
        timestamp = datetime.now().isoformat(timespec="seconds")
        proj_ev = round(best_ev, 4)

        rows = []
        for i, bet in enumerate(best_legs, start=1):
            p_key = make_bet_key(
                bet.get("player_name", ""),
                bet.get("start_time", "")
            )
            self.used_bets.add(p_key)
            true_p = round(float(bet.get("true_prob") or 0), 4)
            rows.append({
                "slip_id":          slip_id,
                "timestamp":        timestamp,
                "slip_type":        "Power",
                "n_legs":           6,
                "proj_slip_ev_pct": proj_ev,
                "leg_num":          i,
                "player":           bet.get("player_name", ""),
                "league":           bet.get("league", ""),
                "prop":             bet.get("prop_type", ""),
                "line":             bet.get("pp_line", ""),
                "side":             bet.get("side", ""),
                "true_prob":        true_p,
                "ind_ev_pct":       round(_ev(bet), 4),
                "game_start":       bet.get("start_time", ""),
                "closing_prob":     true_p,
                "clv_pct":          0.0,
                "result":           "pending",
                "stat_actual":      "",
            })

        # Write to Supabase
        db = get_db()
        if not db:
            logger.error("Backtest: no database connection, cannot log slip %s", slip_id)
            return None

        try:
            # 1. Insert slip header
            db.table("slips").insert({
                "id":               slip_id,
                "timestamp":        timestamp,
                "slip_type":        "Power",
                "n_legs":           6,
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
            db.table("legs").insert(db_legs).execute()
            logger.info("Backtest: logged slip %s (6-leg Power EV=%.2f%%)", slip_id, best_ev * 100)
        except Exception as db_exc:
            logger.error("Backtest: Supabase write failed for slip %s: %s", slip_id, db_exc)
            return None

        return {
            "slip_id":          slip_id,
            "timestamp":        timestamp,
            "slip_type":        "Power",
            "n_legs":           6,
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
                }
                for r in rows
            ],
        }
