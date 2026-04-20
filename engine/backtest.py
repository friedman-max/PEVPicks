"""
Backtest and slip logger for CoreProp.

Automatically documents the best +EV slip combinations as they appear
throughout the day. Logs to Supabase with one row per leg.

Dedup is enforced per (player, game_start) across a rolling 48h window
sourced from Supabase — a player in a given game can appear in at most
one slip during that window. Same player in a different game is fine.
"""
import logging
import uuid
from datetime import date, datetime, timezone, timedelta
from typing import Optional
import unidecode

from engine.constants import BREAK_EVEN
from engine.ev_calculator import power_slip_ev, flex_slip_ev
from engine.database import get_db, get_user_db

logger = logging.getLogger(__name__)

# Hard floor: legs with individual EV below this are never included
MIN_LEG_EV_PCT = -0.01   # -1%

# How far back we scan for duplicate slips and used-player keys.
# 48 h covers every intra-day restart plus the midnight rollover.
_DEDUP_WINDOW_HOURS = 48


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
            
            # Use YYYY-MM-DD instead of full minute precision to prevent time-shifting dupes
            time_key = dt.strftime("%Y-%m-%d")
        except Exception as e:
            # Fallback for malformed strings: take first 10 chars (YYYY-MM-DD) if parsing fails
            logger.warning("Backtest: _make_key failed to parse '%s': %s", start_time, e)
            time_key = start_time[:10] if start_time else "no_time"

    return (_normalize(player), time_key)


class BacktestLogger:
    """
    Builds and logs the best available +EV slips to Supabase.

    Dedup invariant (enforced end-to-end):
      Once a (player, game_start) pair appears in any slip within the last
      _DEDUP_WINDOW_HOURS, that pair cannot appear in any future slip.

    Selection logic:
      - Reads used keys directly from Supabase for the specific user
      - Applies hard floor: individual_ev_pct >= -1%
      - Picks top 6 legs greedily
      - Writes to Supabase using the user's RLS-scoped JWT
    """

    def __init__(self, user_id: str, jwt: Optional[str] = None, db_client=None):
        self.user_id = user_id
        self.jwt = jwt
        self.db_client = db_client

    def _fetch_recent_slips_with_legs(self) -> list[dict]:
        """
        Return all slips for this user from the last _DEDUP_WINDOW_HOURS with their legs
        attached.
        """
        db = self.db_client or get_user_db(self.jwt)
        if not db:
            return []

        cutoff = datetime.now(timezone.utc) - timedelta(hours=_DEDUP_WINDOW_HOURS)
        cutoff_iso = cutoff.isoformat()

        try:
            slips_res = (
                db.table("slips")
                .select("id, timestamp")
                .gte("timestamp", cutoff_iso)
                .eq("user_id", self.user_id)
                .order("timestamp", desc=True)
                .limit(500)
                .execute()
            )
            slips = slips_res.data or []
            if not slips:
                return []

            sids = [s["id"] for s in slips]
            legs_res = (
                db.table("legs")
                .select("slip_id, player, game_start, prop, line, side")
                .in_("slip_id", sids)
                .execute()
            )
            legs_by_slip: dict[str, list[dict]] = {}
            for leg in (legs_res.data or []):
                legs_by_slip.setdefault(leg["slip_id"], []).append(leg)

            out = []
            for s in slips:
                out.append({
                    "id": s["id"],
                    "timestamp": s.get("timestamp", ""),
                    "legs": legs_by_slip.get(s["id"], []),
                })
            return out
        except Exception as exc:
            logger.error("Backtest: could not fetch recent slips from Supabase: %s", exc)
            return []

    def _load_used_keys_from_db(self) -> set[tuple[str, str]]:
        used: set[tuple[str, str]] = set()
        for slip in self._fetch_recent_slips_with_legs():
            for leg in slip["legs"]:
                used.add(make_bet_key(
                    leg.get("player", "") or "",
                    leg.get("game_start", "") or "",
                ))
        return used

    def find_conflicting_legs(self, bets: list[dict], used_keys: set) -> list[dict]:
        if not bets:
            return []
        conflicts = []
        for bet in bets:
            key = make_bet_key(
                bet.get("player_name", "") or "",
                bet.get("start_time", "") or "",
            )
            if key in used_keys:
                conflicts.append(bet)
        return conflicts

    def try_log_slip(self, bets: list[dict], slip_type: str = "Power", n_legs: int = 6) -> Optional[dict]:
        """Build and log the best available auto-slip with unique players."""
        used_keys = self._load_used_keys_from_db()

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
            if p_key in used_keys or p_key in seen_in_this_slip:
                continue
            best_legs.append(bet)
            seen_in_this_slip.add(p_key)
            if len(best_legs) == n_legs:
                break

        if len(best_legs) < n_legs:
            return None

        true_probs = [float(b.get("true_prob") or 0.0) for b in best_legs]
        avg_prob = sum(true_probs) / n_legs
        be_key = (str(n_legs), slip_type.lower())
        slip_be = BREAK_EVEN.get(be_key)

        if slip_be is None or avg_prob < slip_be:
            return None

        # Use the correct EV function for the slip type
        if slip_type.lower() == "power":
            best_ev = power_slip_ev(true_probs)
        else:
            best_ev = flex_slip_ev(true_probs)

        if best_ev is None or best_ev <= 0:
            return None

        slip_id = str(uuid.uuid4())[:8].upper()
        timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
        proj_ev = round(best_ev, 4)

        rows = []
        for i, bet in enumerate(best_legs, start=1):
            true_p = round(float(bet.get("true_prob") or 0), 4)
            rows.append({
                "slip_id":          slip_id,
                "user_id":          self.user_id,
                "timestamp":        timestamp,
                "slip_type":        slip_type,
                "n_legs":           n_legs,
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

        # Write to Supabase using provided client or user-scoped DB
        db = self.db_client or get_user_db(self.jwt)
        if not db:
            logger.error("Backtest: no database connection, cannot log slip %s", slip_id)
            return None

        try:
            # 1. Insert slip header
            db.table("slips").insert({
                "id":               slip_id,
                "user_id":          self.user_id,
                "timestamp":        timestamp,
                "slip_type":        slip_type,
                "n_legs":           n_legs,
                "proj_slip_ev_pct": proj_ev
            }).execute()
            # 2. Insert legs
            db_legs = []
            for r in rows:
                db_legs.append({
                    "slip_id":      slip_id,
                    "user_id":      self.user_id,
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
            logger.info("Backtest: logged Auto-Slip %s (6-leg EV=%.2f%%) for user %s", slip_id, best_ev * 100, self.user_id)
        except Exception as db_exc:
            logger.error("Backtest: Supabase write failed for slip %s: %s", slip_id, db_exc)
            return None

        return {
            "slip_id":          slip_id,
            "timestamp":        timestamp,
            "slip_type":        slip_type,
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
