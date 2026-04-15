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
from engine.database import get_db

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

    Dedup invariant (enforced end-to-end):
      Once a (player, game_start) pair appears in any slip within the last
      _DEDUP_WINDOW_HOURS, that pair cannot appear in any future slip —
      whether auto-generated or manually added — until the window passes.
      The same player in a DIFFERENT game (different start_time) is allowed.

    Selection logic:
      - Resync used_bets from Supabase on every call
      - Filter out legs whose (player, start_time) is in used_bets
      - Hard floor: individual_ev_pct >= -1%
      - Sort greedily by individual EV descending
      - Pick top 6 legs
      - Final authoritative DB check for player conflicts before insert
      - Write to Supabase; lock (player, start_time) keys for the window
    """

    def __init__(self):
        self.used_bets: set[tuple[str, str]] = set()
        self.last_reset_date: date = date.today()
        self._rebuild_used_bets()

    def _fetch_recent_slips_with_legs(self) -> list[dict]:
        """
        Return all slips from the last _DEDUP_WINDOW_HOURS with their legs
        attached. Filters by a UTC timestamp range so we never miss slips
        created in the first few hours of a UTC day when the local date
        hasn't ticked over yet (this was the source of the 2026-04-12 / 13 /
        14 duplicate slips — see commit log).
        """
        db = get_db()
        if not db:
            return []

        cutoff = datetime.now(timezone.utc) - timedelta(hours=_DEDUP_WINDOW_HOURS)
        cutoff_iso = cutoff.isoformat()

        try:
            slips_res = (
                db.table("slips")
                .select("id, timestamp")
                .gte("timestamp", cutoff_iso)
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
            # ERROR (not warning): a failed rebuild silently disables dedup,
            # which is exactly how we produced duplicate slips in the first
            # place. Surface it so monitoring notices.
            logger.error("Backtest: could not fetch recent slips from Supabase: %s", exc)
            return []

    def _load_used_keys_from_db(self) -> set[tuple[str, str]]:
        """
        Return the authoritative set of (player, time_key) pairs that
        appear as legs in any slip logged in the last _DEDUP_WINDOW_HOURS.
        Supabase is the source of truth for this — the in-memory
        `used_bets` is a cache of this same set.
        """
        used: set[tuple[str, str]] = set()
        for slip in self._fetch_recent_slips_with_legs():
            for leg in slip["legs"]:
                used.add(make_bet_key(
                    leg.get("player", "") or "",
                    leg.get("game_start", "") or "",
                ))
        return used

    def _rebuild_used_bets(self) -> None:
        """
        Replace used_bets with the authoritative set from Supabase.

        Replacement (not union): local entries for slips that never made
        it into Supabase should not keep blocking those players. The DB
        is the only record anyone else can see, so in-memory state that
        disagrees with the DB is wrong by definition.
        """
        fresh = self._load_used_keys_from_db()
        prev = len(self.used_bets)
        self.used_bets = fresh
        if fresh or prev:
            logger.info(
                "Backtest: rebuilt used-player-game keys from last %dh of Supabase: %d (was %d)",
                _DEDUP_WINDOW_HOURS, len(fresh), prev,
            )

    def find_conflicting_legs(self, bets: list[dict]) -> list[dict]:
        """
        Return the subset of `bets` whose (player, start_time) pair is
        already used in a slip in the last _DEDUP_WINDOW_HOURS. An empty
        list means the caller is safe to insert.

        This is the primary dedup guard — any non-empty return must block
        the insert, even if only one leg conflicts. The caller is
        responsible for rejecting the slip.

        `bets` uses the in-memory leg shape (player_name, start_time).
        """
        if not bets:
            return []
        used = self._load_used_keys_from_db()
        conflicts = []
        for bet in bets:
            key = make_bet_key(
                bet.get("player_name", "") or "",
                bet.get("start_time", "") or "",
            )
            if key in used:
                conflicts.append(bet)
        return conflicts

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
        # Resync used_bets from Supabase so greedy selection filters
        # correctly on the first pass — even if another process logged
        # slips, or our own used_bets was cleared at midnight while slips
        # from the prior UTC day still sit in the 48h window.
        self._rebuild_used_bets()

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

        # Final authoritative DB check. Catches the race where another
        # process inserted a conflicting leg between our rebuild above
        # and this moment. Any overlap — even one leg — rejects the slip.
        conflicts = self.find_conflicting_legs(best_legs)
        if conflicts:
            logger.warning(
                "Backtest: rejected slip — %d leg(s) conflict with existing slips: %s",
                len(conflicts),
                [(c.get("player_name"), c.get("start_time")) for c in conflicts],
            )
            # Update in-memory set so the next attempt selects around them.
            for c in conflicts:
                self.used_bets.add(make_bet_key(
                    c.get("player_name", "") or "",
                    c.get("start_time", "") or "",
                ))
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
