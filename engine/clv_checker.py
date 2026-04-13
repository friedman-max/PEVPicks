"""
CLV (Closing Line Value) Tracker.

Periodically called to update the `closing_prob` in Supabase for pending bets.
As lines move, this records the last seen VWAP consensus probability before the
game starts and the line is pulled from the board.

Strategy:
  - ALWAYS update closing_prob for any pending bet where current odds are available,
    regardless of how far away the game is. This ensures that every pipeline run
    captures the latest market consensus. The last written value before lines
    disappear (at game start) becomes the de facto closing line.
  - On startup or when the app was down during a game start, a recovery pass will
    mark truly missed games (already finished, no odds available) so they don't
    remain stuck as empty forever.
"""
import logging
from datetime import datetime, timezone, timedelta
from typing import Any

from engine.consensus import compute_true_probability, books_from_match
from engine.database import get_db

logger = logging.getLogger(__name__)

# How long after game start (in minutes) we still attempt to update closing_prob.
# Lines are typically pulled within ~30 min of game start.
POST_START_GRACE_MINUTES = 30

# How long after game start (in hours) before we consider CLV as "missed" and
# finalize with a fallback. This prevents rows from being stuck empty forever
# when the app wasn't running during the tracking window.
MISSED_CUTOFF_HOURS = 1


class CLVTracker:
    def __init__(self):
        pass

    def update_closing_lines(self, matches: list[Any]) -> int:
        """
        Updates pending backtest legs in Supabase with the latest true probability.

        Unlike previous versions, this has NO pre-game time window restriction.
        Any pending bet with available current odds gets updated. This ensures
        that even if the app was down for a while, the most recent odds are
        always captured, and the last update before lines disappear becomes
        the closing line.

        `matches` is the list of MatchResult objects from app.py.
        Returns the number of rows updated.
        """
        db = get_db()
        if not db:
            return 0

        # Build a lookup for current matches: (player_lower, prop_lower, side, line) -> true_prob
        current_probs = self._build_current_probs(matches)

        # Fetch pending legs from Supabase
        try:
            res = db.table("legs").select("*").eq("result", "pending").execute()
            rows = res.data or []
        except Exception as exc:
            logger.error("CLVTracker: cannot read pending legs from Supabase: %s", exc)
            return 0

        if not rows:
            return 0

        now_utc = datetime.now(timezone.utc)
        updated_count = 0

        for row in rows:
            game_start_str = row.get("game_start", "")
            if not game_start_str:
                continue

            try:
                gs = datetime.fromisoformat(game_start_str.replace("Z", "+00:00"))
                if gs.tzinfo is None:
                    gs = gs.replace(tzinfo=timezone.utc)
            except Exception:
                continue

            mins_to_start = (gs - now_utc).total_seconds() / 60.0

            # Skip if the game started more than POST_START_GRACE_MINUTES ago —
            # lines are gone by then, so any match found would be for a different game.
            if mins_to_start < -POST_START_GRACE_MINUTES:
                continue

            player = (row.get("player") or "").lower().strip()
            prop = (row.get("prop") or "").lower().strip()
            side = (row.get("side") or "").lower().strip()
            try:
                line = float(row.get("line", 0))
            except ValueError:
                line = 0.0

            key = (player, prop, side, line)
            if key in current_probs:
                new_cp_val = current_probs[key]
                old_cp_val = row.get("closing_prob")

                # Convert old value for comparison
                if old_cp_val is not None:
                    try:
                        old_cp_val = float(old_cp_val)
                    except (ValueError, TypeError):
                        old_cp_val = None

                # Update if this is a new value or different from the old value.
                if old_cp_val is None or abs(new_cp_val - old_cp_val) > 1e-4:
                    orig_true_prob = float(row.get("true_prob", 0))

                    # CLV edge: (Closing Prob - Original True Prob)
                    clv_pct = new_cp_val - orig_true_prob

                    try:
                        sid = row.get("slip_id")
                        l_num = int(row.get("leg_num", 0))
                        db.table("legs").update({
                            "closing_prob": round(new_cp_val, 4),
                            "clv_pct":      round(clv_pct, 4)
                        }).eq("slip_id", sid).eq("leg_num", l_num).execute()
                        updated_count += 1
                    except Exception as db_exc:
                        logger.error("CLVTracker DB update failed: %s", db_exc)

                    logger.debug(
                        "CLVTracker: Update %s %s %s @%s -> %.4f", 
                        player, prop, side, line, new_cp_val
                    )

        if updated_count:
            logger.info("CLVTracker: updated %d pending bets", updated_count)

        return updated_count

    def finalize_missed(self) -> int:
        """
        Recovery pass: for pending bets where the game started long ago
        (MISSED_CUTOFF_HOURS) and closing_prob was never captured, set
        closing_prob = true_prob and clv_pct = 0.0 so they aren't stuck empty.

        This should be called on startup and periodically to clean up rows
        that were missed because the app wasn't running.

        Returns the number of rows finalized.
        """
        db = get_db()
        if not db:
            return 0

        try:
            # Fetch all legs (pending or resolved) that might have missing CLV data
            res = db.table("legs").select("*").execute()
            rows = res.data or []
        except Exception as exc:
            logger.error("CLVTracker: cannot read legs from Supabase: %s", exc)
            return 0

        if not rows:
            return 0

        now_utc = datetime.now(timezone.utc)
        cutoff = timedelta(hours=MISSED_CUTOFF_HOURS)
        finalized = 0

        for row in rows:
            # Only target rows with missing CLV data
            if row.get("closing_prob") is not None and row.get("clv_pct") is not None:
                continue 

            game_start_str = row.get("game_start", "")
            if not game_start_str:
                continue

            try:
                gs = datetime.fromisoformat(game_start_str.replace("Z", "+00:00"))
                if gs.tzinfo is None:
                    gs = gs.replace(tzinfo=timezone.utc)
            except Exception:
                continue

            # If the game started more than MISSED_CUTOFF_HOURS ago and we still
            # have no/incomplete CLV data, finalize it.
            if (now_utc - gs) > cutoff:
                orig_true_prob = float(row.get("true_prob", 0))
                
                # If we have a closing_prob but no clv_pct, just compute the diff
                if row.get("closing_prob") is not None:
                    try:
                        cp = float(row["closing_prob"])
                        clv_pct = round(cp - orig_true_prob, 4)
                        closing_prob = cp
                    except (ValueError, TypeError):
                        closing_prob = round(orig_true_prob, 4)
                        clv_pct = 0.0
                else:
                    # Fallback: closing_prob = true_prob, clv_pct = 0
                    # This means "no line movement captured — CLV unknown"
                    closing_prob = round(orig_true_prob, 4)
                    clv_pct = 0.0
                
                try:
                    sid = row.get("slip_id")
                    l_num = int(row.get("leg_num", 0))
                    db.table("legs").update({
                        "closing_prob": closing_prob,
                        "clv_pct":      clv_pct
                    }).eq("slip_id", sid).eq("leg_num", l_num).execute()
                    finalized += 1
                except Exception as db_exc:
                    logger.error("CLVTracker DB finalization failed: %s", db_exc)

        if finalized:
            logger.info(
                "CLVTracker: finalized %d missed bets (closing_prob = true_prob)", finalized
            )

        return finalized

    # ── Helpers ──────────────────────────────────────────────────────────────

    def _build_current_probs(self, matches: list[Any]) -> dict[tuple[str, str, str, float], float]:
        """
        Build a lookup: (player_lower, prop_lower, side, line) -> worst_case_probability.
        
        Only books that match the current PrizePicks line are used in the calculation.
        Worst-case probability is used to ensure consistency with the 'True Prob' 
        shown on the main dashboard and used for initial entry.
        """
        current_probs: dict[tuple[str, str, str, float], float] = {}
        for m in matches:
            if not getattr(m, "pp", None):
                continue

            # Core identifying fields from PrizePicks
            player = m.pp.player_name.lower().strip()
            prop = m.pp.stat_type.lower().strip()
            line = float(m.pp.line_score)
            sides = ["over", "under"] if getattr(m.pp, "side", "both") == "both" else [m.pp.side]

            # Line consistency check: only use books that agree with the PP line.
            # This ensures that if FD moved the line but DK didn't, we only use
            # the books that are still quoting the line we care about.
            valid_fd = m.fd if (m.fd and abs(m.fd.line - line) < 1e-4) else None
            valid_dk = m.dk if (m.dk and abs(m.dk.line - line) < 1e-4) else None
            valid_pin = m.pin if (m.pin and abs(m.pin.line - line) < 1e-4) else None

            # Only proceed if at least one book matches the PrizePicks line
            if not any([valid_fd, valid_dk, valid_pin]):
                continue

            match_books = books_from_match(valid_fd, valid_dk, valid_pin)

            for side in sides:
                # Use worst_case_prob for consistency with the rest of the app
                consensus_prob, worst_case_prob, meta = compute_true_probability(match_books, side)
                if worst_case_prob is not None:
                    current_probs[(player, prop, side, line)] = worst_case_prob

        return current_probs
