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

from engine.database import get_db
from engine.consensus import books_from_match, compute_true_probability
from engine.dynamic_calibration import load_calibration_map

logger = logging.getLogger(__name__)

# Once the game starts, lines are no longer "closing" — any match we'd find
# after tip-off is live in-play pricing, not the closing line we want to
# compare against. Closing_prob is therefore frozen at the last value
# captured before start.
POST_START_GRACE_MINUTES = 0

# How long after game start (in hours) before we consider CLV as "missed" and
# finalize with a fallback. This prevents rows from being stuck empty forever
# when the app wasn't running during the tracking window.
MISSED_CUTOFF_HOURS = 1


class CLVTracker:
    def __init__(self):
        # CLV compares closing_prob against the true_prob stored at bet-log
        # time. That stored value is ALREADY calibrated by
        # BetResult.__init__ (raw worst_case_prob × calibration_multiplier),
        # so the closing side must apply the identical multiplier — otherwise
        # CLV shows a phantom jump the size of the multiplier gap the moment
        # the bet is logged.
        self._calibration_map = load_calibration_map()

    def update_closing_lines(self, matches: list[Any]) -> int:
        """
        Updates pending backtest legs in Supabase with the latest true probability.

        `matches` is the list of MatchResult objects from app.py. Most callers
        should prefer `update_closing_lines_from_probs` with a precomputed dict
        so that the heavy MatchedProp list can be freed earlier.
        """
        current_probs = self._build_current_probs(matches)
        return self.update_closing_lines_from_probs(current_probs)

    def update_closing_lines_from_probs(
        self, current_probs: dict[tuple[str, str, str, float], float]
    ) -> int:
        """
        Same as `update_closing_lines` but takes the precomputed
        (player, prop, side, line) -> worst_case_prob dict directly.
        Used by the main pipeline to avoid retaining `matches` until the
        background thread finishes.
        """
        db = get_db()
        if not db:
            return 0

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

            # Hard cutoff at game start: any price we'd see now is live
            # in-play, not closing. Freeze closing_prob at whatever was
            # last captured before tip-off.
            if mins_to_start <= 0:
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

                # Apply the same calibration multiplier BetResult used at
                # bet-log time, so closing_prob lives in the same space as the
                # stored true_prob and CLV reflects only line movement.
                league = row.get("league")
                prop_for_cal = row.get("prop")
                cal_key = f"{league}|{prop_for_cal}"
                multiplier = self._calibration_map.get(cal_key, 1.0)
                calibrated_cp = min(new_cp_val * multiplier, 0.999)

                # Update only when the calibrated value has moved materially
                # from whatever was last written.
                if old_cp_val is None or abs(calibrated_cp - old_cp_val) > 1e-4:
                    orig_true_prob = float(row.get("true_prob", 0))
                    closing_prob = calibrated_cp
                    clv_pct = closing_prob - orig_true_prob

                    try:
                        sid = row.get("slip_id")
                        l_num = int(row.get("leg_num", 0))
                        db.table("legs").update({
                            "closing_prob": round(closing_prob, 4),
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
        Recovery pass: for legs where a `closing_prob` was captured but the
        derived `clv_pct` never got written (partial-write state), fill in the
        diff against the recorded `true_prob`.

        We intentionally do NOT write a placeholder when closing_prob itself is
        missing — there's no way to recover the market's closing line after the
        fact, and writing `closing_prob = true_prob, clv_pct = 0` injects fake
        zeros that bias the CLV+ rate metric downward. Rows whose closing was
        never captured stay null, which the analytics loader correctly excludes.

        Returns the number of rows finalized.
        """
        db = get_db()
        if not db:
            return 0

        try:
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
            # Only target partial-write rows: closing_prob present but clv_pct missing.
            if row.get("closing_prob") is None or row.get("clv_pct") is not None:
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

            if (now_utc - gs) <= cutoff:
                continue

            try:
                cp = float(row["closing_prob"])
                orig_true_prob = float(row.get("true_prob", 0))
                clv_pct = round(cp - orig_true_prob, 4)
            except (ValueError, TypeError):
                continue

            try:
                sid = row.get("slip_id")
                l_num = int(row.get("leg_num", 0))
                db.table("legs").update({"clv_pct": clv_pct}) \
                    .eq("slip_id", sid).eq("leg_num", l_num).execute()
                finalized += 1
            except Exception as db_exc:
                logger.error("CLVTracker DB finalization failed: %s", db_exc)

        if finalized:
            logger.info("CLVTracker: finalized clv_pct on %d partial-write rows", finalized)

        return finalized

    # ── Helpers ──────────────────────────────────────────────────────────────

    def _build_current_probs(self, matches: list[Any]) -> dict[tuple[str, str, str, float], float]:
        """
        Build a lookup: (player_lower, prop_lower, side, line) -> worst_case_prob.

        Uses the **exact same pipeline as live bet placement** in
        `_run_pipeline_body`: `compute_true_probability(books_from_match(
        m.fd, m.dk, m.pin), side)` → worst_case_prob. This guarantees that
        when the lines haven't moved, closing_prob == stored true_prob and
        clv_pct == 0. Previous versions dropped Pinnacle from the book set
        and/or used a different estimator, which produced instantly-negative
        CLV on every bet.
        """
        current_probs: dict[tuple[str, str, str, float], float] = {}
        for m in matches:
            if not getattr(m, "pp", None):
                continue

            player = m.pp.player_name.lower().strip()
            prop = m.pp.stat_type.lower().strip()
            line = float(m.pp.line_score)
            sides = ["over", "under"] if getattr(m.pp, "side", "both") == "both" else [m.pp.side]

            # Drop books that no longer quote the same line we care about so the
            # closing prob reflects the same contract the bet was placed on.
            valid_fd  = m.fd  if (m.fd  and abs(m.fd.line  - line) < 1e-4) else None
            valid_dk  = m.dk  if (m.dk  and abs(m.dk.line  - line) < 1e-4) else None
            valid_pin = m.pin if (m.pin and abs(m.pin.line - line) < 1e-4) else None
            if valid_fd is None and valid_dk is None and valid_pin is None:
                continue

            books = books_from_match(valid_fd, valid_dk, valid_pin)
            for side in sides:
                _consensus, worst_case, _meta = compute_true_probability(books, side)
                if worst_case is not None:
                    current_probs[(player, prop, side, line)] = worst_case

        return current_probs
