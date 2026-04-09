"""
CLV (Closing Line Value) Tracker.

Periodically called to update the `closing_prob` in backtest.csv for pending bets.
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
import csv
import logging
import pathlib
from datetime import datetime, timezone, timedelta
from typing import Any

from engine.consensus import compute_true_probability, books_from_match

logger = logging.getLogger(__name__)

DATA_DIR = pathlib.Path(__file__).parent.parent / "data"
CSV_PATH = DATA_DIR / "backtest.csv"

# How long after game start (in minutes) we still attempt to update closing_prob.
# Lines are typically pulled within ~30 min of game start.
POST_START_GRACE_MINUTES = 30

# How long after game start (in hours) before we consider CLV as "missed" and
# finalize with a fallback. This prevents rows from being stuck empty forever
# when the app wasn't running during the tracking window.
MISSED_CUTOFF_HOURS = 4


class CLVTracker:
    def __init__(self, csv_path: pathlib.Path = CSV_PATH):
        self._csv_path = csv_path

    def update_closing_lines(self, matches: list[Any]) -> int:
        """
        Updates pending backtest rows with the latest true probability.

        Unlike previous versions, this has NO pre-game time window restriction.
        Any pending bet with available current odds gets updated. This ensures
        that even if the app was down for a while, the most recent odds are
        always captured, and the last update before lines disappear becomes
        the closing line.

        `matches` is the list of MatchResult objects from app.py.
        Returns the number of rows updated.
        """
        if not self._csv_path.exists():
            return 0

        # Build a lookup for current matches: (player_lower, prop_lower, side) -> true_prob
        current_probs = self._build_current_probs(matches)

        # Read CSV
        rows, fieldnames = self._read_csv()
        if not rows or not fieldnames:
            return 0

        if "closing_prob" not in fieldnames or "clv_pct" not in fieldnames:
            logger.warning("CLVTracker: CSV missing closing_prob/clv_pct columns. Aborting.")
            return 0

        now_utc = datetime.now(timezone.utc)
        updated_count = 0
        changed = False

        for row in rows:
            if row.get("result") != "pending":
                continue  # already resolved

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

            player = row.get("player", "").lower().strip()
            prop = row.get("prop", "").lower().strip()
            side = row.get("side", "").lower().strip()

            key = (player, prop, side)
            if key in current_probs:
                new_cp_val = current_probs[key]
                old_cp_str = row.get("closing_prob", "")
                try:
                    old_cp_val = float(old_cp_str) if old_cp_str else None
                except ValueError:
                    old_cp_val = None

                # Update if this is a new value or different from the old value.
                # Tolerate small epsilon to avoid rewriting CSV for micro-rounding diffs.
                if old_cp_val is None or abs(new_cp_val - old_cp_val) > 1e-4:
                    orig_true_prob = float(row.get("true_prob", 0))

                    # CLV edge: (Closing Prob - Original True Prob)
                    # Positive CLV = market moved toward our pick = we beat the market.
                    clv_pct = new_cp_val - orig_true_prob

                    row["closing_prob"] = round(new_cp_val, 4)
                    row["clv_pct"] = round(clv_pct, 4)
                    updated_count += 1
                    changed = True

        if changed:
            self._write_csv(self._csv_path, rows, list(fieldnames))
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
        if not self._csv_path.exists():
            return 0

        rows, fieldnames = self._read_csv()
        if not rows or not fieldnames:
            return 0

        if "closing_prob" not in fieldnames or "clv_pct" not in fieldnames:
            return 0

        now_utc = datetime.now(timezone.utc)
        cutoff = timedelta(hours=MISSED_CUTOFF_HOURS)
        finalized = 0
        changed = False

        for row in rows:
            # Only target pending or already-resolved rows with empty closing_prob
            if row.get("closing_prob"):
                continue  # already has CLV data

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
            # have no closing_prob, the tracking window was missed entirely.
            if (now_utc - gs) > cutoff:
                orig_true_prob = float(row.get("true_prob", 0))
                # Fallback: closing_prob = true_prob, clv_pct = 0
                # This means "no line movement captured — CLV unknown"
                row["closing_prob"] = round(orig_true_prob, 4)
                row["clv_pct"] = 0.0
                finalized += 1
                changed = True

        if changed:
            self._write_csv(self._csv_path, rows, list(fieldnames))
            logger.info(
                "CLVTracker: finalized %d missed bets (closing_prob = true_prob)", finalized
            )

        return finalized

    # ── Helpers ──────────────────────────────────────────────────────────────

    def _build_current_probs(self, matches: list[Any]) -> dict[tuple[str, str, str], float]:
        """Build a lookup: (player_lower, prop_lower, side) -> consensus probability."""
        current_probs: dict[tuple[str, str, str], float] = {}
        for m in matches:
            if not getattr(m, "pp", None):
                continue

            player = m.pp.player_name.lower().strip()
            prop = m.pp.stat_type.lower().strip()
            sides = ["over", "under"] if getattr(m.pp, "side", "both") == "both" else [m.pp.side]

            match_books = books_from_match(m.fd, m.dk, m.pin)

            for side in sides:
                consensus_prob, worst_case_prob, meta = compute_true_probability(match_books, side)
                if consensus_prob is not None:
                    current_probs[(player, prop, side)] = consensus_prob

        return current_probs

    def _read_csv(self) -> tuple[list[dict], list[str] | None]:
        """Read the backtest CSV. Returns (rows, fieldnames) or ([], None) on error."""
        try:
            with open(self._csv_path, "r", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
        except Exception as exc:
            logger.error("CLVTracker: cannot read CSV: %s", exc)
            return [], None

        if not rows:
            return [], None

        return rows, list(rows[0].keys())

    def _write_csv(self, csv_path: pathlib.Path, rows: list[dict], fieldnames: list[str]) -> None:
        """Atomically rewrite the CSV (write to .tmp then rename)."""
        tmp = csv_path.with_suffix(".tmp")
        try:
            with open(tmp, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=fieldnames)
                w.writeheader()
                w.writerows(rows)
            tmp.replace(csv_path)
        except Exception as exc:
            logger.error("CLVTracker: CSV write failed: %s", exc)
            if tmp.exists():
                tmp.unlink()
