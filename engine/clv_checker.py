"""
CLV (Closing Line Value) Tracker.

Periodically called to update the `closing_prob` in backtest.csv for pending bets.
As lines move, this records the last seen VWAP consensus probability before the
game starts and the line is pulled from the board.
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

# How close to game_start (in minutes) a bet must be to start locking in CLV updates.
# If the game starts in 2 hours or less, we continuously update the closing_prob with the latest odds.
# When the game starts (or the line disappears right before), the last updated value becomes the final CLV.
CLV_TRACKING_WINDOW_MINUTES = 120


class CLVTracker:
    def __init__(self, csv_path: pathlib.Path = CSV_PATH):
        self._csv_path = csv_path

    def update_closing_lines(self, matches: list[Any]) -> int:
        """
        Updates pending backtest rows that are starting soon with the latest true probability.
        `matches` is the list of MatchResult objects from app.py.
        Returns the number of rows updated.
        """
        if not self._csv_path.exists():
            return 0

        # Build a lookup for current matches: (player_lower, prop_lower, side) -> true_prob
        current_probs = {}
        for m in matches:
            if not getattr(m, "pp", None):
                continue
            
            player = m.pp.player_name.lower().strip()
            prop = m.pp.stat_type.lower().strip()
            # If both sides are available, we must compute for each side
            sides = ["over", "under"] if getattr(m.pp, "side", "both") == "both" else [m.pp.side]
            
            match_books = books_from_match(m.fd, m.dk, m.pin)
            
            for side in sides:
                consensus_prob, worst_case_prob, meta = compute_true_probability(match_books, side)
                # Use consensus (VWAP) as the unbiased closing line
                if consensus_prob is not None:
                    current_probs[(player, prop, side)] = consensus_prob

        # Read CSV
        try:
            with open(self._csv_path, "r", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
        except Exception as exc:
            logger.error("CLVTracker: cannot read CSV: %s", exc)
            return 0

        if not rows:
            return 0

        fieldnames = rows[0].keys()
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
            
            # If game starts within our tracking window (e.g. 2 hours) OR it has started recently
            # (sometimes lines stay up slightly after start), keep updating.
            # Stop updating if it's more than 30 mins past game start.
            if -30 <= mins_to_start <= CLV_TRACKING_WINDOW_MINUTES:
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
                        
                    # If this is a new value, or it's different from the old value, update it.
                    # We tolerate a small epsilon to avoid rewriting CSV for micro-rounding diffs.
                    if old_cp_val is None or abs(new_cp_val - old_cp_val) > 1e-4:
                        orig_true_prob = float(row.get("true_prob", 0))
                        
                        # CLV edge: (Model's original true_prob - closing_prob)
                        # Actually true CLV formula compares odds, but probabilistically:
                        # CLV is just the difference in implied probability, or ratio.
                        # For our tracking, clv_pct = original_prob - closing_prob 
                        # Wait, if our model thought it was 55%, and it closes at 50%, we have a +5% edge!
                        # So original_prob - closing_prob is correct.
                        clv_pct = orig_true_prob - new_cp_val
                        
                        row["closing_prob"] = round(new_cp_val, 4)
                        row["clv_pct"] = round(clv_pct, 4)
                        updated_count += 1
                        changed = True

        if changed:
            self._write_csv(self._csv_path, rows, list(fieldnames))
            logger.info("CLVTracker: updated closing lines for %d pending bets", updated_count)

        return updated_count

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
