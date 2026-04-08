"""
Backtest and slip logger for CoreProp.

Automatically documents the best +EV slip combinations as they appear
throughout the day. Logs to data/backtest.csv with one row per leg.
Tracks which (player, prop, side) combos have been used to avoid repeats.
Resets daily at midnight.
"""
import csv
import logging
import pathlib
import uuid
from datetime import date, datetime, timezone
from typing import Optional

from engine.constants import BREAK_EVEN
from engine.ev_calculator import power_slip_ev, flex_slip_ev

logger = logging.getLogger(__name__)

DATA_DIR      = pathlib.Path(__file__).parent.parent / "data"
CSV_PATH      = DATA_DIR / "backtest.csv"
_TEST_CSV_PATH = DATA_DIR / ".backtest_test.csv"  # scratch path used only by unit tests

CSV_COLUMNS = [
    "slip_id", "timestamp", "slip_type", "n_legs", "proj_slip_ev_pct",
    "leg_num", "player", "league", "prop", "line", "side",
    "true_prob", "ind_ev_pct", "urgency", "game_start",
    "closing_prob", "clv_pct", "result", "stat_actual",
]

# Hard floor: legs with individual EV below this are never included
MIN_LEG_EV_PCT = -0.01   # -1%


class BacktestLogger:
    """
    Builds and logs the best available +EV slips to a CSV file.

    Selection logic:
      - Filter out already-used (player, prop, side) combos
      - Hard floor: individual_ev_pct >= -1% (-0.01)
      - Sort greedily by individual EV descending
      - Pick top 6 legs to form a 6-Leg Power Slip
      - Log to CSV; mark used bets so they won't appear in future slips today
    """

    def __init__(self, csv_path: Optional[pathlib.Path] = None):
        """
        Args:
            csv_path: Override the CSV output path. Leave as None (default) for
                      production use. Pass a temp path in tests to avoid polluting
                      the real data/backtest.csv.
        """
        self.used_bets: set[tuple] = set()  # (player_name_lower, prop_type_lower, side)
        self.last_reset_date: Optional[date] = None
        self._csv_path = csv_path or CSV_PATH
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self._init_csv()
        # Rebuild used_bets from today's CSV rows so server restarts don't
        # lose dedup memory and re-log the same legs in a new slip.
        self._rebuild_used_bets()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def _init_csv(self) -> None:
        """Create CSV with headers if it doesn't already exist."""
        if not self._csv_path.exists():
            with open(self._csv_path, "w", newline="", encoding="utf-8") as f:
                csv.DictWriter(f, fieldnames=CSV_COLUMNS).writeheader()
            logger.info("Created backtest CSV at %s", self._csv_path)

    def _rebuild_used_bets(self) -> None:
        """
        Read today's rows from the CSV and repopulate used_bets.
        Called on startup so server restarts don't lose dedup memory.
        """
        today_str = date.today().isoformat()  # "YYYY-MM-DD"
        if not self._csv_path.exists():
            return
        try:
            with open(self._csv_path, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                count = 0
                for row in reader:
                    # timestamp column is like "2026-04-06T10:27:02"
                    if row.get("timestamp", "").startswith(today_str):
                        key = (
                            row.get("player", "").lower(),
                            row.get("prop", "").lower(),
                            row.get("side", ""),
                        )
                        self.used_bets.add(key)
                        count += 1
            if count:
                logger.info(
                    "Backtest: rebuilt %d used-bet keys from today's CSV on startup",
                    count,
                )
        except Exception as exc:
            logger.warning("Backtest: could not rebuild used_bets from CSV: %s", exc)

    def _midnight_reset(self) -> None:
        """Automatically reset the used-bets pool when the calendar date changes."""
        today = date.today()
        if self.last_reset_date != today:
            if self.last_reset_date is not None:
                logger.info(
                    "Midnight reset: clearing %d used-bet keys", len(self.used_bets)
                )
            self.used_bets = set()
            self.last_reset_date = today

    def reset_daily(self) -> None:
        """Explicit daily reset — called by the APScheduler midnight job."""
        logger.info("Daily reset: clearing %d used-bet keys", len(self.used_bets))
        self.used_bets = set()
        self.last_reset_date = date.today()

    def used_bet_keys(self) -> set[tuple]:
        """Return the current set of (player_lower, prop_lower, side) tuples used today."""
        return set(self.used_bets)

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def try_log_slip(self, bets: list[dict]) -> Optional[dict]:
        """
        Given the current list of bet dicts, find and log the slip combination
        that maximises total slip EV using a greedy approach for 6-leg power slips.

        Selection rules:
          - Filter out already-used (player, prop, side) combos
          - Filter out legs with individual_ev_pct < MIN_LEG_EV_PCT (-1%)
          - Sort descending strictly by EV (ignoring urgency)
          - Pick the top 6 legs
          - Return if EV <= 0
        """
        self._midnight_reset()

        # ── 1. Remove already-used (player, prop, side) combos ──────────────
        available = [
            b for b in bets
            if (
                b.get("player_name", "").lower(),
                b.get("prop_type", "").lower(),
                b.get("side", ""),
            ) not in self.used_bets
        ]

        # ── 2. Apply hard floor and sort ─────────────────────────────────────
        def _ev(b: dict) -> float:
            return float(b.get("individual_ev_pct") or 0.0)

        valid = [b for b in available if _ev(b) >= MIN_LEG_EV_PCT]
        pool = sorted(valid, key=_ev, reverse=True)

        # We strictly want 6-leg power slips
        if len(pool) < 6:
            logger.debug(
                "Backtest: only %d valid bets available (need 6) — skipping", len(pool)
            )
            return None

        # ── 3. Evaluate the 6 best legs ──────────────────────────────────────
        best_legs = pool[:6]
        true_probs = [float(b.get("true_prob") or 0.0) for b in best_legs]
        k = 6
        avg_prob = sum(true_probs) / k

        power_be = BREAK_EVEN.get((str(k), "power"))
        if power_be is None or avg_prob < power_be:
            logger.debug("Backtest: Top 6 legs do not meet break-even threshold.")
            return None

        best_ev = power_slip_ev(true_probs)
        if best_ev is None or best_ev <= 0:
            logger.debug(
                "Backtest: Top 6 legs do not form a +EV power slip. Projected EV: %s", best_ev
            )
            return None

        best_type = "Power"
        slip_id = str(uuid.uuid4())[:8].upper()
        timestamp = datetime.now().isoformat(timespec="seconds")
        proj_ev = round(best_ev, 4)

        # ── 4. Log the slip to CSV ───────────────────────────────────────────
        rows = []
        for i, bet in enumerate(best_legs, start=1):
            rows.append({
                "slip_id":          slip_id,
                "timestamp":        timestamp,
                "slip_type":        best_type,
                "n_legs":           k,
                "proj_slip_ev_pct": proj_ev,
                "leg_num":          i,
                "player":           bet.get("player_name", ""),
                "league":           bet.get("league", ""),
                "prop":             bet.get("prop_type", ""),
                "line":             bet.get("pp_line", ""),
                "side":             bet.get("side", ""),
                "true_prob":        round(float(bet.get("true_prob") or 0), 4),
                "ind_ev_pct":       round(_ev(bet), 4),
                "urgency":          "NORMAL",  # urgency is now ignored entirely
                "game_start":       bet.get("start_time", ""),
                "closing_prob":     "",
                "clv_pct":          "",
                "result":           "pending",
                "stat_actual":      "",
            })

        try:
            with open(self._csv_path, "a", newline="", encoding="utf-8") as f:
                csv.DictWriter(f, fieldnames=CSV_COLUMNS).writerows(rows)
            logger.info(
                "Backtest: logged slip %s  (6-leg Power EV=%.2f%%)",
                slip_id, best_ev * 100
            )
        except Exception as exc:
            logger.error("Backtest: CSV write failed: %s", exc)
            return None

        # ── 5. Mark legs as used ─────────────────────────────────────────────
        for bet in best_legs:
            self.used_bets.add((
                bet.get("player_name", "").lower(),
                bet.get("prop_type", "").lower(),
                bet.get("side", ""),
            ))

        # ── 6. Return slip summary for the frontend notification ─────────────
        return {
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

