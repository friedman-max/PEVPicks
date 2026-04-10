"""
Backtest and slip logger for CoreProp.

Automatically documents the best +EV slip combinations as they appear
throughout the day. Logs to data/backtest.csv with one row per leg.
Tracks which players have been used to avoid repeats in the same betting day.
Resets daily at midnight.
"""
import csv
import logging
import pathlib
import uuid
from datetime import date, datetime, timezone, timedelta
from typing import Optional
import unidecode

from engine.constants import BREAK_EVEN
from engine.ev_calculator import power_slip_ev, flex_slip_ev

logger = logging.getLogger(__name__)

DATA_DIR      = pathlib.Path(__file__).parent.parent / "data"
CSV_PATH      = DATA_DIR / "backtest.csv"

CSV_COLUMNS = [
    "slip_id", "timestamp", "slip_type", "n_legs", "proj_slip_ev_pct",
    "leg_num", "player", "league", "prop", "line", "side",
    "true_prob", "ind_ev_pct", "urgency", "game_start",
    "closing_prob", "clv_pct", "result", "stat_actual",
]

# Hard floor: legs with individual EV below this are never included
MIN_LEG_EV_PCT = -0.01   # -1%

def _normalize(s: str) -> str:
    """Standardize strings: unidecode, lowercase, and strip whitespace."""
    if not s:
        return ""
    s = unidecode.unidecode(s).lower().strip()
    return s

def _make_key(player: str, start_time: str) -> tuple[str, str]:
    """Build a unique signature for a player in a specific game."""
    # Normalize start_time to YYYY-MM-DDTHH:MM to handle tiny variations in ISO format
    time_key = "no_time"
    if start_time:
        # Take first 16 chars: "2023-10-27T19:45"
        time_key = start_time[:16]
    return (_normalize(player), time_key)


class BacktestLogger:
    """
    Builds and logs the best available +EV slips to a CSV file.

    Selection logic:
      - Filter out already-used PLAYERS (Hard Deduplication)
      - Hard floor: individual_ev_pct >= -1%
      - Sort greedily by individual EV descending
      - Pick top 6 legs (must be 6 unique players)
      - Log to CSV; lock players for the day
    """

    def __init__(self, csv_path: Optional[pathlib.Path] = None):
        self.used_bets: set[tuple[str, str]] = set()
        self.last_reset_date: date = date.today()
        self._csv_path = csv_path or CSV_PATH
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self._init_csv()
        self._rebuild_used_bets()

    def _init_csv(self) -> None:
        if not self._csv_path.exists():
            with open(self._csv_path, "w", newline="", encoding="utf-8") as f:
                csv.DictWriter(f, fieldnames=CSV_COLUMNS).writeheader()
            logger.info("Created backtest CSV at %s", self._csv_path)

    def _rebuild_used_bets(self) -> None:
        """Read recent rows (approx last 24h) and repopulate used_players."""
        today = date.today()
        yesterday = today - timedelta(days=1)
        target_dates = [today.isoformat(), yesterday.isoformat()]
        
        if not self._csv_path.exists():
            return
        try:
            with open(self._csv_path, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    ts = row.get("timestamp", "")
                    is_recent = False
                    for d in target_dates:
                        if ts.startswith(d):
                            is_recent = True
                            break
                    if is_recent:
                        self.used_bets.add(_make_key(
                            row.get("player", ""),
                            row.get("game_start", "")
                        ))
            if self.used_bets:
                logger.debug("Backtest: rebuilt %d used-player-game keys", len(self.used_bets))
        except Exception as exc:
            logger.warning("Backtest: could not rebuild used_players from CSV: %s", exc)

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
            p_key = _make_key(
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
            p_key = _make_key(
                bet.get("player_name", ""),
                bet.get("start_time", "")
            )
            self.used_bets.add(p_key)
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
                "true_prob":        round(float(bet.get("true_prob") or 0), 4),
                "ind_ev_pct":       round(_ev(bet), 4),
                "urgency":          "NORMAL",
                "game_start":       bet.get("start_time", ""),
                "closing_prob":     "",
                "clv_pct":          "",
                "result":           "pending",
                "stat_actual":      "",
            })

        try:
            with open(self._csv_path, "a", newline="", encoding="utf-8") as f:
                csv.DictWriter(f, fieldnames=CSV_COLUMNS).writerows(rows)
            logger.info("Backtest: logged slip %s (6-leg Power EV=%.2f%%)", slip_id, best_ev * 100)
        except Exception as exc:
            logger.error("Backtest: CSV write failed: %s", exc)
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
                    "urgency":    r["urgency"],
                    "game_start": r["game_start"],
                }
                for r in rows
            ],
        }
