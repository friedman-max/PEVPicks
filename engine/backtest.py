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
    "result", "stat_actual",
]

# Minutes before game start to treat as "HIGH" urgency
URGENCY_MINUTES = 60

# Extra score added when urgency is HIGH (same units as ind_ev_pct)
URGENCY_BONUS = 0.02

# Hard floor: legs with individual EV below this are never included
MIN_LEG_EV_PCT = -0.01   # -1%

# Maximum number of slightly-negative legs allowed in any one slip
MAX_NEGATIVE_LEGS = 2

# Minimum number of positive-EV legs required in every slip
MIN_POSITIVE_LEGS = 3


class BacktestLogger:
    """
    Builds and logs the best available +EV slips to a CSV file.

    Selection logic:
      - Filter out already-used (player, prop, side) combos
      - Score each bet: score = ind_ev_pct + URGENCY_BONUS if game within 60 min
      - Try slip sizes 6 → 5 → 4 → 3, pick first size where best_ev > 0
        and average true_prob meets the break-even threshold
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

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def _init_csv(self) -> None:
        """Create CSV with headers if it doesn't already exist."""
        if not self._csv_path.exists():
            with open(self._csv_path, "w", newline="", encoding="utf-8") as f:
                csv.DictWriter(f, fieldnames=CSV_COLUMNS).writeheader()
            logger.info("Created backtest CSV at %s", self._csv_path)

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

    # ------------------------------------------------------------------
    # Scoring helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _is_urgent(game_start: Optional[str]) -> bool:
        """Return True if the game starts within URGENCY_MINUTES from now."""
        if not game_start:
            return False
        try:
            gs = datetime.fromisoformat(game_start.replace("Z", "+00:00"))
            if gs.tzinfo is None:
                now = datetime.utcnow()
                gs = gs.replace(tzinfo=None)
            else:
                now = datetime.now(tz=timezone.utc)
            minutes_to_start = (gs - now).total_seconds() / 60
            return 0 < minutes_to_start <= URGENCY_MINUTES
        except Exception:
            return False

    @classmethod
    def _score(cls, bet: dict) -> float:
        ev = float(bet.get("individual_ev_pct") or 0.0)
        bonus = URGENCY_BONUS if cls._is_urgent(bet.get("start_time")) else 0.0
        return ev + bonus

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def try_log_slip(self, bets: list[dict]) -> Optional[dict]:
        """
        Given the current list of bet dicts, find and log the slip combination
        that maximises total slip EV.

        Selection rules:
          - Hard floor: legs with individual_ev_pct < MIN_LEG_EV_PCT (-1%) are ignored
          - Always require at least MIN_POSITIVE_LEGS (3) positive-EV legs
          - Allow up to MAX_NEGATIVE_LEGS (2) slightly-negative legs ONLY when
            adding them produces a net higher slip EV than the all-positive baseline
          - Evaluates every valid combination; picks the global maximum, not a greedy
            first-positive result
          - Returns None (skips silently) if no combination yields slip EV > 0

        Each bet dict must include: player_name, prop_type, side, true_prob,
        individual_ev_pct, pp_line, league, start_time.
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

        # ── 2. Apply hard floor and split into positive / negative pools ─────
        def _ev(b: dict) -> float:
            return float(b.get("individual_ev_pct") or 0.0)

        valid = [b for b in available if _ev(b) >= MIN_LEG_EV_PCT]

        positive_pool = sorted(
            [b for b in valid if _ev(b) >= 0.0],
            key=self._score, reverse=True,
        )
        # Sorted desc by score so index 0 is the "least negative" (best negative)
        negative_pool = sorted(
            [b for b in valid if _ev(b) < 0.0],
            key=self._score, reverse=True,
        )

        if len(positive_pool) < MIN_POSITIVE_LEGS:
            logger.debug(
                "Backtest: only %d positive-EV bets available (need %d) — skipping",
                len(positive_pool), MIN_POSITIVE_LEGS,
            )
            return None

        # ── 3. Search all valid (n_pos, n_neg) combinations ─────────────────
        #  Rules:
        #    • n_pos  ∈ [MIN_POSITIVE_LEGS, 6]
        #    • n_neg  ∈ [0, MAX_NEGATIVE_LEGS]
        #    • total  k = n_pos + n_neg  ∈ [3, 6]
        #    • n_neg ≤ len(negative_pool)
        #
        #  For each combination we evaluate the best of Power / Flex EV and
        #  track the global maximum across all combinations.

        best_ev:        Optional[float] = None
        best_type:      Optional[str]   = None
        best_legs:      Optional[list]  = None

        max_n_neg = min(MAX_NEGATIVE_LEGS, len(negative_pool))

        for n_neg in range(0, max_n_neg + 1):
            neg_legs  = negative_pool[:n_neg]
            max_n_pos = min(6 - n_neg, len(positive_pool))

            for n_pos in range(MIN_POSITIVE_LEGS, max_n_pos + 1):
                k = n_pos + n_neg
                if k < 3 or k > 6:
                    continue

                legs       = positive_pool[:n_pos] + neg_legs
                true_probs = [float(b.get("true_prob") or 0.0) for b in legs]
                avg_prob   = sum(true_probs) / k

                power_be = BREAK_EVEN.get((str(k), "power"))
                flex_be  = BREAK_EVEN.get((str(k), "flex"))

                candidate_ev:   Optional[float] = None
                candidate_type: Optional[str]   = None

                if power_be is not None and avg_prob >= power_be:
                    pev = power_slip_ev(true_probs)
                    if pev is not None and pev > 0:
                        if candidate_ev is None or pev > candidate_ev:
                            candidate_ev, candidate_type = pev, "Power"

                if flex_be is not None and avg_prob >= flex_be:
                    fev = flex_slip_ev(true_probs)
                    if fev is not None and fev > 0:
                        if candidate_ev is None or fev > candidate_ev:
                            candidate_ev, candidate_type = fev, "Flex"

                if candidate_ev is not None and (best_ev is None or candidate_ev > best_ev):
                    best_ev    = candidate_ev
                    best_type  = candidate_type
                    best_legs  = legs

        # ── 4. Nothing viable found — skip silently ──────────────────────────
        if best_ev is None or best_legs is None:
            logger.debug(
                "Backtest: no positive-EV slip found from %d available bets — skipping",
                len(valid),
            )
            return None

        # ── 5. Log the winning slip to CSV ───────────────────────────────────
        k         = len(best_legs)
        slip_id   = str(uuid.uuid4())[:8].upper()
        timestamp = datetime.now().isoformat(timespec="seconds")
        proj_ev   = round(best_ev, 4)

        n_neg_used = sum(1 for b in best_legs if _ev(b) < 0)
        if n_neg_used:
            logger.debug(
                "Backtest: slip %s uses %d negative-EV leg(s) — net gain over "
                "all-positive baseline justified",
                slip_id, n_neg_used,
            )

        rows = []
        for i, bet in enumerate(best_legs, start=1):
            urgency = "HIGH" if self._is_urgent(bet.get("start_time")) else "NORMAL"
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
                "urgency":          urgency,
                "game_start":       bet.get("start_time", ""),
                "result":           "pending",
                "stat_actual":      "",
            })

        try:
            with open(self._csv_path, "a", newline="", encoding="utf-8") as f:
                csv.DictWriter(f, fieldnames=CSV_COLUMNS).writerows(rows)
            logger.info(
                "Backtest: logged slip %s  (%d-leg %s  EV=%.2f%%  neg_legs=%d)",
                slip_id, k, best_type, best_ev * 100, n_neg_used,
            )
        except Exception as exc:
            logger.error("Backtest: CSV write failed: %s", exc)
            return None

        # ── 6. Mark legs as used ─────────────────────────────────────────────
        for bet in best_legs:
            self.used_bets.add((
                bet.get("player_name", "").lower(),
                bet.get("prop_type", "").lower(),
                bet.get("side", ""),
            ))

        # ── 7. Return slip summary for the frontend notification ─────────────
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
