"""
Calibration metrics for evaluating probability estimation accuracy.

Implements:
- **Brier Score**: Mean squared error of predicted probabilities vs outcomes.
- **Log-Loss** (Cross-Entropy Loss): Penalises overconfident mispredictions
  asymmetrically — a 95% forecast that fails is punished far more than a 60%
  forecast that fails.

These metrics are computed against resolved backtest data to continuously
audit the predictive validity of the devigging and consensus algorithms.
"""
from __future__ import annotations

import csv
import math
import logging
import pathlib
from typing import Optional

logger = logging.getLogger(__name__)

DATA_DIR = pathlib.Path(__file__).parent.parent / "data"
CSV_PATH = DATA_DIR / "backtest.csv"

# Clamp probabilities away from 0 and 1 to avoid log(0) in log-loss
_EPS = 1e-7


def _load_resolved_rows(csv_path: pathlib.Path | None = None) -> list[dict]:
    """
    Read resolved rows from the backtest CSV.

    Returns a list of dicts, each with:
      - true_prob: float (the model's predicted probability)
      - result: 1 (hit / won) or 0 (miss / lost)
    """
    path = csv_path or CSV_PATH
    if not path.exists():
        return []

    rows = []
    try:
        with open(path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                result_raw = row.get("result", "").strip().lower()
                if result_raw not in ("won", "lost", "win", "loss", "hit", "miss", "1", "0"):
                    continue  # skip pending / unresolved

                outcome = 1 if result_raw in ("won", "win", "hit", "1") else 0
                try:
                    true_prob = float(row.get("true_prob", 0))
                except (ValueError, TypeError):
                    continue

                if true_prob <= 0 or true_prob >= 1:
                    continue

                rows.append({
                    "true_prob": true_prob,
                    "outcome": outcome,
                    "player": row.get("player", ""),
                    "prop": row.get("prop", ""),
                    "side": row.get("side", ""),
                    "league": row.get("league", ""),
                    "slip_id": row.get("slip_id", ""),
                })
    except Exception as exc:
        logger.warning("Calibration: failed to read CSV: %s", exc)

    return rows

# Filter historical CLV stats to start from the first verified CLV-tracked slip.
# Older slips have missing or partial closing line data.
START_SLIP_ID = "5D3D2A96"


def _load_clv_rows(csv_path: pathlib.Path | None = None) -> list[dict]:
    """Read pending/resolved rows that have a closing_prob/clv_pct tracked."""
    path = csv_path or CSV_PATH
    if not path.exists():
        return []

    rows = []
    found_start = False
    try:
        with open(path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Skip rows until we reach the starting point for CLV tracking
                if not found_start:
                    if row.get("slip_id") == START_SLIP_ID:
                        found_start = True
                    else:
                        continue

                try:
                    cp_str = row.get("closing_prob", "")
                    clv_str = row.get("clv_pct", "")
                    if not cp_str or not clv_str:
                        continue

                    cp = float(cp_str)
                    clv = float(clv_str)
                    rows.append({"closing_prob": cp, "clv_pct": clv})
                except ValueError:
                    continue
    except Exception:
        pass
    return rows


def brier_score(rows: list[dict]) -> Optional[float]:
    """
    Brier Score = (1/N) × Σ(f_t - o_t)²

    Range: [0, 1].  Lower is better.
    - 0.0 = perfect calibration
    - 0.25 = random coin-flip baseline
    """
    if not rows:
        return None
    n = len(rows)
    total = sum((r["true_prob"] - r["outcome"]) ** 2 for r in rows)
    return total / n


def log_loss(rows: list[dict]) -> Optional[float]:
    """
    Log-Loss = -(1/N) × Σ[o_t × ln(f_t) + (1-o_t) × ln(1-f_t)]

    Lower is better.  Aggressively penalises overconfident mispredictions.
    - A 95% forecast that fails gets a massive penalty.
    - A 55% forecast that fails gets a moderate penalty.
    """
    if not rows:
        return None
    n = len(rows)
    total = 0.0
    for r in rows:
        p = max(_EPS, min(1 - _EPS, r["true_prob"]))
        o = r["outcome"]
        total += o * math.log(p) + (1 - o) * math.log(1 - p)
    return -total / n


def evaluate_calibration(csv_path: pathlib.Path | None = None) -> dict:
    """
    Compute calibration metrics from resolved backtest data.

    Returns a dict with:
      - brier_score: float | None
      - log_loss: float | None
      - n_resolved: int
      - n_won: int
      - n_lost: int
      - hit_rate: float | None (raw accuracy)
      - avg_predicted_prob: float | None
      - calibration_buckets: list of {bucket, predicted_avg, actual_avg, count}
    """
    rows = _load_resolved_rows(csv_path)
    n = len(rows)

    if n == 0:
        return {
            "brier_score": None,
            "log_loss": None,
            "n_resolved": 0,
            "n_won": 0,
            "n_lost": 0,
            "hit_rate": None,
            "avg_predicted_prob": None,
            "calibration_buckets": [],
        }

    n_won = sum(1 for r in rows if r["outcome"] == 1)
    n_lost = n - n_won

    bs = brier_score(rows)
    ll = log_loss(rows)
    hit_rate = n_won / n if n > 0 else None
    avg_pred = sum(r["true_prob"] for r in rows) / n

    # Build calibration buckets (deciles: 0-10%, 10-20%, ..., 90-100%)
    buckets = []
    for bucket_start in range(0, 100, 10):
        lo = bucket_start / 100.0
        hi = (bucket_start + 10) / 100.0
        bucket_rows = [r for r in rows if lo <= r["true_prob"] < hi]
        if bucket_rows:
            pred_avg = sum(r["true_prob"] for r in bucket_rows) / len(bucket_rows)
            actual_avg = sum(r["outcome"] for r in bucket_rows) / len(bucket_rows)
            buckets.append({
                "bucket": f"{bucket_start}-{bucket_start+10}%",
                "predicted_avg": round(pred_avg, 4),
                "actual_avg": round(actual_avg, 4),
                "count": len(bucket_rows),
            })

    # CLV
    clv_rows = _load_clv_rows(csv_path)
    n_clv = len(clv_rows)
    clv_plus_rate = None
    avg_clv_pct = None
    if n_clv > 0:
        n_plus = sum(1 for r in clv_rows if r["clv_pct"] > 0)
        n_minus = sum(1 for r in clv_rows if r["clv_pct"] < 0)
        clv_den = n_plus + n_minus
        clv_plus_rate = n_plus / clv_den if clv_den > 0 else None
        avg_clv_pct = sum(r["clv_pct"] for r in clv_rows) / n_clv

    return {
        "brier_score": round(bs, 6) if bs is not None else None,
        "log_loss": round(ll, 6) if ll is not None else None,
        "n_resolved": n,
        "n_won": n_won,
        "n_lost": n_lost,
        "hit_rate": round(hit_rate, 4) if hit_rate is not None else None,
        "avg_predicted_prob": round(avg_pred, 4) if n > 0 else None,
        "calibration_buckets": buckets,
        "n_clv_tracked": n_clv,
        "clv_plus_rate": round(clv_plus_rate, 4) if clv_plus_rate is not None else None,
        "avg_clv_pct": round(avg_clv_pct, 4) if avg_clv_pct is not None else None,
    }
