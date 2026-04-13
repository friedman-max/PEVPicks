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

import math
import logging
from typing import Optional
from engine.database import get_db

logger = logging.getLogger(__name__)

# Clamp probabilities away from 0 and 1 to avoid log(0) in log-loss
_EPS = 1e-7

# Only include data starting from this slip ID for CLV tracking
START_SLIP_ID = "5D3D2A96"


def _load_resolved_rows() -> list[dict]:
    """
    Read resolved rows from the Supabase database.
    """
    db = get_db()
    if not db:
        return []

    try:
        res = db.table("legs").select("*").in_("result", ["won", "win", "hit", "1", "lost", "loss", "miss", "0"]).execute()
        rows = []
        for r in res.data:
            outcome = 1 if str(r.get("result")).lower() in ("won", "win", "hit", "1") else 0
            try:
                true_prob = float(r.get("true_prob", 0))
            except (ValueError, TypeError):
                continue
            if true_prob <= 0 or true_prob >= 1:
                continue
            rows.append({
                "true_prob": true_prob,
                "outcome":   outcome,
                "player":    r.get("player", ""),
                "prop":      r.get("prop", ""),
                "side":      r.get("side", ""),
                "league":    r.get("league", ""),
                "slip_id":   r.get("slip_id", ""),
            })
        return rows
    except Exception as e:
        logger.warning("Calibration: Supabase load failed: %s", e)
        return []


def _load_clv_rows() -> list[dict]:
    """Read rows that have a closing_prob/clv_pct tracked, starting from START_SLIP_ID."""
    db = get_db()
    if not db:
        return []

    try:
        res = db.table("legs").select("closing_prob, clv_pct, slip_id").execute()
        rows = []
        found_start = False
        # Sort data by slip_id to replicate the original ordering
        sorted_data = sorted(res.data, key=lambda x: x.get("slip_id", ""))
        for r in sorted_data:
            if not found_start:
                if r.get("slip_id") == START_SLIP_ID:
                    found_start = True
                else:
                    continue
            if r.get("closing_prob") is not None and r.get("clv_pct") is not None:
                rows.append({"closing_prob": r["closing_prob"], "clv_pct": r["clv_pct"]})
        return rows
    except Exception as e:
        logger.warning("Calibration: Supabase CLV load failed: %s", e)
        return []


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


def evaluate_calibration() -> dict:
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
    rows = _load_resolved_rows()
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

    # Build calibration buckets (5% wide ranges: 50-54, 55-59, ..., 75-79)
    buckets = []
    for bucket_start in range(50, 80, 5):
        lo = bucket_start / 100.0
        hi = (bucket_start + 5) / 100.0
        bucket_rows = [r for r in rows if lo <= r["true_prob"] < hi]

        count = len(bucket_rows)
        if count > 0:
            pred_avg = sum(r["true_prob"] for r in bucket_rows) / count
            actual_avg = sum(r["outcome"] for r in bucket_rows) / count
        else:
            pred_avg = None
            actual_avg = None

        buckets.append({
            "bucket": f"{bucket_start}-{bucket_start+4}%",
            "predicted_avg": round(pred_avg, 4) if pred_avg is not None else None,
            "actual_avg": round(actual_avg, 4) if actual_avg is not None else None,
            "count": count,
        })

    # CLV
    clv_rows = _load_clv_rows()
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
