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
from engine.database import get_user_db

logger = logging.getLogger(__name__)

# Clamp probabilities away from 0 and 1 to avoid log(0) in log-loss
_EPS = 1e-7

# Only include data starting from this slip ID for CLV tracking
START_SLIP_ID = "5D3D2A96"


def _load_resolved_rows(user_jwt: str) -> list[dict]:
    """
    Read resolved rows from the Supabase database.
    """
    db = get_user_db(user_jwt)
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


def _load_clv_rows(user_jwt: str) -> list[dict]:
    """Read rows that have a closing_prob/clv_pct tracked, starting from START_SLIP_ID."""
    db = get_user_db(user_jwt)
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


def evaluate_calibration(user_jwt: str) -> dict:
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
    rows = _load_resolved_rows(user_jwt)
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
    clv_rows = _load_clv_rows(user_jwt)
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


def evaluate_analytics(user_jwt: str) -> dict:
    """
    Richer analytics payload for the Analytics tab.

    Computes:
      - All of evaluate_calibration()
      - Per-league performance (legs, hits, actual vs expected rate)
      - Per-prop-type performance (top 10 by volume)
      - Slip outcome mix (hit / miss / pending counts)
      - Cumulative P&L timeline (one point per resolved slip, using PrizePicks
        payout tables and a 1-unit stake per slip — positive values = profit).
    """
    from engine.constants import POWER_PAYOUTS, FLEX_PAYOUTS

    base = evaluate_calibration(user_jwt)
    rows = _load_resolved_rows(user_jwt)

    # --- Per-league ----------------------------------------------------------
    def _group(rows, key):
        agg: dict[str, dict] = {}
        for r in rows:
            k = (r.get(key) or "").strip() or "—"
            g = agg.setdefault(k, {"key": k, "legs": 0, "hits": 0, "pred_sum": 0.0})
            g["legs"]     += 1
            g["hits"]     += r["outcome"]
            g["pred_sum"] += r["true_prob"]
        out = []
        for g in agg.values():
            legs = g["legs"]
            out.append({
                "key":      g["key"],
                "legs":     legs,
                "hits":     g["hits"],
                "actual":   round(g["hits"] / legs, 4) if legs else None,
                "expected": round(g["pred_sum"] / legs, 4) if legs else None,
                "delta":    round(g["hits"] / legs - g["pred_sum"] / legs, 4) if legs else None,
            })
        out.sort(key=lambda x: x["legs"], reverse=True)
        return out

    by_league = _group(rows, "league")
    by_prop = _group(rows, "prop")[:10]

    # --- Slip mix + cumulative P&L (needs full slip payload) -----------------
    db = get_user_db(user_jwt)
    slip_mix = {"won": 0, "lost": 0, "pending": 0, "partial": 0}
    pnl_timeline: list[dict] = []
    resolved_slips = won_slips = 0

    if db:
        try:
            slips_res = db.table("slips").select("*").order("timestamp", desc=False).execute()
            legs_res  = db.table("legs").select("*").execute()
            legs_by_slip: dict[str, list] = {}
            for l in (legs_res.data or []):
                legs_by_slip.setdefault(l["slip_id"], []).append(l)

            cum_pnl = 0.0
            for s in (slips_res.data or []):
                sid = s["id"]
                legs = legs_by_slip.get(sid, [])
                results = [str(l.get("result", "pending")).lower() for l in legs]
                if not legs:
                    continue
                completed = all(r in ("hit", "miss", "push", "dnp", "won", "win", "lost", "loss") for r in results)
                if not completed:
                    slip_mix["pending"] += 1
                    continue

                effective = [r for r in results if r not in ("push", "dnp")]
                n_eff = len(effective)
                hits_eff = sum(1 for r in effective if r in ("hit", "won", "win"))

                slip_type = (s.get("slip_type") or "").lower()
                if n_eff < 2:
                    payout = 1.0 if (n_eff == 0 or (n_eff == 1 and hits_eff == 1)) else 0.0
                elif slip_type == "power":
                    payout = POWER_PAYOUTS.get(n_eff, 0) if hits_eff == n_eff else 0
                else:
                    if n_eff == 2:
                        payout = POWER_PAYOUTS.get(2, 0) if hits_eff == 2 else 0
                    else:
                        payout = FLEX_PAYOUTS.get(n_eff, {}).get(hits_eff, 0)

                pnl = float(payout) - 1.0  # 1-unit stake per slip
                cum_pnl += pnl
                resolved_slips += 1
                if payout > 1.0:
                    won_slips += 1
                    slip_mix["won"] += 1
                elif hits_eff == n_eff and n_eff > 0:
                    slip_mix["won"] += 1
                elif hits_eff == 0:
                    slip_mix["lost"] += 1
                else:
                    slip_mix["partial"] += 1

                pnl_timeline.append({
                    "slip_id":   sid,
                    "timestamp": s.get("timestamp"),
                    "pnl":       round(pnl, 4),
                    "cum_pnl":   round(cum_pnl, 4),
                })
        except Exception as exc:
            logger.warning("Analytics: slip aggregation failed: %s", exc)

    roi = None
    if resolved_slips > 0 and pnl_timeline:
        roi = round(pnl_timeline[-1]["cum_pnl"] / resolved_slips, 4)

    base.update({
        "by_league":        by_league,
        "by_prop":          by_prop,
        "slip_mix":         slip_mix,
        "pnl_timeline":     pnl_timeline,
        "resolved_slips":   resolved_slips,
        "won_slips":        won_slips,
        "roi_per_slip":     roi,
    })
    return base
