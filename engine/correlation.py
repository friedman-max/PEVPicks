"""
Leg-to-leg correlation model for slip EV.

Independence is a poor assumption for Power/Flex slips. Two legs from the
same game (same player even more so) are positively correlated: a blowout,
high-pace game, or a hot shooting night lifts all overs on that side; a
low-scoring game sinks them together. Treating those as independent inflates
P(all hit) and overstates Power EV, sometimes by 20-30% on same-game stacks.

This module produces an n×n correlation matrix on the latent (Gaussian
copula) scale. The EV calculator consumes it via Monte Carlo simulation:
draw correlated standard normals, threshold each at Φ⁻¹(p_i), and count
hits — which preserves both marginal probabilities AND pairwise correlations.

Correlation heuristics
----------------------
- Different games:                   ρ = 0
- Same game, same player:            ρ = BASE_SAME_PLAYER × league_pace
- Same game, different players:      ρ = BASE_SAME_GAME   × league_pace

The constants are deliberately conservative. They can be recalibrated
empirically from `market_observatory` resolutions later (fit pairwise hit
correlations per league/prop-pair).
"""
from __future__ import annotations

from typing import Iterable

import numpy as np


# Latent-scale (Gaussian copula) correlation magnitudes. These are NOT
# Pearson correlations of {0,1} hit outcomes — they're the correlations of
# the underlying standard-normal variates. A latent ρ of 0.25 produces a
# Bernoulli-scale correlation of roughly 0.15-0.20 for marginals near 0.5.
BASE_SAME_PLAYER: float = 0.30
BASE_SAME_GAME:   float = 0.12

# Per-league multiplier on the base correlations. Pace-driven sports (all
# basketball variants) have stronger same-game performance correlation than
# discrete plate-appearance sports (MLB). Unknown leagues default to 1.0.
LEAGUE_PACE_MULTIPLIER: dict[str, float] = {
    "NBA":        1.20,
    "WNBA":       1.20,
    "NCAAB":      1.15,
    "NBL":        1.15,
    "EUROLEAGUE": 1.15,
    "NFL":        1.00,
    "NCAAF":      1.00,
    "NHL":        0.95,
    "MLB":        0.60,
    "SOCCER":     0.90,
}

# Absolute ceiling on any single ρ entry. Latent ρ close to 1 makes the
# Cholesky factorisation near-singular and makes the slip behaviour
# degenerate (all legs move together). 0.5 is conservative.
MAX_RHO: float = 0.50


def _league_multiplier(league: str) -> float:
    if not league:
        return 1.0
    return LEAGUE_PACE_MULTIPLIER.get(league.upper(), 1.0)


def _pair_correlation(a: dict, b: dict) -> float:
    """Latent-scale correlation for a single leg pair."""
    game_a = a.get("game_key") or ""
    game_b = b.get("game_key") or ""
    if not game_a or game_a != game_b:
        return 0.0

    mult = _league_multiplier(a.get("league", ""))

    pid_a = a.get("player_id") or ""
    pid_b = b.get("player_id") or ""
    if pid_a and pid_a == pid_b:
        rho = BASE_SAME_PLAYER * mult
    else:
        rho = BASE_SAME_GAME * mult

    return float(np.clip(rho, -MAX_RHO, MAX_RHO))


def build_correlation_matrix(legs: list[dict]) -> np.ndarray:
    """Return an n×n latent-scale correlation matrix for the given legs.

    Each leg is a dict with (all optional but recommended):
      - league:     str, upper-case preferred — drives the pace multiplier.
      - game_key:   str, identifies the game (e.g. f"{league}|{start_time}").
      - player_id:  str, identifies the player (same-player legs).

    Any leg missing a `game_key` is treated as an independent contract
    (all off-diagonal entries involving it are zero), which is the safe
    default when upstream metadata is incomplete.

    The returned matrix is symmetric with ones on the diagonal, ρ ∈ [-0.5, 0.5]
    off-diagonal, and is projected to the nearest PSD matrix in the (rare)
    case the heuristic produces one that isn't positive semi-definite.
    """
    n = len(legs)
    if n == 0:
        return np.zeros((0, 0), dtype=float)

    R = np.eye(n, dtype=float)
    for i in range(n):
        for j in range(i + 1, n):
            rho = _pair_correlation(legs[i], legs[j])
            R[i, j] = rho
            R[j, i] = rho

    return _project_to_psd(R)


def _project_to_psd(R: np.ndarray) -> np.ndarray:
    """Project a symmetric matrix onto the PSD cone by zeroing negative
    eigenvalues, then renormalising the diagonal to 1. Cheap for n ≤ 6."""
    try:
        # Quick path: Cholesky succeeds → already PSD.
        np.linalg.cholesky(R + 1e-10 * np.eye(R.shape[0]))
        return R
    except np.linalg.LinAlgError:
        pass

    # Eigendecomposition fallback.
    w, V = np.linalg.eigh(R)
    w_clipped = np.clip(w, 1e-8, None)
    R_psd = (V * w_clipped) @ V.T
    # Renormalise so the diagonal stays 1 (unit variances on the latent scale).
    d = np.sqrt(np.diag(R_psd))
    d_outer = np.outer(d, d)
    d_outer[d_outer == 0] = 1.0
    R_norm = R_psd / d_outer
    np.fill_diagonal(R_norm, 1.0)
    return R_norm


def _field(b, *names, default: str = "") -> str:
    """Look up the first non-empty attribute or dict key from `names`."""
    for n in names:
        if hasattr(b, n):
            val = getattr(b, n)
            if val:
                return str(val)
        elif isinstance(b, dict) and n in b:
            val = b[n]
            if val:
                return str(val)
    return default


def legs_metadata_from_bets(bets: Iterable) -> list[dict]:
    """Extract the minimal leg metadata the correlation model needs.

    Accepts either a list of BetResult-like objects (attributes: `league`,
    `start_time`, `pp_player_id`) OR a list of dicts coming from the
    frontend (keys: `league`, `start_time`, `pp_player_id` or `player_id`).
    """
    out: list[dict] = []
    for b in bets:
        league = _field(b, "league").strip()
        start_time = _field(b, "start_time").strip()
        player_id = _field(b, "pp_player_id", "player_id").strip()
        game_key = f"{league.upper()}|{start_time}" if (league and start_time) else ""
        out.append({
            "league":    league,
            "game_key":  game_key,
            "player_id": player_id,
        })
    return out
