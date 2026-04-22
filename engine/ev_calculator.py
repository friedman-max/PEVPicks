"""
EV calculation engine.

- Per-leg individual EV% (theoretical, using optimal 5/6-Flex break-even)
- Slip EV% for Power (exact) and Flex (full enumeration over all leg combinations)
- Line discrepancy directional logic
"""
import statistics
from itertools import product as itertools_product
from typing import Optional

import numpy as np

from engine.constants import (
    OPTIMAL_BREAK_EVEN,
    OPTIMAL_IMPLIED_DECIMAL,
    POWER_PAYOUTS,
    FLEX_PAYOUTS,
)
from engine.devig import devig_power, devig_single_sided, prob_to_american
from engine.matcher import MatchedProp
from engine.dynamic_calibration import load_calibration_map
from engine.correlation import build_correlation_matrix, legs_metadata_from_bets

# Module-level calibration map (refreshed on import or by calling reload_calibration)
_calibration_map: dict = load_calibration_map()


# ---------------------------------------------------------------------------
# Individual bet result
# ---------------------------------------------------------------------------

def reload_calibration():
    """Reload the calibration map from disk (called after daily recalibration)."""
    global _calibration_map
    _calibration_map = load_calibration_map()


class BetResult:
    __slots__ = (
        "bet_id", "player_name", "league", "prop_type",
        "pp_line", "fd_line", "side",
        "raw_true_prob", "true_prob", "true_odds", "edge", "individual_ev_pct",
        "over_odds", "under_odds", "both_sided",
        "pp_player_id", "start_time",
    )

    def __init__(
        self,
        bet_id: str,
        player_name: str,
        league: str,
        prop_type: str,
        pp_line: float,
        fd_line: float,
        side: str,            # "over" or "under"
        true_prob: float,
        over_odds: Optional[int],
        under_odds: Optional[int],
        both_sided: bool,
        pp_player_id: str,
        start_time: str = "",
    ):
        self.bet_id = bet_id
        self.player_name = player_name
        self.league = league
        self.prop_type = prop_type
        self.pp_line = pp_line
        self.fd_line = fd_line
        self.side = side
        self.raw_true_prob = true_prob
        self.over_odds = over_odds
        self.under_odds = under_odds
        self.both_sided = both_sided
        self.pp_player_id = pp_player_id
        self.start_time = start_time

        # Apply dynamic calibration multiplier
        cal_key = f"{league}|{prop_type}"
        multiplier = _calibration_map.get(cal_key, 1.0)
        calibrated_prob = min(true_prob * multiplier, 0.999)
        self.true_prob = calibrated_prob
        self.true_odds = prob_to_american(calibrated_prob)

        self.edge = round(calibrated_prob - OPTIMAL_BREAK_EVEN, 6)
        self.individual_ev_pct = round((calibrated_prob * OPTIMAL_IMPLIED_DECIMAL) - 1.0, 6)

    def to_dict(self) -> dict:
        return {
            "bet_id":            self.bet_id,
            "player_name":       self.player_name,
            "league":            self.league,
            "prop_type":         self.prop_type,
            "pp_line":           self.pp_line,
            "fd_line":           self.fd_line,
            "side":              self.side,
            "true_prob":         round(self.true_prob, 4),
            "true_odds":         self.true_odds,
            "edge":              round(self.edge, 4),
            "individual_ev_pct": round(self.individual_ev_pct, 4),
            "over_odds":         self.over_odds,
            "under_odds":        self.under_odds,
            "both_sided":        self.both_sided,
            "start_time":        self.start_time,
        }


# ---------------------------------------------------------------------------
# Line discrepancy logic
# ---------------------------------------------------------------------------

def _evaluate_same_line(match: MatchedProp) -> list[BetResult]:
    """Both lines match. Evaluate OVER and UNDER independently."""
    fd = match.fd or match.dk
    pp = match.pp
    results = []

    if fd is None:
        return results

    if fd.both_sided and fd.over_odds is not None and fd.under_odds is not None:
        true_over, true_under = devig_power(fd.over_odds, fd.under_odds)
        sides = [("over", true_over), ("under", true_under)]
    elif fd.over_odds is not None:
        true_over = devig_single_sided(fd.over_odds)
        sides = [("over", true_over)]
    elif fd.under_odds is not None:
        true_under = devig_single_sided(fd.under_odds)
        sides = [("under", true_under)]
    else:
        return results

    for side, true_prob in sides:
        bet_id = f"{pp.player_id}_{pp.stat_type}_{side}"
        result = BetResult(
            bet_id=bet_id,
            player_name=pp.player_name,
            league=pp.league,
            prop_type=pp.stat_type,
            pp_line=pp.line_score,
            fd_line=fd.line,
            side=side,
            true_prob=true_prob,
            over_odds=fd.over_odds,
            under_odds=fd.under_odds,
            both_sided=fd.both_sided,
            pp_player_id=pp.player_id,
            start_time=getattr(pp, "start_time", ""),
        )
        results.append(result)

    return results


def _get_true_prob_for_side(match: MatchedProp, side: str) -> Optional[float]:
    """De-vig and return true probability for a specific side from the best available book."""
    fd = match.fd
    dk = match.dk

    probs = []
    for book in [fd, dk]:
        if book is None: continue
        if book.both_sided and book.over_odds is not None and book.under_odds is not None:
            t_o, t_u = devig_power(book.over_odds, book.under_odds)
            probs.append(t_o if side == "over" else t_u)
        elif side == "over" and book.over_odds is not None:
            probs.append(devig_single_sided(book.over_odds))
        elif side == "under" and book.under_odds is not None:
            probs.append(devig_single_sided(book.under_odds))

    return max(probs) if probs else None


def compute_bet_true_prob_raw(match: MatchedProp, side: str) -> Optional[float]:
    """
    Return the RAW (uncalibrated) true probability for a bet on (match, side),
    using the **same methodology as `evaluate_match`** so that CLV tracking and
    other downstream consumers produce probabilities directly comparable to the
    one recorded at bet-log time.

    - Same-line (pp.line == book.line): devig directly from the best available
      book (FD, else DK).
    - Diff-line: max of devigged probs across FD and DK.

    Callers apply the calibration multiplier themselves, so this function
    returns the pre-calibration value.
    """
    pp = match.pp
    best_book = match.fd or match.dk
    if pp is None or best_book is None:
        return None

    if pp.line_score == best_book.line:
        # Same-line: devig directly from the matched book (mirrors
        # _evaluate_same_line's selection logic).
        if best_book.both_sided and best_book.over_odds is not None and best_book.under_odds is not None:
            t_o, t_u = devig_power(best_book.over_odds, best_book.under_odds)
            return t_o if side == "over" else t_u
        if side == "over" and best_book.over_odds is not None:
            return devig_single_sided(best_book.over_odds)
        if side == "under" and best_book.under_odds is not None:
            return devig_single_sided(best_book.under_odds)
        return None

    # Diff-line: same max-across-books logic the live evaluator uses.
    return _get_true_prob_for_side(match, side)


def evaluate_match(match: MatchedProp, min_ev_pct: float = 0.01) -> list[BetResult]:
    """
    Apply line discrepancy logic and return +EV BetResults.

    Rules:
      - PP line < FD line → only value on PP OVER; discard if FD favors UNDER
      - PP line > FD line → only value on PP UNDER; discard if FD favors OVER
      - PP line == FD line → evaluate both sides
    """
    # Prioritize FanDuel if available for line comparison, else use DraftKings
    # (Since both lines are likely very similar, this works for directional checks)
    best_book = match.fd or match.dk
    pp = match.pp
    results = []

    if not best_book:
        return []

    if pp.line_score == best_book.line:
        candidates = _evaluate_same_line(match)

    elif pp.line_score < best_book.line:
        # PP easier line for OVER → value exclusively on PP OVER
        # Use available odds for the HARDER line as our probability estimate
        true_over = _get_true_prob_for_side(match, "over")
        if true_over is None or true_over <= 0.5:
            return []

        bet_id = f"{pp.player_id}_{pp.stat_type}_over"
        result = BetResult(
            bet_id=bet_id,
            player_name=pp.player_name,
            league=pp.league,
            prop_type=pp.stat_type,
            pp_line=pp.line_score,
            fd_line=best_book.line,
            side="over",
            true_prob=true_over,
            over_odds=best_book.over_odds,
            under_odds=best_book.under_odds,
            both_sided=best_book.both_sided,
            pp_player_id=pp.player_id,
            start_time=getattr(pp, "start_time", ""),
        )
        candidates = [result]

    else:
        # PP harder line for UNDER → value exclusively on PP UNDER
        # Use available odds for the EASIER line as probability estimate
        true_under = _get_true_prob_for_side(match, "under")
        if true_under is None or true_under <= 0.5:
            return []

        bet_id = f"{pp.player_id}_{pp.stat_type}_under"
        result = BetResult(
            bet_id=bet_id,
            player_name=pp.player_name,
            league=pp.league,
            prop_type=pp.stat_type,
            pp_line=pp.line_score,
            fd_line=best_book.line,
            side="under",
            true_prob=true_under,
            over_odds=best_book.over_odds,
            under_odds=best_book.under_odds,
            both_sided=best_book.both_sided,
            pp_player_id=pp.player_id,
            start_time=getattr(pp, "start_time", ""),
        )
        candidates = [result]

    # Apply minimum EV filter & side constraint
    for r in candidates:
        if pp.side != "both" and pp.side != r.side:
            continue
        if r.individual_ev_pct >= min_ev_pct:
            results.append(r)

    return results


# ---------------------------------------------------------------------------
# Slip EV calculations
# ---------------------------------------------------------------------------

def power_slip_ev(true_probs: list[float]) -> Optional[float]:
    """
    EV for a Power slip: all legs must hit.
    Returns None if the pick count isn't supported.
    """
    n = len(true_probs)
    payout = POWER_PAYOUTS.get(n)
    if payout is None:
        return None
    combined = 1.0
    for p in true_probs:
        combined *= p
    return combined * payout - 1.0


def flex_slip_ev(true_probs: list[float]) -> Optional[float]:
    """
    EV for a Flex slip using full enumeration over all 2^n outcome combinations.
    Each leg has its own true_prob (independent).
    Returns None if the pick count isn't supported.
    """
    n = len(true_probs)
    payout_tiers = FLEX_PAYOUTS.get(n)
    if payout_tiers is None:
        return None

    ev = -1.0  # cost of the bet
    for outcome in itertools_product([0, 1], repeat=n):
        prob = 1.0
        for i, hit in enumerate(outcome):
            prob *= true_probs[i] if hit else (1.0 - true_probs[i])
        k = sum(outcome)
        payout = payout_tiers.get(k, 0.0)
        ev += prob * payout

    return ev


_MC_N_SIMS_DEFAULT: int = 20_000


def _sample_correlated_bernoullis(
    probs: np.ndarray,
    corr_matrix: np.ndarray,
    n_sims: int,
    rng: np.random.Generator,
) -> np.ndarray:
    """Draw `n_sims` correlated Bernoulli vectors with the given marginal
    probabilities and latent-scale correlation matrix via a Gaussian copula.

    Returns an (n_sims, n_legs) int8 array of 0/1 outcomes."""
    n = probs.shape[0]
    # Invert the standard-normal CDF once per leg. P(X_i < τ_i) = p_i where
    # X_i ~ N(0, 1). Using stdlib inv_cdf avoids a scipy dependency.
    thresholds = np.array(
        [statistics.NormalDist().inv_cdf(float(p)) for p in probs],
        dtype=float,
    )

    # Cholesky with mild jitter for numerical stability on near-singular
    # matrices (e.g. when two legs have ρ ≈ 1).
    jitter = 1e-10
    for _ in range(4):
        try:
            L = np.linalg.cholesky(corr_matrix + jitter * np.eye(n))
            break
        except np.linalg.LinAlgError:
            jitter *= 100
    else:
        # Absolute fallback: treat as independent. Better to lose the
        # correlation signal than to crash the slip evaluation.
        L = np.eye(n)

    Z = rng.standard_normal((n_sims, n))
    X = Z @ L.T
    hits = (X < thresholds[np.newaxis, :]).astype(np.int8)
    return hits


def power_slip_ev_corr(
    probs: list[float],
    corr_matrix: np.ndarray,
    n_sims: int = _MC_N_SIMS_DEFAULT,
    seed: Optional[int] = None,
) -> Optional[float]:
    """Correlation-aware Power EV via Gaussian copula Monte Carlo.

    Independence-assuming Π(p_i) systematically overstates Power EV on
    same-game stacks. This function samples correlated outcomes and computes
    P(all hit) directly, which at default n_sims has a standard error of
    ~0.7% on a 5% probability.
    """
    n = len(probs)
    payout = POWER_PAYOUTS.get(n)
    if payout is None:
        return None
    if n == 0:
        return None
    if n == 1:
        return probs[0] * payout - 1.0

    rng = np.random.default_rng(seed)
    p_arr = np.asarray(probs, dtype=float)
    hits = _sample_correlated_bernoullis(p_arr, corr_matrix, n_sims, rng)
    all_hit_rate = float((hits.sum(axis=1) == n).mean())
    return all_hit_rate * payout - 1.0


def flex_slip_ev_corr(
    probs: list[float],
    corr_matrix: np.ndarray,
    n_sims: int = _MC_N_SIMS_DEFAULT,
    seed: Optional[int] = None,
) -> Optional[float]:
    """Correlation-aware Flex EV via Gaussian copula Monte Carlo.

    Flex payouts depend on the number of hits, so we compute the full
    distribution of hit counts under the correlation structure rather than
    enumerating 2^n independent outcomes.
    """
    n = len(probs)
    payout_tiers = FLEX_PAYOUTS.get(n)
    if payout_tiers is None:
        return None

    rng = np.random.default_rng(seed)
    p_arr = np.asarray(probs, dtype=float)
    hits = _sample_correlated_bernoullis(p_arr, corr_matrix, n_sims, rng)
    hit_counts = hits.sum(axis=1)

    ev = -1.0  # cost of the bet
    for k, pay in payout_tiers.items():
        if pay == 0.0:
            continue
        ev += float((hit_counts == k).mean()) * pay
    return ev


def calculate_slip(
    bet_results: list[BetResult],
    bankroll: float,
    n_sims: int = _MC_N_SIMS_DEFAULT,
    seed: Optional[int] = None,
) -> dict:
    """
    Given a list of selected BetResults and a bankroll, compute Power and Flex EV.
    Returns a dict with slip stats ready for the frontend.

    Uses correlation-aware Monte Carlo when two or more legs share a game
    (latent-scale ρ > 0). If the correlation matrix is the identity (all
    legs from different games), we short-circuit to the exact independence
    formulas — no MC noise on unrelated slips.
    """
    n = len(bet_results)
    true_probs = [b.true_prob for b in bet_results]

    corr_matrix = build_correlation_matrix(legs_metadata_from_bets(bet_results))
    has_correlation = n >= 2 and not np.allclose(corr_matrix, np.eye(n), atol=1e-8)

    if has_correlation:
        power_ev = power_slip_ev_corr(true_probs, corr_matrix, n_sims=n_sims, seed=seed)
        flex_ev  = flex_slip_ev_corr(true_probs,  corr_matrix, n_sims=n_sims, seed=seed)
    else:
        power_ev = power_slip_ev(true_probs)
        flex_ev  = flex_slip_ev(true_probs)

    # Determine the better play
    best_type = None
    best_ev   = None
    if power_ev is not None and flex_ev is not None:
        if power_ev >= flex_ev:
            best_type, best_ev = "Power", power_ev
        else:
            best_type, best_ev = "Flex", flex_ev
    elif power_ev is not None:
        best_type, best_ev = "Power", power_ev
    elif flex_ev is not None:
        best_type, best_ev = "Flex", flex_ev

    expected_profit = bankroll * best_ev if best_ev is not None else None

    return {
        "n_picks":          n,
        "power_ev_pct":     round(power_ev, 4) if power_ev is not None else None,
        "flex_ev_pct":      round(flex_ev, 4)  if flex_ev  is not None else None,
        "best_play_type":   best_type,
        "best_ev_pct":      round(best_ev, 4)  if best_ev  is not None else None,
        "expected_profit":  round(expected_profit, 2) if expected_profit is not None else None,
        "bankroll":         bankroll,
        "legs": [
            {
                "player_name": b.player_name,
                "prop_type":   b.prop_type,
                "pp_line":     b.pp_line,
                "side":        b.side,
                "true_prob":   round(b.true_prob, 4),
                "ind_ev_pct":  round(b.individual_ev_pct, 4),
            }
            for b in bet_results
        ],
    }
