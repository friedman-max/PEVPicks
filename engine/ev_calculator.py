"""
EV calculation engine.

- Per-leg individual EV% (theoretical, using optimal 5/6-Flex break-even)
- Slip EV% for Power (exact) and Flex (full enumeration over all leg combinations)
- Line discrepancy directional logic
"""
from itertools import product as itertools_product
from typing import Optional

from engine.constants import (
    OPTIMAL_BREAK_EVEN,
    OPTIMAL_IMPLIED_DECIMAL,
    POWER_PAYOUTS,
    FLEX_PAYOUTS,
)
from engine.devig import devig_multiplicative, devig_single_sided, prob_to_american
from engine.matcher import MatchedProp


# ---------------------------------------------------------------------------
# Individual bet result
# ---------------------------------------------------------------------------

class BetResult:
    __slots__ = (
        "bet_id", "player_name", "league", "prop_type",
        "pp_line", "fd_line", "side",
        "true_prob", "true_odds", "edge", "individual_ev_pct",
        "over_odds", "under_odds", "both_sided",
        "pp_player_id",
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
    ):
        self.bet_id = bet_id
        self.player_name = player_name
        self.league = league
        self.prop_type = prop_type
        self.pp_line = pp_line
        self.fd_line = fd_line
        self.side = side
        self.true_prob = true_prob
        self.over_odds = over_odds
        self.under_odds = under_odds
        self.both_sided = both_sided
        self.pp_player_id = pp_player_id
        self.true_odds = prob_to_american(true_prob)

        self.edge = round(true_prob - OPTIMAL_BREAK_EVEN, 6)
        self.individual_ev_pct = round((true_prob * OPTIMAL_IMPLIED_DECIMAL) - 1.0, 6)

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
        }


# ---------------------------------------------------------------------------
# Line discrepancy logic
# ---------------------------------------------------------------------------

def _evaluate_same_line(match: MatchedProp) -> list[BetResult]:
    """Both lines match. Evaluate OVER and UNDER independently."""
    fd = match.fd
    pp = match.pp
    results = []

    if fd.both_sided and fd.over_odds is not None and fd.under_odds is not None:
        true_over, true_under = devig_multiplicative(fd.over_odds, fd.under_odds)
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
        )
        if result.individual_ev_pct > 0:
            results.append(result)

    return results


def _get_true_prob_for_side(fd, side: str) -> Optional[float]:
    """De-vig and return true probability for a specific side."""
    if fd.both_sided and fd.over_odds is not None and fd.under_odds is not None:
        true_over, true_under = devig_multiplicative(fd.over_odds, fd.under_odds)
        return true_over if side == "over" else true_under
    elif side == "over" and fd.over_odds is not None:
        return devig_single_sided(fd.over_odds)
    elif side == "under" and fd.under_odds is not None:
        return devig_single_sided(fd.under_odds)
    return None


def evaluate_match(match: MatchedProp, min_ev_pct: float = 0.01) -> list[BetResult]:
    """
    Apply line discrepancy logic and return +EV BetResults.

    Rules:
      - PP line < FD line → only value on PP OVER; discard if FD favors UNDER
      - PP line > FD line → only value on PP UNDER; discard if FD favors OVER
      - PP line == FD line → evaluate both sides
    """
    fd = match.fd
    pp = match.pp
    results = []

    if pp.line_score == fd.line:
        candidates = _evaluate_same_line(match)

    elif pp.line_score < fd.line:
        # PP easier line for OVER → value exclusively on PP OVER
        # Use FanDuel odds for the HARDER line (fd.line) as our probability estimate
        true_over = _get_true_prob_for_side(fd, "over")
        if true_over is None or true_over <= 0.5:
            # FD doesn't favor the OVER at the higher line → contradiction → discard
            return []

        bet_id = f"{pp.player_id}_{pp.stat_type}_over"
        result = BetResult(
            bet_id=bet_id,
            player_name=pp.player_name,
            league=pp.league,
            prop_type=pp.stat_type,
            pp_line=pp.line_score,
            fd_line=fd.line,
            side="over",
            true_prob=true_over,
            over_odds=fd.over_odds,
            under_odds=fd.under_odds,
            both_sided=fd.both_sided,
            pp_player_id=pp.player_id,
        )
        candidates = [result] if result.individual_ev_pct > 0 else []

    else:
        # PP harder line for UNDER → value exclusively on PP UNDER
        # Use FanDuel odds for the EASIER line (fd.line) as probability estimate
        true_under = _get_true_prob_for_side(fd, "under")
        if true_under is None or true_under <= 0.5:
            # FD doesn't favor UNDER at the lower line → contradiction → discard
            return []

        bet_id = f"{pp.player_id}_{pp.stat_type}_under"
        result = BetResult(
            bet_id=bet_id,
            player_name=pp.player_name,
            league=pp.league,
            prop_type=pp.stat_type,
            pp_line=pp.line_score,
            fd_line=fd.line,
            side="under",
            true_prob=true_under,
            over_odds=fd.over_odds,
            under_odds=fd.under_odds,
            both_sided=fd.both_sided,
            pp_player_id=pp.player_id,
        )
        candidates = [result] if result.individual_ev_pct > 0 else []

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


def calculate_slip(
    bet_results: list[BetResult],
    bankroll: float,
) -> dict:
    """
    Given a list of selected BetResults and a bankroll, compute Power and Flex EV.
    Returns a dict with slip stats ready for the frontend.
    """
    n = len(bet_results)
    true_probs = [b.true_prob for b in bet_results]

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
