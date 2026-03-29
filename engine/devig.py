"""
De-vig utilities.

- Multiplicative de-vig when both sides of a prop are available.
- Single-sided de-vig using a conservative 4.5% vig assumption.
"""
from config import SINGLE_SIDE_VIG


def american_to_decimal(american: int) -> float:
    """Convert American odds to decimal odds."""
    if american > 0:
        return (american / 100.0) + 1.0
    else:
        return (100.0 / abs(american)) + 1.0


def american_to_implied(american: int) -> float:
    """Raw (vigged) implied probability from American odds."""
    return 1.0 / american_to_decimal(american)


def devig_multiplicative(over_american: int, under_american: int) -> tuple[float, float]:
    """
    Multiplicative de-vig. Returns (true_over_prob, true_under_prob).
    Scales each implied probability proportionally so they sum to 1.
    """
    implied_over  = american_to_implied(over_american)
    implied_under = american_to_implied(under_american)
    total = implied_over + implied_under
    return implied_over / total, implied_under / total


def devig_single_sided(american: int) -> float:
    """
    Conservative single-sided de-vig assuming SINGLE_SIDE_VIG total market vig.
    Understates true probability to avoid false +EV signals.

    true_prob = implied_prob / (1 + vig)
    """
    implied = american_to_implied(american)
    return implied / (1.0 + SINGLE_SIDE_VIG)
