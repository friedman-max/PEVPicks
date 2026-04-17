"""
Sharpness-weighted consensus engine (VWAP-style).

Combines devigged probabilities from multiple sportsbooks into a single
"true" probability using:

  1. **Operator sharpness weights** — empirically derived per-book influence
     reflecting each operator's price-discovery quality for player props.
  2. **Market width penalty** — tighter markets (higher confidence) receive
     more influence; wide, uncertain markets are discounted.
  3. **Scaled single-source discount** — when only one book offers a line,
     apply a conservative discount that scales with odds magnitude.

The consensus formula mirrors Volume-Weighted Average Price (VWAP) from
traditional financial markets:

    P_consensus = Σ(p_i × w_i × (1/M_i)) / Σ(w_i × (1/M_i))

Where p_i = Power Method devigged prob, w_i = sharpness weight,
M_i = market width (overround %).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from engine.devig import (
    american_to_implied,
    devig_power,
    devig_multiplicative,
    devig_worst_case,
    devig_single_sided_scaled,
    apply_single_source_discount,
    market_width_cents,
    prob_to_american,
    revigg_power,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Operator sharpness weights for player-prop markets
# ---------------------------------------------------------------------------
# Empirical research indicates that for secondary prop markets, domestic
# US sportsbooks with high SGP/prop liquidity originate price discovery.
# Pinnacle is the sharpest for mainlines but frequently lags on props.
# These weights will be calibrated over time via CLV tracking.

SHARPNESS_WEIGHTS = {
    "fanduel":    1.20,   # Best prop price discovery, fastest to react
    "pinnacle":   0.95,   # Sharp for mainlines, lags on niche props
    "draftkings": 0.90,   # Recreational volume heavy, slower adjustments
}

# Default weight for any unknown sportsbook
_DEFAULT_WEIGHT = 0.80

# Minimum market width (overround %) to avoid division-by-zero.
# A market with lower overround than this is already extremely efficient.
_MIN_MARKET_WIDTH = 1.0  # 1% overround


# ---------------------------------------------------------------------------
# Book data container
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class BookOdds:
    """Odds from a single sportsbook for one prop."""
    book_name: str          # "fanduel", "draftkings", "pinnacle"
    over_odds: Optional[int | float]
    under_odds: Optional[int | float]
    both_sided: bool


# ---------------------------------------------------------------------------
# Core consensus computation
# ---------------------------------------------------------------------------

def _get_sharpness_weight(book_name: str) -> float:
    """Look up the sharpness weight for a book."""
    return SHARPNESS_WEIGHTS.get(book_name.lower(), _DEFAULT_WEIGHT)


def _has_direct_odds(book: BookOdds, side: str) -> bool:
    """Check if a book has direct (non-derived) odds for the requested side."""
    if book.both_sided:
        return True
    if side == "over" and book.over_odds is not None:
        return True
    if side == "under" and book.under_odds is not None:
        return True
    return False


def _devig_book(book: BookOdds, side: str) -> Optional[float]:
    """
    Devig a single book's odds for the requested side using the best
    available method.

    - Both-sided → Power Method (primary)
    - Single-sided → Scaled single-sided devig for the available side,
      complement (1 - p) for the missing side.
    """
    if book.both_sided and book.over_odds is not None and book.under_odds is not None:
        p_over, p_under = devig_power(book.over_odds, book.under_odds)
        return p_over if side == "over" else p_under

    # Single-sided: devig the side we have, derive the other via complement.
    if book.over_odds is not None:
        p_over = devig_single_sided_scaled(book.over_odds)
        return p_over if side == "over" else 1.0 - p_over
    if book.under_odds is not None:
        p_under = devig_single_sided_scaled(book.under_odds)
        return p_under if side == "under" else 1.0 - p_under

    return None


def _devig_book_worst_case(book: BookOdds, side: str) -> Optional[float]:
    """
    Devig using worst-case method for conservative output.
    Same complement logic for single-sided books.
    """
    if book.both_sided and book.over_odds is not None and book.under_odds is not None:
        p_over, p_under = devig_worst_case(book.over_odds, book.under_odds)
        return p_over if side == "over" else p_under

    # Single-sided: devig available side, complement for missing side.
    if book.over_odds is not None:
        p_over = devig_single_sided_scaled(book.over_odds)
        return p_over if side == "over" else 1.0 - p_over
    if book.under_odds is not None:
        p_under = devig_single_sided_scaled(book.under_odds)
        return p_under if side == "under" else 1.0 - p_under

    return None


def _get_market_width(book: BookOdds) -> float:
    """
    Get market width in percentage points.
    Returns the overround for two-sided markets, or a conservative
    default for single-sided markets (indicating low confidence).
    """
    if book.both_sided and book.over_odds is not None and book.under_odds is not None:
        width = market_width_cents(book.over_odds, book.under_odds)
        return max(width, _MIN_MARKET_WIDTH)
    # Single-sided markets get a high-width penalty (low confidence)
    return 15.0  # ~15% assumed overround for one-way markets


_DEFAULT_MARGIN = 0.07   # 7% — typical US sportsbook overround for props

def _get_side_odds(book: BookOdds, side: str) -> Optional[int | float]:
    """
    Get the American odds for a specific side.

    If the book only has the opposite side, derive realistic vigged odds
    for the requested side using the inverse Power Method with a standard
    7% overround.  This ensures derived odds look like real book odds
    (implied probs sum to ~107%) rather than fair/no-vig odds.
    """
    direct = book.over_odds if side == "over" else book.under_odds
    if direct is not None:
        return direct

    # Derive from the opposite side using inverse Power Method re-vig
    opposite = book.under_odds if side == "over" else book.over_odds
    if opposite is not None:
        available_true = devig_single_sided_scaled(opposite)
        missing_true = 1.0 - available_true
        if missing_true <= 0 or missing_true >= 1:
            return None
        # Re-vig with realistic margin
        if side == "over":
            vigged_over, _ = revigg_power(missing_true, available_true, _DEFAULT_MARGIN)
            return prob_to_american(vigged_over)
        else:
            _, vigged_under = revigg_power(available_true, missing_true, _DEFAULT_MARGIN)
            return prob_to_american(vigged_under)

    return None


def compute_true_probability(
    books: list[BookOdds],
    side: str,
) -> tuple[Optional[float], Optional[float], dict]:
    """
    Compute the consensus true probability for a given side (over/under)
    across all available sportsbooks.

    Returns:
        (consensus_prob, worst_case_prob, metadata)

    Where:
    - consensus_prob: VWAP sharpness-weighted probability (informational)
    - worst_case_prob: most conservative probability (used for EV decisions)
    - metadata: dict with n_books, devig_method, market_widths, etc.
    """
    # ── Safeguard: reject purely complement-derived probabilities ─────────
    # If NO book has direct odds for the requested side (i.e. every book's
    # probability is derived via complement from the opposite side), reject.
    # Complement-derived probabilities from extreme longshot single-sided
    # lines (e.g. +700 'to record 1+ shots') are unreliable and produce
    # phantom high-EV bets.
    has_any_direct = any(_has_direct_odds(b, side) for b in books)
    if not has_any_direct:
        return None, None, {"n_books": 0, "devig_method": "no_direct_odds"}

    # Collect per-book data as tuples: (power_prob, worst_prob, weight, width, odds)
    # Tuples are ~3x smaller than equivalent dicts and avoid per-match key overhead.
    entries: list[tuple] = []
    for book in books:
        power_prob = _devig_book(book, side)
        worst_prob = _devig_book_worst_case(book, side)
        odds = _get_side_odds(book, side)

        if power_prob is None or odds is None:
            continue

        weight = _get_sharpness_weight(book.book_name)
        width = _get_market_width(book)

        entries.append((power_prob, worst_prob, weight, width, odds))

    if not entries:
        return None, None, {"n_books": 0, "devig_method": "none"}

    n_books = len(entries)

    # ------------------------------------------------------------------
    # Single-source fallback
    # ------------------------------------------------------------------
    if n_books == 1:
        power_prob, worst_prob, _weight, _width, odds = entries[0]
        prob = worst_prob or power_prob

        # Apply the scaled single-source uncertainty discount
        discounted = apply_single_source_discount(prob, odds)

        return (
            discounted,
            discounted,
            {"n_books": 1, "devig_method": "single_source_scaled"},
        )

    # ------------------------------------------------------------------
    # Multi-source VWAP consensus
    # ------------------------------------------------------------------
    # Consensus formula:
    #   P_c = Σ(p_i × w_i × (1/M_i)) / Σ(w_i × (1/M_i))

    weighted_sum = 0.0
    weight_denom = 0.0
    worst_case_prob: Optional[float] = None

    for power_prob, worst_prob, weight, width, _odds in entries:
        inv_width = 1.0 / width
        effective_weight = weight * inv_width

        weighted_sum += power_prob * effective_weight
        weight_denom += effective_weight

        if worst_prob is not None and (worst_case_prob is None or worst_prob < worst_case_prob):
            worst_case_prob = worst_prob

    consensus_prob = weighted_sum / weight_denom if weight_denom > 0 else None

    # The final worst-case should not exceed the consensus
    # (if consensus is lower due to weighting, use that)
    if consensus_prob is not None and worst_case_prob is not None and worst_case_prob > consensus_prob:
        worst_case_prob = consensus_prob

    # Metadata kept minimal — callers (pipeline, clv_checker) only read the
    # two probabilities. Building a per_book list per match allocated tens of
    # thousands of short-lived dicts on every scrape cycle.
    metadata = {"n_books": n_books, "devig_method": "power_vwap"}

    return consensus_prob, worst_case_prob, metadata


# ---------------------------------------------------------------------------
# Convenience: build BookOdds from matcher objects
# ---------------------------------------------------------------------------

def books_from_match(fd, dk, pin) -> list[BookOdds]:
    """
    Build a list of BookOdds from the FanDuelProp-shaped objects used by the
    matcher (fd, dk, pin can each be None).
    """
    books = []
    if fd is not None:
        books.append(BookOdds(
            book_name="fanduel",
            over_odds=fd.over_odds,
            under_odds=fd.under_odds,
            both_sided=fd.both_sided,
        ))
    if dk is not None:
        books.append(BookOdds(
            book_name="draftkings",
            over_odds=dk.over_odds,
            under_odds=dk.under_odds,
            both_sided=dk.both_sided,
        ))
    if pin is not None:
        books.append(BookOdds(
            book_name="pinnacle",
            over_odds=pin.over_odds,
            under_odds=pin.under_odds,
            both_sided=pin.both_sided,
        ))
    return books
