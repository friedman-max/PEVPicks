"""
Fuzzy matching between FanDuel props and PrizePicks lines.
"""
import re
import unicodedata
from dataclasses import dataclass
from typing import Optional

from rapidfuzz import fuzz, process

from engine.constants import PROP_TYPE_MAP
from config import FUZZY_THRESHOLD


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class FanDuelProp:
    league: str
    player_name: str
    prop_type: str       # normalized to PrizePicks label
    line: float
    over_odds: Optional[int]    # American; None if not available
    under_odds: Optional[int]   # American; None if not available
    both_sided: bool
    start_time: str = ""


@dataclass
class PrizePickLine:
    league: str
    player_name: str
    stat_type: str       # PrizePicks stat_type label
    line_score: float
    player_id: str       # PrizePicks internal ID
    start_time: str = "" # ISO timestamp of game start
    side: str = "both"   # "both", "over", or "under"


@dataclass
class MatchedProp:
    pp: PrizePickLine
    name_score: float    # fuzzy similarity 0-100
    fd: Optional[FanDuelProp] = None
    dk: Optional[FanDuelProp] = None
    pin: Optional[FanDuelProp] = None


# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------

_SUFFIX_RE = re.compile(
    r"\b(jr\.?|sr\.?|ii|iii|iv|v)\b", re.IGNORECASE
)
_PUNCT_RE = re.compile(r"[^\w\s]")
_SPACE_RE = re.compile(r"\s+")


def normalize_name(name: str) -> str:
    """Lowercase, strip accents, remove punctuation/suffixes, collapse whitespace."""
    # Unicode normalization to strip accents
    name = unicodedata.normalize("NFD", name)
    name = "".join(c for c in name if unicodedata.category(c) != "Mn")
    name = name.lower()
    name = _SUFFIX_RE.sub("", name)
    name = _PUNCT_RE.sub("", name)
    name = _SPACE_RE.sub(" ", name).strip()
    return name


def normalize_prop_type(raw: str, league: str) -> Optional[str]:
    """
    Map a raw FanDuel prop type string to the canonical PrizePicks stat_type label,
    using league-aware dictionary lookup.
    Returns None if the prop type is unrecognized (should be skipped).
    """
    key = raw.lower().strip()
    return PROP_TYPE_MAP.get(league.upper(), {}).get(key)


# ---------------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------------

def match_props(
    fd_props: list[FanDuelProp],
    dk_props: list[FanDuelProp],
    pp_lines: list[PrizePickLine],
    pin_props: list[FanDuelProp] | None = None,
) -> list[MatchedProp]:
    """
    For each PrizePicks line, find the best matching FanDuel, DraftKings,
    and Pinnacle prop.
    Only returns matches where at least one book provides a line for that prop.
    """
    pin_props = pin_props or []

    # Build lookup: (league, prop_type_lower) → list of props per book
    fd_index: dict[tuple, list[FanDuelProp]] = {}
    for fd in fd_props:
        key = (fd.league.upper(), fd.prop_type.lower())
        fd_index.setdefault(key, []).append(fd)

    dk_index: dict[tuple, list[FanDuelProp]] = {}
    for dk in dk_props:
        key = (dk.league.upper(), dk.prop_type.lower())
        dk_index.setdefault(key, []).append(dk)

    pin_index: dict[tuple, list[FanDuelProp]] = {}
    for pin in pin_props:
        key = (pin.league.upper(), pin.prop_type.lower())
        pin_index.setdefault(key, []).append(pin)

    # League-aware alias map: PrizePicks stat names that differ from book prop names
    # Only apply aliases for specific leagues to avoid cross-league pollution
    _STAT_ALIASES = {
        "SOCCER": {
            "saves": "goalie saves",
            "goalkeeper saves": "goalie saves",
        },
    }

    results: list[MatchedProp] = []

    for pp in pp_lines:
        stat_key = pp.stat_type.lower()
        league_aliases = _STAT_ALIASES.get(pp.league.upper(), {})
        stat_key = league_aliases.get(stat_key, stat_key)
        key = (pp.league.upper(), stat_key)

        fd_candidates = fd_index.get(key, [])
        dk_candidates = dk_index.get(key, [])
        pin_candidates = pin_index.get(key, [])

        # We need at least one book to compare against
        if not fd_candidates and not dk_candidates and not pin_candidates:
            continue

        norm_pp_name = normalize_name(pp.player_name)

        def _best_match(candidates):
            best = None
            best_score = 0.0
            for c in candidates:
                score = fuzz.token_sort_ratio(norm_pp_name, normalize_name(c.player_name))
                if score > best_score:
                    best_score = score
                    best = c
                elif score == best_score and score >= FUZZY_THRESHOLD and best is not None:
                    if c.line == pp.line_score and best.line != pp.line_score:
                        best = c
            if best_score < FUZZY_THRESHOLD:
                return None, 0.0
            return best, best_score

        best_fd, best_fd_score = _best_match(fd_candidates)
        best_dk, best_dk_score = _best_match(dk_candidates)
        best_pin, best_pin_score = _best_match(pin_candidates)

        # Return if we have a high-confidence match in AT LEAST ONE of the books
        scores = []
        if best_fd is not None: scores.append(best_fd_score)
        if best_dk is not None: scores.append(best_dk_score)
        if best_pin is not None: scores.append(best_pin_score)

        if scores and max(scores) >= FUZZY_THRESHOLD:
            results.append(MatchedProp(
                pp=pp,
                fd=best_fd,
                dk=best_dk,
                pin=best_pin,
                name_score=max(scores),
            ))

    return results
