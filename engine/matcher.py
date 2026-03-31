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
    fd: FanDuelProp
    name_score: float    # fuzzy similarity 0-100


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


def normalize_prop_type(raw: str) -> Optional[str]:
    """
    Map a raw FanDuel prop type string to the canonical PrizePicks stat_type label.
    Returns None if the prop type is unrecognized (should be skipped).
    """
    key = raw.lower().strip()
    return PROP_TYPE_MAP.get(key)


# ---------------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------------

def match_props(
    fd_props: list[FanDuelProp],
    pp_lines: list[PrizePickLine],
) -> list[MatchedProp]:
    """
    For each PrizePicks line, find the best matching FanDuel prop by:
      1. Same league
      2. Same normalized stat_type
      3. Highest fuzzy player name similarity above FUZZY_THRESHOLD

    Returns a list of MatchedProp (one per PP line that found a match).
    """
    # Build lookup: (league, prop_type_lower) → list of FanDuelProp
    fd_index: dict[tuple, list[FanDuelProp]] = {}
    for fd in fd_props:
        key = (fd.league.upper(), fd.prop_type.lower())
        fd_index.setdefault(key, []).append(fd)

    # Alias map: PrizePicks stat names that differ from FanDuel prop names
    _STAT_ALIASES = {
        "goalie saves": "saves",
    }

    results: list[MatchedProp] = []

    for pp in pp_lines:
        stat_key = pp.stat_type.lower()
        stat_key = _STAT_ALIASES.get(stat_key, stat_key)
        key = (pp.league.upper(), stat_key)
        candidates = fd_index.get(key, [])
        if not candidates:
            continue

        # Fuzzy match player names
        norm_pp_name = normalize_name(pp.player_name)
        norm_candidates = [(fd, normalize_name(fd.player_name)) for fd in candidates]

        best_fd = None
        best_score = 0.0
        for fd, norm_fd_name in norm_candidates:
            score = fuzz.token_sort_ratio(norm_pp_name, norm_fd_name)
            if score > best_score:
                best_score = score
                best_fd = fd
            elif score == best_score and score >= FUZZY_THRESHOLD and best_fd is not None:
                # Prefer the FD prop whose line matches the PP line
                if fd.line == pp.line_score and best_fd.line != pp.line_score:
                    best_fd = fd

        if best_fd is not None and best_score >= FUZZY_THRESHOLD:
            results.append(MatchedProp(pp=pp, fd=best_fd, name_score=best_score))

    return results
