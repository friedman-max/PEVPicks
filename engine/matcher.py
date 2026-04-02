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
    dk: Optional[FanDuelProp] = None


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
    dk_props: list[FanDuelProp],
    pp_lines: list[PrizePickLine],
) -> list[MatchedProp]:
    """
    For each PrizePicks line, find the best matching FanDuel and DraftKings prop.
    Only returns matches where BOTH books provide a line for that prop.
    """
    # Build lookup: (league, prop_type_lower) → list of FanDuelProp for FD and DK
    fd_index: dict[tuple, list[FanDuelProp]] = {}
    for fd in fd_props:
        key = (fd.league.upper(), fd.prop_type.lower())
        fd_index.setdefault(key, []).append(fd)
        
    dk_index: dict[tuple, list[FanDuelProp]] = {}
    for dk in dk_props:
        key = (dk.league.upper(), dk.prop_type.lower())
        dk_index.setdefault(key, []).append(dk)

    # Alias map: PrizePicks stat names that differ from FanDuel/DraftKings prop names
    _STAT_ALIASES = {
        "goalie saves": "saves",
    }

    results: list[MatchedProp] = []

    for pp in pp_lines:
        stat_key = pp.stat_type.lower()
        stat_key = _STAT_ALIASES.get(stat_key, stat_key)
        key = (pp.league.upper(), stat_key)
        
        fd_candidates = fd_index.get(key, [])
        dk_candidates = dk_index.get(key, [])
        
        # We only care about props that exist on BOTH books
        if not fd_candidates or not dk_candidates:
            continue

        norm_pp_name = normalize_name(pp.player_name)
        
        # Match FanDuel
        best_fd = None
        best_fd_score = 0.0
        for fd in fd_candidates:
            score = fuzz.token_sort_ratio(norm_pp_name, normalize_name(fd.player_name))
            if score > best_fd_score:
                best_fd_score = score
                best_fd = fd
            elif score == best_fd_score and score >= FUZZY_THRESHOLD and best_fd is not None:
                if fd.line == pp.line_score and best_fd.line != pp.line_score:
                    best_fd = fd
                    
        # Match DraftKings
        best_dk = None
        best_dk_score = 0.0
        for dk in dk_candidates:
            score = fuzz.token_sort_ratio(norm_pp_name, normalize_name(dk.player_name))
            if score > best_dk_score:
                best_dk_score = score
                best_dk = dk
            elif score == best_dk_score and score >= FUZZY_THRESHOLD and best_dk is not None:
                if dk.line == pp.line_score and best_dk.line != pp.line_score:
                    best_dk = dk

        # Only add if we have high-confidence matches on BOTH books
        if best_fd is not None and best_dk is not None and \
           best_fd_score >= FUZZY_THRESHOLD and best_dk_score >= FUZZY_THRESHOLD:
            results.append(MatchedProp(pp=pp, fd=best_fd, dk=best_dk, name_score=best_fd_score))

    return results
