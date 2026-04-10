"""
ESPN unofficial API result checker for CoreProp backtests.

Reads pending rows from data/backtest.csv, fetches ESPN box scores,
and marks each bet as "hit" or "miss" with the actual stat value.
Covers NBA, NCAAB, MLB, NHL.
"""
import csv
import logging
import pathlib
import unidecode
from datetime import datetime, timezone, timedelta
from typing import Optional

import requests as _requests
from rapidfuzz import fuzz

logger = logging.getLogger(__name__)

DATA_DIR = pathlib.Path(__file__).parent.parent / "data"
CSV_PATH  = DATA_DIR / "backtest.csv"

# ESPN scoreboard (for game IDs by date)
ESPN_SCOREBOARD = {
    "NBA":   "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard",
    "NCAAB": "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard",
    "MLB":   "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard",
    "NHL":   "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard",
}

# ESPN event summary (for box scores)
ESPN_SUMMARY = {
    "NBA":   "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary",
    "NCAAB": "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/summary",
    "MLB":   "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/summary",
    "NHL":   "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/summary",
}

# Conservative estimate of how long after game_start a result can be fetched
GAME_DURATION_MINUTES = {
    "NBA":   180,   # 3 h
    "NCAAB": 150,   # 2.5 h
    "MLB":   225,   # 3.75 h
    "NHL":   180,   # 3 h
}

FUZZY_THRESHOLD = 80   # Strict threshold for name matching


class ESPNResultsChecker:
    """Checks ESPN box scores and back-fills result + stat_actual in the backtest CSV."""

    def __init__(self):
        self._session = _requests.Session()
        self._session.headers["User-Agent"] = "Mozilla/5.0"
        # (league, date_str) → {player_name_lower: stats_dict}
        self._cache: dict[tuple, dict] = {}
        # (league_lower, player_name_lower) → stats_dict (closest to target time)
        self._gamelog_cache: dict[tuple, dict] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def check_pending_results(self, csv_path: pathlib.Path = CSV_PATH) -> int:
        """
        Read the backtest CSV, find rows where result == 'pending' and the
        game has had time to finish, then fetch ESPN and update the rows.
        Returns the number of rows updated.
        """
        # Always clear caches so we get fresh ESPN data (stale cache was
        # the #1 cause of permanently-stuck 'pending' rows)
        self._cache.clear()
        self._gamelog_cache.clear()

        if not csv_path.exists():
            return 0

        try:
            with open(csv_path, "r", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
        except Exception as exc:
            logger.error("ResultsChecker: cannot read CSV: %s", exc)
            return 0

        if not rows:
            return 0

        now_utc = datetime.now(timezone.utc)
        updated = 0
        changed = False

        for row in rows:
            if row.get("result") not in ("pending", ""):
                continue

            game_start_str = row.get("game_start", "")
            league = row.get("league", "").upper()
            if not game_start_str or league not in ESPN_SCOREBOARD:
                continue

            # Parse game start
            try:
                gs = datetime.fromisoformat(game_start_str.replace("Z", "+00:00"))
                if gs.tzinfo is None:
                    gs = gs.replace(tzinfo=timezone.utc)
            except Exception:
                continue

            # Only attempt after the game is estimated to have finished
            duration   = GAME_DURATION_MINUTES.get(league, 180)
            likely_end = gs + timedelta(minutes=duration)
            if now_utc < likely_end:
                continue

            date_str    = gs.strftime("%Y%m%d")
            player_name = row.get("player", "")
            prop_type   = row.get("prop", "")
            side        = row.get("side", "over")
            try:
                line = float(row.get("line") or 0)
            except ValueError:
                continue

            player_stats = self._get_player_stats(league, date_str, player_name)
            
            actual = None
            if player_stats is not None:
                actual = self._compute_stat(player_stats, prop_type, league)
                
            if actual is None:
                logger.debug("ResultsChecker: trying gamelog fallback for %s (%s)", player_name, prop_type)
                gl_stats = self._fetch_gamelog_stats(league, player_name, gs)
                if gl_stats is not None:
                    actual = self._compute_stat(gl_stats, prop_type, league)

            if actual is None:
                # If the game ended over 6 hours ago and we still can't find
                # the player, they almost certainly didn't play (DNP/injury).
                hours_since_end = (now_utc - likely_end).total_seconds() / 3600
                if hours_since_end >= 6:
                    row["result"] = "dnp"
                    row["stat_actual"] = ""
                    updated += 1
                    changed = True
                    logger.info(
                        "ResultsChecker: marking %s as DNP (game ended %.0fh ago, "
                        "no stats found for '%s')",
                        player_name, hours_since_end, prop_type,
                    )
                else:
                    logger.debug(
                        "ResultsChecker: cannot compute '%s' for %s (game ended %.1fh ago, will retry)",
                        prop_type, player_name, hours_since_end,
                    )
                continue

            if actual == line:
                result = "push"
            else:
                result = "hit" if (actual > line if side == "over" else actual < line) else "miss"
            row["result"]      = result
            row["stat_actual"] = actual
            updated += 1
            changed = True
            logger.debug(
                "ResultsChecker: %s %s %s %s %.1f  actual=%.1f  →  %s",
                league, player_name, prop_type, side, line, actual, result,
            )

        if changed:
            self._write_csv(csv_path, rows)

        if updated:
            logger.info("ResultsChecker: updated %d pending rows", updated)
        return updated

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _get_player_stats(
        self, league: str, date_str: str, player_name: str
    ) -> Optional[dict]:
        cache_key = (league, date_str)
        if cache_key not in self._cache:
            self._cache[cache_key] = self._fetch_all_stats(league, date_str)

        stats_by_player = self._cache.get(cache_key, {})
        if not stats_by_player:
            return None

        def _norm(n): return unidecode.unidecode(n).lower().strip()
        name_lower = _norm(player_name)
        
        best_score  = 0
        best_stats  = None
        best_display = None

        for known_name, stats in stats_by_player.items():
            score = fuzz.token_sort_ratio(name_lower, _norm(known_name))
            if score > best_score:
                best_score = score
                best_stats = stats
                best_display = known_name

        if best_score >= FUZZY_THRESHOLD:
            logger.debug(
                "ResultsChecker: matched '%s' to ESPN '%s' (score %d)",
                player_name, best_display, best_score
            )
            return best_stats
        return None

    def _fetch_all_stats(self, league: str, date_str: str) -> dict:
        """Fetch and aggregate all player stats for a league + date from ESPN."""
        scoreboard_url = ESPN_SCOREBOARD.get(league)
        summary_url    = ESPN_SUMMARY.get(league)
        if not scoreboard_url or not summary_url:
            return {}

        try:
            r = self._session.get(scoreboard_url, params={"dates": date_str}, timeout=15)
            r.raise_for_status()
            events = r.json().get("events", [])
        except Exception as exc:
            logger.warning(
                "ResultsChecker: scoreboard error %s/%s: %s", league, date_str, exc
            )
            return {}

        all_stats: dict = {}
        for event in events:
            event_id = event.get("id")
            if not event_id:
                continue
            try:
                r2 = self._session.get(summary_url, params={"event": event_id}, timeout=15)
                r2.raise_for_status()
                summary = r2.json()
            except Exception as exc:
                logger.warning(
                    "ResultsChecker: summary error event %s: %s", event_id, exc
                )
                continue

            all_stats.update(self._parse_box_score(summary))

        return all_stats

    @staticmethod
    def _parse_box_score(summary: dict) -> dict:
        """
        Parse ESPN summary JSON → {player_name_lower: {stat_name: raw_value}}.
        """
        result: dict = {}
        for section in summary.get("boxscore", {}).get("players", []):
            for stat_block in section.get("statistics", []):
                field_names = [n.lower() for n in stat_block.get("names", [])]
                if not field_names:
                    field_names = [k.lower() for k in stat_block.get("keys", [])]
                for entry in stat_block.get("athletes", []):
                    display = entry.get("athlete", {}).get("displayName", "")
                    raw     = entry.get("stats", [])
                    if not display or not raw:
                        continue
                    stat_dict = {
                        field_names[i]: raw[i]
                        for i in range(min(len(field_names), len(raw)))
                    }
                    display_lower = display.lower()
                    if display_lower in result:
                        result[display_lower].update(stat_dict)
                    else:
                        result[display_lower] = stat_dict
        return result

    @staticmethod
    def _compute_stat(
        stats: dict, prop_type: str, league: str
    ) -> Optional[float]:
        """Convert raw ESPN stat dict to a float for the given prop type."""

        def _num(*keys) -> Optional[float]:
            """Try each key alias in order until a non-None value is found."""
            for key in keys:
                val = stats.get(key.lower())
                if val is not None:
                    try:
                        if isinstance(val, (int, float)):
                            return float(val)
                        sval = str(val).strip()
                        if not sval or sval == "--":
                            return 0.0
                        return float(sval.split("-")[0])
                    except (ValueError, IndexError):
                        continue
            return None

        # ── Basketball ──────────────────────────────────────────
        pts = _num("pts", "points")
        reb = _num("reb", "rebounds", "totreb", "trb")
        # Fallback for Reb: sum OREB + DREB if total is missing or 0 but components exist
        if (reb is None or reb == 0) and league != "NHL":
            oreb = _num("oreb", "offensiverebounds")
            dreb = _num("dreb", "defensiverebounds")
            if oreb is not None and dreb is not None:
                reb = oreb + dreb

        ast = _num("ast", "assists")
        stl = _num("stl", "steals")
        blk = _num("blk", "blocks", "blockedshots")
        to  = _num("to", "turnovers")
        pm3 = _num("3pt", "3pm", "threepointfieldgoalsmade")

        if prop_type == "Points":
            return pts
        if prop_type == "Rebounds":
            return reb
        if prop_type == "Assists" and league != "NHL":
            return ast
        if prop_type == "3-PT Made":
            return pm3
        if prop_type == "Pts+Rebs+Asts":
            return None if any(v is None for v in (pts, reb, ast)) else pts + reb + ast
        if prop_type == "Pts+Rebs":
            return None if any(v is None for v in (pts, reb)) else pts + reb
        if prop_type == "Pts+Asts":
            return None if any(v is None for v in (pts, ast)) else pts + ast
        if prop_type == "Rebs+Asts":
            return None if any(v is None for v in (reb, ast)) else reb + ast
        if prop_type == "Steals":
            return stl
        if prop_type == "Blocked Shots" and league != "NHL":
            return blk
        if prop_type == "Blks+Stls":
            return None if any(v is None for v in (blk, stl)) else blk + stl
        if prop_type == "Turnovers":
            return to

        # ── MLB ─────────────────────────────────────────────────
        h   = _num("h", "hits")
        k   = _num("k", "strikeouts", "so")
        r   = _num("r", "runs")
        rbi = _num("rbi", "rbis")
        bb  = _num("bb", "walks")
        hr  = _num("hr", "homeruns")
        sb  = _num("sb", "stolenbases")
        d2  = _num("2b", "doubles")
        d3  = _num("3b", "triples")

        if prop_type == "Pitcher Strikeouts":
            return k
        if prop_type in ("Hits Allowed", "Hits"):
            return h
        if prop_type == "Home Runs":
            return hr
        if prop_type == "RBIs":
            return rbi
        if prop_type == "Runs":
            return r
        if prop_type == "Stolen Bases":
            return sb
        if prop_type == "Total Bases":
            if h is None or hr is None: return None
            singles = h - (d2 or 0) - (d3 or 0) - hr
            return singles + 2*(d2 or 0) + 3*(d3 or 0) + 4*hr
        if prop_type == "Hits+Runs+RBIs":
            return None if any(v is None for v in (h, r, rbi)) else h + r + rbi
        if prop_type == "Runs+RBIs":
            return None if any(v is None for v in (r, rbi)) else r + rbi
        if prop_type == "Singles":
            return None if any(v is None for v in (h, d2, d3, hr)) else h - (d2 or 0) - (d3 or 0) - hr
        if prop_type == "Doubles":
            return d2
        if prop_type == "Triples":
            return d3
        if prop_type in ("Walks", "Walks Allowed"):
            return bb
        if prop_type == "Earned Runs Allowed":
            return _num("er", "earnedruns")
        if prop_type == "Pitching Outs":
            ip = stats.get("ip") or stats.get("fullinnings.partinnings")
            if ip is None: return None
            try:
                whole, frac = str(ip).split(".") if "." in str(ip) else (str(ip), "0")
                return float(whole) * 3 + float(frac)
            except Exception: return None

        # ── NHL ─────────────────────────────────────────────────
        gl = _num("goals", "g")
        asst = _num("assists", "a")
        if prop_type == "Goals":
            return gl
        if prop_type == "Assists" and league == "NHL":
            return asst
        if prop_type == "Points" and league == "NHL":
            return None if any(v is None for v in (gl, asst)) else gl + asst
        if prop_type.lower() == "shots on goal":
            return _num("shotstotal", "sog", "shots", "s")
        if prop_type in ("Goalie Saves", "Saves"):
            return _num("saves", "sv")
        if prop_type == "Blocked Shots":
            return _num("blockedshots", "blk")

        return None

    def _fetch_gamelog_stats(self, league: str, player_name: str, target_date: datetime) -> Optional[dict]:
        """Search ESPN and fetch player gamelog to ensure accurate verification when boxscore misses."""
        cache_key = (league.lower(), player_name.lower())
        if cache_key in self._gamelog_cache:
            return self._gamelog_cache[cache_key]
            
        search_url = "https://site.api.espn.com/apis/search/v2"
        try:
            r = self._session.get(search_url, params={"query": player_name, "limit": 3}, timeout=15)
            r.raise_for_status()
            data = r.json()
        except Exception as exc:
            logger.debug("ResultsChecker: search API failed for %s: %s", player_name, exc)
            self._gamelog_cache[cache_key] = None
            return None
            
        uid = None
        for res in data.get("results", []):
            if res.get("type") == "player":
                for c in res.get("contents", []):
                    uid = c.get("uid")
                    break
            if uid:
                break
                
        if not uid or "a:" not in uid:
            self._gamelog_cache[cache_key] = None
            return None
            
        athlete_id = uid.split("a:")[-1]
        
        league_path = {
            "NBA": "basketball/nba",
            "NCAAB": "basketball/mens-college-basketball",
            "MLB": "baseball/mlb",
            "NHL": "hockey/nhl"
        }.get(league.upper())
        
        if not league_path:
            self._gamelog_cache[cache_key] = None
            return None
            
        gl_url = f"https://site.web.api.espn.com/apis/common/v3/sports/{league_path}/athletes/{athlete_id}/gamelog"
        try:
            r2 = self._session.get(gl_url, timeout=15)
            r2.raise_for_status()
            gl = r2.json()
        except Exception as exc:
            logger.debug("ResultsChecker: gamelog fetch failed for %s: %s", player_name, exc)
            self._gamelog_cache[cache_key] = None
            return None
            
        global_labels = gl.get("labels", [])
        events_meta = gl.get("events", {})
        all_game_stats = {}
        
        for st in gl.get("seasonTypes", []):
            for cat in st.get("categories", []):
                labels = cat.get("labels") or global_labels
                labels_lower = [str(L).lower() for L in labels]
                for ev in cat.get("events", []):
                    event_id = ev.get("eventId")
                    if not event_id:
                        continue
                    stats_arr = ev.get("stats", [])
                    stat_dict = dict(zip(labels_lower, stats_arr))
                    
                    if "k" in stat_dict and "so" not in stat_dict:
                        stat_dict["so"] = stat_dict["k"]
                    if "s" in stat_dict and "sog" not in stat_dict:
                        stat_dict["sog"] = stat_dict["s"]
                    if "sv" in stat_dict and "saves" not in stat_dict:
                        stat_dict["saves"] = stat_dict["sv"]
                        
                    if event_id not in all_game_stats:
                        all_game_stats[event_id] = {}
                    all_game_stats[event_id].update(stat_dict)
                    
        best_stats = None
        best_diff = timedelta(days=999)
        for eid, s_dict in all_game_stats.items():
            meta = events_meta.get(eid, {})
            gd_str = meta.get("gameDate")
            if not gd_str: continue
            try:
                ev_dt = datetime.fromisoformat(gd_str.replace("Z", "+00:00"))
                if ev_dt.tzinfo is None:
                    ev_dt = ev_dt.replace(tzinfo=timezone.utc)
                diff = abs(ev_dt - target_date)
                if diff <= timedelta(hours=36):
                    if diff < best_diff:
                        best_diff = diff
                        best_stats = s_dict
            except Exception:
                pass
                
        self._gamelog_cache[cache_key] = best_stats
        return best_stats

    @staticmethod
    def _write_csv(csv_path: pathlib.Path, rows: list[dict]) -> None:
        """Atomically rewrite the CSV (write to .tmp then rename)."""
        if not rows:
            return
        fieldnames = list(rows[0].keys())
        tmp = csv_path.with_suffix(".tmp")
        try:
            with open(tmp, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=fieldnames)
                w.writeheader()
                w.writerows(rows)
            tmp.replace(csv_path)
        except Exception as exc:
            logger.error("ResultsChecker: CSV write failed: %s", exc)
            if tmp.exists():
                tmp.unlink()
