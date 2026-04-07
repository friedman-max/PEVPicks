"""
ESPN unofficial API result checker for CoreProp backtests.

Reads pending rows from data/backtest.csv, fetches ESPN box scores,
and marks each bet as "hit" or "miss" with the actual stat value.
Covers NBA, NCAAB, MLB, NHL.
"""
import csv
import logging
import pathlib
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

FUZZY_THRESHOLD = 78   # lower than main app; ESPN display names can differ slightly


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
            if row.get("result") != "pending":
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
                logger.debug(
                    "ResultsChecker: cannot compute '%s' for %s", prop_type, player_name
                )
                continue

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

        name_lower  = player_name.lower()
        best_score  = 0
        best_stats  = None
        for known_name, stats in stats_by_player.items():
            score = fuzz.token_sort_ratio(name_lower, known_name)
            if score > best_score:
                best_score = score
                best_stats = stats

        return best_stats if best_score >= FUZZY_THRESHOLD else None

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
        ESPN returns stats as parallel arrays: names[] and athletes[].stats[].
        Note: NBA/MLB populate `names`; NHL only populates `keys`.
        We fall back to `keys` when `names` is empty.
        """
        result: dict = {}
        for section in summary.get("boxscore", {}).get("players", []):
            for stat_block in section.get("statistics", []):
                field_names = [n.lower() for n in stat_block.get("names", [])]
                if not field_names:
                    field_names = [k.lower() for k in stat_block.get("keys", [])]
                for entry in stat_block.get("athletes", []):
                    display = entry.get("athlete", {}).get("displayName", "").lower()
                    raw     = entry.get("stats", [])
                    if not display or not raw:
                        continue
                    stat_dict = {
                        field_names[i]: raw[i]
                        for i in range(min(len(field_names), len(raw)))
                    }
                    if display in result:
                        result[display].update(stat_dict)
                    else:
                        result[display] = stat_dict
        return result

    @staticmethod
    def _compute_stat(
        stats: dict, prop_type: str, league: str
    ) -> Optional[float]:
        """Convert raw ESPN stat dict to a float for the given prop type."""

        def _num(key) -> Optional[float]:
            val = stats.get(key)
            if val is None:
                return None
            try:
                # Handle "made-attempted" format like "8-18" or "2-6"
                return float(str(val).split("-")[0])
            except Exception:
                return None

        # ── Basketball ──────────────────────────────────────────
        if prop_type == "Points":
            return _num("pts")
        if prop_type == "Rebounds":
            return _num("reb")
        if prop_type in ("Assists",) and league != "NHL":
            return _num("ast")
        if prop_type == "3-PT Made":
            return _num("3pt")   # ESPN: "3pt": "2-6"
        if prop_type == "Pts+Rebs+Asts":
            p, r, a = _num("pts"), _num("reb"), _num("ast")
            return None if None in (p, r, a) else p + r + a
        if prop_type == "Pts+Rebs":
            p, r = _num("pts"), _num("reb")
            return None if None in (p, r) else p + r
        if prop_type == "Pts+Asts":
            p, a = _num("pts"), _num("ast")
            return None if None in (p, a) else p + a
        if prop_type == "Rebs+Asts":
            r, a = _num("reb"), _num("ast")
            return None if None in (r, a) else r + a
        if prop_type == "Steals":
            return _num("stl")
        if prop_type == "Blocked Shots" and league != "NHL":
            return _num("blk")
        if prop_type == "Blks+Stls":
            b, s = _num("blk"), _num("stl")
            return None if None in (b, s) else b + s
        if prop_type == "Turnovers":
            return _num("to")

        # ── MLB ─────────────────────────────────────────────────
        if prop_type == "Pitcher Strikeouts":
            # ESPN box uses "k" (from names[]) or "strikeouts" (from keys[])
            return _num("k") or _num("strikeouts") or _num("so")
        if prop_type == "Hits Allowed":
            # Pitcher stat block: "h" = hits allowed; "hits" from keys[]
            return _num("h") or _num("hits")
        if prop_type == "Hits":
            return _num("h") or _num("hits")
        if prop_type == "Home Runs":
            return _num("hr") or _num("homeruns")
        if prop_type == "RBIs":
            return _num("rbi") or _num("rbis")
        if prop_type == "Runs":
            return _num("r") or _num("runs")
        if prop_type == "Stolen Bases":
            return _num("sb") or _num("stolenbases")
        if prop_type == "Total Bases":
            h  = _num("h") or _num("hits")
            hr = _num("hr") or _num("homeruns")
            d2 = _num("2b") or _num("doubles")
            d3 = _num("3b") or _num("triples")
            if h is None or hr is None:
                return None
            singles = h - (d2 or 0) - (d3 or 0) - hr
            return singles + 2 * (d2 or 0) + 3 * (d3 or 0) + 4 * hr
        if prop_type == "Hits+Runs+RBIs":
            h   = _num("h") or _num("hits")
            r   = _num("r") or _num("runs")
            rbi = _num("rbi") or _num("rbis")
            return None if None in (h, r, rbi) else h + r + rbi
        if prop_type == "Runs+RBIs":
            r   = _num("r") or _num("runs")
            rbi = _num("rbi") or _num("rbis")
            return None if None in (r, rbi) else r + rbi
        if prop_type == "Singles":
            h, d2, d3, hr = _num("h"), _num("2b"), _num("3b"), _num("hr")
            return None if None in (h, d2, d3, hr) else h - (d2 or 0) - (d3 or 0) - hr
        if prop_type == "Doubles":
            return _num("2b")
        if prop_type == "Triples":
            return _num("3b")
        if prop_type == "Walks" or prop_type == "Walks Allowed":
            return _num("bb")
        if prop_type == "Earned Runs Allowed":
            return _num("er")
        if prop_type == "Hits Allowed":
            return _num("h")
        if prop_type == "Pitching Outs":
            # ESPN stores IP as "6.1" (names) or via keys: "fullinnings.partinnings"
            ip = stats.get("ip") or stats.get("fullinnings.partinnings")
            if ip is None:
                return None
            try:
                whole, frac = str(ip).split(".") if "." in str(ip) else (str(ip), "0")
                return float(whole) * 3 + float(frac)
            except Exception:
                return None

        # ── NHL ─────────────────────────────────────────────────
        # ESPN NHL keys: goals, assists, shotsTotal, blockedShots, saves, etc.
        # (all lowercased by _parse_box_score)
        if prop_type == "Goals":
            return _num("goals") or _num("g")
        if prop_type == "Assists" and league == "NHL":
            return _num("assists") or _num("a")
        if prop_type == "Points" and league == "NHL":
            g = _num("goals") or _num("g")
            a = _num("assists") or _num("a")
            return None if None in (g, a) else g + a
        if prop_type.lower() == "shots on goal":
            return _num("shotstotal") or _num("sog") or _num("shots") or _num("s")
        if prop_type in ("Goalie Saves", "Saves"):
            return _num("saves") or _num("sv")
        if prop_type == "Blocked Shots":
            return _num("blockedshots") or _num("blk")

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
