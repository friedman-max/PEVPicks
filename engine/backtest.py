"""
Backtest and slip logger for CoreProp.

Automatically documents the best +EV slip combinations as they appear
throughout the day. Logs to data/backtest.csv with one row per leg.
Tracks which players have been used to avoid repeats in the same betting day.
Resets daily at midnight.
"""
import csv
import logging
import pathlib
import uuid
from datetime import date, datetime, timezone, timedelta
from typing import Optional
import unidecode

from engine.constants import BREAK_EVEN
from engine.ev_calculator import power_slip_ev, flex_slip_ev
from engine.database import get_db

logger = logging.getLogger(__name__)

DATA_DIR      = pathlib.Path(__file__).parent.parent / "data"
CSV_PATH      = DATA_DIR / "backtest.csv"

CSV_COLUMNS = [
    "slip_id", "timestamp", "slip_type", "n_legs", "proj_slip_ev_pct",
    "leg_num", "player", "league", "prop", "line", "side",
    "true_prob", "ind_ev_pct", "game_start",
    "closing_prob", "clv_pct", "result", "stat_actual",
]

# Hard floor: legs with individual EV below this are never included
MIN_LEG_EV_PCT = -0.01   # -1%

def _normalize(s: str) -> str:
    """Standardize strings: unidecode, lowercase, and strip whitespace."""
    if not s:
        return ""
    s = unidecode.unidecode(s).lower().strip()
    return s

def make_bet_key(player: str, start_time: str) -> tuple[str, str]:
    """Build a unique signature for a player in a specific game (UTC-normalized)."""
    time_key = "no_time"
    if start_time:
        try:
            # Handle standard ISO formats (with or without offset)
            # fromisoformat handles '2023-10-27T19:45:00-04:00' and '2023-10-27T23:45:00+00:00'
            # We replace ' ' with 'T' for consistent ISO parsing
            clean_ts = start_time.replace(" ", "T")
            
            # Python 3.11+ handles the 'Z' suffix, but for older versions we replace it
            if clean_ts.endswith("Z"):
                clean_ts = clean_ts[:-1] + "+00:00"
                
            dt = datetime.fromisoformat(clean_ts)
            
            # If naive, assume it's already UTC (common for scrapers) OR Eastern if we wanted to be specific,
            # but usually scrapers without offsets are UTC or intended to be.
            # Convert to UTC
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            
            # Format back to a string key: YYYY-MM-DDTHH:MM
            time_key = dt.strftime("%Y-%m-%dT%H:%M")
        except Exception as e:
            # Fallback for malformed strings: take first 10-16 chars if parsing fails
            logger.warning("Backtest: _make_key failed to parse '%s': %s", start_time, e)
            time_key = start_time[:16] if start_time else "no_time"

    return (_normalize(player), time_key)


class BacktestLogger:
    """
    Builds and logs the best available +EV slips to a CSV file.

    Selection logic:
      - Filter out already-used PLAYERS (Hard Deduplication)
      - Hard floor: individual_ev_pct >= -1%
      - Sort greedily by individual EV descending
      - Pick top 6 legs (must be 6 unique players)
      - Log to CSV; lock players for the day
    """

    def __init__(self, csv_path: Optional[pathlib.Path] = None):
        self.used_bets: set[tuple[str, str]] = set()
        self.last_reset_date: date = date.today()
        self._csv_path = csv_path or CSV_PATH
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self._init_csv()
        self.sync_from_supabase()
        self._rebuild_used_bets()

    def _init_csv(self) -> None:
        if not self._csv_path.exists():
            with open(self._csv_path, "w", newline="", encoding="utf-8-sig") as f:
                csv.DictWriter(f, fieldnames=CSV_COLUMNS).writeheader()
            logger.info("Created backtest CSV at %s", self._csv_path)
        else:
            self.repair_csv()

    def repair_csv(self) -> None:
        """
        Ensures the CSV header matches CSV_COLUMNS and fixes data misalignment.
        """
        if not self._csv_path.exists():
            return

        try:
            with open(self._csv_path, "r", encoding="utf-8-sig") as f:
                reader = csv.reader(f)
                header = next(reader, None)
                if not header:
                    return
                rows = list(reader)

            if header == CSV_COLUMNS:
                return

            logger.info("Repairing backtest CSV (handling 'urgency' removal)...")
            
            new_rows = []
            if "urgency" in header:
                # User wants to remove urgency. Identify its index.
                u_idx = header.index("urgency")
                logger.info("Stripping 'urgency' column at index %d", u_idx)
                for r in rows:
                    if len(r) > u_idx:
                        new_r = r[:u_idx] + r[u_idx+1:]
                        # Map to CSV_COLUMNS in case of other mismatches
                        row_dict = dict(zip([h for h in header if h != "urgency"], new_r))
                        final_r = [row_dict.get(col, "") for col in CSV_COLUMNS]
                        new_rows.append(final_r)
                    else:
                        new_rows.append(r)
            else:
                # Standard repair: pad or re-map
                for r in rows:
                    row_dict = dict(zip(header, r))
                    new_r = [row_dict.get(col, "") for col in CSV_COLUMNS]
                    new_rows.append(new_r)

            # Rewrite correctly
            tmp = self._csv_path.with_suffix(".repair_tmp")
            with open(tmp, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.writer(f)
                writer.writerow(CSV_COLUMNS)
                writer.writerows(new_rows)
            tmp.replace(self._csv_path)
            logger.info("Backtest CSV repair complete.")

        except Exception as e:
            logger.error("Failed to repair backtest CSV: %s", e)

    def sync_from_supabase(self) -> None:
        """
        Pull the latest 1000 legs from Supabase and rebuild backtest.csv.
        Crucial for ephemeral hosting (Render) where local files are wiped.
        """
        db = get_db()
        if not db:
            return

        try:
            logger.info("Backtest: Syncing from Supabase to rebuild local CSV...")
            # 1. Fetch latest 1000 legs (expanded window for better deduplication)
            # Use 'id' if serial, otherwise order by created_at or just get latest
            res = db.table("legs").select("*").order("slip_id", desc=True).limit(1000).execute()
            legs = res.data
            if not legs:
                return

            # 2. Fetch corresponding slips to get timestamps/types/ev
            sids = list(set(l["slip_id"] for l in legs))
            slips_res = db.table("slips").select("*").in_("id", sids).execute()
            slips_map = {s["id"]: s for s in slips_res.data}

            # 3. Map to CSV row format
            rows = []
            for l in legs:
                s = slips_map.get(l["slip_id"], {})
                rows.append({
                    "slip_id":          l["slip_id"],
                    "timestamp":        s.get("timestamp", ""),
                    "slip_type":        s.get("slip_type", ""),
                    "n_legs":           s.get("n_legs", ""),
                    "proj_slip_ev_pct": s.get("proj_slip_ev_pct", ""),
                    "leg_num":          l.get("leg_num", ""),
                    "player":           l.get("player", ""),
                    "league":           l.get("league", ""),
                    "prop":             l.get("prop", ""),
                    "line":             l.get("line", ""),
                    "side":             l.get("side", ""),
                    "true_prob":        l.get("true_prob", ""),
                    "ind_ev_pct":       l.get("ind_ev_pct", ""),
                    "game_start":       l.get("game_start", ""),
                    "closing_prob":     l.get("closing_prob", ""),
                    "clv_pct":          l.get("clv_pct", ""),
                    "result":           l.get("result", "pending"),
                    "stat_actual":      l.get("stat_actual", ""),
                })

            # Sort by timestamp (asc) for CSV order
            rows = sorted(rows, key=lambda x: str(x["timestamp"] or ""), reverse=False)

            # 4. Write to CSV (overwrite)
            with open(self._csv_path, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
                writer.writeheader()
                writer.writerows(rows)
            
            logger.info("Backtest: Rebuilt CSV with %d rows from Supabase", len(rows))

        except Exception as e:
            logger.error("Backtest: Supabase sync failed: %s", e)

    def _rebuild_used_bets(self) -> None:
        """Read recent rows (approx last 24h) and repopulate used_players."""
        today = date.today()
        yesterday = today - timedelta(days=1)
        target_dates = [today.isoformat(), yesterday.isoformat()]
        
        if not self._csv_path.exists():
            return
        try:
            with open(self._csv_path, newline="", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    ts = row.get("timestamp", "")
                    is_recent = False
                    for d in target_dates:
                        if ts.startswith(d):
                            is_recent = True
                            break
                    if is_recent:
                        self.used_bets.add(make_bet_key(
                            row.get("player", ""),
                            row.get("game_start", "")
                        ))
            if self.used_bets:
                logger.debug("Backtest: rebuilt %d used-player-game keys", len(self.used_bets))
        except Exception as exc:
            logger.warning("Backtest: could not rebuild used_players from CSV: %s", exc)

    def _midnight_reset(self) -> None:
        today = date.today()
        if self.last_reset_date != today:
            if self.last_reset_date is not None:
                logger.info("Midnight reset: clearing %d used-bet keys", len(self.used_bets))
            self.used_bets = set()
            self.last_reset_date = today

    def reset_daily(self) -> None:
        logger.info("Daily reset: clearing %d used-bet keys", len(self.used_bets))
        self.used_bets = set()
        self.last_reset_date = date.today()

    def used_bet_keys(self) -> set[tuple[str, str]]:
        return set(self.used_bets)

    def remove_slip(self, slip_id: str) -> int:
        """
        Remove all rows for a given slip_id from the CSV and un-mark
        the corresponding player-game keys from used_bets.

        Returns the number of rows removed.
        """
        if not self._csv_path.exists():
            return 0

        try:
            with open(self._csv_path, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames
                all_rows = list(reader)
        except Exception as exc:
            logger.error("Backtest remove_slip: cannot read CSV: %s", exc)
            return 0

        keep_rows = []
        removed_rows = []
        for row in all_rows:
            if row.get("slip_id") == slip_id:
                removed_rows.append(row)
            else:
                keep_rows.append(row)

        if not removed_rows:
            logger.warning("Backtest remove_slip: slip_id %s not found", slip_id)
            return 0

        # Un-mark players from used_bets so they become available again
        for row in removed_rows:
            key = make_bet_key(row.get("player", ""), row.get("game_start", ""))
            self.used_bets.discard(key)

        # Atomically rewrite CSV
        tmp = self._csv_path.with_suffix(".tmp")
        try:
            with open(tmp, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames or CSV_COLUMNS)
                writer.writeheader()
                writer.writerows(keep_rows)
            tmp.replace(self._csv_path)
        except Exception as exc:
            logger.error("Backtest remove_slip: CSV rewrite failed: %s", exc)
            if tmp.exists():
                tmp.unlink()
            return 0

        # Supabase Sync
        db = get_db()
        if db:
            try:
                db.table("slips").delete().eq("id", slip_id).execute()
                logger.info("Backtest: removed slip %s from Supabase", slip_id)
            except Exception as db_exc:
                logger.error("Backtest: Supabase delete failed for %s: %s", slip_id, db_exc)

        logger.info("Backtest: removed slip %s (%d rows)", slip_id, len(removed_rows))
        return len(removed_rows)

    def try_log_slip(self, bets: list[dict]) -> Optional[dict]:
        """Build and log the best available 6-leg power slip with unique players."""
        self._midnight_reset()

        def _ev(b: dict) -> float:
            return float(b.get("individual_ev_pct") or 0.0)

        valid = [b for b in bets if _ev(b) >= MIN_LEG_EV_PCT]
        pool = sorted(valid, key=_ev, reverse=True)

        best_legs = []
        seen_in_this_slip = set()
        
        for bet in pool:
            p_key = make_bet_key(
                bet.get("player_name", ""),
                bet.get("start_time", "")
            )
            if p_key in self.used_bets or p_key in seen_in_this_slip:
                continue
            best_legs.append(bet)
            seen_in_this_slip.add(p_key)
            if len(best_legs) == 6:
                break

        if len(best_legs) < 6:
            return None

        true_probs = [float(b.get("true_prob") or 0.0) for b in best_legs]
        avg_prob = sum(true_probs) / 6
        power_be = BREAK_EVEN.get(("6", "power"))

        if power_be is None or avg_prob < power_be:
            return None

        best_ev = power_slip_ev(true_probs)
        if best_ev is None or best_ev <= 0:
            return None

        slip_id = str(uuid.uuid4())[:8].upper()
        timestamp = datetime.now().isoformat(timespec="seconds")
        proj_ev = round(best_ev, 4)

        rows = []
        for i, bet in enumerate(best_legs, start=1):
            p_key = make_bet_key(
                bet.get("player_name", ""),
                bet.get("start_time", "")
            )
            self.used_bets.add(p_key)
            true_p = round(float(bet.get("true_prob") or 0), 4)
            rows.append({
                "slip_id":          slip_id,
                "timestamp":        timestamp,
                "slip_type":        "Power",
                "n_legs":           6,
                "proj_slip_ev_pct": proj_ev,
                "leg_num":          i,
                "player":           bet.get("player_name", ""),
                "league":           bet.get("league", ""),
                "prop":             bet.get("prop_type", ""),
                "line":             bet.get("pp_line", ""),
                "side":             bet.get("side", ""),
                "true_prob":        true_p,
                "ind_ev_pct":       round(_ev(bet), 4),
                "game_start":       bet.get("start_time", ""),
                "closing_prob":     true_p,
                "clv_pct":          0.0,
                "result":           "pending",
                "stat_actual":      "",
            })

        try:
            with open(self._csv_path, "a", newline="", encoding="utf-8-sig") as f:
                csv.DictWriter(f, fieldnames=CSV_COLUMNS).writerows(rows)
            logger.info("Backtest: logged slip %s to CSV (6-leg Power EV=%.2f%%)", slip_id, best_ev * 100)
        except Exception as exc:
            logger.error("Backtest: CSV write failed: %s", exc)
            return None

        # Supabase Dual-Write
        db = get_db()
        if db:
            try:
                # 1. Insert slip header
                db.table("slips").insert({
                    "id":               slip_id,
                    "timestamp":        timestamp,
                    "slip_type":        "Power",
                    "n_legs":           6,
                    "proj_slip_ev_pct": proj_ev
                }).execute()
                # 2. Insert legs
                db_legs = []
                for r in rows:
                    db_legs.append({
                        "slip_id":      slip_id,
                        "leg_num":      r["leg_num"],
                        "player":       r["player"],
                        "league":       r["league"],
                        "prop":         r["prop"],
                        "line":         r["line"],
                        "side":         r["side"],
                        "true_prob":    r["true_prob"],
                        "ind_ev_pct":   r["ind_ev_pct"],
                        "game_start":   r["game_start"] if r["game_start"] else None,
                        "closing_prob": r["closing_prob"],
                        "clv_pct":      r["clv_pct"],
                        "result":       r["result"],
                        "stat_actual":  None if r["stat_actual"] == "" else r["stat_actual"]
                    })
                db.table("legs").insert(db_legs).execute()
                logger.info("Backtest: slip %s sync'd to Supabase", slip_id)
            except Exception as db_exc:
                logger.error("Backtest: Supabase sync failed: %s", db_exc)

        return {
            "slip_id":          slip_id,
            "timestamp":        timestamp,
            "slip_type":        "Power",
            "n_legs":           6,
            "proj_slip_ev_pct": proj_ev,
            "legs": [
                {
                    "player":     r["player"],
                    "league":     r["league"],
                    "prop":       r["prop"],
                    "line":       r["line"],
                    "side":       r["side"],
                    "true_prob":  r["true_prob"],
                    "ind_ev_pct": r["ind_ev_pct"],
                    "game_start": r["game_start"],
                }
                for r in rows
            ],
        }
