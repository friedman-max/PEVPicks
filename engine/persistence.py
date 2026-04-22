import base64
import gzip
import json
import logging
from datetime import datetime
from engine.database import get_db

logger = logging.getLogger(__name__)

# Entries larger than this (as serialized JSON bytes) auto-compress on write.
# PostgREST / edge gateways on free-tier Supabase will reject payloads above
# ~4MB, and the books datasets (fd_lines/dk_lines/pin_lines) are 2–5MB raw.
# Gzip gets them to ~10–15% of raw; well under the cap.
_COMPRESS_THRESHOLD_BYTES = 256 * 1024  # 256KB
_GZ_MARKER = "__gz__"


def _maybe_compress(data):
    """If `data` serializes to more than the threshold, wrap it in a
    {'__gz__': base64(gzip(json))} envelope. load_state_from_supabase
    transparently decompresses."""
    try:
        raw = json.dumps(data, separators=(",", ":"), default=str).encode("utf-8")
    except Exception as exc:
        logger.warning("Compress: could not serialize for size check: %s", exc)
        return data
    if len(raw) <= _COMPRESS_THRESHOLD_BYTES:
        return data
    compressed = gzip.compress(raw, compresslevel=6)
    return {_GZ_MARKER: base64.b64encode(compressed).decode("ascii")}


def _maybe_decompress(value):
    """Inverse of _maybe_compress. If `value` is a compressed envelope,
    inflate it back to the original object; otherwise return as-is."""
    if isinstance(value, dict) and _GZ_MARKER in value and len(value) == 1:
        try:
            compressed = base64.b64decode(value[_GZ_MARKER])
            raw = gzip.decompress(compressed)
            return json.loads(raw)
        except Exception as exc:
            logger.warning("Decompress: envelope present but inflate failed: %s", exc)
            return None
    return value


def sync_state_to_supabase(key: str, data: any):
    """
    Overwrites the cached state for a given key in Supabase.
    'data' can be a list, dict, or primitive (will be stored as JSONB).
    Large payloads are transparently gzip+base64 compressed.
    """
    db = get_db()
    if not db:
        return False

    try:
        payload = {
            "key": key,
            "value": _maybe_compress(data),
            "updated_at": datetime.now().isoformat()
        }
        db.table("app_state_cache").upsert(payload, on_conflict="key").execute()
        return True
    except Exception as e:
        logger.error(f"Failed to sync state '{key}' to Supabase: {e}")
        return False

def load_state_from_supabase(key: str):
    """
    Fetches the cached state for a given key from Supabase.
    Returns (data, updated_at_str) or (None, None).
    """
    db = get_db()
    if not db:
        return None, None

    try:
        res = db.table("app_state_cache").select("value, updated_at").eq("key", key).execute()
        if res.data and len(res.data) > 0:
            row = res.data[0]
            return _maybe_decompress(row.get("value")), row.get("updated_at")
    except Exception as e:
        logger.error(f"Failed to load state '{key}' from Supabase: {e}")

    return None, None

def load_multiple_states_from_supabase(keys: list[str]):
    """
    Fetches the cached state for multiple keys from Supabase via a single request.
    Returns a dict mapping key -> (value, updated_at).
    """
    db = get_db()
    if not db:
        return {}

    try:
        # We need to query where key IN (keys)
        res = db.table("app_state_cache").select("key, value, updated_at").in_("key", keys).execute()
        result_map = {}
        if res.data:
            for row in res.data:
                result_map[row["key"]] = (_maybe_decompress(row.get("value")), row.get("updated_at"))
        return result_map
    except Exception as e:
        logger.error(f"Failed to load multiple states from Supabase: {e}")
    
    return {}
