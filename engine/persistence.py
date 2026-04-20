import logging
from datetime import datetime
from engine.database import get_db

logger = logging.getLogger(__name__)

def sync_state_to_supabase(key: str, data: any):
    """
    Overwrites the cached state for a given key in Supabase.
    'data' can be a list, dict, or primitive (will be stored as JSONB).
    """
    db = get_db()
    if not db:
        return False

    try:
        # We use an 'upsert' logic: delete then insert, or use PostgREST upsert if supported.
        # SyncPostgrestClient supports .upsert()
        payload = {
            "key": key,
            "value": data,
            "updated_at": datetime.now().isoformat()
        }
        
        # upsert() in postgrest-py handles the primary key conflict automatically
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
            return row.get("value"), row.get("updated_at")
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
                result_map[row["key"]] = (row.get("value"), row.get("updated_at"))
        return result_map
    except Exception as e:
        logger.error(f"Failed to load multiple states from Supabase: {e}")
    
    return {}
