import os
import logging
from postgrest import SyncPostgrestClient
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Load .env from project root
load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY", SUPABASE_KEY) # Fallback to service key if anon key missing, but RLS works with JWT.

db = None

if SUPABASE_URL and SUPABASE_KEY:
    try:
        # Construct the REST URL (usually SU_URL + /rest/v1)
        rest_url = f"{SUPABASE_URL.rstrip('/')}/rest/v1"
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal,resolution=merge-duplicates"
        }
        db = SyncPostgrestClient(rest_url, headers=headers)
        logger.info("Supabase PostgREST client initialized successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize PostgREST client: {e}")
else:
    logger.warning("SUPABASE_URL or SUPABASE_SERVICE_KEY missing from environment.")

def get_db() -> SyncPostgrestClient:
    """Service-role client (bypasses RLS)"""
    return db

def get_user_db(jwt: str) -> SyncPostgrestClient:
    """Supabase client scoped to a user's JWT — RLS applies."""
    if not SUPABASE_URL:
        return None
    rest_url = f"{SUPABASE_URL.rstrip('/')}/rest/v1"
    headers = {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {jwt}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal,resolution=merge-duplicates"
    }
    return SyncPostgrestClient(rest_url, headers=headers)
