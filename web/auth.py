import os
import time
import logging
import jwt
from jwt import PyJWKClient
from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_JWT_SECRET = os.getenv("SUPABASE_JWT_SECRET")

_JWKS_URL = f"{SUPABASE_URL}/auth/v1/.well-known/jwks.json" if SUPABASE_URL else None
_jwk_client: Optional[PyJWKClient] = None
if _JWKS_URL:
    try:
        _jwk_client = PyJWKClient(_JWKS_URL, cache_keys=True, lifespan=3600)
    except Exception as exc:  # pragma: no cover
        logger.warning("Failed to initialize PyJWKClient for %s: %s", _JWKS_URL, exc)
        _jwk_client = None
else:
    logger.warning(
        "SUPABASE_URL missing at auth import — JWKS verification disabled. "
        "Only HS256 tokens with SUPABASE_JWT_SECRET will verify."
    )

logger.info(
    "Auth init: jwks=%s hs256_secret=%s issuer=%s",
    "ok" if _jwk_client is not None else "none",
    "set" if SUPABASE_JWT_SECRET else "unset",
    f"{SUPABASE_URL}/auth/v1" if SUPABASE_URL else "none",
)

# Using FastAPI's standard Bearer token schema
token_auth_scheme = HTTPBearer(auto_error=False)

def _decode(token: str) -> dict:
    """Verify a Supabase access token. Tries JWKS (ES256/RS256) first, then
    falls back to HS256 with SUPABASE_JWT_SECRET.
    Raises specific jwt exceptions if invalid.
    """
    try:
        header = jwt.get_unverified_header(token)
    except Exception as exc:
        raise jwt.InvalidTokenError(f"Invalid token header: {exc}")

    alg = header.get("alg", "")
    
    # Expected standard claims for Supabase
    expected_issuer = f"{SUPABASE_URL}/auth/v1" if SUPABASE_URL else None
    decode_kwargs = {
        "audience": "authenticated",
    }
    if expected_issuer:
        decode_kwargs["issuer"] = expected_issuer

    # Asymmetric: verify against Supabase JWKS.
    if alg in ("ES256", "RS256", "ES384", "RS384"):
        if _jwk_client is None:
            raise jwt.InvalidTokenError(
                f"Token alg={alg} requires JWKS but SUPABASE_URL is not configured on this server."
            )
        try:
            signing_key = _jwk_client.get_signing_key_from_jwt(token).key
            return jwt.decode(
                token,
                signing_key,
                algorithms=[alg],
                **decode_kwargs
            )
        except Exception as exc:
            # Re-raise explicit token errors so the handler catches them
            if isinstance(exc, (jwt.ExpiredSignatureError, jwt.InvalidTokenError, jwt.InvalidIssuerError, jwt.InvalidAudienceError)):
                raise exc
            # Non-JWT failure (likely PyJWKClient network/DNS error). Log loud
            # so prod operators can see why auth broke.
            logger.error("JWKS verify failed (alg=%s): %s", alg, exc)
            raise jwt.InvalidTokenError(f"JWKS verification failed: {exc}")

    # Symmetric legacy path or fallback.
    if alg == "HS256":
        if not SUPABASE_JWT_SECRET:
            raise jwt.InvalidTokenError(
                "Token alg=HS256 but SUPABASE_JWT_SECRET is not set on this server."
            )
        return jwt.decode(
            token,
            SUPABASE_JWT_SECRET,
            algorithms=["HS256"],
            **decode_kwargs
        )

    raise jwt.InvalidTokenError(f"Unsupported token algorithm: {alg or '(missing)'}")


async def get_current_user_optional(
    auth_creds: Optional[HTTPAuthorizationCredentials] = Depends(token_auth_scheme)
) -> Optional[dict]:
    """Extract and verify Supabase JWT if present. Returns None if missing or invalid."""
    if not auth_creds or not auth_creds.credentials:
        return None
        
    token = auth_creds.credentials

    try:
        payload = _decode(token)
    except (jwt.ExpiredSignatureError, jwt.InvalidTokenError):
        return None
    except Exception as exc:
        logger.error("Unexpected error decoding optional token: %s", exc)
        return None
        
    if payload.get("role") != "authenticated":
        return None

    return {"id": payload["sub"], "email": payload.get("email"), "jwt": token}


async def get_current_user(
    auth_creds: Optional[HTTPAuthorizationCredentials] = Depends(token_auth_scheme)
) -> dict:
    """Extract and verify Supabase JWT. Raises 401 if missing, expired, or invalid."""
    if not auth_creds or not auth_creds.credentials:
         raise HTTPException(
             status_code=401, 
             detail="Not authenticated", 
             headers={"WWW-Authenticate": "Bearer"}
         )
         
    token = auth_creds.credentials

    try:
        payload = _decode(token)
    except jwt.ExpiredSignatureError:
        raise HTTPException(
             status_code=401, 
             detail="Token has expired", 
             headers={"WWW-Authenticate": "Bearer"}
        )
    except jwt.InvalidTokenError as exc:
        raise HTTPException(
             status_code=401, 
             detail=f"Invalid token: {exc}", 
             headers={"WWW-Authenticate": "Bearer"}
        )
    except Exception as exc:
        logger.error("Unexpected error decoding token: %s", exc)
        raise HTTPException(
             status_code=500, 
             detail="Internal server error parsing token"
        )
        
    if payload.get("role") != "authenticated":
        raise HTTPException(
             status_code=403, 
             detail="Insufficient permissions",
        )

    return {"id": payload["sub"], "email": payload.get("email"), "jwt": token}
