import os
import datetime
import logging
from typing import Optional

logger = logging.getLogger("chainpulse")

JWT_SECRET = os.getenv("JWT_SECRET", "")
JWT_ALGORITHM = "HS256"
JWT_EXPIRY_DAYS = 90


def create_jwt_token(email: str, tier: str, user_id: int) -> str:
    """Creates a signed JWT token."""
    try:
        import jwt

        payload = {
            "sub": email,
            "tier": tier,
            "uid": user_id,
            "iat": datetime.datetime.utcnow(),
            "exp": datetime.datetime.utcnow()
            + datetime.timedelta(days=JWT_EXPIRY_DAYS),
        }
        return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
    except ImportError:
        logger.warning("PyJWT not installed - falling back to UUID tokens")
        import secrets

        return secrets.token_urlsafe(32)


def decode_jwt_token(token: str) -> Optional[dict]:
    """Decodes and validates a JWT token without DB lookup."""
    if not JWT_SECRET:
        return None
    try:
        import jwt

        payload = jwt.decode(
            token,
            JWT_SECRET,
            algorithms=[JWT_ALGORITHM],
        )
        return payload
    except Exception:
        return None


def verify_token_without_db(token: str) -> Optional[dict]:
    """
    Verifies token without hitting the database.
    Use this for high-frequency read endpoints.
    """
    if not token or len(token) < 20:
        return None
    payload = decode_jwt_token(token)
    if not payload:
        return None
    return {
        "email": payload.get("sub"),
        "tier": payload.get("tier", "free"),
        "user_id": payload.get("uid"),
        "is_pro": payload.get("tier") in ("essential", "pro", "institutional"),
    }
