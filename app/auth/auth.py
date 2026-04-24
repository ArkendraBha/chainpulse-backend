import datetime
import hashlib
import logging
import secrets
from typing import Optional

from fastapi import HTTPException, Request
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.models import User, UserProfile

logger = logging.getLogger("chainpulse")


# ──────────────────────────────────────────────
# Token utilities
# ──────────────────────────────────────────────

def generate_access_token() -> str:
    """Cryptographically secure token - 256 bits of entropy."""
    return secrets.token_urlsafe(32)


def hash_token(token: str) -> str:
    """One-way hash for DB storage. Raw token goes to user via email, hash goes to DB."""
    return hashlib.sha256(token.encode()).hexdigest()


# ──────────────────────────────────────────────
# Tier resolution
# ──────────────────────────────────────────────

def resolve_user_tier(authorization: Optional[str], db: Session) -> dict:
    """
    Resolves the user's tier from the Authorization header.

    Returns dict with keys:
        is_pro   (bool)  — True if user has any paid tier
        tier     (str)   — "free" | "expired" | "essential" | "pro" | "institutional"
        user     (User)  — SQLAlchemy User object or None
        email    (str)   — email address (present if resolved via JWT)
        expired  (bool)  — True if token is past TOKEN_EXPIRY_DAYS
    """

    # No auth header at all
    if not authorization or not authorization.startswith("Bearer "):
        return {
            "is_pro": False,
            "tier": "free",
            "user": None,
            "expired": False,                       # BUG #3 FIX
        }

    token = authorization.replace("Bearer ", "").strip()

    # Token too short / empty
    if not token or len(token) < 20:
        return {
            "is_pro": False,
            "tier": "free",
            "user": None,
            "expired": False,                       # BUG #3 FIX
        }

    # ── Try JWT first (stateless, no DB hit) ──────────────
    # BUG #5 PREP: wire JWT in so it works if you enable it later
    try:
        from app.auth.jwt_auth import verify_token_without_db

        jwt_info = verify_token_without_db(token)
        if jwt_info:
            return {
                "is_pro": jwt_info.get("is_pro", False),
                "tier": jwt_info.get("tier", "free"),
                "user": None,
                "email": jwt_info.get("email"),
                "expired": False,
            }
    except Exception:
        pass

    # ── Fallback: hashed access token (DB lookup) ─────────
    # BUG #1 FIX: hash the incoming token before comparing to DB
    token_hash = hash_token(token)
    user = db.query(User).filter(User.access_token == token_hash).first()

    # Token not found in DB
    if not user:
        return {
            "is_pro": False,
            "tier": "free",
            "user": None,
            "expired": False,                       # BUG #3 FIX
        }

    # ── Check token expiry ────────────────────────────────
    # BUG #2 FIX: return explicit "expired" instead of silent downgrade to "free"
    if user.token_created_at:
        age = (datetime.datetime.utcnow() - user.token_created_at).days
        if age > settings.TOKEN_EXPIRY_DAYS:
            return {
                "is_pro": False,
                "tier": "expired",
                "user": user,
                "expired": True,                    # BUG #2 FIX
            }

    # ── Check subscription status ─────────────────────────
    if user.subscription_status not in ("active", "trialing"):
        return {
            "is_pro": False,
            "tier": "free",
            "user": user,
            "expired": False,                       # BUG #3 FIX
        }

    # ── Success: active paying user ───────────────────────
    tier = user.tier or "free"
    return {
        "is_pro": tier in ("essential", "pro", "institutional"),
        "tier": tier,
        "user": user,
        "expired": False,                           # BUG #3 FIX
    }


def resolve_pro_status(authorization: Optional[str], db: Session) -> bool:
    """Legacy helper — returns True if user has any paid tier."""
    info = resolve_user_tier(authorization, db)
    return info["is_pro"]


# ──────────────────────────────────────────────
# Tier enforcement
# ──────────────────────────────────────────────

def require_tier(
    authorization: str,
    db: Session,
    minimum_tier: str = "essential",
) -> dict:
    """
    Checks user has at least the specified tier.
    Raises 401 for expired tokens, 403 for insufficient tier.
    """
    user_info = resolve_user_tier(authorization, db)

    # BUG #4 FIX: explicit error for expired tokens instead of confusing 403
    if user_info.get("expired"):
        raise HTTPException(
            status_code=401,
            detail="Your session has expired. Please request a new login link.",
        )

    user_level = settings.TIER_LEVELS.get(user_info["tier"], 0)
    required_level = settings.TIER_LEVELS.get(minimum_tier, 0)

    if user_level < required_level:
        raise HTTPException(
            status_code=403,
            detail=(
                f"This feature requires {minimum_tier} tier or higher. "
                f"Your tier: {user_info['tier']}"
            ),
        )

    return user_info


# ──────────────────────────────────────────────
# Email ownership enforcement
# ──────────────────────────────────────────────

def require_email_ownership(user_info: dict, requested_email: str) -> str:
    """
    Enforces that authenticated user can only access their own data.
    Returns the normalized email on success.

    Supports both:
      - DB-based auth (user_info has "user" object with .email)
      - JWT-based auth (user_info has "email" string directly)
    """
    # BUG #5 FIX: support both JWT (email key) and DB-based auth (user object)
    authenticated_email = None
    if user_info.get("user"):
        authenticated_email = user_info["user"].email
    elif user_info.get("email"):
        authenticated_email = user_info["email"]

    if not authenticated_email:
        raise HTTPException(
            status_code=401,
            detail="Authentication required",
        )

    requested_email = requested_email.strip().lower()
    if requested_email and requested_email != authenticated_email:
        raise HTTPException(
            status_code=403,
            detail="You can only access your own data.",
        )

    return authenticated_email


# ──────────────────────────────────────────────
# Activity tracking
# ──────────────────────────────────────────────

def update_last_active(request: Request, db: Session):
    """
    Updates user.last_active_at timestamp.
    Called from pro/premium endpoints to track engagement.
    """
    from app.core.security import get_auth_header

    token = get_auth_header(request)
    if not token:
        return

    # Strip "Bearer " prefix
    token_val = (
        token.replace("Bearer ", "").strip()
        if token.startswith("Bearer ")
        else token
    )

    if not token_val or len(token_val) < 20:
        return

    # BUG #1 FIX: hash the token before DB lookup
    # DB stores hashed tokens, so we must hash the incoming raw token to match
    token_hash = hash_token(token_val)
    user = db.query(User).filter(User.access_token == token_hash).first()

    if user:
        user.last_active_at = datetime.datetime.utcnow()
        try:
            db.commit()
        except Exception:
            db.rollback()