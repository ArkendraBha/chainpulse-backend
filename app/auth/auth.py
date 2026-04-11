import datetime
from typing import Optional
from fastapi import HTTPException, Request
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.models import User, UserProfile
import secrets
import hashlib

def generate_access_token() -> str:
    """Cryptographically secure token - 256 bits of entropy."""
    return secrets.token_urlsafe(32)

def hash_token(token: str) -> str:
    """Store hashed token in DB, compare hash on lookup."""
    return hashlib.sha256(token.encode()).hexdigest()



def resolve_user_tier(authorization: Optional[str], db: Session) -> dict:
    """Returns {'is_pro': bool, 'tier': str, 'user': User|None}"""
    if not authorization or not authorization.startswith("Bearer "):
        return {"is_pro": False, "tier": "free", "user": None}

    token = authorization.replace("Bearer ", "").strip()
    if not token or len(token) < 20:
        return {"is_pro": False, "tier": "free", "user": None}

    token_hash = hash_token(token)
    user = db.query(User).filter(User.access_token == token_hash).first()
    if not user:
        return {"is_pro": False, "tier": "free", "user": None}

    if user.token_created_at:
        age = (datetime.datetime.utcnow() - user.token_created_at).days
        if age > settings.TOKEN_EXPIRY_DAYS:
            return {"is_pro": False, "tier": "free", "user": user}

    if user.subscription_status not in ("active", "trialing"):
        return {"is_pro": False, "tier": "free", "user": user}

    tier = user.tier or "free"
    return {
        "is_pro": tier in ("essential", "pro", "institutional"),
        "tier": tier,
        "user": user,
    }


def resolve_pro_status(authorization: Optional[str], db: Session) -> bool:
    """Legacy helper - returns True if user has any paid tier."""
    info = resolve_user_tier(authorization, db)
    return info["is_pro"]


def require_tier(
    authorization: str,
    db: Session,
    minimum_tier: str = "essential",
) -> dict:
    """Checks user has at least the specified tier. Raises 403 if not."""
    user_info = resolve_user_tier(authorization, db)
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


def require_email_ownership(user_info: dict, requested_email: str) -> str:
    """
    FIX 1: Enforces that authenticated user can only access their own data.
    Returns the normalized email on success.
    """
    authenticated_email = (
        user_info.get("user").email
        if user_info.get("user")
        else None
    )
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


def update_last_active(request: Request, db: Session):
    from app.core.security import get_auth_header
    token = get_auth_header(request)
    if not token:
        return
    token_val = (
        token.replace("Bearer ", "").strip()
        if token.startswith("Bearer ")
        else token
    )
    user = db.query(User).filter(User.access_token == token_val).first()
    if user:
        user.last_active_at = datetime.datetime.utcnow()
        db.commit()


