import datetime
from typing import Optional
from fastapi import HTTPException, Request
from sqlalchemy.orm import Session

from app.db.models import ApiKey, User


def resolve_api_key(request: Request, db: Session) -> Optional[dict]:
    """
    Checks for API key in X-API-Key header or ?api_key= query param.
    Returns {'email': str, 'tier': str, 'api_key_id': int} or None.
    """
    api_key = (
        request.headers.get("X-API-Key")
        or request.headers.get("x-api-key")
        or request.query_params.get("api_key")
    )

    if not api_key or len(api_key) < 20:
        return None

    key_record = db.query(ApiKey).filter(
        ApiKey.key == api_key,
        ApiKey.is_active == True,
    ).first()

    if not key_record:
        return None

    # Check daily rate limit
    today = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    if key_record.last_request_date != today:
        key_record.requests_today = 0
        key_record.last_request_date = today

    if key_record.requests_today >= key_record.daily_limit:
        raise HTTPException(
            status_code=429,
            detail=(
                f"Daily API limit reached "
                f"({key_record.daily_limit} requests/day). "
                f"Resets at midnight UTC."
            ),
        )

    # Increment counter
    key_record.requests_today += 1
    key_record.last_used_at = datetime.datetime.utcnow()
    db.commit()

    # Verify the user is still institutional
    user = db.query(User).filter(
        User.email == key_record.email
    ).first()
    if (
        not user
        or user.subscription_status != "active"
        or user.tier != "institutional"
    ):
        return None

    return {
        "email": key_record.email,
        "tier": user.tier,
        "api_key_id": key_record.id,
        "requests_remaining": (
            key_record.daily_limit - key_record.requests_today
        ),
    }


def require_api_key(request: Request, db: Session) -> dict:
    """Requires valid API key. Raises 401 if invalid."""
    result = resolve_api_key(request, db)
    if not result:
        raise HTTPException(
            status_code=401,
            detail="Valid API key required. Get yours at /api/v1/keys",
        )
    return result


