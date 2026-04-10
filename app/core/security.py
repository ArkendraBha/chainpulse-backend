import hmac as hmac_lib
import hashlib
from typing import Optional
from fastapi import HTTPException, Request
from app.core.config import settings


def constant_time_compare(secret: str):
    """
    FIX 6: Constant-time compare for all secret checks.
    Replaces every `if secret != UPDATE_SECRET` across the codebase.
    """
    if not hmac_lib.compare_digest(
        secret or "",
        settings.UPDATE_SECRET or ""
    ):
        raise HTTPException(status_code=403, detail="Unauthorized")


def get_auth_header(request: Request) -> Optional[str]:
    return (
        request.headers.get("authorization")
        or request.headers.get("Authorization")
    )


def sign_webhook_payload(payload_str: str, secret: str) -> str:
    """Creates HMAC-SHA256 signature for webhook payload."""
    return hmac_lib.new(
        secret.encode("utf-8"),
        payload_str.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


