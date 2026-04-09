import datetime
import secrets as secrets_mod
from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session

from app.core.security import get_auth_header
from app.db.database import get_db
from app.db.models import WebhookEndpoint, WebhookDelivery
from app.auth.auth import require_tier, require_email_ownership
from app.services.webhooks import deliver_webhook
from app.utils.schemas import WebhookCreateRequest, WebhookUpdateRequest
from app.utils.validation import validate_webhook_url

router = APIRouter()


@router.post("/api/v1/webhooks")
def create_webhook(
    body: WebhookCreateRequest,
    request: Request,
    db: Session = Depends(get_db),
):
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="institutional")
    email = require_email_ownership(user_info, body.email)

    # FIX 3: SSRF-safe URL validation
    body.url = validate_webhook_url(body.url)

    existing = db.query(WebhookEndpoint).filter(
        WebhookEndpoint.email == email,
        WebhookEndpoint.is_active == True,
    ).count()
    if existing >= 5:
        raise HTTPException(
            400, detail="Maximum 5 active webhooks per account"
        )

    webhook_secret = (
        body.secret or f"whsec_{secrets_mod.token_hex(20)}"
    )
    endpoint = WebhookEndpoint(
        email=email,
        url=body.url,
        secret=webhook_secret,
        events=body.events,
    )
    db.add(endpoint)
    db.commit()
    db.refresh(endpoint)

    return {
        "webhook_id": endpoint.id,
        "url": endpoint.url,
        "secret": webhook_secret,
        "events": body.events.split(","),
        "message": "Store the secret securely.",
        "verification": {
            "header": "X-ChainPulse-Signature",
            "format": "sha256=HMAC_SHA256(payload, secret)",
        },
    }


@router.get("/api/v1/webhooks")
def list_webhooks(
    request: Request,
    email: str = "",
    db: Session = Depends(get_db),
):
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="institutional")
    email = require_email_ownership(user_info, email)

    endpoints = db.query(WebhookEndpoint).filter(
        WebhookEndpoint.email == email
    ).all()
    return {
        "webhooks": [
            {
                "id": e.id,
                "url": e.url,
                "events": (
                    e.events.split(",") if e.events else []
                ),
                "is_active": e.is_active,
                "failure_count": e.failure_count,
                "last_triggered_at": e.last_triggered_at,
                "created_at": e.created_at,
            }
            for e in endpoints
        ]
    }


@router.put("/api/v1/webhooks/{webhook_id}")
def update_webhook(
    webhook_id: int,
    body: WebhookUpdateRequest,
    request: Request,
    db: Session = Depends(get_db),
):
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="institutional")
    email = require_email_ownership(user_info, body.email)

    endpoint = db.query(WebhookEndpoint).filter(
        WebhookEndpoint.id == webhook_id,
        WebhookEndpoint.email == email,
    ).first()
    if not endpoint:
        raise HTTPException(404, detail="Webhook not found")

    if body.url is not None:
        # FIX 3: SSRF-safe URL validation
        body.url = validate_webhook_url(body.url)
        endpoint.url = body.url
    if body.events is not None:
        endpoint.events = body.events
    if body.is_active is not None:
        endpoint.is_active = body.is_active
        if body.is_active:
            endpoint.failure_count = 0

    db.commit()
    return {"status": "updated", "webhook_id": webhook_id}


@router.delete("/api/v1/webhooks/{webhook_id}")
def delete_webhook(
    webhook_id: int,
    request: Request,
    email: str = "",
    db: Session = Depends(get_db),
):
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="institutional")
    email = require_email_ownership(user_info, email)

    endpoint = db.query(WebhookEndpoint).filter(
        WebhookEndpoint.id == webhook_id,
        WebhookEndpoint.email == email,
    ).first()
    if not endpoint:
        raise HTTPException(404, detail="Webhook not found")

    db.delete(endpoint)
    db.commit()
    return {"status": "deleted", "webhook_id": webhook_id}


@router.get("/api/v1/webhooks/{webhook_id}/deliveries")
def webhook_deliveries(
    webhook_id: int,
    request: Request,
    email: str = "",
    limit: int = 20,
    db: Session = Depends(get_db),
):
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="institutional")
    email = require_email_ownership(user_info, email)

    endpoint = db.query(WebhookEndpoint).filter(
        WebhookEndpoint.id == webhook_id,
        WebhookEndpoint.email == email,
    ).first()
    if not endpoint:
        raise HTTPException(404, detail="Webhook not found")

    deliveries = (
        db.query(WebhookDelivery)
        .filter(WebhookDelivery.endpoint_id == webhook_id)
        .order_by(WebhookDelivery.created_at.desc())
        .limit(min(limit, 50))
        .all()
    )
    return {
        "webhook_id": webhook_id,
        "url": endpoint.url,
        "deliveries": [
            {
                "id": d.id,
                "event_type": d.event_type,
                "success": d.success,
                "response_status": d.response_status,
                "attempt": d.attempt,
                "created_at": d.created_at,
            }
            for d in deliveries
        ],
    }


@router.post("/api/v1/webhooks/{webhook_id}/test")
async def test_webhook(
    webhook_id: int,
    request: Request,
    email: str = "",
    db: Session = Depends(get_db),
):
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="institutional")
    email = require_email_ownership(user_info, email)

    endpoint = db.query(WebhookEndpoint).filter(
        WebhookEndpoint.id == webhook_id,
        WebhookEndpoint.email == email,
    ).first()
    if not endpoint:
        raise HTTPException(404, detail="Webhook not found")

    test_payload = {
        "event": "test",
        "message": "This is a test webhook from ChainPulse",
        "coin": "BTC",
        "regime": "Risk-On",
        "exposure": 65.0,
        "shift_risk": 35.0,
        "timestamp": datetime.datetime.utcnow().isoformat(),
    }

    success = await deliver_webhook(
        endpoint, "test", test_payload, db
    )
    return {
        "success": success,
        "message": (
            "Test webhook delivered"
            if success
            else "Test webhook failed - check your endpoint"
        ),
    }


