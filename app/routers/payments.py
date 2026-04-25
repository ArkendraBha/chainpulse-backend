import datetime
import logging
import stripe
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import Optional

from app.core.config import settings
from app.db.database import get_db
from app.db.models import User, StripeWebhookEvent
from app.auth.auth import generate_access_token, hash_token

logger = logging.getLogger("chainpulse")
router = APIRouter()

stripe.api_key = settings.STRIPE_SECRET_KEY

PRICE_IDS = settings.STRIPE_PRICE_MAP

PRICE_TO_TIER = {}
for _tier_name, _cycles in PRICE_IDS.items():
    for _cycle, _price_id in _cycles.items():
        if _price_id:
            PRICE_TO_TIER[_price_id] = _tier_name


class CheckoutRequest(BaseModel):
    tier: str
    billing_cycle: str = "monthly"
    email: Optional[str] = None


@router.post("/create-checkout-session")
def create_checkout_session(
    body: CheckoutRequest,
    request: Request,
    db: Session = Depends(get_db),
):
    tier = body.tier.lower()
    cycle = body.billing_cycle.lower()

    if tier not in PRICE_IDS:
        raise HTTPException(400, detail=f"Invalid tier: {tier}")
    if cycle not in ("monthly", "annual"):
        raise HTTPException(400, detail=f"Invalid billing cycle: {cycle}")

    price_id = PRICE_IDS[tier].get(cycle)
    if not price_id:
        raise HTTPException(400, detail=f"No price configured for {tier}/{cycle}")

    email = body.email
    if not email:
        auth_header = request.headers.get("authorization", "")
        if auth_header.startswith("Bearer "):
            token_raw = auth_header.replace("Bearer ", "")
            token_hash = hash_token(token_raw)
            user = db.query(User).filter(User.access_token == token_hash).first()
            if user:
                email = user.email

    try:
        checkout_params = {
            "line_items": [{"price": price_id, "quantity": 1}],
            "mode": "subscription",
            "success_url": f"{settings.FRONTEND_URL}/app?success=true&tier={tier}",
            "cancel_url": f"{settings.FRONTEND_URL}/pricing?cancelled=true",
            "metadata": {"tier": tier, "billing_cycle": cycle},
            "subscription_data": {
                "trial_period_days": 7,
                "metadata": {"tier": tier},
            },
            "allow_promotion_codes": True,
        }

        if email:
            checkout_params["customer_email"] = email
            checkout_params["client_reference_id"] = email

        session = stripe.checkout.Session.create(**checkout_params)
        return {"url": session.url}

    except stripe.error.StripeError as e:
        logger.error(f"Stripe checkout error: {e}")
        raise HTTPException(500, detail="Payment system error. Please try again.")


@router.post("/stripe-webhook")
async def stripe_webhook(request: Request, db: Session = Depends(get_db)):
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature")

    if not sig_header:
        raise HTTPException(400, detail="Missing stripe-signature header")

    try:
        event = stripe.Webhook.construct_event(
            payload, sig_header, settings.STRIPE_WEBHOOK_SECRET
        )
    except stripe.error.SignatureVerificationError:
        logger.error("Stripe webhook signature verification failed")
        raise HTTPException(400, detail="Invalid signature")
    except Exception as e:
        logger.error(f"Stripe webhook error: {e}")
        raise HTTPException(400, detail="Webhook error")

    existing = (
        db.query(StripeWebhookEvent)
        .filter(StripeWebhookEvent.stripe_event_id == event["id"])
        .first()
    )
    if existing:
        return JSONResponse(content={"status": "already_processed"}, status_code=200)

    db.add(StripeWebhookEvent(
        stripe_event_id=event["id"],
        event_type=event["type"],
    ))

    event_type = event["type"]
    logger.info(f"Stripe webhook received: {event_type}")

    try:
        if event_type == "checkout.session.completed":
            session = event["data"]["object"]
            customer_email = (session.get("customer_email") or "").lower().strip()
            client_ref = (session.get("client_reference_id") or "").lower().strip()
            stripe_customer_id = session.get("customer")
            stripe_subscription_id = session.get("subscription")
            tier = session.get("metadata", {}).get("tier", "essential")

            email = customer_email or client_ref
            if not email:
                logger.error(f"Stripe webhook: No email in checkout session {session.get('id')}")
                db.commit()
                return JSONResponse(content={"status": "no_email"}, status_code=200)

            user = db.query(User).filter(User.email == email).first()

            if not user:
                user = User(
                    email=email,
                    tier=tier,
                    subscription_status="active",
                    stripe_customer_id=stripe_customer_id,
                    stripe_subscription_id=stripe_subscription_id,
                    trial_start_date=datetime.datetime.utcnow(),
                    created_at=datetime.datetime.utcnow(),
                )
                db.add(user)
                logger.info(f"Created new user: {email} with tier: {tier}")
            else:
                user.tier = tier
                user.subscription_status = "active"
                user.stripe_customer_id = stripe_customer_id
                user.stripe_subscription_id = stripe_subscription_id
                logger.info(f"Updated user: {email} to tier: {tier}")

            db.commit()

            try:
                from app.auth.login import send_login_email
                send_login_email(email, db)
                logger.info(f"Login email sent to {email}")
            except Exception as e:
                logger.error(f"Failed to send login email to {email}: {e}")

        elif event_type == "customer.subscription.updated":
            subscription = event["data"]["object"]
            stripe_customer_id = subscription.get("customer")
            status = subscription.get("status")

            tier = subscription.get("metadata", {}).get("tier")
            if not tier and subscription.get("items", {}).get("data"):
                price_id = subscription["items"]["data"][0].get("price", {}).get("id")
                tier = PRICE_TO_TIER.get(price_id, "essential")

            user = db.query(User).filter(
                User.stripe_customer_id == stripe_customer_id
            ).first()

            if user:
                if status == "active":
                    user.tier = tier or user.tier
                    user.subscription_status = "active"
                elif status in ("past_due", "unpaid"):
                    user.subscription_status = "past_due"
                elif status == "canceled":
                    user.tier = "free"
                    user.subscription_status = "inactive"
                db.commit()
                logger.info(f"Subscription updated for {user.email}: status={status}, tier={user.tier}")

        elif event_type == "customer.subscription.deleted":
            subscription = event["data"]["object"]
            stripe_customer_id = subscription.get("customer")

            user = db.query(User).filter(
                User.stripe_customer_id == stripe_customer_id
            ).first()

            if user:
                user.tier = "free"
                user.subscription_status = "inactive"
                db.commit()
                logger.info(f"Subscription cancelled for {user.email}")

        elif event_type == "invoice.payment_failed":
            invoice = event["data"]["object"]
            stripe_customer_id = invoice.get("customer")

            user = db.query(User).filter(
                User.stripe_customer_id == stripe_customer_id
            ).first()

            if user:
                user.subscription_status = "past_due"
                db.commit()
                logger.warning(f"Payment failed for {user.email}")

    except Exception as e:
        logger.error(f"Error processing Stripe webhook {event_type}: {e}")

    db.commit()
    return JSONResponse(content={"status": "ok"}, status_code=200)


@router.get("/user-status")
def user_status(request: Request, db: Session = Depends(get_db)):
    auth_header = request.headers.get("authorization", "")
    if not auth_header.startswith("Bearer "):
        raise HTTPException(401, detail="Not authenticated")

    token_raw = auth_header.replace("Bearer ", "")
    token_hash = hash_token(token_raw)

    user = db.query(User).filter(User.access_token == token_hash).first()
    if not user:
        raise HTTPException(401, detail="Invalid token")

    if user.token_created_at:
        token_age = datetime.datetime.utcnow() - user.token_created_at
        if token_age.days > 90:
            raise HTTPException(401, detail="Token expired")

    is_pro = user.subscription_status == "active" and user.tier != "free"

    return {
        "email": user.email,
        "tier": user.tier,
        "is_pro": is_pro,
        "subscription_status": user.subscription_status,
        "stripe_customer_id": user.stripe_customer_id,
        "token_created_at": user.token_created_at.isoformat() if user.token_created_at else None,
        "created_at": user.created_at.isoformat() if user.created_at else None,
    }
