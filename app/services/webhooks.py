import json
import asyncio
import logging
import datetime
from sqlalchemy.orm import Session

from app.core.security import sign_webhook_payload
from app.core.startup import httpx_client
from app.db.models import WebhookEndpoint, WebhookDelivery, User

logger = logging.getLogger("chainpulse")

RETRY_DELAYS = [10, 30, 120, 300, 600]


async def deliver_webhook_with_retry(
    endpoint: WebhookEndpoint,
    event_type: str,
    payload: dict,
    db: Session,
    max_retries: int = 5,
) -> bool:
    """Delivers webhook with exponential backoff retry."""
    for attempt in range(max_retries):
        success = await deliver_webhook(endpoint, event_type, payload, db)
        if success:
            return True

        if attempt < max_retries - 1:
            delay = RETRY_DELAYS[attempt]
            import logging

            logging.getLogger("chainpulse").warning(
                f"Webhook retry {attempt + 1} in {delay}s: {endpoint.url}"
            )
            await asyncio.sleep(delay)

    return False


async def deliver_webhook(
    endpoint: WebhookEndpoint,
    event_type: str,
    payload: dict,
    db: Session,
) -> bool:
    """FIX 7: Async webhook delivery using httpx.AsyncClient."""
    payload_str = json.dumps(payload)

    headers = {
        "Content-Type": "application/json",
        "User-Agent": "ChainPulse-Webhook/1.0",
        "X-ChainPulse-Event": event_type,
        "X-ChainPulse-Timestamp": datetime.datetime.utcnow().isoformat(),
    }

    if endpoint.secret:
        signature = sign_webhook_payload(payload_str, endpoint.secret)
        headers["X-ChainPulse-Signature"] = f"sha256={signature}"

    delivery = WebhookDelivery(
        endpoint_id=endpoint.id,
        event_type=event_type,
        payload=payload_str,
    )

    try:
        client = httpx_client
        if client is None:
            import httpx

            client = httpx.AsyncClient(timeout=10)

        r = await client.post(
            endpoint.url,
            content=payload_str,
            headers=headers,
        )
        delivery.response_status = r.status_code
        delivery.response_body = r.text[:500] if r.text else None
        delivery.success = 200 <= r.status_code < 300

        if delivery.success:
            endpoint.failure_count = 0
        else:
            endpoint.failure_count += 1

    except Exception as e:
        delivery.response_status = 0
        delivery.response_body = str(e)[:500]
        delivery.success = False
        endpoint.failure_count += 1
        logger.error(f"Webhook delivery failed for {endpoint.url}: {e}")

    endpoint.last_triggered_at = datetime.datetime.utcnow()
    db.add(delivery)

    if endpoint.failure_count >= 10:
        endpoint.is_active = False
        logger.warning(f"Webhook disabled after 10 failures: {endpoint.url}")

    db.commit()
    return delivery.success


async def trigger_webhooks(
    event_type: str,
    payload: dict,
    db: Session,
    coin: str = None,
):
    """Triggers all active webhooks for a given event type."""
    endpoints = (
        db.query(WebhookEndpoint)
        .filter(
            WebhookEndpoint.is_active == True,
        )
        .all()
    )

    sent = 0
    for endpoint in endpoints:
        subscribed_events = [e.strip() for e in (endpoint.events or "").split(",")]
        if event_type not in subscribed_events and "*" not in subscribed_events:
            continue

        user = db.query(User).filter(User.email == endpoint.email).first()
        if (
            not user
            or user.tier != "institutional"
            or user.subscription_status != "active"
        ):
            continue

        full_payload = {
            "event": event_type,
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "coin": coin,
            **payload,
        }

        await deliver_webhook_with_retry(endpoint, event_type, full_payload, db)
        sent += 1

    return sent


async def requeue_failed_webhooks(db: Session) -> int:
    """
    Processes dead letter queue.
    Call from hourly cron to retry failed webhooks.
    """
    from app.db.models import FailedWebhookQueue
    import datetime

    now = datetime.datetime.utcnow()
    pending = (
        db.query(FailedWebhookQueue)
        .filter(
            FailedWebhookQueue.permanently_failed == False,
            FailedWebhookQueue.next_retry_at <= now,
            FailedWebhookQueue.attempt_count < 10,
        )
        .limit(50)
        .all()
    )

    retried = 0
    for item in pending:
        endpoint = (
            db.query(WebhookEndpoint)
            .filter(
                WebhookEndpoint.id == item.endpoint_id,
                WebhookEndpoint.is_active == True,
            )
            .first()
        )

        if not endpoint:
            item.permanently_failed = True
            db.commit()
            continue

        import json

        payload = json.loads(item.payload)
        success = await deliver_webhook(endpoint, item.event_type, payload, db)

        item.attempt_count += 1
        item.last_attempted_at = now

        if success:
            db.delete(item)
        elif item.attempt_count >= 10:
            item.permanently_failed = True
            item.error_message = "Max retries exceeded"
        else:
            delay_minutes = 2**item.attempt_count
            item.next_retry_at = now + datetime.timedelta(minutes=delay_minutes)

        db.commit()
        retried += 1

    return retried
