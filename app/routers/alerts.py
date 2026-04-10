import datetime
from fastapi import APIRouter, Depends, HTTPException, Request, BackgroundTasks
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.security import get_auth_header, constant_time_compare
from app.db.database import get_db, SessionLocal
from app.db.models import User, ExposureLog, AlertThreshold
from app.auth.auth import require_tier, require_email_ownership, update_last_active
from app.services.alerts import evaluate_dynamic_alerts, run_dynamic_alert_dispatch
from app.services.market_data import build_regime_stack, compute_regime_quality
from app.services.emails import send_email, regime_alert_html, morning_email_html
from app.services.market_data import build_regime_stack
from app.utils.schemas import AlertThresholdRequest

router = APIRouter()


@router.post("/alert-thresholds")
def save_alert_thresholds(
    request: Request,
    body: AlertThresholdRequest,
    db: Session = Depends(get_db),
):
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="institutional")
    email = require_email_ownership(user_info, body.email)
    existing = (
        db.query(AlertThreshold)
        .filter(
            AlertThreshold.email == email,
            AlertThreshold.coin == body.coin,
        )
        .first()
    )
    if existing:
        existing.shift_risk_threshold = body.shift_risk_threshold
        existing.exposure_change_threshold = body.exposure_change_threshold
        existing.setup_quality_threshold = body.setup_quality_threshold
        existing.regime_quality_threshold = body.regime_quality_threshold
    else:
        existing = AlertThreshold(
            email=email,
            coin=body.coin,
            shift_risk_threshold=body.shift_risk_threshold,
            exposure_change_threshold=body.exposure_change_threshold,
            setup_quality_threshold=body.setup_quality_threshold,
            regime_quality_threshold=body.regime_quality_threshold,
        )
        db.add(existing)
    db.commit()
    return {
        "status": "saved",
        "email": email,
        "coin": body.coin,
        "thresholds": {
            "shift_risk": body.shift_risk_threshold,
            "exposure_change": body.exposure_change_threshold,
            "setup_quality": body.setup_quality_threshold,
            "regime_quality": body.regime_quality_threshold,
        },
    }


@router.get("/alert-thresholds")
def get_alert_thresholds(
    request: Request,
    email: str = "",
    db: Session = Depends(get_db),
):
    if not email:
        raise HTTPException(400, detail="Email required")
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="institutional")
    email = require_email_ownership(user_info, email)
    thresholds = (
        db.query(AlertThreshold)
        .filter(AlertThreshold.email == email)
        .all()
    )
    return {
        "email": email,
        "thresholds": [
            {
                "coin": t.coin,
                "shift_risk_threshold": t.shift_risk_threshold,
                "exposure_change_threshold": t.exposure_change_threshold,
                "setup_quality_threshold": t.setup_quality_threshold,
                "regime_quality_threshold": t.regime_quality_threshold,
                "enabled": t.enabled,
            }
            for t in thresholds
        ],
    }


@router.get("/evaluate-alerts")
async def evaluate_alerts_endpoint(
    request: Request,
    email: str = "",
    db: Session = Depends(get_db),
):
    if not email:
        raise HTTPException(400, detail="Email required")
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="pro")
    email = require_email_ownership(user_info, email)
    update_last_active(request, db)
    alerts = await evaluate_dynamic_alerts(email, db)
    return {
        "email": email,
        "alerts": alerts,
        "alert_count": len(alerts),
        "high_severity_count": sum(
            1 for a in alerts if a.get("severity") == "high"
        ),
        "timestamp": datetime.datetime.utcnow().isoformat(),
    }


@router.get("/send-alerts")
def send_alerts(
    secret: str = "",
    db: Session = Depends(get_db),
):
    constant_time_compare(secret)
    pro_users = db.query(User).filter(
        User.subscription_status == "active",
        User.alerts_enabled == True,
        User.tier.in_(["essential", "pro", "institutional"]),
    ).all()

    sent = 0
    for coin in settings.SUPPORTED_COINS:
        stack = build_regime_stack(coin, db)
        if stack["incomplete"]:
            continue
        if (stack.get("shift_risk") or 0) < 70:
            continue
        quality = compute_regime_quality(stack)

        for user in pro_users:
            if user.tier == "institutional":
                min_hours = 2
            elif user.tier == "pro":
                min_hours = 6
            else:
                min_hours = 12

            if user.last_alert_sent:
                hrs = (
                    datetime.datetime.utcnow() - user.last_alert_sent
                ).total_seconds() / 3600
                if hrs < min_hours:
                    continue

            subject_prefix = (
                "? Priority: " if user.tier == "institutional" else ""
            )
            send_email(
                user.email,
                f"{subject_prefix}ChainPulse Alert - {coin} Regime Shift Risk Elevated",
                regime_alert_html(coin, stack, quality),
            )
            user.last_alert_sent = datetime.datetime.utcnow()
            db.commit()
            sent += 1

    return {"status": "complete", "alerts_sent": sent}


@router.get("/send-morning-email")
def send_morning_email(
    secret: str = "",
    db: Session = Depends(get_db),
):
    constant_time_compare(secret)
    subscribers = db.query(User).filter(
        User.alerts_enabled == True
    ).all()
    stacks = []
    for coin in settings.SUPPORTED_COINS:
        stack = build_regime_stack(coin, db)
        if not stack["incomplete"]:
            stack["coin"] = coin
            stacks.append(stack)
    sent = 0
    for user in subscribers:
        send_email(
            user.email,
            "ChainPulse Morning Regime Brief",
            morning_email_html(stacks, user.access_token or ""),
        )
        sent += 1
    return {"status": "sent", "count": sent}


@router.get("/send-weekly-discipline")
def send_weekly_discipline(
    secret: str = "",
    db: Session = Depends(get_db),
):
    from app.services.regime_engine import compute_discipline_score
    from app.services.emails import weekly_discipline_email_html
    constant_time_compare(secret)
    pro_users = db.query(User).filter(
        User.subscription_status == "active",
        User.alerts_enabled == True,
    ).all()
    sent = 0
    errors = 0
    for user in pro_users:
        try:
            cutoff = datetime.datetime.utcnow() - datetime.timedelta(days=7)
            logs = (
                db.query(ExposureLog)
                .filter(
                    ExposureLog.email == user.email,
                    ExposureLog.created_at >= cutoff,
                )
                .order_by(ExposureLog.created_at.desc())
                .all()
            )
            discipline = compute_discipline_score(logs)
            if discipline.get("total", 0) == 0:
                continue
            send_email(
                user.email,
                "ChainPulse - Your Weekly Discipline Summary",
                weekly_discipline_email_html(
                    email=user.email,
                    discipline=discipline,
                    access_token=user.access_token or "",
                ),
            )
            sent += 1
        except Exception as e:
            import logging
            logging.getLogger("chainpulse").error(
                f"Weekly discipline email failed for {user.email}: {e}"
            )
            errors += 1
    return {"status": "complete", "sent": sent, "errors": errors}


@router.get("/send-dynamic-alerts")
async def send_dynamic_alerts(
    secret: str = "",
    db: Session = Depends(get_db),
):
    constant_time_compare(secret)
    result = await run_dynamic_alert_dispatch(db)
    return {"status": "complete", **result}


@router.get("/send-onboarding-drip")
def send_onboarding_drip(
    secret: str = "",
    db: Session = Depends(get_db),
):
    from app.services.emails import (
        onboarding_day0_html, onboarding_day2_html,
        onboarding_day5_html, onboarding_day6_html,
    )
    from app.services.market_data import build_regime_stack
    constant_time_compare(secret)
    now = datetime.datetime.utcnow()
    users = db.query(User).filter(
        User.subscription_status == "active",
        User.trial_start_date.isnot(None),
    ).all()

    sent = 0
    errors = 0
    for user in users:
        try:
            days_since = (now - user.trial_start_date).days
            if days_since == 0 and user.onboarding_step < 1:
                stack = build_regime_stack("BTC", db)
                send_email(
                    user.email,
                    "Welcome to ChainPulse Pro - Your first action",
                    onboarding_day0_html(
                        user.email, user.access_token or "", stack
                    ),
                )
                user.onboarding_step = 1
                sent += 1
            elif days_since >= 2 and user.onboarding_step < 2:
                send_email(
                    user.email,
                    "Day 2: Log your first exposure",
                    onboarding_day2_html(user.email, user.access_token or ""),
                )
                user.onboarding_step = 2
                sent += 1
            elif days_since >= 5 and user.onboarding_step < 5:
                send_email(
                    user.email,
                    "Day 5: Your behavior profile is ready",
                    onboarding_day5_html(user.email, user.access_token or ""),
                )
                user.onboarding_step = 5
                sent += 1
            elif days_since >= 6 and user.onboarding_step < 6:
                send_email(
                    user.email,
                    "Your trial ends tomorrow",
                    onboarding_day6_html(user.email, user.access_token or ""),
                )
                user.onboarding_step = 6
                sent += 1
            db.commit()
        except Exception as e:
            import logging
            logging.getLogger("chainpulse").error(
                f"Onboarding drip failed for {user.email}: {e}"
            )
            errors += 1
    return {"status": "complete", "sent": sent, "errors": errors}


