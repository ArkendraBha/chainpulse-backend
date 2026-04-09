import logging
import datetime
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.models import User, ExposureLog, AlertThreshold
from app.services.market_data import (
    build_regime_stack,
    compute_regime_quality,
    compute_market_breadth,
)
from app.services.regime_engine import (
    compute_setup_quality,
    compute_internal_damage,
    compute_discipline_score,
)
from app.services.emails import send_email
from app.services.webhooks import trigger_webhooks

logger = logging.getLogger("chainpulse")


async def evaluate_dynamic_alerts(email: str, db: Session) -> list:
    user = db.query(User).filter(User.email == email).first()
    if not user or user.subscription_status != "active":
        return []

    thresholds = (
        db.query(AlertThreshold)
        .filter(
            AlertThreshold.email == email,
            AlertThreshold.enabled == True,
        )
        .all()
    )

    if not thresholds:
        thresholds = []
        for coin in settings.SUPPORTED_COINS:
            thresholds.append(AlertThreshold(
                email=email,
                coin=coin,
                shift_risk_threshold=70,
                exposure_change_threshold=10,
                setup_quality_threshold=70,
                regime_quality_threshold=50,
            ))

    alerts = []

    for threshold in thresholds:
        coin = threshold.coin
        stack = build_regime_stack(coin, db)
        if stack.get("incomplete"):
            continue

        shift_risk = stack.get("shift_risk") or 0
        hazard = stack.get("hazard") or 0
        exposure = stack.get("exposure") or 50

        # Shift Risk Alert
        if shift_risk >= threshold.shift_risk_threshold:
            alerts.append({
                "type": "shift_risk_elevated",
                "coin": coin,
                "severity": "high" if shift_risk > 80 else "medium",
                "message": (
                    f"{coin} shift risk at {shift_risk}% - exceeds your "
                    f"threshold of {threshold.shift_risk_threshold}%"
                ),
                "action": (
                    f"Consider reducing {coin} exposure to "
                    f"{int(max(5, exposure * 0.7))}%"
                ),
                "value": shift_risk,
                "threshold": threshold.shift_risk_threshold,
            })

        # Setup Quality Alert
        setup = await compute_setup_quality(coin, db, stack=stack)
        setup_score = setup.get("setup_quality_score") or 0

        if setup_score >= threshold.setup_quality_threshold:
            alerts.append({
                "type": "setup_quality_upgraded",
                "coin": coin,
                "severity": "positive",
                "message": (
                    f"{coin} setup quality upgraded to {setup_score} - "
                    f"{setup.get('setup_label', '')}. "
                    f"Entry mode: {setup.get('entry_mode', 'Wait')}"
                ),
                "action": f"Consider entering {coin} per trade plan",
                "value": setup_score,
                "threshold": threshold.setup_quality_threshold,
            })

        # Regime Quality Alert
        quality = compute_regime_quality(stack)
        if quality["score"] < threshold.regime_quality_threshold:
            alerts.append({
                "type": "regime_quality_degraded",
                "coin": coin,
                "severity": "medium",
                "message": (
                    f"{coin} regime quality dropped to "
                    f"{quality['grade']} ({quality['score']}) - "
                    f"{quality['structural']}"
                ),
                "action": "Reduce exposure and tighten stops",
                "value": quality["score"],
                "threshold": threshold.regime_quality_threshold,
            })

        # Exposure Misalignment Alert
        recent_log = (
            db.query(ExposureLog)
            .filter(
                ExposureLog.email == email,
                ExposureLog.coin == coin,
            )
            .order_by(ExposureLog.created_at.desc())
            .first()
        )
        if recent_log:
            user_exp = recent_log.user_exposure_pct or 0
            delta = abs(user_exp - exposure)
            if delta > threshold.exposure_change_threshold + 10:
                alerts.append({
                    "type": "exposure_misalignment",
                    "coin": coin,
                    "severity": "medium",
                    "message": (
                        f"Your {coin} exposure ({user_exp}%) is "
                        f"{round(delta, 1)}% away from model "
                        f"recommendation ({exposure}%)"
                    ),
                    "action": f"Adjust toward {exposure}% recommended exposure",
                    "value": delta,
                    "threshold": threshold.exposure_change_threshold,
                })

        # Internal Damage Alert
        damage = await compute_internal_damage(coin, db, stack=stack)
        if (
            damage.get("internal_damage_score")
            and damage["internal_damage_score"] > 60
        ):
            alerts.append({
                "type": "internal_damage",
                "coin": coin,
                "severity": (
                    "high"
                    if damage["internal_damage_score"] > 75
                    else "medium"
                ),
                "message": (
                    f"{coin} internal damage score: "
                    f"{damage['internal_damage_score']} "
                    f"({damage['damage_label']})"
                ),
                "action": damage["damage_message"],
                "value": damage["internal_damage_score"],
                "signals": [
                    s["message"]
                    for s in damage.get("signals", [])[:3]
                ],
            })

    return alerts


async def run_dynamic_alert_dispatch(db: Session) -> dict:
    """
    Sends dynamic alerts to all eligible users.
    Called from cron endpoint.
    """
    pro_users = db.query(User).filter(
        User.subscription_status == "active",
        User.alerts_enabled == True,
        User.tier.in_(["essential", "pro", "institutional"]),
    ).all()

    sent = 0
    errors = 0

    for user in pro_users:
        try:
            if user.tier == "institutional":
                min_hours = 1
            elif user.tier == "pro":
                min_hours = 4
            else:
                min_hours = 8

            if user.last_alert_sent:
                hrs = (
                    datetime.datetime.utcnow() - user.last_alert_sent
                ).total_seconds() / 3600
                if hrs < min_hours:
                    continue

            alerts = await evaluate_dynamic_alerts(user.email, db)

            if user.tier == "institutional":
                filtered = [
                    a for a in alerts
                    if a.get("severity") in ("high", "medium", "positive")
                ]
            elif user.tier == "pro":
                filtered = [
                    a for a in alerts
                    if a.get("severity") in ("high", "medium")
                ]
            else:
                filtered = [
                    a for a in alerts
                    if a.get("severity") == "high"
                ]

            if not filtered:
                continue

            priority_prefix = (
                "? Priority "
                if user.tier == "institutional"
                else ""
            )

            alert_lines = []
            for a in filtered[:3]:
                alert_lines.append(
                    f"� {a.get('coin', '')} - {a.get('message', '')}"
                )
            alert_text = "<br>".join(alert_lines)

            send_email(
                user.email,
                f"ChainPulse - {priority_prefix}{len(filtered)} Alert{'s' if len(filtered) > 1 else ''}",
                f"""
<div style="font-family:sans-serif;max-width:560px;margin:0 auto;
     background:#000;color:#fff;padding:40px;">
  <div style="font-size:11px;color:#555;text-transform:uppercase;
       letter-spacing:2px;margin-bottom:16px;">ChainPulse Alert</div>
  <h2 style="color:#f87171;margin-bottom:24px;">
    {priority_prefix}{len(filtered)} Alert{'s' if len(filtered) > 1 else ''}
  </h2>
  <div style="color:#ccc;font-size:14px;line-height:2;">{alert_text}</div>
  <a href="{settings.FRONTEND_URL}/app?token={user.access_token or ''}"
     style="display:inline-block;background:#fff;color:#000;
            padding:14px 28px;margin-top:24px;
            text-decoration:none;font-weight:bold;border-radius:4px;">
    Open Dashboard
  </a>
  <p style="color:#333;font-size:11px;margin-top:40px;">
    ChainPulse. Not financial advice.
  </p>
</div>
""",
            )
            user.last_alert_sent = datetime.datetime.utcnow()
            db.commit()
            sent += 1

        except Exception as e:
            logger.error(f"Alert dispatch failed for {user.email}: {e}")
            errors += 1

    return {"sent": sent, "errors": errors}


