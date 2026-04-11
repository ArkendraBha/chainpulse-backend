import datetime
from fastapi import APIRouter, Depends, BackgroundTasks
from sqlalchemy.orm import Session
import asyncio

from app.core.security import constant_time_compare
from app.core.config import settings
from app.db.database import get_db, SessionLocal
from app.db.models import User, ExposureLog
from app.services.market_data import (
    build_regime_stack,
    compute_market_breadth,
    update_market,
)
from app.services.regime_engine import compute_setup_quality
from app.services.alerts import evaluate_dynamic_alerts
from app.services.emails import send_email
from app.services.webhooks import trigger_webhooks

router = APIRouter()


async def run_full_update(db_factory):
    """Background task - full update cycle."""
    db = db_factory()
    try:
        results = {"updates": [], "alerts_sent": 0, "errors": []}

        for coin in settings.SUPPORTED_COINS:
            for tf in settings.SUPPORTED_TIMEFRAMES:
                try:
                    entry = await update_market(coin, tf, db)
                    if entry:
                        results["updates"].append({
                            "coin": coin,
                            "timeframe": tf,
                            "label": entry.label,
                            "score": entry.score,
                        })
                except Exception as e:
                    results["errors"].append(
                        f"Update {coin}/{tf}: {str(e)}"
                    )

        try:
            for coin in settings.SUPPORTED_COINS:
                stack = build_regime_stack(coin, db)
                if stack.get("incomplete"):
                    continue

                shift_risk = stack.get("shift_risk") or 0
                hazard = stack.get("hazard") or 0
                exec_label = (
                    stack["execution"]["label"]
                    if stack.get("execution") else "Neutral"
                )

                regime_payload = {
                    "coin": coin,
                    "macro": stack["macro"]["label"] if stack.get("macro") else None,
                    "trend": stack["trend"]["label"] if stack.get("trend") else None,
                    "execution": exec_label,
                    "alignment": stack.get("alignment"),
                    "direction": stack.get("direction"),
                    "exposure": stack.get("exposure"),
                    "shift_risk": shift_risk,
                    "hazard": hazard,
                    "survival": stack.get("survival"),
                }
                await trigger_webhooks(
                    "regime_change", regime_payload, db, coin=coin
                )

                if shift_risk > 65:
                    await trigger_webhooks(
                        "shift_risk_alert",
                        {
                            "coin": coin,
                            "shift_risk": shift_risk,
                            "hazard": hazard,
                            "regime": exec_label,
                            "exposure": stack.get("exposure"),
                            "message": f"{coin} shift risk elevated at {shift_risk}%",
                        },
                        db,
                        coin=coin,
                    )

                try:
                    setup = await compute_setup_quality(
                        coin, db, stack=stack
                    )
                    setup_score = setup.get("setup_quality_score") or 0
                    if setup_score >= 70:
                        await trigger_webhooks(
                            "setup_quality_alert",
                            {
                                "coin": coin,
                                "setup_score": setup_score,
                                "setup_label": setup.get("setup_label"),
                                "entry_mode": setup.get("entry_mode"),
                                "chase_risk": setup.get("chase_risk"),
                            },
                            db,
                            coin=coin,
                        )
                except Exception:
                    pass

        except Exception as e:
            results["errors"].append(f"Webhook dispatch: {str(e)}")

        try:
            pro_users = db.query(User).filter(
                User.subscription_status == "active",
                User.alerts_enabled == True,
            ).all()

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
                        high_alerts = [
                            a for a in alerts
                            if a.get("severity") in ("high", "medium", "positive")
                        ]
                    elif user.tier == "pro":
                        high_alerts = [
                            a for a in alerts
                            if a.get("severity") in ("high", "medium")
                        ]
                    else:
                        high_alerts = [
                            a for a in alerts
                            if a.get("severity") == "high"
                        ]

                    if not high_alerts:
                        continue

                    alert_lines = [
                        f"Ã¯Â¿Â½ {a.get('coin', '')} - {a.get('message', '')}"
                        for a in high_alerts[:3]
                    ]
                    alert_text = "<br>".join(alert_lines)
                    priority_prefix = (
                        "? Priority "
                        if user.tier == "institutional" else ""
                    )

                    send_email(
                        user.email,
                        f"ChainPulse - {priority_prefix}{len(high_alerts)} Alert{'s' if len(high_alerts) > 1 else ''}",
                        f"""
<div style="font-family:sans-serif;max-width:560px;margin:0 auto;
     background:#000;color:#fff;padding:40px;">
  <h2 style="color:#f87171;">{priority_prefix}{len(high_alerts)} Alert{'s' if len(high_alerts) > 1 else ''}</h2>
  <div style="color:#ccc;font-size:14px;line-height:2;">{alert_text}</div>
  <a href="{settings.FRONTEND_URL}/app?token={user.access_token or ''}"
     style="display:inline-block;background:#fff;color:#000;
            padding:14px 28px;margin-top:24px;
            text-decoration:none;font-weight:bold;border-radius:4px;">
    Open Dashboard
  </a>
</div>
""",
                    )
                    user.last_alert_sent = datetime.datetime.utcnow()
                    db.commit()
                    results["alerts_sent"] += 1
                except Exception as e:
                    results["errors"].append(
                        f"Alert {user.email}: {str(e)}"
                    )
        except Exception as e:
            results["errors"].append(f"Alert dispatch: {str(e)}")

        import logging
        logging.getLogger("chainpulse").info(
            f"cron_all complete: {len(results['updates'])} updates, "
            f"{results['alerts_sent']} alerts, "
            f"{len(results['errors'])} errors"
        )
    finally:
        db.close()


@router.get("/cron-all")
async def cron_all(
    secret: str = "",
    background_tasks: BackgroundTasks = None,
    db: Session = Depends(get_db),
):
    constant_time_compare(secret)
    if background_tasks:
        background_tasks.add_task(run_full_update, SessionLocal)
        return {
            "status": "started",
            "message": "Update running in background",
            "timestamp": datetime.datetime.utcnow().isoformat(),
        }
    else:
        await run_full_update(SessionLocal)
        return {
            "status": "complete",
            "timestamp": datetime.datetime.utcnow().isoformat(),
        }


@router.get("/send-what-changed")
def send_what_changed_email(
    secret: str = "",
    db: Session = Depends(get_db),
):
    from app.services.regime_engine import compute_what_changed
    constant_time_compare(secret)

    from app.services.regime_engine import get_or_compute_brief, compute_what_changed
    what_changed = get_or_compute_brief(
        db=db,
        brief_type="what_changed_72h",
        compute_fn=compute_what_changed,
        max_age_minutes=120,
        lookback_hours=72,
    )
    pro_users = db.query(User).filter(
        User.subscription_status == "active",
        User.alerts_enabled == True,
    ).all()

    changes_html = ""
    for change in what_changed.get("changes", [])[:10]:
        color = (
            "#4ade80" if change["severity"] == "positive"
            else "#f87171"
        )
        changes_html += f"""
        <tr>
          <td style="padding:10px 8px;border-bottom:1px solid #1f1f1f;
               color:#fff;font-weight:600;">{change['coin']}</td>
          <td style="padding:10px 8px;border-bottom:1px solid #1f1f1f;
               color:#999;font-size:12px;">{change['timeframe_label']}</td>
          <td style="padding:10px 8px;border-bottom:1px solid #1f1f1f;
               color:#999;font-size:12px;">{change['previous']}</td>
          <td style="padding:10px 8px;border-bottom:1px solid #1f1f1f;
               color:{color};font-weight:600;">? {change['current']}</td>
        </tr>"""

    if not changes_html:
        changes_html = '<tr><td colspan="4" style="padding:16px;color:#555;">No regime changes in the last 72 hours.</td></tr>'

    takeaways_html = "".join(
        f'<li style="color:#999;font-size:13px;line-height:2;">{t}</li>'
        for t in what_changed.get("takeaways", [])
    )

    tone = what_changed.get("tone", "stable")
    tone_color = {
        "improving": "#4ade80",
        "deteriorating": "#f87171",
        "mixed": "#facc15",
        "stable": "#999",
    }.get(tone, "#999")

    sent = 0
    errors = 0
    for user in pro_users:
        try:
            url = (
                f"{settings.FRONTEND_URL}/app?token={user.access_token}"
                if user.access_token
                else f"{settings.FRONTEND_URL}/app"
            )
            email_html = f"""
<div style="font-family:sans-serif;max-width:640px;margin:0 auto;
     background:#000;color:#fff;padding:40px;">
  <div style="font-size:11px;color:#555;text-transform:uppercase;
       letter-spacing:2px;margin-bottom:16px;">ChainPulse Intelligence Brief</div>
  <h1 style="font-size:22px;margin-bottom:8px;">What Changed - Last 72 Hours</h1>
  <p style="color:{tone_color};font-size:14px;margin-bottom:24px;">
    {what_changed.get('headline', 'No major changes')}
  </p>
  <table style="width:100%;border-collapse:collapse;margin-bottom:24px;">
    <thead><tr>
      <th style="text-align:left;padding:8px;color:#444;font-size:11px;
           text-transform:uppercase;border-bottom:1px solid #222;">Asset</th>
      <th style="text-align:left;padding:8px;color:#444;font-size:11px;
           text-transform:uppercase;border-bottom:1px solid #222;">Timeframe</th>
      <th style="text-align:left;padding:8px;color:#444;font-size:11px;
           text-transform:uppercase;border-bottom:1px solid #222;">Previous</th>
      <th style="text-align:left;padding:8px;color:#444;font-size:11px;
           text-transform:uppercase;border-bottom:1px solid #222;">Current</th>
    </tr></thead>
    <tbody>{changes_html}</tbody>
  </table>
  <div style="border:1px solid #1f1f1f;padding:20px;margin-bottom:24px;">
    <div style="font-size:11px;color:#555;text-transform:uppercase;
         letter-spacing:1px;margin-bottom:12px;">Key Takeaways</div>
    <ul style="padding-left:16px;margin:0;">{takeaways_html}</ul>
  </div>
  <a href="{url}"
     style="display:inline-block;background:#fff;color:#000;padding:14px 28px;
            text-decoration:none;font-weight:bold;border-radius:4px;">
    Open Dashboard
  </a>
  <p style="color:#333;font-size:11px;margin-top:40px;
       border-top:1px solid #111;padding-top:20px;">
    ChainPulse. Not financial advice.
  </p>
</div>
"""
            send_email(
                user.email,
                f"ChainPulse - What Changed ({tone.title()})",
                email_html,
            )
            sent += 1
        except Exception as e:
            import logging
            logging.getLogger("chainpulse").error(
                f"What Changed email failed for {user.email}: {e}"
            )
            errors += 1

    return {"status": "complete", "sent": sent, "errors": errors}


@router.get("/admin/churn-risk")
def churn_risk(
    secret: str = "",
    db: Session = Depends(get_db),
):
    constant_time_compare(secret)
    now = datetime.datetime.utcnow()
    users = db.query(User).filter(
        User.subscription_status == "active"
    ).all()

    at_risk = []
    for user in users:
        risk_score = 0
        reasons = []

        # Never logged in
        if not user.last_active_at:
            risk_score += 40
            reasons.append("Never logged in")

        # Days inactive
        elif (now - user.last_active_at).days > 7:
            days = (now - user.last_active_at).days
            risk_score += min(40, days * 3)
            reasons.append(f"Inactive {days} days")

        # Never logged exposure
        exposure_count = db.query(ExposureLog).filter(
            ExposureLog.email == user.email
        ).count()
        if exposure_count == 0:
            risk_score += 20
            reasons.append("Never logged exposure")
        elif exposure_count < 3:
            risk_score += 10
            reasons.append(f"Only {exposure_count} exposure logs")

        # Trial ending soon
        if user.trial_start_date:
            days_since_trial = (now - user.trial_start_date).days
            if 5 <= days_since_trial <= 7:
                risk_score += 25
                reasons.append(f"Trial day {days_since_trial}")

        # Low onboarding completion
        if (user.onboarding_step or 0) < 2:
            risk_score += 15
            reasons.append(f"Onboarding step {user.onboarding_step or 0}/6")

        if risk_score >= 30:
            risk_level = (
                "critical" if risk_score >= 70
                else "high" if risk_score >= 50
                else "medium"
            )
            at_risk.append({
                "email": user.email,
                "tier": user.tier,
                "risk_score": min(100, risk_score),
                "risk_level": risk_level,
                "reasons": reasons,
                "days_inactive": (
                    (now - user.last_active_at).days
                    if user.last_active_at else None
                ),
                "trial_day": (
                    (now - user.trial_start_date).days
                    if user.trial_start_date else None
                ),
                "exposure_logs": exposure_count,
                "onboarding_step": user.onboarding_step or 0,
            })

    at_risk.sort(key=lambda x: x["risk_score"], reverse=True)

    return {
        "at_risk_users": at_risk,
        "total_active": len(users),
        "critical_count": sum(1 for u in at_risk if u["risk_level"] == "critical"),
        "high_count": sum(1 for u in at_risk if u["risk_level"] == "high"),
        "medium_count": sum(1 for u in at_risk if u["risk_level"] == "medium"),
        "timestamp": datetime.datetime.utcnow().isoformat(),
    }



