import json
import datetime
from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session
from fastapi.responses import StreamingResponse
import csv
import io

from app.core.config import settings
from app.core.security import get_auth_header
from app.db.database import get_db
from app.db.models import ExposureLog, PerformanceEntry, UserProfile
from app.auth.auth import require_tier, require_email_ownership, update_last_active
from app.services.market_data import build_regime_stack
from app.services.regime_engine import (
    compute_discipline_score,
    compute_performance_comparison,
    compute_mistake_replay,
    compute_behavioral_alpha_report,
)
from app.services.market_data import get_klines
from app.utils.schemas import ExposureLogRequest, PerformanceEntryRequest

router = APIRouter()


@router.post("/log-exposure")
async def log_exposure(
    request: Request,
    body: ExposureLogRequest,
    db: Session = Depends(get_db),
):
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="essential")
    email = require_email_ownership(user_info, body.email)

    if body.coin not in settings.SUPPORTED_COINS:
        raise HTTPException(400, detail="Unsupported coin")

    stack = build_regime_stack(body.coin, db)
    if stack["incomplete"]:
        raise HTTPException(400, detail="No regime data yet")

    model_exp = stack.get("exposure") or 50
    hazard = stack.get("hazard") or 0
    shift_risk = stack.get("shift_risk") or 0
    alignment = stack.get("alignment") or 0
    exec_label = stack["execution"]["label"] if stack.get("execution") else "Neutral"
    delta = body.user_exposure_pct - model_exp
    followed = abs(delta) <= 10

    current_price = 0.0
    try:
        prices, _ = await get_klines(body.coin, "1h", limit=2)
        if prices:
            current_price = prices[-1]
    except Exception:
        pass

    log = ExposureLog(
        email=email,
        coin=body.coin,
        user_exposure_pct=body.user_exposure_pct,
        model_exposure_pct=model_exp,
        regime_label=exec_label,
        hazard_at_log=hazard,
        shift_risk_at_log=shift_risk,
        alignment_at_log=alignment,
        followed_model=followed,
        price_at_log=current_price,
    )
    db.add(log)
    db.commit()

    if abs(delta) > 20:
        feedback = "? Large deviation from model recommendation"
        severity = "warning"
    elif abs(delta) > 10:
        feedback = "Moderate deviation - within acceptable range"
        severity = "caution"
    else:
        feedback = "? Aligned with model recommendation"
        severity = "ok"

    return {
        "status": "logged",
        "user_exposure": body.user_exposure_pct,
        "model_exposure": model_exp,
        "delta": round(delta, 1),
        "followed_model": followed,
        "feedback": feedback,
        "severity": severity,
        "regime": exec_label,
        "price_at_log": current_price,
    }


@router.get("/discipline-score")
def discipline_score_endpoint(
    request: Request,
    email: str,
    db: Session = Depends(get_db),
):
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="essential")
    email = require_email_ownership(user_info, email)
    update_last_active(request, db)
    logs = (
        db.query(ExposureLog)
        .filter(ExposureLog.email == email)
        .order_by(ExposureLog.created_at.desc())
        .limit(30)
        .all()
    )
    result = compute_discipline_score(logs)
    result["email"] = email
    return result


@router.get("/performance-comparison")
def performance_comparison_endpoint(
    request: Request,
    email: str,
    coin: str = "BTC",
    limit: int = 30,
    db: Session = Depends(get_db),
):
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="essential")
    email = require_email_ownership(user_info, email)
    entries = (
        db.query(PerformanceEntry)
        .filter(
            PerformanceEntry.email == email,
            PerformanceEntry.coin == coin,
        )
        .order_by(PerformanceEntry.date.asc())
        .limit(limit)
        .all()
    )
    result = compute_performance_comparison(entries)
    result["email"] = email
    result["coin"] = coin
    return result


@router.post("/log-performance")
async def log_performance(
    request: Request,
    body: PerformanceEntryRequest,
    db: Session = Depends(get_db),
):
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="essential")
    email = require_email_ownership(user_info, body.email)

    if body.coin not in settings.SUPPORTED_COINS:
        raise HTTPException(400, detail="Unsupported coin")
    if body.price_open <= 0 or body.price_close <= 0:
        raise HTTPException(400, detail="Invalid prices")

    stack = build_regime_stack(body.coin, db)
    model_exp = stack.get("exposure") or 50
    exec_label = (
        stack["execution"]["label"]
        if not stack["incomplete"] and stack.get("execution")
        else "Neutral"
    )

    price_return = ((body.price_close - body.price_open) / body.price_open) * 100
    user_return = round(price_return * (body.user_exposure_pct / 100), 2)
    model_return = round(price_return * (model_exp / 100), 2)

    flags = []
    delta = body.user_exposure_pct - model_exp
    hazard = stack.get("hazard") or 0
    shift_r = stack.get("shift_risk") or 0
    if hazard > 65 and delta > 10:
        flags.append("over_exposed_high_hazard")
    if "Risk-Off" in exec_label and delta > 15:
        flags.append("over_exposed_risk_off")
    if shift_r > 70 and delta < -5:
        flags.append("reduced_on_hazard_spike")
    if abs(delta) <= 10:
        flags.append("followed_model")

    entry = PerformanceEntry(
        email=email,
        coin=body.coin,
        date=datetime.datetime.utcnow(),
        user_exposure_pct=body.user_exposure_pct,
        model_exposure_pct=model_exp,
        price_open=body.price_open,
        price_close=body.price_close,
        user_return_pct=user_return,
        model_return_pct=model_return,
        regime_label=exec_label,
        discipline_flags=json.dumps(flags),
    )
    db.add(entry)
    db.commit()
    return {
        "status": "logged",
        "price_return": round(price_return, 2),
        "user_return": user_return,
        "model_return": model_return,
        "alpha": round(user_return - model_return, 2),
        "regime": exec_label,
        "discipline_flags": flags,
    }


@router.get("/mistake-replay")
def mistake_replay_endpoint(
    request: Request,
    email: str,
    coin: str = "BTC",
    db: Session = Depends(get_db),
):
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="essential")
    email = require_email_ownership(user_info, email)
    logs = (
        db.query(ExposureLog)
        .filter(
            ExposureLog.email == email,
            ExposureLog.coin == coin,
        )
        .order_by(ExposureLog.created_at.desc())
        .limit(50)
        .all()
    )
    replays = compute_mistake_replay(logs, db, coin)
    return {
        "email": email,
        "coin": coin,
        "replays": replays,
        "count": len(replays),
    }


@router.get("/edge-profile")
def edge_profile_endpoint(
    request: Request,
    email: str,
    db: Session = Depends(get_db),
):
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="essential")
    email = require_email_ownership(user_info, email)
    entries = (
        db.query(PerformanceEntry)
        .filter(PerformanceEntry.email == email)
        .order_by(PerformanceEntry.date.asc())
        .all()
    )

    if len(entries) < 5:
        return {
            "email": email,
            "ready": False,
            "message": f"Need {5 - len(entries)} more entries.",
            "entry_count": len(entries),
        }

    regime_data = {}
    for e in entries:
        label = e.regime_label or "Neutral"
        if label not in regime_data:
            regime_data[label] = []
        if e.user_return_pct is not None:
            regime_data[label].append(e.user_return_pct)

    profile = {}
    for regime, rets in regime_data.items():
        if rets:
            avg = round(sum(rets) / len(rets), 2)
            wins = sum(1 for r in rets if r > 0)
            profile[regime] = {
                "avg_return": avg,
                "win_rate": round((wins / len(rets)) * 100, 1),
                "count": len(rets),
                "performance": (
                    "Strong"
                    if avg > 2
                    else "Good" if avg > 0.5 else "Weak" if avg > -1 else "Poor"
                ),
            }

    if not profile:
        return {"email": email, "ready": False, "message": "No return data."}

    best_regime = max(profile.items(), key=lambda x: x[1]["avg_return"])
    worst_regime = min(profile.items(), key=lambda x: x[1]["avg_return"])

    recommendations = []
    for regime, data in profile.items():
        if data["performance"] in ("Weak", "Poor"):
            recommendations.append(
                f"Reduce exposure faster in {regime} (avg {data['avg_return']:+.1f}%)"
            )
        elif data["performance"] == "Strong":
            recommendations.append(
                f"You have edge in {regime} - stay disciplined (avg {data['avg_return']:+.1f}%)"
            )

    return {
        "email": email,
        "ready": True,
        "entry_count": len(entries),
        "best_regime": best_regime[0],
        "worst_regime": worst_regime[0],
        "profile": profile,
        "recommendations": recommendations,
    }


@router.get("/full-accountability")
def full_accountability(
    request: Request,
    email: str,
    coin: str = "BTC",
    db: Session = Depends(get_db),
):
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="essential")
    email = require_email_ownership(user_info, email)

    logs = (
        db.query(ExposureLog)
        .filter(ExposureLog.email == email)
        .order_by(ExposureLog.created_at.desc())
        .limit(50)
        .all()
    )
    entries = (
        db.query(PerformanceEntry)
        .filter(
            PerformanceEntry.email == email,
            PerformanceEntry.coin == coin,
        )
        .order_by(PerformanceEntry.date.asc())
        .limit(30)
        .all()
    )
    user_profile = db.query(UserProfile).filter(UserProfile.email == email).first()

    discipline = compute_discipline_score(logs)
    performance = compute_performance_comparison(entries)
    replays = compute_mistake_replay(logs, db, coin)

    edge = None
    if len(entries) >= 5:
        regime_data = {}
        for e in entries:
            label = e.regime_label or "Neutral"
            if label not in regime_data:
                regime_data[label] = []
            if e.user_return_pct is not None:
                regime_data[label].append(e.user_return_pct)
        edge = {
            regime: {
                "avg_return": round(sum(r) / len(r), 2),
                "win_rate": round(sum(1 for x in r if x > 0) / len(r) * 100, 1),
                "count": len(r),
            }
            for regime, r in regime_data.items()
            if r
        }

    return {
        "email": email,
        "coin": coin,
        "discipline": discipline,
        "performance": performance,
        "replays": replays,
        "edge": edge,
        "profile": (
            {
                "risk_identity": user_profile.risk_identity if user_profile else None,
                "risk_multiplier": (
                    user_profile.risk_multiplier if user_profile else None
                ),
                "max_drawdown_pct": (
                    user_profile.max_drawdown_pct if user_profile else None
                ),
                "holding_period_days": (
                    user_profile.holding_period_days if user_profile else None
                ),
            }
            if user_profile
            else None
        ),
        "has_profile": user_profile is not None,
    }


@router.get("/behavioral-alpha")
def behavioral_alpha_endpoint(
    request: Request,
    email: str = "",
    lookback_days: int = 30,
    db: Session = Depends(get_db),
):
    if not email:
        raise HTTPException(400, detail="Email required")
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="pro")
    email = require_email_ownership(user_info, email)
    update_last_active(request, db)
    lookback_days = min(max(7, lookback_days), 90)
    return compute_behavioral_alpha_report(email, db, lookback_days)


@router.get("/export/exposure-log")
def export_exposure_log(
    request: Request,
    email: str,
    format: str = "csv",
    db: Session = Depends(get_db),
):
    """Export exposure log as CSV."""
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="essential")
    email = require_email_ownership(user_info, email)

    logs = (
        db.query(ExposureLog)
        .filter(ExposureLog.email == email)
        .order_by(ExposureLog.created_at.desc())
        .limit(500)
        .all()
    )

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "date",
            "coin",
            "user_exposure_pct",
            "model_exposure_pct",
            "delta",
            "regime",
            "hazard",
            "shift_risk",
            "followed_model",
            "price_at_log",
        ]
    )
    for log in logs:
        writer.writerow(
            [
                log.created_at.strftime("%Y-%m-%d %H:%M"),
                log.coin,
                log.user_exposure_pct,
                log.model_exposure_pct,
                round((log.user_exposure_pct or 0) - (log.model_exposure_pct or 0), 1),
                log.regime_label,
                log.hazard_at_log,
                log.shift_risk_at_log,
                log.followed_model,
                log.price_at_log,
            ]
        )

    output.seek(0)
    return StreamingResponse(
        io.BytesIO(output.getvalue().encode()),
        media_type="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename=exposure_log_{email}.csv"
        },
    )


@router.get("/export/performance")
def export_performance(
    request: Request,
    email: str,
    coin: str = "BTC",
    db: Session = Depends(get_db),
):
    """Export performance entries as CSV."""
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="essential")
    email = require_email_ownership(user_info, email)

    entries = (
        db.query(PerformanceEntry)
        .filter(
            PerformanceEntry.email == email,
            PerformanceEntry.coin == coin,
        )
        .order_by(PerformanceEntry.date.desc())
        .limit(500)
        .all()
    )

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "date",
            "coin",
            "user_exposure_pct",
            "model_exposure_pct",
            "price_open",
            "price_close",
            "user_return_pct",
            "model_return_pct",
            "alpha",
            "regime",
        ]
    )
    for e in entries:
        writer.writerow(
            [
                e.date.strftime("%Y-%m-%d") if e.date else "",
                e.coin,
                e.user_exposure_pct,
                e.model_exposure_pct,
                e.price_open,
                e.price_close,
                e.user_return_pct,
                e.model_return_pct,
                round((e.user_return_pct or 0) - (e.model_return_pct or 0), 2),
                e.regime_label,
            ]
        )

    output.seek(0)
    return StreamingResponse(
        io.BytesIO(output.getvalue().encode()),
        media_type="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename=performance_{coin}_{email}.csv"
        },
    )
