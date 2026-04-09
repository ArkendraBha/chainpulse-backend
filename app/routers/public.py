import datetime
import json
import requests
import os
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.security import get_auth_header, constant_time_compare
from app.core.rate_limit import rate_limiter
from app.db.database import get_db
from app.db.models import User, MarketSummary
from app.auth.auth import (
    resolve_pro_status,
    resolve_user_tier,
    update_last_active,
)
from app.services.market_data import (
    build_regime_stack,
    compute_market_breadth,
    regime_durations,
    current_age,
    average_regime_duration,
    trend_maturity_score,
    percentile_rank,
    compute_regime_quality,
    regime_transition_matrix,
    volatility_environment,
    build_correlation_matrix,
    regime_confidence_score,
    get_klines,
    update_market,
)
from app.core.cache import get_or_compute, cache_get, cache_set
from app.utils.enums import RISK_EVENTS, PLAYBOOK_DATA

router = APIRouter()


@router.get("/update-now")
async def update_now(
    coin: str = "BTC",
    timeframe: str = "1h",
    secret: str = "",
    db: Session = Depends(get_db),
):
    constant_time_compare(secret)
    if coin not in settings.SUPPORTED_COINS:
        raise HTTPException(400, detail="Unsupported coin")
    if timeframe not in settings.SUPPORTED_TIMEFRAMES:
        raise HTTPException(400, detail="Unsupported timeframe")
    entry = await update_market(coin, timeframe, db)
    if not entry:
        raise HTTPException(500, detail="Update failed")
    return {
        "status": "updated",
        "coin": coin,
        "timeframe": timeframe,
        "label": entry.label,
        "score": entry.score,
    }


@router.get("/update-all")
async def update_all(
    secret: str = "",
    db: Session = Depends(get_db),
):
    constant_time_compare(secret)
    results = []
    for coin in settings.SUPPORTED_COINS:
        for tf in settings.SUPPORTED_TIMEFRAMES:
            entry = await update_market(coin, tf, db)
            if entry:
                results.append({
                    "coin": coin,
                    "timeframe": tf,
                    "label": entry.label,
                    "score": entry.score,
                })
    return {
        "status": "updated",
        "count": len(results),
        "results": results,
    }


@router.get("/regime-stack")
async def regime_stack_endpoint(
    request: Request,
    coin: str = "BTC",
    db: Session = Depends(get_db),
):
    if coin not in settings.SUPPORTED_COINS:
        raise HTTPException(400, detail="Unsupported coin")

    is_pro = resolve_pro_status(get_auth_header(request), db)
    stack = build_regime_stack(coin, db)

    if stack["incomplete"]:
        return {**stack, "pro_required": False}

    if not is_pro:
        return {
            "coin": stack["coin"],
            "execution": (
                {"label": stack["execution"]["label"]}
                if stack["execution"] else None
            ),
            "direction": stack["direction"],
            "pro_required": True,
            "upgrade_message": (
                "Unlock macro + trend regimes, exposure guidance, "
                "and 15+ premium tools"
            ),
        }

    update_last_active(request, db)
    age_1h = current_age(db, coin, "1h")
    avg_dur = average_regime_duration(db, coin, "1h")
    maturity = trend_maturity_score(age_1h, avg_dur, stack["hazard"])
    pct_rank = percentile_rank(
        db, coin, stack["execution"]["score"], "1h"
    )
    quality = compute_regime_quality(stack)

    return {
        "coin": stack["coin"],
        "macro": stack["macro"],
        "trend": stack["trend"],
        "execution": stack["execution"],
        "alignment": stack["alignment"],
        "direction": stack["direction"],
        "pro_required": False,
        "exposure": stack["exposure"],
        "shift_risk": stack["shift_risk"],
        "survival": stack["survival"],
        "hazard": stack["hazard"],
        "trend_maturity": maturity,
        "percentile": pct_rank,
        "macro_coherence": stack["macro"]["coherence"],
        "trend_coherence": stack["trend"]["coherence"],
        "exec_coherence": stack["execution"]["coherence"],
        "regime_age_hours": round(age_1h, 2),
        "avg_regime_duration_hours": round(avg_dur, 2),
        "regime_quality": quality,
    }


@router.get("/market-overview")
async def market_overview(
    request: Request,
    coin: str = "ALL",
    db: Session = Depends(get_db),
):
    is_pro = resolve_pro_status(get_auth_header(request), db)
    result = []
    breadth = get_or_compute(
        "market_breadth", compute_market_breadth, ttl=60, db=db
    )

    coins_to_scan = (
        settings.SUPPORTED_COINS
        if coin == "ALL"
        else [coin] if coin in settings.SUPPORTED_COINS
        else settings.SUPPORTED_COINS
    )

    for c in coins_to_scan:
        stack = build_regime_stack(c, db)
        if stack["incomplete"]:
            continue
        if is_pro:
            row = {
                "coin": stack["coin"],
                "macro": stack["macro"]["label"] if stack["macro"] else None,
                "trend": stack["trend"]["label"] if stack["trend"] else None,
                "execution": stack["execution"]["label"] if stack["execution"] else None,
                "alignment": stack["alignment"],
                "direction": stack["direction"],
                "exposure": stack["exposure"],
                "shift_risk": stack["shift_risk"],
            }
        else:
            row = {
                "coin": stack["coin"],
                "execution": stack["execution"]["label"] if stack["execution"] else None,
                "direction": stack["direction"],
                "pro_required": True,
            }
        result.append(row)

    if not is_pro:
        breadth_free = {
            "total": breadth.get("total", 0),
            "sentiment": (
                "Bullish" if breadth.get("breadth_score", 0) > 30
                else "Bearish" if breadth.get("breadth_score", 0) < -30
                else "Neutral"
            ),
            "pro_required": True,
        }
        return {"data": result, "breadth": breadth_free}

    return {"data": result, "breadth": breadth}


@router.get("/latest")
def latest(coin: str = "BTC", db: Session = Depends(get_db)):
    if coin not in settings.SUPPORTED_COINS:
        raise HTTPException(400, detail="Unsupported coin")
    r = (
        db.query(MarketSummary)
        .filter(
            MarketSummary.coin == coin,
            MarketSummary.timeframe == "1h",
        )
        .order_by(MarketSummary.created_at.desc())
        .first()
    )
    if not r:
        return {"message": "No data yet."}
    return {
        "coin": r.coin,
        "score": r.score,
        "label": r.label,
        "coherence": r.coherence,
        "momentum_4h": r.momentum_4h,
        "momentum_24h": r.momentum_24h,
        "volatility": r.volatility_val,
        "timeframe": r.timeframe,
        "timestamp": r.created_at,
    }


@router.get("/statistics")
def statistics(coin: str = "BTC", db: Session = Depends(get_db)):
    if coin not in settings.SUPPORTED_COINS:
        raise HTTPException(400, detail="Unsupported coin")
    record = (
        db.query(MarketSummary)
        .filter(
            MarketSummary.coin == coin,
            MarketSummary.timeframe == "1h",
        )
        .order_by(MarketSummary.created_at.desc())
        .first()
    )
    if not record:
        return {"message": "No data yet"}
    return {
        "coin": coin,
        "label": record.label,
        "score": record.score,
        "coherence": record.coherence,
        "timestamp": record.created_at,
    }


@router.get("/regime-history")
def regime_history(
    coin: str = "BTC",
    timeframe: str = "1h",
    limit: int = 48,
    db: Session = Depends(get_db),
):
    if timeframe not in settings.SUPPORTED_TIMEFRAMES:
        raise HTTPException(400, detail="Unsupported timeframe")
    if coin not in settings.SUPPORTED_COINS:
        raise HTTPException(400, detail="Unsupported coin")
    limit = min(max(1, limit), 500)
    records = (
        db.query(MarketSummary)
        .filter(
            MarketSummary.coin == coin,
            MarketSummary.timeframe == timeframe,
        )
        .order_by(MarketSummary.created_at.desc())
        .limit(limit)
        .all()
    )
    records.reverse()
    return {
        "data": [
            {
                "hour": i,
                "score": r.score,
                "label": r.label,
                "coherence": r.coherence,
                "timestamp": r.created_at,
            }
            for i, r in enumerate(records)
        ]
    }


@router.get("/risk-events")
def risk_events():
    return {"events": RISK_EVENTS}


@router.get("/archetypes")
def list_archetypes():
    from app.utils.enums import ARCHETYPE_CONFIG
    return {
        "archetypes": {
            key: {
                "label": config["label"],
                "description": config["description"],
                "exposure_mult": config["exposure_mult"],
                "alert_sensitivity": config["alert_sensitivity"],
                "preferred_timeframe": config["preferred_timeframe"],
                "max_hold_days": config["max_hold_days"],
                "stop_width_mult": config["stop_width_mult"],
                "playbook_bias": config["playbook_bias"],
            }
            for key, config in ARCHETYPE_CONFIG.items()
        }
    }


@router.get("/user-status")
def user_status(request: Request, db: Session = Depends(get_db)):
    user_info = resolve_user_tier(get_auth_header(request), db)
    return {
        "is_pro": user_info["is_pro"],
        "tier": user_info["tier"],
        "timestamp": datetime.datetime.utcnow(),
    }


@router.get("/ticker")
def ticker(request: Request):
    rate_limiter.require(request, max_requests=30, window_seconds=60)
    symbols = [f"{c}USDT" for c in settings.SUPPORTED_COINS]
    try:
        r = requests.get(
            "[api.binance.com](https://api.binance.com/api/v3/ticker/24hr)",
            params={"symbols": json.dumps(symbols)},
            timeout=10,
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        import logging
        logging.getLogger("chainpulse").error(f"Ticker fetch failed: {e}")
        return []


@router.get("/debug-prices")
def debug_prices(
    coin: str = "BTC",
    interval: str = "1h",
    secret: str = "",
):
    import hmac as hmac_lib
    if not hmac_lib.compare_digest(
        secret or "", settings.UPDATE_SECRET or ""
    ):
        raise HTTPException(403, detail="Unauthorized")
    import asyncio
    prices, volumes = asyncio.run(
        get_klines(coin, interval, limit=120)
    )
    return {
        "coin": coin,
        "interval": interval,
        "price_count": len(prices),
        "volume_count": len(volumes),
        "last_price": prices[-1] if prices else None,
        "first_price": prices[0] if prices else None,
        "last_volume": volumes[-1] if volumes else None,
    }


@router.get("/debug-stack")
def debug_stack(
    coin: str = "BTC",
    secret: str = "",
    db: Session = Depends(get_db),
):
    import hmac as hmac_lib
    if not hmac_lib.compare_digest(
        secret or "", settings.UPDATE_SECRET or ""
    ):
        raise HTTPException(403, detail="Unauthorized")
    if coin not in settings.SUPPORTED_COINS:
        raise HTTPException(400, detail="Unsupported coin")
    stack = build_regime_stack(coin, db)
    breadth = compute_market_breadth(db)
    quality = (
        compute_regime_quality(stack)
        if not stack["incomplete"] else None
    )
    return {"stack": stack, "breadth": breadth, "quality": quality}


@router.get("/sample-report")
def sample_report():
    path = "sample_report.pdf"
    if not os.path.exists(path):
        raise HTTPException(404, detail="Report not found")
    return FileResponse(path, media_type="application/pdf")


@router.get("/playbook")
def playbook(
    request: Request,
    coin: str = "BTC",
    db: Session = Depends(get_db),
):
    if coin not in settings.SUPPORTED_COINS:
        raise HTTPException(400, detail="Unsupported coin")
    is_pro = resolve_pro_status(get_auth_header(request), db)
    stack = build_regime_stack(coin, db)
    if stack["incomplete"]:
        return {"error": "Insufficient data"}
    exec_label = (
        stack["execution"]["label"]
        if stack.get("execution") else "Neutral"
    )
    pb = PLAYBOOK_DATA.get(exec_label, PLAYBOOK_DATA["Neutral"])

    if not is_pro:
        return {
            "coin": coin,
            "regime": exec_label,
            "strategy_mode": pb["strategy_mode"],
            "pro_required": True,
        }

    return {
        "coin": coin,
        "regime": exec_label,
        "strategy_mode": pb["strategy_mode"],
        "exposure_band": pb["exposure_band"],
        "trend_follow_wr": pb["trend_follow_wr"],
        "mean_revert_wr": pb["mean_revert_wr"],
        "avg_remaining_days": pb["avg_remaining_days"],
        "data_source": pb.get("data_source", "backtested_estimates"),
        "actions": pb["actions"],
        "avoid": pb["avoid"],
        "pro_required": False,
    }


@router.post("/subscribe")
def subscribe(
    request: Request,
    db: Session = Depends(get_db),
):
    from app.utils.schemas import SubscribeRequest
    from app.services.emails import send_email
    rate_limiter.require(request, max_requests=5, window_seconds=3600)
    import pydantic
    try:
        body = SubscribeRequest.model_validate(request)
    except Exception:
        raise HTTPException(400, detail="Invalid request body")
    email = body.email.strip().lower()
    user = db.query(User).filter(User.email == email).first()
    if not user:
        user = User(
            email=email,
            subscription_status="inactive",
            alerts_enabled=False,
        )
        db.add(user)
        db.commit()

    confirmation_link = f"{settings.BACKEND_URL}/confirm?email={email}"
    html = f"""
<div style="background:#000;padding:40px 0;font-family:-apple-system,sans-serif;">
  <div style="max-width:600px;margin:0 auto;background:#0b0b0f;
       border:1px solid rgba(255,255,255,0.08);border-radius:24px;
       padding:40px;color:#fff;">
    <div style="font-size:12px;letter-spacing:2px;text-transform:uppercase;
         color:#6b7280;">ChainPulse Quant</div>
    <h1 style="margin:16px 0 8px;font-size:26px;">
      Confirm Your Subscription
    </h1>
    <p style="color:#9ca3af;font-size:15px;line-height:1.6;">
      You're one click away from receiving your Daily Regime Brief.
    </p>
    <div style="margin:30px 0;">
      <a href="{confirmation_link}"
         style="background:#fff;color:#000;padding:14px 28px;
                border-radius:14px;text-decoration:none;
                font-weight:600;display:inline-block;">
        Confirm Subscription
      </a>
    </div>
  </div>
</div>
"""
    try:
        send_email(email, "Confirm your Daily Regime Brief", html)
    except Exception as e:
        return {"status": "registered", "email_sent": False}
    return {"status": "confirmation_sent", "email_sent": True}


@router.get("/confirm")
def confirm(email: str, db: Session = Depends(get_db)):
    email = email.strip().lower()
    user = db.query(User).filter(User.email == email).first()
    if not user:
        raise HTTPException(404, detail="Email not found")
    user.alerts_enabled = True
    db.commit()
    return HTMLResponse(content=f"""
<html>
<head><title>Subscription Confirmed</title>
<style>
  body {{
    background-color:#000;color:#fff;
    font-family:-apple-system,BlinkMacSystemFont,sans-serif;
    display:flex;justify-content:center;align-items:center;
    height:100vh;margin:0;
  }}
  .card {{
    background:rgba(255,255,255,0.05);
    border:1px solid rgba(255,255,255,0.1);
    padding:50px;border-radius:24px;text-align:center;
    backdrop-filter:blur(12px);
    box-shadow:0 20px 60px rgba(0,0,0,0.6);
  }}
  .btn {{
    display:inline-block;margin-top:25px;padding:14px 28px;
    background:white;color:black;border-radius:14px;
    text-decoration:none;font-weight:600;
  }}
</style>
</head>
<body>
  <div class="card">
    <h1>? Subscription Confirmed</h1>
    <p>Your Daily Regime Brief is now active.</p>
    <a href="[chainpulse.pro](https://chainpulse.pro/app)" class="btn">Go to Dashboard</a>
  </div>
</body>
</html>
""")


@router.post("/restore-access")
def restore_access(
    request: Request,
    db: Session = Depends(get_db),
):
    from app.utils.schemas import RestoreRequest
    from app.services.emails import send_email, welcome_email_html
    import uuid
    rate_limiter.require(request, max_requests=3, window_seconds=3600)
    import pydantic
    try:
        body = RestoreRequest.model_validate(request)
    except Exception:
        raise HTTPException(400, detail="Invalid request body")
    email = body.email.strip().lower()
    user = db.query(User).filter(User.email == email).first()
    if not user or user.subscription_status != "active":
        raise HTTPException(
            404, detail="No active Pro subscription found"
        )
    user.access_token = str(uuid.uuid4())
    user.token_created_at = datetime.datetime.utcnow()
    db.commit()
    send_email(
        email,
        "ChainPulse Pro - Your Login Link",
        welcome_email_html(email, user.access_token),
    )
    return {"status": "sent"}


