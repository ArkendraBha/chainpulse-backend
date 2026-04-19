import datetime
import secrets as secrets_mod
from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.security import get_auth_header
from app.db.database import get_db
from app.db.models import ApiKey, User
from app.auth.auth import require_tier, require_email_ownership
from app.auth.api_keys import require_api_key
from app.services.market_data import (
    build_regime_stack,
    compute_regime_quality,
    compute_market_breadth,
    current_age,
    average_regime_duration,
    trend_maturity_score,
    compute_decision_score,
)
from app.services.regime_engine import (
    compute_setup_quality,
    compute_scenarios,
    compute_internal_damage,
    compute_opportunity_ranking,
)
from app.utils.schemas import ApiKeyRequest

router = APIRouter()


@router.post("/api/v1/keys")
def create_api_key(
    body: ApiKeyRequest,
    request: Request,
    db: Session = Depends(get_db),
):
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="institutional")
    email = require_email_ownership(user_info, body.email)

    existing = (
        db.query(ApiKey)
        .filter(
            ApiKey.email == email,
            ApiKey.is_active == True,
        )
        .count()
    )
    if existing >= 3:
        raise HTTPException(400, detail="Maximum 3 active API keys per account")

    key = f"cp_live_{secrets_mod.token_hex(24)}"
    api_key = ApiKey(
        email=email,
        key=key,
        label=body.label,
        tier="institutional",
        daily_limit=1000,
    )
    db.add(api_key)
    db.commit()

    return {
        "api_key": key,
        "label": body.label,
        "daily_limit": 1000,
        "message": "Store this key securely. It won't be shown again.",
    }


@router.get("/api/v1/keys")
def list_api_keys(
    request: Request,
    email: str = "",
    db: Session = Depends(get_db),
):
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="institutional")
    email = require_email_ownership(user_info, email)

    keys = db.query(ApiKey).filter(ApiKey.email == email).all()
    return {
        "keys": [
            {
                "id": k.id,
                "label": k.label,
                "key_preview": f"{k.key[:8]}...{k.key[-4:]}",
                "is_active": k.is_active,
                "requests_today": k.requests_today,
                "daily_limit": k.daily_limit,
                "last_used_at": k.last_used_at,
                "created_at": k.created_at,
            }
            for k in keys
        ]
    }


@router.delete("/api/v1/keys/{key_id}")
def revoke_api_key(
    key_id: int,
    request: Request,
    email: str = "",
    db: Session = Depends(get_db),
):
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="institutional")
    email = require_email_ownership(user_info, email)

    key = (
        db.query(ApiKey)
        .filter(
            ApiKey.id == key_id,
            ApiKey.email == email,
        )
        .first()
    )
    if not key:
        raise HTTPException(404, detail="API key not found")

    key.is_active = False
    db.commit()
    return {"status": "revoked", "key_id": key_id}


@router.get("/api/v1/regime/{coin}")
async def api_regime(
    coin: str,
    request: Request,
    db: Session = Depends(get_db),
):
    api_info = require_api_key(request, db)
    coin = coin.upper()
    if coin not in settings.SUPPORTED_COINS:
        raise HTTPException(
            400,
            detail=f"Unsupported coin. Choose from: {settings.SUPPORTED_COINS}",
        )
    stack = build_regime_stack(coin, db)
    quality = compute_regime_quality(stack) if not stack.get("incomplete") else None
    return {
        "coin": coin,
        "stack": stack,
        "quality": quality,
        "api_requests_remaining": api_info["requests_remaining"],
        "timestamp": datetime.datetime.utcnow().isoformat(),
    }


@router.get("/api/v1/regime")
def api_regime_all(
    request: Request,
    db: Session = Depends(get_db),
):
    api_info = require_api_key(request, db)
    results = []
    for coin in settings.SUPPORTED_COINS:
        stack = build_regime_stack(coin, db)
        if stack.get("incomplete"):
            continue
        quality = compute_regime_quality(stack)
        results.append(
            {
                "coin": coin,
                "macro": stack["macro"]["label"] if stack.get("macro") else None,
                "trend": stack["trend"]["label"] if stack.get("trend") else None,
                "execution": (
                    stack["execution"]["label"] if stack.get("execution") else None
                ),
                "alignment": stack.get("alignment"),
                "direction": stack.get("direction"),
                "exposure": stack.get("exposure"),
                "shift_risk": stack.get("shift_risk"),
                "hazard": stack.get("hazard"),
                "survival": stack.get("survival"),
                "quality_grade": quality["grade"],
                "quality_score": quality["score"],
            }
        )
    return {
        "coins": results,
        "count": len(results),
        "api_requests_remaining": api_info["requests_remaining"],
        "timestamp": datetime.datetime.utcnow().isoformat(),
    }


@router.get("/api/v1/setup-quality/{coin}")
async def api_setup_quality(
    coin: str,
    request: Request,
    db: Session = Depends(get_db),
):
    api_info = require_api_key(request, db)
    coin = coin.upper()
    if coin not in settings.SUPPORTED_COINS:
        raise HTTPException(400, detail="Unsupported coin")
    setup = await compute_setup_quality(coin, db)
    return {
        **setup,
        "api_requests_remaining": api_info["requests_remaining"],
        "timestamp": datetime.datetime.utcnow().isoformat(),
    }


@router.get("/api/v1/scenarios/{coin}")
async def api_scenarios(
    coin: str,
    request: Request,
    db: Session = Depends(get_db),
):
    api_info = require_api_key(request, db)
    coin = coin.upper()
    if coin not in settings.SUPPORTED_COINS:
        raise HTTPException(400, detail="Unsupported coin")
    scenarios = await compute_scenarios(coin, db)
    return {
        **scenarios,
        "api_requests_remaining": api_info["requests_remaining"],
    }


@router.get("/api/v1/decision/{coin}")
async def api_decision(
    coin: str,
    request: Request,
    db: Session = Depends(get_db),
):
    api_info = require_api_key(request, db)
    coin = coin.upper()
    if coin not in settings.SUPPORTED_COINS:
        raise HTTPException(400, detail="Unsupported coin")

    stack = build_regime_stack(coin, db)
    if stack.get("incomplete"):
        return {"coin": coin, "error": "Insufficient data"}

    breadth = compute_market_breadth(db)
    hazard = stack.get("hazard") or 0
    age_1h = current_age(db, coin, "1h")
    avg_dur = average_regime_duration(db, coin, "1h")
    maturity = trend_maturity_score(age_1h, avg_dur, hazard)

    decision = compute_decision_score(
        hazard=hazard,
        shift_risk=stack.get("shift_risk") or 0,
        alignment=stack.get("alignment") or 0,
        survival=stack.get("survival") or 50,
        breadth_score=breadth.get("breadth_score", 0),
        maturity_pct=maturity,
    )
    exec_label = stack["execution"]["label"] if stack.get("execution") else "Neutral"
    decision["regime"] = exec_label
    decision["exposure"] = stack.get("exposure", 50)
    decision["coin"] = coin
    return {
        **decision,
        "api_requests_remaining": api_info["requests_remaining"],
        "timestamp": datetime.datetime.utcnow().isoformat(),
    }


@router.get("/api/v1/opportunity-ranking")
async def api_opportunity_ranking(
    request: Request,
    db: Session = Depends(get_db),
):
    api_info = require_api_key(request, db)
    ranking = await compute_opportunity_ranking(db)
    return {
        **ranking,
        "api_requests_remaining": api_info["requests_remaining"],
    }


@router.get("/api/v1/internal-damage/{coin}")
async def api_internal_damage(
    coin: str,
    request: Request,
    db: Session = Depends(get_db),
):
    api_info = require_api_key(request, db)
    coin = coin.upper()
    if coin not in settings.SUPPORTED_COINS:
        raise HTTPException(400, detail="Unsupported coin")
    damage = await compute_internal_damage(coin, db)
    return {
        **damage,
        "api_requests_remaining": api_info["requests_remaining"],
    }


@router.get("/api/v1/breadth")
def api_breadth(
    request: Request,
    db: Session = Depends(get_db),
):
    api_info = require_api_key(request, db)
    breadth = compute_market_breadth(db)
    return {
        **breadth,
        "api_requests_remaining": api_info["requests_remaining"],
        "timestamp": datetime.datetime.utcnow().isoformat(),
    }


@router.get("/api/v1/usage")
def api_usage(
    request: Request,
    db: Session = Depends(get_db),
):
    api_info = require_api_key(request, db)
    return {
        "email": api_info["email"],
        "requests_remaining": api_info["requests_remaining"],
        "daily_limit": 1000,
        "timestamp": datetime.datetime.utcnow().isoformat(),
    }


@router.post("/api/v1/regime-thresholds")
def set_custom_thresholds(
    request: Request,
    email: str,
    strong_risk_on_min: float = 35.0,
    risk_on_min: float = 15.0,
    risk_off_max: float = -15.0,
    strong_risk_off_max: float = -35.0,
    db: Session = Depends(get_db),
):
    """Set custom regime classification thresholds."""
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="institutional")
    email = require_email_ownership(user_info, email)

    from app.db.models import CustomRegimeThreshold

    existing = (
        db.query(CustomRegimeThreshold)
        .filter(CustomRegimeThreshold.email == email)
        .first()
    )

    if existing:
        existing.strong_risk_on_min = strong_risk_on_min
        existing.risk_on_min = risk_on_min
        existing.risk_off_max = risk_off_max
        existing.strong_risk_off_max = strong_risk_off_max
        existing.updated_at = datetime.datetime.utcnow()
    else:
        db.add(
            CustomRegimeThreshold(
                email=email,
                strong_risk_on_min=strong_risk_on_min,
                risk_on_min=risk_on_min,
                risk_off_max=risk_off_max,
                strong_risk_off_max=strong_risk_off_max,
            )
        )
    db.commit()

    return {
        "status": "saved",
        "email": email,
        "thresholds": {
            "strong_risk_on": f"score > {strong_risk_on_min}",
            "risk_on": f"score > {risk_on_min}",
            "neutral": f"{risk_off_max} to {risk_on_min}",
            "risk_off": f"score < {risk_off_max}",
            "strong_risk_off": f"score < {strong_risk_off_max}",
        },
    }


@router.get("/api/v1/regime-thresholds")
def get_custom_thresholds(
    request: Request,
    email: str,
    db: Session = Depends(get_db),
):
    """Get your custom regime thresholds."""
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="institutional")
    email = require_email_ownership(user_info, email)

    from app.db.models import CustomRegimeThreshold

    t = (
        db.query(CustomRegimeThreshold)
        .filter(CustomRegimeThreshold.email == email)
        .first()
    )

    if not t:
        return {
            "email": email,
            "using_defaults": True,
            "thresholds": {
                "strong_risk_on_min": 35.0,
                "risk_on_min": 15.0,
                "risk_off_max": -15.0,
                "strong_risk_off_max": -35.0,
            },
        }

    return {
        "email": email,
        "using_defaults": False,
        "thresholds": {
            "strong_risk_on_min": t.strong_risk_on_min,
            "risk_on_min": t.risk_on_min,
            "risk_off_max": t.risk_off_max,
            "strong_risk_off_max": t.strong_risk_off_max,
        },
    }
