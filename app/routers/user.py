import datetime
from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.security import get_auth_header
from app.db.database import get_db
from app.db.models import User, UserProfile
from app.auth.auth import require_tier, require_email_ownership
from app.services.market_data import build_regime_stack
from app.utils.schemas import UserProfileRequest, TraderArchetype
from app.utils.enums import ARCHETYPE_CONFIG

router = APIRouter()


@router.post("/user-profile")
def save_user_profile(
    request: Request,
    body: UserProfileRequest,
    db: Session = Depends(get_db),
):
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="essential")
    email = require_email_ownership(user_info, body.email)

    mult_map = {
        "conservative": 0.70,
        "balanced": 1.00,
        "aggressive": 1.25,
    }
    risk_mult = mult_map.get(body.risk_identity, 1.0)
    user = db.query(User).filter(User.email == email).first()
    user_id = user.id if user else None
    profile = (
        db.query(UserProfile)
        .filter(UserProfile.email == email)
        .first()
    )
    if not profile:
        profile = UserProfile(email=email, user_id=user_id)
        db.add(profile)
    profile.user_id = user_id
    profile.max_drawdown_pct = body.max_drawdown_pct
    profile.typical_leverage = body.typical_leverage
    profile.holding_period_days = body.holding_period_days
    profile.risk_identity = body.risk_identity
    profile.risk_multiplier = risk_mult
    profile.updated_at = datetime.datetime.utcnow()
    db.commit()
    return {
        "status": "saved",
        "email": email,
        "risk_multiplier": risk_mult,
        "profile": {
            "max_drawdown_pct": profile.max_drawdown_pct,
            "typical_leverage": profile.typical_leverage,
            "holding_period_days": profile.holding_period_days,
            "risk_identity": profile.risk_identity,
        },
    }


@router.get("/user-profile")
def get_user_profile(
    request: Request,
    email: str,
    coin: str = "BTC",
    db: Session = Depends(get_db),
):
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="essential")
    email = require_email_ownership(user_info, email)
    profile = (
        db.query(UserProfile)
        .filter(UserProfile.email == email)
        .first()
    )
    if not profile:
        return {
            "exists": False,
            "message": "No profile found. Complete onboarding to personalise.",
        }
    stack = build_regime_stack(coin, db)
    personalised_exposure = None
    if not stack["incomplete"] and stack.get("exposure"):
        personalised_exposure = round(
            min(95, max(5, stack["exposure"] * profile.risk_multiplier)), 1
        )
    return {
        "exists": True,
        "email": email,
        "risk_identity": profile.risk_identity,
        "risk_multiplier": profile.risk_multiplier,
        "max_drawdown_pct": profile.max_drawdown_pct,
        "typical_leverage": profile.typical_leverage,
        "holding_period_days": profile.holding_period_days,
        "personalised_exposure": personalised_exposure,
        "model_exposure": (
            stack.get("exposure") if not stack.get("incomplete") else None
        ),
        "created_at": profile.created_at,
    }


@router.post("/save-archetype")
def save_archetype_endpoint(
    request: Request,
    body: TraderArchetype,
    db: Session = Depends(get_db),
):
    if body.archetype not in ARCHETYPE_CONFIG:
        raise HTTPException(
            400,
            detail=f"Invalid archetype. Choose from: {list(ARCHETYPE_CONFIG.keys())}",
        )
    user_info = require_tier(
        get_auth_header(request), db, minimum_tier="essential"
    )
    email = require_email_ownership(user_info, body.email)
    config = ARCHETYPE_CONFIG[body.archetype]
    profile = (
        db.query(UserProfile)
        .filter(UserProfile.email == email)
        .first()
    )
    if not profile:
        user = db.query(User).filter(User.email == email).first()
        profile = UserProfile(
            email=email, user_id=user.id if user else None
        )
        db.add(profile)
    profile.risk_identity = body.archetype
    profile.risk_multiplier = config["exposure_mult"]
    profile.holding_period_days = config["max_hold_days"]
    profile.updated_at = datetime.datetime.utcnow()
    db.commit()
    return {
        "status": "saved",
        "email": email,
        "archetype": body.archetype,
        "archetype_label": config["label"],
        "exposure_multiplier": config["exposure_mult"],
        "max_hold_days": config["max_hold_days"],
        "alert_sensitivity": config["alert_sensitivity"],
    }


