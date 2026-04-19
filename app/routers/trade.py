from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.security import get_auth_header
from app.core.rate_limit import rate_limiter
from app.db.database import get_db
from app.auth.auth import require_tier, require_email_ownership
from app.services.regime_engine import compute_trade_plan
from app.utils.schemas import TradePlanRequest
from app.utils.enums import ARCHETYPE_CONFIG

router = APIRouter()


@router.post("/trade-plan")
async def trade_plan_endpoint(
    request: Request,
    body: TradePlanRequest,
    db: Session = Depends(get_db),
):
    rate_limiter.require(request, max_requests=10, window_seconds=60)
    if body.coin not in settings.SUPPORTED_COINS:
        raise HTTPException(400, detail="Unsupported coin")
    if body.account_size <= 0:
        raise HTTPException(400, detail="Invalid account size")
    if body.strategy_mode not in ARCHETYPE_CONFIG:
        raise HTTPException(
            400,
            detail=f"Invalid strategy. Choose from: {list(ARCHETYPE_CONFIG.keys())}",
        )
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="pro")
    email = require_email_ownership(user_info, body.email)
    return await compute_trade_plan(
        coin=body.coin,
        account_size=body.account_size,
        strategy_mode=body.strategy_mode,
        db=db,
        email=email,
    )
