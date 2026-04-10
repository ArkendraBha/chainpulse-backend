import time
from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.security import get_auth_header
from app.core.rate_limit import rate_limiter
from app.core.cache import get_or_compute, cache_get, cache_set
from app.db.database import get_db
from app.auth.auth import (
    require_tier,
    require_email_ownership,
    update_last_active,
)
from app.services.market_data import (
    build_regime_stack,
    compute_market_breadth,
    compute_regime_quality,
    regime_confidence_score,
    regime_transition_matrix,
    volatility_environment,
    build_correlation_matrix,
    regime_durations,
    current_age,
    average_regime_duration,
    trend_maturity_score,
    percentile_rank,
    portfolio_allocation,
    compute_decision_score,
)
from app.services.regime_engine import (
    compute_setup_quality,
    compute_opportunity_ranking,
    find_historical_analogs,
    compute_scenarios,
    compute_internal_damage,
    compute_behavioral_alpha_report,
    compute_event_risk_overlay,
    apply_archetype_overlay,
    compute_what_changed,
    compute_if_nothing_panel,
)
from app.core.config import settings

router = APIRouter()


@router.get("/survival-curve")
def survival_curve(
    request: Request,
    coin: str = "BTC",
    timeframe: str = "1h",
    db: Session = Depends(get_db),
):
    auth = get_auth_header(request)
    require_tier(auth, db, minimum_tier="essential")

    cache_key = f"survival:{coin}:{timeframe}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    durations = regime_durations(db, coin, timeframe)
    if len(durations) < 5:
        return {
            "data": [
                {
                    "hour": h,
                    "survival": max(0, 100 - h * 4),
                    "hazard": min(100, h * 4.5),
                }
                for h in range(25)
            ],
            "source": "estimated",
        }

    max_dur = int(max(durations))
    curve = []
    for hour in range(max_dur + 1):
        survivors = [d for d in durations if d > hour]
        surv_pct = (len(survivors) / len(durations)) * 100
        hz = 0.0
        if hour > 0 and survivors:
            exited = [d for d in durations if hour - 1 < d <= hour]
            hz = (len(exited) / len(survivors)) * 100
        curve.append({
            "hour": hour,
            "survival": round(surv_pct, 2),
            "hazard": round(hz, 2),
        })

    response = {"data": curve, "source": "historical"}
    cache_set(cache_key, response, ttl=300)
    return response


@router.get("/regime-transitions")
def regime_transitions(
    request: Request,
    coin: str = "BTC",
    timeframe: str = "1h",
    db: Session = Depends(get_db),
):
    if coin not in settings.SUPPORTED_COINS:
        raise HTTPException(400, detail="Unsupported coin")
    auth = get_auth_header(request)
    require_tier(auth, db, minimum_tier="essential")

    result = get_or_compute(
        f"transitions:{coin}:{timeframe}",
        regime_transition_matrix,
        ttl=300,
        db=db,
        coin=coin,
        timeframe=timeframe,
    )
    if result is None:
        return {
            "current_state": "Insufficient data",
            "transitions": {},
            "data_sufficient": False,
        }
    return result


@router.get("/volatility-environment")
async def volatility_env(
    request: Request,
    coin: str = "BTC",
    db: Session = Depends(get_db),
):
    if coin not in settings.SUPPORTED_COINS:
        raise HTTPException(400, detail="Unsupported coin")
    auth = get_auth_header(request)
    require_tier(auth, db, minimum_tier="essential")
    result = await volatility_environment(coin, db)
    if result is None:
        return {"error": "Insufficient data"}
    return result


@router.get("/correlation")
@router.get("/correlation-matrix")
async def correlation_endpoint(
    request: Request,
    coins: str = "BTC,ETH,SOL",
    db: Session = Depends(get_db),
):
    rate_limiter.require(request, max_requests=10, window_seconds=60)
    auth = get_auth_header(request)
    require_tier(auth, db, minimum_tier="essential")
    coin_list = [c.strip().upper() for c in coins.split(",") if c.strip()]
    sorted_key = ",".join(sorted(coin_list))
    cached = cache_get(f"correlation:{sorted_key}")
    if cached:
        return cached
    result = await build_correlation_matrix(coin_list)
    cache_set(f"correlation:{sorted_key}", result, ttl=300)
    return result


@router.get("/regime-confidence")
def regime_confidence_endpoint(
    request: Request,
    coin: str = "BTC",
    db: Session = Depends(get_db),
):
    if coin not in settings.SUPPORTED_COINS:
        raise HTTPException(400, detail="Unsupported coin")
    auth = get_auth_header(request)
    require_tier(auth, db, minimum_tier="essential")

    stack = build_regime_stack(coin, db)
    breadth = compute_market_breadth(db)
    if stack["incomplete"]:
        return {"error": "Insufficient regime data"}

    survival_val = stack.get("survival") or 50.0
    coherence_val = (
        stack["execution"]["coherence"]
        if stack.get("execution") and stack["execution"].get("coherence")
        else 0.0
    )
    confidence = regime_confidence_score(
        alignment=stack["alignment"] or 0,
        survival=survival_val,
        coherence=coherence_val,
        breadth_score=breadth.get("breadth_score", 0),
    )
    return {**confidence, "coin": coin}


@router.get("/regime-quality")
def regime_quality_endpoint(
    request: Request,
    coin: str = "BTC",
    db: Session = Depends(get_db),
):
    if coin not in settings.SUPPORTED_COINS:
        raise HTTPException(400, detail="Unsupported coin")
    auth = get_auth_header(request)
    require_tier(auth, db, minimum_tier="essential")
    update_last_active(request, db)

    stack = get_or_compute(
        f"stack:{coin}", build_regime_stack, ttl=60, coin=coin, db=db
    )
    if stack["incomplete"]:
        return {"error": "Insufficient data"}
    quality = compute_regime_quality(stack)
    return {
        **quality,
        "coin": coin,
        "regime": (
            stack["execution"]["label"]
            if stack.get("execution") else "Neutral"
        ),
        "exposure": stack.get("exposure"),
        "shift_risk": stack.get("shift_risk"),
        "hazard": stack.get("hazard"),
        "survival": stack.get("survival"),
    }


@router.post("/portfolio-allocator")
def portfolio_allocator_endpoint(
    request: Request,
    account_size: float = 10000,
    strategy_mode: str = "balanced",
    coin: str = "BTC",
    db: Session = Depends(get_db),
):
    if coin not in settings.SUPPORTED_COINS:
        raise HTTPException(400, detail="Unsupported coin")
    if strategy_mode not in ("conservative", "balanced", "aggressive"):
        raise HTTPException(400, detail="Invalid strategy mode")
    if account_size <= 0:
        raise HTTPException(400, detail="Invalid account size")
    auth = get_auth_header(request)
    require_tier(auth, db, minimum_tier="essential")
    update_last_active(request, db)

    stack = build_regime_stack(coin, db)
    if stack["incomplete"]:
        return {"error": "Insufficient data"}
    breadth = compute_market_breadth(db)
    survival_v = stack.get("survival") or 50.0
    coherence_v = (
        stack["execution"]["coherence"]
        if stack.get("execution") and stack["execution"].get("coherence")
        else 0.0
    )
    confidence = regime_confidence_score(
        alignment=stack["alignment"] or 0,
        survival=survival_v,
        coherence=coherence_v,
        breadth_score=breadth.get("breadth_score", 0),
    )
    allocation = portfolio_allocation(
        account_size=account_size,
        exposure_pct=stack["exposure"] or 5,
        confidence_score=confidence["score"],
        strategy_mode=strategy_mode,
    )
    return {
        **allocation,
        "regime": (
            stack["execution"]["label"]
            if stack.get("execution") else "-"
        ),
        "confidence": confidence["score"],
        "alignment": stack["alignment"],
    }


@router.get("/decision-engine")
def decision_engine_endpoint(
    request: Request,
    coin: str = "BTC",
    db: Session = Depends(get_db),
):
    if coin not in settings.SUPPORTED_COINS:
        raise HTTPException(400, detail="Unsupported coin")
    auth = get_auth_header(request)
    require_tier(auth, db, minimum_tier="essential")
    update_last_active(request, db)

    cache_key = f"decision:{coin}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    stack = build_regime_stack(coin, db)
    breadth = compute_market_breadth(db)
    if stack["incomplete"]:
        return {"error": "Insufficient data"}

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
    exec_label = (
        stack["execution"]["label"]
        if stack.get("execution") else "Neutral"
    )
    decision["regime"] = exec_label
    decision["exposure"] = stack.get("exposure", 50)
    decision["coin"] = coin
    decision["model_version"] = settings.MODEL_VERSION
    cache_set(cache_key, decision, ttl=60)
    return decision


@router.post("/if-nothing-panel")
def if_nothing_panel_endpoint(
    request: Request,
    coin: str = "BTC",
    user_exposure: float = 50.0,
    db: Session = Depends(get_db),
):
    if coin not in settings.SUPPORTED_COINS:
        raise HTTPException(400, detail="Unsupported coin")
    auth = get_auth_header(request)
    require_tier(auth, db, minimum_tier="essential")
    stack = build_regime_stack(coin, db)
    if stack["incomplete"]:
        return {"error": "Insufficient data"}
    exec_label = (
        stack["execution"]["label"]
        if stack.get("execution") else "Neutral"
    )
    return compute_if_nothing_panel(
        user_exposure=user_exposure,
        model_exposure=stack.get("exposure") or 50,
        hazard=stack.get("hazard") or 0,
        shift_risk=stack.get("shift_risk") or 0,
        regime_label=exec_label,
    )


@router.get("/setup-quality")
async def setup_quality_endpoint(
    request: Request,
    coin: str = "BTC",
    db: Session = Depends(get_db),
):
    rate_limiter.require(request, max_requests=20, window_seconds=60)
    if coin not in settings.SUPPORTED_COINS:
        raise HTTPException(400, detail="Unsupported coin")
    auth = get_auth_header(request)
    require_tier(auth, db, minimum_tier="pro")
    update_last_active(request, db)
    cached = cache_get(f"setup_quality:{coin}")
    if cached:
        return cached
    result = await compute_setup_quality(coin, db)
    if result.get("setup_quality_score") is not None:
        cache_set(f"setup_quality:{coin}", result, ttl=120)
    return result


@router.get("/opportunity-ranking")
async def opportunity_ranking_endpoint(
    request: Request,
    db: Session = Depends(get_db),
):
    rate_limiter.require(request, max_requests=10, window_seconds=60)
    auth = get_auth_header(request)
    require_tier(auth, db, minimum_tier="pro")
    update_last_active(request, db)
    cached = cache_get("opportunity_ranking")
    if cached:
        return cached
    result = await compute_opportunity_ranking(db)
    cache_set("opportunity_ranking", result, ttl=180)
    return result


@router.get("/historical-analogs")
async def historical_analogs_endpoint(
    request: Request,
    coin: str = "BTC",
    db: Session = Depends(get_db),
):
    rate_limiter.require(request, max_requests=10, window_seconds=60)
    if coin not in settings.SUPPORTED_COINS:
        raise HTTPException(400, detail="Unsupported coin")
    auth = get_auth_header(request)
    require_tier(auth, db, minimum_tier="pro")
    update_last_active(request, db)

    cache_key = f"analogs:{coin}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    stack = build_regime_stack(coin, db)
    if stack["incomplete"]:
        return {"coin": coin, "error": "Insufficient regime data"}

    macro_label = (
        stack["macro"]["label"] if stack.get("macro") else "Neutral"
    )
    trend_label = (
        stack["trend"]["label"] if stack.get("trend") else "Neutral"
    )
    exec_label = (
        stack["execution"]["label"]
        if stack.get("execution") else "Neutral"
    )
    hazard = stack.get("hazard") or 50

    result = await find_historical_analogs(
        db=db,
        coin=coin,
        target_macro=macro_label,
        target_trend=trend_label,
        target_exec=exec_label,
        target_hazard=hazard,
    )
    if result.get("data_sufficient"):
        cache_set(cache_key, result, ttl=300)
    return result


@router.get("/scenarios")
async def scenarios_endpoint(
    request: Request,
    coin: str = "BTC",
    db: Session = Depends(get_db),
):
    rate_limiter.require(request, max_requests=20, window_seconds=60)
    if coin not in settings.SUPPORTED_COINS:
        raise HTTPException(400, detail="Unsupported coin")
    auth = get_auth_header(request)
    require_tier(auth, db, minimum_tier="pro")
    update_last_active(request, db)
    cached = cache_get(f"scenarios:{coin}")
    if cached:
        return cached
    result = await compute_scenarios(coin, db)
    cache_set(f"scenarios:{coin}", result, ttl=120)
    return result


@router.get("/internal-damage")
async def internal_damage_endpoint(
    request: Request,
    coin: str = "BTC",
    db: Session = Depends(get_db),
):
    rate_limiter.require(request, max_requests=20, window_seconds=60)
    if coin not in settings.SUPPORTED_COINS:
        raise HTTPException(400, detail="Unsupported coin")
    auth = get_auth_header(request)
    require_tier(auth, db, minimum_tier="pro")
    update_last_active(request, db)
    cached = cache_get(f"damage:{coin}")
    if cached:
        return cached
    result = await compute_internal_damage(coin, db)
    cache_set(f"damage:{coin}", result, ttl=120)
    return result


@router.get("/event-risk-overlay")
def event_risk_overlay_endpoint(
    request: Request,
    coin: str = "BTC",
    db: Session = Depends(get_db),
):
    if coin not in settings.SUPPORTED_COINS:
        raise HTTPException(400, detail="Unsupported coin")
    auth = get_auth_header(request)
    require_tier(auth, db, minimum_tier="pro")
    update_last_active(request, db)
    cached = cache_get(f"event_risk:{coin}")
    if cached:
        return cached
    result = compute_event_risk_overlay(coin, db)
    cache_set(f"event_risk:{coin}", result, ttl=300)
    return result


@router.get("/what-changed")
def what_changed_endpoint(
    request: Request,
    lookback_hours: int = 24,
    db: Session = Depends(get_db),
):
    from app.services.regime_engine import get_or_compute_brief

    auth = get_auth_header(request)
    require_tier(auth, db, minimum_tier="pro")
    update_last_active(request, db)
    lookback_hours = min(max(1, lookback_hours), 168)

    # Try in-memory cache first (fast)
    cached = cache_get(f"what_changed:{lookback_hours}")
    if cached:
        return cached

    # Try DB cache second (survives restarts)
    from app.services.regime_engine import (
        get_or_compute_brief,
        compute_what_changed,
    )
    result = get_or_compute_brief(
        db=db,
        brief_type=f"what_changed_{lookback_hours}h",
        compute_fn=compute_what_changed,
        max_age_minutes=60,
        db=db,
        lookback_hours=lookback_hours,
    )

    # Save to in-memory cache too
    cache_set(f"what_changed:{lookback_hours}", result, ttl=120)
    return result



@router.get("/archetype-overlay")
def archetype_overlay_endpoint(
    request: Request,
    coin: str = "BTC",
    archetype: str = "swing",
    email: str = "",
    db: Session = Depends(get_db),
):
    from app.utils.enums import ARCHETYPE_CONFIG
    if coin not in settings.SUPPORTED_COINS:
        raise HTTPException(400, detail="Unsupported coin")
    if archetype not in ARCHETYPE_CONFIG:
        raise HTTPException(
            400,
            detail=f"Invalid archetype. Choose from: {list(ARCHETYPE_CONFIG.keys())}",
        )
    auth = get_auth_header(request)
    require_tier(auth, db, minimum_tier="institutional")
    update_last_active(request, db)
    return apply_archetype_overlay(
        coin=coin,
        archetype=archetype,
        db=db,
        email=email.strip().lower() if email else None,
    )


