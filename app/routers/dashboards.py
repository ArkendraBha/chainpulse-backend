import time
import datetime
from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.security import get_auth_header
from app.core.rate_limit import rate_limiter
from app.core.cache import cache_get, cache_set
from app.db.database import get_db
from app.db.models import MarketSummary, ExposureLog, UserProfile
from app.auth.auth import (
    resolve_pro_status,
    resolve_user_tier,
    require_tier,
    require_email_ownership,
    update_last_active,
)
from app.services.market_data import (
    build_regime_stack,
    fetch_all_market_data,
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
    compute_decision_score,
)
from app.services.regime_engine import (
    compute_setup_quality,
    compute_scenarios,
    compute_internal_damage,
    compute_event_risk_overlay,
    compute_behavioral_alpha_report,
    compute_discipline_score,
)
from app.services.alerts import evaluate_dynamic_alerts
from app.utils.enums import PLAYBOOK_DATA, RISK_EVENTS

router = APIRouter()


@router.get("/dashboard")
async def dashboard(
    request: Request,
    coin: str = "BTC",
    db: Session = Depends(get_db),
):
    rate_limiter.require(request, max_requests=30, window_seconds=60)
    if coin not in settings.SUPPORTED_COINS:
        raise HTTPException(400, detail="Unsupported coin")

    authorization = get_auth_header(request)
    user_info = resolve_user_tier(authorization, db)
    is_pro = user_info["is_pro"]
    tier = user_info["tier"]

    # Core stack
    stack = build_regime_stack(coin, db)
    exec_label = (
        stack["execution"]["label"]
        if not stack.get("incomplete") and stack.get("execution")
        else "Neutral"
    )

    # Latest record
    latest_r = (
        db.query(MarketSummary)
        .filter(
            MarketSummary.coin == coin,
            MarketSummary.timeframe == "1h",
        )
        .order_by(MarketSummary.created_at.desc())
        .first()
    )
    latest_data = (
        {
            "coin": latest_r.coin,
            "score": latest_r.score,
            "label": latest_r.label,
            "coherence": latest_r.coherence,
            "timestamp": latest_r.created_at,
        }
        if latest_r
        else {"message": "No data yet."}
    )

    # History
    records = (
        db.query(MarketSummary)
        .filter(
            MarketSummary.coin == coin,
            MarketSummary.timeframe == "1h",
        )
        .order_by(MarketSummary.created_at.desc())
        .limit(48)
        .all()
    )
    records.reverse()
    history_data = [
        {
            "hour": i,
            "score": r.score,
            "label": r.label,
            "coherence": r.coherence,
            "timestamp": r.created_at,
        }
        for i, r in enumerate(records)
    ]

    # Overview
    from app.services.market_data import build_regime_stack_bulk

    overview_list = []
    breadth = compute_market_breadth(db)
    all_stacks = build_regime_stack_bulk(settings.SUPPORTED_COINS, db)
    for c in settings.SUPPORTED_COINS:
        s = all_stacks.get(c, {"incomplete": True, "coin": c})
        if s.get("incomplete"):
            continue

        if is_pro:
            overview_list.append(
                {
                    "coin": s["coin"],
                    "macro": s["macro"]["label"] if s["macro"] else None,
                    "trend": s["trend"]["label"] if s["trend"] else None,
                    "execution": s["execution"]["label"] if s["execution"] else None,
                    "alignment": s["alignment"],
                    "direction": s["direction"],
                    "exposure": s["exposure"],
                    "shift_risk": s["shift_risk"],
                }
            )
        else:
            overview_list.append(
                {
                    "coin": s["coin"],
                    "execution": s["execution"]["label"] if s["execution"] else None,
                    "direction": s["direction"],
                    "pro_required": True,
                }
            )

    curve_data = None
    transitions_data = None
    vol_env_data = None
    correlation_data = None
    confidence_data = None

    if is_pro:
        durations = regime_durations(db, coin, "1h")
        if len(durations) >= 5:
            max_dur = int(max(durations))
            curve = []
            for hour in range(max_dur + 1):
                survivors = [d for d in durations if d > hour]
                surv_pct = (len(survivors) / len(durations)) * 100
                hz = 0.0
                if hour > 0 and survivors:
                    exited = [d for d in durations if hour - 1 < d <= hour]
                    hz = (len(exited) / len(survivors)) * 100
                curve.append(
                    {
                        "hour": hour,
                        "survival": round(surv_pct, 2),
                        "hazard": round(hz, 2),
                    }
                )
            curve_data = {"data": curve, "source": "historical"}

        try:
            transitions_data = regime_transition_matrix(db, coin, "1h")
        except Exception:
            transitions_data = None
        try:
            vol_env_data = await volatility_environment(coin, db)
        except Exception:
            vol_env_data = None
        try:
            correlation_data = await build_correlation_matrix(settings.SUPPORTED_COINS)
        except Exception:
            correlation_data = None
        try:
            survival_v = stack.get("survival") or 50.0
            coherence_v = (
                stack["execution"]["coherence"]
                if stack.get("execution") and stack["execution"].get("coherence")
                else 50.0
            )
            confidence_data = regime_confidence_score(
                alignment=stack.get("alignment") or 0,
                survival=survival_v,
                coherence=coherence_v,
                breadth_score=breadth.get("breadth_score", 0),
            )
        except Exception:
            confidence_data = None

    return {
        "stack": stack,
        "pro_required": not is_pro,
        "tier": tier,
        "latest": latest_data,
        "history": history_data,
        "overview": overview_list,
        "breadth": (
            breadth
            if is_pro
            else {
                "total": breadth.get("total", 0),
                "sentiment": (
                    "Bullish"
                    if breadth.get("breadth_score", 0) > 30
                    else (
                        "Bearish"
                        if breadth.get("breadth_score", 0) < -30
                        else "Neutral"
                    )
                ),
                "pro_required": True,
            }
        ),
        "confidence": confidence_data,
        "volEnv": vol_env_data,
        "transitions": transitions_data,
        "correlation": correlation_data,
        "curve": curve_data.get("data") if curve_data else [],
        "events": RISK_EVENTS,
    }


@router.get("/premium-dashboard")
async def premium_dashboard(
    request: Request,
    coin: str = "BTC",
    email: str = "",
    db: Session = Depends(get_db),
):
    rate_limiter.require(request, max_requests=30, window_seconds=60)
    if coin not in settings.SUPPORTED_COINS:
        raise HTTPException(400, detail="Unsupported coin")
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="essential")
    tier = user_info["tier"]
    update_last_active(request, db)

    start = time.perf_counter()
    if email:
        email = require_email_ownership(user_info, email)

    market_data = await fetch_all_market_data(coin)
    stack = build_regime_stack(coin, db)

    try:
        setup = await compute_setup_quality(
            coin, db, market_data=market_data, stack=stack
        )
    except Exception:
        setup = {"setup_quality_score": None, "error": "Failed"}

    quality = compute_regime_quality(stack) if not stack.get("incomplete") else None

    age_1h = current_age(db, coin, "1h")
    avg_dur = average_regime_duration(db, coin, "1h")
    hazard_val = stack.get("hazard") or 0
    maturity = trend_maturity_score(age_1h, avg_dur, hazard_val)
    exec_score = stack["execution"]["score"] if stack.get("execution") else 0
    pct_rank = percentile_rank(db, coin, exec_score, "1h")

    breadth = compute_market_breadth(db)
    decision = None
    if not stack.get("incomplete"):
        try:
            decision = compute_decision_score(
                hazard=hazard_val,
                shift_risk=stack.get("shift_risk") or 0,
                alignment=stack.get("alignment") or 0,
                survival=stack.get("survival") or 50,
                breadth_score=breadth.get("breadth_score", 0),
                maturity_pct=maturity,
            )
            exec_label = (
                stack["execution"]["label"] if stack.get("execution") else "Neutral"
            )
            decision["regime"] = exec_label
            decision["exposure"] = stack.get("exposure", 50)
            decision["coin"] = coin
            decision["model_version"] = settings.MODEL_VERSION
        except Exception:
            decision = None

    try:
        scenarios = await compute_scenarios(coin, db, stack=stack, setup=setup)
    except Exception:
        scenarios = None

    try:
        damage = await compute_internal_damage(
            coin, db, market_data=market_data, stack=stack
        )
    except Exception:
        damage = None

    try:
        event_risk = compute_event_risk_overlay(coin, db, stack=stack)
    except Exception:
        event_risk = None

    # AI Narrative
    try:
        from app.services.ai_narrative import generate_regime_narrative

        narrative = await generate_regime_narrative(
            coin, stack, setup, scenarios, damage
        )
    except Exception:
        narrative = {"available": False}

    try:
        durations = regime_durations(db, coin, "1h")
        if len(durations) >= 5:
            max_dur = int(max(durations))
            curve = []
            for hour in range(max_dur + 1):
                survivors = [d for d in durations if d > hour]
                surv_pct = (len(survivors) / len(durations)) * 100
                hz = 0.0
                if hour > 0 and survivors:
                    exited = [d for d in durations if hour - 1 < d <= hour]
                    hz = (len(exited) / len(survivors)) * 100
                curve.append(
                    {
                        "hour": hour,
                        "survival": round(surv_pct, 2),
                        "hazard": round(hz, 2),
                    }
                )
            survival_data = {"data": curve, "source": "historical"}
        else:
            survival_data = {
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
    except Exception:
        survival_data = {"data": [], "source": "error"}

    try:
        transitions = regime_transition_matrix(db, coin, "1h")
    except Exception:
        transitions = None

    try:
        vol_env = await volatility_environment(coin, db, market_data=market_data)
    except Exception:
        vol_env = None

    exec_label = (
        stack["execution"]["label"]
        if not stack.get("incomplete") and stack.get("execution")
        else "Neutral"
    )
    pb = PLAYBOOK_DATA.get(exec_label, PLAYBOOK_DATA["Neutral"])

    records = (
        db.query(MarketSummary)
        .filter(
            MarketSummary.coin == coin,
            MarketSummary.timeframe == "1h",
        )
        .order_by(MarketSummary.created_at.desc())
        .limit(48)
        .all()
    )
    records.reverse()
    history = [
        {
            "hour": i,
            "score": r.score,
            "label": r.label,
            "coherence": r.coherence,
            "timestamp": r.created_at,
        }
        for i, r in enumerate(records)
    ]

    discipline = None
    behavioral = None
    user_alerts = None

    if email:
        try:
            logs = (
                db.query(ExposureLog)
                .filter(ExposureLog.email == email)
                .order_by(ExposureLog.created_at.desc())
                .limit(30)
                .all()
            )
            discipline = compute_discipline_score(logs)
        except Exception:
            discipline = None
        try:
            behavioral = compute_behavioral_alpha_report(email, db, 30)
        except Exception:
            behavioral = None
        try:
            user_alerts = await evaluate_dynamic_alerts(email, db)
        except Exception:
            user_alerts = None

    duration_ms = round((time.perf_counter() - start) * 1000, 2)

    return {
        "coin": coin,
        "ai_narrative": narrative,
        "stack": {
            "coin": stack["coin"],
            "macro": stack.get("macro"),
            "trend": stack.get("trend"),
            "execution": stack.get("execution"),
            "alignment": stack.get("alignment"),
            "direction": stack.get("direction"),
            "exposure": stack.get("exposure"),
            "shift_risk": stack.get("shift_risk"),
            "survival": stack.get("survival"),
            "hazard": stack.get("hazard"),
            "incomplete": stack.get("incomplete", False),
            "pro_required": False,
            "tier": tier,
        },
        "quality": quality,
        "setup": setup,
        "decision": decision,
        "scenarios": scenarios,
        "damage": damage,
        "event_risk": event_risk,
        "survival_curve": survival_data,
        "transitions": transitions,
        "volatility_env": vol_env,
        "playbook": {
            "regime": exec_label,
            "strategy_mode": pb["strategy_mode"],
            "exposure_band": pb["exposure_band"],
            "trend_follow_wr": pb["trend_follow_wr"],
            "mean_revert_wr": pb["mean_revert_wr"],
            "avg_remaining_days": pb["avg_remaining_days"],
            "data_source": pb.get("data_source", "backtested_estimates"),
            "actions": pb["actions"],
            "avoid": pb["avoid"],
        },
        "breadth": breadth,
        "history": history,
        "statistics": {
            "trend_maturity": maturity,
            "percentile": pct_rank,
            "regime_age_hours": round(age_1h, 2),
            "avg_regime_duration_hours": round(avg_dur, 2),
        },
        "discipline": discipline,
        "behavioral_alpha": behavioral,
        "user_alerts": user_alerts,
        "model_version": settings.MODEL_VERSION,
        "duration_ms": duration_ms,
    }


@router.get("/dashboard-v2")
async def dashboard_v2(
    request: Request,
    coin: str = "BTC",
    email: str = "",
    db: Session = Depends(get_db),
):
    rate_limiter.require(request, max_requests=30, window_seconds=60)
    if coin not in settings.SUPPORTED_COINS:
        raise HTTPException(400, detail="Unsupported coin")

    authorization = get_auth_header(request)
    user_info = resolve_user_tier(authorization, db)
    is_pro = user_info["is_pro"]
    tier = user_info["tier"]

    if email:
        email = require_email_ownership(user_info, email)

    stack = build_regime_stack(coin, db)
    exec_label = (
        stack["execution"]["label"]
        if not stack.get("incomplete") and stack.get("execution")
        else "Neutral"
    )

    latest_r = (
        db.query(MarketSummary)
        .filter(
            MarketSummary.coin == coin,
            MarketSummary.timeframe == "1h",
        )
        .order_by(MarketSummary.created_at.desc())
        .first()
    )
    latest_data = (
        {
            "coin": latest_r.coin,
            "score": latest_r.score,
            "label": latest_r.label,
            "coherence": latest_r.coherence,
            "timestamp": latest_r.created_at,
        }
        if latest_r
        else {"message": "No data yet."}
    )

    records = (
        db.query(MarketSummary)
        .filter(
            MarketSummary.coin == coin,
            MarketSummary.timeframe == "1h",
        )
        .order_by(MarketSummary.created_at.desc())
        .limit(48)
        .all()
    )
    records.reverse()
    history_data = [
        {
            "hour": i,
            "score": r.score,
            "label": r.label,
            "coherence": r.coherence,
            "timestamp": r.created_at,
        }
        for i, r in enumerate(records)
    ]

    from app.services.market_data import build_regime_stack_bulk

    overview_list = []
    breadth = compute_market_breadth(db)
    all_stacks = build_regime_stack_bulk(settings.SUPPORTED_COINS, db)
    for c in settings.SUPPORTED_COINS:
        s = all_stacks.get(c, {"incomplete": True, "coin": c})
        if s.get("incomplete"):
            continue

        if is_pro:
            overview_list.append(
                {
                    "coin": s["coin"],
                    "macro": s["macro"]["label"] if s["macro"] else None,
                    "trend": s["trend"]["label"] if s["trend"] else None,
                    "execution": s["execution"]["label"] if s["execution"] else None,
                    "alignment": s["alignment"],
                    "direction": s["direction"],
                    "exposure": s["exposure"],
                    "shift_risk": s["shift_risk"],
                }
            )
        else:
            overview_list.append(
                {
                    "coin": s["coin"],
                    "execution": s["execution"]["label"] if s["execution"] else None,
                    "direction": s["direction"],
                    "pro_required": True,
                }
            )

    result = {
        "stack": stack,
        "latest": latest_data,
        "history": history_data,
        "overview": overview_list,
        "breadth": breadth,
        "events": RISK_EVENTS,
        "is_pro": is_pro,
        "tier": tier,
    }

    if not is_pro:
        result["pro_features_available"] = [
            "setup_quality",
            "scenarios",
            "internal_damage",
            "event_risk",
            "trade_plan",
            "behavioral_alpha",
            "opportunity_ranking",
            "historical_analogs",
            "archetype_overlay",
            "what_changed",
            "dynamic_alerts",
            "premium_overview",
        ]
        return result

    update_last_active(request, db)
    market_data = await fetch_all_market_data(coin)

    try:
        result["setup_quality"] = await compute_setup_quality(
            coin, db, market_data=market_data, stack=stack
        )
    except Exception:
        result["setup_quality"] = None

    if not stack.get("incomplete"):
        try:
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
            decision["regime"] = exec_label
            decision["exposure"] = stack.get("exposure", 50)
            decision["coin"] = coin
            decision["model_version"] = settings.MODEL_VERSION
            result["decision"] = decision
        except Exception:
            result["decision"] = None
    else:
        result["decision"] = None

    try:
        result["scenarios"] = await compute_scenarios(
            coin,
            db,
            stack=stack,
            setup=result.get("setup_quality"),
        )
    except Exception:
        result["scenarios"] = None

    try:
        result["internal_damage"] = await compute_internal_damage(
            coin, db, market_data=market_data, stack=stack
        )
    except Exception:
        result["internal_damage"] = None

    try:
        result["event_risk"] = compute_event_risk_overlay(coin, db, stack=stack)
    except Exception:
        result["event_risk"] = None

    try:
        result["regime_quality"] = (
            compute_regime_quality(stack) if not stack.get("incomplete") else None
        )
    except Exception:
        result["regime_quality"] = None

    try:
        durations_list = regime_durations(db, coin, "1h")
        if len(durations_list) >= 5:
            max_d = int(max(durations_list))
            curve = []
            for hour in range(max_d + 1):
                survivors = [d for d in durations_list if d > hour]
                surv_pct = (len(survivors) / len(durations_list)) * 100
                hz = 0.0
                if hour > 0 and survivors:
                    exited = [d for d in durations_list if hour - 1 < d <= hour]
                    hz = (len(exited) / len(survivors)) * 100
                curve.append(
                    {
                        "hour": hour,
                        "survival": round(surv_pct, 2),
                        "hazard": round(hz, 2),
                    }
                )
            result["survival_curve"] = {"data": curve, "source": "historical"}
        else:
            result["survival_curve"] = {
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
    except Exception:
        result["survival_curve"] = {"data": [], "source": "error"}

    try:
        result["transitions"] = regime_transition_matrix(db, coin, "1h")
    except Exception:
        result["transitions"] = None

    try:
        result["volatility_env"] = await volatility_environment(
            coin, db, market_data=market_data
        )
    except Exception:
        result["volatility_env"] = None

    try:
        result["correlation"] = await build_correlation_matrix(
            settings.SUPPORTED_COINS[:5]
        )
    except Exception:
        result["correlation"] = None

    try:
        survival_val = stack.get("survival") or 50
        coherence_val = (
            stack["execution"]["coherence"]
            if stack.get("execution") and stack["execution"].get("coherence")
            else 50
        )
        result["confidence"] = regime_confidence_score(
            alignment=stack.get("alignment") or 0,
            survival=survival_val,
            coherence=coherence_val,
            breadth_score=breadth.get("breadth_score", 0),
        )
    except Exception:
        result["confidence"] = None

    try:
        pb = PLAYBOOK_DATA.get(exec_label, PLAYBOOK_DATA["Neutral"])
        result["playbook"] = {
            "regime": exec_label,
            "strategy_mode": pb["strategy_mode"],
            "exposure_band": pb["exposure_band"],
            "trend_follow_wr": pb["trend_follow_wr"],
            "mean_revert_wr": pb["mean_revert_wr"],
            "avg_remaining_days": pb["avg_remaining_days"],
            "data_source": pb.get("data_source", "backtested_estimates"),
            "actions": pb["actions"],
            "avoid": pb["avoid"],
        }
    except Exception:
        result["playbook"] = None

    if email:
        try:
            logs = (
                db.query(ExposureLog)
                .filter(ExposureLog.email == email)
                .order_by(ExposureLog.created_at.desc())
                .limit(30)
                .all()
            )
            result["discipline"] = compute_discipline_score(logs)
        except Exception:
            result["discipline"] = None
        try:
            result["behavioral_alpha"] = compute_behavioral_alpha_report(email, db, 30)
        except Exception:
            result["behavioral_alpha"] = None
        try:
            result["user_alerts"] = await evaluate_dynamic_alerts(email, db)
        except Exception:
            result["user_alerts"] = None
        try:
            profile = db.query(UserProfile).filter(UserProfile.email == email).first()
            result["user_profile"] = (
                {
                    "risk_identity": profile.risk_identity,
                    "risk_multiplier": profile.risk_multiplier,
                    "max_drawdown_pct": profile.max_drawdown_pct,
                    "holding_period_days": profile.holding_period_days,
                }
                if profile
                else None
            )
        except Exception:
            result["user_profile"] = None

    result["model_version"] = settings.MODEL_VERSION
    return result


@router.get("/premium-overview")
async def premium_overview(
    request: Request,
    db: Session = Depends(get_db),
):
    rate_limiter.require(request, max_requests=10, window_seconds=60)
    auth = get_auth_header(request)
    require_tier(auth, db, minimum_tier="pro")
    update_last_active(request, db)

    cached = cache_get("premium_overview")
    if cached:
        return cached

    from app.services.market_data import build_regime_stack_bulk

    coins_data = []
    all_stacks = build_regime_stack_bulk(settings.SUPPORTED_COINS, db)
    for coin in settings.SUPPORTED_COINS:
        stack = all_stacks.get(coin, {"incomplete": True, "coin": coin})
        if stack.get("incomplete"):
            continue

        quality = compute_regime_quality(stack)
        try:
            setup = await compute_setup_quality(coin, db, stack=stack)
            setup_score = setup.get("setup_quality_score")
            setup_label = setup.get("setup_label")
            entry_mode = setup.get("entry_mode")
            chase_risk = setup.get("chase_risk")
        except Exception:
            setup_score = setup_label = entry_mode = chase_risk = None

        coins_data.append(
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
                "setup_score": setup_score,
                "setup_label": setup_label,
                "entry_mode": entry_mode,
                "chase_risk": chase_risk,
            }
        )

    coins_data.sort(
        key=lambda x: (
            (x.get("setup_score") or 0) * 0.5 + (x.get("quality_score") or 0) * 0.5
        ),
        reverse=True,
    )
    breadth = compute_market_breadth(db)

    best_long = None
    avoid = []
    for c in coins_data:
        if c["direction"] == "bullish" and best_long is None:
            best_long = c["coin"]
        if (c.get("setup_score") or 0) < 30 or (c.get("chase_risk") or 0) > 80:
            avoid.append(c["coin"])

    result = {
        "coins": coins_data,
        "breadth": breadth,
        "best_long": best_long,
        "avoid": avoid,
        "coin_count": len(coins_data),
        "timestamp": datetime.datetime.utcnow().isoformat(),
    }
    cache_set("premium_overview", result, ttl=120)
    return result
