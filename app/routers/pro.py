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
        curve.append(
            {
                "hour": hour,
                "survival": round(surv_pct, 2),
                "hazard": round(hz, 2),
            }
        )

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
            stack["execution"]["label"] if stack.get("execution") else "Neutral"
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
        "regime": (stack["execution"]["label"] if stack.get("execution") else "-"),
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
    exec_label = stack["execution"]["label"] if stack.get("execution") else "Neutral"
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
    exec_label = stack["execution"]["label"] if stack.get("execution") else "Neutral"
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

    macro_label = stack["macro"]["label"] if stack.get("macro") else "Neutral"
    trend_label = stack["trend"]["label"] if stack.get("trend") else "Neutral"
    exec_label = stack["execution"]["label"] if stack.get("execution") else "Neutral"
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


# app/services/backtester.py
import datetime
from typing import List, Optional
from sqlalchemy.orm import Session
from app.db.models import MarketSummary
from app.core.config import settings


def run_backtest(
    db: Session,
    coin: str,
    start_date: datetime.datetime,
    end_date: datetime.datetime,
    initial_capital: float = 10000,
    strategy: str = "follow_model",
    rebalance_frequency_hours: int = 4,
) -> dict:
    """
    Backtests a strategy against historical regime data.

    Strategies:
    - follow_model: Use ChainPulse recommended exposure
    - buy_and_hold: Always 100% exposed
    - risk_off_only: Reduce to 10% in Risk-Off, else 80%
    - momentum: Only hold in Strong Risk-On/Risk-On
    """
    records_1h = (
        db.query(MarketSummary)
        .filter(
            MarketSummary.coin == coin,
            MarketSummary.timeframe == "1h",
            MarketSummary.created_at >= start_date,
            MarketSummary.created_at <= end_date,
        )
        .order_by(MarketSummary.created_at.asc())
        .all()
    )

    records_1d = (
        db.query(MarketSummary)
        .filter(
            MarketSummary.coin == coin,
            MarketSummary.timeframe == "1d",
            MarketSummary.created_at >= start_date,
            MarketSummary.created_at <= end_date,
        )
        .order_by(MarketSummary.created_at.asc())
        .all()
    )

    if len(records_1h) < 24:
        return {
            "error": "Insufficient historical data",
            "available_records": len(records_1h),
        }

    # Build price series from score changes (proxy for returns)
    equity_curve = [initial_capital]
    benchmark_curve = [initial_capital]
    trades = []
    current_exposure = 0.5
    last_rebalance = records_1h[0].created_at

    EXPOSURE_MAP = {
        "Strong Risk-On": 0.85,
        "Risk-On": 0.65,
        "Neutral": 0.40,
        "Risk-Off": 0.20,
        "Strong Risk-Off": 0.05,
    }

    for i in range(1, len(records_1h)):
        record = records_1h[i]
        prev_record = records_1h[i - 1]

        # Calculate hourly return from score momentum
        score_change = record.score - prev_record.score
        price_return_estimate = score_change * 0.002  # Rough approximation

        # Determine strategy exposure
        hours_since_rebalance = (
            record.created_at - last_rebalance
        ).total_seconds() / 3600

        if hours_since_rebalance >= rebalance_frequency_hours:
            if strategy == "follow_model":
                target_exposure = EXPOSURE_MAP.get(record.label, 0.4)
            elif strategy == "buy_and_hold":
                target_exposure = 1.0
            elif strategy == "risk_off_only":
                target_exposure = 0.10 if "Risk-Off" in record.label else 0.80
            elif strategy == "momentum":
                target_exposure = 0.85 if "Risk-On" in record.label else 0.05
            else:
                target_exposure = 0.5

            if abs(target_exposure - current_exposure) > 0.05:
                trades.append(
                    {
                        "timestamp": record.created_at.isoformat(),
                        "regime": record.label,
                        "old_exposure": round(current_exposure, 2),
                        "new_exposure": round(target_exposure, 2),
                        "reason": f"Rebalance to {record.label}",
                    }
                )
                current_exposure = target_exposure
                last_rebalance = record.created_at

        # Apply returns
        strategy_return = price_return_estimate * current_exposure
        benchmark_return = price_return_estimate * 1.0  # Always 100%

        new_equity = equity_curve[-1] * (1 + strategy_return)
        new_benchmark = benchmark_curve[-1] * (1 + benchmark_return)

        equity_curve.append(round(new_equity, 2))
        benchmark_curve.append(round(new_benchmark, 2))

    # Calculate statistics
    final_equity = equity_curve[-1]
    final_benchmark = benchmark_curve[-1]
    total_return = ((final_equity - initial_capital) / initial_capital) * 100
    benchmark_return_pct = ((final_benchmark - initial_capital) / initial_capital) * 100
    alpha = total_return - benchmark_return_pct

    # Max drawdown
    peak = initial_capital
    max_dd = 0
    for val in equity_curve:
        if val > peak:
            peak = val
        dd = ((peak - val) / peak) * 100
        if dd > max_dd:
            max_dd = dd

    # Sharpe ratio (simplified)
    returns = [
        (equity_curve[i] - equity_curve[i - 1]) / equity_curve[i - 1]
        for i in range(1, len(equity_curve))
        if equity_curve[i - 1] > 0
    ]
    if returns:
        avg_return = sum(returns) / len(returns)
        std_return = (sum((r - avg_return) ** 2 for r in returns) / len(returns)) ** 0.5
        sharpe = (avg_return / std_return) * (8760**0.5) if std_return > 0 else 0
    else:
        sharpe = 0

    # Regime breakdown
    regime_performance = {}
    for i in range(1, len(records_1h)):
        label = records_1h[i - 1].label
        if label not in regime_performance:
            regime_performance[label] = {
                "hours": 0,
                "return_sum": 0,
            }
        score_change = records_1h[i].score - records_1h[i - 1].score
        regime_performance[label]["hours"] += 1
        regime_performance[label]["return_sum"] += (
            score_change * 0.002 * EXPOSURE_MAP.get(label, 0.4)
        )

    regime_summary = {
        label: {
            "hours": data["hours"],
            "avg_hourly_return_pct": (
                round(data["return_sum"] / data["hours"] * 100, 4)
                if data["hours"] > 0
                else 0
            ),
        }
        for label, data in regime_performance.items()
    }

    # Sample curve (every 24 points for efficiency)
    sample_step = max(1, len(equity_curve) // 200)
    sampled_curve = [
        {
            "timestamp": records_1h[i].created_at.isoformat(),
            "equity": equity_curve[i],
            "benchmark": benchmark_curve[i],
            "regime": records_1h[i].label,
        }
        for i in range(0, len(equity_curve), sample_step)
        if i < len(records_1h)
    ]

    return {
        "coin": coin,
        "strategy": strategy,
        "period": {
            "start": start_date.isoformat(),
            "end": end_date.isoformat(),
            "days": (end_date - start_date).days,
        },
        "results": {
            "initial_capital": initial_capital,
            "final_capital": round(final_equity, 2),
            "total_return_pct": round(total_return, 2),
            "benchmark_return_pct": round(benchmark_return_pct, 2),
            "alpha_pct": round(alpha, 2),
            "max_drawdown_pct": round(max_dd, 2),
            "sharpe_ratio": round(sharpe, 3),
            "total_trades": len(trades),
            "data_points": len(records_1h),
        },
        "regime_breakdown": regime_summary,
        "equity_curve": sampled_curve,
        "recent_trades": trades[-20:],
        "disclaimer": (
            "Backtest uses regime score momentum as a price proxy. "
            "Results are directional estimates only, not precise P&L."
        ),
    }


@router.get("/backtest/{coin}")
def backtest_endpoint(
    request: Request,
    coin: str,
    days: int = 90,
    strategy: str = "follow_model",
    db: Session = Depends(get_db),
):
    auth = get_auth_header(request)
    require_tier(auth, db, minimum_tier="pro")

    if coin not in settings.SUPPORTED_COINS:
        raise HTTPException(400, detail="Unsupported coin")
    if strategy not in ["follow_model", "buy_and_hold", "risk_off_only", "momentum"]:
        raise HTTPException(400, detail="Invalid strategy")
    if not 7 <= days <= 365:
        raise HTTPException(400, detail="Days must be 7-365")

    end = datetime.datetime.utcnow()
    start = end - datetime.timedelta(days=days)

    cache_key = f"backtest:{coin}:{strategy}:{days}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    result = run_backtest(db, coin, start, end, strategy=strategy)
    cache_set(cache_key, result, ttl=3600)
    return result


@router.get("/ai-narrative")
async def ai_narrative_endpoint(
    request: Request,
    coin: str = "BTC",
    db: Session = Depends(get_db),
):
    """Get AI-generated regime narrative for a coin."""
    if coin not in settings.SUPPORTED_COINS:
        raise HTTPException(400, detail="Unsupported coin")
    auth = get_auth_header(request)
    require_tier(auth, db, minimum_tier="pro")
    update_last_active(request, db)

    from app.services.ai_narrative import generate_regime_narrative
    from app.services.market_data import build_regime_stack

    stack = build_regime_stack(coin, db)
    if stack.get("incomplete"):
        return {"available": False, "reason": "Insufficient regime data"}

    return await generate_regime_narrative(coin, stack)


@router.get("/backtest/{coin}")
def backtest_endpoint(
    request: Request,
    coin: str,
    days: int = 90,
    strategy: str = "follow_model",
    db: Session = Depends(get_db),
):
    """
    Backtest a strategy against historical regime data.

    Strategies: follow_model, buy_and_hold, risk_off_only,
                momentum, inverse

    Days: 7-365
    """
    rate_limiter.require(request, max_requests=5, window_seconds=60)
    if coin not in settings.SUPPORTED_COINS:
        raise HTTPException(400, detail="Unsupported coin")

    from app.services.backtester import STRATEGY_DESCRIPTIONS

    if strategy not in STRATEGY_DESCRIPTIONS:
        raise HTTPException(
            400,
            detail=f"Invalid strategy. Choose from: {list(STRATEGY_DESCRIPTIONS.keys())}",
        )
    if not 7 <= days <= 365:
        raise HTTPException(400, detail="Days must be between 7 and 365")

    auth = get_auth_header(request)
    require_tier(auth, db, minimum_tier="pro")
    update_last_active(request, db)

    import datetime as dt
    from app.services.backtester import run_backtest
    from app.core.cache import cache_get, cache_set

    cache_key = f"backtest:{coin}:{strategy}:{days}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    end = dt.datetime.utcnow()
    start = end - dt.timedelta(days=days)

    result = run_backtest(db, coin, start, end, strategy=strategy)
    if "error" not in result:
        cache_set(cache_key, result, ttl=3600)
    return result


@router.get("/backtest-compare/{coin}")
def backtest_compare_endpoint(
    request: Request,
    coin: str,
    days: int = 90,
    db: Session = Depends(get_db),
):
    """
    Compare all strategies against each other for a coin.
    Shows which approach worked best historically.
    """
    rate_limiter.require(request, max_requests=3, window_seconds=60)
    if coin not in settings.SUPPORTED_COINS:
        raise HTTPException(400, detail="Unsupported coin")
    if not 7 <= days <= 365:
        raise HTTPException(400, detail="Days must be between 7 and 365")

    auth = get_auth_header(request)
    require_tier(auth, db, minimum_tier="pro")
    update_last_active(request, db)

    import datetime as dt
    from app.services.backtester import compare_strategies
    from app.core.cache import cache_get, cache_set

    cache_key = f"backtest_compare:{coin}:{days}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    end = dt.datetime.utcnow()
    start = end - dt.timedelta(days=days)

    result = compare_strategies(db, coin, start, end)
    if "error" not in result:
        cache_set(cache_key, result, ttl=3600)
    return result


@router.post("/monte-carlo-var")
def monte_carlo_var_endpoint(
    request: Request,
    coin: str = "BTC",
    exposure_pct: float = 50.0,
    account_size: float = 10000.0,
    horizon_days: int = 7,
    simulations: int = 5000,
    db: Session = Depends(get_db),
):
    """
    Monte Carlo Value at Risk simulation.
    Uses regime-conditioned return distribution.

    Parameters:
      coin: asset to simulate
      exposure_pct: your current exposure percentage
      account_size: total portfolio in USD
      horizon_days: simulation horizon (1-30)
      simulations: number of Monte Carlo paths (1000-10000)
    """
    rate_limiter.require(request, max_requests=10, window_seconds=60)
    if coin not in settings.SUPPORTED_COINS:
        raise HTTPException(400, detail="Unsupported coin")
    if not 1 <= horizon_days <= 30:
        raise HTTPException(400, detail="horizon_days must be 1-30")
    if not 1000 <= simulations <= 10000:
        raise HTTPException(400, detail="simulations must be 1000-10000")
    if not 0 < exposure_pct <= 200:
        raise HTTPException(400, detail="exposure_pct must be 1-200")
    if account_size <= 0:
        raise HTTPException(400, detail="account_size must be positive")

    auth = get_auth_header(request)
    require_tier(auth, db, minimum_tier="pro")
    update_last_active(request, db)

    from app.services.risk_engine import monte_carlo_var
    from app.services.market_data import build_regime_stack

    stack = build_regime_stack(coin, db)
    hazard = stack.get("hazard") or 50
    shift_risk = stack.get("shift_risk") or 50

    # Volatility score from regime conditions
    vol_score = hazard * 0.6 + shift_risk * 0.4

    from app.core.cache import cache_get, cache_set

    cache_key = (
        f"mcvar:{coin}:{int(exposure_pct)}:{int(account_size)}:"
        f"{horizon_days}:{simulations}"
    )
    cached = cache_get(cache_key)
    if cached:
        return cached

    result = monte_carlo_var(
        exposure_pct=exposure_pct,
        account_size=account_size,
        hazard_rate=hazard,
        volatility_score=vol_score,
        simulations=simulations,
        horizon_days=horizon_days,
    )

    result["regime_context"] = {
        "coin": coin,
        "hazard": hazard,
        "shift_risk": shift_risk,
        "volatility_score_used": round(vol_score, 1),
        "regime": (
            stack["execution"]["label"] if stack.get("execution") else "Unknown"
        ),
    }

    cache_set(cache_key, result, ttl=300)
    return result


@router.get("/kelly-criterion")
def kelly_criterion_endpoint(
    request: Request,
    coin: str = "BTC",
    win_rate: float = 0.55,
    avg_win_pct: float = 3.0,
    avg_loss_pct: float = 2.0,
    account_size: float = 10000.0,
    db: Session = Depends(get_db),
):
    """
    Kelly Criterion optimal position sizing
    adjusted for current regime conditions.

    Parameters:
      win_rate: your historical win rate (0.0-1.0)
      avg_win_pct: average winning trade size in %
      avg_loss_pct: average losing trade size in %
      account_size: total portfolio in USD
    """
    rate_limiter.require(request, max_requests=20, window_seconds=60)
    if coin not in settings.SUPPORTED_COINS:
        raise HTTPException(400, detail="Unsupported coin")
    if not 0 < win_rate < 1:
        raise HTTPException(400, detail="win_rate must be between 0 and 1")
    if avg_win_pct <= 0 or avg_loss_pct <= 0:
        raise HTTPException(400, detail="avg_win_pct and avg_loss_pct must be positive")
    if account_size <= 0:
        raise HTTPException(400, detail="account_size must be positive")

    auth = get_auth_header(request)
    require_tier(auth, db, minimum_tier="pro")
    update_last_active(request, db)

    from app.services.risk_engine import kelly_criterion
    from app.services.market_data import build_regime_stack

    stack = build_regime_stack(coin, db)
    hazard = stack.get("hazard") or 50
    exec_label = stack["execution"]["label"] if stack.get("execution") else "Neutral"

    return kelly_criterion(
        win_rate=win_rate,
        avg_win_pct=avg_win_pct,
        avg_loss_pct=avg_loss_pct,
        account_size=account_size,
        regime_label=exec_label,
        hazard=hazard,
    )


@router.get("/kelly-criterion")
def kelly_criterion_endpoint(
    request: Request,
    coin: str = "BTC",
    win_rate: float = 0.55,
    avg_win_pct: float = 3.0,
    avg_loss_pct: float = 2.0,
    account_size: float = 10000.0,
    db: Session = Depends(get_db),
):
    """
    Calculates Kelly Criterion optimal position size
    adjusted for current regime conditions.
    """
    auth = get_auth_header(request)
    require_tier(auth, db, minimum_tier="pro")

    if not 0 < win_rate < 1:
        raise HTTPException(400, detail="Win rate must be between 0 and 1")
    if avg_win_pct <= 0 or avg_loss_pct <= 0:
        raise HTTPException(400, detail="Win/loss percentages must be positive")

    from app.services.market_data import build_regime_stack

    stack = build_regime_stack(coin, db)
    hazard = stack.get("hazard") or 50
    regime_exposure = stack.get("exposure") or 50
    exec_label = stack["execution"]["label"] if stack.get("execution") else "Neutral"

    # Kelly formula: f = (bp - q) / b
    b = avg_win_pct / avg_loss_pct
    p = win_rate
    q = 1 - win_rate
    full_kelly = (b * p - q) / b
    half_kelly = full_kelly / 2
    quarter_kelly = full_kelly / 4

    # Regime adjustment
    regime_mult = {
        "Strong Risk-On": 1.0,
        "Risk-On": 0.85,
        "Neutral": 0.60,
        "Risk-Off": 0.35,
        "Strong Risk-Off": 0.10,
    }.get(exec_label, 0.60)

    hazard_mult = 1 - (hazard / 100) * 0.5
    adjusted_kelly = max(0, full_kelly * regime_mult * hazard_mult)

    recommendation = min(adjusted_kelly, half_kelly)

    return {
        "coin": coin,
        "inputs": {
            "win_rate": win_rate,
            "avg_win_pct": avg_win_pct,
            "avg_loss_pct": avg_loss_pct,
            "account_size": account_size,
        },
        "kelly": {
            "full_kelly_pct": round(full_kelly * 100, 2),
            "half_kelly_pct": round(half_kelly * 100, 2),
            "quarter_kelly_pct": round(quarter_kelly * 100, 2),
            "regime_adjusted_pct": round(adjusted_kelly * 100, 2),
            "recommendation_pct": round(recommendation * 100, 2),
        },
        "position_sizes": {
            "full_kelly_usd": round(account_size * full_kelly, 2),
            "half_kelly_usd": round(account_size * half_kelly, 2),
            "recommended_usd": round(account_size * recommendation, 2),
        },
        "regime_context": {
            "label": exec_label,
            "hazard": hazard,
            "regime_multiplier": regime_mult,
            "hazard_multiplier": round(hazard_mult, 3),
            "model_exposure": regime_exposure,
        },
        "interpretation": (
            f"Full Kelly suggests {round(full_kelly * 100, 1)}% exposure. "
            f"In {exec_label} with {hazard}% hazard, "
            f"regime-adjusted recommendation is {round(recommendation * 100, 1)}%. "
            f"Never bet full Kelly â€” half Kelly is standard practice."
        ),
        "disclaimer": "Kelly Criterion is a mathematical framework. Not financial advice.",
    }
