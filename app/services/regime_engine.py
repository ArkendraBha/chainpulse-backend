import math
import json
import logging
import datetime
from typing import Optional
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.cache import cache_get, cache_set
from app.db.models import (
    ExposureLog, PerformanceEntry, UserProfile,
    AlertThreshold, MarketSummary,
)
from app.services.market_data import (
    get_klines, fetch_all_market_data,
    build_regime_stack, compute_regime_quality,
    compute_market_breadth, volatility_environment,
    regime_durations, current_age, average_regime_duration,
    trend_maturity_score, regime_transition_matrix,
    compute_decision_score, volatility, volume_momentum,
)
from app.utils.enums import (
    PLAYBOOK_DATA, ARCHETYPE_CONFIG, LEAK_TYPES,
    DYNAMIC_RISK_EVENTS,
)

logger = logging.getLogger("chainpulse")


# -----------------------------------------
# SETUP QUALITY ENGINE
# -----------------------------------------
def compute_extension_from_mean(prices: list, period: int = 20) -> float:
    if len(prices) < period:
        return 0.0
    ma = sum(prices[-period:]) / period
    if ma == 0:
        return 0.0
    return round(((prices[-1] - ma) / ma) * 100, 4)


def compute_atr(prices: list, period: int = 14) -> float:
    if len(prices) < period + 1:
        return 0.0
    ranges = [abs(prices[i] - prices[i - 1]) for i in range(1, len(prices))]
    if len(ranges) < period:
        return 0.0
    return sum(ranges[-period:]) / period


def compute_pullback_depth(prices: list, lookback: int = 20) -> float:
    if len(prices) < lookback:
        return 0.0
    recent_high = max(prices[-lookback:])
    if recent_high == 0:
        return 0.0
    return round(((recent_high - prices[-1]) / recent_high) * 100, 4)


def compute_range_position(prices: list, period: int = 48) -> float:
    if len(prices) < period:
        return 50.0
    high = max(prices[-period:])
    low = min(prices[-period:])
    if high == low:
        return 50.0
    return round(((prices[-1] - low) / (high - low)) * 100, 2)


def compute_momentum_slope(prices: list, period: int = 10) -> float:
    if len(prices) < period + 5:
        return 0.0
    mom_now = ((prices[-1] - prices[-period]) / prices[-period]) * 100
    mom_prev = (
        ((prices[-period] - prices[-period * 2]) / prices[-period * 2]) * 100
        if len(prices) >= period * 2
        else 0
    )
    return round(mom_now - mom_prev, 4)


def compute_volume_confirmation(volumes: list, period: int = 10) -> float:
    if len(volumes) < period * 2:
        return 50.0
    recent_avg = sum(volumes[-period:]) / period
    prior_avg = sum(volumes[-period * 2:-period]) / period
    if prior_avg == 0:
        return 50.0
    ratio = recent_avg / prior_avg
    return round(min(100, max(0, ratio * 50)), 2)


async def compute_setup_quality(
    coin: str,
    db: Session,
    market_data: dict = None,
    stack: dict = None,
) -> dict:
    if market_data:
        prices_1h = market_data.get("1h", {}).get("prices", [])
        volumes_1h = market_data.get("1h", {}).get("volumes", [])
        prices_4h = market_data.get("4h", {}).get("prices", [])
        volumes_4h = market_data.get("4h", {}).get("volumes", [])
    else:
        prices_1h, volumes_1h = await get_klines(coin, "1h", limit=120)
        prices_4h, volumes_4h = await get_klines(coin, "4h", limit=60)

    if len(prices_1h) < 50 or len(prices_4h) < 20:
        return {
            "coin": coin,
            "setup_quality_score": None,
            "error": "Insufficient price data",
        }

    current_price = prices_1h[-1]
    ext_20 = compute_extension_from_mean(prices_1h, 20)
    ext_50 = compute_extension_from_mean(prices_1h, 50)
    pullback_depth = compute_pullback_depth(prices_1h, 24)
    pullback_depth_4h = compute_pullback_depth(prices_4h, 12)
    range_pos = compute_range_position(prices_1h, 48)
    mom_slope_1h = compute_momentum_slope(prices_1h, 8)
    mom_slope_4h = compute_momentum_slope(prices_4h, 6)
    vol_confirm = compute_volume_confirmation(volumes_1h, 10)
    atr_1h = compute_atr(prices_1h, 14)
    atr_4h = compute_atr(prices_4h, 14)

    if stack is None:
        stack = build_regime_stack(coin, db)

    exec_label = "Neutral"
    trend_label = "Neutral"
    macro_label = "Neutral"
    coherence = 50.0
    hazard = 50.0
    alignment = 50.0

    if not stack.get("incomplete"):
        exec_label = (
            stack["execution"]["label"]
            if stack.get("execution") else "Neutral"
        )
        trend_label = (
            stack["trend"]["label"]
            if stack.get("trend") else "Neutral"
        )
        macro_label = (
            stack["macro"]["label"]
            if stack.get("macro") else "Neutral"
        )
        coherence = (
            stack["execution"]["coherence"]
            if stack.get("execution") else 50.0
        )
        hazard = stack.get("hazard") or 50.0
        alignment = stack.get("alignment") or 50.0

    regime_num = settings.REGIME_NUMERIC.get(exec_label, 0)
    is_bullish_regime = regime_num > 0
    is_bearish_regime = regime_num < 0

    # Chase Risk
    chase_risk = 0.0
    if is_bullish_regime:
        chase_risk = (
            min(100, abs(ext_20) * 8) * 0.30
            + range_pos * 0.25
            + max(0, 100 - vol_confirm) * 0.20
            + hazard * 0.15
            + max(0, -mom_slope_1h * 5) * 0.10
        )
    elif is_bearish_regime:
        chase_risk = (
            min(100, abs(ext_20) * 8) * 0.30
            + (100 - range_pos) * 0.25
            + max(0, 100 - vol_confirm) * 0.20
            + hazard * 0.15
            + max(0, mom_slope_1h * 5) * 0.10
        )
    else:
        chase_risk = 60.0
    chase_risk = round(min(100, max(0, chase_risk)), 1)

    # Exhaustion
    exhaustion = round(min(100, max(0,
        min(100, abs(ext_20) * 6) * 0.25
        + max(0, -mom_slope_1h * 10 if is_bullish_regime else mom_slope_1h * 10) * 0.25
        + hazard * 0.25
        + max(0, 100 - coherence) * 0.15
        + max(0, 100 - alignment) * 0.10
    )), 1)

    # Pullback Quality
    if is_bullish_regime:
        pullback_quality = round(min(100, max(0,
            min(100, pullback_depth * 15) * 0.30
            + min(100, max(0, mom_slope_1h * 15 + 50)) * 0.25
            + vol_confirm * 0.20
            + coherence * 0.15
            + (100 - hazard) * 0.10
        )), 1)
    elif is_bearish_regime:
        pullback_quality = round(min(100, max(0,
            min(100, (100 - range_pos) * 1.2) * 0.30
            + min(100, max(0, -mom_slope_1h * 15 + 50)) * 0.25
            + vol_confirm * 0.20
            + coherence * 0.15
            + (100 - hazard) * 0.10
        )), 1)
    else:
        pullback_quality = 30.0

    # Breakout Quality
    breakout_quality = round(min(100, max(0,
        (range_pos if is_bullish_regime else 100 - range_pos) * 0.25
        + vol_confirm * 0.25
        + coherence * 0.20
        + alignment * 0.15
        + (100 - hazard) * 0.15
    )), 1)

    # Master Setup Score
    if is_bullish_regime:
        setup_score = round(min(100, max(0,
            (100 - chase_risk) * 0.25
            + (100 - exhaustion) * 0.20
            + pullback_quality * 0.20
            + coherence * 0.15
            + (100 - hazard) * 0.10
            + alignment * 0.10
        )), 1)
    elif is_bearish_regime:
        setup_score = round(min(100, max(0,
            (100 - chase_risk) * 0.20
            + exhaustion * 0.15
            + (100 - hazard) * 0.20
            + coherence * 0.15
            + (100 - range_pos) * 0.15
            + alignment * 0.15
        )), 1)
    else:
        setup_score = round(min(100, max(0,
            (100 - chase_risk) * 0.25
            + (100 - abs(ext_20) * 5) * 0.20
            + vol_confirm * 0.15
            + coherence * 0.20
            + (100 - hazard) * 0.20
        )), 1)

    if setup_score >= 80:
        setup_label = "Excellent Setup"
    elif setup_score >= 65:
        setup_label = "Good Setup"
    elif setup_score >= 50:
        setup_label = "Moderate Setup"
    elif setup_score >= 35:
        setup_label = "Weak Setup"
    else:
        setup_label = "Poor Setup - Wait"

    if setup_score < 30:
        entry_mode = "No Entry"
    elif chase_risk > 75:
        entry_mode = "Wait for Pullback"
    elif pullback_quality > 65 and is_bullish_regime:
        entry_mode = "Scale In - Pullback"
    elif breakout_quality > 70 and range_pos > 85:
        entry_mode = "Breakout Entry"
    elif setup_score > 60:
        entry_mode = "Scale In"
    else:
        entry_mode = "Wait"

    if atr_1h > 0:
        if is_bullish_regime:
            entry_low = round(current_price - atr_1h * 1.5, 2)
            entry_high = round(current_price - atr_1h * 0.3, 2)
            invalidation = round(current_price - atr_1h * 3.0, 2)
            tp1 = round(current_price + atr_1h * 2.0, 2)
            tp2 = round(current_price + atr_1h * 4.0, 2)
        elif is_bearish_regime:
            entry_low = round(current_price + atr_1h * 0.3, 2)
            entry_high = round(current_price + atr_1h * 1.5, 2)
            invalidation = round(current_price + atr_1h * 3.0, 2)
            tp1 = round(current_price - atr_1h * 2.0, 2)
            tp2 = round(current_price - atr_1h * 4.0, 2)
        else:
            entry_low = round(current_price - atr_1h * 1.0, 2)
            entry_high = round(current_price + atr_1h * 0.5, 2)
            invalidation = round(current_price - atr_1h * 2.5, 2)
            tp1 = round(current_price + atr_1h * 1.5, 2)
            tp2 = round(current_price + atr_1h * 3.0, 2)
    else:
        entry_low = entry_high = invalidation = tp1 = tp2 = 0

    if atr_1h > 0:
        tight_stop = round(atr_1h * 1.5, 2)
        normal_stop = round(atr_1h * 2.5, 2)
        wide_stop = round(atr_1h * 4.0, 2)
        stop_pct = (
            round((normal_stop / current_price) * 100, 2)
            if current_price > 0 else 0
        )
    else:
        tight_stop = normal_stop = wide_stop = stop_pct = 0

    return {
        "coin": coin,
        "current_price": current_price,
        "setup_quality_score": setup_score,
        "setup_label": setup_label,
        "entry_mode": entry_mode,
        "chase_risk": chase_risk,
        "trend_exhaustion": exhaustion,
        "pullback_quality": pullback_quality,
        "breakout_quality": breakout_quality,
        "extension_from_mean_pct": round(ext_20, 2),
        "extension_from_50_pct": round(ext_50, 2),
        "range_position": range_pos,
        "momentum_slope_1h": round(mom_slope_1h, 3),
        "momentum_slope_4h": round(mom_slope_4h, 3),
        "volume_confirmation": vol_confirm,
        "optimal_entry_zone": {"low": entry_low, "high": entry_high},
        "invalidation_level": invalidation,
        "take_profit_zones": [tp1, tp2],
        "stop_guidance": {
            "tight": tight_stop,
            "normal": normal_stop,
            "wide": wide_stop,
            "normal_pct": stop_pct,
        },
        "atr_1h": round(atr_1h, 2),
        "atr_4h": round(atr_4h, 2),
        "regime_context": {
            "execution": exec_label,
            "trend": trend_label,
            "macro": macro_label,
            "coherence": coherence,
            "hazard": hazard,
            "alignment": alignment,
        },
    }


# -----------------------------------------
# OPPORTUNITY RANKING
# -----------------------------------------
async def compute_opportunity_score(
    coin: str, db: Session
) -> Optional[dict]:
    stack = build_regime_stack(coin, db)
    if stack["incomplete"]:
        return None

    regime_quality = compute_regime_quality(stack)
    regime_score = regime_quality["score"]
    setup = await compute_setup_quality(coin, db, stack=stack)
    setup_score = setup.get("setup_quality_score") or 50
    chase_risk = setup.get("chase_risk") or 50
    shift_risk = stack.get("shift_risk") or 50
    shift_opportunity = 100 - shift_risk
    exposure = stack.get("exposure") or 50
    survival = stack.get("survival") or 50
    hazard = stack.get("hazard") or 50
    hazard_penalty = hazard
    coherence = 50.0
    if stack.get("execution") and stack["execution"].get("coherence"):
        coherence = stack["execution"]["coherence"]

    direction = stack.get("direction") or "mixed"
    direction_mult = (
        1.0 if direction == "bullish"
        else 0.7 if direction == "mixed"
        else 0.4
    )

    raw_score = (
        regime_score * 0.20
        + setup_score * 0.20
        + shift_opportunity * 0.15
        + exposure * 0.15
        + survival * 0.10
        + (100 - chase_risk) * 0.10
        + coherence * 0.10
    ) * direction_mult

    raw_score = raw_score * (1 - (hazard_penalty / 100) * 0.3)
    opportunity_score = round(min(100, max(0, raw_score)), 1)

    reasons = []
    if regime_score >= 65:
        reasons.append("Strong regime quality")
    elif regime_score < 40:
        reasons.append("Weak regime structure")
    if setup_score >= 65:
        reasons.append("Good entry conditions")
    elif setup_score < 35:
        reasons.append("Poor entry timing")
    if shift_risk > 65:
        reasons.append("Elevated shift risk")
    elif shift_risk < 30:
        reasons.append("Low shift risk")
    if coherence > 70:
        reasons.append("High coherence")
    if chase_risk > 70:
        reasons.append("High chase risk - wait for pullback")
    if hazard > 65:
        reasons.append("Hazard rate elevated")
    if survival > 75:
        reasons.append("Regime persistence strong")
    reason_str = "; ".join(reasons[:4]) if reasons else "Moderate conditions"

    return {
        "coin": coin,
        "opportunity_score": opportunity_score,
        "regime_quality_grade": regime_quality["grade"],
        "setup_quality_score": setup_score,
        "setup_label": setup.get("setup_label") or "-",
        "entry_mode": setup.get("entry_mode") or "-",
        "chase_risk": chase_risk,
        "shift_risk": shift_risk,
        "exposure_rec": exposure,
        "direction": direction,
        "survival": survival,
        "hazard": hazard,
        "coherence": coherence,
        "reason": reason_str,
    }


async def compute_opportunity_ranking(db: Session) -> dict:
    rankings = []
    for coin in settings.SUPPORTED_COINS:
        opp = await compute_opportunity_score(coin, db)
        if opp:
            rankings.append(opp)

    rankings.sort(
        key=lambda x: x["opportunity_score"], reverse=True
    )

    best_long = None
    most_defensive = None
    avoid = []

    for r in rankings:
        if r["direction"] == "bullish" and best_long is None:
            best_long = r["coin"]
        if r["shift_risk"] < 30 and most_defensive is None:
            most_defensive = r["coin"]
        if r["opportunity_score"] < 30 or r["chase_risk"] > 80:
            avoid.append(r["coin"])

    if not most_defensive and rankings:
        most_defensive = min(
            rankings, key=lambda x: x["shift_risk"]
        )["coin"]

    rotation_signals = []
    if len(rankings) >= 2:
        top = rankings[0]
        bottom = rankings[-1]
        if top["opportunity_score"] - bottom["opportunity_score"] > 30:
            rotation_signals.append(
                f"Strong divergence: {top['coin']} ({top['opportunity_score']}) "
                f"vs {bottom['coin']} ({bottom['opportunity_score']}). "
                f"Consider rotating toward {top['coin']}."
            )

    for r in rankings:
        if r["chase_risk"] > 75 and r["opportunity_score"] > 60:
            rotation_signals.append(
                f"{r['coin']} has high opportunity but elevated chase risk - "
                f"wait for pullback before adding."
            )

    return {
        "rankings": rankings,
        "best_long": best_long,
        "most_defensive": most_defensive,
        "avoid": avoid,
        "rotation_signals": rotation_signals,
        "timestamp": datetime.datetime.utcnow().isoformat(),
    }


# -----------------------------------------
# HISTORICAL ANALOGS ENGINE
# -----------------------------------------
async def find_historical_analogs(
    db: Session,
    coin: str,
    target_macro: str,
    target_trend: str,
    target_exec: str,
    target_hazard: float = 50,
    hazard_tolerance: float = 20,
) -> dict:
    records = (
        db.query(MarketSummary)
        .filter(
            MarketSummary.coin == coin,
            MarketSummary.timeframe == "1h",
        )
        .order_by(MarketSummary.created_at.asc())
        .all()
    )

    if len(records) < 100:
        return {
            "coin": coin,
            "sample_size": 0,
            "data_sufficient": False,
            "message": f"Need more history. Currently {len(records)} records, need 100+.",
        }

    prices_1d, _ = await get_klines(coin, "1d", limit=90)
    prices_1h, _ = await get_klines(coin, "1h", limit=120)

    if len(prices_1h) < 30:
        return {
            "coin": coin,
            "sample_size": 0,
            "data_sufficient": False,
            "message": "Insufficient price data for forward return calculation.",
        }

    records_4h = (
        db.query(MarketSummary)
        .filter(
            MarketSummary.coin == coin,
            MarketSummary.timeframe == "4h",
        )
        .order_by(MarketSummary.created_at.asc())
        .all()
    )
    records_1d_db = (
        db.query(MarketSummary)
        .filter(
            MarketSummary.coin == coin,
            MarketSummary.timeframe == "1d",
        )
        .order_by(MarketSummary.created_at.asc())
        .all()
    )

    def build_label_lookup(recs, window_hours=6):
        if not recs:
            return lambda ts: None
        sorted_recs = sorted(recs, key=lambda r: r.created_at)

        def find_label(ts):
            best = None
            best_delta = None
            for r in sorted_recs:
                delta = abs((r.created_at - ts).total_seconds())
                if best_delta is None or delta < best_delta:
                    best_delta = delta
                    best = r.label
                if r.created_at > ts and (
                    r.created_at - ts
                ).total_seconds() > window_hours * 3600:
                    break
            return best

        return find_label

    find_4h_label = build_label_lookup(records_4h, window_hours=6)
    find_1d_label = build_label_lookup(records_1d_db, window_hours=26)

    matching_periods = []
    records_list = list(records)

    for i in range(len(records_list) - 24):
        r = records_list[i]
        if r.label != target_exec:
            continue
        trend_label_at_time = find_4h_label(r.created_at)
        if trend_label_at_time and trend_label_at_time != target_trend:
            continue
        macro_label_at_time = find_1d_label(r.created_at)
        if macro_label_at_time and macro_label_at_time != target_macro:
            continue

        forward_prices = []
        for j in range(i, min(i + 168, len(records_list))):
            forward_prices.append(records_list[j].score)

        if len(forward_prices) >= 24:
            labels_forward = [
                records_list[j].label
                for j in range(i, min(i + 72, len(records_list)))
            ]
            same_count = 0
            for lbl in labels_forward:
                if lbl == r.label:
                    same_count += 1
                else:
                    break

            matching_periods.append({
                "date": r.created_at.strftime("%Y-%m-%d %H:%M"),
                "label": r.label,
                "score": r.score,
                "coherence": r.coherence,
                "continuation_hours": same_count,
                "matched_macro": macro_label_at_time or "unknown",
                "matched_trend": trend_label_at_time or "unknown",
            })

    if len(matching_periods) < 3:
        matching_periods = []
        for i in range(len(records_list) - 24):
            r = records_list[i]
            if r.label != target_exec:
                continue
            forward_prices = []
            for j in range(i, min(i + 168, len(records_list))):
                forward_prices.append(records_list[j].score)
            if len(forward_prices) >= 24:
                labels_forward = [
                    records_list[j].label
                    for j in range(i, min(i + 72, len(records_list)))
                ]
                same_count = 0
                for lbl in labels_forward:
                    if lbl == r.label:
                        same_count += 1
                    else:
                        break
                matching_periods.append({
                    "date": r.created_at.strftime("%Y-%m-%d %H:%M"),
                    "label": r.label,
                    "score": r.score,
                    "coherence": r.coherence,
                    "continuation_hours": same_count,
                    "matched_macro": "relaxed",
                    "matched_trend": "relaxed",
                })

    match_type = "multi_timeframe"
    if matching_periods and matching_periods[0].get("matched_macro") == "relaxed":
        match_type = "execution_only_fallback"

    if len(matching_periods) < 5:
        return {
            "coin": coin,
            "target_regime": target_exec,
            "sample_size": len(matching_periods),
            "data_sufficient": False,
            "match_type": match_type,
            "message": f"Only {len(matching_periods)} matching periods found. Need 5+.",
        }

    current_price = prices_1h[-1] if prices_1h else 0
    continuation_hours_list = [
        m["continuation_hours"] for m in matching_periods
    ]
    avg_continuation = sum(continuation_hours_list) / len(continuation_hours_list)
    max_continuation = max(continuation_hours_list)
    min_continuation = min(continuation_hours_list)
    continued_24h = sum(1 for h in continuation_hours_list if h >= 24)
    continued_72h = sum(1 for h in continuation_hours_list if h >= 72)
    continuation_prob_24h = round(
        (continued_24h / len(continuation_hours_list)) * 100, 1
    )
    continuation_prob_72h = round(
        (continued_72h / len(continuation_hours_list)) * 100, 1
    )

    forward_returns = {
        "1d": {"avg": 0, "median": 0, "best": 0, "worst": 0, "positive_pct": 50},
        "3d": {"avg": 0, "median": 0, "best": 0, "worst": 0, "positive_pct": 50},
        "7d": {"avg": 0, "median": 0, "best": 0, "worst": 0, "positive_pct": 50},
    }

    if len(prices_1d) >= 10:
        daily_returns = []
        for i in range(1, len(prices_1d)):
            ret = (
                (prices_1d[i] - prices_1d[i - 1]) / prices_1d[i - 1]
            ) * 100
            daily_returns.append(ret)

        if daily_returns:
            for horizon_key, days in [("1d", 1), ("3d", 3), ("7d", 7)]:
                if len(daily_returns) >= days:
                    fwd_rets = []
                    for i in range(len(daily_returns) - days + 1):
                        compound = 1.0
                        for j in range(days):
                            compound *= (1 + daily_returns[i + j] / 100)
                        fwd_rets.append(round((compound - 1) * 100, 2))

                    if fwd_rets:
                        sorted_rets = sorted(fwd_rets)
                        forward_returns[horizon_key] = {
                            "avg": round(sum(fwd_rets) / len(fwd_rets), 2),
                            "median": sorted_rets[len(sorted_rets) // 2],
                            "best": sorted_rets[-1],
                            "worst": sorted_rets[0],
                            "positive_pct": round(
                                sum(1 for r in fwd_rets if r > 0) / len(fwd_rets) * 100, 1
                            ),
                        }

    mae_estimates = []
    if len(prices_1h) >= 48:
        for i in range(24, len(prices_1h)):
            window = prices_1h[i - 24:i]
            low = min(window)
            entry = window[0]
            if entry > 0:
                mae = ((low - entry) / entry) * 100
                mae_estimates.append(round(mae, 2))

    avg_mae = round(sum(mae_estimates) / len(mae_estimates), 2) if mae_estimates else -3.0
    worst_mae = min(mae_estimates) if mae_estimates else -8.0

    if mae_estimates:
        dd_gt_3pct = sum(1 for m in mae_estimates if m < -3)
        dd_gt_5pct = sum(1 for m in mae_estimates if m < -5)
        dd_gt_3pct_prob = round((dd_gt_3pct / len(mae_estimates)) * 100, 1)
        dd_gt_5pct_prob = round((dd_gt_5pct / len(mae_estimates)) * 100, 1)
    else:
        dd_gt_3pct_prob = 30
        dd_gt_5pct_prob = 15

    return {
        "coin": coin,
        "target_regime": {
            "macro": target_macro,
            "trend": target_trend,
            "execution": target_exec,
        },
        "sample_size": len(matching_periods),
        "data_sufficient": len(matching_periods) >= 5,
        "match_type": match_type,
        "continuation": {
            "avg_hours": round(avg_continuation, 1),
            "max_hours": max_continuation,
            "min_hours": min_continuation,
            "prob_24h_pct": continuation_prob_24h,
            "prob_72h_pct": continuation_prob_72h,
        },
        "forward_returns": forward_returns,
        "max_adverse_excursion": {
            "avg_pct": avg_mae,
            "worst_pct": worst_mae,
            "drawdown_gt_3pct_prob": dd_gt_3pct_prob,
            "drawdown_gt_5pct_prob": dd_gt_5pct_prob,
        },
        "matching_periods": matching_periods[:20],
        "current_price": current_price,
        "timestamp": datetime.datetime.utcnow().isoformat(),
    }


# -----------------------------------------
# PROBABILISTIC SCENARIOS
# -----------------------------------------
async def compute_scenarios(
    coin: str,
    db: Session,
    stack: dict = None,
    setup: dict = None,
) -> dict:
    if stack is None:
        stack = build_regime_stack(coin, db)
    if stack.get("incomplete"):
        return {"coin": coin, "error": "Insufficient data"}

    exec_label = (
        stack["execution"]["label"] if stack.get("execution") else "Neutral"
    )
    trend_label = (
        stack["trend"]["label"] if stack.get("trend") else "Neutral"
    )
    macro_label = (
        stack["macro"]["label"] if stack.get("macro") else "Neutral"
    )

    hazard = stack.get("hazard") or 50
    survival = stack.get("survival") or 50
    shift_risk = stack.get("shift_risk") or 50
    alignment = stack.get("alignment") or 50
    exposure = stack.get("exposure") or 50
    direction = stack.get("direction") or "mixed"

    transitions = regime_transition_matrix(db, coin, "1h")

    if setup is None:
        setup = await compute_setup_quality(coin, db, stack=stack)
    setup_score = setup.get("setup_quality_score") or 50
    exhaustion = setup.get("trend_exhaustion") or 50

    regime_num = settings.REGIME_NUMERIC.get(exec_label, 0)

    base_prob = round(min(75, max(25,
        survival * 0.40
        + (100 - hazard) * 0.30
        + alignment * 0.15
        + (100 - shift_risk) * 0.15
    )), 0)

    if regime_num > 0:
        base_outcome = "Regime continuation - trend maintains current direction"
        base_exposure = f"Maintain {int(exposure * 0.9)}-{int(min(95, exposure * 1.1))}%"
        base_actions = [
            "Hold existing positions",
            "Trail stops to recent support",
            "Monitor hazard for acceleration",
        ]
    elif regime_num < 0:
        base_outcome = "Risk-Off continuation - defensive positioning maintained"
        base_exposure = f"Maintain {int(max(5, exposure * 0.8))}-{int(exposure * 1.1)}%"
        base_actions = [
            "Stay defensive - hold cash",
            "No new long entries",
            "Monitor for capitulation signals",
        ]
    else:
        base_outcome = "Range-bound continuation - neutral positioning"
        base_exposure = f"Maintain {int(max(5, exposure * 0.85))}-{int(min(95, exposure * 1.15))}%"
        base_actions = [
            "Reduce position sizes",
            "Wait for directional clarity",
            "Monitor regime stack for shifts",
        ]

    if regime_num >= 0:
        bull_prob = round(min(45, max(5,
            (100 - hazard) * 0.25
            + alignment * 0.20
            + setup_score * 0.20
            + (100 - exhaustion) * 0.20
            + survival * 0.15
        )), 0)
        bull_outcome = "Breakout to higher - regime upgrades to stronger risk-on"
        bull_exposure = f"Increase to {int(min(95, exposure * 1.3))}-{int(min(95, exposure * 1.5))}%"
        bull_invalidation = "1h regime downgrades or hazard > 70%"
        bull_actions = [
            "Add on breakout confirmation",
            "Pyramiding valid",
            "Extend targets",
        ]
    else:
        bull_prob = round(min(35, max(5,
            hazard * 0.30
            + exhaustion * 0.25
            + (100 - survival) * 0.25
            + shift_risk * 0.20
        )), 0)
        bull_outcome = "Relief bounce - regime stabilizes and shifts toward Neutral"
        bull_exposure = f"Cautiously increase to {int(min(60, exposure * 1.5))}-{int(min(70, exposure * 2.0))}%"
        bull_invalidation = "New momentum lows or hazard re-acceleration"
        bull_actions = [
            "Only add if regime shifts to Neutral on all timeframes",
            "Small sizes - countertrend",
            "Tight stops",
        ]

    if regime_num <= 0:
        bear_prob = round(min(45, max(5,
            hazard * 0.30
            + shift_risk * 0.25
            + (100 - alignment) * 0.20
            + (100 - survival) * 0.25
        )), 0)
        bear_outcome = "Accelerated sell-off - regime deteriorates further"
        bear_exposure = f"Reduce to {int(max(0, exposure * 0.3))}-{int(max(5, exposure * 0.5))}%"
        bear_invalidation = "Strong reversal with volume and 4h regime upgrade"
        bear_actions = [
            "Exit remaining positions",
            "Move to full cash / stables",
            "Do not attempt to catch bottom",
        ]
    else:
        bear_prob = round(min(40, max(5,
            hazard * 0.30
            + exhaustion * 0.25
            + shift_risk * 0.25
            + (100 - alignment) * 0.20
        )), 0)
        bear_outcome = "Regime failure - trend breaks and shifts to Risk-Off"
        bear_exposure = f"Reduce to {int(max(5, exposure * 0.4))}-{int(max(10, exposure * 0.6))}%"
        bear_invalidation = "Structural break of 4h trend support"
        bear_actions = [
            "Reduce exposure immediately",
            "No new longs until regime stabilizes",
            "Move stops to breakeven on remaining positions",
        ]

    total = base_prob + bull_prob + bear_prob
    if total > 0:
        base_prob = round((base_prob / total) * 100, 0)
        bull_prob = round((bull_prob / total) * 100, 0)
        bear_prob = 100 - base_prob - bull_prob

    scenarios = [
        {
            "name": "Base Case",
            "probability": int(base_prob),
            "outcome": base_outcome,
            "exposure": base_exposure,
            "actions": base_actions,
            "invalidation": f"Hazard exceeds {int(min(100, hazard + 25))}% or regime shifts",
        },
        {
            "name": "Bull Case",
            "probability": int(bull_prob),
            "outcome": bull_outcome,
            "exposure": bull_exposure,
            "actions": bull_actions,
            "invalidation": bull_invalidation,
        },
        {
            "name": "Bear Case",
            "probability": int(bear_prob),
            "outcome": bear_outcome,
            "exposure": bear_exposure,
            "actions": bear_actions,
            "invalidation": bear_invalidation,
        },
    ]

    invalidation_triggers = []
    if hazard > 50:
        invalidation_triggers.append(
            f"Hazard at {hazard}% - approaching instability"
        )
    if shift_risk > 55:
        invalidation_triggers.append(
            f"Shift risk at {shift_risk}% - transition pressure building"
        )
    if exhaustion > 65:
        invalidation_triggers.append(
            f"Trend exhaustion at {exhaustion}% - momentum fading"
        )
    if alignment < 40:
        invalidation_triggers.append(
            f"Alignment only {alignment}% - timeframes diverging"
        )

    if regime_num > 0 and hazard < 50:
        expected_24h = "Continuation higher with possible shallow pullback"
        expected_7d = "Trend intact if hazard stays below 60%"
    elif regime_num > 0 and hazard >= 50:
        expected_24h = "Possible consolidation or pullback as hazard elevates"
        expected_7d = "Watch for regime transition - survival declining"
    elif regime_num < 0 and hazard < 50:
        expected_24h = "Continued weakness - bounces likely sold"
        expected_7d = "Risk-Off persists until capitulation signals appear"
    elif regime_num < 0 and hazard >= 50:
        expected_24h = "Possible stabilization attempt - but premature to buy"
        expected_7d = "Risk-Off regime may be exhausting - watch for Neutral shift"
    else:
        expected_24h = "Range-bound - no clear directional signal"
        expected_7d = "Wait for regime stack alignment before committing"

    return {
        "coin": coin,
        "scenarios": scenarios,
        "current_regime": exec_label,
        "direction": direction,
        "invalidation_triggers": invalidation_triggers,
        "expected_path": {"24h": expected_24h, "7d": expected_7d},
        "context": {
            "hazard": hazard,
            "survival": survival,
            "shift_risk": shift_risk,
            "alignment": alignment,
            "exhaustion": exhaustion,
            "setup_score": setup_score,
        },
        "timestamp": datetime.datetime.utcnow().isoformat(),
    }


# -----------------------------------------
# INTERNAL DAMAGE MONITOR
# -----------------------------------------
async def compute_internal_damage(
    coin: str,
    db: Session,
    market_data: dict = None,
    stack: dict = None,
) -> dict:
    records_1h = (
        db.query(MarketSummary)
        .filter(
            MarketSummary.coin == coin,
            MarketSummary.timeframe == "1h",
        )
        .order_by(MarketSummary.created_at.desc())
        .limit(24)
        .all()
    )
    records_1h.reverse()

    if len(records_1h) < 6:
        return {
            "coin": coin,
            "internal_damage_score": None,
            "error": "Insufficient history for damage analysis",
        }

    signals = []
    damage_components = {}

    # Coherence Rollover
    if len(records_1h) >= 6:
        recent_coherence = [
            r.coherence for r in records_1h[-6:]
            if r.coherence is not None
        ]
        if len(recent_coherence) >= 4:
            avg_recent = sum(recent_coherence[-3:]) / 3
            avg_prior = sum(recent_coherence[:3]) / 3
            coherence_decline = round(avg_prior - avg_recent, 1)
            damage_components["coherence_rollover"] = min(
                100, max(0, coherence_decline * 5)
            )
            declining = all(
                recent_coherence[i] <= recent_coherence[i - 1]
                for i in range(1, len(recent_coherence))
            )
            if declining and coherence_decline > 5:
                signals.append({
                    "type": "coherence_rollover",
                    "severity": "high" if coherence_decline > 15 else "medium",
                    "message": f"Coherence declined {coherence_decline} pts over last 6 updates",
                    "value": coherence_decline,
                })
            elif coherence_decline > 3:
                signals.append({
                    "type": "coherence_weakening",
                    "severity": "low",
                    "message": f"Coherence weakening by {coherence_decline} pts",
                    "value": coherence_decline,
                })
        else:
            damage_components["coherence_rollover"] = 0
    else:
        damage_components["coherence_rollover"] = 0

    # Momentum Divergence
    if len(records_1h) >= 6:
        recent_scores = [r.score for r in records_1h[-6:]]
        if market_data and "1h" in market_data:
            prices_1h = market_data["1h"]["prices"]
        else:
            prices_1h, _ = await get_klines(coin, "1h", limit=12)

        if len(prices_1h) >= 6 and len(recent_scores) >= 6:
            price_direction = prices_1h[-1] - prices_1h[-6]
            score_direction = recent_scores[-1] - recent_scores[-6]

            if price_direction > 0 and score_direction < -3:
                div_strength = abs(score_direction)
                damage_components["momentum_divergence"] = min(
                    100, div_strength * 5
                )
                signals.append({
                    "type": "bearish_divergence",
                    "severity": "high" if div_strength > 10 else "medium",
                    "message": f"Price rising but regime score declining ({round(score_direction, 1)} pts)",
                    "value": round(score_direction, 1),
                })
            elif price_direction < 0 and score_direction > 3:
                div_strength = abs(score_direction)
                damage_components["momentum_divergence"] = min(
                    100, div_strength * 3
                )
                signals.append({
                    "type": "bullish_divergence",
                    "severity": "medium",
                    "message": f"Price falling but regime score improving (+{round(score_direction, 1)} pts)",
                    "value": round(score_direction, 1),
                })
            else:
                damage_components["momentum_divergence"] = 0
        else:
            damage_components["momentum_divergence"] = 0
    else:
        damage_components["momentum_divergence"] = 0

    # Timeframe Divergence
    if stack is None:
        stack = build_regime_stack(coin, db)
    if not stack.get("incomplete"):
        exec_num = settings.REGIME_NUMERIC.get(
            stack["execution"]["label"] if stack.get("execution") else "Neutral", 0
        )
        trend_num = settings.REGIME_NUMERIC.get(
            stack["trend"]["label"] if stack.get("trend") else "Neutral", 0
        )
        macro_num = settings.REGIME_NUMERIC.get(
            stack["macro"]["label"] if stack.get("macro") else "Neutral", 0
        )
        tf_spread = (
            max(exec_num, trend_num, macro_num)
            - min(exec_num, trend_num, macro_num)
        )
        damage_components["timeframe_divergence"] = min(100, tf_spread * 25)

        if tf_spread >= 3:
            signals.append({
                "type": "timeframe_conflict",
                "severity": "high",
                "message": "Major timeframe disagreement - macro and execution regimes conflict",
                "value": tf_spread,
            })
        elif tf_spread >= 2:
            signals.append({
                "type": "timeframe_tension",
                "severity": "medium",
                "message": "Timeframe tension - trend and execution regimes misaligned",
                "value": tf_spread,
            })
    else:
        damage_components["timeframe_divergence"] = 0

    # Volatility Expansion
    if len(records_1h) >= 8:
        recent_vol = [
            r.volatility_val for r in records_1h[-4:]
            if r.volatility_val
        ]
        prior_vol = [
            r.volatility_val for r in records_1h[-8:-4]
            if r.volatility_val
        ]
        if recent_vol and prior_vol:
            avg_recent_vol = sum(recent_vol) / len(recent_vol)
            avg_prior_vol = sum(prior_vol) / len(prior_vol)
            vol_expansion = (
                ((avg_recent_vol - avg_prior_vol) / avg_prior_vol) * 100
                if avg_prior_vol > 0 else 0
            )
            damage_components["volatility_expansion"] = min(
                100, max(0, vol_expansion * 2)
            )
            if vol_expansion > 30:
                signals.append({
                    "type": "volatility_expansion",
                    "severity": "high" if vol_expansion > 60 else "medium",
                    "message": f"Volatility expanding {round(vol_expansion, 1)}% - instability rising",
                    "value": round(vol_expansion, 1),
                })
        else:
            damage_components["volatility_expansion"] = 0
    else:
        damage_components["volatility_expansion"] = 0

    # Score Trajectory
    if len(records_1h) >= 8:
        scores_recent = [r.score for r in records_1h[-4:]]
        scores_prior = [r.score for r in records_1h[-8:-4]]
        avg_recent_score = sum(scores_recent) / len(scores_recent)
        avg_prior_score = sum(scores_prior) / len(scores_prior)
        score_drift = avg_recent_score - avg_prior_score
        current_label = records_1h[-1].label if records_1h else "Neutral"
        current_num = settings.REGIME_NUMERIC.get(current_label, 0)

        if current_num > 0 and score_drift < -3:
            damage_components["score_deterioration"] = min(
                100, abs(score_drift) * 5
            )
            signals.append({
                "type": "score_deterioration",
                "severity": "medium" if abs(score_drift) > 8 else "low",
                "message": f"Regime score drifting lower ({round(score_drift, 1)} pts) within bullish regime",
                "value": round(score_drift, 1),
            })
        elif current_num < 0 and score_drift > 3:
            damage_components["score_deterioration"] = min(
                100, abs(score_drift) * 3
            )
            signals.append({
                "type": "score_improvement",
                "severity": "low",
                "message": f"Score improving within Risk-Off ({round(score_drift, 1)} pts) - watch for regime shift",
                "value": round(score_drift, 1),
            })
        else:
            damage_components["score_deterioration"] = 0
    else:
        damage_components["score_deterioration"] = 0

    weights = {
        "coherence_rollover": 0.25,
        "momentum_divergence": 0.25,
        "timeframe_divergence": 0.20,
        "volatility_expansion": 0.15,
        "score_deterioration": 0.15,
    }
    damage_score = sum(
        damage_components.get(c, 0) * w for c, w in weights.items()
    )
    damage_score = round(min(100, max(0, damage_score)), 1)

    if damage_score >= 70:
        damage_label = "Severe"
        damage_message = "Internal structure heavily damaged. Regime likely to shift soon."
    elif damage_score >= 50:
        damage_label = "Moderate"
        damage_message = "Internal weakening detected. Reduce new risk. Tighten stops."
    elif damage_score >= 30:
        damage_label = "Mild"
        damage_message = "Minor internal stress. Monitor but no immediate action required."
    else:
        damage_label = "Healthy"
        damage_message = "Internal structure intact. Trend supported by internals."

    return {
        "coin": coin,
        "internal_damage_score": damage_score,
        "damage_label": damage_label,
        "damage_message": damage_message,
        "signals": sorted(
            signals,
            key=lambda x: {"high": 0, "medium": 1, "low": 2}.get(
                x["severity"], 3
            ),
        ),
        "components": damage_components,
        "signal_count": len(signals),
        "high_severity_count": sum(
            1 for s in signals if s["severity"] == "high"
        ),
        "timestamp": datetime.datetime.utcnow().isoformat(),
    }


# -----------------------------------------
# TRADE PLAN ENGINE
# -----------------------------------------
async def compute_trade_plan(
    coin: str,
    account_size: float,
    strategy_mode: str,
    db: Session,
    email: str = None,
    stack: dict = None,
    setup: dict = None,
) -> dict:
    if stack is None:
        stack = build_regime_stack(coin, db)
    if stack.get("incomplete"):
        return {"coin": coin, "error": "Insufficient regime data"}

    if setup is None:
        setup = await compute_setup_quality(coin, db, stack=stack)

    setup_score = setup.get("setup_quality_score") or 50
    entry_mode = setup.get("entry_mode") or "Wait"
    chase_risk = setup.get("chase_risk") or 50
    current_price = setup.get("current_price") or 0
    atr_1h = setup.get("atr_1h") or 0

    exec_label = (
        stack["execution"]["label"] if stack.get("execution") else "Neutral"
    )
    trend_label = (
        stack["trend"]["label"] if stack.get("trend") else "Neutral"
    )
    macro_label = (
        stack["macro"]["label"] if stack.get("macro") else "Neutral"
    )
    exposure = stack.get("exposure") or 50
    hazard = stack.get("hazard") or 50
    shift_risk = stack.get("shift_risk") or 50
    survival = stack.get("survival") or 50
    regime_num = settings.REGIME_NUMERIC.get(exec_label, 0)

    risk_mult = 1.0
    archetype_config = ARCHETYPE_CONFIG.get(
        strategy_mode, ARCHETYPE_CONFIG["swing"]
    )

    if email:
        profile = db.query(UserProfile).filter(
            UserProfile.email == email
        ).first()
        if profile:
            risk_mult = profile.risk_multiplier or 1.0

    adjusted_exposure = round(
        min(95, max(5, exposure * risk_mult * archetype_config["exposure_mult"])), 1
    )

    if regime_num >= 1:
        bias = "Long"
    elif regime_num <= -1:
        bias = "Short / Cash"
    else:
        bias = "Neutral / Reduced"

    band_low = round(max(5, adjusted_exposure * 0.75), 0)
    band_high = round(min(95, adjusted_exposure * 1.25), 0)
    allocation_band = f"{int(band_low)}-{int(band_high)}%"

    if chase_risk > 70:
        entry_style = "Wait for Pullback"
    elif setup_score > 65 and regime_num > 0:
        entry_style = "Pullback - Scale In"
    elif setup_score > 75 and setup.get("range_position", 0) > 85:
        entry_style = "Breakout"
    elif regime_num < 0:
        entry_style = "No Long Entry"
    else:
        entry_style = "Wait for Setup Quality > 60"

    tranches = archetype_config["typical_tranches"]
    deployed_capital = round(account_size * adjusted_exposure / 100, 2)
    tranche_amounts = [
        round(deployed_capital * (t / 100), 2) for t in tranches
    ]

    stop_mult = archetype_config["stop_width_mult"]
    if atr_1h > 0 and current_price > 0:
        if regime_num >= 0:
            stop_price = round(current_price - atr_1h * 2.5 * stop_mult, 2)
            stop_pct = round(
                ((current_price - stop_price) / current_price) * 100, 2
            )
        else:
            stop_price = round(current_price + atr_1h * 2.5 * stop_mult, 2)
            stop_pct = round(
                ((stop_price - current_price) / current_price) * 100, 2
            )
    else:
        stop_price = 0
        stop_pct = 3.0

    invalidation_conditions = []
    if regime_num > 0:
        invalidation_conditions.append("Execution regime shifts to Risk-Off")
        invalidation_conditions.append(
            f"Hazard rate exceeds {int(min(100, hazard + 30))}%"
        )
        invalidation_conditions.append(f"Price breaks below {stop_price}")
        invalidation_conditions.append("4h trend regime downgrades")
    elif regime_num < 0:
        invalidation_conditions.append("Execution regime shifts to Risk-On")
        invalidation_conditions.append(
            f"Price breaks above {round(current_price + atr_1h * 3, 2) if atr_1h else 'resistance'}"
        )
    else:
        invalidation_conditions.append("Regime stack aligns directionally")
        invalidation_conditions.append("Hazard rate exceeds 70%")

    profit_rules = []
    if regime_num > 0:
        if atr_1h > 0:
            tp1 = round(current_price + atr_1h * 2.0, 2)
            tp2 = round(current_price + atr_1h * 4.0, 2)
            profit_rules.append(f"Trim 25% at {tp1} (+{round(atr_1h * 2, 2)})")
            profit_rules.append(f"Trim 25% at {tp2} (+{round(atr_1h * 4, 2)})")
            profit_rules.append(
                f"Trail remaining with stop at {round(current_price + atr_1h * 1.0, 2)}"
            )
        else:
            profit_rules.append("Trim 25% on first extension")
            profit_rules.append("Trim 25% on second extension")
            profit_rules.append("Trail remaining under 4h trend support")
    elif regime_num < 0:
        profit_rules.append("No long profit targets - defensive mode")
        profit_rules.append("Cover shorts on oversold signal or regime upgrade")
    else:
        profit_rules.append("Take quick profits on any 2-3% move")
        profit_rules.append("No holding through Neutral - reduce on strength")

    conditional = []
    if regime_num > 0:
        conditional.append({
            "condition": "Price pulls back 2-3%",
            "action": "Deploy next tranche",
        })
        conditional.append({
            "condition": "Hazard exceeds 65%",
            "action": "Tighten stops and reduce by 20%",
        })
        conditional.append({
            "condition": "Regime shifts to Neutral",
            "action": "Close 50% and trail remainder",
        })
        conditional.append({
            "condition": "Shift risk exceeds 75%",
            "action": "Reduce to minimum allocation",
        })
    elif regime_num < 0:
        conditional.append({
            "condition": "Regime upgrades to Neutral",
            "action": "Scout small positions with tight stops",
        })
        conditional.append({
            "condition": "Capitulation signals appear",
            "action": "Begin deploying first tranche cautiously",
        })
    else:
        conditional.append({
            "condition": "Regime stack aligns bullish",
            "action": "Deploy first two tranches",
        })
        conditional.append({
            "condition": "Regime stack aligns bearish",
            "action": "Move to full cash",
        })

    max_hold = archetype_config["max_hold_days"]
    avg_regime_dur = average_regime_duration(db, coin, "1h")
    estimated_hold = round(min(max_hold, avg_regime_dur / 24 * 1.5), 0)

    risk_per_trade_pct = round(stop_pct * (adjusted_exposure / 100), 2)
    risk_per_trade_usd = round(account_size * risk_per_trade_pct / 100, 2)

    return {
        "coin": coin,
        "current_price": current_price,
        "bias": bias,
        "allocation_band": allocation_band,
        "adjusted_exposure": adjusted_exposure,
        "entry_style": entry_style,
        "setup_quality": setup_score,
        "chase_risk": chase_risk,
        "tranches": {
            "percentages": tranches,
            "amounts": tranche_amounts,
            "deployed_total": deployed_capital,
        },
        "stop": {
            "price": stop_price,
            "distance_pct": stop_pct,
            "type": "ATR-based" if atr_1h > 0 else "Default",
        },
        "risk_per_trade": {
            "pct_of_account": risk_per_trade_pct,
            "usd": risk_per_trade_usd,
        },
        "invalidation": invalidation_conditions,
        "profit_taking": profit_rules,
        "conditional_actions": conditional,
        "time_horizon_days": int(estimated_hold),
        "max_hold_days": max_hold,
        "regime_context": {
            "execution": exec_label,
            "trend": trend_label,
            "macro": macro_label,
            "hazard": hazard,
            "shift_risk": shift_risk,
            "survival": survival,
        },
        "archetype": archetype_config["label"],
        "timestamp": datetime.datetime.utcnow().isoformat(),
    }


# -----------------------------------------
# EVENT RISK OVERLAY
# -----------------------------------------
def compute_event_risk_overlay(
    coin: str, db: Session, stack: dict = None
) -> dict:
    if stack is None:
        stack = build_regime_stack(coin, db)
    exposure = (
        stack.get("exposure") or 50
        if not stack.get("incomplete") else 50
    )
    hazard = (
        stack.get("hazard") or 50
        if not stack.get("incomplete") else 50
    )

    now = datetime.datetime.utcnow()

    KNOWN_SCHEDULE = {
        "FOMC Meeting": {
            "day_of_month_range": (12, 16),
            "months": [1, 3, 5, 6, 7, 9, 11, 12],
        },
        "CPI Release": {
            "day_of_month_range": (10, 14),
            "months": list(range(1, 13)),
        },
        "Options Expiry": {
            "day_of_month_range": (25, 28),
            "months": list(range(1, 13)),
        },
        "ETF Flow Report": {
            "weekday": 4,
            "months": list(range(1, 13)),
        },
        "PCE Inflation": {
            "day_of_month_range": (26, 31),
            "months": list(range(1, 13)),
        },
        "Fed Minutes": {
            "day_of_month_range": (18, 22),
            "months": [1, 2, 4, 5, 7, 8, 10, 11],
        },
        "Jobs Report (NFP)": {
            "day_of_month_range": (1, 7),
            "months": list(range(1, 13)),
        },
        "Quarterly GDP": {
            "day_of_month_range": (25, 30),
            "months": [1, 4, 7, 10],
        },
    }

    active_events = []
    for event in DYNAMIC_RISK_EVENTS:
        schedule = KNOWN_SCHEDULE.get(event["name"])
        if not schedule:
            continue

        hours_until = None
        if "weekday" in schedule:
            target_wd = schedule["weekday"]
            days_ahead = (target_wd - now.weekday()) % 7
            hours_until = (
                max(1, 24 - now.hour)
                if days_ahead == 0
                else days_ahead * 24
            )
        elif "day_of_month_range" in schedule:
            low, high = schedule["day_of_month_range"]
            if now.month in schedule.get("months", []):
                if now.day < low:
                    hours_until = (low - now.day) * 24
                elif now.day <= high:
                    hours_until = max(
                        1, (high - now.day) * 24 + (24 - now.hour)
                    )
                else:
                    hours_until = (30 - now.day + low) * 24
            else:
                for offset in range(1, 13):
                    check_month = ((now.month - 1 + offset) % 12) + 1
                    if check_month in schedule.get("months", []):
                        hours_until = offset * 30 * 24 + low * 24
                        break

        if hours_until is not None:
            active_events.append({**event, "hours_until": int(hours_until)})

    active_events.sort(key=lambda x: x["hours_until"])
    imminent = [e for e in active_events if e["hours_until"] <= 48]
    upcoming = [e for e in active_events if 48 < e["hours_until"] <= 168]

    if imminent:
        max_vol_mult = max(e["typical_vol_multiplier"] for e in imminent)
        max_survival_impact = min(
            e["regime_survival_impact"] for e in imminent
        )
    else:
        max_vol_mult = 1.0
        max_survival_impact = 0

    event_risk_multiplier = round(max_vol_mult, 2)

    if event_risk_multiplier > 1.5:
        exposure_adjustment = -20
        adjustment_label = "Significant Reduction"
        adjustment_message = "High-impact event imminent. Reduce new risk by 20%."
    elif event_risk_multiplier > 1.2:
        exposure_adjustment = -10
        adjustment_label = "Moderate Reduction"
        adjustment_message = "Medium-impact event approaching. Reduce new risk by 10%."
    else:
        exposure_adjustment = 0
        adjustment_label = "No Adjustment"
        adjustment_message = "No imminent high-impact events."

    adjusted_exposure = round(
        max(5, min(95, exposure + exposure_adjustment)), 1
    )
    survival_current = (
        stack.get("survival") or 50
        if not stack.get("incomplete") else 50
    )
    survival_adjusted = round(
        max(0, survival_current + max_survival_impact), 1
    )

    event_guidance = []
    for e in imminent[:3]:
        if e["impact"] == "High":
            event_guidance.append({
                "event": e["name"],
                "hours_until": e["hours_until"],
                "action": f"Reduce position size by 15-25% ahead of {e['name']}",
                "volatility_multiplier": e["typical_vol_multiplier"],
                "stop_guidance": "Widen stops by 50% or reduce size equivalent",
            })
        elif e["impact"] == "Medium":
            event_guidance.append({
                "event": e["name"],
                "hours_until": e["hours_until"],
                "action": f"Consider tightening stops ahead of {e['name']}",
                "volatility_multiplier": e["typical_vol_multiplier"],
                "stop_guidance": "Widen stops by 25% or reduce size slightly",
            })

    return {
        "coin": coin,
        "event_risk_multiplier": event_risk_multiplier,
        "exposure_before_event": exposure,
        "exposure_adjusted": adjusted_exposure,
        "exposure_adjustment": exposure_adjustment,
        "adjustment_label": adjustment_label,
        "adjustment_message": adjustment_message,
        "survival_current": survival_current,
        "survival_adjusted": survival_adjusted,
        "schedule_disclaimer": (
            "Event times are estimated from known recurring schedules. "
            "For precise timing, verify with an official economic calendar."
        ),
        "imminent_events": [
            {
                "name": e["name"],
                "type": e["type"],
                "impact": e["impact"],
                "hours_until": e["hours_until"],
                "vol_multiplier": e["typical_vol_multiplier"],
            }
            for e in imminent[:5]
        ],
        "upcoming_events": [
            {
                "name": e["name"],
                "type": e["type"],
                "impact": e["impact"],
                "hours_until": e["hours_until"],
            }
            for e in upcoming[:5]
        ],
        "event_guidance": event_guidance,
        "timestamp": datetime.datetime.utcnow().isoformat(),
    }


# -----------------------------------------
# ARCHETYPE OVERLAY
# -----------------------------------------
def apply_archetype_overlay(
    coin: str,
    archetype: str,
    db: Session,
    email: str = None,
    stack: dict = None,
) -> dict:
    config = ARCHETYPE_CONFIG.get(archetype, ARCHETYPE_CONFIG["swing"])

    if stack is None:
        stack = build_regime_stack(coin, db)
    if stack.get("incomplete"):
        return {"coin": coin, "error": "Insufficient data", "archetype": archetype}

    base_exposure = stack.get("exposure") or 50
    hazard = stack.get("hazard") or 50
    shift_risk = stack.get("shift_risk") or 50
    survival = stack.get("survival") or 50
    exec_label = (
        stack["execution"]["label"] if stack.get("execution") else "Neutral"
    )

    adjusted_exposure = round(
        min(95, max(5, base_exposure * config["exposure_mult"])), 1
    )

    if config["alert_sensitivity"] == "high":
        alert_shift_risk_threshold = 55
        alert_hazard_threshold = 50
    elif config["alert_sensitivity"] == "low":
        alert_shift_risk_threshold = 80
        alert_hazard_threshold = 70
    else:
        alert_shift_risk_threshold = 70
        alert_hazard_threshold = 60

    should_alert = (
        shift_risk >= alert_shift_risk_threshold
        or hazard >= alert_hazard_threshold
    )
    pb = PLAYBOOK_DATA.get(exec_label, PLAYBOOK_DATA["Neutral"])

    archetype_actions = []
    regime_num = settings.REGIME_NUMERIC.get(exec_label, 0)

    if archetype == "leverage":
        if hazard > 50:
            archetype_actions.append(
                "? Reduce leverage immediately - hazard elevated"
            )
        if regime_num <= 0:
            archetype_actions.append("No leveraged longs in this regime")
        if shift_risk > 60:
            archetype_actions.append(
                "Close leveraged positions - shift risk too high"
            )
    elif archetype == "spot_allocator":
        if regime_num > 0 and hazard < 40:
            archetype_actions.append("Good conditions for DCA allocation")
        elif regime_num < 0:
            archetype_actions.append(
                "Pause DCA - accumulate cash for better entry"
            )
        else:
            archetype_actions.append(
                "Reduce DCA amount - neutral conditions"
            )
    elif archetype == "tactical":
        if shift_risk > 55:
            archetype_actions.append("Actively de-risk - shift risk rising")
        if hazard > 55:
            archetype_actions.append("Tighten all stops by 25%")
        if regime_num > 0 and hazard < 35:
            archetype_actions.append(
                "Tactical add on pullback - conditions favorable"
            )
    elif archetype == "position":
        if regime_num > 0 and survival > 70:
            archetype_actions.append("Hold - regime persistence strong")
        elif hazard > 60:
            archetype_actions.append(
                "Begin scaling out - hazard approaching critical"
            )
        else:
            archetype_actions.append(
                "Monitor daily regime - no change needed"
            )
    else:
        archetype_actions.extend(pb["actions"][:3])

    return {
        "coin": coin,
        "archetype": archetype,
        "archetype_label": config["label"],
        "description": config["description"],
        "base_exposure": base_exposure,
        "adjusted_exposure": adjusted_exposure,
        "exposure_multiplier": config["exposure_mult"],
        "preferred_timeframe": config["preferred_timeframe"],
        "max_hold_days": config["max_hold_days"],
        "stop_width_multiplier": config["stop_width_mult"],
        "alert_sensitivity": config["alert_sensitivity"],
        "should_alert_now": should_alert,
        "alert_thresholds": {
            "shift_risk": alert_shift_risk_threshold,
            "hazard": alert_hazard_threshold,
        },
        "archetype_actions": archetype_actions,
        "playbook_bias": config["playbook_bias"],
        "regime_context": {
            "execution": exec_label,
            "hazard": hazard,
            "shift_risk": shift_risk,
            "survival": survival,
        },
        "timestamp": datetime.datetime.utcnow().isoformat(),
    }


# -----------------------------------------
# BEHAVIORAL ALPHA ENGINE
# -----------------------------------------
def compute_behavioral_alpha_report(
    email: str, db: Session, lookback_days: int = 30
) -> dict:
    cutoff = datetime.datetime.utcnow() - datetime.timedelta(
        days=lookback_days
    )

    logs = (
        db.query(ExposureLog)
        .filter(
            ExposureLog.email == email,
            ExposureLog.created_at >= cutoff,
        )
        .order_by(ExposureLog.created_at.asc())
        .all()
    )

    entries = (
        db.query(PerformanceEntry)
        .filter(
            PerformanceEntry.email == email,
            PerformanceEntry.date >= cutoff,
        )
        .order_by(PerformanceEntry.date.asc())
        .all()
    )

    if len(logs) < 3:
        return {
            "email": email,
            "ready": False,
            "message": f"Need at least 3 exposure logs. Currently have {len(logs)}.",
            "log_count": len(logs),
        }

    leaks = {
        lt: {"count": 0, "instances": [], "estimated_drag": 0}
        for lt in LEAK_TYPES
    }

    log_times = [l.created_at for l in logs]
    log_gaps_hours = []
    for i in range(1, len(log_times)):
        gap = (log_times[i] - log_times[i - 1]).total_seconds() / 3600
        log_gaps_hours.append(gap)

    avg_log_gap = (
        sum(log_gaps_hours) / len(log_gaps_hours)
        if log_gaps_hours else 24
    )

    if avg_log_gap < 4 and len(logs) > 10:
        leaks["overtrading"]["count"] = len(logs)
        leaks["overtrading"]["estimated_drag"] = round(len(logs) * 0.15, 1)
        leaks["overtrading"]["instances"].append({
            "detail": f"Avg {round(avg_log_gap, 1)}h between changes. {len(logs)} adjustments in {lookback_days}d.",
            "period": f"Last {lookback_days} days",
        })

    for log in logs:
        user_exp = log.user_exposure_pct or 0
        model_exp = log.model_exposure_pct or 50
        delta = user_exp - model_exp
        hazard = log.hazard_at_log or 0
        shift_risk = log.shift_risk_at_log or 0
        regime = log.regime_label or "Neutral"

        if delta > 15 and hazard > 50:
            leaks["late_entry_chasing"]["count"] += 1
            leaks["late_entry_chasing"]["estimated_drag"] += round(delta * 0.08, 2)
            leaks["late_entry_chasing"]["instances"].append({
                "date": log.created_at.strftime("%b %d"),
                "delta": round(delta, 1),
                "hazard": hazard,
                "regime": regime,
            })

        if "Risk-Off" in regime and user_exp > model_exp + 15:
            leaks["overexposed_risk_off"]["count"] += 1
            leaks["overexposed_risk_off"]["estimated_drag"] += round(delta * 0.12, 2)
            leaks["overexposed_risk_off"]["instances"].append({
                "date": log.created_at.strftime("%b %d"),
                "user_exp": user_exp,
                "model_exp": model_exp,
                "regime": regime,
            })

        if hazard > 65 and delta > 10:
            leaks["ignored_hazard_spike"]["count"] += 1
            leaks["ignored_hazard_spike"]["estimated_drag"] += round(hazard * 0.05, 2)
            leaks["ignored_hazard_spike"]["instances"].append({
                "date": log.created_at.strftime("%b %d"),
                "hazard": hazard,
                "delta": round(delta, 1),
            })

        if "Risk-On" in regime and "Strong" in regime and delta < -15 and hazard < 40:
            leaks["premature_exit_strength"]["count"] += 1
            leaks["premature_exit_strength"]["estimated_drag"] += round(abs(delta) * 0.06, 2)
            leaks["premature_exit_strength"]["instances"].append({
                "date": log.created_at.strftime("%b %d"),
                "delta": round(delta, 1),
                "regime": regime,
            })

        if "Risk-Off" in regime and delta > 20:
            prev_logs = [
                pl for pl in logs
                if pl.created_at < log.created_at
                and (log.created_at - pl.created_at).total_seconds() < 86400
            ]
            if prev_logs:
                prev_delta = (
                    prev_logs[-1].user_exposure_pct
                    - prev_logs[-1].model_exposure_pct
                )
                if delta > prev_delta + 5:
                    leaks["averaging_down_risk_off"]["count"] += 1
                    leaks["averaging_down_risk_off"]["estimated_drag"] += round(delta * 0.15, 2)
                    leaks["averaging_down_risk_off"]["instances"].append({
                        "date": log.created_at.strftime("%b %d"),
                        "delta": round(delta, 1),
                        "regime": regime,
                    })

        if abs(delta) > 25:
            leaks["size_too_large"]["count"] += 1
            leaks["size_too_large"]["estimated_drag"] += round(abs(delta) * 0.04, 2)
            leaks["size_too_large"]["instances"].append({
                "date": log.created_at.strftime("%b %d"),
                "delta": round(delta, 1),
                "regime": regime,
            })

        if "Strong Risk-On" in regime and delta < -10 and hazard < 30:
            leaks["failed_to_press_edge"]["count"] += 1
            leaks["failed_to_press_edge"]["estimated_drag"] += round(abs(delta) * 0.05, 2)
            leaks["failed_to_press_edge"]["instances"].append({
                "date": log.created_at.strftime("%b %d"),
                "delta": round(delta, 1),
                "regime": regime,
                "hazard": hazard,
            })

    active_leaks = []
    total_drag = 0

    for leak_type, data in leaks.items():
        if data["count"] > 0:
            config = LEAK_TYPES[leak_type]
            weighted_drag = round(
                data["estimated_drag"] * config["severity_weight"], 1
            )
            total_drag += weighted_drag
            active_leaks.append({
                "type": leak_type,
                "label": config["label"],
                "description": config["description"],
                "frequency": data["count"],
                "estimated_alpha_drag_pct": weighted_drag,
                "severity_weight": config["severity_weight"],
                "instances": data["instances"][:5],
            })

    active_leaks.sort(
        key=lambda x: x["estimated_alpha_drag_pct"], reverse=True
    )

    strengths = []
    followed_count = sum(1 for l in logs if l.followed_model)
    follow_rate = (followed_count / len(logs)) * 100 if logs else 0

    if follow_rate > 70:
        strengths.append(
            f"Strong model adherence ({round(follow_rate)}% follow rate)"
        )

    risk_off_logs = [
        l for l in logs if "Risk-Off" in (l.regime_label or "")
    ]
    if risk_off_logs:
        risk_off_followed = sum(1 for l in risk_off_logs if l.followed_model)
        risk_off_rate = (risk_off_followed / len(risk_off_logs)) * 100
        if risk_off_rate > 60:
            strengths.append(
                f"Good defensive discipline ({round(risk_off_rate)}% in Risk-Off)"
            )

    hazard_spike_logs = [
        l for l in logs if (l.hazard_at_log or 0) > 60
    ]
    if hazard_spike_logs:
        reduced = sum(
            1 for l in hazard_spike_logs
            if (l.user_exposure_pct or 0) < (l.model_exposure_pct or 50)
        )
        if reduced > len(hazard_spike_logs) * 0.5:
            strengths.append("Responds well to hazard spikes")

    recommendations = []
    for leak in active_leaks[:3]:
        if leak["type"] == "late_entry_chasing":
            recommendations.append(
                "Use the Setup Quality score to avoid chasing. Wait for chase risk < 50 before entering."
            )
        elif leak["type"] == "overexposed_risk_off":
            recommendations.append(
                "Set a hard rule: max exposure = model recommendation in Risk-Off."
            )
        elif leak["type"] == "ignored_hazard_spike":
            recommendations.append(
                "Enable hazard alerts. When hazard > 65, reduce exposure within 1 hour."
            )
        elif leak["type"] == "overtrading":
            recommendations.append(
                "Limit exposure changes to once per regime shift."
            )
        elif leak["type"] == "size_too_large":
            recommendations.append(
                "Use the Portfolio Allocator to right-size positions."
            )
        elif leak["type"] == "averaging_down_risk_off":
            recommendations.append(
                "Never add to positions in Risk-Off."
            )
        elif leak["type"] == "failed_to_press_edge":
            recommendations.append(
                "In Strong Risk-On with low hazard, trust the model."
            )
        elif leak["type"] == "premature_exit_strength":
            recommendations.append(
                "In strong regimes with low hazard, hold longer."
            )

    if total_drag <= 2:
        behavior_grade, behavior_label = "A", "Excellent"
    elif total_drag <= 5:
        behavior_grade, behavior_label = "B+", "Good"
    elif total_drag <= 10:
        behavior_grade, behavior_label = "B", "Above Average"
    elif total_drag <= 20:
        behavior_grade, behavior_label = "C", "Needs Improvement"
    else:
        behavior_grade, behavior_label = "D", "Significant Leaks"

    return {
        "email": email,
        "ready": True,
        "lookback_days": lookback_days,
        "log_count": len(logs),
        "performance_count": len(entries),
        "behavior_grade": behavior_grade,
        "behavior_label": behavior_label,
        "total_estimated_alpha_drag_pct": round(total_drag, 1),
        "leaks": active_leaks,
        "strengths": strengths,
        "recommendations": recommendations,
        "follow_rate": round(follow_rate, 1),
        "timestamp": datetime.datetime.utcnow().isoformat(),
    }


# -----------------------------------------
# DISCIPLINE SCORE
# -----------------------------------------
def compute_discipline_score(logs: list) -> dict:
    if not logs:
        return {
            "score": None,
            "label": "No data yet",
            "flags": [],
            "summary": "Log your exposure to start tracking discipline.",
        }

    total_logs = len(logs)
    followed = sum(1 for l in logs if l.followed_model)
    base_score = (
        round((followed / total_logs) * 100, 1) if total_logs > 0 else 50
    )
    flags = []
    penalties = 0
    bonuses = 0

    for log in logs:
        hazard = log.hazard_at_log or 0
        shift_risk = log.shift_risk_at_log or 0
        user_exp = log.user_exposure_pct or 0
        model_exp = log.model_exposure_pct or 50
        delta = user_exp - model_exp

        if hazard > 65 and delta > 10:
            flags.append({
                "type": "penalty",
                "label": "Added leverage in elevated hazard",
                "date": log.created_at.strftime("%b %d"),
                "regime": log.regime_label,
            })
            penalties += 10

        if "Risk-Off" in (log.regime_label or "") and user_exp > model_exp + 15:
            flags.append({
                "type": "penalty",
                "label": "Over-exposed in Risk-Off regime",
                "date": log.created_at.strftime("%b %d"),
                "regime": log.regime_label,
            })
            penalties += 15

        if shift_risk > 70 and delta < -5:
            flags.append({
                "type": "bonus",
                "label": "Reduced exposure on hazard spike",
                "date": log.created_at.strftime("%b %d"),
                "regime": log.regime_label,
            })
            bonuses += 10

        if "Strong Risk-On" in (log.regime_label or "") and abs(delta) < 10:
            flags.append({
                "type": "bonus",
                "label": "Stayed within band in strong regime",
                "date": log.created_at.strftime("%b %d"),
                "regime": log.regime_label,
            })
            bonuses += 5

    final_score = round(min(100, max(0, base_score + bonuses - penalties)), 1)

    if final_score >= 85:
        label = "Excellent"
    elif final_score >= 70:
        label = "Good"
    elif final_score >= 50:
        label = "Average"
    elif final_score >= 30:
        label = "Needs Work"
    else:
        label = "Poor"

    return {
        "score": final_score,
        "label": label,
        "flags": flags[-10:],
        "followed": followed,
        "total": total_logs,
        "bonuses": bonuses,
        "penalties": penalties,
        "summary": f"You followed the model {followed}/{total_logs} times.",
    }


# -----------------------------------------
# PERFORMANCE COMPARISON
# -----------------------------------------
def compute_performance_comparison(entries: list) -> dict:
    if len(entries) < 3:
        return {
            "user_total_return": None,
            "model_total_return": None,
            "alpha": None,
            "periods": len(entries),
            "message": "Need at least 3 entries for comparison.",
        }

    user_returns = [
        e.user_return_pct for e in entries
        if e.user_return_pct is not None
    ]
    model_returns = [
        e.model_return_pct for e in entries
        if e.model_return_pct is not None
    ]

    if not user_returns or not model_returns:
        return {
            "user_total_return": None,
            "model_total_return": None,
            "alpha": None,
        }

    def compound(returns):
        result = 1.0
        for r in returns:
            result *= (1 + r / 100)
        return round((result - 1) * 100, 2)

    user_total = compound(user_returns)
    model_total = compound(model_returns)
    alpha = round(user_total - model_total, 2)

    regime_perf = {}
    for e in entries:
        label = e.regime_label or "Neutral"
        if label not in regime_perf:
            regime_perf[label] = {"user": [], "model": []}
        if e.user_return_pct is not None:
            regime_perf[label]["user"].append(e.user_return_pct)
        if e.model_return_pct is not None:
            regime_perf[label]["model"].append(e.model_return_pct)

    regime_summary = {}
    for label, data in regime_perf.items():
        if data["user"] and data["model"]:
            regime_summary[label] = {
                "user_avg": round(sum(data["user"]) / len(data["user"]), 2),
                "model_avg": round(sum(data["model"]) / len(data["model"]), 2),
                "count": len(data["user"]),
            }

    best_regime = max(
        regime_summary.items(),
        key=lambda x: x[1]["user_avg"],
        default=(None, {}),
    )
    worst_regime = min(
        regime_summary.items(),
        key=lambda x: x[1]["user_avg"],
        default=(None, {}),
    )

    curve = []
    user_cum = 1.0
    model_cum = 1.0
    for i, e in enumerate(entries):
        user_cum *= (1 + (e.user_return_pct or 0) / 100)
        model_cum *= (1 + (e.model_return_pct or 0) / 100)
        curve.append({
            "period": i + 1,
            "user_cum": round((user_cum - 1) * 100, 2),
            "model_cum": round((model_cum - 1) * 100, 2),
            "date": e.date.strftime("%b %d") if e.date else "",
            "regime": e.regime_label or "-",
        })

    return {
        "user_total_return": user_total,
        "model_total_return": model_total,
        "alpha": alpha,
        "periods": len(entries),
        "regime_breakdown": regime_summary,
        "best_regime": best_regime[0],
        "worst_regime": worst_regime[0],
        "curve": curve,
        "message": (
            f"Following ChainPulse would have returned {model_total:+.1f}%. "
            f"Your actual: {user_total:+.1f}%."
        ),
    }


# -----------------------------------------
# MISTAKE REPLAY
# -----------------------------------------
def compute_mistake_replay(
    logs: list, db: Session, coin: str
) -> list:
    replays = []
    for log in logs:
        hazard = log.hazard_at_log or 0
        shift_risk = log.shift_risk_at_log or 0
        user_exp = log.user_exposure_pct or 0
        model_exp = log.model_exposure_pct or 50
        delta = user_exp - model_exp
        regime = log.regime_label or "Neutral"

        if (hazard > 55 or shift_risk > 60) and abs(delta) > 12:
            severity = (
                "high"
                if (hazard > 70 or shift_risk > 75) and abs(delta) > 20
                else "medium" if abs(delta) > 15
                else "low"
            )
            direction = "over-exposed" if delta > 0 else "under-exposed"
            replays.append({
                "date": log.created_at.strftime("%b %d, %Y"),
                "regime": regime,
                "hazard": hazard,
                "shift_risk": shift_risk,
                "user_exp": user_exp,
                "model_exp": model_exp,
                "delta": round(delta, 1),
                "direction": direction,
                "severity": severity,
                "message": (
                    f"You were {direction} by {abs(round(delta, 1))}% "
                    f"while hazard was {hazard}% in {regime} regime."
                ),
                "signals_at_time": {
                    "hazard": hazard,
                    "shift_risk": shift_risk,
                    "alignment": log.alignment_at_log or 0,
                },
            })

    return sorted(
        replays,
        key=lambda x: x["severity"] == "high",
        reverse=True,
    )[:10]


# -----------------------------------------
# IF NOTHING PANEL
# -----------------------------------------
def compute_if_nothing_panel(
    user_exposure: float,
    model_exposure: float,
    hazard: float,
    shift_risk: float,
    regime_label: str,
) -> dict:
    delta = user_exposure - model_exposure
    over_exposed = delta > 0
    delta_abs = abs(round(delta, 1))

    base_dd_prob = round((hazard * 0.5 + shift_risk * 0.5), 1)
    exposure_multiplier = (
        1 + (delta / 100) * 0.8 if over_exposed else 1.0
    )
    adj_dd_prob = round(min(95, base_dd_prob * exposure_multiplier), 1)
    dd_prob_increase = round(adj_dd_prob - base_dd_prob, 1)
    dd_magnitude = round((hazard / 100) * 0.25 * 100, 1)
    expected_loss_pct = round(
        (user_exposure / 100) * (dd_magnitude / 100) * 100, 1
    )
    model_loss_pct = round(
        (model_exposure / 100) * (dd_magnitude / 100) * 100, 1
    )

    if over_exposed and delta_abs > 15:
        severity = "high"
        message = f"You are {delta_abs}% over regime tolerance"
        sub = "Maintaining this exposure significantly increases drawdown probability."
    elif over_exposed and delta_abs > 5:
        severity = "medium"
        message = f"You are {delta_abs}% above optimal"
        sub = "Small overexposure - consider trimming on the next strength."
    elif not over_exposed:
        severity = "low"
        message = f"You are {delta_abs}% below optimal - room to add"
        sub = "Consider scaling in on the next pullback if signals hold."
    else:
        severity = "low"
        message = "Exposure aligned with regime recommendation"
        sub = "No action required."

    return {
        "user_exposure": round(user_exposure, 1),
        "model_exposure": round(model_exposure, 1),
        "delta": round(delta, 1),
        "delta_abs": delta_abs,
        "over_exposed": over_exposed,
        "severity": severity,
        "message": message,
        "sub": sub,
        "drawdown_prob": adj_dd_prob,
        "dd_prob_increase": dd_prob_increase,
        "expected_loss_pct": expected_loss_pct,
        "model_loss_pct": model_loss_pct,
        "dd_magnitude_est": dd_magnitude,
        "regime_label": regime_label,
    }


# -----------------------------------------
# WHAT CHANGED
# -----------------------------------------
def compute_what_changed(
    db: Session, lookback_hours: int = 24
) -> dict:
    cutoff = datetime.datetime.utcnow() - datetime.timedelta(
        hours=lookback_hours
    )

    changes = []
    current_states = {}

    for coin in settings.SUPPORTED_COINS:
        current_stack = build_regime_stack(coin, db)
        if current_stack["incomplete"]:
            continue

        current_states[coin] = {
            "macro": current_stack["macro"]["label"] if current_stack.get("macro") else None,
            "trend": current_stack["trend"]["label"] if current_stack.get("trend") else None,
            "execution": current_stack["execution"]["label"] if current_stack.get("execution") else None,
            "exposure": current_stack.get("exposure"),
            "shift_risk": current_stack.get("shift_risk"),
            "hazard": current_stack.get("hazard"),
            "alignment": current_stack.get("alignment"),
        }

        for tf in ["1d", "4h", "1h"]:
            prev_record = (
                db.query(MarketSummary)
                .filter(
                    MarketSummary.coin == coin,
                    MarketSummary.timeframe == tf,
                    MarketSummary.created_at <= cutoff,
                )
                .order_by(MarketSummary.created_at.desc())
                .first()
            )
            current_record = (
                db.query(MarketSummary)
                .filter(
                    MarketSummary.coin == coin,
                    MarketSummary.timeframe == tf,
                )
                .order_by(MarketSummary.created_at.desc())
                .first()
            )

            if (
                prev_record
                and current_record
                and prev_record.label != current_record.label
            ):
                prev_num = settings.REGIME_NUMERIC.get(prev_record.label, 0)
                curr_num = settings.REGIME_NUMERIC.get(current_record.label, 0)
                direction = "upgraded" if curr_num > prev_num else "downgraded"
                severity = "positive" if curr_num > prev_num else "negative"
                tf_label = settings.TIMEFRAME_LABELS.get(tf, tf)

                changes.append({
                    "coin": coin,
                    "timeframe": tf,
                    "timeframe_label": tf_label,
                    "previous": prev_record.label,
                    "current": current_record.label,
                    "direction": direction,
                    "severity": severity,
                    "score_change": round(
                        current_record.score - prev_record.score, 2
                    ),
                    "message": (
                        f"{coin} {tf_label} regime {direction}: "
                        f"{prev_record.label} ? {current_record.label}"
                    ),
                })

    exposure_changes = []
    for coin, state in current_states.items():
        if state["shift_risk"] and state["shift_risk"] > 65:
            exposure_changes.append({
                "coin": coin,
                "type": "risk_warning",
                "message": f"{coin} shift risk at {state['shift_risk']}% - elevated",
            })

    breadth = compute_market_breadth(db)
    upgrade_count = sum(1 for c in changes if c["direction"] == "upgraded")
    downgrade_count = sum(1 for c in changes if c["direction"] == "downgraded")

    if not changes:
        headline = "No regime changes in the last 24 hours"
        tone = "stable"
    elif upgrade_count > downgrade_count:
        headline = f"{upgrade_count} regime upgrades vs {downgrade_count} downgrades - market improving"
        tone = "improving"
    elif downgrade_count > upgrade_count:
        headline = f"{downgrade_count} regime downgrades vs {upgrade_count} upgrades - market deteriorating"
        tone = "deteriorating"
    else:
        headline = f"{len(changes)} regime changes - mixed signals"
        tone = "mixed"

    takeaways = []
    high_impact_changes = [
        c for c in changes if c["timeframe"] in ("1d", "4h")
    ]
    if high_impact_changes:
        for c in high_impact_changes[:3]:
            takeaways.append(c["message"])
    else:
        takeaways.append("No major timeframe changes - short-term noise only")

    if breadth.get("breadth_score", 0) > 50:
        takeaways.append(f"Market breadth bullish ({breadth['breadth_score']})")
    elif breadth.get("breadth_score", 0) < -50:
        takeaways.append(f"Market breadth bearish ({breadth['breadth_score']})")

    return {
        "lookback_hours": lookback_hours,
        "headline": headline,
        "tone": tone,
        "changes": changes,
        "change_count": len(changes),
        "upgrades": upgrade_count,
        "downgrades": downgrade_count,
        "exposure_warnings": exposure_changes,
        "breadth": breadth,
        "takeaways": takeaways,
        "current_states": current_states,
        "timestamp": datetime.datetime.utcnow().isoformat(),
    }

# ─────────────────────────────────────────
# INTELLIGENCE BRIEF CACHE ENGINE
# Stores expensive computed briefs in DB
# so they survive server restarts
# ─────────────────────────────────────────
import json as _json
from app.db.models import IntelligenceBrief


def save_intelligence_brief(
    db: Session,
    brief_type: str,
    content: dict,
) -> None:
    """
    Saves a computed brief to the DB.
    Overwrites existing brief of same type.
    Used to persist expensive computations across restarts.
    """
    existing = (
        db.query(IntelligenceBrief)
        .filter(IntelligenceBrief.brief_type == brief_type)
        .order_by(IntelligenceBrief.created_at.desc())
        .first()
    )

    if existing:
        existing.content_json = _json.dumps(content)
        existing.created_at = datetime.datetime.utcnow()
    else:
        brief = IntelligenceBrief(
            brief_type=brief_type,
            content_json=_json.dumps(content),
        )
        db.add(brief)

    db.commit()


def get_intelligence_brief(
    db: Session,
    brief_type: str,
    max_age_minutes: int = 60,
) -> dict | None:
    """
    Retrieves a cached brief from DB.
    Returns None if not found or too old.
    Used as fallback when live computation fails.
    """
    brief = (
        db.query(IntelligenceBrief)
        .filter(IntelligenceBrief.brief_type == brief_type)
        .order_by(IntelligenceBrief.created_at.desc())
        .first()
    )

    if not brief:
        return None

    age_minutes = (
        datetime.datetime.utcnow() - brief.created_at
    ).total_seconds() / 60

    if age_minutes > max_age_minutes:
        return None

    try:
        return _json.loads(brief.content_json)
    except Exception:
        return None


def get_or_compute_brief(
    db: Session,
    brief_type: str,
    compute_fn,
    max_age_minutes: int = 60,
    **kwargs,
) -> dict:
    """
    Check DB cache first, compute and save if missing or stale.
    Drop-in replacement for expensive brief computations.
    """
    cached = get_intelligence_brief(db, brief_type, max_age_minutes)
    if cached:
        return cached

    result = compute_fn(**kwargs)
    if result:
        try:
            save_intelligence_brief(db, brief_type, result)
        except Exception:
            pass

    return result

