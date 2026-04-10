import math
import logging
import asyncio
import datetime
from typing import Optional
from sqlalchemy.orm import Session

from app.core.cache import cache_get, cache_set
from app.core.config import settings
from app.core.startup import httpx_client
from app.db.models import MarketSummary

logger = logging.getLogger("chainpulse")

async def get_klines(symbol: str, interval: str, limit: int = 120):
    """
    FIX 7: Async kline fetcher using httpx.
    Cache-first with stale fallback.
    Uses fresh client per call to avoid stale global reference.
    """
    cache_key = f"klines:{symbol}:{interval}:{limit}"
    cached = cache_get(cache_key)

    urls = [
        "[api.binance.com](https://api.binance.com/api/v3/klines)",
        "[api.binance.us](https://api.binance.us/api/v3/klines)",
    ]
    params = {
        "symbol": f"{symbol}USDT",
        "interval": interval,
        "limit": limit,
    }

    for url in urls:
        try:
            import httpx as _httpx
            async with _httpx.AsyncClient(timeout=10) as client:
                r = await client.get(url, params=params)
                r.raise_for_status()
                data = r.json()
                if not isinstance(data, list) or len(data) == 0:
                    continue
                prices = [float(c[4]) for c in data]
                volumes = [float(c[5]) for c in data]
                logger.info(
                    f"Got {len(prices)} candles for {symbol}/{interval}"
                )
                cache_set(
                    cache_key,
                    {"prices": prices, "volumes": volumes},
                    ttl=300,
                )
                return prices, volumes
        except Exception as e:
            logger.error(
                f"Kline fetch failed {url} {symbol}/{interval}: {e}"
            )
            continue

    # Fallback to stale cached data
    if cached:
        logger.warning(f"Using stale kline data for {symbol}/{interval}")
        return cached["prices"], cached["volumes"]

    return [], []




# -----------------------------------------
# FIX 7: Async bulk market data fetcher
# -----------------------------------------
async def fetch_all_market_data(coin: str) -> dict:
    """
    FIX 7: Fetches 1h, 4h, 1d kline data concurrently using asyncio.gather.
    Returns dict keyed by timeframe with prices and volumes.
    """
    cache_key = f"market_data_all:{coin}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    results = await asyncio.gather(
        get_klines(coin, "1h", limit=120),
        get_klines(coin, "4h", limit=60),
        get_klines(coin, "1d", limit=90),
        return_exceptions=True,
    )

    market_data = {}
    for tf, result in zip(["1h", "4h", "1d"], results):
        if isinstance(result, Exception):
            logger.error(f"fetch_all_market_data {coin}/{tf}: {result}")
            market_data[tf] = {"prices": [], "volumes": []}
        else:
            prices, volumes = result
            market_data[tf] = {"prices": prices, "volumes": volumes}

    cache_set(cache_key, market_data, ttl=60)
    return market_data


# -----------------------------------------
# PURE MATH HELPERS (unchanged logic)
# -----------------------------------------
def volatility(prices: list, period: int = 20) -> float:
    if len(prices) < period:
        return 0.0
    subset = prices[-period:]
    mean = sum(subset) / len(subset)
    var = sum((p - mean) ** 2 for p in subset) / len(subset)
    return math.sqrt(var)


def volume_momentum(volumes: list, period: int = 10) -> float:
    if len(volumes) < period * 2:
        return 0.0
    recent = sum(volumes[-period:]) / period
    prior = sum(volumes[-period * 2:-period]) / period
    if prior == 0:
        return 0.0
    return ((recent - prior) / prior) * 100


def calculate_coherence(
    mom_short: float,
    mom_long: float,
    vol_score: float,
) -> float:
    if (mom_short >= 0 and mom_long >= 0) or (
        mom_short < 0 and mom_long < 0
    ):
        alignment = 1.0
    else:
        alignment = 0.3
    magnitude = (abs(mom_short) + abs(mom_long)) / 2
    magnitude_norm = min(magnitude / 5.0, 1.0) * 100
    vol_penalty = min(vol_score / 500, 0.5)
    raw = alignment * magnitude_norm * (1 - vol_penalty)
    return round(max(0, min(100, raw)), 2)


def classify(score: float) -> str:
    if score > 35:
        return "Strong Risk-On"
    if score > 15:
        return "Risk-On"
    if score < -35:
        return "Strong Risk-Off"
    if score < -15:
        return "Risk-Off"
    return "Neutral"


async def calculate_score_for_timeframe(
    coin: str,
    interval: str,
    market_data: dict = None,
) -> Optional[dict]:
    """FIX 7: Async. Accepts optional pre-fetched market_data."""
    if market_data and interval in market_data:
        prices = market_data[interval]["prices"]
        volumes = market_data[interval]["volumes"]
    else:
        prices, volumes = await get_klines(coin, interval, limit=120)

    if len(prices) < 30:
        return None

    if interval == "1h":
        short_lb, long_lb = 4, 24
    elif interval == "4h":
        short_lb, long_lb = 6, 24
    else:
        short_lb, long_lb = 7, 30

    if len(prices) < long_lb + 1:
        return None

    mom_short = ((prices[-1] - prices[-short_lb]) / prices[-short_lb]) * 100
    mom_long = ((prices[-1] - prices[-long_lb]) / prices[-long_lb]) * 100
    vol = volatility(prices)
    vol_mom = volume_momentum(volumes)
    score = (
        0.55 * mom_long
        + 0.35 * mom_short
        - 0.08 * vol
        + 0.02 * vol_mom
    )
    score = max(-100, min(100, score))
    coherence = calculate_coherence(mom_short, mom_long, vol)
    return {
        "score": round(score, 4),
        "mom_short": round(mom_short, 4),
        "mom_long": round(mom_long, 4),
        "volatility": round(vol, 4),
        "coherence": coherence,
    }


# -----------------------------------------
# REGIME ALIGNMENT ENGINE
# -----------------------------------------
def regime_alignment(labels: list) -> float:
    scores = [settings.REGIME_NUMERIC.get(l, 0) for l in labels]
    if not scores:
        return 0.0
    max_sum = 2 * len(scores)
    return round((abs(sum(scores)) / max_sum) * 100, 2)


def alignment_direction(labels: list) -> str:
    scores = [settings.REGIME_NUMERIC.get(l, 0) for l in labels]
    total = sum(scores)
    if total > 0:
        return "bullish"
    if total < 0:
        return "bearish"
    return "mixed"


# -----------------------------------------
# STATISTICS ENGINE
# -----------------------------------------
def get_history(db: Session, coin: str, timeframe: str = "1h"):
    return (
        db.query(MarketSummary)
        .filter(
            MarketSummary.coin == coin,
            MarketSummary.timeframe == timeframe,
        )
        .order_by(MarketSummary.created_at.asc())
        .all()
    )


def regime_durations(
    db: Session, coin: str, timeframe: str = "1h"
) -> list:
    records = get_history(db, coin, timeframe)
    if not records:
        return []
    durations = []
    current_label = records[0].label
    start_time = records[0].created_at
    for r in records[1:]:
        if r.label != current_label:
            d = (r.created_at - start_time).total_seconds() / 3600
            if d > 0:
                durations.append(d)
            current_label = r.label
            start_time = r.created_at
    return durations


def current_age(
    db: Session, coin: str, timeframe: str = "1h"
) -> float:
    records = (
        db.query(MarketSummary)
        .filter(
            MarketSummary.coin == coin,
            MarketSummary.timeframe == timeframe,
        )
        .order_by(MarketSummary.created_at.desc())
        .all()
    )
    if not records:
        return 0.0
    latest_label = records[0].label
    start_time = records[0].created_at
    for r in records:
        if r.label != latest_label:
            break
        start_time = r.created_at
    return (
        datetime.datetime.utcnow() - start_time
    ).total_seconds() / 3600


def survival_probability(
    db: Session, coin: str, timeframe: str = "1h"
) -> float:
    durations = regime_durations(db, coin, timeframe)
    age = current_age(db, coin, timeframe)
    if len(durations) < 5:
        return round(max(20.0, 90.0 - age * 4), 2)
    longer = [d for d in durations if d > age]
    return round((len(longer) / len(durations)) * 100, 2)


def hazard_rate(
    db: Session, coin: str, timeframe: str = "1h"
) -> float:
    durations = regime_durations(db, coin, timeframe)
    age = current_age(db, coin, timeframe)
    if len(durations) < 5:
        return round(min(70.0, age * 5), 2)
    avg = sum(durations) / len(durations)
    return round(min(100.0, (age / (avg + 0.01)) * 100), 2)


def percentile_rank(
    db: Session,
    coin: str,
    current_score: float,
    timeframe: str = "1h",
) -> float:
    scores = [r.score for r in get_history(db, coin, timeframe)]
    if len(scores) < 5:
        return round(50 + current_score / 2, 2)
    lower = [s for s in scores if s < current_score]
    return round((len(lower) / len(scores)) * 100, 2)


def average_regime_duration(
    db: Session, coin: str, timeframe: str = "1h"
) -> float:
    durations = regime_durations(db, coin, timeframe)
    if not durations:
        return 24.0
    return sum(durations) / len(durations)


def trend_maturity_score(
    age: float, avg_duration: float, hazard: float
) -> float:
    if avg_duration == 0:
        age_component = min(100, age * 5)
    else:
        age_component = min(100, (age / avg_duration) * 100)
    return round(min(100, max(0, age_component * 0.6 + hazard * 0.4)), 2)


def regime_shift_risk(
    hazard: float, survival: float, coherence: float
) -> float:
    return round(
        min(
            100.0,
            hazard * 0.50
            + (100 - survival) * 0.35
            + (100 - coherence) * 0.15,
        ),
        2,
    )


def exposure_recommendation(
    score: float,
    survival: float,
    hazard: float,
    coherence: float,
) -> float:
    if score > 35:
        base = 0.85
    elif score > 15:
        base = 0.65
    elif score < -35:
        base = 0.08
    elif score < -15:
        base = 0.22
    else:
        base = 0.42
    persistence_factor = survival / 100
    hazard_penalty = 1 - (hazard / 100) * 0.65
    coherence_factor = 0.7 + (coherence / 100) * 0.3
    exposure = base * persistence_factor * hazard_penalty * coherence_factor
    return round(max(5.0, min(95.0, exposure * 100)), 2)


def exposure_recommendation_stacked(
    macro_label: str,
    trend_label: str,
    exec_label: str,
    alignment: float,
    survival_1h: float,
    hazard_1h: float,
    coherence_1h: float,
) -> float:
    macro_num = settings.REGIME_NUMERIC.get(macro_label, 0)
    if macro_num >= 1:
        macro_ceiling, macro_floor = 0.90, 0.30
    elif macro_num == 0:
        macro_ceiling, macro_floor = 0.60, 0.20
    else:
        macro_ceiling, macro_floor = 0.35, 0.05

    trend_num = settings.REGIME_NUMERIC.get(trend_label, 0)
    rang = macro_ceiling - macro_floor
    if trend_num == 2:
        base = macro_ceiling
    elif trend_num == 1:
        base = macro_floor + rang * 0.75
    elif trend_num == 0:
        base = macro_floor + rang * 0.50
    elif trend_num == -1:
        base = macro_floor + rang * 0.25
    else:
        base = macro_floor

    exec_num = settings.REGIME_NUMERIC.get(exec_label, 0)
    base = base + (exec_num / 2) * 0.10

    persistence_factor = survival_1h / 100
    hazard_penalty = 1 - (hazard_1h / 100) * 0.65
    coherence_factor = 0.7 + (coherence_1h / 100) * 0.3
    alignment_mult = 0.5 + alignment / 200

    exposure = (
        base
        * persistence_factor
        * hazard_penalty
        * coherence_factor
        * alignment_mult
    )
    return round(max(5.0, min(95.0, exposure * 100)), 2)


# -----------------------------------------
# REGIME STACK BUILDER
# -----------------------------------------
def build_regime_stack(coin: str, db: Session) -> dict:
    stack = {}
    labels = []
    coherences = []

    for tf in ["1d", "4h", "1h"]:
        record = (
            db.query(MarketSummary)
            .filter(
                MarketSummary.coin == coin,
                MarketSummary.timeframe == tf,
            )
            .order_by(MarketSummary.created_at.desc())
            .first()
        )
        if record:
            stack[tf] = {
                "label": record.label,
                "score": record.score,
                "coherence": record.coherence,
                "timestamp": record.created_at,
            }
            labels.append(record.label)
            coherences.append(record.coherence)
        else:
            stack[tf] = None

    if len(labels) < 3:
        return {
            "coin": coin,
            "macro": stack.get("1d"),
            "trend": stack.get("4h"),
            "execution": stack.get("1h"),
            "alignment": None,
            "direction": None,
            "exposure": None,
            "shift_risk": None,
            "survival": None,
            "hazard": None,
            "incomplete": True,
        }

    align = regime_alignment(labels)
    direction = alignment_direction(labels)
    avg_coh = sum(coherences) / len(coherences)
    survival_1h = survival_probability(db, coin, "1h")
    hazard_1h = hazard_rate(db, coin, "1h")

    exposure = exposure_recommendation_stacked(
        macro_label=stack["1d"]["label"],
        trend_label=stack["4h"]["label"],
        exec_label=stack["1h"]["label"],
        alignment=align,
        survival_1h=survival_1h,
        hazard_1h=hazard_1h,
        coherence_1h=stack["1h"]["coherence"],
    )
    shift_risk = regime_shift_risk(hazard_1h, survival_1h, avg_coh)

    return {
        "coin": coin,
        "macro": stack["1d"],
        "trend": stack["4h"],
        "execution": stack["1h"],
        "alignment": align,
        "direction": direction,
        "exposure": exposure,
        "shift_risk": shift_risk,
        "survival": survival_1h,
        "hazard": hazard_1h,
        "incomplete": False,
    }


# -----------------------------------------
# MARKET BREADTH
# -----------------------------------------
def compute_market_breadth(db: Session) -> dict:
    bullish = neutral = bearish = 0
    for coin in settings.SUPPORTED_COINS:
        record = (
            db.query(MarketSummary)
            .filter(
                MarketSummary.coin == coin,
                MarketSummary.timeframe == "1d",
            )
            .order_by(MarketSummary.created_at.desc())
            .first()
        )
        if not record:
            continue
        n = settings.REGIME_NUMERIC.get(record.label, 0)
        if n > 0:
            bullish += 1
        elif n < 0:
            bearish += 1
        else:
            neutral += 1

    total = bullish + neutral + bearish
    if total == 0:
        return {
            "bullish": 0, "neutral": 0, "bearish": 0,
            "total": 0, "breadth_score": 0,
        }
    return {
        "bullish": bullish,
        "neutral": neutral,
        "bearish": bearish,
        "total": total,
        "breadth_score": round(
            ((bullish - bearish) / total) * 100, 2
        ),
    }


# -----------------------------------------
# VOLATILITY ENVIRONMENT
# -----------------------------------------
async def volatility_environment(
    coin: str,
    db: Session,
    market_data: dict = None,
) -> Optional[dict]:
    if market_data:
        prices_1h = market_data.get("1h", {}).get("prices", [])
        volumes_1h = market_data.get("1h", {}).get("volumes", [])
        prices_1d = market_data.get("1d", {}).get("prices", [])
    else:
        prices_1h, volumes_1h = await get_klines(coin, "1h", limit=48)
        prices_1d, _ = await get_klines(coin, "1d", limit=30)

    if not prices_1h or not prices_1d:
        return None

    vol_1h = volatility(prices_1h, period=min(24, len(prices_1h)))
    vol_1d = volatility(prices_1d, period=min(20, len(prices_1d)))
    vol_ratio = vol_1h / (vol_1d + 0.0001)

    if vol_ratio > 1.5:
        vol_label, vol_score = "Extreme", 90
    elif vol_ratio > 1.0:
        vol_label, vol_score = "Elevated", 65
    elif vol_ratio > 0.5:
        vol_label, vol_score = "Moderate", 40
    else:
        vol_label, vol_score = "Low", 15

    if len(prices_1h) >= 24:
        rets = [
            (prices_1h[i] - prices_1h[i - 1]) / prices_1h[i - 1]
            for i in range(1, min(24, len(prices_1h)))
        ]
        positive = sum(1 for r in rets if r > 0)
        stab_pct = round((positive / len(rets)) * 100, 1)
        stab_lbl = (
            "Strong" if stab_pct > 65
            else "Moderate" if stab_pct > 50
            else "Weak" if stab_pct > 35
            else "Deteriorating"
        )
    else:
        stab_pct, stab_lbl = 50, "Insufficient data"

    stress_score = round(vol_score * 0.6 + (100 - stab_pct) * 0.4, 1)
    stress_label = (
        "High" if stress_score > 70
        else "Moderate" if stress_score > 40
        else "Low"
    )

    if not volumes_1h:
        _, volumes_1h = await get_klines(coin, "1h", limit=24)

    if volumes_1h and len(volumes_1h) >= 10:
        avg_vol = sum(volumes_1h) / len(volumes_1h)
        recent_v = sum(volumes_1h[-6:]) / 6
        liq_ratio = recent_v / (avg_vol + 0.0001)
        liq_label = (
            "High" if liq_ratio > 1.3
            else "Normal" if liq_ratio > 0.7
            else "Thin"
        )
    else:
        liq_label = "Unknown"

    return {
        "volatility_label": vol_label,
        "volatility_score": vol_score,
        "stability_label": stab_lbl,
        "stability_score": round(stab_pct, 1),
        "stress_label": stress_label,
        "stress_score": round(stress_score, 1),
        "liquidity_label": liq_label,
    }


# -----------------------------------------
# CORRELATION MONITOR
# -----------------------------------------
def compute_correlation(
    prices_a: list,
    prices_b: list,
    period: int = 24,
) -> Optional[float]:
    if len(prices_a) < period + 1 or len(prices_b) < period + 1:
        return None

    def returns(prices):
        return [
            (prices[i] - prices[i - 1]) / prices[i - 1]
            for i in range(len(prices) - period, len(prices))
        ]

    ra = returns(prices_a)
    rb = returns(prices_b)
    if len(ra) != len(rb):
        return None
    mean_a = sum(ra) / len(ra)
    mean_b = sum(rb) / len(rb)
    num = sum(
        (a - mean_a) * (b - mean_b) for a, b in zip(ra, rb)
    )
    den_a = math.sqrt(sum((a - mean_a) ** 2 for a in ra))
    den_b = math.sqrt(sum((b - mean_b) ** 2 for b in rb))
    if den_a == 0 or den_b == 0:
        return None
    return round(num / (den_a * den_b), 3)


async def build_correlation_matrix(
    coins: Optional[list] = None,
) -> dict:
    """FIX 7: Async correlation matrix builder."""
    coins_to_use = coins if coins else ["BTC", "ETH", "SOL"]
    coins_to_use = [
        c for c in coins_to_use if c in settings.SUPPORTED_COINS
    ]
    if len(coins_to_use) < 2:
        coins_to_use = ["BTC", "ETH", "SOL"]

    # FIX 7: Fetch all concurrently
    tasks = [get_klines(coin, "1h", limit=50) for coin in coins_to_use]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    price_map = {}
    for coin, result in zip(coins_to_use, results):
        if not isinstance(result, Exception):
            prices, _ = result
            if prices:
                price_map[coin] = prices

    pairs = []
    alerts = []
    coin_list = list(price_map.keys())

    for i in range(len(coin_list)):
        for j in range(i + 1, len(coin_list)):
            a = coin_list[i]
            b = coin_list[j]
            corr = compute_correlation(price_map[a], price_map[b])
            if corr is not None:
                abs_corr = abs(corr)
                pairs.append({
                    "pair": f"{a}-{b}",
                    "correlation": corr,
                    "label": (
                        "Strong" if abs_corr > 0.8
                        else "Moderate" if abs_corr > 0.5
                        else "Weak"
                    ),
                })
                if corr < 0.4:
                    alerts.append(
                        f"{a}-{b} correlation breakdown detected ({corr})"
                    )

    return {"pairs": pairs, "alerts": alerts}


# -----------------------------------------
# REGIME QUALITY + CONFIDENCE
# -----------------------------------------
def compute_regime_quality(stack: dict) -> dict:
    alignment = stack.get("alignment") or 0
    survival = stack.get("survival") or 50
    hazard = stack.get("hazard") or 50
    shift_risk = stack.get("shift_risk") or 50
    coherence = 50.0
    if stack.get("execution") and stack["execution"].get("coherence"):
        coherence = stack["execution"]["coherence"]

    score = round(
        alignment * 0.30
        + survival * 0.25
        + (100 - hazard) * 0.20
        + (100 - shift_risk) * 0.15
        + coherence * 0.10,
        1,
    )

    if score >= 80:
        grade, structural, breakdown = "A", "Excellent", "Low"
    elif score >= 65:
        grade, structural, breakdown = "B+", "Strong", "Low-Moderate"
    elif score >= 50:
        grade, structural, breakdown = "B", "Healthy", "Moderate"
    elif score >= 35:
        grade, structural, breakdown = "C", "Weakening", "Elevated"
    else:
        grade, structural, breakdown = "D", "Fragile", "High"

    return {
        "grade": grade,
        "score": score,
        "structural": structural,
        "breakdown": breakdown,
    }


def regime_confidence_score(
    alignment: float,
    survival: float,
    coherence: float,
    breadth_score: float,
) -> dict:
    breadth_norm = (breadth_score + 100) / 2
    confidence = round(
        alignment * 0.30
        + survival * 0.25
        + abs(coherence) * 0.25
        + breadth_norm * 0.20,
        1,
    )
    confidence = min(100, max(0, confidence))

    if confidence > 75:
        label, desc = "High", "Strong regime - elevated conviction warranted"
    elif confidence > 50:
        label, desc = "Moderate", "Developing regime - standard position sizing"
    elif confidence > 30:
        label, desc = "Low", "Weak regime - reduce size, widen stops"
    else:
        label, desc = "Very Low", "No clear regime - minimal exposure only"

    return {
        "score": confidence,
        "label": label,
        "description": desc,
        "components": {
            "alignment": round(alignment, 1),
            "survival": round(survival, 1),
            "coherence": round(abs(coherence), 1),
            "breadth": round(breadth_norm, 1),
        },
    }


# -----------------------------------------
# REGIME TRANSITION MATRIX
# -----------------------------------------
def regime_transition_matrix(
    db: Session, coin: str, timeframe: str = "1h"
) -> Optional[dict]:
    records = get_history(db, coin, timeframe)
    if len(records) < 10:
        return None

    STATES = [
        "Strong Risk-On", "Risk-On", "Neutral",
        "Risk-Off", "Strong Risk-Off",
    ]
    transitions = {s: {t: 0 for t in STATES} for s in STATES}

    for i in range(len(records) - 1):
        cur = records[i].label
        nxt = records[i + 1].label
        if cur in transitions and nxt in transitions:
            transitions[cur][nxt] += 1

    current_state = records[-1].label if records else "Neutral"
    row = transitions.get(current_state, {})
    total = sum(row.values())

    if total == 0:
        probs = {s: round(100 / len(STATES), 1) for s in STATES}
    else:
        probs = {
            s: round((row.get(s, 0) / total) * 100, 1)
            for s in STATES
        }

    sorted_probs = dict(
        sorted(probs.items(), key=lambda x: x[1], reverse=True)
    )
    return {
        "current_state": current_state,
        "transitions": sorted_probs,
        "sample_size": total,
        "data_sufficient": total >= 10,
    }


# -----------------------------------------
# PORTFOLIO ALLOCATOR
# -----------------------------------------
def portfolio_allocation(
    account_size: float,
    exposure_pct: float,
    confidence_score: float,
    strategy_mode: str = "balanced",
) -> dict:
    mode_mult = {
        "conservative": 0.70,
        "balanced": 1.00,
        "aggressive": 1.25,
    }
    mult = mode_mult.get(strategy_mode, 1.0)
    adj_exposure = min(95, exposure_pct * mult)
    deployed = round(account_size * adj_exposure / 100, 2)
    cash = round(account_size - deployed, 2)
    swing_pct = 0.35 + (confidence_score / 100) * 0.25
    spot_pct = 1 - swing_pct

    return {
        "account_size": account_size,
        "strategy_mode": strategy_mode,
        "adjusted_exposure": round(adj_exposure, 1),
        "deployed_capital": deployed,
        "cash_reserve": cash,
        "spot_allocation": round(deployed * spot_pct, 2),
        "swing_allocation": round(deployed * swing_pct, 2),
        "cash_pct": round((cash / account_size) * 100, 1),
    }


# -----------------------------------------
# DECISION ENGINE
# -----------------------------------------
def compute_decision_score(
    hazard: float,
    shift_risk: float,
    alignment: float,
    survival: float,
    breadth_score: float,
    maturity_pct: float,
) -> dict:
    breadth_norm = (breadth_score + 100) / 2
    survival_score = survival
    safety_score = 100 - hazard
    shift_score = 100 - shift_risk
    maturity_score = 100 - maturity_pct
    breadth_bullish = breadth_norm

    decision_score = round(
        survival_score * 0.25
        + safety_score * 0.25
        + shift_score * 0.20
        + alignment * 0.15
        + maturity_score * 0.10
        + breadth_bullish * 0.05,
        1,
    )
    decision_score = min(100, max(0, decision_score))

    if decision_score >= 80:
        directive, action, color = "Increase Exposure", "aggressive", "emerald"
        description = "All signals aligned bullish. Regime is healthy and persistent."
        actions = [
            "Add to existing positions on pullbacks",
            "Increase position size toward upper band",
            "Trail stops to lock in gains",
            "Monitor for breadth confirmation",
        ]
    elif decision_score >= 60:
        directive, action, color = "Maintain Exposure", "hold", "green"
        description = "Regime intact. No action required. Stay the course."
        actions = [
            "Hold current positions",
            "No new leverage",
            "Monitor hazard rate for changes",
            "Re-evaluate if shift risk exceeds 60%",
        ]
    elif decision_score >= 40:
        directive, action, color = "Trim Exposure", "trim", "yellow"
        description = "Regime showing early deterioration. Reduce risk selectively."
        actions = [
            "Reduce position size by 15-25%",
            "Avoid adding new breakout entries",
            "Take partial profits on extended positions",
            "Tighten stop losses",
        ]
    elif decision_score >= 20:
        directive, action, color = "Switch to Defensive", "defensive", "orange"
        description = "Multiple deterioration signals active. Reduce exposure significantly."
        actions = [
            "Reduce exposure to lower band immediately",
            "No new long entries",
            "Move profits to cash or stables",
            "Wait for regime confirmation before re-entering",
        ]
    else:
        directive, action, color = "Risk-Off - Exit", "exit", "red"
        description = "Regime breakdown in progress. Capital preservation is the priority."
        actions = [
            "Exit or heavily reduce all positions",
            "Move to maximum cash allocation",
            "Do not average down",
            "Wait for full regime reset before re-entry",
        ]

    return {
        "score": decision_score,
        "directive": directive,
        "action": action,
        "color": color,
        "description": description,
        "actions": actions,
        "components": {
            "survival": round(survival_score, 1),
            "safety": round(safety_score, 1),
            "shift": round(shift_score, 1),
            "alignment": round(alignment, 1),
            "maturity": round(maturity_score, 1),
            "breadth": round(breadth_bullish, 1),
        },
    }


# -----------------------------------------
# UPDATE MARKET ENTRY
# -----------------------------------------
async def update_market(
    coin: str,
    timeframe: str,
    db: Session,
    market_data: dict = None,
):
    """FIX 7: Async update with detailed error logging."""
    try:
        # First test if we can get klines at all
        if market_data and timeframe in market_data:
            prices = market_data[timeframe]["prices"]
            volumes = market_data[timeframe]["volumes"]
            logger.info(
                f"update_market {coin}/{timeframe}: "
                f"using pre-fetched data, {len(prices)} prices"
            )
        else:
            prices, volumes = await get_klines(coin, timeframe, limit=120)
            logger.info(
                f"update_market {coin}/{timeframe}: "
                f"fetched {len(prices)} prices from Binance"
            )

        if len(prices) < 30:
            logger.warning(
                f"update_market {coin}/{timeframe}: "
                f"insufficient data ({len(prices)} prices, need 30)"
            )
            return None

        result = await calculate_score_for_timeframe(
            coin, timeframe, market_data=market_data
        )

        if result is None:
            logger.warning(
                f"update_market {coin}/{timeframe}: "
                f"calculate_score_for_timeframe returned None"
            )
            return None

        logger.info(
            f"update_market {coin}/{timeframe}: "
            f"score={result['score']}, label={classify(result['score'])}"
        )

        entry = MarketSummary(
            coin=coin,
            timeframe=timeframe,
            score=result["score"],
            label=classify(result["score"]),
            coherence=result["coherence"],
            momentum_4h=result["mom_short"],
            momentum_24h=result["mom_long"],
            volatility_val=result["volatility"],
        )
        db.add(entry)
        db.commit()
        db.refresh(entry)
        logger.info(
            f"Saved {coin}/{timeframe}: "
            f"{entry.label} ({entry.score})"
        )
        return entry

    except Exception as e:
        logger.error(
            f"update_market FAILED {coin}/{timeframe}: "
            f"{type(e).__name__}: {e}"
        )
        import traceback
        logger.error(traceback.format_exc())
        return None


def build_regime_stack_bulk(coins: list, db: Session) -> dict:
    """
    Fetches regime data for ALL coins in 3 DB queries total.
    Use this instead of calling build_regime_stack in a loop.
    """
    from sqlalchemy import func

    stacks_raw = {}

    for tf in ["1d", "4h", "1h"]:
        subq = (
            db.query(
                MarketSummary.coin,
                func.max(MarketSummary.created_at).label("max_created"),
            )
            .filter(
                MarketSummary.coin.in_(coins),
                MarketSummary.timeframe == tf,
            )
            .group_by(MarketSummary.coin)
            .subquery()
        )

        records = (
            db.query(MarketSummary)
            .join(
                subq,
                (MarketSummary.coin == subq.c.coin)
                & (MarketSummary.created_at == subq.c.max_created),
            )
            .filter(MarketSummary.timeframe == tf)
            .all()
        )

        for record in records:
            if record.coin not in stacks_raw:
                stacks_raw[record.coin] = {}
            stacks_raw[record.coin][tf] = {
                "label": record.label,
                "score": record.score,
                "coherence": record.coherence,
                "timestamp": record.created_at,
            }

    result = {}
    for coin in coins:
        coin_data = stacks_raw.get(coin, {})

        if len(coin_data) < 3:
            result[coin] = {
                "coin": coin,
                "macro": coin_data.get("1d"),
                "trend": coin_data.get("4h"),
                "execution": coin_data.get("1h"),
                "alignment": None,
                "direction": None,
                "exposure": None,
                "shift_risk": None,
                "survival": None,
                "hazard": None,
                "incomplete": True,
            }
            continue

        labels = [
            coin_data["1d"]["label"],
            coin_data["4h"]["label"],
            coin_data["1h"]["label"],
        ]
        coherences = [
            coin_data["1d"]["coherence"] or 0,
            coin_data["4h"]["coherence"] or 0,
            coin_data["1h"]["coherence"] or 0,
        ]

        align = regime_alignment(labels)
        direction = alignment_direction(labels)
        avg_coh = sum(coherences) / len(coherences)
        survival_1h = survival_probability(db, coin, "1h")
        hazard_1h = hazard_rate(db, coin, "1h")

        exposure = exposure_recommendation_stacked(
            macro_label=coin_data["1d"]["label"],
            trend_label=coin_data["4h"]["label"],
            exec_label=coin_data["1h"]["label"],
            alignment=align,
            survival_1h=survival_1h,
            hazard_1h=hazard_1h,
            coherence_1h=coin_data["1h"]["coherence"] or 50,
        )
        shift_risk_val = regime_shift_risk(
            hazard_1h, survival_1h, avg_coh
        )

        result[coin] = {
            "coin": coin,
            "macro": coin_data["1d"],
            "trend": coin_data["4h"],
            "execution": coin_data["1h"],
            "alignment": align,
            "direction": direction,
            "exposure": exposure,
            "shift_risk": shift_risk_val,
            "survival": survival_1h,
            "hazard": hazard_1h,
            "incomplete": False,
        }

    return result
