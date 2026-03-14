from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean
from sqlalchemy.orm import sessionmaker, declarative_base, Session
from pydantic import BaseModel
from dotenv import load_dotenv
from typing import Optional
import os
import uuid
import datetime
import requests
import math
import stripe
import logging

# ─────────────────────────────────────────
# SETUP
# ─────────────────────────────────────────

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("chainpulse")

load_dotenv()

DATABASE_URL          = os.getenv("DATABASE_URL", "sqlite:///./chainpulse.db")
STRIPE_SECRET_KEY     = os.getenv("STRIPE_SECRET_KEY")
STRIPE_PRICE_ID       = os.getenv("STRIPE_PRICE_ID")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET")
RESEND_API_KEY        = os.getenv("RESEND_API_KEY")
UPDATE_SECRET         = os.getenv("UPDATE_SECRET", "changeme")
FRONTEND_URL          = os.getenv("FRONTEND_URL", "https://chainpulse.pro")

if STRIPE_SECRET_KEY:
    stripe.api_key = STRIPE_SECRET_KEY

engine       = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine)
Base         = declarative_base()

# ─────────────────────────────────────────
# DATABASE MODELS
# ─────────────────────────────────────────

class MarketSummary(Base):
    __tablename__ = "market_summary"
    id             = Column(Integer, primary_key=True)
    coin           = Column(String, index=True)
    timeframe      = Column(String, index=True, default="1h")
    score          = Column(Float)
    label          = Column(String)
    coherence      = Column(Float)
    momentum_4h    = Column(Float, default=0)
    momentum_24h   = Column(Float, default=0)
    volatility_val = Column(Float, default=0)
    created_at     = Column(DateTime, default=datetime.datetime.utcnow)


class User(Base):
    __tablename__ = "users"
    id                     = Column(Integer, primary_key=True)
    email                  = Column(String, unique=True, index=True)
    subscription_status    = Column(String, default="inactive")
    stripe_customer_id     = Column(String, nullable=True)
    stripe_subscription_id = Column(String, nullable=True)
    alerts_enabled         = Column(Boolean, default=False)
    last_alert_sent        = Column(DateTime, nullable=True)
    access_token           = Column(String, nullable=True, index=True)
    created_at             = Column(DateTime, default=datetime.datetime.utcnow)


Base.metadata.create_all(bind=engine)

app = FastAPI(title="ChainPulse API", version="4.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://chainpulse.pro",
        "https://www.chainpulse.pro",
        "http://localhost:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────
# DB DEPENDENCY
# ─────────────────────────────────────────

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ─────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────

SUPPORTED_COINS      = ["BTC", "ETH", "SOL", "BNB", "AVAX", "LINK", "ADA"]
SUPPORTED_TIMEFRAMES = ["1h", "4h", "1d"]

TIMEFRAME_LABELS = {
    "1h": "Execution",
    "4h": "Trend",
    "1d": "Macro",
}

REGIME_NUMERIC = {
    "Strong Risk-On":   2,
    "Risk-On":          1,
    "Neutral":          0,
    "Risk-Off":        -1,
    "Strong Risk-Off": -2,
}

RISK_EVENTS = [
    {"name": "FOMC Meeting",   "type": "macro",  "impact": "High"},
    {"name": "CPI Release",    "type": "macro",  "impact": "High"},
    {"name": "Options Expiry", "type": "market", "impact": "Medium"},
    {"name": "ETF Flow Report","type": "market", "impact": "Medium"},
    {"name": "BTC Halving",    "type": "crypto", "impact": "High"},
    {"name": "Fed Minutes",    "type": "macro",  "impact": "Medium"},
    {"name": "PCE Inflation",  "type": "macro",  "impact": "High"},
]

# Playbook data — mirrors frontend for email generation
PLAYBOOK_DATA = {
    "Strong Risk-On": {
        "exposure_band": "65–80%",
        "strategy_mode": "Aggressive",
        "trend_follow_wr": 72,
        "mean_revert_wr":  38,
        "avg_remaining_days": 14,
        "actions": [
            "Favour trend continuation entries",
            "Pyramiding into strength is valid",
            "Tight stops — volatility is compressed",
            "Hold winners longer than feels comfortable",
        ],
        "avoid": ["Shorting into strength", "Waiting for deep pullbacks"],
    },
    "Risk-On": {
        "exposure_band": "50–65%",
        "strategy_mode": "Balanced",
        "trend_follow_wr": 63,
        "mean_revert_wr":  44,
        "avg_remaining_days": 9,
        "actions": [
            "Favour pullback entries in trend direction",
            "Scale into positions over 2–3 entries",
            "Monitor breadth for continuation signal",
        ],
        "avoid": ["Over-leveraging at breakouts", "Chasing extended moves"],
    },
    "Neutral": {
        "exposure_band": "25–45%",
        "strategy_mode": "Neutral",
        "trend_follow_wr": 49,
        "mean_revert_wr":  51,
        "avg_remaining_days": 6,
        "actions": [
            "Reduce overall exposure",
            "Preserve capital — this is a transition zone",
        ],
        "avoid": ["Strong directional bias", "Large position sizes"],
    },
    "Risk-Off": {
        "exposure_band": "10–25%",
        "strategy_mode": "Defensive",
        "trend_follow_wr": 31,
        "mean_revert_wr":  57,
        "avg_remaining_days": 7,
        "actions": [
            "Reduce long exposure significantly",
            "Hold cash — optionality has value",
        ],
        "avoid": ["Buying dips aggressively", "Adding to losing longs"],
    },
    "Strong Risk-Off": {
        "exposure_band": "0–10%",
        "strategy_mode": "Fully Defensive",
        "trend_follow_wr": 22,
        "mean_revert_wr":  48,
        "avg_remaining_days": 11,
        "actions": [
            "Move to maximum cash allocation",
            "Monitor for capitulation signals",
        ],
        "avoid": ["Catching falling knives", "Any leveraged long exposure"],
    },
}

# ─────────────────────────────────────────
# AUTH HELPER
# ─────────────────────────────────────────

def resolve_pro_status(authorization: Optional[str], db: Session) -> bool:
    if not authorization or not authorization.startswith("Bearer "):
        return False
    token = authorization.replace("Bearer ", "").strip()
    if not token:
        return False
    user = db.query(User).filter(User.access_token == token).first()
    if not user:
        return False
    return user.subscription_status == "active"

# ─────────────────────────────────────────
# MARKET DATA
# ─────────────────────────────────────────

def get_klines(symbol: str, interval: str, limit: int = 120):
    urls = [
        "https://api.binance.com/api/v3/klines",
        "https://api.binance.us/api/v3/klines",
    ]
    params = {"symbol": f"{symbol}USDT", "interval": interval, "limit": limit}
    for url in urls:
        try:
            r = requests.get(url, params=params, timeout=10)
            r.raise_for_status()
            data = r.json()
            if not isinstance(data, list) or len(data) == 0:
                continue
            prices  = [float(c[4]) for c in data]
            volumes = [float(c[5]) for c in data]
            logger.info(f"Got {len(prices)} candles for {symbol}/{interval}")
            return prices, volumes
        except Exception as e:
            logger.error(f"Kline fetch failed {url} {symbol}/{interval}: {e}")
            continue
    return [], []


def volatility(prices: list, period: int = 20) -> float:
    if len(prices) < period:
        return 0.0
    subset = prices[-period:]
    mean   = sum(subset) / len(subset)
    var    = sum((p - mean) ** 2 for p in subset) / len(subset)
    return math.sqrt(var)


def volume_momentum(volumes: list, period: int = 10) -> float:
    if len(volumes) < period * 2:
        return 0.0
    recent = sum(volumes[-period:]) / period
    prior  = sum(volumes[-period * 2:-period]) / period
    if prior == 0:
        return 0.0
    return ((recent - prior) / prior) * 100


def calculate_coherence(
    mom_short: float, mom_long: float, vol_score: float
) -> float:
    if (mom_short >= 0 and mom_long >= 0) or (mom_short < 0 and mom_long < 0):
        alignment = 1.0
    else:
        alignment = 0.3
    magnitude      = (abs(mom_short) + abs(mom_long)) / 2
    magnitude_norm = min(magnitude / 5.0, 1.0) * 100
    vol_penalty    = min(vol_score / 500, 0.5)
    raw = alignment * magnitude_norm * (1 - vol_penalty)
    return round(max(0, min(100, raw)), 2)


def calculate_score_for_timeframe(coin: str, interval: str) -> Optional[dict]:
    prices, volumes = get_klines(coin, interval, limit=120)
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
    mom_long  = ((prices[-1] - prices[-long_lb])  / prices[-long_lb])  * 100
    vol       = volatility(prices)
    vol_mom   = volume_momentum(volumes)
    score = 0.55 * mom_long + 0.35 * mom_short - 0.08 * vol + 0.02 * vol_mom
    score = max(-100, min(100, score))
    coherence = calculate_coherence(mom_short, mom_long, vol)
    return {
        "score":      round(score, 4),
        "mom_short":  round(mom_short, 4),
        "mom_long":   round(mom_long, 4),
        "volatility": round(vol, 4),
        "coherence":  coherence,
    }


def classify(score: float) -> str:
    if score > 35:   return "Strong Risk-On"
    if score > 15:   return "Risk-On"
    if score < -35:  return "Strong Risk-Off"
    if score < -15:  return "Risk-Off"
    return "Neutral"

# ─────────────────────────────────────────
# REGIME ALIGNMENT ENGINE
# ─────────────────────────────────────────

def regime_alignment(labels: list) -> float:
    scores  = [REGIME_NUMERIC.get(l, 0) for l in labels]
    if not scores:
        return 0.0
    max_sum = 2 * len(scores)
    return round((abs(sum(scores)) / max_sum) * 100, 2)


def alignment_direction(labels: list) -> str:
    scores = [REGIME_NUMERIC.get(l, 0) for l in labels]
    total  = sum(scores)
    if total > 0:  return "bullish"
    if total < 0:  return "bearish"
    return "mixed"

# ─────────────────────────────────────────
# STATISTICS ENGINE
# ─────────────────────────────────────────

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


def regime_durations(db: Session, coin: str, timeframe: str = "1h") -> list:
    records = get_history(db, coin, timeframe)
    if not records:
        return []
    durations     = []
    current_label = records[0].label
    start_time    = records[0].created_at
    for r in records[1:]:
        if r.label != current_label:
            d = (r.created_at - start_time).total_seconds() / 3600
            if d > 0:
                durations.append(d)
            current_label = r.label
            start_time    = r.created_at
    return durations


def current_age(db: Session, coin: str, timeframe: str = "1h") -> float:
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
    start_time   = records[0].created_at
    for r in records:
        if r.label != latest_label:
            break
        start_time = r.created_at
    return (datetime.datetime.utcnow() - start_time).total_seconds() / 3600


def survival_probability(db: Session, coin: str, timeframe: str = "1h") -> float:
    durations = regime_durations(db, coin, timeframe)
    age       = current_age(db, coin, timeframe)
    if len(durations) < 5:
        return round(max(20.0, 90.0 - age * 4), 2)
    longer = [d for d in durations if d > age]
    return round((len(longer) / len(durations)) * 100, 2)


def hazard_rate(db: Session, coin: str, timeframe: str = "1h") -> float:
    durations = regime_durations(db, coin, timeframe)
    age       = current_age(db, coin, timeframe)
    if len(durations) < 5:
        return round(min(70.0, age * 5), 2)
    avg = sum(durations) / len(durations)
    return round(min(100.0, (age / (avg + 0.01)) * 100), 2)


def percentile_rank(
    db: Session, coin: str, current_score: float, timeframe: str = "1h"
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
        min(100.0,
            hazard         * 0.50 +
            (100-survival) * 0.35 +
            (100-coherence)* 0.15),
        2,
    )


def exposure_recommendation(
    score: float, survival: float, hazard: float, coherence: float
) -> float:
    if score > 35:    base = 0.85
    elif score > 15:  base = 0.65
    elif score < -35: base = 0.08
    elif score < -15: base = 0.22
    else:             base = 0.42
    persistence_factor = survival / 100
    hazard_penalty     = 1 - (hazard / 100) * 0.65
    coherence_factor   = 0.7 + (coherence / 100) * 0.3
    exposure = base * persistence_factor * hazard_penalty * coherence_factor
    return round(max(5.0, min(95.0, exposure * 100)), 2)


def exposure_recommendation_stacked(
    macro_label:  str,
    trend_label:  str,
    exec_label:   str,
    alignment:    float,
    survival_1h:  float,
    hazard_1h:    float,
    coherence_1h: float,
) -> float:
    macro_num = REGIME_NUMERIC.get(macro_label, 0)
    if macro_num >= 1:
        macro_ceiling, macro_floor = 0.90, 0.30
    elif macro_num == 0:
        macro_ceiling, macro_floor = 0.60, 0.20
    else:
        macro_ceiling, macro_floor = 0.35, 0.05

    trend_num = REGIME_NUMERIC.get(trend_label, 0)
    rang      = macro_ceiling - macro_floor
    if trend_num == 2:    base = macro_ceiling
    elif trend_num == 1:  base = macro_floor + rang * 0.75
    elif trend_num == 0:  base = macro_floor + rang * 0.50
    elif trend_num == -1: base = macro_floor + rang * 0.25
    else:                 base = macro_floor

    exec_num = REGIME_NUMERIC.get(exec_label, 0)
    base     = base + (exec_num / 2) * 0.10

    persistence_factor = survival_1h  / 100
    hazard_penalty     = 1 - (hazard_1h  / 100) * 0.65
    coherence_factor   = 0.7 + (coherence_1h / 100) * 0.3
    alignment_mult     = 0.5 + alignment / 200

    exposure = (
        base
        * persistence_factor
        * hazard_penalty
        * coherence_factor
        * alignment_mult
    )
    return round(max(5.0, min(95.0, exposure * 100)), 2)

# ─────────────────────────────────────────
# REGIME STACK BUILDER
# ─────────────────────────────────────────

def build_regime_stack(coin: str, db: Session) -> dict:
    stack      = {}
    labels     = []
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
                "label":     record.label,
                "score":     record.score,
                "coherence": record.coherence,
                "timestamp": record.created_at,
            }
            labels.append(record.label)
            coherences.append(record.coherence)
        else:
            stack[tf] = None

    if len(labels) < 3:
        return {
            "coin":       coin,
            "macro":      stack.get("1d"),
            "trend":      stack.get("4h"),
            "execution":  stack.get("1h"),
            "alignment":  None,
            "direction":  None,
            "exposure":   None,
            "shift_risk": None,
            "survival":   None,
            "hazard":     None,
            "incomplete": True,
        }

    align     = regime_alignment(labels)
    direction = alignment_direction(labels)
    avg_coh   = sum(coherences) / len(coherences)

    survival_1h = survival_probability(db, coin, "1h")
    hazard_1h   = hazard_rate(db, coin, "1h")

    exposure = exposure_recommendation_stacked(
        macro_label  = stack["1d"]["label"],
        trend_label  = stack["4h"]["label"],
        exec_label   = stack["1h"]["label"],
        alignment    = align,
        survival_1h  = survival_1h,
        hazard_1h    = hazard_1h,
        coherence_1h = stack["1h"]["coherence"],
    )

    shift_risk = regime_shift_risk(hazard_1h, survival_1h, avg_coh)

    return {
        "coin":       coin,
        "macro":      stack["1d"],
        "trend":      stack["4h"],
        "execution":  stack["1h"],
        "alignment":  align,
        "direction":  direction,
        "exposure":   exposure,
        "shift_risk": shift_risk,
        "survival":   survival_1h,
        "hazard":     hazard_1h,
        "incomplete": False,
    }

# ─────────────────────────────────────────
# MARKET BREADTH
# ─────────────────────────────────────────

def compute_market_breadth(db: Session) -> dict:
    bullish = neutral = bearish = 0
    for coin in SUPPORTED_COINS:
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
        n = REGIME_NUMERIC.get(record.label, 0)
        if n > 0:   bullish += 1
        elif n < 0: bearish += 1
        else:       neutral += 1

    total = bullish + neutral + bearish
    if total == 0:
        return {
            "bullish": 0, "neutral": 0, "bearish": 0,
            "total": 0,   "breadth_score": 0,
        }
    return {
        "bullish":       bullish,
        "neutral":       neutral,
        "bearish":       bearish,
        "total":         total,
        "breadth_score": round(((bullish - bearish) / total) * 100, 2),
    }

# ─────────────────────────────────────────
# VOLATILITY ENVIRONMENT
# ─────────────────────────────────────────

def volatility_environment(coin: str, db: Session) -> Optional[dict]:
    prices_1h, _ = get_klines(coin, "1h", limit=48)
    prices_1d, _ = get_klines(coin, "1d", limit=30)
    if not prices_1h or not prices_1d:
        return None

    vol_1h    = volatility(prices_1h, period=24)
    vol_1d    = volatility(prices_1d, period=20)
    vol_ratio = vol_1h / (vol_1d + 0.0001)

    if vol_ratio > 1.5:
        vol_label, vol_score = "Extreme",  90
    elif vol_ratio > 1.0:
        vol_label, vol_score = "Elevated", 65
    elif vol_ratio > 0.5:
        vol_label, vol_score = "Moderate", 40
    else:
        vol_label, vol_score = "Low",      15

    if len(prices_1h) >= 24:
        returns = [
            (prices_1h[i] - prices_1h[i-1]) / prices_1h[i-1]
            for i in range(1, 24)
        ]
        positive       = sum(1 for r in returns if r > 0)
        stability_pct  = round((positive / len(returns)) * 100, 1)
        stability_label = (
            "Strong"        if stability_pct > 65 else
            "Moderate"      if stability_pct > 50 else
            "Weak"          if stability_pct > 35 else
            "Deteriorating"
        )
    else:
        stability_pct, stability_label = 50, "Insufficient data"

    stress_score = round(vol_score * 0.6 + (100 - stability_pct) * 0.4, 1)
    stress_label = (
        "High"     if stress_score > 70 else
        "Moderate" if stress_score > 40 else
        "Low"
    )

    _, volumes = get_klines(coin, "1h", limit=24)
    if volumes and len(volumes) >= 10:
        avg_vol    = sum(volumes) / len(volumes)
        recent_vol = sum(volumes[-6:]) / 6
        liq_ratio  = recent_vol / (avg_vol + 0.0001)
        liquidity_label = (
            "High"   if liq_ratio > 1.3 else
            "Normal" if liq_ratio > 0.7 else
            "Thin"
        )
    else:
        liquidity_label = "Unknown"

    return {
        "volatility_label": vol_label,
        "volatility_score": vol_score,
        "stability_label":  stability_label,
        "stability_score":  round(stability_pct, 1),
        "stress_label":     stress_label,
        "stress_score":     round(stress_score, 1),
        "liquidity_label":  liquidity_label,
    }

# ─────────────────────────────────────────
# CORRELATION MONITOR
# ─────────────────────────────────────────

def compute_correlation(
    prices_a: list, prices_b: list, period: int = 24
) -> Optional[float]:
    if len(prices_a) < period + 1 or len(prices_b) < period + 1:
        return None

    def returns(prices):
        return [
            (prices[i] - prices[i-1]) / prices[i-1]
            for i in range(len(prices) - period, len(prices))
        ]

    ra = returns(prices_a)
    rb = returns(prices_b)
    if len(ra) != len(rb):
        return None

    mean_a = sum(ra) / len(ra)
    mean_b = sum(rb) / len(rb)
    num    = sum((a - mean_a) * (b - mean_b) for a, b in zip(ra, rb))
    den_a  = math.sqrt(sum((a - mean_a) ** 2 for a in ra))
    den_b  = math.sqrt(sum((b - mean_b) ** 2 for b in rb))
    if den_a == 0 or den_b == 0:
        return None
    return round(num / (den_a * den_b), 3)


def build_correlation_matrix(coins: Optional[list] = None) -> dict:
    """
    Computes pairwise correlation.
    Accepts optional list of coins; defaults to BTC/ETH/SOL.
    """
    coins_to_use = coins if coins else ["BTC", "ETH", "SOL"]
    # Clamp to supported coins
    coins_to_use = [c for c in coins_to_use if c in SUPPORTED_COINS]
    if len(coins_to_use) < 2:
        coins_to_use = ["BTC", "ETH", "SOL"]

    price_map = {}
    for coin in coins_to_use:
        prices, _ = get_klines(coin, "1h", limit=50)
        if prices:
            price_map[coin] = prices

    pairs  = []
    alerts = []
    coin_list = list(price_map.keys())

    for i in range(len(coin_list)):
        for j in range(i + 1, len(coin_list)):
            a    = coin_list[i]
            b    = coin_list[j]
            corr = compute_correlation(price_map[a], price_map[b])
            if corr is not None:
                abs_corr = abs(corr)
                pairs.append({
                    "pair":        f"{a}-{b}",
                    "correlation": corr,
                    "label": (
                        "Strong"   if abs_corr > 0.8 else
                        "Moderate" if abs_corr > 0.5 else
                        "Weak"
                    ),
                })
                if corr < 0.4:
                    alerts.append(
                        f"{a}-{b} correlation breakdown detected ({corr})"
                    )

    return {"pairs": pairs, "alerts": alerts}

# ─────────────────────────────────────────
# REGIME CONFIDENCE SCORE
# ─────────────────────────────────────────

def regime_confidence_score(
    alignment:     float,
    survival:      float,
    coherence:     float,
    breadth_score: float,
) -> dict:
    breadth_norm = (breadth_score + 100) / 2
    confidence   = round(
        alignment      * 0.30 +
        survival       * 0.25 +
        abs(coherence) * 0.25 +
        breadth_norm   * 0.20,
        1,
    )
    confidence = min(100, max(0, confidence))

    if confidence > 75:
        label = "High"
        desc  = "Strong regime — elevated conviction warranted"
    elif confidence > 50:
        label = "Moderate"
        desc  = "Developing regime — standard position sizing"
    elif confidence > 30:
        label = "Low"
        desc  = "Weak regime — reduce size, widen stops"
    else:
        label = "Very Low"
        desc  = "No clear regime — minimal exposure only"

    return {
        "score":       confidence,
        "label":       label,
        "description": desc,
        "components": {
            "alignment": round(alignment,       1),
            "survival":  round(survival,        1),
            "coherence": round(abs(coherence),  1),
            "breadth":   round(breadth_norm,    1),
        },
    }

# ─────────────────────────────────────────
# REGIME TRANSITION MATRIX
# ─────────────────────────────────────────

def regime_transition_matrix(
    db: Session, coin: str, timeframe: str = "1h"
) -> Optional[dict]:
    records = get_history(db, coin, timeframe)
    if len(records) < 10:
        return None

    STATES = [
        "Strong Risk-On", "Risk-On", "Neutral", "Risk-Off", "Strong Risk-Off"
    ]
    transitions = {s: {t: 0 for t in STATES} for s in STATES}

    for i in range(len(records) - 1):
        cur = records[i].label
        nxt = records[i + 1].label
        if cur in transitions and nxt in transitions:
            transitions[cur][nxt] += 1

    current_state = records[-1].label if records else "Neutral"
    row   = transitions.get(current_state, {})
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
        "current_state":   current_state,
        "transitions":     sorted_probs,
        "sample_size":     total,
        "data_sufficient": total >= 10,
    }

# ─────────────────────────────────────────
# PORTFOLIO ALLOCATOR
# ─────────────────────────────────────────

def portfolio_allocation(
    account_size:     float,
    exposure_pct:     float,
    confidence_score: float,
    strategy_mode:    str = "balanced",
) -> dict:
    mode_mult = {
        "conservative": 0.70,
        "balanced":     1.00,
        "aggressive":   1.25,
    }
    mult         = mode_mult.get(strategy_mode, 1.0)
    adj_exposure = min(95, exposure_pct * mult)
    deployed     = round(account_size * adj_exposure / 100, 2)
    cash         = round(account_size - deployed, 2)
    swing_pct    = 0.35 + (confidence_score / 100) * 0.25
    spot_pct     = 1 - swing_pct

    return {
        "account_size":      account_size,
        "strategy_mode":     strategy_mode,
        "adjusted_exposure": round(adj_exposure, 1),
        "deployed_capital":  deployed,
        "cash_reserve":      cash,
        "spot_allocation":   round(deployed * spot_pct,  2),
        "swing_allocation":  round(deployed * swing_pct, 2),
        "cash_pct":          round((cash / account_size) * 100, 1),
    }

# ─────────────────────────────────────────
# REGIME QUALITY SCORE (server-side helper)
# ─────────────────────────────────────────

def compute_regime_quality(stack: dict) -> dict:
    alignment  = stack.get("alignment")  or 0
    survival   = stack.get("survival")   or 50
    hazard     = stack.get("hazard")     or 50
    shift_risk = stack.get("shift_risk") or 50
    coherence  = 50.0
    if stack.get("execution") and stack["execution"].get("coherence"):
        coherence = stack["execution"]["coherence"]

    score = round(
        alignment          * 0.30 +
        survival           * 0.25 +
        (100 - hazard)     * 0.20 +
        (100 - shift_risk) * 0.15 +
        coherence          * 0.10,
        1,
    )

    if score >= 80:
        grade, structural, breakdown = "A",  "Excellent", "Low"
    elif score >= 65:
        grade, structural, breakdown = "B+", "Strong",    "Low-Moderate"
    elif score >= 50:
        grade, structural, breakdown = "B",  "Healthy",   "Moderate"
    elif score >= 35:
        grade, structural, breakdown = "C",  "Weakening", "Elevated"
    else:
        grade, structural, breakdown = "D",  "Fragile",   "High"

    return {
        "grade":      grade,
        "score":      score,
        "structural": structural,
        "breakdown":  breakdown,
    }

# ─────────────────────────────────────────
# UPDATE ENGINE
# ─────────────────────────────────────────

def update_market(coin: str, timeframe: str, db: Session):
    result = calculate_score_for_timeframe(coin, timeframe)
    if result is None:
        logger.warning(f"Insufficient data for {coin}/{timeframe}")
        return None
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
    logger.info(f"Updated {coin}/{timeframe}: {entry.label} ({entry.score})")
    return entry

# ═══════════════════════════════════════════
# ROUTES
# ═══════════════════════════════════════════

# ── Health ──────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.datetime.utcnow()}

# ── Update ──────────────────────────────────

@app.get("/update-now")
def update_now(
    coin: str = "BTC",
    timeframe: str = "1h",
    secret: str = "",
    db: Session = Depends(get_db),
):
    if secret != UPDATE_SECRET:
        raise HTTPException(status_code=403, detail="Invalid secret")
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")
    if timeframe not in SUPPORTED_TIMEFRAMES:
        raise HTTPException(status_code=400, detail="Unsupported timeframe")
    entry = update_market(coin, timeframe, db)
    if not entry:
        raise HTTPException(status_code=500, detail="Update failed")
    return {
        "status":    "updated",
        "coin":      coin,
        "timeframe": timeframe,
        "label":     entry.label,
        "score":     entry.score,
    }


@app.get("/update-all")
def update_all(secret: str = "", db: Session = Depends(get_db)):
    if secret != UPDATE_SECRET:
        raise HTTPException(status_code=403, detail="Invalid secret")
    results = []
    for coin in SUPPORTED_COINS:
        for tf in SUPPORTED_TIMEFRAMES:
            entry = update_market(coin, tf, db)
            if entry:
                results.append({
                    "coin":      coin,
                    "timeframe": tf,
                    "label":     entry.label,
                    "score":     entry.score,
                })
    return {"status": "updated", "count": len(results), "results": results}

# ── Regime Stack ─────────────────────────────

@app.get("/regime-stack")
def regime_stack_endpoint(
    request: Request,
    coin: str = "BTC",
    db: Session = Depends(get_db),
):
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")

    auth_header = (
        request.headers.get("authorization")
        or request.headers.get("Authorization")
    )
    is_pro = resolve_pro_status(auth_header, db)
    stack  = build_regime_stack(coin, db)

    if stack["incomplete"]:
        return {**stack, "pro_required": False}

    # Free tier fields
    base = {
        "coin":       stack["coin"],
        "macro":      {"label": stack["macro"]["label"]}     if stack["macro"]     else None,
        "trend":      {"label": stack["trend"]["label"]}     if stack["trend"]     else None,
        "execution":  {"label": stack["execution"]["label"]} if stack["execution"] else None,
        "alignment":  stack["alignment"],
        "direction":  stack["direction"],
        "exposure":   stack["exposure"],
        "shift_risk": stack["shift_risk"],
    }

    if not is_pro:
        return {
            **base,
            "pro_required":              True,
            "survival":                  None,
            "hazard":                    None,
            "trend_maturity":            None,
            "percentile":                None,
            "macro_coherence":           None,
            "trend_coherence":           None,
            "exec_coherence":            None,
            "regime_age_hours":          None,
            "avg_regime_duration_hours": None,
        }

    # Pro — full data
    age_1h    = current_age(db, coin, "1h")
    avg_dur   = average_regime_duration(db, coin, "1h")
    maturity  = trend_maturity_score(age_1h, avg_dur, stack["hazard"])
    pct_rank  = percentile_rank(db, coin, stack["execution"]["score"], "1h")

    return {
        **base,
        "macro":                     stack["macro"],
        "trend":                     stack["trend"],
        "execution":                 stack["execution"],
        "pro_required":              False,
        "survival":                  stack["survival"],
        "hazard":                    stack["hazard"],
        "trend_maturity":            maturity,
        "percentile":                pct_rank,
        "macro_coherence":           stack["macro"]["coherence"],
        "trend_coherence":           stack["trend"]["coherence"],
        "exec_coherence":            stack["execution"]["coherence"],
        "regime_age_hours":          round(age_1h, 2),
        "avg_regime_duration_hours": round(avg_dur, 2),
    }


# ── Market Overview ──────────────────────────

@app.get("/market-overview")
def market_overview(db: Session = Depends(get_db)):
    result  = []
    breadth = compute_market_breadth(db)
    for coin in SUPPORTED_COINS:
        stack = build_regime_stack(coin, db)
        if not stack["incomplete"]:
            result.append({
                "coin":       stack["coin"],
                "macro":      stack["macro"]["label"]     if stack["macro"]     else None,
                "trend":      stack["trend"]["label"]     if stack["trend"]     else None,
                "execution":  stack["execution"]["label"] if stack["execution"] else None,
                "alignment":  stack["alignment"],
                "direction":  stack["direction"],
                "exposure":   stack["exposure"],
                "shift_risk": stack["shift_risk"],
            })
    return {"data": result, "breadth": breadth}


# ── Latest ──────────────────────────────────

@app.get("/latest")
def latest(coin: str = "BTC", db: Session = Depends(get_db)):
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")
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
        "coin":         r.coin,
        "score":        r.score,
        "label":        r.label,
        "coherence":    r.coherence,
        "momentum_4h":  r.momentum_4h,
        "momentum_24h": r.momentum_24h,
        "volatility":   r.volatility_val,
        "timeframe":    r.timeframe,
        "timestamp":    r.created_at,
    }


# ── Regime History ──────────────────────────

@app.get("/regime-history")
def regime_history(
    coin: str = "BTC",
    timeframe: str = "1h",
    limit: int = 48,
    db: Session = Depends(get_db),
):
    if timeframe not in SUPPORTED_TIMEFRAMES:
        raise HTTPException(status_code=400, detail="Unsupported timeframe")
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
                "hour":      i,
                "score":     r.score,
                "label":     r.label,
                "coherence": r.coherence,
                "timestamp": r.created_at,
            }
            for i, r in enumerate(records)
        ]
    }


# ── Survival Curve ──────────────────────────

@app.get("/survival-curve")
def survival_curve(
    coin: str = "BTC",
    timeframe: str = "1h",
    db: Session = Depends(get_db),
):
    durations = regime_durations(db, coin, timeframe)
    if len(durations) < 5:
        return {
            "data": [
                {
                    "hour":     h,
                    "survival": max(0, 100 - h * 4),
                    "hazard":   min(100, h * 4.5),
                }
                for h in range(25)
            ],
            "source": "estimated",
        }
    max_dur = int(max(durations))
    curve   = []
    for hour in range(max_dur + 1):
        survivors = [d for d in durations if d > hour]
        surv_pct  = (len(survivors) / len(durations)) * 100
        hz = 0.0
        if hour > 0 and survivors:
            exited = [d for d in durations if hour - 1 < d <= hour]
            hz     = (len(exited) / len(survivors)) * 100
        curve.append({
            "hour":     hour,
            "survival": round(surv_pct, 2),
            "hazard":   round(hz, 2),
        })
    return {"data": curve, "source": "historical"}


# ── New Feature Endpoints ────────────────────

@app.get("/regime-transitions")
def regime_transitions(
    coin: str = "BTC",
    timeframe: str = "1h",
    db: Session = Depends(get_db),
):
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")
    result = regime_transition_matrix(db, coin, timeframe)
    if result is None:
        return {
            "current_state":   "Insufficient data",
            "transitions":     {},
            "data_sufficient": False,
        }
    return result


@app.get("/volatility-environment")
def volatility_env(
    coin: str = "BTC",
    db: Session = Depends(get_db),
):
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")
    result = volatility_environment(coin, db)
    if result is None:
        return {"error": "Insufficient data"}
    return result


@app.get("/correlation")
@app.get("/correlation-matrix")
def correlation_endpoint(
    coins: str = "BTC,ETH,SOL",
    db: Session = Depends(get_db),
):
    """
    Accepts ?coins=BTC,ETH,SOL  (comma-separated).
    Both /correlation and /correlation-matrix are valid.
    """
    coin_list = [c.strip().upper() for c in coins.split(",") if c.strip()]
    return build_correlation_matrix(coin_list)


@app.get("/regime-confidence")
def regime_confidence_endpoint(
    coin: str = "BTC",
    db: Session = Depends(get_db),
):
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")
    stack   = build_regime_stack(coin, db)
    breadth = compute_market_breadth(db)
    if stack["incomplete"]:
        return {"error": "Insufficient regime data"}

    survival_val  = stack.get("survival")  or 50.0
    coherence_val = 0.0
    if stack.get("execution") and stack["execution"].get("coherence"):
        coherence_val = stack["execution"]["coherence"]

    confidence = regime_confidence_score(
        alignment     = stack["alignment"] or 0,
        survival      = survival_val,
        coherence     = coherence_val,
        breadth_score = breadth.get("breadth_score", 0),
    )
    return {**confidence, "coin": coin}


@app.post("/portfolio-allocator")
def portfolio_allocator_endpoint(
    account_size:  float = 10000,
    strategy_mode: str   = "balanced",
    coin:          str   = "BTC",
    db: Session = Depends(get_db),
):
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")
    if strategy_mode not in ("conservative", "balanced", "aggressive"):
        raise HTTPException(status_code=400, detail="Invalid strategy mode")
    if account_size <= 0:
        raise HTTPException(status_code=400, detail="Invalid account size")

    stack = build_regime_stack(coin, db)
    if stack["incomplete"]:
        return {"error": "Insufficient data"}

    breadth     = compute_market_breadth(db)
    survival_v  = stack.get("survival")  or 50.0
    coherence_v = 0.0
    if stack.get("execution") and stack["execution"].get("coherence"):
        coherence_v = stack["execution"]["coherence"]

    confidence = regime_confidence_score(
        alignment     = stack["alignment"] or 0,
        survival      = survival_v,
        coherence     = coherence_v,
        breadth_score = breadth.get("breadth_score", 0),
    )
    allocation = portfolio_allocation(
        account_size     = account_size,
        exposure_pct     = stack["exposure"] or 5,
        confidence_score = confidence["score"],
        strategy_mode    = strategy_mode,
    )
    return {
        **allocation,
        "regime":     stack["execution"]["label"] if stack.get("execution") else "—",
        "confidence": confidence["score"],
        "alignment":  stack["alignment"],
    }


@app.get("/risk-events")
def risk_events():
    return {"events": RISK_EVENTS}


# ── Legacy ───────────────────────────────────

@app.get("/statistics")
def statistics(coin: str = "BTC", db: Session = Depends(get_db)):
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")
    r = (
        db.query(MarketSummary)
        .filter(MarketSummary.coin == coin, MarketSummary.timeframe == "1h")
        .order_by(MarketSummary.created_at.desc())
        .first()
    )
    if not r:
        return {"message": "No data."}
    survival = survival_probability(db, coin, "1h")
    hazard   = hazard_rate(db, coin, "1h")
    exposure = exposure_recommendation(r.score, survival, hazard, r.coherence)
    age      = current_age(db, coin, "1h")
    shift    = regime_shift_risk(hazard, survival, r.coherence)
    avg_dur  = average_regime_duration(db, coin, "1h")
    maturity = trend_maturity_score(age, avg_dur, hazard)
    return {
        "coin":                             coin,
        "score":                            r.score,
        "label":                            r.label,
        "coherence":                        r.coherence,
        "survival_probability_percent":     survival,
        "hazard_percent":                   hazard,
        "percentile_rank_percent":          percentile_rank(db, coin, r.score, "1h"),
        "exposure_recommendation_percent":  exposure,
        "regime_shift_risk_percent":        shift,
        "trend_maturity_score":             maturity,
        "current_regime_age_hours":         round(age, 2),
        "timestamp":                        r.created_at,
        "pro_required":                     False,
    }


@app.get("/statistics-gated")
def statistics_gated(
    request: Request,
    coin: str = "BTC",
    db: Session = Depends(get_db),
):
    return regime_stack_endpoint(request=request, coin=coin, db=db)


# ── Full Intelligence (single fetch) ─────────

@app.get("/full-intelligence")
def full_intelligence(
    request: Request,
    coin: str = "BTC",
    db: Session = Depends(get_db),
):
    """
    Single endpoint — returns everything the dashboard needs.
    Reduces 10 frontend fetches to 1.
    """
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")

    auth_header = (
        request.headers.get("authorization")
        or request.headers.get("Authorization")
    )
    is_pro = resolve_pro_status(auth_header, db)

    stack   = build_regime_stack(coin, db)
    breadth = compute_market_breadth(db)
    vol_env = volatility_environment(coin, db)
    corr    = build_correlation_matrix(["BTC", "ETH", "SOL"])

    # Overview for all coins
    overview = []
    for c in SUPPORTED_COINS:
        s = build_regime_stack(c, db)
        if not s["incomplete"]:
            overview.append({
                "coin":       s["coin"],
                "macro":      s["macro"]["label"]     if s["macro"]     else None,
                "trend":      s["trend"]["label"]     if s["trend"]     else None,
                "execution":  s["execution"]["label"] if s["execution"] else None,
                "alignment":  s["alignment"],
                "direction":  s["direction"],
                "exposure":   s["exposure"],
                "shift_risk": s["shift_risk"],
            })

    # Confidence
    survival_v  = stack.get("survival")  or 50.0
    coherence_v = 0.0
    if stack.get("execution") and stack["execution"].get("coherence"):
        coherence_v = stack["execution"]["coherence"]

    confidence  = regime_confidence_score(
        alignment     = stack.get("alignment") or 0,
        survival      = survival_v,
        coherence     = coherence_v,
        breadth_score = breadth.get("breadth_score", 0),
    )

    # Transitions
    transitions = regime_transition_matrix(db, coin, "1h")

    # History
    history_records = (
        db.query(MarketSummary)
        .filter(
            MarketSummary.coin == coin,
            MarketSummary.timeframe == "1h",
        )
        .order_by(MarketSummary.created_at.desc())
        .limit(48)
        .all()
    )
    history_records.reverse()
    history = [
        {
            "hour":      i,
            "score":     r.score,
            "label":     r.label,
            "coherence": r.coherence,
            "timestamp": r.created_at,
        }
        for i, r in enumerate(history_records)
    ]

    # Survival curve
    durations = regime_durations(db, coin, "1h")
    age_1h    = current_age(db, coin, "1h")
    avg_dur   = average_regime_duration(db, coin, "1h")

    if len(durations) < 5:
        curve = [
            {
                "hour":     h,
                "survival": max(0, 100 - h * 4),
                "hazard":   min(100, h * 4.5),
            }
            for h in range(25)
        ]
    else:
        max_dur = int(max(durations))
        curve   = []
        for hour in range(max_dur + 1):
            survivors = [d for d in durations if d > hour]
            surv_pct  = (len(survivors) / len(durations)) * 100
            hz = 0.0
            if hour > 0 and survivors:
                exited = [d for d in durations if hour - 1 < d <= hour]
                hz     = (len(exited) / len(survivors)) * 100
            curve.append({
                "hour":     hour,
                "survival": round(surv_pct, 2),
                "hazard":   round(hz, 2),
            })

    # Maturity
    maturity_pct = trend_maturity_score(
        age_1h, avg_dur, stack.get("hazard") or 0
    )
    maturity_label = (
        "Early Phase" if maturity_pct < 25 else
        "Mid Phase"   if maturity_pct < 55 else
        "Late Phase"  if maturity_pct < 80 else
        "Overextended"
    )

    # Regime quality
    quality = compute_regime_quality(stack)

    # Base stack (free)
    base_stack = {
        "coin":       stack["coin"],
        "macro":      {"label": stack["macro"]["label"]}     if stack.get("macro")     else None,
        "trend":      {"label": stack["trend"]["label"]}     if stack.get("trend")     else None,
        "execution":  {"label": stack["execution"]["label"]} if stack.get("execution") else None,
        "alignment":  stack["alignment"],
        "direction":  stack["direction"],
        "exposure":   stack["exposure"],
        "shift_risk": stack["shift_risk"],
        "pro_required": not is_pro,
    }

    if is_pro:
        pct_rank = percentile_rank(
            db, coin, stack["execution"]["score"], "1h"
        ) if stack.get("execution") else None

        base_stack.update({
            "survival":                  stack["survival"],
            "hazard":                    stack["hazard"],
            "macro":                     stack["macro"],
            "trend":                     stack["trend"],
            "execution":                 stack["execution"],
            "macro_coherence":           stack["macro"]["coherence"]     if stack.get("macro")     else None,
            "trend_coherence":           stack["trend"]["coherence"]     if stack.get("trend")     else None,
            "exec_coherence":            stack["execution"]["coherence"] if stack.get("execution") else None,
            "regime_age_hours":          round(age_1h, 2),
            "trend_maturity":            maturity_pct,
            "percentile":                pct_rank,
            "avg_regime_duration_hours": round(avg_dur, 2),
        })

    return {
        "stack":                     base_stack,
        "confidence":                confidence,
        "volatility_env":            vol_env,
        "transitions":               transitions,
        "breadth":                   breadth,
        "overview":                  overview,
        "correlation":               corr,
        "history":                   history,
        "survival_curve":            curve,
        "risk_events":               RISK_EVENTS,
        "maturity_label":            maturity_label,
        "quality":                   quality,
        "avg_regime_duration_hours": round(avg_dur, 1),
    }


# ── Stripe ───────────────────────────────────

class CheckoutRequest(BaseModel):
    email: str = ""


@app.post("/create-checkout-session")
def create_checkout_session(body: CheckoutRequest = CheckoutRequest()):
    if not STRIPE_SECRET_KEY:
        raise HTTPException(status_code=500, detail="Stripe not configured")
    try:
        params = {
            "payment_method_types":  ["card"],
            "mode":                  "subscription",
            "line_items":            [{"price": STRIPE_PRICE_ID, "quantity": 1}],
            "success_url":           f"{FRONTEND_URL}/app?success=true",
            "cancel_url":            f"{FRONTEND_URL}/pricing",
            "allow_promotion_codes": True,
        }
        if body.email:
            params["customer_email"] = body.email
        session = stripe.checkout.Session.create(**params)
        return {"url": session.url}
    except stripe.error.StripeError as e:
        logger.error(f"Stripe error: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/stripe-webhook")
async def stripe_webhook(request: Request, db: Session = Depends(get_db)):
    payload    = await request.body()
    sig_header = request.headers.get("stripe-signature", "")

    if not STRIPE_WEBHOOK_SECRET:
        raise HTTPException(status_code=500, detail="Webhook secret not configured")

    try:
        event = stripe.Webhook.construct_event(
            payload, sig_header, STRIPE_WEBHOOK_SECRET
        )
    except stripe.error.SignatureVerificationError:
        raise HTTPException(status_code=400, detail="Invalid signature")

    event_type = event["type"]
    data       = event["data"]["object"]

    if event_type == "checkout.session.completed":
        customer_email  = data.get("customer_details", {}).get("email")
        customer_id     = data.get("customer")
        subscription_id = data.get("subscription")

        if customer_email:
            user = db.query(User).filter(User.email == customer_email).first()
            if not user:
                user = User(email=customer_email)
                db.add(user)

            access_token                 = str(uuid.uuid4())
            user.subscription_status    = "active"
            user.stripe_customer_id     = customer_id
            user.stripe_subscription_id = subscription_id
            user.alerts_enabled         = True
            user.access_token           = access_token
            db.commit()

            send_email(
                customer_email,
                "Welcome to ChainPulse Pro — Your Access Link",
                welcome_email_html(customer_email, access_token),
            )
            logger.info(f"Pro activated: {customer_email}")

    elif event_type in (
        "customer.subscription.deleted",
        "customer.subscription.paused",
    ):
        sub_id = data.get("id")
        user   = db.query(User).filter(
            User.stripe_subscription_id == sub_id
        ).first()
        if user:
            user.subscription_status = "inactive"
            user.access_token        = None
            db.commit()
            logger.info(f"Subscription deactivated: {user.email}")

    elif event_type == "invoice.payment_failed":
        customer_id = data.get("customer")
        user = db.query(User).filter(
            User.stripe_customer_id == customer_id
        ).first()
        if user:
            send_email(
                user.email,
                "ChainPulse — Payment Failed",
                f"""
                <div style="font-family:sans-serif;max-width:560px;margin:0 auto;
                            background:#000;color:#fff;padding:40px;">
                  <h2 style="color:#f87171;">Payment Failed</h2>
                  <p style="color:#999;">
                    Your ChainPulse Pro payment could not be processed.
                    Please update your payment method to maintain access.
                  </p>
                  <a href="{FRONTEND_URL}/pricing"
                     style="display:inline-block;background:#fff;color:#000;
                            padding:14px 28px;margin-top:24px;
                            text-decoration:none;font-weight:bold;border-radius:4px;">
                    Update Payment
                  </a>
                </div>
                """,
            )

    elif event_type == "customer.subscription.updated":
        sub_id = data.get("id")
        status = data.get("status")
        user   = db.query(User).filter(
            User.stripe_subscription_id == sub_id
        ).first()
        if user:
            if status == "active":
                user.subscription_status = "active"
                if not user.access_token:
                    user.access_token = str(uuid.uuid4())
            else:
                user.subscription_status = "inactive"
            db.commit()
            logger.info(f"Subscription updated: {user.email} -> {status}")

    return {"status": "received"}


# ── Email Helpers ────────────────────────────

def send_email(to_email: str, subject: str, html_content: str):
    if not RESEND_API_KEY:
        logger.warning("RESEND_API_KEY not set — email skipped")
        return
    try:
        r = requests.post(
            "https://api.resend.com/emails",
            headers={
                "Authorization": f"Bearer {RESEND_API_KEY}",
                "Content-Type":  "application/json",
            },
            json={
                "from":    "ChainPulse <alerts@chainpulse.pro>",
                "to":      [to_email],
                "subject": subject,
                "html":    html_content,
            },
            timeout=8,
        )
        r.raise_for_status()
        logger.info(f"Email sent to {to_email}")
    except Exception as e:
        logger.error(f"Email failed to {to_email}: {e}")


def welcome_email_html(email: str, access_token: str) -> str:
    url = f"{FRONTEND_URL}/app?token={access_token}"
    return f"""
    <div style="font-family:sans-serif;max-width:560px;margin:0 auto;
                background:#000;color:#fff;padding:40px;">
      <div style="font-size:11px;color:#555;text-transform:uppercase;
                  letter-spacing:2px;margin-bottom:16px;">ChainPulse Pro</div>
      <h1 style="font-size:24px;margin-bottom:8px;">Your Pro Access Is Active</h1>
      <p style="color:#999;margin-bottom:32px;">
        Click below to open your Pro dashboard.
        This link logs you in automatically. Bookmark it.
      </p>
      <a href="{url}"
         style="display:inline-block;background:#fff;color:#000;padding:14px 28px;
                text-decoration:none;font-weight:bold;border-radius:4px;">
        Open Pro Dashboard
      </a>
      <div style="margin-top:40px;border-top:1px solid #222;padding-top:24px;">
        <p style="color:#555;font-size:12px;margin-bottom:12px;">
          What you now have access to:
        </p>
        <ul style="color:#666;font-size:12px;line-height:2.2;padding-left:16px;">
          <li>Multi-timeframe regime stack — Macro / Trend / Execution</li>
          <li>Regime alignment score and direction</li>
          <li>Survival curve and hazard modeling</li>
          <li>Coherence index per timeframe layer</li>
          <li>Trend maturity score</li>
          <li>Regime playbook — what to do in each regime</li>
          <li>Regime quality grade (A / B / C / D)</li>
          <li>Regime stress meter</li>
          <li>Regime countdown timer</li>
          <li>Personalized exposure tracker</li>
          <li>Confidence trend chart</li>
          <li>Volatility and liquidity environment</li>
          <li>Transition probability matrix</li>
          <li>Portfolio exposure allocator</li>
          <li>Cross-asset correlation monitor</li>
          <li>Real-time shift alerts via email</li>
          <li>Daily morning regime brief</li>
          <li>Multi-asset: BTC, ETH, SOL, BNB, AVAX, LINK, ADA</li>
        </ul>
      </div>
      <p style="color:#333;font-size:11px;margin-top:40px;
                border-top:1px solid #111;padding-top:20px;">
        ChainPulse. Not financial advice.
      </p>
    </div>
    """


def regime_alert_html(coin: str, stack: dict, quality: dict = None) -> str:
    macro_l    = stack["macro"]["label"]     if stack.get("macro")     else "—"
    trend_l    = stack["trend"]["label"]     if stack.get("trend")     else "—"
    exec_l     = stack["execution"]["label"] if stack.get("execution") else "—"
    align      = stack.get("alignment",  0)
    shift_risk = stack.get("shift_risk", 0)
    exposure   = stack.get("exposure",   0)
    pb         = PLAYBOOK_DATA.get(exec_l, PLAYBOOK_DATA["Neutral"])

    quality_row = ""
    if quality:
        grade_color = (
            "#34d399" if quality["grade"].startswith("A") else
            "#4ade80" if quality["grade"].startswith("B") else
            "#facc15" if quality["grade"].startswith("C") else
            "#f87171"
        )
        quality_row = f"""
        <tr>
          <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;
                     color:#555;font-size:12px;">Regime Grade</td>
          <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;
                     color:{grade_color};text-align:right;font-weight:bold;">
            {quality['grade']} — {quality['structural']}
          </td>
        </tr>
        """

    actions_html = "".join(
        f'<li style="color:#999;font-size:12px;line-height:1.8;">{a}</li>'
        for a in pb["actions"][:3]
    )

    return f"""
    <div style="font-family:sans-serif;max-width:560px;margin:0 auto;
                background:#000;color:#fff;padding:40px;">
      <div style="font-size:11px;color:#555;text-transform:uppercase;
                  letter-spacing:2px;margin-bottom:16px;">ChainPulse Alert</div>
      <h2 style="color:#f87171;margin-bottom:16px;">
        ⚠ Regime Shift Risk Elevated — {coin}
      </h2>

      <table style="width:100%;border-collapse:collapse;margin-bottom:24px;">
        <tr>
          <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;
                     color:#555;font-size:12px;">Macro (1D)</td>
          <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;
                     color:#fff;text-align:right;">{macro_l}</td>
        </tr>
        <tr>
          <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;
                     color:#555;font-size:12px;">Trend (4H)</td>
          <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;
                     color:#fff;text-align:right;">{trend_l}</td>
        </tr>
        <tr>
          <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;
                     color:#555;font-size:12px;">Execution (1H)</td>
          <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;
                     color:#fff;text-align:right;">{exec_l}</td>
        </tr>
        <tr>
          <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;
                     color:#555;font-size:12px;">Alignment</td>
          <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;
                     color:#fff;text-align:right;">{align}%</td>
        </tr>
        {quality_row}
      </table>

      <p style="color:#999;margin-bottom:24px;">
        Shift Risk: <strong style="color:#f87171;">{shift_risk}%</strong>
        &nbsp;·&nbsp;
        Recommended Exposure: <strong style="color:#fff;">{exposure}%</strong>
        &nbsp;·&nbsp;
        Strategy: <strong style="color:#fff;">{pb['strategy_mode']}</strong>
      </p>

      <div style="border:1px solid #1f1f1f;padding:16px;margin-bottom:24px;">
        <div style="font-size:11px;color:#555;text-transform:uppercase;
                    letter-spacing:1px;margin-bottom:10px;">
          Regime Playbook — {exec_l}
        </div>
        <ul style="padding-left:16px;margin:0;">
          {actions_html}
        </ul>
      </div>

      <a href="{FRONTEND_URL}/app"
         style="display:inline-block;background:#fff;color:#000;padding:14px 28px;
                text-decoration:none;font-weight:bold;border-radius:4px;">
        View Dashboard
      </a>
      <p style="color:#333;font-size:11px;margin-top:40px;
                border-top:1px solid #111;padding-top:20px;">
        ChainPulse. Not financial advice.
      </p>
    </div>
    """


def morning_email_html(stacks: list, access_token: str) -> str:
    url  = (
        f"{FRONTEND_URL}/app?token={access_token}"
        if access_token else f"{FRONTEND_URL}/app"
    )
    rows = ""
    for s in stacks:
        shift_risk  = s.get("shift_risk") or 0
        exposure    = s.get("exposure")   or 0
        exec_label  = s["execution"]["label"] if s.get("execution") else "—"
        macro_label = s["macro"]["label"]     if s.get("macro")     else "—"
        pb          = PLAYBOOK_DATA.get(exec_label, PLAYBOOK_DATA["Neutral"])
        quality     = compute_regime_quality(s)

        risk_color = (
            "#f87171" if shift_risk > 70 else
            "#facc15" if shift_risk > 45 else
            "#4ade80"
        )
        grade_color = (
            "#34d399" if quality["grade"].startswith("A") else
            "#4ade80" if quality["grade"].startswith("B") else
            "#facc15" if quality["grade"].startswith("C") else
            "#f87171"
        )
        rows += f"""
        <tr>
          <td style="padding:12px 8px;border-bottom:1px solid #1f1f1f;
                     color:#fff;font-weight:600;">{s["coin"]}</td>
          <td style="padding:12px 8px;border-bottom:1px solid #1f1f1f;
                     color:#999;font-size:12px;">{macro_label}</td>
          <td style="padding:12px 8px;border-bottom:1px solid #1f1f1f;
                     color:#999;font-size:12px;">{exec_label}</td>
          <td style="padding:12px 8px;border-bottom:1px solid #1f1f1f;
                     color:#fff;">{exposure}%</td>
          <td style="padding:12px 8px;border-bottom:1px solid #1f1f1f;
                     color:{risk_color};font-weight:600;">{shift_risk}%</td>
          <td style="padding:12px 8px;border-bottom:1px solid #1f1f1f;
                     color:{grade_color};font-weight:600;">{quality["grade"]}</td>
          <td style="padding:12px 8px;border-bottom:1px solid #1f1f1f;
                     color:#666;font-size:11px;">{pb['strategy_mode']}</td>
        </tr>
        """

    return f"""
    <div style="font-family:sans-serif;max-width:640px;margin:0 auto;
                background:#000;color:#fff;padding:40px;">
      <div style="font-size:11px;color:#555;text-transform:uppercase;
                  letter-spacing:2px;margin-bottom:16px;">
        ChainPulse Morning Brief
      </div>
      <h1 style="font-size:22px;margin-bottom:8px;">Daily Regime Snapshot</h1>
      <p style="color:#666;font-size:13px;margin-bottom:32px;">
        Multi-timeframe regime conditions across all tracked assets.
      </p>

      <table style="width:100%;border-collapse:collapse;">
        <thead>
          <tr>
            {"".join(
                f'<th style="text-align:left;padding:8px;color:#444;font-size:11px;'
                f'text-transform:uppercase;border-bottom:1px solid #222;">{h}</th>'
                for h in ["Asset","Macro","Execution","Exposure","Shift Risk","Grade","Mode"]
            )}
          </tr>
        </thead>
        <tbody>{rows}</tbody>
      </table>

      <div style="margin-top:32px;border:1px solid #1f1f1f;padding:20px;">
        <div style="font-size:11px;color:#555;text-transform:uppercase;
                    letter-spacing:1px;margin-bottom:12px;">
          How to use this brief
        </div>
        <ul style="color:#666;font-size:12px;line-height:2;padding-left:16px;margin:0;">
          <li>Grade A/B+ = high quality regime — favour continuation trades</li>
          <li>Grade C/D = fragile regime — reduce size, widen stops</li>
          <li>Shift Risk &gt;70% = consider reducing exposure now</li>
          <li>Check dashboard for full playbook and survival curve</li>
        </ul>
      </div>

      <a href="{url}"
         style="display:inline-block;background:#fff;color:#000;padding:14px 28px;
                margin-top:32px;text-decoration:none;font-weight:bold;border-radius:4px;">
        Open Dashboard
      </a>
      <p style="color:#333;font-size:11px;margin-top:40px;
                border-top:1px solid #111;padding-top:20px;">
        ChainPulse. Not financial advice.
      </p>
    </div>
    """


# ── Alert Dispatch ───────────────────────────

@app.get("/send-alerts")
def send_alerts(secret: str = "", db: Session = Depends(get_db)):
    if secret != UPDATE_SECRET:
        raise HTTPException(status_code=403, detail="Unauthorized")

    pro_users = db.query(User).filter(
        User.subscription_status == "active",
        User.alerts_enabled      == True,
    ).all()

    sent = 0
    for coin in SUPPORTED_COINS:
        stack = build_regime_stack(coin, db)
        if stack["incomplete"]:
            continue
        if (stack.get("shift_risk") or 0) < 70:
            continue

        quality = compute_regime_quality(stack)

        for user in pro_users:
            if user.last_alert_sent:
                hrs = (
                    datetime.datetime.utcnow() - user.last_alert_sent
                ).total_seconds() / 3600
                if hrs < 12:
                    continue
            send_email(
                user.email,
                f"ChainPulse Alert — {coin} Regime Shift Risk Elevated",
                regime_alert_html(coin, stack, quality),
            )
            user.last_alert_sent = datetime.datetime.utcnow()
            db.commit()
            sent += 1

    return {"status": "complete", "alerts_sent": sent}


@app.get("/send-morning-email")
def send_morning_email(secret: str = "", db: Session = Depends(get_db)):
    if secret != UPDATE_SECRET:
        raise HTTPException(status_code=403, detail="Unauthorized")

    pro_users = db.query(User).filter(
        User.subscription_status == "active",
        User.alerts_enabled      == True,
    ).all()

    stacks = []
    for coin in SUPPORTED_COINS:
        stack = build_regime_stack(coin, db)
        if not stack["incomplete"]:
            stacks.append(stack)

    sent = 0
    for user in pro_users:
        send_email(
            user.email,
            "ChainPulse Morning Regime Brief",
            morning_email_html(stacks, user.access_token or ""),
        )
        sent += 1

    return {"status": "sent", "count": sent}


# ── Subscribe / Confirm ──────────────────────

class SubscribeRequest(BaseModel):
    email: str


@app.post("/subscribe")
def subscribe(body: SubscribeRequest, db: Session = Depends(get_db)):
    email = body.email.strip().lower()
    user  = db.query(User).filter(User.email == email).first()
    if not user:
        user = User(
            email=email,
            subscription_status="inactive",
            alerts_enabled=False,
        )
        db.add(user)
        db.commit()

    send_email(
        email,
        "Confirm your ChainPulse subscription",
        f"""
        <div style="font-family:sans-serif;max-width:560px;margin:0 auto;
                    background:#000;color:#fff;padding:40px;">
          <div style="font-size:11px;color:#555;text-transform:uppercase;
                      letter-spacing:2px;margin-bottom:16px;">ChainPulse</div>
          <h2 style="margin-bottom:16px;">Confirm Your Subscription</h2>
          <p style="color:#999;margin-bottom:32px;">
            Click below to activate regime alerts and daily briefs:
          </p>
          <a href="https://chainpulse-backend-2xok.onrender.com/confirm?email={email}"
             style="display:inline-block;background:#fff;color:#000;padding:14px 28px;
                    text-decoration:none;font-weight:bold;border-radius:4px;">
            Confirm Subscription
          </a>
          <p style="color:#333;font-size:11px;margin-top:40px;">
            ChainPulse. Not financial advice.
          </p>
        </div>
        """,
    )
    return {"status": "confirmation_sent"}


@app.get("/confirm")
def confirm(email: str, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == email).first()
    if user:
        user.alerts_enabled = True
        db.commit()
        return {"status": "confirmed", "email": email}
    raise HTTPException(status_code=404, detail="Email not found")


# ── Debug / Utility ──────────────────────────

@app.get("/debug-prices")
def debug_prices(coin: str = "BTC", interval: str = "1h"):
    prices, volumes = get_klines(coin, interval, limit=120)
    return {
        "coin":         coin,
        "interval":     interval,
        "price_count":  len(prices),
        "volume_count": len(volumes),
        "last_price":   prices[-1]  if prices  else None,
        "first_price":  prices[0]   if prices  else None,
        "last_volume":  volumes[-1] if volumes else None,
    }


@app.get("/debug-stack")
def debug_stack(coin: str = "BTC", db: Session = Depends(get_db)):
    """Shows full raw stack without auth gating — for debugging only."""
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")
    stack   = build_regime_stack(coin, db)
    breadth = compute_market_breadth(db)
    quality = compute_regime_quality(stack) if not stack["incomplete"] else None
    return {
        "stack":   stack,
        "breadth": breadth,
        "quality": quality,
    }


@app.get("/sample-report")
def sample_report():
    path = "sample_report.pdf"
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Report not found")
    return FileResponse(path, media_type="application/pdf")


@app.get("/user-status")
def user_status(
    request: Request,
    db: Session = Depends(get_db),
):
    """
    Lets the frontend check subscription status without
    making a full data fetch.
    """
    auth_header = (
        request.headers.get("authorization")
        or request.headers.get("Authorization")
    )
    is_pro = resolve_pro_status(auth_header, db)
    return {
        "is_pro":    is_pro,
        "timestamp": datetime.datetime.utcnow(),
    }