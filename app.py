# ─────────────────────────────────────────
# main.py — ChainPulse API v4.1
# ─────────────────────────────────────────
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
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
import json
import resend

# ─────────────────────────────────────────
# SETUP
# ─────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("chainpulse")

load_dotenv()

DATABASE_URL           = os.getenv("DATABASE_URL", "sqlite:///./chainpulse.db")
STRIPE_SECRET_KEY      = os.getenv("STRIPE_SECRET_KEY")
STRIPE_PRICE_ID        = os.getenv("STRIPE_PRICE_ID")
STRIPE_PRICE_ID_ANNUAL = os.getenv("STRIPE_PRICE_ID_ANNUAL")
STRIPE_WEBHOOK_SECRET  = os.getenv("STRIPE_WEBHOOK_SECRET")
RESEND_API_KEY         = os.getenv("RESEND_API_KEY")
UPDATE_SECRET          = os.getenv("UPDATE_SECRET", "changeme")
FRONTEND_URL           = os.getenv("FRONTEND_URL", "https://chainpulse.pro")
BACKEND_URL            = os.getenv("BACKEND_URL", "https://chainpulse-backend-2xok.onrender.com")
RESEND_FROM_EMAIL = (os.getenv("RESEND_FROM_EMAIL") or "onboarding@resend.dev").strip()


if STRIPE_SECRET_KEY:
    stripe.api_key = STRIPE_SECRET_KEY

engine       = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine)
Base         = declarative_base()

# ─────────────────────────────────────────
# DATABASE MODELS
# ─────────────────────────────────────────
class MarketSummary(Base):
    __tablename__  = "market_summary"
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
    __tablename__          = "users"
    id                     = Column(Integer, primary_key=True)
    email                  = Column(String, unique=True, index=True)
    subscription_status    = Column(String, default="inactive")
    stripe_customer_id     = Column(String, nullable=True)
    stripe_subscription_id = Column(String, nullable=True)
    alerts_enabled         = Column(Boolean, default=False)
    last_alert_sent        = Column(DateTime, nullable=True)
    access_token           = Column(String, nullable=True, index=True)
    created_at             = Column(DateTime, default=datetime.datetime.utcnow)


class UserProfile(Base):
    __tablename__       = "user_profiles"
    id                  = Column(Integer, primary_key=True)
    user_id             = Column(Integer, index=True)
    email               = Column(String, unique=True, index=True)
    max_drawdown_pct    = Column(Float, default=20.0)
    typical_leverage    = Column(Float, default=1.0)
    holding_period_days = Column(Integer, default=10)
    risk_identity       = Column(String, default="balanced")
    risk_multiplier     = Column(Float, default=1.0)
    created_at          = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at          = Column(DateTime, default=datetime.datetime.utcnow)


class ExposureLog(Base):
    __tablename__      = "exposure_logs"
    id                 = Column(Integer, primary_key=True)
    email              = Column(String, index=True)
    coin               = Column(String, default="BTC")
    user_exposure_pct  = Column(Float)
    model_exposure_pct = Column(Float)
    regime_label       = Column(String)
    hazard_at_log      = Column(Float, default=0)
    shift_risk_at_log  = Column(Float, default=0)
    alignment_at_log   = Column(Float, default=0)
    followed_model     = Column(Boolean, default=False)
    price_at_log       = Column(Float, default=0)
    created_at         = Column(DateTime, default=datetime.datetime.utcnow)


class PerformanceEntry(Base):
    __tablename__      = "performance_entries"
    id                 = Column(Integer, primary_key=True)
    email              = Column(String, index=True)
    coin               = Column(String, default="BTC")
    date               = Column(DateTime, default=datetime.datetime.utcnow)
    user_exposure_pct  = Column(Float, default=0)
    model_exposure_pct = Column(Float, default=0)
    price_open         = Column(Float, default=0)
    price_close        = Column(Float, default=0)
    user_return_pct    = Column(Float, default=0)
    model_return_pct   = Column(Float, default=0)
    regime_label       = Column(String, default="Neutral")
    discipline_flags   = Column(String, default="")


Base.metadata.create_all(bind=engine)

app = FastAPI(title="ChainPulse API", version="4.1")

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
# PYDANTIC REQUEST MODELS
# ─────────────────────────────────────────
class SubscribeRequest(BaseModel):
    email: str


class UserProfileRequest(BaseModel):
    email:               str
    max_drawdown_pct:    float = 20.0
    typical_leverage:    float = 1.0
    holding_period_days: int   = 10
    risk_identity:       str   = "balanced"


class ExposureLogRequest(BaseModel):
    email:             str
    coin:              str   = "BTC"
    user_exposure_pct: float


class PerformanceEntryRequest(BaseModel):
    email:             str
    coin:              str   = "BTC"
    user_exposure_pct: float
    price_open:        float
    price_close:       float


class CheckoutRequest(BaseModel):
    email:         str = ""
    billing_cycle: str = "monthly"  
    annual:        bool = False 


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
    "Strong Risk-On":  2,
    "Risk-On":         1,
    "Neutral":         0,
    "Risk-Off":       -1,
    "Strong Risk-Off": -2,
}

PRICE_MONTHLY = 39
PRICE_ANNUAL  = 348

RISK_EVENTS = [
    {"name": "FOMC Meeting",    "type": "macro",  "impact": "High"},
    {"name": "CPI Release",     "type": "macro",  "impact": "High"},
    {"name": "Options Expiry",  "type": "market", "impact": "Medium"},
    {"name": "ETF Flow Report", "type": "market", "impact": "Medium"},
    {"name": "BTC Halving",     "type": "crypto", "impact": "High"},
    {"name": "Fed Minutes",     "type": "macro",  "impact": "Medium"},
    {"name": "PCE Inflation",   "type": "macro",  "impact": "High"},
]

PLAYBOOK_DATA = {
    "Strong Risk-On": {
        "exposure_band":      "65–80%",
        "strategy_mode":      "Aggressive",
        "trend_follow_wr":    72,
        "mean_revert_wr":     38,
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
        "exposure_band":      "50–65%",
        "strategy_mode":      "Balanced",
        "trend_follow_wr":    63,
        "mean_revert_wr":     44,
        "avg_remaining_days": 9,
        "actions": [
            "Favour pullback entries in trend direction",
            "Scale into positions over 2–3 entries",
            "Monitor breadth for continuation signal",
        ],
        "avoid": ["Over-leveraging at breakouts", "Chasing extended moves"],
    },
    "Neutral": {
        "exposure_band":      "25–45%",
        "strategy_mode":      "Neutral",
        "trend_follow_wr":    49,
        "mean_revert_wr":     51,
        "avg_remaining_days": 6,
        "actions": [
            "Reduce overall exposure",
            "Preserve capital — this is a transition zone",
        ],
        "avoid": ["Strong directional bias", "Large position sizes"],
    },
    "Risk-Off": {
        "exposure_band":      "10–25%",
        "strategy_mode":      "Defensive",
        "trend_follow_wr":    31,
        "mean_revert_wr":     57,
        "avg_remaining_days": 7,
        "actions": [
            "Reduce long exposure significantly",
            "Hold cash — optionality has value",
        ],
        "avoid": ["Buying dips aggressively", "Adding to losing longs"],
    },
    "Strong Risk-Off": {
        "exposure_band":      "0–10%",
        "strategy_mode":      "Fully Defensive",
        "trend_follow_wr":    22,
        "mean_revert_wr":     48,
        "avg_remaining_days": 11,
        "actions": [
            "Move to maximum cash allocation",
            "Monitor for capitulation signals",
        ],
        "avoid": ["Catching falling knives", "Any leveraged long exposure"],
    },
}

# ─────────────────────────────────────────
# AUTH HELPERS
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


def get_auth_header(request: Request) -> Optional[str]:
    return (
        request.headers.get("authorization")
        or request.headers.get("Authorization")
    )


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
    raw            = alignment * magnitude_norm * (1 - vol_penalty)
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
    score     = 0.55 * mom_long + 0.35 * mom_short - 0.08 * vol + 0.02 * vol_mom
    score     = max(-100, min(100, score))
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
    if total > 0: return "bullish"
    if total < 0: return "bearish"
    return "mixed"


# ─────────────────────────────────────────
# STATISTICS ENGINE
# ─────────────────────────────────────────
def get_history(db: Session, coin: str, timeframe: str = "1h"):
    return (
        db.query(MarketSummary)
        .filter(
            MarketSummary.coin      == coin,
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
            MarketSummary.coin      == coin,
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


def trend_maturity_score(age: float, avg_duration: float, hazard: float) -> float:
    if avg_duration == 0:
        age_component = min(100, age * 5)
    else:
        age_component = min(100, (age / avg_duration) * 100)
    return round(min(100, max(0, age_component * 0.6 + hazard * 0.4)), 2)


def regime_shift_risk(hazard: float, survival: float, coherence: float) -> float:
    return round(
        min(100.0,
            hazard           * 0.50 +
            (100 - survival) * 0.35 +
            (100 - coherence)* 0.15),
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
    exposure           = base * persistence_factor * hazard_penalty * coherence_factor
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

    persistence_factor = survival_1h / 100
    hazard_penalty     = 1 - (hazard_1h / 100) * 0.65
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
                MarketSummary.coin      == coin,
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
                MarketSummary.coin      == coin,
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
            "total": 0, "breadth_score": 0,
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
        vol_label, vol_score = "Extreme", 90
    elif vol_ratio > 1.0:
        vol_label, vol_score = "Elevated", 65
    elif vol_ratio > 0.5:
        vol_label, vol_score = "Moderate", 40
    else:
        vol_label, vol_score = "Low", 15

    if len(prices_1h) >= 24:
        rets     = [
            (prices_1h[i] - prices_1h[i - 1]) / prices_1h[i - 1]
            for i in range(1, 24)
        ]
        positive = sum(1 for r in rets if r > 0)
        stab_pct = round((positive / len(rets)) * 100, 1)
        stab_lbl = (
            "Strong"        if stab_pct > 65 else
            "Moderate"      if stab_pct > 50 else
            "Weak"          if stab_pct > 35 else
            "Deteriorating"
        )
    else:
        stab_pct, stab_lbl = 50, "Insufficient data"

    stress_score = round(vol_score * 0.6 + (100 - stab_pct) * 0.4, 1)
    stress_label = (
        "High"     if stress_score > 70 else
        "Moderate" if stress_score > 40 else
        "Low"
    )

    _, volumes = get_klines(coin, "1h", limit=24)
    if volumes and len(volumes) >= 10:
        avg_vol   = sum(volumes) / len(volumes)
        recent_v  = sum(volumes[-6:]) / 6
        liq_ratio = recent_v / (avg_vol + 0.0001)
        liq_label = (
            "High"   if liq_ratio > 1.3 else
            "Normal" if liq_ratio > 0.7 else
            "Thin"
        )
    else:
        liq_label = "Unknown"

    return {
        "volatility_label": vol_label,
        "volatility_score": vol_score,
        "stability_label":  stab_lbl,
        "stability_score":  round(stab_pct, 1),
        "stress_label":     stress_label,
        "stress_score":     round(stress_score, 1),
        "liquidity_label":  liq_label,
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
            (prices[i] - prices[i - 1]) / prices[i - 1]
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
    coins_to_use = coins if coins else ["BTC", "ETH", "SOL"]
    coins_to_use = [c for c in coins_to_use if c in SUPPORTED_COINS]
    if len(coins_to_use) < 2:
        coins_to_use = ["BTC", "ETH", "SOL"]

    price_map = {}
    for coin in coins_to_use:
        prices, _ = get_klines(coin, "1h", limit=50)
        if prices:
            price_map[coin] = prices

    pairs     = []
    alerts    = []
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
        label = "High";     desc = "Strong regime — elevated conviction warranted"
    elif confidence > 50:
        label = "Moderate"; desc = "Developing regime — standard position sizing"
    elif confidence > 30:
        label = "Low";      desc = "Weak regime — reduce size, widen stops"
    else:
        label = "Very Low"; desc = "No clear regime — minimal exposure only"

    return {
        "score":       confidence,
        "label":       label,
        "description": desc,
        "components": {
            "alignment": round(alignment, 1),
            "survival":  round(survival, 1),
            "coherence": round(abs(coherence), 1),
            "breadth":   round(breadth_norm, 1),
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

    STATES      = ["Strong Risk-On", "Risk-On", "Neutral", "Risk-Off", "Strong Risk-Off"]
    transitions = {s: {t: 0 for t in STATES} for s in STATES}

    for i in range(len(records) - 1):
        cur = records[i].label
        nxt = records[i + 1].label
        if cur in transitions and nxt in transitions:
            transitions[cur][nxt] += 1

    current_state = records[-1].label if records else "Neutral"
    row           = transitions.get(current_state, {})
    total         = sum(row.values())

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
    mode_mult    = {"conservative": 0.70, "balanced": 1.00, "aggressive": 1.25}
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
        "spot_allocation":   round(deployed * spot_pct, 2),
        "swing_allocation":  round(deployed * swing_pct, 2),
        "cash_pct":          round((cash / account_size) * 100, 1),
    }
# ─────────────────────────────────────────
# Daily Email Template
# ─────────────────────────────────────────
def generate_daily_brief_html(regime, exposure, shift_risk, directive):
    return f"""
    <div style="background:#000;padding:40px 0;font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif;">
      <div style="max-width:600px;margin:0 auto;background:#0b0b0f;border:1px solid rgba(255,255,255,0.08);border-radius:24px;padding:40px;color:#fff;">

        <div style="font-size:12px;letter-spacing:2px;text-transform:uppercase;color:#6b7280;">
          ChainPulse Daily Brief
        </div>

        <h2 style="margin-top:16px;font-size:22px;">
          BTC Regime: <span style="color:#ef4444;">{regime}</span>
        </h2>

        <div style="margin-top:24px;">
          <div style="font-size:14px;color:#9ca3af;">Recommended Exposure</div>
          <div style="font-size:28px;font-weight:600;margin-top:4px;">
            {exposure}%
          </div>
        </div>

        <div style="margin-top:24px;">
          <div style="font-size:14px;color:#9ca3af;">Shift Risk</div>
          <div style="font-size:22px;font-weight:600;margin-top:4px;">
            {shift_risk}%
          </div>
        </div>

        <div style="margin-top:24px;">
          <div style="font-size:14px;color:#9ca3af;">Directive</div>
          <div style="font-size:18px;font-weight:600;margin-top:4px;">
            {directive}
          </div>
        </div>

        <div style="margin:30px 0;">
          <a href="https://chainpulse.pro/app"
             style="background:#fff;color:#000;padding:14px 28px;border-radius:14px;text-decoration:none;font-weight:600;display:inline-block;">
             View Full Dashboard
          </a>
        </div>

        <hr style="border:none;border-top:1px solid rgba(255,255,255,0.08);margin:30px 0;">

        <p style="font-size:12px;color:#6b7280;">
          You are receiving this because you subscribed to ChainPulse Daily Brief.
        </p>

      </div>
    </div>
    """

# ─────────────────────────────────────────
# REGIME QUALITY SCORE
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

    if score >= 80:   grade, structural, breakdown = "A",  "Excellent", "Low"
    elif score >= 65: grade, structural, breakdown = "B+", "Strong",    "Low-Moderate"
    elif score >= 50: grade, structural, breakdown = "B",  "Healthy",   "Moderate"
    elif score >= 35: grade, structural, breakdown = "C",  "Weakening", "Elevated"
    else:             grade, structural, breakdown = "D",  "Fragile",   "High"

    return {
        "grade":      grade,
        "score":      score,
        "structural": structural,
        "breakdown":  breakdown,
    }


# ─────────────────────────────────────────
# DECISION ENGINE
# ─────────────────────────────────────────
def compute_decision_score(
    hazard:        float,
    shift_risk:    float,
    alignment:     float,
    survival:      float,
    breadth_score: float,
    maturity_pct:  float,
) -> dict:
    breadth_norm    = (breadth_score + 100) / 2
    survival_score  = survival
    safety_score    = 100 - hazard
    shift_score     = 100 - shift_risk
    maturity_score  = 100 - maturity_pct
    breadth_bullish = breadth_norm

    decision_score = round(
        survival_score  * 0.25 +
        safety_score    * 0.25 +
        shift_score     * 0.20 +
        alignment       * 0.15 +
        maturity_score  * 0.10 +
        breadth_bullish * 0.05,
        1,
    )
    decision_score = min(100, max(0, decision_score))

    if decision_score >= 80:
        directive   = "Increase Exposure"
        action      = "aggressive"
        color       = "emerald"
        description = "All signals aligned bullish. Regime is healthy and persistent."
        actions     = [
            "Add to existing positions on pullbacks",
            "Increase position size toward upper band",
            "Trail stops to lock in gains",
            "Monitor for breadth confirmation",
        ]
    elif decision_score >= 60:
        directive   = "Maintain Exposure"
        action      = "hold"
        color       = "green"
        description = "Regime intact. No action required. Stay the course."
        actions     = [
            "Hold current positions",
            "No new leverage",
            "Monitor hazard rate for changes",
            "Re-evaluate if shift risk exceeds 60%",
        ]
    elif decision_score >= 40:
        directive   = "Trim Exposure"
        action      = "trim"
        color       = "yellow"
        description = "Regime showing early deterioration. Reduce risk selectively."
        actions     = [
            "Reduce position size by 15–25%",
            "Avoid adding new breakout entries",
            "Take partial profits on extended positions",
            "Tighten stop losses",
        ]
    elif decision_score >= 20:
        directive   = "Switch to Defensive"
        action      = "defensive"
        color       = "orange"
        description = "Multiple deterioration signals active. Reduce exposure significantly."
        actions     = [
            "Reduce exposure to lower band immediately",
            "No new long entries",
            "Move profits to cash or stables",
            "Wait for regime confirmation before re-entering",
        ]
    else:
        directive   = "Risk-Off — Exit"
        action      = "exit"
        color       = "red"
        description = "Regime breakdown in progress. Capital preservation is the priority."
        actions     = [
            "Exit or heavily reduce all positions",
            "Move to maximum cash allocation",
            "Do not average down",
            "Wait for full regime reset before re-entry",
        ]

    return {
        "score":       decision_score,
        "directive":   directive,
        "action":      action,
        "color":       color,
        "description": description,
        "actions":     actions,
        "components": {
            "survival":  round(survival_score, 1),
            "safety":    round(safety_score, 1),
            "shift":     round(shift_score, 1),
            "alignment": round(alignment, 1),
            "maturity":  round(maturity_score, 1),
            "breadth":   round(breadth_bullish, 1),
        },
    }


# ─────────────────────────────────────────
# IF NOTHING PANEL
# ─────────────────────────────────────────
def compute_if_nothing_panel(
    user_exposure:  float,
    model_exposure: float,
    hazard:         float,
    shift_risk:     float,
    regime_label:   str,
) -> dict:
    delta        = user_exposure - model_exposure
    over_exposed = delta > 0
    delta_abs    = abs(round(delta, 1))

    base_dd_prob        = round((hazard * 0.5 + shift_risk * 0.5), 1)
    exposure_multiplier = 1 + (delta / 100) * 0.8 if over_exposed else 1.0
    adj_dd_prob         = round(min(95, base_dd_prob * exposure_multiplier), 1)
    dd_prob_increase    = round(adj_dd_prob - base_dd_prob, 1)
    dd_magnitude        = round((hazard / 100) * 0.25 * 100, 1)
    expected_loss_pct   = round((user_exposure / 100) * (dd_magnitude / 100) * 100, 1)
    model_loss_pct      = round((model_exposure / 100) * (dd_magnitude / 100) * 100, 1)

    if over_exposed and delta_abs > 15:
        severity = "high"
        message  = f"You are {delta_abs}% over regime tolerance"
        sub      = "Maintaining this exposure significantly increases drawdown probability."
    elif over_exposed and delta_abs > 5:
        severity = "medium"
        message  = f"You are {delta_abs}% above optimal"
        sub      = "Small overexposure — consider trimming on the next strength."
    elif not over_exposed:
        severity = "low"
        message  = f"You are {delta_abs}% below optimal — room to add"
        sub      = "Consider scaling in on the next pullback if signals hold."
    else:
        severity = "low"
        message  = "Exposure aligned with regime recommendation"
        sub      = "No action required."

    return {
        "user_exposure":     round(user_exposure, 1),
        "model_exposure":    round(model_exposure, 1),
        "delta":             round(delta, 1),
        "delta_abs":         delta_abs,
        "over_exposed":      over_exposed,
        "severity":          severity,
        "message":           message,
        "sub":               sub,
        "drawdown_prob":     adj_dd_prob,
        "dd_prob_increase":  dd_prob_increase,
        "expected_loss_pct": expected_loss_pct,
        "model_loss_pct":    model_loss_pct,
        "dd_magnitude_est":  dd_magnitude,
        "regime_label":      regime_label,
    }


# ─────────────────────────────────────────
# DISCIPLINE SCORING ENGINE
# ─────────────────────────────────────────
def compute_discipline_score(logs: list) -> dict:
    if not logs:
        return {
            "score":   None,
            "label":   "No data yet",
            "flags":   [],
            "summary": "Log your exposure to start tracking discipline.",
        }

    total_logs = len(logs)
    followed   = sum(1 for l in logs if l.followed_model)
    base_score = round((followed / total_logs) * 100, 1) if total_logs > 0 else 50
    flags      = []
    penalties  = 0
    bonuses    = 0

    for log in logs:
        hazard     = log.hazard_at_log    or 0
        shift_risk = log.shift_risk_at_log or 0
        user_exp   = log.user_exposure_pct  or 0
        model_exp  = log.model_exposure_pct or 50
        delta      = user_exp - model_exp

        if hazard > 65 and delta > 10:
            flags.append({
                "type":   "penalty",
                "label":  "Added leverage in elevated hazard",
                "date":   log.created_at.strftime("%b %d"),
                "regime": log.regime_label,
            })
            penalties += 10

        if "Risk-Off" in (log.regime_label or "") and user_exp > model_exp + 15:
            flags.append({
                "type":   "penalty",
                "label":  "Over-exposed in Risk-Off regime",
                "date":   log.created_at.strftime("%b %d"),
                "regime": log.regime_label,
            })
            penalties += 15

        if shift_risk > 70 and delta < -5:
            flags.append({
                "type":   "bonus",
                "label":  "Reduced exposure on hazard spike",
                "date":   log.created_at.strftime("%b %d"),
                "regime": log.regime_label,
            })
            bonuses += 10

        if "Strong Risk-On" in (log.regime_label or "") and abs(delta) < 10:
            flags.append({
                "type":   "bonus",
                "label":  "Stayed within band in strong regime",
                "date":   log.created_at.strftime("%b %d"),
                "regime": log.regime_label,
            })
            bonuses += 5

    final_score = round(min(100, max(0, base_score + bonuses - penalties)), 1)

    if final_score >= 85:   label = "Excellent"
    elif final_score >= 70: label = "Good"
    elif final_score >= 50: label = "Average"
    elif final_score >= 30: label = "Needs Work"
    else:                   label = "Poor"

    return {
        "score":     final_score,
        "label":     label,
        "flags":     flags[-10:],
        "followed":  followed,
        "total":     total_logs,
        "bonuses":   bonuses,
        "penalties": penalties,
        "summary":   f"You followed the model {followed}/{total_logs} times.",
    }


def compute_performance_comparison(entries: list) -> dict:
    if len(entries) < 3:
        return {
            "user_total_return":  None,
            "model_total_return": None,
            "alpha":              None,
            "periods":            len(entries),
            "message":            "Need at least 3 entries for comparison.",
        }

    user_returns  = [e.user_return_pct  for e in entries if e.user_return_pct  is not None]
    model_returns = [e.model_return_pct for e in entries if e.model_return_pct is not None]

    if not user_returns or not model_returns:
        return {"user_total_return": None, "model_total_return": None, "alpha": None}

    def compound(returns):
        result = 1.0
        for r in returns:
            result *= (1 + r / 100)
        return round((result - 1) * 100, 2)

    user_total  = compound(user_returns)
    model_total = compound(model_returns)
    alpha       = round(user_total - model_total, 2)

    regime_perf = {}
    for e in entries:
        label = e.regime_label or "Neutral"
        if label not in regime_perf:
            regime_perf[label] = {"user": [], "model": []}
        if e.user_return_pct  is not None: regime_perf[label]["user"].append(e.user_return_pct)
        if e.model_return_pct is not None: regime_perf[label]["model"].append(e.model_return_pct)

    regime_summary = {}
    for label, data in regime_perf.items():
        if data["user"] and data["model"]:
            regime_summary[label] = {
                "user_avg":  round(sum(data["user"])  / len(data["user"]),  2),
                "model_avg": round(sum(data["model"]) / len(data["model"]), 2),
                "count":     len(data["user"]),
            }

    best_regime  = max(regime_summary.items(), key=lambda x: x[1]["user_avg"],  default=(None, {}))
    worst_regime = min(regime_summary.items(), key=lambda x: x[1]["user_avg"],  default=(None, {}))

    curve     = []
    user_cum  = 1.0
    model_cum = 1.0
    for i, e in enumerate(entries):
        user_cum  *= (1 + (e.user_return_pct  or 0) / 100)
        model_cum *= (1 + (e.model_return_pct or 0) / 100)
        curve.append({
            "period":    i + 1,
            "user_cum":  round((user_cum  - 1) * 100, 2),
            "model_cum": round((model_cum - 1) * 100, 2),
            "date":      e.date.strftime("%b %d") if e.date else "",
            "regime":    e.regime_label or "—",
        })

    return {
        "user_total_return":  user_total,
        "model_total_return": model_total,
        "alpha":              alpha,
        "periods":            len(entries),
        "regime_breakdown":   regime_summary,
        "best_regime":        best_regime[0],
        "worst_regime":       worst_regime[0],
        "curve":              curve,
        "message": (
            f"Following ChainPulse would have returned {model_total:+.1f}%. "
            f"Your actual: {user_total:+.1f}%."
        ),
    }


# ─────────────────────────────────────────
# MISTAKE REPLAY ENGINE
# ─────────────────────────────────────────
def compute_mistake_replay(logs: list, db: Session, coin: str) -> list:
    replays = []
    for log in logs:
        hazard     = log.hazard_at_log    or 0
        shift_risk = log.shift_risk_at_log or 0
        user_exp   = log.user_exposure_pct  or 0
        model_exp  = log.model_exposure_pct or 50
        delta      = user_exp - model_exp
        regime     = log.regime_label or "Neutral"

        if (hazard > 55 or shift_risk > 60) and abs(delta) > 12:
            severity  = (
                "high"   if (hazard > 70 or shift_risk > 75) and abs(delta) > 20 else
                "medium" if abs(delta) > 15 else
                "low"
            )
            direction = "over-exposed" if delta > 0 else "under-exposed"
            replays.append({
                "date":      log.created_at.strftime("%b %d, %Y"),
                "regime":    regime,
                "hazard":    hazard,
                "shift_risk": shift_risk,
                "user_exp":  user_exp,
                "model_exp": model_exp,
                "delta":     round(delta, 1),
                "direction": direction,
                "severity":  severity,
                "message": (
                    f"You were {direction} by {abs(round(delta, 1))}% "
                    f"while hazard was {hazard}% in {regime} regime."
                ),
                "signals_at_time": {
                    "hazard":     hazard,
                    "shift_risk": shift_risk,
                    "alignment":  log.alignment_at_log or 0,
                },
            })

    return sorted(replays, key=lambda x: x["severity"] == "high", reverse=True)[:10]


# ─────────────────────────────────────────
# UPDATE ENGINE
# ─────────────────────────────────────────
def update_market(coin: str, timeframe: str, db: Session):
    result = calculate_score_for_timeframe(coin, timeframe)
    if result is None:
        logger.warning(f"Insufficient data for {coin}/{timeframe}")
        return None
    entry = MarketSummary(
        coin           = coin,
        timeframe      = timeframe,
        score          = result["score"],
        label          = classify(result["score"]),
        coherence      = result["coherence"],
        momentum_4h    = result["mom_short"],
        momentum_24h   = result["mom_long"],
        volatility_val = result["volatility"],
    )
    db.add(entry)
    db.commit()
    db.refresh(entry)
    logger.info(f"Updated {coin}/{timeframe}: {entry.label} ({entry.score})")
    return entry


# ─────────────────────────────────────────
# EMAIL HELPERS
# ─────────────────────────────────────────
def send_email(to: str, subject: str, html: str) -> bool:
    if not RESEND_API_KEY:
        logger.warning("RESEND_API_KEY not set — skipping email")
        return False

    try:
        resend.api_key = RESEND_API_KEY
        resend.Emails.send({
            "from": RESEND_FROM_EMAIL,
            "to": to,
            "subject": subject,
            "html": html,
        })
        return True
    except Exception as e:
        logger.error(f"send_email failed for {to}: {e}")
        return False


def welcome_email_html(email: str, access_token: str) -> str:
    url = f"{FRONTEND_URL}/app?token={access_token}"
    return f"""
<div style="font-family:sans-serif;max-width:560px;margin:0 auto;
            background:#000;color:#fff;padding:40px;">
  <div style="font-size:11px;color:#555;text-transform:uppercase;
              letter-spacing:2px;margin-bottom:16px;">ChainPulse Pro</div>
  <h1 style="font-size:24px;margin-bottom:8px;">Your Pro Access Is Active</h1>
  <p style="color:#999;margin-bottom:32px;">
    Click below to open your Pro dashboard. This link logs you in automatically.
    Bookmark it.
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
      <li>Exposure recommendation %</li>
      <li>Shift risk % and hazard rate</li>
      <li>Survival probability and curve</li>
      <li>Decision Engine — Daily Directive</li>
      <li>If You Do Nothing simulator</li>
      <li>Regime stress meter and countdown timer</li>
      <li>Volatility and liquidity environment</li>
      <li>Transition probability matrix</li>
      <li>Portfolio exposure allocator</li>
      <li>Exposure logger and discipline score</li>
      <li>Performance comparison vs model</li>
      <li>Edge profile and mistake replay</li>
      <li>Risk profile calibration</li>
      <li>Full cross-asset correlation monitor</li>
      <li>Real-time shift alerts via email</li>
      <li>Daily morning regime brief</li>
      <li>Weekly discipline summary</li>
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
    </tr>"""

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
    url = (
        f"{FRONTEND_URL}/app?token={access_token}"
        if access_token
        else f"{FRONTEND_URL}/app"
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
        </tr>"""

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
            for h in ["Asset", "Macro", "Execution", "Exposure", "Shift Risk", "Grade", "Mode"]
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


def weekly_discipline_email_html(
    email:        str,
    discipline:   dict,
    access_token: str,
) -> str:
    url         = (
        f"{FRONTEND_URL}/app?token={access_token}"
        if access_token
        else f"{FRONTEND_URL}/app"
    )
    score     = discipline.get("score")
    label     = discipline.get("label",   "—")
    summary   = discipline.get("summary", "")
    followed  = discipline.get("followed", 0)
    total     = discipline.get("total",    0)
    bonuses   = discipline.get("bonuses",  0)
    penalties = discipline.get("penalties", 0)
    flags     = discipline.get("flags",    [])

    score_color = (
        "#34d399" if score and score >= 85 else
        "#4ade80" if score and score >= 70 else
        "#facc15" if score and score >= 50 else
        "#f87171"
    )
    score_display = f"{score}" if score is not None else "N/A"

    flags_html = ""
    for f in flags[-5:]:
        flag_color = "#4ade80" if f["type"] == "bonus" else "#f87171"
        flags_html += f"""
        <tr>
          <td style="padding:8px 0;border-bottom:1px solid #1a1a1a;
                     color:{flag_color};font-size:12px;">{f['label']}</td>
          <td style="padding:8px 0;border-bottom:1px solid #1a1a1a;
                     color:#555;font-size:11px;text-align:right;">
            {f['date']} — {f['regime']}
          </td>
        </tr>"""

    if not flags_html:
        flags_html = """
        <tr>
          <td colspan="2"
              style="padding:8px 0;color:#444;font-size:12px;">
            No discipline events recorded this week.
          </td>
        </tr>"""

    return f"""
<div style="font-family:sans-serif;max-width:560px;margin:0 auto;
            background:#000;color:#fff;padding:40px;">
  <div style="font-size:11px;color:#555;text-transform:uppercase;
              letter-spacing:2px;margin-bottom:16px;">
    ChainPulse Weekly Summary
  </div>
  <h1 style="font-size:22px;margin-bottom:8px;">Your Discipline Report</h1>
  <p style="color:#666;font-size:13px;margin-bottom:32px;">
    Here is how you tracked against the model this week.
  </p>
  <div style="text-align:center;padding:32px;border:1px solid #1f1f1f;
              margin-bottom:32px;">
    <div style="font-size:48px;font-weight:700;color:{score_color};">
      {score_display}
    </div>
    <div style="font-size:14px;color:{score_color};margin-top:8px;">
      {label}
    </div>
    <div style="font-size:12px;color:#555;margin-top:8px;">{summary}</div>
  </div>
  <table style="width:100%;border-collapse:collapse;margin-bottom:24px;">
    <tr>
      <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;
                 color:#555;font-size:12px;">Times Followed Model</td>
      <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;
                 color:#fff;text-align:right;">{followed} / {total}</td>
    </tr>
    <tr>
      <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;
                 color:#555;font-size:12px;">Discipline Bonuses</td>
      <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;
                 color:#4ade80;text-align:right;">+{bonuses}</td>
    </tr>
    <tr>
      <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;
                 color:#555;font-size:12px;">Discipline Penalties</td>
      <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;
                 color:#f87171;text-align:right;">-{penalties}</td>
    </tr>
  </table>
  <div style="border:1px solid #1f1f1f;padding:16px;margin-bottom:24px;">
    <div style="font-size:11px;color:#555;text-transform:uppercase;
                letter-spacing:1px;margin-bottom:12px;">
      Recent Discipline Events
    </div>
    <table style="width:100%;border-collapse:collapse;">
      {flags_html}
    </table>
  </div>
  <a href="{url}"
     style="display:inline-block;background:#fff;color:#000;padding:14px 28px;
            text-decoration:none;font-weight:bold;border-radius:4px;">
    View Full Dashboard
  </a>
  <p style="color:#333;font-size:11px;margin-top:40px;
            border-top:1px solid #111;padding-top:20px;">
    ChainPulse. Not financial advice.
  </p>
</div>
"""


# ═══════════════════════════════════════════
# ROUTES
# ═══════════════════════════════════════════

# ── Health ──────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.datetime.utcnow()}


# ── Pricing Info ─────────────────────────────
@app.get("/pricing-info")
def pricing_info():
    return {
        "monthly": {
            "price":    PRICE_MONTHLY,
            "currency": "USD",
            "label":    "$39 / month",
            "period":   "month",
        },
        "annual": {
            "price":      PRICE_ANNUAL,
            "currency":   "USD",
            "label":      "$348 / year",
            "period":     "year",
            "saving":     (PRICE_MONTHLY * 12) - PRICE_ANNUAL,
            "saving_pct": round(
                ((PRICE_MONTHLY * 12 - PRICE_ANNUAL) / (PRICE_MONTHLY * 12)) * 100, 1
            ),
        },
        "free_tier": {
            "includes": [
                "Macro / Trend / Execution regime labels",
                "Direction (Bullish / Bearish / Mixed)",
                "Alignment %",
                "Basic market breadth",
                "Risk events calendar",
                "Execution score (raw)",
                "Basic heatmap (labels only)",
            ],
            "excludes": [
                "Exposure recommendation %",
                "Shift risk %",
                "Hazard rate",
                "Survival probability",
                "Decision Engine",
                "If You Do Nothing simulator",
                "Stress meter and countdown timer",
                "Portfolio allocator",
                "Exposure logger and discipline score",
                "Performance comparison",
                "Edge profile and mistake replay",
                "Risk profile calibration",
                "Full correlation matrix",
                "Email alerts and briefs",
            ],
        },
        "pro_tier": {
            "includes": [
                "Everything in Free",
                "Exposure recommendation %",
                "Shift risk % and hazard rate",
                "Survival probability and survival curve",
                "Decision Engine — Daily Directive",
                "If You Do Nothing simulator",
                "Regime stress meter and countdown timer",
                "Volatility and liquidity environment",
                "Transition probability matrix",
                "Portfolio exposure allocator",
                "Exposure logger",
                "Discipline score",
                "Performance comparison vs model",
                "Edge profile",
                "Mistake replay",
                "Risk profile calibration",
                "Full cross-asset correlation monitor",
                "Real-time shift alerts via email",
                "Daily morning regime brief",
                "Weekly discipline summary email",
            ],
        },
    }


# ── Update ──────────────────────────────────
@app.get("/update-now")
def update_now(
    coin:      str = "BTC",
    timeframe: str = "1h",
    secret:    str = "",
    db:        Session = Depends(get_db),
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
    coin:    str = "BTC",
    db:      Session = Depends(get_db),
):
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")

    is_pro = resolve_pro_status(get_auth_header(request), db)
    stack  = build_regime_stack(coin, db)

    if stack["incomplete"]:
        return {**stack, "pro_required": False}

    base = {
        "coin":      stack["coin"],
        "macro":     {"label": stack["macro"]["label"]}     if stack["macro"]     else None,
        "trend":     {"label": stack["trend"]["label"]}     if stack["trend"]     else None,
        "execution": {"label": stack["execution"]["label"]} if stack["execution"] else None,
        "alignment": stack["alignment"],
        "direction": stack["direction"],
    }

    if not is_pro:
        return {
            **base,
            "pro_required":              True,
            "exposure":                  None,
            "shift_risk":                None,
            "survival":                  None,
            "hazard":                    None,
            "trend_maturity":            None,
            "percentile":                None,
            "macro_coherence":           None,
            "trend_coherence":           None,
            "exec_coherence":            None,
            "regime_age_hours":          None,
            "avg_regime_duration_hours": None,
            "regime_quality":            None,
        }

    age_1h   = current_age(db, coin, "1h")
    avg_dur  = average_regime_duration(db, coin, "1h")
    maturity = trend_maturity_score(age_1h, avg_dur, stack["hazard"])
    pct_rank = percentile_rank(db, coin, stack["execution"]["score"], "1h")
    quality  = compute_regime_quality(stack)

    return {
        **base,
        "macro":                     stack["macro"],
        "trend":                     stack["trend"],
        "execution":                 stack["execution"],
        "pro_required":              False,
        "exposure":                  stack["exposure"],
        "shift_risk":                stack["shift_risk"],
        "survival":                  stack["survival"],
        "hazard":                    stack["hazard"],
        "trend_maturity":            maturity,
        "percentile":                pct_rank,
        "macro_coherence":           stack["macro"]["coherence"],
        "trend_coherence":           stack["trend"]["coherence"],
        "exec_coherence":            stack["execution"]["coherence"],
        "regime_age_hours":          round(age_1h, 2),
        "avg_regime_duration_hours": round(avg_dur, 2),
        "regime_quality":            quality,
    }


# ── Market Overview ──────────────────────────
@app.get("/market-overview")
def market_overview(
    request: Request,
    coin:    str = "ALL",
    db:      Session = Depends(get_db),
):
    is_pro  = resolve_pro_status(get_auth_header(request), db)
    result  = []
    breadth = compute_market_breadth(db)

    coins_to_scan = (
        SUPPORTED_COINS
        if coin == "ALL"
        else [coin] if coin in SUPPORTED_COINS
        else SUPPORTED_COINS
    )

    for c in coins_to_scan:
        stack = build_regime_stack(c, db)
        if stack["incomplete"]:
            continue

        row = {
            "coin":      stack["coin"],
            "macro":     stack["macro"]["label"]     if stack["macro"]     else None,
            "trend":     stack["trend"]["label"]     if stack["trend"]     else None,
            "execution": stack["execution"]["label"] if stack["execution"] else None,
            "alignment": stack["alignment"],
            "direction": stack["direction"],
        }

        if is_pro:
            row["exposure"]   = stack["exposure"]
            row["shift_risk"] = stack["shift_risk"]
        else:
            row["exposure"]     = None
            row["shift_risk"]   = None
            row["pro_required"] = True

        result.append(row)

    return {"data": result, "breadth": breadth}


# ── Latest ──────────────────────────────────
@app.get("/latest")
def latest(coin: str = "BTC", db: Session = Depends(get_db)):
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")
    r = (
        db.query(MarketSummary)
        .filter(
            MarketSummary.coin      == coin,
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


# ── Statistics (landing page — free) ─────────
@app.get("/statistics")
def statistics(
    coin: str = "BTC",
    db:   Session = Depends(get_db),
):
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")

    record = (
        db.query(MarketSummary)
        .filter(
            MarketSummary.coin      == coin,
            MarketSummary.timeframe == "1h",
        )
        .order_by(MarketSummary.created_at.desc())
        .first()
    )
    if not record:
        return {"message": "No data yet"}

    return {
        "coin":      coin,
        "label":     record.label,
        "score":     record.score,
        "coherence": record.coherence,
        "timestamp": record.created_at,
    }


# ── Regime History ──────────────────────────
@app.get("/regime-history")
def regime_history(
    coin:      str = "BTC",
    timeframe: str = "1h",
    limit:     int = 48,
    db:        Session = Depends(get_db),
):
    if timeframe not in SUPPORTED_TIMEFRAMES:
        raise HTTPException(status_code=400, detail="Unsupported timeframe")
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")

    limit   = min(max(1, limit), 500)
    records = (
        db.query(MarketSummary)
        .filter(
            MarketSummary.coin      == coin,
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


# ── Survival Curve (PRO) ─────────────────────
@app.get("/survival-curve")
def survival_curve(
    request:   Request,
    coin:      str = "BTC",
    timeframe: str = "1h",
    db:        Session = Depends(get_db),
):
    if not resolve_pro_status(get_auth_header(request), db):
        raise HTTPException(status_code=403, detail="Pro subscription required.")

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
        hz        = 0.0
        if hour > 0 and survivors:
            exited = [d for d in durations if hour - 1 < d <= hour]
            hz     = (len(exited) / len(survivors)) * 100
        curve.append({
            "hour":     hour,
            "survival": round(surv_pct, 2),
            "hazard":   round(hz, 2),
        })
    return {"data": curve, "source": "historical"}


# ── Regime Transitions (PRO) ─────────────────
@app.get("/regime-transitions")
def regime_transitions(
    request:   Request,
    coin:      str = "BTC",
    timeframe: str = "1h",
    db:        Session = Depends(get_db),
):
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")
    if not resolve_pro_status(get_auth_header(request), db):
        raise HTTPException(status_code=403, detail="Pro subscription required.")

    result = regime_transition_matrix(db, coin, timeframe)
    if result is None:
        return {
            "current_state":   "Insufficient data",
            "transitions":     {},
            "data_sufficient": False,
        }
    return result


# ── Volatility Environment (PRO) ─────────────
@app.get("/volatility-environment")
def volatility_env(
    request: Request,
    coin:    str = "BTC",
    db:      Session = Depends(get_db),
):
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")
    if not resolve_pro_status(get_auth_header(request), db):
        raise HTTPException(status_code=403, detail="Pro subscription required.")

    result = volatility_environment(coin, db)
    if result is None:
        return {"error": "Insufficient data"}
    return result


# ── Correlation Matrix (PRO) ─────────────────
@app.get("/correlation")
@app.get("/correlation-matrix")
def correlation_endpoint(
    request: Request,
    coins:   str = "BTC,ETH,SOL",
    db:      Session = Depends(get_db),
):
    if not resolve_pro_status(get_auth_header(request), db):
        raise HTTPException(status_code=403, detail="Pro subscription required.")

    coin_list = [c.strip().upper() for c in coins.split(",") if c.strip()]
    return build_correlation_matrix(coin_list)


# ── Regime Confidence (PRO) ──────────────────
@app.get("/regime-confidence")
def regime_confidence_endpoint(
    request: Request,
    coin:    str = "BTC",
    db:      Session = Depends(get_db),
):
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")
    if not resolve_pro_status(get_auth_header(request), db):
        raise HTTPException(status_code=403, detail="Pro subscription required.")

    stack   = build_regime_stack(coin, db)
    breadth = compute_market_breadth(db)
    if stack["incomplete"]:
        return {"error": "Insufficient regime data"}

    survival_val  = stack.get("survival") or 50.0
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


# ── Regime Quality (PRO) ─────────────────────
@app.get("/regime-quality")
def regime_quality_endpoint(
    request: Request,
    coin:    str = "BTC",
    db:      Session = Depends(get_db),
):
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")
    if not resolve_pro_status(get_auth_header(request), db):
        raise HTTPException(status_code=403, detail="Pro subscription required.")

    stack = build_regime_stack(coin, db)
    if stack["incomplete"]:
        return {"error": "Insufficient data"}

    quality = compute_regime_quality(stack)
    return {
        **quality,
        "coin":       coin,
        "regime":     stack["execution"]["label"] if stack.get("execution") else "Neutral",
        "exposure":   stack.get("exposure"),
        "shift_risk": stack.get("shift_risk"),
        "hazard":     stack.get("hazard"),
        "survival":   stack.get("survival"),
    }


# ── Playbook (free preview / PRO full) ───────
@app.get("/playbook")
def playbook(
    request: Request,
    coin:    str = "BTC",
    db:      Session = Depends(get_db),
):
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")

    is_pro = resolve_pro_status(get_auth_header(request), db)
    stack  = build_regime_stack(coin, db)
    if stack["incomplete"]:
        return {"error": "Insufficient data"}

    exec_label = stack["execution"]["label"] if stack.get("execution") else "Neutral"
    pb         = PLAYBOOK_DATA.get(exec_label, PLAYBOOK_DATA["Neutral"])

    if not is_pro:
        return {
            "coin":          coin,
            "regime":        exec_label,
            "strategy_mode": pb["strategy_mode"],
            "exposure_band": pb["exposure_band"],
            "pro_required":  True,
        }

    return {
        "coin":               coin,
        "regime":             exec_label,
        "strategy_mode":      pb["strategy_mode"],
        "exposure_band":      pb["exposure_band"],
        "trend_follow_wr":    pb["trend_follow_wr"],
        "mean_revert_wr":     pb["mean_revert_wr"],
        "avg_remaining_days": pb["avg_remaining_days"],
        "actions":            pb["actions"],
        "avoid":              pb["avoid"],
        "pro_required":       False,
    }


# ── Portfolio Allocator (PRO) ────────────────
@app.post("/portfolio-allocator")
def portfolio_allocator_endpoint(
    request:       Request,
    account_size:  float  = 10000,
    strategy_mode: str    = "balanced",
    coin:          str    = "BTC",
    db:            Session = Depends(get_db),
):
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")
    if strategy_mode not in ("conservative", "balanced", "aggressive"):
        raise HTTPException(status_code=400, detail="Invalid strategy mode")
    if account_size <= 0:
        raise HTTPException(status_code=400, detail="Invalid account size")
    if not resolve_pro_status(get_auth_header(request), db):
        raise HTTPException(status_code=403, detail="Pro subscription required.")

    stack = build_regime_stack(coin, db)
    if stack["incomplete"]:
        return {"error": "Insufficient data"}

    breadth     = compute_market_breadth(db)
    survival_v  = stack.get("survival") or 50.0
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


# ── Risk Events (FREE) ───────────────────────
@app.get("/risk-events")
def risk_events():
    return {"events": RISK_EVENTS}


# ── Decision Engine (PRO) ────────────────────
@app.get("/decision-engine")
def decision_engine_endpoint(
    request: Request,
    coin:    str = "BTC",
    db:      Session = Depends(get_db),
):
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")
    if not resolve_pro_status(get_auth_header(request), db):
        raise HTTPException(status_code=403, detail="Pro subscription required.")

    stack   = build_regime_stack(coin, db)
    breadth = compute_market_breadth(db)
    if stack["incomplete"]:
        return {"error": "Insufficient data"}

    hazard     = stack.get("hazard")     or 0
    shift_risk = stack.get("shift_risk") or 0
    alignment  = stack.get("alignment")  or 0
    survival   = stack.get("survival")   or 50
    age_1h     = current_age(db, coin, "1h")
    avg_dur    = average_regime_duration(db, coin, "1h")
    maturity   = trend_maturity_score(age_1h, avg_dur, hazard)

    decision = compute_decision_score(
        hazard        = hazard,
        shift_risk    = shift_risk,
        alignment     = alignment,
        survival      = survival,
        breadth_score = breadth.get("breadth_score", 0),
        maturity_pct  = maturity,
    )
    exec_label           = stack["execution"]["label"] if stack.get("execution") else "Neutral"
    decision["regime"]   = exec_label
    decision["exposure"] = stack.get("exposure", 50)
    decision["coin"]     = coin
    return decision


# ── If You Do Nothing (PRO) ──────────────────
@app.post("/if-nothing-panel")
def if_nothing_panel_endpoint(
    request:       Request,
    coin:          str   = "BTC",
    user_exposure: float = 50.0,
    db:            Session = Depends(get_db),
):
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")
    if not resolve_pro_status(get_auth_header(request), db):
        raise HTTPException(status_code=403, detail="Pro subscription required.")

    stack = build_regime_stack(coin, db)
    if stack["incomplete"]:
        return {"error": "Insufficient data"}

    exec_label     = stack["execution"]["label"] if stack.get("execution") else "Neutral"
    model_exposure = stack.get("exposure") or 50

    return compute_if_nothing_panel(
        user_exposure  = user_exposure,
        model_exposure = model_exposure,
        hazard         = stack.get("hazard")     or 0,
        shift_risk     = stack.get("shift_risk") or 0,
        regime_label   = exec_label,
    )


# ── User Profile (PRO) ───────────────────────
@app.post("/user-profile")
def save_user_profile(
    request: Request,
    body:    UserProfileRequest,
    db:      Session = Depends(get_db),
):
    if not resolve_pro_status(get_auth_header(request), db):
        raise HTTPException(status_code=403, detail="Pro subscription required.")

    email     = body.email.strip().lower()
    mult_map  = {"conservative": 0.70, "balanced": 1.00, "aggressive": 1.25}
    risk_mult = mult_map.get(body.risk_identity, 1.0)

    user    = db.query(User).filter(User.email == email).first()
    user_id = user.id if user else None

    profile = db.query(UserProfile).filter(UserProfile.email == email).first()
    if not profile:
        profile = UserProfile(email=email, user_id=user_id)
        db.add(profile)

    profile.user_id             = user_id
    profile.max_drawdown_pct    = body.max_drawdown_pct
    profile.typical_leverage    = body.typical_leverage
    profile.holding_period_days = body.holding_period_days
    profile.risk_identity       = body.risk_identity
    profile.risk_multiplier     = risk_mult
    profile.updated_at          = datetime.datetime.utcnow()
    db.commit()

    return {
        "status":          "saved",
        "email":           email,
        "risk_multiplier": risk_mult,
        "profile": {
            "max_drawdown_pct":    profile.max_drawdown_pct,
            "typical_leverage":    profile.typical_leverage,
            "holding_period_days": profile.holding_period_days,
            "risk_identity":       profile.risk_identity,
        },
    }


@app.get("/user-profile")
def get_user_profile(
    request: Request,
    email:   str,
    coin:    str = "BTC",
    db:      Session = Depends(get_db),
):
    if not resolve_pro_status(get_auth_header(request), db):
        raise HTTPException(status_code=403, detail="Pro subscription required.")

    email   = email.strip().lower()
    profile = db.query(UserProfile).filter(UserProfile.email == email).first()

    if not profile:
        return {
            "exists":  False,
            "message": "No profile found. Complete onboarding to personalise.",
        }

    stack                 = build_regime_stack(coin, db)
    personalised_exposure = None
    if not stack["incomplete"] and stack.get("exposure"):
        personalised_exposure = round(
            min(95, max(5, stack["exposure"] * profile.risk_multiplier)), 1
        )

    return {
        "exists":                True,
        "email":                 email,
        "risk_identity":         profile.risk_identity,
        "risk_multiplier":       profile.risk_multiplier,
        "max_drawdown_pct":      profile.max_drawdown_pct,
        "typical_leverage":      profile.typical_leverage,
        "holding_period_days":   profile.holding_period_days,
        "personalised_exposure": personalised_exposure,
        "model_exposure":        stack.get("exposure") if not stack.get("incomplete") else None,
        "created_at":            profile.created_at,
    }


# ── Exposure Logger (PRO) ────────────────────
@app.post("/log-exposure")
def log_exposure(
    request: Request,
    body:    ExposureLogRequest,
    db:      Session = Depends(get_db),
):
    if not resolve_pro_status(get_auth_header(request), db):
        raise HTTPException(status_code=403, detail="Pro subscription required.")

    email = body.email.strip().lower()
    if body.coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")

    stack = build_regime_stack(body.coin, db)
    if stack["incomplete"]:
        raise HTTPException(status_code=400, detail="No regime data yet")

    model_exp  = stack.get("exposure")   or 50
    hazard     = stack.get("hazard")     or 0
    shift_risk = stack.get("shift_risk") or 0
    alignment  = stack.get("alignment")  or 0
    exec_label = stack["execution"]["label"] if stack.get("execution") else "Neutral"
    delta      = body.user_exposure_pct - model_exp
    followed   = abs(delta) <= 10

    current_price = 0.0
    try:
        prices, _ = get_klines(body.coin, "1h", limit=2)
        if prices:
            current_price = prices[-1]
    except Exception:
        pass

    log = ExposureLog(
        email              = email,
        coin               = body.coin,
        user_exposure_pct  = body.user_exposure_pct,
        model_exposure_pct = model_exp,
        regime_label       = exec_label,
        hazard_at_log      = hazard,
        shift_risk_at_log  = shift_risk,
        alignment_at_log   = alignment,
        followed_model     = followed,
        price_at_log       = current_price,
    )
    db.add(log)
    db.commit()

    if abs(delta) > 20:
        feedback = "⚠ Large deviation from model recommendation"
        severity = "warning"
    elif abs(delta) > 10:
        feedback = "Moderate deviation — within acceptable range"
        severity = "caution"
    else:
        feedback = "✓ Aligned with model recommendation"
        severity = "ok"

    return {
        "status":          "logged",
        "user_exposure":   body.user_exposure_pct,
        "model_exposure":  model_exp,
        "delta":           round(delta, 1),
        "followed_model":  followed,
        "feedback":        feedback,
        "severity":        severity,
        "regime":          exec_label,
        "price_at_log":    current_price,
    }


# ── Discipline Score (PRO) ───────────────────
@app.get("/discipline-score")
def discipline_score_endpoint(
    request: Request,
    email:   str,
    db:      Session = Depends(get_db),
):
    if not resolve_pro_status(get_auth_header(request), db):
        raise HTTPException(status_code=403, detail="Pro subscription required.")

    email = email.strip().lower()
    logs  = (
        db.query(ExposureLog)
        .filter(ExposureLog.email == email)
        .order_by(ExposureLog.created_at.desc())
        .limit(30)
        .all()
    )
    result          = compute_discipline_score(logs)
    result["email"] = email
    return result


# ── Performance Comparison (PRO) ─────────────
@app.get("/performance-comparison")
def performance_comparison_endpoint(
    request: Request,
    email:   str,
    coin:    str = "BTC",
    limit:   int = 30,
    db:      Session = Depends(get_db),
):
    if not resolve_pro_status(get_auth_header(request), db):
        raise HTTPException(status_code=403, detail="Pro subscription required.")

    email   = email.strip().lower()
    entries = (
        db.query(PerformanceEntry)
        .filter(
            PerformanceEntry.email == email,
            PerformanceEntry.coin  == coin,
        )
        .order_by(PerformanceEntry.date.asc())
        .limit(limit)
        .all()
    )
    result          = compute_performance_comparison(entries)
    result["email"] = email
    result["coin"]  = coin
    return result


# ── Log Performance (PRO) ────────────────────
@app.post("/log-performance")
def log_performance(
    request: Request,
    body:    PerformanceEntryRequest,
    db:      Session = Depends(get_db),
):
    if not resolve_pro_status(get_auth_header(request), db):
        raise HTTPException(status_code=403, detail="Pro subscription required.")

    email = body.email.strip().lower()
    if body.coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")
    if body.price_open <= 0 or body.price_close <= 0:
        raise HTTPException(status_code=400, detail="Invalid prices")

    stack      = build_regime_stack(body.coin, db)
    model_exp  = stack.get("exposure") or 50
    exec_label = stack["execution"]["label"] if (
        not stack["incomplete"] and stack.get("execution")
    ) else "Neutral"

    price_return = ((body.price_close - body.price_open) / body.price_open) * 100
    user_return  = round(price_return * (body.user_exposure_pct / 100), 2)
    model_return = round(price_return * (model_exp / 100), 2)

    flags   = []
    delta   = body.user_exposure_pct - model_exp
    hazard  = stack.get("hazard")     or 0
    shift_r = stack.get("shift_risk") or 0

    if hazard > 65 and delta > 10:
        flags.append("over_exposed_high_hazard")
    if "Risk-Off" in exec_label and delta > 15:
        flags.append("over_exposed_risk_off")
    if shift_r > 70 and delta < -5:
        flags.append("reduced_on_hazard_spike")
    if abs(delta) <= 10:
        flags.append("followed_model")

    entry = PerformanceEntry(
        email              = email,
        coin               = body.coin,
        date               = datetime.datetime.utcnow(),
        user_exposure_pct  = body.user_exposure_pct,
        model_exposure_pct = model_exp,
        price_open         = body.price_open,
        price_close        = body.price_close,
        user_return_pct    = user_return,
        model_return_pct   = model_return,
        regime_label       = exec_label,
        discipline_flags   = json.dumps(flags),
    )
    db.add(entry)
    db.commit()

    return {
        "status":           "logged",
        "price_return":     round(price_return, 2),
        "user_return":      user_return,
        "model_return":     model_return,
        "alpha":            round(user_return - model_return, 2),
        "regime":           exec_label,
        "discipline_flags": flags,
    }


# ── Mistake Replay (PRO) ─────────────────────
@app.get("/mistake-replay")
def mistake_replay_endpoint(
    request: Request,
    email:   str,
    coin:    str = "BTC",
    db:      Session = Depends(get_db),
):
    if not resolve_pro_status(get_auth_header(request), db):
        raise HTTPException(status_code=403, detail="Pro subscription required.")

    email = email.strip().lower()
    logs  = (
        db.query(ExposureLog)
        .filter(
            ExposureLog.email == email,
            ExposureLog.coin  == coin,
        )
        .order_by(ExposureLog.created_at.desc())
        .limit(50)
        .all()
    )
    replays = compute_mistake_replay(logs, db, coin)
    return {
        "email":   email,
        "coin":    coin,
        "replays": replays,
        "count":   len(replays),
    }


# ── Edge Profile (PRO) ───────────────────────
@app.get("/edge-profile")
def edge_profile_endpoint(
    request: Request,
    email:   str,
    db:      Session = Depends(get_db),
):
    if not resolve_pro_status(get_auth_header(request), db):
        raise HTTPException(status_code=403, detail="Pro subscription required.")

    email   = email.strip().lower()
    entries = (
        db.query(PerformanceEntry)
        .filter(PerformanceEntry.email == email)
        .order_by(PerformanceEntry.date.asc())
        .all()
    )

    if len(entries) < 5:
        return {
            "email":       email,
            "ready":       False,
            "message":     f"Need {5 - len(entries)} more entries to build your edge profile.",
            "entry_count": len(entries),
        }

    regime_data = {}
    for e in entries:
        label = e.regime_label or "Neutral"
        if label not in regime_data:
            regime_data[label] = []
        if e.user_return_pct is not None:
            regime_data[label].append(e.user_return_pct)

    profile = {}
    for regime, rets in regime_data.items():
        if rets:
            avg  = round(sum(rets) / len(rets), 2)
            wins = sum(1 for r in rets if r > 0)
            profile[regime] = {
                "avg_return":  avg,
                "win_rate":    round((wins / len(rets)) * 100, 1),
                "count":       len(rets),
                "performance": (
                    "Strong" if avg > 2   else
                    "Good"   if avg > 0.5 else
                    "Weak"   if avg > -1  else
                    "Poor"
                ),
            }

    if not profile:
        return {"email": email, "ready": False, "message": "No return data."}

    best_regime  = max(profile.items(), key=lambda x: x[1]["avg_return"])
    worst_regime = min(profile.items(), key=lambda x: x[1]["avg_return"])

    recommendations = []
    for regime, data in profile.items():
        if data["performance"] in ("Weak", "Poor"):
            recommendations.append(
                f"Reduce exposure faster in {regime} conditions "
                f"(avg {data['avg_return']:+.1f}%)"
            )
        elif data["performance"] == "Strong":
            recommendations.append(
                f"You have edge in {regime} — stay disciplined here "
                f"(avg {data['avg_return']:+.1f}%)"
            )

    return {
        "email":           email,
        "ready":           True,
        "entry_count":     len(entries),
        "best_regime":     best_regime[0],
        "worst_regime":    worst_regime[0],
        "profile":         profile,
        "recommendations": recommendations,
    }


# ── Full Accountability (PRO) ────────────────
@app.get("/full-accountability")
def full_accountability(
    request: Request,
    email:   str,
    coin:    str = "BTC",
    db:      Session = Depends(get_db),
):
    if not resolve_pro_status(get_auth_header(request), db):
        raise HTTPException(status_code=403, detail="Pro subscription required.")

    email = email.strip().lower()

    logs = (
        db.query(ExposureLog)
        .filter(ExposureLog.email == email)
        .order_by(ExposureLog.created_at.desc())
        .limit(50)
        .all()
    )
    entries = (
        db.query(PerformanceEntry)
        .filter(
            PerformanceEntry.email == email,
            PerformanceEntry.coin  == coin,
        )
        .order_by(PerformanceEntry.date.asc())
        .limit(30)
        .all()
    )
    user_profile = db.query(UserProfile).filter(UserProfile.email == email).first()

    discipline  = compute_discipline_score(logs)
    performance = compute_performance_comparison(entries)
    replays     = compute_mistake_replay(logs, db, coin)

    edge = None
    if len(entries) >= 5:
        regime_data = {}
        for e in entries:
            label = e.regime_label or "Neutral"
            if label not in regime_data:
                regime_data[label] = []
            if e.user_return_pct is not None:
                regime_data[label].append(e.user_return_pct)
        edge = {
            regime: {
                "avg_return": round(sum(r) / len(r), 2),
                "win_rate":   round(sum(1 for x in r if x > 0) / len(r) * 100, 1),
                "count":      len(r),
            }
            for regime, r in regime_data.items() if r
        }

    return {
        "email":       email,
        "coin":        coin,
        "discipline":  discipline,
        "performance": performance,
        "replays":     replays,
        "edge":        edge,
        "profile": {
            "risk_identity":       user_profile.risk_identity       if user_profile else None,
            "risk_multiplier":     user_profile.risk_multiplier     if user_profile else None,
            "max_drawdown_pct":    user_profile.max_drawdown_pct    if user_profile else None,
            "holding_period_days": user_profile.holding_period_days if user_profile else None,
        } if user_profile else None,
        "has_profile": user_profile is not None,
    }


# ── Stripe Webhook ───────────────────────────
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
        user        = db.query(User).filter(
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
            padding:14px 28px;margin-top:24px;text-decoration:none;
            font-weight:bold;border-radius:4px;">
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


# ── Checkout Session ─────────────────────────
@app.post("/create-checkout-session")
def create_checkout_session(
    body: CheckoutRequest,
    db:   Session = Depends(get_db),
):
    if not STRIPE_SECRET_KEY:
        raise HTTPException(status_code=500, detail="Stripe not configured")
    if not STRIPE_PRICE_ID:
        raise HTTPException(status_code=500, detail="Stripe price ID not configured")

    try:
        customer_email       = body.email.strip().lower() if body.email else None
        existing_customer_id = None

        if customer_email:
            user = db.query(User).filter(User.email == customer_email).first()
            if user and user.stripe_customer_id:
                existing_customer_id = user.stripe_customer_id

        # support both billing_cycle string and legacy annual boolean
        is_annual = body.billing_cycle == "annual" or body.annual

        price_id = (
            STRIPE_PRICE_ID_ANNUAL
            if is_annual and STRIPE_PRICE_ID_ANNUAL
            else STRIPE_PRICE_ID
        )

        session_params = {
            "payment_method_types":  ["card"],
            "line_items":            [{"price": price_id, "quantity": 1}],
            "mode":                  "subscription",
            "success_url":           f"{FRONTEND_URL}/app?success=true",
            "cancel_url":            f"{FRONTEND_URL}/pricing?cancelled=true",
            "allow_promotion_codes": True,
            "subscription_data":     {"trial_period_days": 7},
        }

        if existing_customer_id:
            session_params["customer"] = existing_customer_id
        elif customer_email:
            session_params["customer_email"] = customer_email

        session = stripe.checkout.Session.create(**session_params)
        return {"url": session.url, "session_id": session.id}

    except stripe.error.StripeError as e:
        logger.error(f"Stripe error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Checkout error: {e}")
        raise HTTPException(status_code=500, detail="Checkout creation failed")


@app.post("/subscribe")
def subscribe(body: SubscribeRequest, db: Session = Depends(get_db)):
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

    confirmation_link = f"{BACKEND_URL}/confirm?email={email}"

    # FIX: use your actual verified sender domain
    html = f"""
    <div style="background:#000;padding:40px 0;font-family:-apple-system,sans-serif;">
      <div style="max-width:600px;margin:0 auto;background:#0b0b0f;border:1px solid rgba(255,255,255,0.08);border-radius:24px;padding:40px;color:#fff;">
        <div style="font-size:12px;letter-spacing:2px;text-transform:uppercase;color:#6b7280;">
          ChainPulse Quant
        </div>
        <h1 style="margin:16px 0 8px;font-size:26px;">Confirm Your Subscription</h1>
        <p style="color:#9ca3af;font-size:15px;line-height:1.6;">
          You're one click away from receiving your Daily Regime Brief.
        </p>
        <div style="margin:30px 0;">
          <a href="{confirmation_link}"
             style="background:#fff;color:#000;padding:14px 28px;border-radius:14px;text-decoration:none;font-weight:600;display:inline-block;">
            Confirm Subscription
          </a>
        </div>
      </div>
    </div>
    """

    try:
        # Use your send_email helper which uses resend properly
        send_email(
            email,
            "Confirm your Daily Regime Brief",
            html,
        )
        logger.info(f"Confirmation email sent to {email}")
    except Exception as e:
        logger.error(f"Failed to send confirmation email to {email}: {e}")
        # Still return success so user is registered even if email fails
        return {"status": "registered", "email_sent": False}

    return {"status": "confirmation_sent", "email_sent": True}


@app.get("/confirm")
def confirm(email: str, db: Session = Depends(get_db)):
    email = email.strip().lower()
    user = db.query(User).filter(User.email == email).first()

    if not user:
        raise HTTPException(status_code=404, detail="Email not found")

    user.alerts_enabled = True
    db.commit()

    return HTMLResponse(content=f"""
    <html>
    <head>
        <title>Subscription Confirmed</title>
        <style>
            body {{
                background-color: #000;
                color: #fff;
                font-family: -apple-system, BlinkMacSystemFont, sans-serif;
                display: flex;
                justify-content: center;
                align-items: center;
                height: 100vh;
                margin: 0;
            }}
            .card {{
                background: rgba(255,255,255,0.05);
                border: 1px solid rgba(255,255,255,0.1);
                padding: 50px;
                border-radius: 24px;
                text-align: center;
                backdrop-filter: blur(12px);
                box-shadow: 0 20px 60px rgba(0,0,0,0.6);
            }}
            .btn {{
                display: inline-block;
                margin-top: 25px;
                padding: 14px 28px;
                background: white;
                color: black;
                border-radius: 14px;
                text-decoration: none;
                font-weight: 600;
                transition: 0.2s ease;
            }}
            .btn:hover {{
                transform: translateY(-2px);
            }}
        </style>
    </head>
    <body>
        <div class="card">
            <h1>✅ Subscription Confirmed</h1>
            <p>Your Daily Regime Brief is now active.</p>
            <a href="https://chainpulse.pro/app" class="btn">
                Go to Dashboard
            </a>
        </div>
    </body>
    </html>
    """)


# ── Alert Dispatch (PRO — internal cron) ─────
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


# ── Morning Email (PRO — internal cron) ──────
@app.get("/send-morning-email")
def send_morning_email(secret: str = "", db: Session = Depends(get_db)):
    if secret != UPDATE_SECRET:
        raise HTTPException(status_code=403, detail="Unauthorized")

    subscribers = db.query(User).filter(
        User.alerts_enabled == True
    ).all()

    stacks = []
    for coin in SUPPORTED_COINS:
        stack = build_regime_stack(coin, db)
        if not stack["incomplete"]:
            stacks.append(stack)

    sent = 0
    for user in subscribers:
        send_email(
            user.email,
            "ChainPulse Morning Regime Brief",
            morning_email_html(stacks, user.access_token or ""),
        )
        sent += 1

    return {"status": "sent", "count": sent}


# ── Weekly Discipline Email (PRO — internal cron) ──
@app.get("/send-weekly-discipline")
def send_weekly_discipline(secret: str = "", db: Session = Depends(get_db)):
    """
    Sends weekly discipline summary to all active Pro users.
    Designed to be called by a cron job every Monday morning.
    """
    if secret != UPDATE_SECRET:
        raise HTTPException(status_code=403, detail="Unauthorized")

    pro_users = db.query(User).filter(
        User.subscription_status == "active",
        User.alerts_enabled      == True,
    ).all()

    sent   = 0
    errors = 0

    for user in pro_users:
        try:
            cutoff = datetime.datetime.utcnow() - datetime.timedelta(days=7)
            logs   = (
                db.query(ExposureLog)
                .filter(
                    ExposureLog.email      == user.email,
                    ExposureLog.created_at >= cutoff,
                )
                .order_by(ExposureLog.created_at.desc())
                .all()
            )

            discipline = compute_discipline_score(logs)

            if discipline.get("total", 0) == 0:
                continue

            send_email(
                user.email,
                "ChainPulse — Your Weekly Discipline Summary",
                weekly_discipline_email_html(
                    email        = user.email,
                    discipline   = discipline,
                    access_token = user.access_token or "",
                ),
            )
            sent += 1

        except Exception as e:
            logger.error(
                f"Weekly discipline email failed for {user.email}: {e}"
            )
            errors += 1

    return {"status": "complete", "sent": sent, "errors": errors}


# ── User Status ──────────────────────────────
@app.get("/user-status")
def user_status(
    request: Request,
    db:      Session = Depends(get_db),
):
    is_pro = resolve_pro_status(get_auth_header(request), db)
    return {
        "is_pro":    is_pro,
        "timestamp": datetime.datetime.utcnow(),
    }


# ── Debug Prices ─────────────────────────────
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


# ── Debug Stack ──────────────────────────────
@app.get("/debug-stack")
def debug_stack(coin: str = "BTC", db: Session = Depends(get_db)):
    """Shows full raw stack — for internal debugging only."""
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


# ── Sample Report ────────────────────────────
@app.get("/sample-report")
def sample_report():
    path = "sample_report.pdf"
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Report not found")
    return FileResponse(path, media_type="application/pdf")

class RestoreRequest(BaseModel):
    email: str

# ── Restore Access ────────────────────────────
@app.post("/restore-access")
def restore_access(body: RestoreRequest, db: Session = Depends(get_db)):
    email = body.email.strip().lower()
    user = db.query(User).filter(User.email == email).first()
    if not user or user.subscription_status != "active":
        raise HTTPException(status_code=404, detail="No active Pro subscription found")

    user.access_token = str(uuid.uuid4())  # rotate token
    db.commit()

    send_email(
        email,
        "ChainPulse Pro — Your Login Link",
        welcome_email_html(email, user.access_token),
    )
    return {"status": "sent"}
@app.get("/ticker")
def ticker():
    symbols = [f"{c}USDT" for c in SUPPORTED_COINS]
    try:
        r = requests.get(
            "https://api.binance.com/api/v3/ticker/24hr",
            params={"symbols": json.dumps(symbols)},
            timeout=10,
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.error(f"Ticker fetch failed: {e}")
        return []