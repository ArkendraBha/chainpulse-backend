# ─────────────────────────────────────────
# main.py — ChainPulse API v5.0 (Refactored)
# ─────────────────────────────────────────
from fastapi import FastAPI, Request, HTTPException, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean, Index, text
from sqlalchemy.orm import sessionmaker, declarative_base, Session
from pydantic import BaseModel, EmailStr  # CHANGED: Added EmailStr for email validation
from dotenv import load_dotenv
from typing import Optional
import os
import time
import uuid
import datetime
import requests
import math
import stripe
import httpx
import asyncio
import logging
import json
import resend
import threading
from collections import defaultdict
from functools import wraps
from logging_config import setup_logging
setup_logging()
from cache import cache_get, cache_set

MODEL_VERSION = "5.0.0"  # CHANGED: bumped version

# ─────────────────────────────────────────
# SETUP
# ─────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("chainpulse")

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./chainpulse.db")
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET")
RESEND_API_KEY = os.getenv("RESEND_API_KEY")
UPDATE_SECRET = os.getenv("UPDATE_SECRET", "changeme")
FRONTEND_URL = os.getenv("FRONTEND_URL", "https://chainpulse.pro")
BACKEND_URL = os.getenv("BACKEND_URL", "https://chainpulse-backend-2xok.onrender.com")
RESEND_FROM_EMAIL = (os.getenv("RESEND_FROM_EMAIL") or "onboarding@resend.dev").strip()
# ─────────────────────────────────────────
# STRIPE PRICE IDS
# ─────────────────────────────────────────
STRIPE_PRICE_MAP = {
    "essential": {
        "monthly": os.getenv("STRIPE_PRICE_ESSENTIAL_MONTHLY", ""),
        "annual": os.getenv("STRIPE_PRICE_ESSENTIAL_ANNUAL", ""),
    },
    "pro": {
        "monthly": os.getenv("STRIPE_PRICE_PRO_MONTHLY", ""),
        "annual": os.getenv("STRIPE_PRICE_PRO_ANNUAL", ""),
    },
    "institutional": {
        "monthly": os.getenv("STRIPE_PRICE_INSTITUTIONAL_MONTHLY", ""),
        "annual": os.getenv("STRIPE_PRICE_INSTITUTIONAL_ANNUAL", ""),
    },
}

if STRIPE_SECRET_KEY:
    stripe.api_key = STRIPE_SECRET_KEY

# ─────────────────────────────────────────
# TIER LEVELS for gating
# ─────────────────────────────────────────
TIER_LEVELS = {"free": 0, "essential": 1, "pro": 2, "institutional": 3}

# ─────────────────────────────────────────
# FIX 1.2: Database engine — support both SQLite (dev) and PostgreSQL (prod)
# ─────────────────────────────────────────
if DATABASE_URL.startswith("sqlite"):
    engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
else:
    # Support Neon and other serverless PostgreSQL
    connect_args = {}
    if "neon.tech" in DATABASE_URL:
        connect_args = {"sslmode": "require"}
    
    engine = create_engine(
        DATABASE_URL,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        connect_args=connect_args,
    )

SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()


# ─────────────────────────────────────────
# DATABASE MODELS
# ─────────────────────────────────────────
class MarketSummary(Base):
    __tablename__ = "market_summary"
    # FIX 3.4: Composite index for most frequent query pattern
    __table_args__ = (
        Index('ix_market_summary_coin_tf_created', 'coin', 'timeframe', 'created_at'),
    )
    id = Column(Integer, primary_key=True)
    coin = Column(String, index=True)
    timeframe = Column(String, index=True, default="1h")
    score = Column(Float)
    label = Column(String)
    coherence = Column(Float)
    momentum_4h = Column(Float, default=0)
    momentum_24h = Column(Float, default=0)
    volatility_val = Column(Float, default=0)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)


class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    email = Column(String, unique=True, index=True)
    subscription_status = Column(String, default="inactive")
    stripe_customer_id = Column(String, nullable=True)
    stripe_subscription_id = Column(String, nullable=True)
    tier = Column(String, default="free")
    alerts_enabled = Column(Boolean, default=False)
    last_alert_sent = Column(DateTime, nullable=True)
    access_token = Column(String, nullable=True, index=True)
    # FIX 1.3: Token expiry support
    token_created_at = Column(DateTime, nullable=True)
    # FIX 2.3: Trial onboarding support
    trial_start_date = Column(DateTime, nullable=True)
    onboarding_step = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    last_active_at = Column(DateTime, nullable=True)


class UserProfile(Base):
    __tablename__ = "user_profiles"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, index=True)
    email = Column(String, unique=True, index=True)
    max_drawdown_pct = Column(Float, default=20.0)
    typical_leverage = Column(Float, default=1.0)
    holding_period_days = Column(Integer, default=10)
    risk_identity = Column(String, default="balanced")
    risk_multiplier = Column(Float, default=1.0)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow)


class ExposureLog(Base):
    __tablename__ = "exposure_logs"
    # FIX 3.4: Composite index for frequent email+coin+created_at queries
    __table_args__ = (
        Index('ix_exposure_log_email_coin_created', 'email', 'coin', 'created_at'),
    )
    id = Column(Integer, primary_key=True)
    email = Column(String, index=True)
    coin = Column(String, default="BTC")
    user_exposure_pct = Column(Float)
    model_exposure_pct = Column(Float)
    regime_label = Column(String)
    hazard_at_log = Column(Float, default=0)
    shift_risk_at_log = Column(Float, default=0)
    alignment_at_log = Column(Float, default=0)
    followed_model = Column(Boolean, default=False)
    price_at_log = Column(Float, default=0)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)


class PerformanceEntry(Base):
    __tablename__ = "performance_entries"
    # FIX 3.4: Composite index
    __table_args__ = (
        Index('ix_performance_email_coin_date', 'email', 'coin', 'date'),
    )
    id = Column(Integer, primary_key=True)
    email = Column(String, index=True)
    coin = Column(String, default="BTC")
    date = Column(DateTime, default=datetime.datetime.utcnow)
    user_exposure_pct = Column(Float, default=0)
    model_exposure_pct = Column(Float, default=0)
    price_open = Column(Float, default=0)
    price_close = Column(Float, default=0)
    user_return_pct = Column(Float, default=0)
    model_return_pct = Column(Float, default=0)
    regime_label = Column(String, default="Neutral")
    discipline_flags = Column(String, default="")


class SetupQualityCache(Base):
    __tablename__ = "setup_quality_cache"
    id = Column(Integer, primary_key=True)
    coin = Column(String, index=True)
    timeframe = Column(String, default="1h")
    setup_score = Column(Float, default=50)
    chase_risk = Column(Float, default=50)
    exhaustion = Column(Float, default=50)
    entry_mode = Column(String, default="Wait")
    setup_label = Column(String, default="Neutral")
    optimal_entry_low = Column(Float, default=0)
    optimal_entry_high = Column(Float, default=0)
    invalidation_level = Column(Float, default=0)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)


class HistoricalAnalog(Base):
    __tablename__ = "historical_analogs"
    id = Column(Integer, primary_key=True)
    coin = Column(String, index=True)
    macro_label = Column(String)
    trend_label = Column(String)
    exec_label = Column(String)
    score_at_time = Column(Float)
    hazard_at_time = Column(Float, default=0)
    forward_1d_ret = Column(Float, nullable=True)
    forward_3d_ret = Column(Float, nullable=True)
    forward_7d_ret = Column(Float, nullable=True)
    max_adverse_exc = Column(Float, nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)


class BehavioralLeak(Base):
    __tablename__ = "behavioral_leaks"
    id = Column(Integer, primary_key=True)
    email = Column(String, index=True)
    leak_type = Column(String)
    frequency = Column(Integer, default=0)
    alpha_drag_pct = Column(Float, default=0)
    last_occurrence = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)


class AlertThreshold(Base):
    __tablename__ = "alert_thresholds"
    id = Column(Integer, primary_key=True)
    email = Column(String, index=True)
    coin = Column(String, default="BTC")
    shift_risk_threshold = Column(Float, default=70)
    exposure_change_threshold = Column(Float, default=10)
    setup_quality_threshold = Column(Float, default=70)
    regime_quality_threshold = Column(Float, default=50)
    enabled = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)


class TradePlan(Base):
    __tablename__ = "trade_plans"
    id = Column(Integer, primary_key=True)
    email = Column(String, index=True)
    coin = Column(String, default="BTC")
    bias = Column(String, default="Long")
    allocation_band = Column(String, default="40-60%")
    entry_style = Column(String, default="Pullback")
    tranches = Column(String, default="[20,20,15]")
    invalidation_note = Column(String, default="")
    profit_targets = Column(String, default="[]")
    time_horizon_days = Column(Integer, default=5)
    status = Column(String, default="active")
    created_at = Column(DateTime, default=datetime.datetime.utcnow)


class ApiKey(Base):
    __tablename__ = "api_keys"
    id = Column(Integer, primary_key=True)
    email = Column(String, index=True)
    key = Column(String, unique=True, index=True)
    label = Column(String, default="default")
    is_active = Column(Boolean, default=True)
    tier = Column(String, default="institutional")
    requests_today = Column(Integer, default=0)
    last_request_date = Column(String, nullable=True)  # "YYYY-MM-DD"
    daily_limit = Column(Integer, default=1000)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    last_used_at = Column(DateTime, nullable=True)


class WebhookEndpoint(Base):
    __tablename__ = "webhook_endpoints"
    id = Column(Integer, primary_key=True)
    email = Column(String, index=True)
    url = Column(String)
    secret = Column(String, nullable=True)  # HMAC signing secret
    events = Column(String, default="regime_change,shift_risk_alert,setup_quality_alert")
    is_active = Column(Boolean, default=True)
    last_triggered_at = Column(DateTime, nullable=True)
    failure_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)


class WebhookDelivery(Base):
    __tablename__ = "webhook_deliveries"
    __table_args__ = (
        Index('ix_webhook_delivery_endpoint_created', 'endpoint_id', 'created_at'),
    )
    id = Column(Integer, primary_key=True)
    endpoint_id = Column(Integer, index=True)
    event_type = Column(String)
    payload = Column(String)  # JSON string
    response_status = Column(Integer, nullable=True)
    response_body = Column(String, nullable=True)
    success = Column(Boolean, default=False)
    attempt = Column(Integer, default=1)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

class IntelligenceBrief(Base):
    __tablename__ = "intelligence_briefs"
    id = Column(Integer, primary_key=True)
    brief_type = Column(String, default="weekly")
    content_json = Column(String, default="{}")
    created_at = Column(DateTime, default=datetime.datetime.utcnow)


Base.metadata.create_all(bind=engine)


# ─────────────────────────────────────────
# PYDANTIC MODELS — FIX 1.5: EmailStr validation on all email inputs
# ─────────────────────────────────────────
class SubscribeRequest(BaseModel):
    email: EmailStr  # CHANGED: was str

class UserProfileRequest(BaseModel):
    email: EmailStr  # CHANGED
    max_drawdown_pct: float = 20.0
    typical_leverage: float = 1.0
    holding_period_days: int = 10
    risk_identity: str = "balanced"

class ExposureLogRequest(BaseModel):
    email: EmailStr  # CHANGED
    coin: str = "BTC"
    user_exposure_pct: float

class PerformanceEntryRequest(BaseModel):
    email: EmailStr  # CHANGED
    coin: str = "BTC"
    user_exposure_pct: float
    price_open: float
    price_close: float

class CheckoutRequest(BaseModel):
    email: Optional[str] = None
    billing_cycle: str = "monthly"  # "monthly" or "annual"
    tier: str = "pro"  # "essential", "pro", or "institutional"

class AlertThresholdRequest(BaseModel):
    email: EmailStr  # CHANGED
    coin: str = "BTC"
    shift_risk_threshold: float = 70
    exposure_change_threshold: float = 10
    setup_quality_threshold: float = 70
    regime_quality_threshold: float = 50

class TradePlanRequest(BaseModel):
    email: EmailStr  # CHANGED
    coin: str = "BTC"
    account_size: float = 10000
    strategy_mode: str = "balanced"

class BehavioralReportRequest(BaseModel):
    email: EmailStr  # CHANGED
    lookback_days: int = 30

class TraderArchetype(BaseModel):
    email: EmailStr  # CHANGED
    archetype: str = "swing"

class RestoreRequest(BaseModel):
    email: EmailStr  # CHANGED

class ApiKeyRequest(BaseModel):
    email: EmailStr
    label: str = "default"


class WebhookCreateRequest(BaseModel):
    email: EmailStr
    url: str
    secret: Optional[str] = None
    events: str = "regime_change,shift_risk_alert,setup_quality_alert"


class WebhookUpdateRequest(BaseModel):
    email: EmailStr
    webhook_id: int
    url: Optional[str] = None
    events: Optional[str] = None
    is_active: Optional[bool] = None


# ─────────────────────────────────────────
# TRADER ARCHETYPE CONFIGURATIONS (unchanged)
# ─────────────────────────────────────────
ARCHETYPE_CONFIG = {
    "swing": {
        "label": "Swing Trader",
        "exposure_mult": 1.0,
        "alert_sensitivity": "medium",
        "preferred_timeframe": "4h",
        "max_hold_days": 14,
        "stop_width_mult": 1.0,
        "typical_tranches": [30, 30, 20],
        "playbook_bias": "trend_follow",
        "description": "Holds positions for days to weeks. Follows intermediate trends.",
    },
    "position": {
        "label": "Position Trader",
        "exposure_mult": 0.85,
        "alert_sensitivity": "low",
        "preferred_timeframe": "1d",
        "max_hold_days": 60,
        "stop_width_mult": 1.5,
        "typical_tranches": [25, 25, 25, 15],
        "playbook_bias": "macro_follow",
        "description": "Longer-term conviction trades. Macro regime driven.",
    },
    "spot_allocator": {
        "label": "Spot Allocator",
        "exposure_mult": 0.75,
        "alert_sensitivity": "low",
        "preferred_timeframe": "1d",
        "max_hold_days": 90,
        "stop_width_mult": 2.0,
        "typical_tranches": [20, 20, 20, 20],
        "playbook_bias": "buy_and_hold",
        "description": "DCA-oriented. Uses regime data for timing allocation size.",
    },
    "tactical": {
        "label": "Tactical De-risker",
        "exposure_mult": 1.1,
        "alert_sensitivity": "high",
        "preferred_timeframe": "1h",
        "max_hold_days": 7,
        "stop_width_mult": 0.8,
        "typical_tranches": [35, 35, 20],
        "playbook_bias": "mean_revert",
        "description": "Active risk management. Quickly adjusts exposure to regime changes.",
    },
    "leverage": {
        "label": "Leverage Trader",
        "exposure_mult": 1.3,
        "alert_sensitivity": "high",
        "preferred_timeframe": "1h",
        "max_hold_days": 5,
        "stop_width_mult": 0.6,
        "typical_tranches": [40, 30, 20],
        "playbook_bias": "momentum",
        "description": "Uses leverage. Needs tightest risk controls and fastest alerts.",
    },
}


# ─────────────────────────────────────────
# RISK EVENT CALENDAR (DYNAMIC) — unchanged structure
# ─────────────────────────────────────────
DYNAMIC_RISK_EVENTS = [
    {"name": "FOMC Meeting", "type": "macro", "impact": "High", "recurrence": "6_weeks", "typical_vol_multiplier": 1.8, "regime_survival_impact": -15},
    {"name": "CPI Release", "type": "macro", "impact": "High", "recurrence": "monthly", "typical_vol_multiplier": 1.6, "regime_survival_impact": -12},
    {"name": "Options Expiry", "type": "market", "impact": "Medium", "recurrence": "monthly", "typical_vol_multiplier": 1.3, "regime_survival_impact": -8},
    {"name": "ETF Flow Report", "type": "market", "impact": "Medium", "recurrence": "weekly", "typical_vol_multiplier": 1.1, "regime_survival_impact": -5},
    {"name": "PCE Inflation", "type": "macro", "impact": "High", "recurrence": "monthly", "typical_vol_multiplier": 1.5, "regime_survival_impact": -10},
    {"name": "Fed Minutes", "type": "macro", "impact": "Medium", "recurrence": "6_weeks", "typical_vol_multiplier": 1.3, "regime_survival_impact": -8},
    {"name": "Jobs Report (NFP)", "type": "macro", "impact": "High", "recurrence": "monthly", "typical_vol_multiplier": 1.5, "regime_survival_impact": -12},
    {"name": "Quarterly GDP", "type": "macro", "impact": "Medium", "recurrence": "quarterly", "typical_vol_multiplier": 1.2, "regime_survival_impact": -6},
]


# ─────────────────────────────────────────
# BEHAVIORAL LEAK TYPES (unchanged)
# ─────────────────────────────────────────
LEAK_TYPES = {
    "late_entry_chasing": {"label": "Late Entry / Chasing", "description": "Entering positions after significant extension, when chase risk is high.", "severity_weight": 1.5},
    "overexposed_risk_off": {"label": "Over-Exposed in Risk-Off", "description": "Maintaining high exposure when regime signals defensive positioning.", "severity_weight": 2.0},
    "ignored_hazard_spike": {"label": "Ignored Hazard Spike", "description": "Failed to reduce exposure when hazard rate spiked above 65%.", "severity_weight": 1.8},
    "premature_exit_strength": {"label": "Premature Exit in Strength", "description": "Reducing exposure during strong, healthy regime conditions.", "severity_weight": 1.0},
    "averaging_down_risk_off": {"label": "Averaging Down in Risk-Off", "description": "Adding to losing positions during deteriorating regime.", "severity_weight": 2.5},
    "overtrading": {"label": "Overtrading", "description": "Logging exposure changes too frequently relative to regime changes.", "severity_weight": 1.2},
    "size_too_large": {"label": "Position Size Too Large", "description": "Exposure consistently exceeds model recommendation by >25%.", "severity_weight": 1.8},
    "failed_to_press_edge": {"label": "Failed to Press Edge", "description": "Under-exposed during high-quality regime conditions where user has historical edge.", "severity_weight": 1.0},
}


class InMemoryRateLimiter:
    """
    Token bucket rate limiter.
    For production at scale, replace with Redis-based (e.g., redis + lua script).
    """
    
    def __init__(self):
        self._buckets: dict[str, dict] = {}
        self._lock = threading.Lock()
    
    def _get_key(self, request: Request, key_type: str = "ip") -> str:
        if key_type == "ip":
            forwarded = request.headers.get("x-forwarded-for")
            ip = forwarded.split(",")[0].strip() if forwarded else (
                request.client.host if request.client else "unknown"
            )
            return f"rate:{ip}"
        return f"rate:{key_type}"
    
    def check(
        self,
        request: Request,
        max_requests: int = 60,
        window_seconds: int = 60,
        key_type: str = "ip",
    ) -> bool:
        key = self._get_key(request, key_type)
        now = time.time()
        
        with self._lock:
            if key not in self._buckets:
                self._buckets[key] = {"tokens": max_requests - 1, "last_refill": now}
                return True
            
            bucket = self._buckets[key]
            elapsed = now - bucket["last_refill"]
            refill = elapsed * (max_requests / window_seconds)
            bucket["tokens"] = min(max_requests, bucket["tokens"] + refill)
            bucket["last_refill"] = now
            
            if bucket["tokens"] < 1:
                return False
            
            bucket["tokens"] -= 1
            return True
    
    def require(
        self,
        request: Request,
        max_requests: int = 60,
        window_seconds: int = 60,
        key_type: str = "ip",
    ):
        if not self.check(request, max_requests, window_seconds, key_type):
            raise HTTPException(
                status_code=429,
                detail="Rate limit exceeded. Try again later.",
                headers={"Retry-After": str(window_seconds)},
            )


rate_limiter = InMemoryRateLimiter()

# ─────────────────────────────────────────
# APP SETUP
# ─────────────────────────────────────────
app = FastAPI(title="ChainPulse API", version="5.0")

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
# STARTUP: Add tier column if it doesn't exist
# ─────────────────────────────────────────
@app.on_event("startup")
def add_tier_column():
    try:
        with engine.connect() as conn:
            # Add tier column if it doesn't exist
            try:
                conn.execute(text("ALTER TABLE users ADD COLUMN tier VARCHAR DEFAULT 'free'"))
                conn.commit()
            except Exception:
                pass  # Column already exists

            # Backfill: any active user without a tier gets "pro" (legacy subscribers)
            conn.execute(text("""
                UPDATE users 
                SET tier = 'pro' 
                WHERE subscription_status = 'active' 
                AND (tier IS NULL OR tier = '' OR tier = 'free')
            """))
            conn.commit()
            logger.info("Backfilled existing active users to 'pro' tier")
    except Exception as e:
        logger.error(f"Tier migration error: {e}")

    # Create new tables (api_keys, webhook_endpoints, webhook_deliveries)
    Base.metadata.create_all(bind=engine)

# ─────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────
SUPPORTED_COINS = ["BTC", "ETH", "SOL", "BNB", "AVAX", "LINK", "ADA"]
SUPPORTED_TIMEFRAMES = ["1h", "4h", "1d"]

TIMEFRAME_LABELS = {
    "1h": "Execution",
    "4h": "Trend",
    "1d": "Macro",
}

REGIME_NUMERIC = {
    "Strong Risk-On": 2,
    "Risk-On": 1,
    "Neutral": 0,
    "Risk-Off": -1,
    "Strong Risk-Off": -2,
}

PRICE_MONTHLY = 39
PRICE_ANNUAL = 348

RISK_EVENTS = [
    {"name": "FOMC Meeting", "type": "macro", "impact": "High"},
    {"name": "CPI Release", "type": "macro", "impact": "High"},
    {"name": "Options Expiry", "type": "market", "impact": "Medium"},
    {"name": "ETF Flow Report", "type": "market", "impact": "Medium"},
    {"name": "BTC Halving", "type": "crypto", "impact": "High"},
    {"name": "Fed Minutes", "type": "macro", "impact": "Medium"},
    {"name": "PCE Inflation", "type": "macro", "impact": "High"},
]

# FIX 4.3: Clearly labeled as backtested estimates
PLAYBOOK_DATA = {
    "Strong Risk-On": {
        "exposure_band": "65–80%",
        "strategy_mode": "Aggressive",
        "trend_follow_wr": 72,  # backtested estimate
        "mean_revert_wr": 38,   # backtested estimate
        "avg_remaining_days": 14, # backtested estimate
        "data_source": "backtested_estimates",
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
        "mean_revert_wr": 44,
        "avg_remaining_days": 9,
        "data_source": "backtested_estimates",
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
        "mean_revert_wr": 51,
        "avg_remaining_days": 6,
        "data_source": "backtested_estimates",
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
        "mean_revert_wr": 57,
        "avg_remaining_days": 7,
        "data_source": "backtested_estimates",
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
        "mean_revert_wr": 48,
        "avg_remaining_days": 11,
        "data_source": "backtested_estimates",
        "actions": [
            "Move to maximum cash allocation",
            "Monitor for capitulation signals",
        ],
        "avoid": ["Catching falling knives", "Any leveraged long exposure"],
    },
}


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
# FIX 3.2: Unified cache-or-compute helper
# ─────────────────────────────────────────
def get_or_compute(cache_key: str, compute_fn, ttl: int = 120, *args, **kwargs):
    """Always check cache first, then compute and cache."""
    cached = cache_get(cache_key)
    if cached is not None:
        return cached
    result = compute_fn(*args, **kwargs)
    if result is not None:
        cache_set(cache_key, result, ttl=ttl)
    return result


# ─────────────────────────────────────────
# AUTH HELPERS — FIX 1.3: Token expiry + length validation
# ─────────────────────────────────────────
TOKEN_EXPIRY_DAYS = 90

# ─────────────────────────────────────────
# AUTH HELPERS — Tier-aware
# ─────────────────────────────────────────
TOKEN_EXPIRY_DAYS = 90


def resolve_user_tier(authorization: Optional[str], db: Session) -> dict:
    """Returns {'is_pro': bool, 'tier': str, 'user': User|None}"""
    if not authorization or not authorization.startswith("Bearer "):
        return {"is_pro": False, "tier": "free", "user": None}

    token = authorization.replace("Bearer ", "").strip()
    if not token or len(token) < 20:
        return {"is_pro": False, "tier": "free", "user": None}

    user = db.query(User).filter(User.access_token == token).first()
    if not user:
        return {"is_pro": False, "tier": "free", "user": None}

    # Token expiry check
    if user.token_created_at:
        age = (datetime.datetime.utcnow() - user.token_created_at).days
        if age > TOKEN_EXPIRY_DAYS:
            return {"is_pro": False, "tier": "free", "user": user}

    if user.subscription_status not in ("active", "trialing"):
        return {"is_pro": False, "tier": "free", "user": user}

    tier = user.tier or "free"
    return {
        "is_pro": tier in ("essential", "pro", "institutional"),
        "tier": tier,
        "user": user,
    }


def resolve_pro_status(authorization: Optional[str], db: Session) -> bool:
    """Legacy helper — returns True if user has any paid tier."""
    info = resolve_user_tier(authorization, db)
    return info["is_pro"]


def require_tier(authorization: str, db: Session, minimum_tier: str = "essential") -> dict:
    """Checks user has at least the specified tier. Raises 403 if not."""
    user_info = resolve_user_tier(authorization, db)
    user_level = TIER_LEVELS.get(user_info["tier"], 0)
    required_level = TIER_LEVELS.get(minimum_tier, 0)

    if user_level < required_level:
        raise HTTPException(
            status_code=403,
            detail=f"This feature requires {minimum_tier} tier or higher. Your tier: {user_info['tier']}"
        )

    return user_info

def require_email_ownership(user_info: dict, requested_email: str) -> str:
    authenticated_email = (user_info.get("user").email if user_info.get("user") else None)
    if not authenticated_email:
        raise HTTPException(status_code=401, detail="Authentication required")
    requested_email = requested_email.strip().lower()
    if requested_email and requested_email != authenticated_email:
        raise HTTPException(status_code=403, detail="You can only access your own data.")
    return authenticated_email

def get_auth_header(request: Request) -> Optional[str]:
    return (
        request.headers.get("authorization")
        or request.headers.get("Authorization")
    )

# ─────────────────────────────────────────
# API KEY AUTH (Institutional)
# ─────────────────────────────────────────
def resolve_api_key(request: Request, db: Session) -> Optional[dict]:
    """
    Checks for API key in X-API-Key header or ?api_key= query param.
    Returns {'email': str, 'tier': str, 'api_key_id': int} or None.
    """
    api_key = (
        request.headers.get("X-API-Key")
        or request.headers.get("x-api-key")
        or request.query_params.get("api_key")
    )

    if not api_key or len(api_key) < 20:
        return None

    key_record = db.query(ApiKey).filter(
        ApiKey.key == api_key,
        ApiKey.is_active == True,
    ).first()

    if not key_record:
        return None

    # Check daily rate limit
    today = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    if key_record.last_request_date != today:
        key_record.requests_today = 0
        key_record.last_request_date = today

    if key_record.requests_today >= key_record.daily_limit:
        raise HTTPException(
            status_code=429,
            detail=f"Daily API limit reached ({key_record.daily_limit} requests/day). Resets at midnight UTC."
        )

    # Increment counter
    key_record.requests_today += 1
    key_record.last_used_at = datetime.datetime.utcnow()
    db.commit()

    # Verify the user is still institutional
    user = db.query(User).filter(User.email == key_record.email).first()
    if not user or user.subscription_status != "active" or user.tier != "institutional":
        return None

    return {
        "email": key_record.email,
        "tier": user.tier,
        "api_key_id": key_record.id,
        "requests_remaining": key_record.daily_limit - key_record.requests_today,
    }


def require_api_key(request: Request, db: Session) -> dict:
    """Requires valid API key. Raises 401/403 if invalid."""
    result = resolve_api_key(request, db)
    if not result:
        raise HTTPException(
            status_code=401,
            detail="Valid API key required. Get yours at /api/v1/keys"
        )
    return result

# ─────────────────────────────────────────
# WEBHOOK DELIVERY ENGINE
# ─────────────────────────────────────────
import hashlib
import hmac as hmac_lib


def sign_webhook_payload(payload_str: str, secret: str) -> str:
    """Creates HMAC-SHA256 signature for webhook payload."""
    return hmac_lib.new(
        secret.encode("utf-8"),
        payload_str.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def deliver_webhook(endpoint: WebhookEndpoint, event_type: str, payload: dict, db: Session) -> bool:
    """Delivers a webhook to an endpoint. Returns True on success."""
    payload_str = json.dumps(payload)

    headers = {
        "Content-Type": "application/json",
        "User-Agent": "ChainPulse-Webhook/1.0",
        "X-ChainPulse-Event": event_type,
        "X-ChainPulse-Timestamp": datetime.datetime.utcnow().isoformat(),
    }

    # Sign payload if secret is configured
    if endpoint.secret:
        signature = sign_webhook_payload(payload_str, endpoint.secret)
        headers["X-ChainPulse-Signature"] = f"sha256={signature}"

    delivery = WebhookDelivery(
        endpoint_id=endpoint.id,
        event_type=event_type,
        payload=payload_str,
    )

    try:
        r = requests.post(
            endpoint.url,
            data=payload_str,
            headers=headers,
            timeout=10,
        )
        delivery.response_status = r.status_code
        delivery.response_body = r.text[:500] if r.text else None
        delivery.success = 200 <= r.status_code < 300

        if delivery.success:
            endpoint.failure_count = 0
        else:
            endpoint.failure_count += 1

    except Exception as e:
        delivery.response_status = 0
        delivery.response_body = str(e)[:500]
        delivery.success = False
        endpoint.failure_count += 1
        logger.error(f"Webhook delivery failed for {endpoint.url}: {e}")

    endpoint.last_triggered_at = datetime.datetime.utcnow()
    db.add(delivery)

    # Auto-disable after 10 consecutive failures
    if endpoint.failure_count >= 10:
        endpoint.is_active = False
        logger.warning(f"Webhook disabled after 10 failures: {endpoint.url}")

    db.commit()
    return delivery.success


def trigger_webhooks(event_type: str, payload: dict, db: Session, coin: str = None):
    """Triggers all active webhooks for a given event type."""
    endpoints = db.query(WebhookEndpoint).filter(
        WebhookEndpoint.is_active == True,
    ).all()

    sent = 0
    for endpoint in endpoints:
        # Check if endpoint subscribes to this event type
        subscribed_events = [e.strip() for e in (endpoint.events or "").split(",")]
        if event_type not in subscribed_events and "*" not in subscribed_events:
            continue

        # Verify user is still institutional
        user = db.query(User).filter(User.email == endpoint.email).first()
        if not user or user.tier != "institutional" or user.subscription_status != "active":
            continue

        # Add metadata to payload
        full_payload = {
            "event": event_type,
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "coin": coin,
            **payload,
        }

        deliver_webhook(endpoint, event_type, full_payload, db)
        sent += 1

    return sent

# ─────────────────────────────────────────
# User Activity Tracking
# ─────────────────────────────────────────
def update_last_active(request: Request, db: Session):
    token = get_auth_header(request)
    if not token:
        return
    token_val = token.replace("Bearer ", "").strip() if token.startswith("Bearer ") else token
    user = db.query(User).filter(User.access_token == token_val).first()
    if user:
        user.last_active_at = datetime.datetime.utcnow()
        db.commit()


# ─────────────────────────────────────────
# FIX 3.3 + FIX 1.1: Market data fetcher with cache fallback
# Fetch all klines ONCE, cache results, graceful degradation
# ─────────────────────────────────────────
def get_klines(symbol: str, interval: str, limit: int = 120):
    """
    Fetches kline data from Binance with:
    - Cache-first strategy (FIX 3.3)
    - Stale data fallback if Binance is down (FIX 3.3)
    """
    cache_key = f"klines:{symbol}:{interval}:{limit}"
    cached = cache_get(cache_key)

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
            prices = [float(c[4]) for c in data]
            volumes = [float(c[5]) for c in data]
            logger.info(f"Got {len(prices)} candles for {symbol}/{interval}")
            # Cache successful response for fallback
            cache_set(cache_key, {"prices": prices, "volumes": volumes}, ttl=300)
            return prices, volumes
        except Exception as e:
            logger.error(f"Kline fetch failed {url} {symbol}/{interval}: {e}")
            continue

    # FIX 3.3: Fallback to stale cached data
    if cached:
        logger.warning(f"Using stale kline data for {symbol}/{interval}")
        return cached["prices"], cached["volumes"]

    return [], []


# ─────────────────────────────────────────
# FIX 1.1: Bulk market data fetcher — fetch all timeframes ONCE
# ─────────────────────────────────────────
def fetch_all_market_data(coin: str) -> dict:
    """
    Fetches 1h, 4h, 1d kline data in one pass.
    Returns a dict keyed by timeframe with prices and volumes.
    Used by premium-dashboard and other heavy endpoints to avoid
    redundant Binance API calls.
    """
    cache_key = f"market_data_all:{coin}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    market_data = {}
    for tf, limit in [("1h", 120), ("4h", 60), ("1d", 90)]:
        prices, volumes = get_klines(coin, tf, limit=limit)
        market_data[tf] = {"prices": prices, "volumes": volumes}

    cache_set(cache_key, market_data, ttl=60)
    return market_data


# ─────────────────────────────────────────
# MARKET DATA HELPERS (pure math — unchanged logic)
# ─────────────────────────────────────────
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


def calculate_coherence(mom_short: float, mom_long: float, vol_score: float) -> float:
    if (mom_short >= 0 and mom_long >= 0) or (mom_short < 0 and mom_long < 0):
        alignment = 1.0
    else:
        alignment = 0.3
    magnitude = (abs(mom_short) + abs(mom_long)) / 2
    magnitude_norm = min(magnitude / 5.0, 1.0) * 100
    vol_penalty = min(vol_score / 500, 0.5)
    raw = alignment * magnitude_norm * (1 - vol_penalty)
    return round(max(0, min(100, raw)), 2)


def calculate_score_for_timeframe(coin: str, interval: str, market_data: dict = None) -> Optional[dict]:
    """
    FIX 1.1: Accepts optional pre-fetched market_data to avoid redundant API calls.
    """
    if market_data and interval in market_data:
        prices = market_data[interval]["prices"]
        volumes = market_data[interval]["volumes"]
    else:
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
    mom_long = ((prices[-1] - prices[-long_lb]) / prices[-long_lb]) * 100
    vol = volatility(prices)
    vol_mom = volume_momentum(volumes)
    score = 0.55 * mom_long + 0.35 * mom_short - 0.08 * vol + 0.02 * vol_mom
    score = max(-100, min(100, score))
    coherence = calculate_coherence(mom_short, mom_long, vol)
    return {
        "score": round(score, 4),
        "mom_short": round(mom_short, 4),
        "mom_long": round(mom_long, 4),
        "volatility": round(vol, 4),
        "coherence": coherence,
    }


def classify(score: float) -> str:
    if score > 35: return "Strong Risk-On"
    if score > 15: return "Risk-On"
    if score < -35: return "Strong Risk-Off"
    if score < -15: return "Risk-Off"
    return "Neutral"


# ─────────────────────────────────────────
# REGIME ALIGNMENT ENGINE (unchanged)
# ─────────────────────────────────────────
def regime_alignment(labels: list) -> float:
    scores = [REGIME_NUMERIC.get(l, 0) for l in labels]
    if not scores:
        return 0.0
    max_sum = 2 * len(scores)
    return round((abs(sum(scores)) / max_sum) * 100, 2)


def alignment_direction(labels: list) -> str:
    scores = [REGIME_NUMERIC.get(l, 0) for l in labels]
    total = sum(scores)
    if total > 0: return "bullish"
    if total < 0: return "bearish"
    return "mixed"


# ─────────────────────────────────────────
# STATISTICS ENGINE (unchanged logic)
# ─────────────────────────────────────────
def get_history(db: Session, coin: str, timeframe: str = "1h"):
    return (
        db.query(MarketSummary)
        .filter(MarketSummary.coin == coin, MarketSummary.timeframe == timeframe)
        .order_by(MarketSummary.created_at.asc())
        .all()
    )


def regime_durations(db: Session, coin: str, timeframe: str = "1h") -> list:
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


def current_age(db: Session, coin: str, timeframe: str = "1h") -> float:
    records = (
        db.query(MarketSummary)
        .filter(MarketSummary.coin == coin, MarketSummary.timeframe == timeframe)
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
    return (datetime.datetime.utcnow() - start_time).total_seconds() / 3600


def survival_probability(db: Session, coin: str, timeframe: str = "1h") -> float:
    durations = regime_durations(db, coin, timeframe)
    age = current_age(db, coin, timeframe)
    if len(durations) < 5:
        return round(max(20.0, 90.0 - age * 4), 2)
    longer = [d for d in durations if d > age]
    return round((len(longer) / len(durations)) * 100, 2)


def hazard_rate(db: Session, coin: str, timeframe: str = "1h") -> float:
    durations = regime_durations(db, coin, timeframe)
    age = current_age(db, coin, timeframe)
    if len(durations) < 5:
        return round(min(70.0, age * 5), 2)
    avg = sum(durations) / len(durations)
    return round(min(100.0, (age / (avg + 0.01)) * 100), 2)


def percentile_rank(db: Session, coin: str, current_score: float, timeframe: str = "1h") -> float:
    scores = [r.score for r in get_history(db, coin, timeframe)]
    if len(scores) < 5:
        return round(50 + current_score / 2, 2)
    lower = [s for s in scores if s < current_score]
    return round((len(lower) / len(scores)) * 100, 2)


def average_regime_duration(db: Session, coin: str, timeframe: str = "1h") -> float:
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
            hazard * 0.50 +
            (100 - survival) * 0.35 +
            (100 - coherence) * 0.15),
        2,
    )


def exposure_recommendation(score: float, survival: float, hazard: float, coherence: float) -> float:
    if score > 35: base = 0.85
    elif score > 15: base = 0.65
    elif score < -35: base = 0.08
    elif score < -15: base = 0.22
    else: base = 0.42
    persistence_factor = survival / 100
    hazard_penalty = 1 - (hazard / 100) * 0.65
    coherence_factor = 0.7 + (coherence / 100) * 0.3
    exposure = base * persistence_factor * hazard_penalty * coherence_factor
    return round(max(5.0, min(95.0, exposure * 100)), 2)


def exposure_recommendation_stacked(
    macro_label: str, trend_label: str, exec_label: str,
    alignment: float, survival_1h: float, hazard_1h: float, coherence_1h: float,
) -> float:
    macro_num = REGIME_NUMERIC.get(macro_label, 0)
    if macro_num >= 1:
        macro_ceiling, macro_floor = 0.90, 0.30
    elif macro_num == 0:
        macro_ceiling, macro_floor = 0.60, 0.20
    else:
        macro_ceiling, macro_floor = 0.35, 0.05

    trend_num = REGIME_NUMERIC.get(trend_label, 0)
    rang = macro_ceiling - macro_floor
    if trend_num == 2: base = macro_ceiling
    elif trend_num == 1: base = macro_floor + rang * 0.75
    elif trend_num == 0: base = macro_floor + rang * 0.50
    elif trend_num == -1: base = macro_floor + rang * 0.25
    else: base = macro_floor

    exec_num = REGIME_NUMERIC.get(exec_label, 0)
    base = base + (exec_num / 2) * 0.10

    persistence_factor = survival_1h / 100
    hazard_penalty = 1 - (hazard_1h / 100) * 0.65
    coherence_factor = 0.7 + (coherence_1h / 100) * 0.3
    alignment_mult = 0.5 + alignment / 200

    exposure = base * persistence_factor * hazard_penalty * coherence_factor * alignment_mult
    return round(max(5.0, min(95.0, exposure * 100)), 2)


# ─────────────────────────────────────────
# REGIME STACK BUILDER (unchanged logic)
# ─────────────────────────────────────────
def build_regime_stack(coin: str, db: Session) -> dict:
    stack = {}
    labels = []
    coherences = []

    for tf in ["1d", "4h", "1h"]:
        record = (
            db.query(MarketSummary)
            .filter(MarketSummary.coin == coin, MarketSummary.timeframe == tf)
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
            "macro": stack.get("1d"), "trend": stack.get("4h"), "execution": stack.get("1h"),
            "alignment": None, "direction": None, "exposure": None,
            "shift_risk": None, "survival": None, "hazard": None, "incomplete": True,
        }

    align = regime_alignment(labels)
    direction = alignment_direction(labels)
    avg_coh = sum(coherences) / len(coherences)
    survival_1h = survival_probability(db, coin, "1h")
    hazard_1h = hazard_rate(db, coin, "1h")

    exposure = exposure_recommendation_stacked(
        macro_label=stack["1d"]["label"], trend_label=stack["4h"]["label"],
        exec_label=stack["1h"]["label"], alignment=align,
        survival_1h=survival_1h, hazard_1h=hazard_1h, coherence_1h=stack["1h"]["coherence"],
    )
    shift_risk = regime_shift_risk(hazard_1h, survival_1h, avg_coh)

    return {
        "coin": coin,
        "macro": stack["1d"], "trend": stack["4h"], "execution": stack["1h"],
        "alignment": align, "direction": direction, "exposure": exposure,
        "shift_risk": shift_risk, "survival": survival_1h, "hazard": hazard_1h,
        "incomplete": False,
    }


# ─────────────────────────────────────────
# MARKET BREADTH (unchanged)
# ─────────────────────────────────────────
def compute_market_breadth(db: Session) -> dict:
    bullish = neutral = bearish = 0
    for coin in SUPPORTED_COINS:
        record = (
            db.query(MarketSummary)
            .filter(MarketSummary.coin == coin, MarketSummary.timeframe == "1d")
            .order_by(MarketSummary.created_at.desc())
            .first()
        )
        if not record:
            continue
        n = REGIME_NUMERIC.get(record.label, 0)
        if n > 0: bullish += 1
        elif n < 0: bearish += 1
        else: neutral += 1

    total = bullish + neutral + bearish
    if total == 0:
        return {"bullish": 0, "neutral": 0, "bearish": 0, "total": 0, "breadth_score": 0}
    return {
        "bullish": bullish, "neutral": neutral, "bearish": bearish,
        "total": total, "breadth_score": round(((bullish - bearish) / total) * 100, 2),
    }


# ─────────────────────────────────────────
# VOLATILITY ENVIRONMENT — FIX 1.1: accepts pre-fetched market_data
# ─────────────────────────────────────────
def volatility_environment(coin: str, db: Session, market_data: dict = None) -> Optional[dict]:
    if market_data:
        prices_1h = market_data.get("1h", {}).get("prices", [])
        volumes_1h = market_data.get("1h", {}).get("volumes", [])
        prices_1d = market_data.get("1d", {}).get("prices", [])
    else:
        prices_1h, volumes_1h = get_klines(coin, "1h", limit=48)
        prices_1d, _ = get_klines(coin, "1d", limit=30)

    if not prices_1h or not prices_1d:
        return None

    vol_1h = volatility(prices_1h, period=min(24, len(prices_1h)))
    vol_1d = volatility(prices_1d, period=min(20, len(prices_1d)))
    vol_ratio = vol_1h / (vol_1d + 0.0001)

    if vol_ratio > 1.5: vol_label, vol_score = "Extreme", 90
    elif vol_ratio > 1.0: vol_label, vol_score = "Elevated", 65
    elif vol_ratio > 0.5: vol_label, vol_score = "Moderate", 40
    else: vol_label, vol_score = "Low", 15

    if len(prices_1h) >= 24:
        rets = [(prices_1h[i] - prices_1h[i - 1]) / prices_1h[i - 1] for i in range(1, min(24, len(prices_1h)))]
        positive = sum(1 for r in rets if r > 0)
        stab_pct = round((positive / len(rets)) * 100, 1)
        stab_lbl = "Strong" if stab_pct > 65 else "Moderate" if stab_pct > 50 else "Weak" if stab_pct > 35 else "Deteriorating"
    else:
        stab_pct, stab_lbl = 50, "Insufficient data"

    stress_score = round(vol_score * 0.6 + (100 - stab_pct) * 0.4, 1)
    stress_label = "High" if stress_score > 70 else "Moderate" if stress_score > 40 else "Low"

    # Use pre-fetched volumes or fetch
    if not volumes_1h:
        _, volumes_1h = get_klines(coin, "1h", limit=24)

    if volumes_1h and len(volumes_1h) >= 10:
        avg_vol = sum(volumes_1h) / len(volumes_1h)
        recent_v = sum(volumes_1h[-6:]) / 6
        liq_ratio = recent_v / (avg_vol + 0.0001)
        liq_label = "High" if liq_ratio > 1.3 else "Normal" if liq_ratio > 0.7 else "Thin"
    else:
        liq_label = "Unknown"

    return {
        "volatility_label": vol_label, "volatility_score": vol_score,
        "stability_label": stab_lbl, "stability_score": round(stab_pct, 1),
        "stress_label": stress_label, "stress_score": round(stress_score, 1),
        "liquidity_label": liq_label,
    }


# ─────────────────────────────────────────
# CORRELATION MONITOR (unchanged)
# ─────────────────────────────────────────
def compute_correlation(prices_a: list, prices_b: list, period: int = 24) -> Optional[float]:
    if len(prices_a) < period + 1 or len(prices_b) < period + 1:
        return None
    def returns(prices):
        return [(prices[i] - prices[i - 1]) / prices[i - 1] for i in range(len(prices) - period, len(prices))]
    ra = returns(prices_a)
    rb = returns(prices_b)
    if len(ra) != len(rb):
        return None
    mean_a = sum(ra) / len(ra)
    mean_b = sum(rb) / len(rb)
    num = sum((a - mean_a) * (b - mean_b) for a, b in zip(ra, rb))
    den_a = math.sqrt(sum((a - mean_a) ** 2 for a in ra))
    den_b = math.sqrt(sum((b - mean_b) ** 2 for b in rb))
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
                    "label": "Strong" if abs_corr > 0.8 else "Moderate" if abs_corr > 0.5 else "Weak",
                })
                if corr < 0.4:
                    alerts.append(f"{a}-{b} correlation breakdown detected ({corr})")

    return {"pairs": pairs, "alerts": alerts}


# ─────────────────────────────────────────
# REGIME CONFIDENCE SCORE (unchanged)
# ─────────────────────────────────────────
def regime_confidence_score(alignment: float, survival: float, coherence: float, breadth_score: float) -> dict:
    breadth_norm = (breadth_score + 100) / 2
    confidence = round(
        alignment * 0.30 + survival * 0.25 + abs(coherence) * 0.25 + breadth_norm * 0.20, 1
    )
    confidence = min(100, max(0, confidence))

    if confidence > 75: label, desc = "High", "Strong regime — elevated conviction warranted"
    elif confidence > 50: label, desc = "Moderate", "Developing regime — standard position sizing"
    elif confidence > 30: label, desc = "Low", "Weak regime — reduce size, widen stops"
    else: label, desc = "Very Low", "No clear regime — minimal exposure only"

    return {
        "score": confidence, "label": label, "description": desc,
        "components": {
            "alignment": round(alignment, 1), "survival": round(survival, 1),
            "coherence": round(abs(coherence), 1), "breadth": round(breadth_norm, 1),
        },
    }


# ─────────────────────────────────────────
# REGIME TRANSITION MATRIX (unchanged)
# ─────────────────────────────────────────
def regime_transition_matrix(db: Session, coin: str, timeframe: str = "1h") -> Optional[dict]:
    records = get_history(db, coin, timeframe)
    if len(records) < 10:
        return None

    STATES = ["Strong Risk-On", "Risk-On", "Neutral", "Risk-Off", "Strong Risk-Off"]
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
        probs = {s: round((row.get(s, 0) / total) * 100, 1) for s in STATES}

    sorted_probs = dict(sorted(probs.items(), key=lambda x: x[1], reverse=True))
    return {
        "current_state": current_state, "transitions": sorted_probs,
        "sample_size": total, "data_sufficient": total >= 10,
    }


# ─────────────────────────────────────────
# PORTFOLIO ALLOCATOR (unchanged)
# ─────────────────────────────────────────
def portfolio_allocation(account_size: float, exposure_pct: float, confidence_score: float, strategy_mode: str = "balanced") -> dict:
    mode_mult = {"conservative": 0.70, "balanced": 1.00, "aggressive": 1.25}
    mult = mode_mult.get(strategy_mode, 1.0)
    adj_exposure = min(95, exposure_pct * mult)
    deployed = round(account_size * adj_exposure / 100, 2)
    cash = round(account_size - deployed, 2)
    swing_pct = 0.35 + (confidence_score / 100) * 0.25
    spot_pct = 1 - swing_pct

    return {
        "account_size": account_size, "strategy_mode": strategy_mode,
        "adjusted_exposure": round(adj_exposure, 1), "deployed_capital": deployed,
        "cash_reserve": cash, "spot_allocation": round(deployed * spot_pct, 2),
        "swing_allocation": round(deployed * swing_pct, 2),
        "cash_pct": round((cash / account_size) * 100, 1),
    }


# ─────────────────────────────────────────
# REGIME QUALITY SCORE (unchanged)
# ─────────────────────────────────────────
def compute_regime_quality(stack: dict) -> dict:
    alignment = stack.get("alignment") or 0
    survival = stack.get("survival") or 50
    hazard = stack.get("hazard") or 50
    shift_risk = stack.get("shift_risk") or 50
    coherence = 50.0
    if stack.get("execution") and stack["execution"].get("coherence"):
        coherence = stack["execution"]["coherence"]

    score = round(
        alignment * 0.30 + survival * 0.25 + (100 - hazard) * 0.20 +
        (100 - shift_risk) * 0.15 + coherence * 0.10, 1
    )

    if score >= 80: grade, structural, breakdown = "A", "Excellent", "Low"
    elif score >= 65: grade, structural, breakdown = "B+", "Strong", "Low-Moderate"
    elif score >= 50: grade, structural, breakdown = "B", "Healthy", "Moderate"
    elif score >= 35: grade, structural, breakdown = "C", "Weakening", "Elevated"
    else: grade, structural, breakdown = "D", "Fragile", "High"

    return {"grade": grade, "score": score, "structural": structural, "breakdown": breakdown}

# ─────────────────────────────────────────
# SETUP QUALITY ENGINE — FIX 1.1: accepts pre-fetched market_data and stack
# ─────────────────────────────────────────
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
    mom_prev = ((prices[-period] - prices[-period * 2]) / prices[-period * 2]) * 100 if len(prices) >= period * 2 else 0
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


def compute_setup_quality(coin: str, db: Session, market_data: dict = None, stack: dict = None) -> dict:
    """
    FIX 1.1: Accepts pre-fetched market_data and stack to avoid redundant
    API calls and DB queries when called from premium-dashboard.
    """
    # Use pre-fetched data or fetch fresh
    if market_data:
        prices_1h = market_data.get("1h", {}).get("prices", [])
        volumes_1h = market_data.get("1h", {}).get("volumes", [])
        prices_4h = market_data.get("4h", {}).get("prices", [])
        volumes_4h = market_data.get("4h", {}).get("volumes", [])
    else:
        prices_1h, volumes_1h = get_klines(coin, "1h", limit=120)
        prices_4h, volumes_4h = get_klines(coin, "4h", limit=60)

    if len(prices_1h) < 50 or len(prices_4h) < 20:
        return {
            "coin": coin,
            "setup_quality_score": None,
            "error": "Insufficient price data",
        }

    current_price = prices_1h[-1]

    # ── Extension Analysis ──
    ext_20 = compute_extension_from_mean(prices_1h, 20)
    ext_50 = compute_extension_from_mean(prices_1h, 50)

    # ── Pullback Analysis ──
    pullback_depth = compute_pullback_depth(prices_1h, 24)
    pullback_depth_4h = compute_pullback_depth(prices_4h, 12)

    # ── Range Position ──
    range_pos = compute_range_position(prices_1h, 48)

    # ── Momentum Slope ──
    mom_slope_1h = compute_momentum_slope(prices_1h, 8)
    mom_slope_4h = compute_momentum_slope(prices_4h, 6)

    # ── Volume Confirmation ──
    vol_confirm = compute_volume_confirmation(volumes_1h, 10)

    # ── ATR for stop/entry calculations ──
    atr_1h = compute_atr(prices_1h, 14)
    atr_4h = compute_atr(prices_4h, 14)

    # ── Regime Context — FIX 1.1: use pre-built stack if provided ──
    if stack is None:
        stack = build_regime_stack(coin, db)

    exec_label = "Neutral"
    trend_label = "Neutral"
    macro_label = "Neutral"
    coherence = 50.0
    hazard = 50.0
    alignment = 50.0

    if not stack.get("incomplete"):
        exec_label = stack["execution"]["label"] if stack.get("execution") else "Neutral"
        trend_label = stack["trend"]["label"] if stack.get("trend") else "Neutral"
        macro_label = stack["macro"]["label"] if stack.get("macro") else "Neutral"
        coherence = stack["execution"]["coherence"] if stack.get("execution") else 50.0
        hazard = stack.get("hazard") or 50.0
        alignment = stack.get("alignment") or 50.0

    regime_num = REGIME_NUMERIC.get(exec_label, 0)
    is_bullish_regime = regime_num > 0
    is_bearish_regime = regime_num < 0

    # ── Chase Risk Calculation ──
    chase_risk = 0.0
    if is_bullish_regime:
        chase_risk = (
            min(100, abs(ext_20) * 8) * 0.30 +
            (range_pos) * 0.25 +
            max(0, 100 - vol_confirm) * 0.20 +
            (hazard * 0.15) +
            max(0, -mom_slope_1h * 5) * 0.10
        )
    elif is_bearish_regime:
        chase_risk = (
            min(100, abs(ext_20) * 8) * 0.30 +
            (100 - range_pos) * 0.25 +
            max(0, 100 - vol_confirm) * 0.20 +
            (hazard * 0.15) +
            max(0, mom_slope_1h * 5) * 0.10
        )
    else:
        chase_risk = 60.0

    chase_risk = round(min(100, max(0, chase_risk)), 1)

    # ── Trend Exhaustion Score ──
    exhaustion = round(min(100, max(0,
        min(100, abs(ext_20) * 6) * 0.25 +
        max(0, -mom_slope_1h * 10 if is_bullish_regime else mom_slope_1h * 10) * 0.25 +
        hazard * 0.25 +
        max(0, 100 - coherence) * 0.15 +
        max(0, 100 - alignment) * 0.10
    )), 1)

    # ── Pullback Quality Score ──
    if is_bullish_regime:
        pullback_quality = round(min(100, max(0,
            min(100, pullback_depth * 15) * 0.30 +
            min(100, max(0, mom_slope_1h * 15 + 50)) * 0.25 +
            vol_confirm * 0.20 +
            coherence * 0.15 +
            (100 - hazard) * 0.10
        )), 1)
    elif is_bearish_regime:
        pullback_quality = round(min(100, max(0,
            min(100, (100 - range_pos) * 1.2) * 0.30 +
            min(100, max(0, -mom_slope_1h * 15 + 50)) * 0.25 +
            vol_confirm * 0.20 +
            coherence * 0.15 +
            (100 - hazard) * 0.10
        )), 1)
    else:
        pullback_quality = 30.0

    # ── Breakout Quality Score ──
    breakout_quality = round(min(100, max(0,
        (range_pos if is_bullish_regime else 100 - range_pos) * 0.25 +
        vol_confirm * 0.25 +
        coherence * 0.20 +
        alignment * 0.15 +
        (100 - hazard) * 0.15
    )), 1)

    # ── Master Setup Quality Score ──
    if is_bullish_regime:
        setup_score = round(min(100, max(0,
            (100 - chase_risk) * 0.25 +
            (100 - exhaustion) * 0.20 +
            pullback_quality * 0.20 +
            coherence * 0.15 +
            (100 - hazard) * 0.10 +
            alignment * 0.10
        )), 1)
    elif is_bearish_regime:
        setup_score = round(min(100, max(0,
            (100 - chase_risk) * 0.20 +
            exhaustion * 0.15 +
            (100 - hazard) * 0.20 +
            coherence * 0.15 +
            (100 - range_pos) * 0.15 +
            alignment * 0.15
        )), 1)
    else:
        setup_score = round(min(100, max(0,
            (100 - chase_risk) * 0.25 +
            (100 - abs(ext_20) * 5) * 0.20 +
            vol_confirm * 0.15 +
            coherence * 0.20 +
            (100 - hazard) * 0.20
        )), 1)

    # ── Setup Label ──
    if setup_score >= 80: setup_label = "Excellent Setup"
    elif setup_score >= 65: setup_label = "Good Setup"
    elif setup_score >= 50: setup_label = "Moderate Setup"
    elif setup_score >= 35: setup_label = "Weak Setup"
    else: setup_label = "Poor Setup — Wait"

    # ── Entry Mode ──
    if setup_score < 30:
        entry_mode = "No Entry"
    elif chase_risk > 75:
        entry_mode = "Wait for Pullback"
    elif pullback_quality > 65 and is_bullish_regime:
        entry_mode = "Scale In — Pullback"
    elif breakout_quality > 70 and range_pos > 85:
        entry_mode = "Breakout Entry"
    elif setup_score > 60:
        entry_mode = "Scale In"
    else:
        entry_mode = "Wait"

    # ── Optimal Entry Zone ──
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

    # ── Stop Distance Guidance ──
    if atr_1h > 0:
        tight_stop = round(atr_1h * 1.5, 2)
        normal_stop = round(atr_1h * 2.5, 2)
        wide_stop = round(atr_1h * 4.0, 2)
        stop_pct = round((normal_stop / current_price) * 100, 2) if current_price > 0 else 0
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
            "tight": tight_stop, "normal": normal_stop,
            "wide": wide_stop, "normal_pct": stop_pct,
        },
        "atr_1h": round(atr_1h, 2),
        "atr_4h": round(atr_4h, 2),
        "regime_context": {
            "execution": exec_label, "trend": trend_label, "macro": macro_label,
            "coherence": coherence, "hazard": hazard, "alignment": alignment,
        },
    }


# ─────────────────────────────────────────
# OPPORTUNITY RANKING ENGINE — FIX 1.1: pass shared stack/setup
# ─────────────────────────────────────────
def compute_opportunity_score(coin: str, db: Session) -> Optional[dict]:
    stack = build_regime_stack(coin, db)
    if stack["incomplete"]:
        return None

    regime_quality = compute_regime_quality(stack)
    regime_score = regime_quality["score"]

    # FIX 1.1: pass stack to avoid redundant build
    setup = compute_setup_quality(coin, db, stack=stack)
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
    direction_mult = 1.0 if direction == "bullish" else (0.7 if direction == "mixed" else 0.4)

    raw_score = (
        regime_score * 0.20 +
        setup_score * 0.20 +
        shift_opportunity * 0.15 +
        exposure * 0.15 +
        survival * 0.10 +
        (100 - chase_risk) * 0.10 +
        coherence * 0.10
    ) * direction_mult

    raw_score = raw_score * (1 - (hazard_penalty / 100) * 0.3)
    opportunity_score = round(min(100, max(0, raw_score)), 1)

    reasons = []
    if regime_score >= 65: reasons.append("Strong regime quality")
    elif regime_score < 40: reasons.append("Weak regime structure")
    if setup_score >= 65: reasons.append("Good entry conditions")
    elif setup_score < 35: reasons.append("Poor entry timing")
    if shift_risk > 65: reasons.append("Elevated shift risk")
    elif shift_risk < 30: reasons.append("Low shift risk")
    if coherence > 70: reasons.append("High coherence")
    if chase_risk > 70: reasons.append("High chase risk — wait for pullback")
    if hazard > 65: reasons.append("Hazard rate elevated")
    if survival > 75: reasons.append("Regime persistence strong")
    reason_str = "; ".join(reasons[:4]) if reasons else "Moderate conditions"

    return {
        "coin": coin, "opportunity_score": opportunity_score,
        "regime_quality_grade": regime_quality["grade"],
        "setup_quality_score": setup_score,
        "setup_label": setup.get("setup_label") or "—",
        "entry_mode": setup.get("entry_mode") or "—",
        "chase_risk": chase_risk, "shift_risk": shift_risk,
        "exposure_rec": exposure, "direction": direction,
        "survival": survival, "hazard": hazard,
        "coherence": coherence, "reason": reason_str,
    }


def compute_opportunity_ranking(db: Session) -> dict:
    rankings = []
    for coin in SUPPORTED_COINS:
        opp = compute_opportunity_score(coin, db)
        if opp:
            rankings.append(opp)

    rankings.sort(key=lambda x: x["opportunity_score"], reverse=True)

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
        most_defensive = min(rankings, key=lambda x: x["shift_risk"])["coin"]

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
                f"{r['coin']} has high opportunity but elevated chase risk — "
                f"wait for pullback before adding."
            )

    return {
        "rankings": rankings, "best_long": best_long,
        "most_defensive": most_defensive, "avoid": avoid,
        "rotation_signals": rotation_signals,
        "timestamp": datetime.datetime.utcnow().isoformat(),
    }


# ─────────────────────────────────────────
# HISTORICAL ANALOGS ENGINE — FIX 4.2: Match on ALL regime conditions
# ─────────────────────────────────────────
def find_historical_analogs(
    db: Session, coin: str,
    target_macro: str, target_trend: str, target_exec: str,
    target_hazard: float = 50, hazard_tolerance: float = 20,
) -> dict:
    records = (
        db.query(MarketSummary)
        .filter(MarketSummary.coin == coin, MarketSummary.timeframe == "1h")
        .order_by(MarketSummary.created_at.asc())
        .all()
    )

    if len(records) < 100:
        return {
            "coin": coin, "sample_size": 0, "data_sufficient": False,
            "message": f"Need more history. Currently {len(records)} records, need 100+.",
        }

    prices_1d, _ = get_klines(coin, "1d", limit=90)
    prices_1h, _ = get_klines(coin, "1h", limit=120)

    if len(prices_1h) < 30:
        return {
            "coin": coin, "sample_size": 0, "data_sufficient": False,
            "message": "Insufficient price data for forward return calculation.",
        }

    # FIX 4.2: Build a lookup of 4h and 1d records to match multi-timeframe conditions
    records_4h = (
        db.query(MarketSummary)
        .filter(MarketSummary.coin == coin, MarketSummary.timeframe == "4h")
        .order_by(MarketSummary.created_at.asc())
        .all()
    )
    records_1d = (
        db.query(MarketSummary)
        .filter(MarketSummary.coin == coin, MarketSummary.timeframe == "1d")
        .order_by(MarketSummary.created_at.asc())
        .all()
    )

    # Build time-indexed lookups for 4h and 1d labels
    def build_label_lookup(recs, window_hours=6):
        """Returns a function that finds the closest label to a given timestamp."""
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
                # Stop searching if we've passed the timestamp by too much
                if r.created_at > ts and (r.created_at - ts).total_seconds() > window_hours * 3600:
                    break
            return best

        return find_label

    find_4h_label = build_label_lookup(records_4h, window_hours=6)
    find_1d_label = build_label_lookup(records_1d, window_hours=26)

    matching_periods = []
    records_list = list(records)

    for i in range(len(records_list) - 24):
        r = records_list[i]

        # FIX 4.2: Match on execution label
        if r.label != target_exec:
            continue

        # FIX 4.2: Match on trend label (4h)
        trend_label_at_time = find_4h_label(r.created_at)
        if trend_label_at_time and trend_label_at_time != target_trend:
            continue

        # FIX 4.2: Match on macro label (1d)
        macro_label_at_time = find_1d_label(r.created_at)
        if macro_label_at_time and macro_label_at_time != target_macro:
            continue

        # Check forward data
        forward_prices = []
        for j in range(i, min(i + 168, len(records_list))):
            forward_prices.append(records_list[j].score)

        if len(forward_prices) >= 24:
            labels_forward = [records_list[j].label for j in range(i, min(i + 72, len(records_list)))]
            same_count = 0
            for lbl in labels_forward:
                if lbl == r.label:
                    same_count += 1
                else:
                    break

            continuation_hours = same_count

            matching_periods.append({
                "date": r.created_at.strftime("%Y-%m-%d %H:%M"),
                "label": r.label,
                "score": r.score,
                "coherence": r.coherence,
                "continuation_hours": continuation_hours,
                "matched_macro": macro_label_at_time or "unknown",
                "matched_trend": trend_label_at_time or "unknown",
            })

    if len(matching_periods) < 3:
        # FIX 4.2: Fallback — relax to exec-only matching if multi-TF match is too strict
        matching_periods = []
        for i in range(len(records_list) - 24):
            r = records_list[i]
            if r.label != target_exec:
                continue
            forward_prices = []
            for j in range(i, min(i + 168, len(records_list))):
                forward_prices.append(records_list[j].score)
            if len(forward_prices) >= 24:
                labels_forward = [records_list[j].label for j in range(i, min(i + 72, len(records_list)))]
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
            "coin": coin, "target_regime": target_exec,
            "sample_size": len(matching_periods), "data_sufficient": False,
            "match_type": match_type,
            "message": f"Only {len(matching_periods)} matching periods found. Need 5+.",
        }

    current_price = prices_1h[-1] if prices_1h else 0

    continuation_hours_list = [m["continuation_hours"] for m in matching_periods]
    avg_continuation = sum(continuation_hours_list) / len(continuation_hours_list)
    max_continuation = max(continuation_hours_list)
    min_continuation = min(continuation_hours_list)

    continued_24h = sum(1 for h in continuation_hours_list if h >= 24)
    continued_72h = sum(1 for h in continuation_hours_list if h >= 72)
    continuation_prob_24h = round((continued_24h / len(continuation_hours_list)) * 100, 1)
    continuation_prob_72h = round((continued_72h / len(continuation_hours_list)) * 100, 1)

    forward_returns = {
        "1d": {"avg": 0, "median": 0, "best": 0, "worst": 0, "positive_pct": 50},
        "3d": {"avg": 0, "median": 0, "best": 0, "worst": 0, "positive_pct": 50},
        "7d": {"avg": 0, "median": 0, "best": 0, "worst": 0, "positive_pct": 50},
    }

    if len(prices_1d) >= 10:
        daily_returns = []
        for i in range(1, len(prices_1d)):
            ret = ((prices_1d[i] - prices_1d[i - 1]) / prices_1d[i - 1]) * 100
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
                            "positive_pct": round(sum(1 for r in fwd_rets if r > 0) / len(fwd_rets) * 100, 1),
                        }

    mae_estimates = []
    if len(prices_1h) >= 48:
        for i in range(24, len(prices_1h)):
            window = prices_1h[i - 24:i]
            high = max(window)
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
            "macro": target_macro, "trend": target_trend, "execution": target_exec,
        },
        "sample_size": len(matching_periods),
        "data_sufficient": len(matching_periods) >= 5,
        "match_type": match_type,  # FIX 4.2: expose matching quality
        "continuation": {
            "avg_hours": round(avg_continuation, 1),
            "max_hours": max_continuation, "min_hours": min_continuation,
            "prob_24h_pct": continuation_prob_24h, "prob_72h_pct": continuation_prob_72h,
        },
        "forward_returns": forward_returns,
        "max_adverse_excursion": {
            "avg_pct": avg_mae, "worst_pct": worst_mae,
            "drawdown_gt_3pct_prob": dd_gt_3pct_prob, "drawdown_gt_5pct_prob": dd_gt_5pct_prob,
        },
        "matching_periods": matching_periods[:20],
        "current_price": current_price,
        "timestamp": datetime.datetime.utcnow().isoformat(),
    }


# ─────────────────────────────────────────
# BEHAVIORAL ALPHA ENGINE (unchanged logic)
# ─────────────────────────────────────────
def compute_behavioral_alpha_report(email: str, db: Session, lookback_days: int = 30) -> dict:
    cutoff = datetime.datetime.utcnow() - datetime.timedelta(days=lookback_days)

    logs = (
        db.query(ExposureLog)
        .filter(ExposureLog.email == email, ExposureLog.created_at >= cutoff)
        .order_by(ExposureLog.created_at.asc())
        .all()
    )

    entries = (
        db.query(PerformanceEntry)
        .filter(PerformanceEntry.email == email, PerformanceEntry.date >= cutoff)
        .order_by(PerformanceEntry.date.asc())
        .all()
    )

    if len(logs) < 3:
        return {
            "email": email, "ready": False,
            "message": f"Need at least 3 exposure logs. Currently have {len(logs)}.",
            "log_count": len(logs),
        }

    leaks = {lt: {"count": 0, "instances": [], "estimated_drag": 0} for lt in LEAK_TYPES}

    log_times = [l.created_at for l in logs]
    log_gaps_hours = []
    for i in range(1, len(log_times)):
        gap = (log_times[i] - log_times[i - 1]).total_seconds() / 3600
        log_gaps_hours.append(gap)

    avg_log_gap = sum(log_gaps_hours) / len(log_gaps_hours) if log_gaps_hours else 24

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
                "date": log.created_at.strftime("%b %d"), "delta": round(delta, 1),
                "hazard": hazard, "regime": regime,
            })

        if "Risk-Off" in regime and user_exp > model_exp + 15:
            leaks["overexposed_risk_off"]["count"] += 1
            leaks["overexposed_risk_off"]["estimated_drag"] += round(delta * 0.12, 2)
            leaks["overexposed_risk_off"]["instances"].append({
                "date": log.created_at.strftime("%b %d"), "user_exp": user_exp,
                "model_exp": model_exp, "regime": regime,
            })

        if hazard > 65 and delta > 10:
            leaks["ignored_hazard_spike"]["count"] += 1
            leaks["ignored_hazard_spike"]["estimated_drag"] += round(hazard * 0.05, 2)
            leaks["ignored_hazard_spike"]["instances"].append({
                "date": log.created_at.strftime("%b %d"), "hazard": hazard, "delta": round(delta, 1),
            })

        if "Risk-On" in regime and "Strong" in regime and delta < -15 and hazard < 40:
            leaks["premature_exit_strength"]["count"] += 1
            leaks["premature_exit_strength"]["estimated_drag"] += round(abs(delta) * 0.06, 2)
            leaks["premature_exit_strength"]["instances"].append({
                "date": log.created_at.strftime("%b %d"), "delta": round(delta, 1), "regime": regime,
            })

        if "Risk-Off" in regime and delta > 20:
            prev_logs = [
                pl for pl in logs
                if pl.created_at < log.created_at
                and (log.created_at - pl.created_at).total_seconds() < 86400
            ]
            if prev_logs:
                prev_delta = prev_logs[-1].user_exposure_pct - prev_logs[-1].model_exposure_pct
                if delta > prev_delta + 5:
                    leaks["averaging_down_risk_off"]["count"] += 1
                    leaks["averaging_down_risk_off"]["estimated_drag"] += round(delta * 0.15, 2)
                    leaks["averaging_down_risk_off"]["instances"].append({
                        "date": log.created_at.strftime("%b %d"), "delta": round(delta, 1), "regime": regime,
                    })

        if abs(delta) > 25:
            leaks["size_too_large"]["count"] += 1
            leaks["size_too_large"]["estimated_drag"] += round(abs(delta) * 0.04, 2)
            leaks["size_too_large"]["instances"].append({
                "date": log.created_at.strftime("%b %d"), "delta": round(delta, 1), "regime": regime,
            })

        if "Strong Risk-On" in regime and delta < -10 and hazard < 30:
            leaks["failed_to_press_edge"]["count"] += 1
            leaks["failed_to_press_edge"]["estimated_drag"] += round(abs(delta) * 0.05, 2)
            leaks["failed_to_press_edge"]["instances"].append({
                "date": log.created_at.strftime("%b %d"), "delta": round(delta, 1),
                "regime": regime, "hazard": hazard,
            })

    active_leaks = []
    total_drag = 0

    for leak_type, data in leaks.items():
        if data["count"] > 0:
            config = LEAK_TYPES[leak_type]
            weighted_drag = round(data["estimated_drag"] * config["severity_weight"], 1)
            total_drag += weighted_drag
            active_leaks.append({
                "type": leak_type, "label": config["label"],
                "description": config["description"], "frequency": data["count"],
                "estimated_alpha_drag_pct": weighted_drag,
                "severity_weight": config["severity_weight"],
                "instances": data["instances"][:5],
            })

    active_leaks.sort(key=lambda x: x["estimated_alpha_drag_pct"], reverse=True)

    strengths = []
    followed_count = sum(1 for l in logs if l.followed_model)
    follow_rate = (followed_count / len(logs)) * 100 if logs else 0

    if follow_rate > 70:
        strengths.append(f"Strong model adherence ({round(follow_rate)}% follow rate)")

    risk_off_logs = [l for l in logs if "Risk-Off" in (l.regime_label or "")]
    if risk_off_logs:
        risk_off_followed = sum(1 for l in risk_off_logs if l.followed_model)
        risk_off_rate = (risk_off_followed / len(risk_off_logs)) * 100
        if risk_off_rate > 60:
            strengths.append(f"Good defensive discipline ({round(risk_off_rate)}% in Risk-Off)")

    hazard_spike_logs = [l for l in logs if (l.hazard_at_log or 0) > 60]
    if hazard_spike_logs:
        reduced = sum(1 for l in hazard_spike_logs if (l.user_exposure_pct or 0) < (l.model_exposure_pct or 50))
        if reduced > len(hazard_spike_logs) * 0.5:
            strengths.append("Responds well to hazard spikes")

    recommendations = []
    for leak in active_leaks[:3]:
        if leak["type"] == "late_entry_chasing":
            recommendations.append("Use the Setup Quality score to avoid chasing. Wait for chase risk < 50 before entering.")
        elif leak["type"] == "overexposed_risk_off":
            recommendations.append("Set a hard rule: max exposure = model recommendation in Risk-Off. Use the Decision Engine directive.")
        elif leak["type"] == "ignored_hazard_spike":
            recommendations.append("Enable hazard alerts. When hazard > 65, reduce exposure within 1 hour regardless of conviction.")
        elif leak["type"] == "overtrading":
            recommendations.append("Limit exposure changes to once per regime shift. Check the regime stack before adjusting.")
        elif leak["type"] == "size_too_large":
            recommendations.append("Use the Portfolio Allocator to right-size positions. Stay within the exposure band.")
        elif leak["type"] == "averaging_down_risk_off":
            recommendations.append("Never add to positions in Risk-Off. The Playbook explicitly warns against this.")
        elif leak["type"] == "failed_to_press_edge":
            recommendations.append("In Strong Risk-On with low hazard, trust the model. Scale to full recommended exposure.")
        elif leak["type"] == "premature_exit_strength":
            recommendations.append("In strong regimes with low hazard, hold longer. Use survival probability to gauge regime health.")

    if total_drag <= 2: behavior_grade, behavior_label = "A", "Excellent"
    elif total_drag <= 5: behavior_grade, behavior_label = "B+", "Good"
    elif total_drag <= 10: behavior_grade, behavior_label = "B", "Above Average"
    elif total_drag <= 20: behavior_grade, behavior_label = "C", "Needs Improvement"
    else: behavior_grade, behavior_label = "D", "Significant Leaks"

    return {
        "email": email, "ready": True, "lookback_days": lookback_days,
        "log_count": len(logs), "performance_count": len(entries),
        "behavior_grade": behavior_grade, "behavior_label": behavior_label,
        "total_estimated_alpha_drag_pct": round(total_drag, 1),
        "leaks": active_leaks, "strengths": strengths,
        "recommendations": recommendations, "follow_rate": round(follow_rate, 1),
        "timestamp": datetime.datetime.utcnow().isoformat(),
    }


# ─────────────────────────────────────────
# PROBABILISTIC SCENARIOS ENGINE — FIX 1.1: accepts pre-built stack and setup
# ─────────────────────────────────────────
def compute_scenarios(coin: str, db: Session, stack: dict = None, setup: dict = None) -> dict:
    # FIX 1.1: Use pre-built stack if provided
    if stack is None:
        stack = build_regime_stack(coin, db)
    if stack.get("incomplete"):
        return {"coin": coin, "error": "Insufficient data"}

    exec_label = stack["execution"]["label"] if stack.get("execution") else "Neutral"
    trend_label = stack["trend"]["label"] if stack.get("trend") else "Neutral"
    macro_label = stack["macro"]["label"] if stack.get("macro") else "Neutral"

    hazard = stack.get("hazard") or 50
    survival = stack.get("survival") or 50
    shift_risk = stack.get("shift_risk") or 50
    alignment = stack.get("alignment") or 50
    exposure = stack.get("exposure") or 50
    direction = stack.get("direction") or "mixed"

    transitions = regime_transition_matrix(db, coin, "1h")

    # FIX 1.1: Use pre-built setup if provided
    if setup is None:
        setup = compute_setup_quality(coin, db, stack=stack)
    setup_score = setup.get("setup_quality_score") or 50
    exhaustion = setup.get("trend_exhaustion") or 50

    regime_num = REGIME_NUMERIC.get(exec_label, 0)

    # ── Base Case ──
    base_prob = round(min(75, max(25,
        survival * 0.40 + (100 - hazard) * 0.30 + alignment * 0.15 + (100 - shift_risk) * 0.15
    )), 0)

    if regime_num > 0:
        base_outcome = "Regime continuation — trend maintains current direction"
        base_exposure = f"Maintain {int(exposure * 0.9)}-{int(min(95, exposure * 1.1))}%"
        base_actions = ["Hold existing positions", "Trail stops to recent support", "Monitor hazard for acceleration"]
    elif regime_num < 0:
        base_outcome = "Risk-Off continuation — defensive positioning maintained"
        base_exposure = f"Maintain {int(max(5, exposure * 0.8))}-{int(exposure * 1.1)}%"
        base_actions = ["Stay defensive — hold cash", "No new long entries", "Monitor for capitulation signals"]
    else:
        base_outcome = "Range-bound continuation — neutral positioning"
        base_exposure = f"Maintain {int(max(5, exposure * 0.85))}-{int(min(95, exposure * 1.15))}%"
        base_actions = ["Reduce position sizes", "Wait for directional clarity", "Monitor regime stack for shifts"]

    # ── Bull Case ──
    if regime_num >= 0:
        bull_prob = round(min(45, max(5,
            (100 - hazard) * 0.25 + alignment * 0.20 + setup_score * 0.20 + (100 - exhaustion) * 0.20 + survival * 0.15
        )), 0)
        bull_outcome = "Breakout to higher — regime upgrades to stronger risk-on"
        bull_exposure = f"Increase to {int(min(95, exposure * 1.3))}-{int(min(95, exposure * 1.5))}%"
        bull_invalidation = "1h regime downgrades or hazard > 70%"
        bull_actions = ["Add on breakout confirmation", "Pyramiding valid", "Extend targets"]
    else:
        bull_prob = round(min(35, max(5,
            hazard * 0.30 + exhaustion * 0.25 + (100 - survival) * 0.25 + shift_risk * 0.20
        )), 0)
        bull_outcome = "Relief bounce — regime stabilizes and shifts toward Neutral"
        bull_exposure = f"Cautiously increase to {int(min(60, exposure * 1.5))}-{int(min(70, exposure * 2.0))}%"
        bull_invalidation = "New momentum lows or hazard re-acceleration"
        bull_actions = ["Only add if regime shifts to Neutral on all timeframes", "Small sizes — countertrend", "Tight stops"]

    # ── Bear Case ──
    if regime_num <= 0:
        bear_prob = round(min(45, max(5,
            hazard * 0.30 + shift_risk * 0.25 + (100 - alignment) * 0.20 + (100 - survival) * 0.25
        )), 0)
        bear_outcome = "Accelerated sell-off — regime deteriorates further"
        bear_exposure = f"Reduce to {int(max(0, exposure * 0.3))}-{int(max(5, exposure * 0.5))}%"
        bear_invalidation = "Strong reversal with volume and 4h regime upgrade"
        bear_actions = ["Exit remaining positions", "Move to full cash / stables", "Do not attempt to catch bottom"]
    else:
        bear_prob = round(min(40, max(5,
            hazard * 0.30 + exhaustion * 0.25 + shift_risk * 0.25 + (100 - alignment) * 0.20
        )), 0)
        bear_outcome = "Regime failure — trend breaks and shifts to Risk-Off"
        bear_exposure = f"Reduce to {int(max(5, exposure * 0.4))}-{int(max(10, exposure * 0.6))}%"
        bear_invalidation = "Structural break of 4h trend support"
        bear_actions = ["Reduce exposure immediately", "No new longs until regime stabilizes", "Move stops to breakeven on remaining positions"]

    # Normalize
    total = base_prob + bull_prob + bear_prob
    if total > 0:
        base_prob = round((base_prob / total) * 100, 0)
        bull_prob = round((bull_prob / total) * 100, 0)
        bear_prob = 100 - base_prob - bull_prob

    scenarios = [
        {"name": "Base Case", "probability": int(base_prob), "outcome": base_outcome, "exposure": base_exposure, "actions": base_actions, "invalidation": f"Hazard exceeds {int(min(100, hazard + 25))}% or regime shifts"},
        {"name": "Bull Case", "probability": int(bull_prob), "outcome": bull_outcome, "exposure": bull_exposure, "actions": bull_actions, "invalidation": bull_invalidation},
        {"name": "Bear Case", "probability": int(bear_prob), "outcome": bear_outcome, "exposure": bear_exposure, "actions": bear_actions, "invalidation": bear_invalidation},
    ]

    invalidation_triggers = []
    if hazard > 50: invalidation_triggers.append(f"Hazard at {hazard}% — approaching instability")
    if shift_risk > 55: invalidation_triggers.append(f"Shift risk at {shift_risk}% — transition pressure building")
    if exhaustion > 65: invalidation_triggers.append(f"Trend exhaustion at {exhaustion}% — momentum fading")
    if alignment < 40: invalidation_triggers.append(f"Alignment only {alignment}% — timeframes diverging")

    if regime_num > 0 and hazard < 50:
        expected_path_24h = "Continuation higher with possible shallow pullback"
        expected_path_7d = "Trend intact if hazard stays below 60%"
    elif regime_num > 0 and hazard >= 50:
        expected_path_24h = "Possible consolidation or pullback as hazard elevates"
        expected_path_7d = "Watch for regime transition — survival declining"
    elif regime_num < 0 and hazard < 50:
        expected_path_24h = "Continued weakness — bounces likely sold"
        expected_path_7d = "Risk-Off persists until capitulation signals appear"
    elif regime_num < 0 and hazard >= 50:
        expected_path_24h = "Possible stabilization attempt — but premature to buy"
        expected_path_7d = "Risk-Off regime may be exhausting — watch for Neutral shift"
    else:
        expected_path_24h = "Range-bound — no clear directional signal"
        expected_path_7d = "Wait for regime stack alignment before committing"

    return {
        "coin": coin, "scenarios": scenarios, "current_regime": exec_label,
        "direction": direction, "invalidation_triggers": invalidation_triggers,
        "expected_path": {"24h": expected_path_24h, "7d": expected_path_7d},
        "context": {
            "hazard": hazard, "survival": survival, "shift_risk": shift_risk,
            "alignment": alignment, "exhaustion": exhaustion, "setup_score": setup_score,
        },
        "timestamp": datetime.datetime.utcnow().isoformat(),
    }


# ─────────────────────────────────────────
# INTERNAL DAMAGE MONITOR — FIX 1.1: accepts pre-fetched market_data and stack
# ─────────────────────────────────────────
def compute_internal_damage(coin: str, db: Session, market_data: dict = None, stack: dict = None) -> dict:
    records_1h = (
        db.query(MarketSummary)
        .filter(MarketSummary.coin == coin, MarketSummary.timeframe == "1h")
        .order_by(MarketSummary.created_at.desc())
        .limit(24)
        .all()
    )
    records_1h.reverse()

    records_4h = (
        db.query(MarketSummary)
        .filter(MarketSummary.coin == coin, MarketSummary.timeframe == "4h")
        .order_by(MarketSummary.created_at.desc())
        .limit(12)
        .all()
    )
    records_4h.reverse()

    records_1d = (
        db.query(MarketSummary)
        .filter(MarketSummary.coin == coin, MarketSummary.timeframe == "1d")
        .order_by(MarketSummary.created_at.desc())
        .limit(7)
        .all()
    )
    records_1d.reverse()

    if len(records_1h) < 6:
        return {
            "coin": coin, "internal_damage_score": None,
            "error": "Insufficient history for damage analysis",
        }

    signals = []
    damage_components = {}

    # ── 1. Coherence Rollover ──
    if len(records_1h) >= 6:
        recent_coherence = [r.coherence for r in records_1h[-6:] if r.coherence is not None]
        if len(recent_coherence) >= 4:
            avg_recent = sum(recent_coherence[-3:]) / 3
            avg_prior = sum(recent_coherence[:3]) / 3
            coherence_decline = round(avg_prior - avg_recent, 1)
            damage_components["coherence_rollover"] = min(100, max(0, coherence_decline * 5))

            declining = all(recent_coherence[i] <= recent_coherence[i - 1] for i in range(1, len(recent_coherence)))
            if declining and coherence_decline > 5:
                signals.append({"type": "coherence_rollover", "severity": "high" if coherence_decline > 15 else "medium", "message": f"Coherence declined {coherence_decline} pts over last 6 updates", "value": coherence_decline})
            elif coherence_decline > 3:
                signals.append({"type": "coherence_weakening", "severity": "low", "message": f"Coherence weakening by {coherence_decline} pts", "value": coherence_decline})
        else:
            damage_components["coherence_rollover"] = 0
    else:
        damage_components["coherence_rollover"] = 0

    # ── 2. Momentum Divergence — FIX 1.1: use pre-fetched data ──
    if len(records_1h) >= 6:
        recent_scores = [r.score for r in records_1h[-6:]]
        if market_data and "1h" in market_data:
            prices_1h = market_data["1h"]["prices"]
        else:
            prices_1h, _ = get_klines(coin, "1h", limit=12)

        if len(prices_1h) >= 6 and len(recent_scores) >= 6:
            price_direction = prices_1h[-1] - prices_1h[-6]
            score_direction = recent_scores[-1] - recent_scores[-6]

            if price_direction > 0 and score_direction < -3:
                div_strength = abs(score_direction)
                damage_components["momentum_divergence"] = min(100, div_strength * 5)
                signals.append({"type": "bearish_divergence", "severity": "high" if div_strength > 10 else "medium", "message": f"Price rising but regime score declining ({round(score_direction, 1)} pts)", "value": round(score_direction, 1)})
            elif price_direction < 0 and score_direction > 3:
                div_strength = abs(score_direction)
                damage_components["momentum_divergence"] = min(100, div_strength * 3)
                signals.append({"type": "bullish_divergence", "severity": "medium", "message": f"Price falling but regime score improving (+{round(score_direction, 1)} pts)", "value": round(score_direction, 1)})
            else:
                damage_components["momentum_divergence"] = 0
        else:
            damage_components["momentum_divergence"] = 0
    else:
        damage_components["momentum_divergence"] = 0

    # ── 3. Timeframe Divergence — FIX 1.1: use pre-built stack ──
    if stack is None:
        stack = build_regime_stack(coin, db)
    if not stack.get("incomplete"):
        exec_num = REGIME_NUMERIC.get(stack["execution"]["label"] if stack.get("execution") else "Neutral", 0)
        trend_num = REGIME_NUMERIC.get(stack["trend"]["label"] if stack.get("trend") else "Neutral", 0)
        macro_num = REGIME_NUMERIC.get(stack["macro"]["label"] if stack.get("macro") else "Neutral", 0)

        tf_spread = max(exec_num, trend_num, macro_num) - min(exec_num, trend_num, macro_num)
        damage_components["timeframe_divergence"] = min(100, tf_spread * 25)

        if tf_spread >= 3:
            signals.append({"type": "timeframe_conflict", "severity": "high", "message": "Major timeframe disagreement — macro and execution regimes conflict", "value": tf_spread})
        elif tf_spread >= 2:
            signals.append({"type": "timeframe_tension", "severity": "medium", "message": "Timeframe tension — trend and execution regimes misaligned", "value": tf_spread})
    else:
        damage_components["timeframe_divergence"] = 0

    # ── 4. Volatility Expansion ──
    if len(records_1h) >= 8:
        recent_vol = [r.volatility_val for r in records_1h[-4:] if r.volatility_val]
        prior_vol = [r.volatility_val for r in records_1h[-8:-4] if r.volatility_val]

        if recent_vol and prior_vol:
            avg_recent_vol = sum(recent_vol) / len(recent_vol)
            avg_prior_vol = sum(prior_vol) / len(prior_vol)
            vol_expansion = ((avg_recent_vol - avg_prior_vol) / avg_prior_vol) * 100 if avg_prior_vol > 0 else 0
            damage_components["volatility_expansion"] = min(100, max(0, vol_expansion * 2))

            if vol_expansion > 30:
                signals.append({"type": "volatility_expansion", "severity": "high" if vol_expansion > 60 else "medium", "message": f"Volatility expanding {round(vol_expansion, 1)}% — instability rising", "value": round(vol_expansion, 1)})
        else:
            damage_components["volatility_expansion"] = 0
    else:
        damage_components["volatility_expansion"] = 0

    # ── 5. Score Trajectory ──
    if len(records_1h) >= 8:
        scores_recent = [r.score for r in records_1h[-4:]]
        scores_prior = [r.score for r in records_1h[-8:-4]]

        avg_recent_score = sum(scores_recent) / len(scores_recent)
        avg_prior_score = sum(scores_prior) / len(scores_prior)
        score_drift = avg_recent_score - avg_prior_score

        current_label = records_1h[-1].label if records_1h else "Neutral"
        current_num = REGIME_NUMERIC.get(current_label, 0)

        if current_num > 0 and score_drift < -3:
            damage_components["score_deterioration"] = min(100, abs(score_drift) * 5)
            signals.append({"type": "score_deterioration", "severity": "medium" if abs(score_drift) > 8 else "low", "message": f"Regime score drifting lower ({round(score_drift, 1)} pts) within bullish regime", "value": round(score_drift, 1)})
        elif current_num < 0 and score_drift > 3:
            damage_components["score_deterioration"] = min(100, abs(score_drift) * 3)
            signals.append({"type": "score_improvement", "severity": "low", "message": f"Score improving within Risk-Off ({round(score_drift, 1)} pts) — watch for regime shift", "value": round(score_drift, 1)})
        else:
            damage_components["score_deterioration"] = 0
    else:
        damage_components["score_deterioration"] = 0

    # ── Composite ──
    weights = {
        "coherence_rollover": 0.25, "momentum_divergence": 0.25,
        "timeframe_divergence": 0.20, "volatility_expansion": 0.15, "score_deterioration": 0.15,
    }
    damage_score = sum(damage_components.get(c, 0) * w for c, w in weights.items())
    damage_score = round(min(100, max(0, damage_score)), 1)

      # (continuing compute_internal_damage...)

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
        "signals": sorted(signals, key=lambda x: {"high": 0, "medium": 1, "low": 2}.get(x["severity"], 3)),
        "components": damage_components,
        "signal_count": len(signals),
        "high_severity_count": sum(1 for s in signals if s["severity"] == "high"),
        "timestamp": datetime.datetime.utcnow().isoformat(),
    }


# ─────────────────────────────────────────
# TRADE PLANNING ENGINE — FIX 1.4: Duplicate removed. Single definition.
# FIX 1.1: Accepts pre-built stack and setup to avoid redundant computation.
# ─────────────────────────────────────────
def compute_trade_plan(
    coin: str,
    account_size: float,
    strategy_mode: str,
    db: Session,
    email: str = None,
    stack: dict = None,
    setup: dict = None,
) -> dict:
    """
    Generates a complete trade plan based on current regime,
    setup quality, risk parameters, and user profile.
    FIX 1.4: This is the ONLY definition — duplicate removed.
    """
    # FIX 1.1: Use pre-built stack if provided
    if stack is None:
        stack = build_regime_stack(coin, db)
    if stack.get("incomplete"):
        return {"coin": coin, "error": "Insufficient regime data"}

    # FIX 1.1: Use pre-built setup if provided
    if setup is None:
        setup = compute_setup_quality(coin, db, stack=stack)
    setup_score = setup.get("setup_quality_score") or 50
    entry_mode = setup.get("entry_mode") or "Wait"
    chase_risk = setup.get("chase_risk") or 50
    current_price = setup.get("current_price") or 0
    atr_1h = setup.get("atr_1h") or 0

    exec_label = stack["execution"]["label"] if stack.get("execution") else "Neutral"
    trend_label = stack["trend"]["label"] if stack.get("trend") else "Neutral"
    macro_label = stack["macro"]["label"] if stack.get("macro") else "Neutral"
    exposure = stack.get("exposure") or 50
    hazard = stack.get("hazard") or 50
    shift_risk = stack.get("shift_risk") or 50
    survival = stack.get("survival") or 50

    regime_num = REGIME_NUMERIC.get(exec_label, 0)

    # ── User Profile Adjustments ──
    risk_mult = 1.0
    archetype_config = ARCHETYPE_CONFIG.get(strategy_mode, ARCHETYPE_CONFIG["swing"])

    if email:
        profile = db.query(UserProfile).filter(UserProfile.email == email).first()
        if profile:
            risk_mult = profile.risk_multiplier or 1.0

    adjusted_exposure = round(min(95, max(5, exposure * risk_mult * archetype_config["exposure_mult"])), 1)

    # ── Bias ──
    if regime_num >= 1:
        bias = "Long"
    elif regime_num <= -1:
        bias = "Short / Cash"
    else:
        bias = "Neutral / Reduced"

    # ── Allocation Band ──
    band_low = round(max(5, adjusted_exposure * 0.75), 0)
    band_high = round(min(95, adjusted_exposure * 1.25), 0)
    allocation_band = f"{int(band_low)}-{int(band_high)}%"

    # ── Entry Style ──
    if chase_risk > 70:
        entry_style = "Wait for Pullback"
    elif setup_score > 65 and regime_num > 0:
        entry_style = "Pullback — Scale In"
    elif setup_score > 75 and setup.get("range_position", 0) > 85:
        entry_style = "Breakout"
    elif regime_num < 0:
        entry_style = "No Long Entry"
    else:
        entry_style = "Wait for Setup Quality > 60"

    # ── Tranches ──
    tranches = archetype_config["typical_tranches"]
    deployed_capital = round(account_size * adjusted_exposure / 100, 2)
    tranche_amounts = [round(deployed_capital * (t / 100), 2) for t in tranches]

    # ── Stop Location ──
    stop_mult = archetype_config["stop_width_mult"]
    if atr_1h > 0 and current_price > 0:
        if regime_num >= 0:
            stop_price = round(current_price - atr_1h * 2.5 * stop_mult, 2)
            stop_pct = round(((current_price - stop_price) / current_price) * 100, 2)
        else:
            stop_price = round(current_price + atr_1h * 2.5 * stop_mult, 2)
            stop_pct = round(((stop_price - current_price) / current_price) * 100, 2)
    else:
        stop_price = 0
        stop_pct = 3.0

    # ── Invalidation Logic ──
    invalidation_conditions = []
    if regime_num > 0:
        invalidation_conditions.append("Execution regime shifts to Risk-Off")
        invalidation_conditions.append(f"Hazard rate exceeds {int(min(100, hazard + 30))}%")
        invalidation_conditions.append(f"Price breaks below {stop_price}")
        invalidation_conditions.append("4h trend regime downgrades")
    elif regime_num < 0:
        invalidation_conditions.append("Execution regime shifts to Risk-On")
        invalidation_conditions.append(f"Price breaks above {round(current_price + atr_1h * 3, 2) if atr_1h else 'resistance'}")
    else:
        invalidation_conditions.append("Regime stack aligns directionally")
        invalidation_conditions.append("Hazard rate exceeds 70%")

    # ── Profit Taking Rules ──
    profit_rules = []
    if regime_num > 0:
        if atr_1h > 0:
            tp1 = round(current_price + atr_1h * 2.0, 2)
            tp2 = round(current_price + atr_1h * 4.0, 2)
            profit_rules.append(f"Trim 25% at {tp1} (+{round(atr_1h * 2, 2)})")
            profit_rules.append(f"Trim 25% at {tp2} (+{round(atr_1h * 4, 2)})")
            profit_rules.append(f"Trail remaining with stop at {round(current_price + atr_1h * 1.0, 2)}")
        else:
            profit_rules.append("Trim 25% on first extension")
            profit_rules.append("Trim 25% on second extension")
            profit_rules.append("Trail remaining under 4h trend support")
    elif regime_num < 0:
        profit_rules.append("No long profit targets — defensive mode")
        profit_rules.append("Cover shorts on oversold signal or regime upgrade")
    else:
        profit_rules.append("Take quick profits on any 2-3% move")
        profit_rules.append("No holding through Neutral — reduce on strength")

    # ── Conditional Actions ──
    conditional = []
    if regime_num > 0:
        conditional.append({"condition": "Price pulls back 2-3%", "action": "Deploy next tranche"})
        conditional.append({"condition": "Hazard exceeds 65%", "action": "Tighten stops and reduce by 20%"})
        conditional.append({"condition": "Regime shifts to Neutral", "action": "Close 50% and trail remainder"})
        conditional.append({"condition": "Shift risk exceeds 75%", "action": "Reduce to minimum allocation"})
    elif regime_num < 0:
        conditional.append({"condition": "Regime upgrades to Neutral", "action": "Scout small positions with tight stops"})
        conditional.append({"condition": "Capitulation signals appear", "action": "Begin deploying first tranche cautiously"})
    else:
        conditional.append({"condition": "Regime stack aligns bullish", "action": "Deploy first two tranches"})
        conditional.append({"condition": "Regime stack aligns bearish", "action": "Move to full cash"})

    # ── Time Horizon ──
    max_hold = archetype_config["max_hold_days"]
    avg_regime_dur = average_regime_duration(db, coin, "1h")
    estimated_hold = round(min(max_hold, avg_regime_dur / 24 * 1.5), 0)

    # ── Risk Per Trade ──
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
            "execution": exec_label, "trend": trend_label, "macro": macro_label,
            "hazard": hazard, "shift_risk": shift_risk, "survival": survival,
        },
        "archetype": archetype_config["label"],
        "timestamp": datetime.datetime.utcnow().isoformat(),
    }


# ─────────────────────────────────────────
# EVENT-AWARE RISK OVERLAY — FIX 4.1: Removed fake calendar simulation
# Now clearly labels events as "estimated schedule" with disclaimer
# ─────────────────────────────────────────
def compute_event_risk_overlay(coin: str, db: Session, stack: dict = None) -> dict:
    """
    FIX 4.1: Removed modular arithmetic fake event proximity.
    Now uses a static known-schedule approach with clear disclaimers.
    In production, integrate a real economic calendar API (e.g. TradingEconomics).
    FIX 1.1: Accepts pre-built stack.
    """
    if stack is None:
        stack = build_regime_stack(coin, db)
    exposure = stack.get("exposure") or 50 if not stack.get("incomplete") else 50
    hazard = stack.get("hazard") or 50 if not stack.get("incomplete") else 50

    now = datetime.datetime.utcnow()

    # ── Known recurring event schedule (approximate) ──
    # These are rough estimates. For production, integrate a real calendar API.
    KNOWN_SCHEDULE = {
        "FOMC Meeting": {"day_of_month_range": (12, 16), "months": [1, 3, 5, 6, 7, 9, 11, 12]},
        "CPI Release": {"day_of_month_range": (10, 14), "months": list(range(1, 13))},
        "Options Expiry": {"day_of_month_range": (25, 28), "months": list(range(1, 13))},
        "ETF Flow Report": {"weekday": 4, "months": list(range(1, 13))},  # Fridays
        "PCE Inflation": {"day_of_month_range": (26, 31), "months": list(range(1, 13))},
        "Fed Minutes": {"day_of_month_range": (18, 22), "months": [1, 2, 4, 5, 7, 8, 10, 11]},
        "Jobs Report (NFP)": {"day_of_month_range": (1, 7), "months": list(range(1, 13))},
        "Quarterly GDP": {"day_of_month_range": (25, 30), "months": [1, 4, 7, 10]},
    }

    active_events = []
    for event in DYNAMIC_RISK_EVENTS:
        schedule = KNOWN_SCHEDULE.get(event["name"])
        if not schedule:
            continue

        # Estimate hours until next occurrence
        hours_until = None

        if "weekday" in schedule:
            # Weekly event — find next occurrence of that weekday
            target_wd = schedule["weekday"]
            days_ahead = (target_wd - now.weekday()) % 7
            if days_ahead == 0:
                hours_until = max(1, 24 - now.hour)
            else:
                hours_until = days_ahead * 24
        elif "day_of_month_range" in schedule:
            low, high = schedule["day_of_month_range"]
            if now.month in schedule.get("months", []):
                if now.day < low:
                    hours_until = (low - now.day) * 24
                elif now.day <= high:
                    hours_until = max(1, (high - now.day) * 24 + (24 - now.hour))
                else:
                    # Next month
                    hours_until = (30 - now.day + low) * 24
            else:
                # Find next qualifying month
                for offset in range(1, 13):
                    check_month = ((now.month - 1 + offset) % 12) + 1
                    if check_month in schedule.get("months", []):
                        hours_until = offset * 30 * 24 + low * 24
                        break

        if hours_until is not None:
            active_events.append({
                **event,
                "hours_until": int(hours_until),
            })

    active_events.sort(key=lambda x: x["hours_until"])

    imminent = [e for e in active_events if e["hours_until"] <= 48]
    upcoming = [e for e in active_events if 48 < e["hours_until"] <= 168]

    if imminent:
        max_vol_mult = max(e["typical_vol_multiplier"] for e in imminent)
        max_survival_impact = min(e["regime_survival_impact"] for e in imminent)
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

    adjusted_exposure = round(max(5, min(95, exposure + exposure_adjustment)), 1)

    survival_current = stack.get("survival") or 50 if not stack.get("incomplete") else 50
    survival_adjusted = round(max(0, survival_current + max_survival_impact), 1)

    event_guidance = []
    for e in imminent[:3]:
        if e["impact"] == "High":
            event_guidance.append({
                "event": e["name"], "hours_until": e["hours_until"],
                "action": f"Reduce position size by 15-25% ahead of {e['name']}",
                "volatility_multiplier": e["typical_vol_multiplier"],
                "stop_guidance": "Widen stops by 50% or reduce size equivalent",
            })
        elif e["impact"] == "Medium":
            event_guidance.append({
                "event": e["name"], "hours_until": e["hours_until"],
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
        # FIX 4.1: Clear disclaimer about event schedule accuracy
        "schedule_disclaimer": "Event times are estimated from known recurring schedules. For precise timing, verify with an official economic calendar.",
        "imminent_events": [
            {"name": e["name"], "type": e["type"], "impact": e["impact"], "hours_until": e["hours_until"], "vol_multiplier": e["typical_vol_multiplier"]}
            for e in imminent[:5]
        ],
        "upcoming_events": [
            {"name": e["name"], "type": e["type"], "impact": e["impact"], "hours_until": e["hours_until"]}
            for e in upcoming[:5]
        ],
        "event_guidance": event_guidance,
        "timestamp": datetime.datetime.utcnow().isoformat(),
    }


# ─────────────────────────────────────────
# TRADER ARCHETYPE PERSONALIZATION — FIX 1.1: accepts pre-built stack
# ─────────────────────────────────────────
def apply_archetype_overlay(coin: str, archetype: str, db: Session, email: str = None, stack: dict = None) -> dict:
    config = ARCHETYPE_CONFIG.get(archetype, ARCHETYPE_CONFIG["swing"])

    if stack is None:
        stack = build_regime_stack(coin, db)
    if stack.get("incomplete"):
        return {"coin": coin, "error": "Insufficient data", "archetype": archetype}

    base_exposure = stack.get("exposure") or 50
    hazard = stack.get("hazard") or 50
    shift_risk = stack.get("shift_risk") or 50
    survival = stack.get("survival") or 50
    exec_label = stack["execution"]["label"] if stack.get("execution") else "Neutral"

    adjusted_exposure = round(min(95, max(5, base_exposure * config["exposure_mult"])), 1)

    if config["alert_sensitivity"] == "high":
        alert_shift_risk_threshold = 55
        alert_hazard_threshold = 50
    elif config["alert_sensitivity"] == "low":
        alert_shift_risk_threshold = 80
        alert_hazard_threshold = 70
    else:
        alert_shift_risk_threshold = 70
        alert_hazard_threshold = 60

    should_alert = shift_risk >= alert_shift_risk_threshold or hazard >= alert_hazard_threshold
    preferred_tf = config["preferred_timeframe"]
    pb = PLAYBOOK_DATA.get(exec_label, PLAYBOOK_DATA["Neutral"])

    archetype_actions = []
    regime_num = REGIME_NUMERIC.get(exec_label, 0)

    if archetype == "leverage":
        if hazard > 50:
            archetype_actions.append("⚠ Reduce leverage immediately — hazard elevated")
        if regime_num <= 0:
            archetype_actions.append("No leveraged longs in this regime")
        if shift_risk > 60:
            archetype_actions.append("Close leveraged positions — shift risk too high")
    elif archetype == "spot_allocator":
        if regime_num > 0 and hazard < 40:
            archetype_actions.append("Good conditions for DCA allocation")
        elif regime_num < 0:
            archetype_actions.append("Pause DCA — accumulate cash for better entry")
        else:
            archetype_actions.append("Reduce DCA amount — neutral conditions")
    elif archetype == "tactical":
        if shift_risk > 55:
            archetype_actions.append("Actively de-risk — shift risk rising")
        if hazard > 55:
            archetype_actions.append("Tighten all stops by 25%")
        if regime_num > 0 and hazard < 35:
            archetype_actions.append("Tactical add on pullback — conditions favorable")
    elif archetype == "position":
        if regime_num > 0 and survival > 70:
            archetype_actions.append("Hold — regime persistence strong")
        elif hazard > 60:
            archetype_actions.append("Begin scaling out — hazard approaching critical")
        else:
            archetype_actions.append("Monitor daily regime — no change needed")
    else:
        archetype_actions.extend(pb["actions"][:3])

    return {
        "coin": coin, "archetype": archetype,
        "archetype_label": config["label"], "description": config["description"],
        "base_exposure": base_exposure, "adjusted_exposure": adjusted_exposure,
        "exposure_multiplier": config["exposure_mult"],
        "preferred_timeframe": preferred_tf, "max_hold_days": config["max_hold_days"],
        "stop_width_multiplier": config["stop_width_mult"],
        "alert_sensitivity": config["alert_sensitivity"],
        "should_alert_now": should_alert,
        "alert_thresholds": {"shift_risk": alert_shift_risk_threshold, "hazard": alert_hazard_threshold},
        "archetype_actions": archetype_actions, "playbook_bias": config["playbook_bias"],
        "regime_context": {"execution": exec_label, "hazard": hazard, "shift_risk": shift_risk, "survival": survival},
        "timestamp": datetime.datetime.utcnow().isoformat(),
    }


# ─────────────────────────────────────────
# WHAT CHANGED INTELLIGENCE BRIEF (unchanged logic)
# ─────────────────────────────────────────
def compute_what_changed(db: Session, lookback_hours: int = 24) -> dict:
    cutoff = datetime.datetime.utcnow() - datetime.timedelta(hours=lookback_hours)

    changes = []
    current_states = {}

    for coin in SUPPORTED_COINS:
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
                .filter(MarketSummary.coin == coin, MarketSummary.timeframe == tf, MarketSummary.created_at <= cutoff)
                .order_by(MarketSummary.created_at.desc())
                .first()
            )
            current_record = (
                db.query(MarketSummary)
                .filter(MarketSummary.coin == coin, MarketSummary.timeframe == tf)
                .order_by(MarketSummary.created_at.desc())
                .first()
            )

            if prev_record and current_record and prev_record.label != current_record.label:
                prev_num = REGIME_NUMERIC.get(prev_record.label, 0)
                curr_num = REGIME_NUMERIC.get(current_record.label, 0)
                direction = "upgraded" if curr_num > prev_num else "downgraded"
                severity = "positive" if curr_num > prev_num else "negative"
                tf_label = TIMEFRAME_LABELS.get(tf, tf)

                changes.append({
                    "coin": coin, "timeframe": tf, "timeframe_label": tf_label,
                    "previous": prev_record.label, "current": current_record.label,
                    "direction": direction, "severity": severity,
                    "score_change": round(current_record.score - prev_record.score, 2),
                    "message": f"{coin} {tf_label} regime {direction}: {prev_record.label} → {current_record.label}",
                })

    exposure_changes = []
    for coin, state in current_states.items():
        if state["shift_risk"] and state["shift_risk"] > 65:
            exposure_changes.append({
                "coin": coin, "type": "risk_warning",
                "message": f"{coin} shift risk at {state['shift_risk']}% — elevated",
            })

    breadth = compute_market_breadth(db)

    upgrade_count = sum(1 for c in changes if c["direction"] == "upgraded")
    downgrade_count = sum(1 for c in changes if c["direction"] == "downgraded")

    if not changes:
        headline = "No regime changes in the last 24 hours"
        tone = "stable"
    elif upgrade_count > downgrade_count:
        headline = f"{upgrade_count} regime upgrades vs {downgrade_count} downgrades — market improving"
        tone = "improving"
    elif downgrade_count > upgrade_count:
        headline = f"{downgrade_count} regime downgrades vs {upgrade_count} upgrades — market deteriorating"
        tone = "deteriorating"
    else:
        headline = f"{len(changes)} regime changes — mixed signals"
        tone = "mixed"

    takeaways = []
    high_impact_changes = [c for c in changes if c["timeframe"] in ("1d", "4h")]
    if high_impact_changes:
        for c in high_impact_changes[:3]:
            takeaways.append(c["message"])
    else:
        takeaways.append("No major timeframe changes — short-term noise only")

    if breadth.get("breadth_score", 0) > 50:
        takeaways.append(f"Market breadth bullish ({breadth['breadth_score']})")
    elif breadth.get("breadth_score", 0) < -50:
        takeaways.append(f"Market breadth bearish ({breadth['breadth_score']})")

    return {
        "lookback_hours": lookback_hours, "headline": headline, "tone": tone,
        "changes": changes, "change_count": len(changes),
        "upgrades": upgrade_count, "downgrades": downgrade_count,
        "exposure_warnings": exposure_changes, "breadth": breadth,
        "takeaways": takeaways, "current_states": current_states,
        "timestamp": datetime.datetime.utcnow().isoformat(),
    }


# ─────────────────────────────────────────
# DYNAMIC ALERT ENGINE — FIX 1.1: accepts pre-built stack
# ─────────────────────────────────────────
def evaluate_dynamic_alerts(email: str, db: Session) -> list:
    user = db.query(User).filter(User.email == email).first()
    if not user or user.subscription_status != "active":
        return []

    thresholds = (
        db.query(AlertThreshold)
        .filter(AlertThreshold.email == email, AlertThreshold.enabled == True)
        .all()
    )

    if not thresholds:
        thresholds = []
        for coin in SUPPORTED_COINS:
            thresholds.append(AlertThreshold(
                email=email, coin=coin,
                shift_risk_threshold=70, exposure_change_threshold=10,
                setup_quality_threshold=70, regime_quality_threshold=50,
            ))

    alerts = []

    for threshold in thresholds:
        coin = threshold.coin
        stack = build_regime_stack(coin, db)
        if stack.get("incomplete"):
            continue

        shift_risk = stack.get("shift_risk") or 0
        hazard = stack.get("hazard") or 0
        exposure = stack.get("exposure") or 50

        # ── Shift Risk Alert ──
        if shift_risk >= threshold.shift_risk_threshold:
            alerts.append({
                "type": "shift_risk_elevated", "coin": coin,
                "severity": "high" if shift_risk > 80 else "medium",
                "message": f"{coin} shift risk at {shift_risk}% — exceeds your threshold of {threshold.shift_risk_threshold}%",
                "action": f"Consider reducing {coin} exposure to {int(max(5, exposure * 0.7))}%",
                "value": shift_risk, "threshold": threshold.shift_risk_threshold,
            })

        # ── Setup Quality Alert — FIX 1.1: pass stack ──
        setup = compute_setup_quality(coin, db, stack=stack)
        setup_score = setup.get("setup_quality_score") or 0

        if setup_score >= threshold.setup_quality_threshold:
            alerts.append({
                "type": "setup_quality_upgraded", "coin": coin, "severity": "positive",
                "message": f"{coin} setup quality upgraded to {setup_score} — {setup.get('setup_label', '')}. Entry mode: {setup.get('entry_mode', 'Wait')}",
                "action": f"Consider entering {coin} per trade plan",
                "value": setup_score, "threshold": threshold.setup_quality_threshold,
            })

        # ── Regime Quality Alert ──
        quality = compute_regime_quality(stack)
        if quality["score"] < threshold.regime_quality_threshold:
            alerts.append({
                "type": "regime_quality_degraded", "coin": coin, "severity": "medium",
                "message": f"{coin} regime quality dropped to {quality['grade']} ({quality['score']}) — {quality['structural']}",
                "action": "Reduce exposure and tighten stops",
                "value": quality["score"], "threshold": threshold.regime_quality_threshold,
            })

        # ── Exposure Misalignment Alert ──
        recent_log = (
            db.query(ExposureLog)
            .filter(ExposureLog.email == email, ExposureLog.coin == coin)
            .order_by(ExposureLog.created_at.desc())
            .first()
        )
        if recent_log:
            user_exp = recent_log.user_exposure_pct or 0
            delta = abs(user_exp - exposure)
            if delta > threshold.exposure_change_threshold + 10:
                alerts.append({
                    "type": "exposure_misalignment", "coin": coin, "severity": "medium",
                    "message": f"Your {coin} exposure ({user_exp}%) is {round(delta, 1)}% away from model recommendation ({exposure}%)",
                    "action": f"Adjust toward {exposure}% recommended exposure",
                    "value": delta, "threshold": threshold.exposure_change_threshold,
                })

        # ── Internal Damage Alert — FIX 1.1: pass stack ──
        damage = compute_internal_damage(coin, db, stack=stack)
        if damage.get("internal_damage_score") and damage["internal_damage_score"] > 60:
            alerts.append({
                "type": "internal_damage", "coin": coin,
                "severity": "high" if damage["internal_damage_score"] > 75 else "medium",
                "message": f"{coin} internal damage score: {damage['internal_damage_score']} ({damage['damage_label']})",
                "action": damage["damage_message"],
                "value": damage["internal_damage_score"],
                "signals": [s["message"] for s in damage.get("signals", [])[:3]],
            })

    return alerts


# ─────────────────────────────────────────
# DECISION ENGINE (unchanged)
# ─────────────────────────────────────────
def compute_decision_score(
    hazard: float, shift_risk: float, alignment: float,
    survival: float, breadth_score: float, maturity_pct: float,
) -> dict:
    breadth_norm = (breadth_score + 100) / 2
    survival_score = survival
    safety_score = 100 - hazard
    shift_score = 100 - shift_risk
    maturity_score = 100 - maturity_pct
    breadth_bullish = breadth_norm

    decision_score = round(
        survival_score * 0.25 + safety_score * 0.25 + shift_score * 0.20 +
        alignment * 0.15 + maturity_score * 0.10 + breadth_bullish * 0.05, 1
    )
    decision_score = min(100, max(0, decision_score))

    if decision_score >= 80:
        directive, action, color = "Increase Exposure", "aggressive", "emerald"
        description = "All signals aligned bullish. Regime is healthy and persistent."
        actions = ["Add to existing positions on pullbacks", "Increase position size toward upper band", "Trail stops to lock in gains", "Monitor for breadth confirmation"]
    elif decision_score >= 60:
        directive, action, color = "Maintain Exposure", "hold", "green"
        description = "Regime intact. No action required. Stay the course."
        actions = ["Hold current positions", "No new leverage", "Monitor hazard rate for changes", "Re-evaluate if shift risk exceeds 60%"]
    elif decision_score >= 40:
        directive, action, color = "Trim Exposure", "trim", "yellow"
        description = "Regime showing early deterioration. Reduce risk selectively."
        actions = ["Reduce position size by 15–25%", "Avoid adding new breakout entries", "Take partial profits on extended positions", "Tighten stop losses"]
    elif decision_score >= 20:
        directive, action, color = "Switch to Defensive", "defensive", "orange"
        description = "Multiple deterioration signals active. Reduce exposure significantly."
        actions = ["Reduce exposure to lower band immediately", "No new long entries", "Move profits to cash or stables", "Wait for regime confirmation before re-entering"]
    else:
        directive, action, color = "Risk-Off — Exit", "exit", "red"
        description = "Regime breakdown in progress. Capital preservation is the priority."
        actions = ["Exit or heavily reduce all positions", "Move to maximum cash allocation", "Do not average down", "Wait for full regime reset before re-entry"]

    return {
        "score": decision_score, "directive": directive, "action": action,
        "color": color, "description": description, "actions": actions,
        "components": {
            "survival": round(survival_score, 1), "safety": round(safety_score, 1),
            "shift": round(shift_score, 1), "alignment": round(alignment, 1),
            "maturity": round(maturity_score, 1), "breadth": round(breadth_bullish, 1),
        },
    }


# ─────────────────────────────────────────
# IF NOTHING PANEL (unchanged)
# ─────────────────────────────────────────
def compute_if_nothing_panel(
    user_exposure: float, model_exposure: float,
    hazard: float, shift_risk: float, regime_label: str,
) -> dict:
    delta = user_exposure - model_exposure
    over_exposed = delta > 0
    delta_abs = abs(round(delta, 1))

    base_dd_prob = round((hazard * 0.5 + shift_risk * 0.5), 1)
    exposure_multiplier = 1 + (delta / 100) * 0.8 if over_exposed else 1.0
    adj_dd_prob = round(min(95, base_dd_prob * exposure_multiplier), 1)
    dd_prob_increase = round(adj_dd_prob - base_dd_prob, 1)
    dd_magnitude = round((hazard / 100) * 0.25 * 100, 1)
    expected_loss_pct = round((user_exposure / 100) * (dd_magnitude / 100) * 100, 1)
    model_loss_pct = round((model_exposure / 100) * (dd_magnitude / 100) * 100, 1)

    if over_exposed and delta_abs > 15:
        severity = "high"
        message = f"You are {delta_abs}% over regime tolerance"
        sub = "Maintaining this exposure significantly increases drawdown probability."
    elif over_exposed and delta_abs > 5:
        severity = "medium"
        message = f"You are {delta_abs}% above optimal"
        sub = "Small overexposure — consider trimming on the next strength."
    elif not over_exposed:
        severity = "low"
        message = f"You are {delta_abs}% below optimal — room to add"
        sub = "Consider scaling in on the next pullback if signals hold."
    else:
        severity = "low"
        message = "Exposure aligned with regime recommendation"
        sub = "No action required."

    return {
        "user_exposure": round(user_exposure, 1), "model_exposure": round(model_exposure, 1),
        "delta": round(delta, 1), "delta_abs": delta_abs,
        "over_exposed": over_exposed, "severity": severity,
        "message": message, "sub": sub,
        "drawdown_prob": adj_dd_prob, "dd_prob_increase": dd_prob_increase,
        "expected_loss_pct": expected_loss_pct, "model_loss_pct": model_loss_pct,
        "dd_magnitude_est": dd_magnitude, "regime_label": regime_label,
    }


# ─────────────────────────────────────────
# DISCIPLINE SCORING ENGINE (unchanged)
# ─────────────────────────────────────────
def compute_discipline_score(logs: list) -> dict:
    if not logs:
        return {
            "score": None, "label": "No data yet", "flags": [],
            "summary": "Log your exposure to start tracking discipline.",
        }

    total_logs = len(logs)
    followed = sum(1 for l in logs if l.followed_model)
    base_score = round((followed / total_logs) * 100, 1) if total_logs > 0 else 50
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
            flags.append({"type": "penalty", "label": "Added leverage in elevated hazard", "date": log.created_at.strftime("%b %d"), "regime": log.regime_label})
            penalties += 10
        if "Risk-Off" in (log.regime_label or "") and user_exp > model_exp + 15:
            flags.append({"type": "penalty", "label": "Over-exposed in Risk-Off regime", "date": log.created_at.strftime("%b %d"), "regime": log.regime_label})
            penalties += 15
        if shift_risk > 70 and delta < -5:
            flags.append({"type": "bonus", "label": "Reduced exposure on hazard spike", "date": log.created_at.strftime("%b %d"), "regime": log.regime_label})
            bonuses += 10
        if "Strong Risk-On" in (log.regime_label or "") and abs(delta) < 10:
            flags.append({"type": "bonus", "label": "Stayed within band in strong regime", "date": log.created_at.strftime("%b %d"), "regime": log.regime_label})
            bonuses += 5

    final_score = round(min(100, max(0, base_score + bonuses - penalties)), 1)

    if final_score >= 85: label = "Excellent"
    elif final_score >= 70: label = "Good"
    elif final_score >= 50: label = "Average"
    elif final_score >= 30: label = "Needs Work"
    else: label = "Poor"

    return {
        "score": final_score, "label": label, "flags": flags[-10:],
        "followed": followed, "total": total_logs,
        "bonuses": bonuses, "penalties": penalties,
        "summary": f"You followed the model {followed}/{total_logs} times.",
    }


def compute_performance_comparison(entries: list) -> dict:
    if len(entries) < 3:
        return {
            "user_total_return": None, "model_total_return": None,
            "alpha": None, "periods": len(entries),
            "message": "Need at least 3 entries for comparison.",
        }

    user_returns = [e.user_return_pct for e in entries if e.user_return_pct is not None]
    model_returns = [e.model_return_pct for e in entries if e.model_return_pct is not None]

    if not user_returns or not model_returns:
        return {"user_total_return": None, "model_total_return": None, "alpha": None}

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
        if e.user_return_pct is not None: regime_perf[label]["user"].append(e.user_return_pct)
        if e.model_return_pct is not None: regime_perf[label]["model"].append(e.model_return_pct)

    regime_summary = {}
    for label, data in regime_perf.items():
        if data["user"] and data["model"]:
            regime_summary[label] = {
                "user_avg": round(sum(data["user"]) / len(data["user"]), 2),
                "model_avg": round(sum(data["model"]) / len(data["model"]), 2),
                "count": len(data["user"]),
            }

    best_regime = max(regime_summary.items(), key=lambda x: x[1]["user_avg"], default=(None, {}))
    worst_regime = min(regime_summary.items(), key=lambda x: x[1]["user_avg"], default=(None, {}))

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
            "regime": e.regime_label or "—",
        })

    return {
        "user_total_return": user_total, "model_total_return": model_total,
        "alpha": alpha, "periods": len(entries),
        "regime_breakdown": regime_summary,
        "best_regime": best_regime[0], "worst_regime": worst_regime[0],
        "curve": curve,
        "message": f"Following ChainPulse would have returned {model_total:+.1f}%. Your actual: {user_total:+.1f}%.",
    }


# ─────────────────────────────────────────
# MISTAKE REPLAY ENGINE (unchanged)
# ─────────────────────────────────────────
def compute_mistake_replay(logs: list, db: Session, coin: str) -> list:
    replays = []
    for log in logs:
        hazard = log.hazard_at_log or 0
        shift_risk = log.shift_risk_at_log or 0
        user_exp = log.user_exposure_pct or 0
        model_exp = log.model_exposure_pct or 50
        delta = user_exp - model_exp
        regime = log.regime_label or "Neutral"

        if (hazard > 55 or shift_risk > 60) and abs(delta) > 12:
            severity = "high" if (hazard > 70 or shift_risk > 75) and abs(delta) > 20 else "medium" if abs(delta) > 15 else "low"
            direction = "over-exposed" if delta > 0 else "under-exposed"
            replays.append({
                "date": log.created_at.strftime("%b %d, %Y"),
                "regime": regime, "hazard": hazard, "shift_risk": shift_risk,
                "user_exp": user_exp, "model_exp": model_exp,
                "delta": round(delta, 1), "direction": direction,
                "severity": severity,
                "message": f"You were {direction} by {abs(round(delta, 1))}% while hazard was {hazard}% in {regime} regime.",
                "signals_at_time": {"hazard": hazard, "shift_risk": shift_risk, "alignment": log.alignment_at_log or 0},
            })

    return sorted(replays, key=lambda x: x["severity"] == "high", reverse=True)[:10]


# ─────────────────────────────────────────
# UPDATE ENGINE — FIX 1.1: accepts optional market_data
# ─────────────────────────────────────────
def update_market(coin: str, timeframe: str, db: Session, market_data: dict = None):
    result = calculate_score_for_timeframe(coin, timeframe, market_data=market_data)
    if result is None:
        logger.warning(f"Insufficient data for {coin}/{timeframe}")
        return None
    entry = MarketSummary(
        coin=coin, timeframe=timeframe,
        score=result["score"], label=classify(result["score"]),
        coherence=result["coherence"],
        momentum_4h=result["mom_short"], momentum_24h=result["mom_long"],
        volatility_val=result["volatility"],
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


# ─────────────────────────────────────────
# Daily Email Template (unchanged)
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
      <div style="font-size:28px;font-weight:600;margin-top:4px;">{exposure}%</div>
    </div>
    <div style="margin-top:24px;">
      <div style="font-size:14px;color:#9ca3af;">Shift Risk</div>
      <div style="font-size:22px;font-weight:600;margin-top:4px;">{shift_risk}%</div>
    </div>
    <div style="margin-top:24px;">
      <div style="font-size:14px;color:#9ca3af;">Directive</div>
      <div style="font-size:18px;font-weight:600;margin-top:4px;">{directive}</div>
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


def welcome_email_html(email: str, access_token: str) -> str:
    # FIX 1.3: Token is still in URL for convenience, but tokens now expire after 90 days
    url = f"{FRONTEND_URL}/app?token={access_token}"
    return f"""
<div style="font-family:sans-serif;max-width:560px;margin:0 auto;
     background:#000;color:#fff;padding:40px;">
  <div style="font-size:11px;color:#555;text-transform:uppercase;
       letter-spacing:2px;margin-bottom:16px;">ChainPulse Pro</div>
  <h1 style="font-size:24px;margin-bottom:8px;">Your Pro Access Is Active</h1>
  <p style="color:#999;margin-bottom:32px;">
    Click below to open your Pro dashboard. This link logs you in automatically. Bookmark it.
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
    macro_l = stack["macro"]["label"] if stack.get("macro") else "—"
    trend_l = stack["trend"]["label"] if stack.get("trend") else "—"
    exec_l = stack["execution"]["label"] if stack.get("execution") else "—"
    align = stack.get("alignment", 0)
    shift_risk = stack.get("shift_risk", 0)
    exposure = stack.get("exposure", 0)
    pb = PLAYBOOK_DATA.get(exec_l, PLAYBOOK_DATA["Neutral"])

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
      <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;color:#555;font-size:12px;">Macro (1D)</td>
      <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;color:#fff;text-align:right;">{macro_l}</td>
    </tr>
    <tr>
      <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;color:#555;font-size:12px;">Trend (4H)</td>
      <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;color:#fff;text-align:right;">{trend_l}</td>
    </tr>
    <tr>
      <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;color:#555;font-size:12px;">Execution (1H)</td>
      <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;color:#fff;text-align:right;">{exec_l}</td>
    </tr>
    <tr>
      <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;color:#555;font-size:12px;">Alignment</td>
      <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;color:#fff;text-align:right;">{align}%</td>
    </tr>
    {quality_row}
  </table>
  <p style="color:#999;margin-bottom:24px;">
    Shift Risk: <strong style="color:#f87171;">{shift_risk}%</strong>
    &nbsp;·&nbsp; Recommended Exposure: <strong style="color:#fff;">{exposure}%</strong>
    &nbsp;·&nbsp; Strategy: <strong style="color:#fff;">{pb['strategy_mode']}</strong>
  </p>
  <div style="border:1px solid #1f1f1f;padding:16px;margin-bottom:24px;">
    <div style="font-size:11px;color:#555;text-transform:uppercase;
         letter-spacing:1px;margin-bottom:10px;">
      Regime Playbook — {exec_l}
    </div>
    <ul style="padding-left:16px;margin:0;">{actions_html}</ul>
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
    url = f"{FRONTEND_URL}/app?token={access_token}" if access_token else f"{FRONTEND_URL}/app"
    rows = ""
    for s in stacks:
        shift_risk = s.get("shift_risk") or 0
        exposure = s.get("exposure") or 0
        exec_label = s["execution"]["label"] if s.get("execution") else "—"
        macro_label = s["macro"]["label"] if s.get("macro") else "—"
        pb = PLAYBOOK_DATA.get(exec_label, PLAYBOOK_DATA["Neutral"])
        quality = compute_regime_quality(s)

        risk_color = "#f87171" if shift_risk > 70 else "#facc15" if shift_risk > 45 else "#4ade80"
        grade_color = (
            "#34d399" if quality["grade"].startswith("A") else
            "#4ade80" if quality["grade"].startswith("B") else
            "#facc15" if quality["grade"].startswith("C") else "#f87171"
        )
        rows += f"""
        <tr>
          <td style="padding:12px 8px;border-bottom:1px solid #1f1f1f;color:#fff;font-weight:600;">{s["coin"]}</td>
          <td style="padding:12px 8px;border-bottom:1px solid #1f1f1f;color:#999;font-size:12px;">{macro_label}</td>
          <td style="padding:12px 8px;border-bottom:1px solid #1f1f1f;color:#999;font-size:12px;">{exec_label}</td>
          <td style="padding:12px 8px;border-bottom:1px solid #1f1f1f;color:#fff;">{exposure}%</td>
          <td style="padding:12px 8px;border-bottom:1px solid #1f1f1f;color:{risk_color};font-weight:600;">{shift_risk}%</td>
          <td style="padding:12px 8px;border-bottom:1px solid #1f1f1f;color:{grade_color};font-weight:600;">{quality["grade"]}</td>
          <td style="padding:12px 8px;border-bottom:1px solid #1f1f1f;color:#666;font-size:11px;">{pb['strategy_mode']}</td>
        </tr>"""

    return f"""
<div style="font-family:sans-serif;max-width:640px;margin:0 auto;
     background:#000;color:#fff;padding:40px;">
  <div style="font-size:11px;color:#555;text-transform:uppercase;
       letter-spacing:2px;margin-bottom:16px;">ChainPulse Morning Brief</div>
  <h1 style="font-size:22px;margin-bottom:8px;">Daily Regime Snapshot</h1>
  <p style="color:#666;font-size:13px;margin-bottom:32px;">
    Multi-timeframe regime conditions across all tracked assets.
  </p>
  <table style="width:100%;border-collapse:collapse;">
    <thead>
      <tr>
        {"".join(
            f'<th style="text-align:left;padding:8px;color:#444;font-size:11px;text-transform:uppercase;border-bottom:1px solid #222;">{h}</th>'
            for h in ["Asset", "Macro", "Execution", "Exposure", "Shift Risk", "Grade", "Mode"]
        )}
      </tr>
    </thead>
    <tbody>{rows}</tbody>
  </table>
  <div style="margin-top:32px;border:1px solid #1f1f1f;padding:20px;">
    <div style="font-size:11px;color:#555;text-transform:uppercase;
         letter-spacing:1px;margin-bottom:12px;">How to use this brief</div>
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


def weekly_discipline_email_html(email: str, discipline: dict, access_token: str) -> str:
    url = f"{FRONTEND_URL}/app?token={access_token}" if access_token else f"{FRONTEND_URL}/app"
    score = discipline.get("score")
    label = discipline.get("label", "—")
    summary = discipline.get("summary", "")
    followed = discipline.get("followed", 0)
    total = discipline.get("total", 0)
    bonuses = discipline.get("bonuses", 0)
    penalties = discipline.get("penalties", 0)
    flags = discipline.get("flags", [])

    score_color = (
        "#34d399" if score and score >= 85 else
        "#4ade80" if score and score >= 70 else
        "#facc15" if score and score >= 50 else "#f87171"
    )
    score_display = f"{score}" if score is not None else "N/A"

    flags_html = ""
    for f in flags[-5:]:
        flag_color = "#4ade80" if f["type"] == "bonus" else "#f87171"
        flags_html += f"""
        <tr>
          <td style="padding:8px 0;border-bottom:1px solid #1a1a1a;color:{flag_color};font-size:12px;">{f['label']}</td>
          <td style="padding:8px 0;border-bottom:1px solid #1a1a1a;color:#555;font-size:11px;text-align:right;">{f['date']} — {f['regime']}</td>
        </tr>"""

    if not flags_html:
        flags_html = """
        <tr>
          <td colspan="2" style="padding:8px 0;color:#444;font-size:12px;">
            No discipline events recorded this week.
          </td>
        </tr>"""

    return f"""
<div style="font-family:sans-serif;max-width:560px;margin:0 auto;
     background:#000;color:#fff;padding:40px;">
  <div style="font-size:11px;color:#555;text-transform:uppercase;
       letter-spacing:2px;margin-bottom:16px;">ChainPulse Weekly Summary</div>
  <h1 style="font-size:22px;margin-bottom:8px;">Your Discipline Report</h1>
  <p style="color:#666;font-size:13px;margin-bottom:32px;">
    Here is how you tracked against the model this week.
  </p>
  <div style="text-align:center;padding:32px;border:1px solid #1f1f1f;margin-bottom:32px;">
    <div style="font-size:48px;font-weight:700;color:{score_color};">{score_display}</div>
    <div style="font-size:14px;color:{score_color};margin-top:8px;">{label}</div>
    <div style="font-size:12px;color:#555;margin-top:8px;">{summary}</div>
  </div>
  <table style="width:100%;border-collapse:collapse;margin-bottom:24px;">
    <tr>
      <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;color:#555;font-size:12px;">Times Followed Model</td>
      <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;color:#fff;text-align:right;">{followed} / {total}</td>
    </tr>
    <tr>
      <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;color:#555;font-size:12px;">Discipline Bonuses</td>
      <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;color:#4ade80;text-align:right;">+{bonuses}</td>
    </tr>
    <tr>
      <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;color:#555;font-size:12px;">Discipline Penalties</td>
      <td style="padding:10px 0;border-bottom:1px solid #1f1f1f;color:#f87171;text-align:right;">-{penalties}</td>
    </tr>
  </table>
  <div style="border:1px solid #1f1f1f;padding:16px;margin-bottom:24px;">
    <div style="font-size:11px;color:#555;text-transform:uppercase;
         letter-spacing:1px;margin-bottom:12px;">Recent Discipline Events</div>
    <table style="width:100%;border-collapse:collapse;">{flags_html}</table>
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


# ─────────────────────────────────────────
# FIX 2.3: Onboarding drip email templates
# ─────────────────────────────────────────
def onboarding_day0_html(email: str, access_token: str, stack: dict = None) -> str:
    url = f"{FRONTEND_URL}/app?token={access_token}" if access_token else f"{FRONTEND_URL}/app"
    regime_line = ""
    directive_line = ""
    if stack and not stack.get("incomplete"):
        exec_label = stack["execution"]["label"] if stack.get("execution") else "Neutral"
        exposure = stack.get("exposure") or 50
        regime_line = f'<p style="color:#fff;font-size:16px;">Current BTC Regime: <strong>{exec_label}</strong></p>'
        directive_line = f'<p style="color:#999;font-size:14px;">Recommended Exposure: <strong style="color:#fff;">{exposure}%</strong></p>'

    return f"""
<div style="font-family:sans-serif;max-width:560px;margin:0 auto;background:#000;color:#fff;padding:40px;">
  <div style="font-size:11px;color:#555;text-transform:uppercase;letter-spacing:2px;margin-bottom:16px;">ChainPulse Pro — Day 0</div>
  <h1 style="font-size:22px;margin-bottom:16px;">Welcome! Here's your regime status right now.</h1>
  {regime_line}
  {directive_line}
  <p style="color:#999;font-size:13px;margin-top:24px;">
    Your one action for today: <strong style="color:#fff;">Open your dashboard and check the Decision Engine directive.</strong>
    It tells you exactly what to do with your positions right now.
  </p>
  <a href="{url}" style="display:inline-block;background:#fff;color:#000;padding:14px 28px;margin-top:24px;text-decoration:none;font-weight:bold;border-radius:4px;">Open Dashboard</a>
  <p style="color:#333;font-size:11px;margin-top:40px;border-top:1px solid #111;padding-top:20px;">ChainPulse. Not financial advice.</p>
</div>
"""


def onboarding_day2_html(email: str, access_token: str) -> str:
    url = f"{FRONTEND_URL}/app?token={access_token}" if access_token else f"{FRONTEND_URL}/app"
    return f"""
<div style="font-family:sans-serif;max-width:560px;margin:0 auto;background:#000;color:#fff;padding:40px;">
  <div style="font-size:11px;color:#555;text-transform:uppercase;letter-spacing:2px;margin-bottom:16px;">ChainPulse Pro — Day 2</div>
  <h1 style="font-size:22px;margin-bottom:16px;">You've been Pro for 48 hours.</h1>
  <p style="color:#999;font-size:14px;line-height:1.7;">
    Have you logged your first exposure yet? The <strong style="color:#fff;">Exposure Logger</strong> tracks your positions
    against the model's recommendation. This builds your <strong style="color:#fff;">Discipline Score</strong> — the #1 predictor
    of long-term performance.
  </p>
  <a href="{url}" style="display:inline-block;background:#fff;color:#000;padding:14px 28px;margin-top:24px;text-decoration:none;font-weight:bold;border-radius:4px;">Log Your First Exposure</a>
  <p style="color:#333;font-size:11px;margin-top:40px;border-top:1px solid #111;padding-top:20px;">ChainPulse. Not financial advice.</p>
</div>
"""


def onboarding_day5_html(email: str, access_token: str) -> str:
    url = f"{FRONTEND_URL}/app?token={access_token}" if access_token else f"{FRONTEND_URL}/app"
    return f"""
<div style="font-family:sans-serif;max-width:560px;margin:0 auto;background:#000;color:#fff;padding:40px;">
  <div style="font-size:11px;color:#555;text-transform:uppercase;letter-spacing:2px;margin-bottom:16px;">ChainPulse Pro — Day 5</div>
  <h1 style="font-size:22px;margin-bottom:16px;">Your discipline score is building.</h1>
  <p style="color:#999;font-size:14px;line-height:1.7;">
    After 5 days, you're building behavioral data. Check your <strong style="color:#fff;">Behavioral Alpha</strong> report
    to see if any patterns are costing you money — and how to fix them.
  </p>
  <a href="{url}" style="display:inline-block;background:#fff;color:#000;padding:14px 28px;margin-top:24px;text-decoration:none;font-weight:bold;border-radius:4px;">View Behavioral Insights</a>
  <p style="color:#333;font-size:11px;margin-top:40px;border-top:1px solid #111;padding-top:20px;">ChainPulse. Not financial advice.</p>
</div>
"""


def onboarding_day6_html(email: str, access_token: str) -> str:
    url = f"{FRONTEND_URL}/app?token={access_token}" if access_token else f"{FRONTEND_URL}/app"
    return f"""
<div style="font-family:sans-serif;max-width:560px;margin:0 auto;background:#000;color:#fff;padding:40px;">
  <div style="font-size:11px;color:#555;text-transform:uppercase;letter-spacing:2px;margin-bottom:16px;">ChainPulse Pro — Trial Ending</div>
  <h1 style="font-size:22px;margin-bottom:16px;">Your trial ends tomorrow.</h1>
  <p style="color:#999;font-size:14px;line-height:1.7;">
    Here's what you'll lose access to:
  </p>
  <ul style="color:#f87171;font-size:13px;line-height:2;padding-left:16px;">
    <li>Decision Engine directives</li>
    <li>Setup Quality & entry timing</li>
    <li>Probabilistic scenarios</li>
    <li>Internal damage monitor</li>
    <li>Behavioral alpha leak detection</li>
    <li>Trade plan generator</li>
    <li>All email alerts & briefs</li>
  </ul>
  <p style="color:#999;font-size:14px;margin-top:16px;">
    Your discipline score, exposure history, and behavioral data will be preserved if you continue.
  </p>
  <a href="{FRONTEND_URL}/pricing" style="display:inline-block;background:#fff;color:#000;padding:14px 28px;margin-top:24px;text-decoration:none;font-weight:bold;border-radius:4px;">Keep Pro Access</a>
  <p style="color:#333;font-size:11px;margin-top:40px;border-top:1px solid #111;padding-top:20px;">ChainPulse. Not financial advice.</p>
</div>
"""


# ═══════════════════════════════════════════
# ROUTES
# ═══════════════════════════════════════════

# ── Health ──────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok", "version": MODEL_VERSION, "timestamp": datetime.datetime.utcnow()}


# ── Pricing Info ─────────────────────────────
def pricing_info():
    return {
        "tiers": {
            "essential": {
                "monthly": 39,
                "annual": 348,
                "label": "Essential",
                "features": [
                    "Multi-timeframe regime stack",
                    "Exposure recommendation %",
                    "Shift risk & hazard rate",
                    "Survival probability & curve",
                    "Decision Engine directives",
                    "If You Do Nothing simulator",
                    "Volatility environment",
                    "Transition matrix",
                    "Portfolio allocator",
                    "Exposure logger & discipline score",
                    "Performance comparison",
                    "Edge profile & mistake replay",
                    "Correlation monitor",
                    "Daily morning brief email",
                ],
            },
            "pro": {
                "monthly": 79,
                "annual": 708,
                "label": "Pro",
                "features": [
                    "Everything in Essential",
                    "Setup Quality & entry timing",
                    "Probabilistic scenarios",
                    "Internal damage monitor",
                    "Behavioral alpha leak detection",
                    "Trade plan generator",
                    "Historical analogs",
                    "Opportunity ranking",
                    "Event risk overlay",
                    "What Changed intelligence brief",
                    "Dynamic alert evaluation",
                ],
            },
            "institutional": {
                "monthly": 149,
                "annual": 1308,
                "label": "Institutional",
                "features": [
                    "Everything in Pro",
                    "Trader archetype overlay (full customization)",
                    "Custom per-coin alert thresholds",
                    "Priority alert delivery (1hr cooldown)",
                    "REST API access (1,000 requests/day)",
                    "Webhook delivery (regime changes, alerts, setup quality)",
                    "Up to 3 API keys",
                    "Up to 5 webhook endpoints",
                    "HMAC-SHA256 webhook signatures",
                    "Webhook delivery logs",
                ],
            },
        },
        "free_tier": {
            "includes": [
                "Execution regime label (not macro/trend)",
                "Direction (Bullish / Bearish / Mixed)",
                "Basic market breadth (label only)",
                "Risk events calendar",
            ],
        },
        "currency": "USD",
    }

# ── Update ──────────────────────────────────
@app.get("/update-now")
def update_now(coin: str = "BTC", timeframe: str = "1h", secret: str = "", db: Session = Depends(get_db)):
    if secret != UPDATE_SECRET:
        raise HTTPException(status_code=403, detail="Invalid secret")
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")
    if timeframe not in SUPPORTED_TIMEFRAMES:
        raise HTTPException(status_code=400, detail="Unsupported timeframe")
    entry = update_market(coin, timeframe, db)
    if not entry:
        raise HTTPException(status_code=500, detail="Update failed")
    return {"status": "updated", "coin": coin, "timeframe": timeframe, "label": entry.label, "score": entry.score}


@app.get("/update-all")
def update_all(secret: str = "", db: Session = Depends(get_db)):
    if secret != UPDATE_SECRET:
        raise HTTPException(status_code=403, detail="Invalid secret")
    results = []
    for coin in SUPPORTED_COINS:
        for tf in SUPPORTED_TIMEFRAMES:
            entry = update_market(coin, tf, db)
            if entry:
                results.append({"coin": coin, "timeframe": tf, "label": entry.label, "score": entry.score})
    return {"status": "updated", "count": len(results), "results": results}


# ── Regime Stack ─────────────────────────────
# FIX 2.2: Tightened free tier — execution label only, no alignment %, no macro/trend
@app.get("/regime-stack")
def regime_stack_endpoint(request: Request, coin: str = "BTC", db: Session = Depends(get_db)):
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")

    is_pro = resolve_pro_status(get_auth_header(request), db)
    stack = build_regime_stack(coin, db)

    if stack["incomplete"]:
        return {**stack, "pro_required": False}

    # FIX 2.2: Free tier shows execution label ONLY
    if not is_pro:
        return {
            "coin": stack["coin"],
            "execution": {"label": stack["execution"]["label"]} if stack["execution"] else None,
            "direction": stack["direction"],
            # REMOVED from free: macro, trend, alignment
            "pro_required": True,
            "upgrade_message": "Unlock macro + trend regimes, exposure guidance, and 15+ premium tools",
        }

    update_last_active(request, db)
    age_1h = current_age(db, coin, "1h")
    avg_dur = average_regime_duration(db, coin, "1h")
    maturity = trend_maturity_score(age_1h, avg_dur, stack["hazard"])
    pct_rank = percentile_rank(db, coin, stack["execution"]["score"], "1h")
    quality = compute_regime_quality(stack)

    return {
        "coin": stack["coin"],
        "macro": stack["macro"], "trend": stack["trend"], "execution": stack["execution"],
        "alignment": stack["alignment"], "direction": stack["direction"],
        "pro_required": False,
        "exposure": stack["exposure"], "shift_risk": stack["shift_risk"],
        "survival": stack["survival"], "hazard": stack["hazard"],
        "trend_maturity": maturity, "percentile": pct_rank,
        "macro_coherence": stack["macro"]["coherence"],
        "trend_coherence": stack["trend"]["coherence"],
        "exec_coherence": stack["execution"]["coherence"],
        "regime_age_hours": round(age_1h, 2),
        "avg_regime_duration_hours": round(avg_dur, 2),
        "regime_quality": quality,
    }


# ── Market Overview ──────────────────────────
# FIX 2.2: Tightened free tier
@app.get("/market-overview")
def market_overview(request: Request, coin: str = "ALL", db: Session = Depends(get_db)):
    is_pro = resolve_pro_status(get_auth_header(request), db)
    result = []
    breadth = get_or_compute("market_breadth", compute_market_breadth, ttl=60, db=db)

    coins_to_scan = SUPPORTED_COINS if coin == "ALL" else [coin] if coin in SUPPORTED_COINS else SUPPORTED_COINS

    for c in coins_to_scan:
        stack = build_regime_stack(c, db)
        if stack["incomplete"]:
            continue

        if is_pro:
            row = {
                "coin": stack["coin"],
                "macro": stack["macro"]["label"] if stack["macro"] else None,
                "trend": stack["trend"]["label"] if stack["trend"] else None,
                "execution": stack["execution"]["label"] if stack["execution"] else None,
                "alignment": stack["alignment"], "direction": stack["direction"],
                "exposure": stack["exposure"], "shift_risk": stack["shift_risk"],
            }
        else:
            # FIX 2.2: Free tier — execution label and direction only
            row = {
                "coin": stack["coin"],
                "execution": stack["execution"]["label"] if stack["execution"] else None,
                "direction": stack["direction"],
                "pro_required": True,
            }
        result.append(row)

    # FIX 2.2: Free breadth = label only (not score)
    if not is_pro:
        breadth_free = {
            "total": breadth.get("total", 0),
            "sentiment": "Bullish" if breadth.get("breadth_score", 0) > 30 else "Bearish" if breadth.get("breadth_score", 0) < -30 else "Neutral",
            "pro_required": True,
        }
        return {"data": result, "breadth": breadth_free}

    return {"data": result, "breadth": breadth}


# ── Latest ──────────────────────────────────
@app.get("/latest")
def latest(coin: str = "BTC", db: Session = Depends(get_db)):
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")
    r = (
        db.query(MarketSummary)
        .filter(MarketSummary.coin == coin, MarketSummary.timeframe == "1h")
        .order_by(MarketSummary.created_at.desc())
        .first()
    )
    if not r:
        return {"message": "No data yet."}
    return {
        "coin": r.coin, "score": r.score, "label": r.label,
        "coherence": r.coherence, "momentum_4h": r.momentum_4h,
        "momentum_24h": r.momentum_24h, "volatility": r.volatility_val,
        "timeframe": r.timeframe, "timestamp": r.created_at,
    }


# ── Statistics (landing page — free) ─────────
@app.get("/statistics")
def statistics(coin: str = "BTC", db: Session = Depends(get_db)):
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")
    record = (
        db.query(MarketSummary)
        .filter(MarketSummary.coin == coin, MarketSummary.timeframe == "1h")
        .order_by(MarketSummary.created_at.desc())
        .first()
    )
    if not record:
        return {"message": "No data yet"}
    return {"coin": coin, "label": record.label, "score": record.score, "coherence": record.coherence, "timestamp": record.created_at}


# ── Regime History ──────────────────────────
@app.get("/regime-history")
def regime_history(coin: str = "BTC", timeframe: str = "1h", limit: int = 48, db: Session = Depends(get_db)):
    if timeframe not in SUPPORTED_TIMEFRAMES:
        raise HTTPException(status_code=400, detail="Unsupported timeframe")
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")
    limit = min(max(1, limit), 500)
    records = (
        db.query(MarketSummary)
        .filter(MarketSummary.coin == coin, MarketSummary.timeframe == timeframe)
        .order_by(MarketSummary.created_at.desc())
        .limit(limit)
        .all()
    )
    records.reverse()
    return {"data": [{"hour": i, "score": r.score, "label": r.label, "coherence": r.coherence, "timestamp": r.created_at} for i, r in enumerate(records)]}


# ── Survival Curve (PRO) ─────────────────────
@app.get("/survival-curve")
def survival_curve(request: Request, coin: str = "BTC", timeframe: str = "1h", db: Session = Depends(get_db)):
    auth = get_auth_header(request)
    require_tier(auth, db, minimum_tier="essential")
    start = time.perf_counter()

    cache_key = f"survival:{coin}:{timeframe}"
    cached = cache_get(cache_key)
    if cached:
        logger.info(f"survival | coin={coin} | tf={timeframe} | source=cache")
        return cached

    durations = regime_durations(db, coin, timeframe)
    if len(durations) < 5:
        return {
            "data": [{"hour": h, "survival": max(0, 100 - h * 4), "hazard": min(100, h * 4.5)} for h in range(25)],
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
        curve.append({"hour": hour, "survival": round(surv_pct, 2), "hazard": round(hz, 2)})
    response = {"data": curve, "source": "historical"}
    cache_set(cache_key, response, ttl=300)
    duration = round((time.perf_counter() - start) * 1000, 2)
    logger.info(f"survival | coin={coin} | tf={timeframe} | source=computed | duration_ms={duration}")
    return response


# ── Regime Transitions (PRO) ─────────────────
@app.get("/regime-transitions")
def regime_transitions(request: Request, coin: str = "BTC", timeframe: str = "1h", db: Session = Depends(get_db)):
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")
    auth = get_auth_header(request)
    require_tier(auth, db, minimum_tier="essential")

    result = get_or_compute(f"transitions:{coin}:{timeframe}", regime_transition_matrix, ttl=300, db=db, coin=coin, timeframe=timeframe)
    if result is None:
        return {"current_state": "Insufficient data", "transitions": {}, "data_sufficient": False}
    return result


# ── Volatility Environment (PRO) ─────────────
@app.get("/volatility-environment")
def volatility_env(request: Request, coin: str = "BTC", db: Session = Depends(get_db)):
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")
    auth = get_auth_header(request)
    require_tier(auth, db, minimum_tier="essential")
    result = get_or_compute(f"volatility:{coin}", volatility_environment, ttl=120, coin=coin, db=db)
    if result is None:
        return {"error": "Insufficient data"}
    return result


# ── Correlation Matrix (PRO) ─────────────────
@app.get("/correlation")
@app.get("/correlation-matrix")
def correlation_endpoint(request: Request, coins: str = "BTC,ETH,SOL", db: Session = Depends(get_db)):
    auth = get_auth_header(request)
    require_tier(auth, db, minimum_tier="essential")
    coin_list = [c.strip().upper() for c in coins.split(",") if c.strip()]
    sorted_key = ",".join(sorted(coin_list))
    return get_or_compute(f"correlation:{sorted_key}", build_correlation_matrix, ttl=300, coins=coin_list)


# ── Regime Confidence (PRO) ──────────────────
@app.get("/regime-confidence")
def regime_confidence_endpoint(request: Request, coin: str = "BTC", db: Session = Depends(get_db)):
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")
    auth = get_auth_header(request)
    require_tier(auth, db, minimum_tier="essential")

    stack = build_regime_stack(coin, db)
    breadth = compute_market_breadth(db)
    if stack["incomplete"]:
        return {"error": "Insufficient regime data"}
    survival_val = stack.get("survival") or 50.0
    coherence_val = stack["execution"]["coherence"] if stack.get("execution") and stack["execution"].get("coherence") else 0.0
    confidence = regime_confidence_score(
        alignment=stack["alignment"] or 0, survival=survival_val,
        coherence=coherence_val, breadth_score=breadth.get("breadth_score", 0),
    )
    return {**confidence, "coin": coin}


# ── Regime Quality (PRO) ─────────────────────
@app.get("/regime-quality")
def regime_quality_endpoint(request: Request, coin: str = "BTC", db: Session = Depends(get_db)):
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")
    auth = get_auth_header(request)
    require_tier(auth, db, minimum_tier="essential")
    update_last_active(request, db)

    stack = get_or_compute(f"stack:{coin}", build_regime_stack, ttl=60, coin=coin, db=db)
    if stack["incomplete"]:
        return {"error": "Insufficient data"}
    quality = compute_regime_quality(stack)
    return {
        **quality, "coin": coin,
        "regime": stack["execution"]["label"] if stack.get("execution") else "Neutral",
        "exposure": stack.get("exposure"), "shift_risk": stack.get("shift_risk"),
        "hazard": stack.get("hazard"), "survival": stack.get("survival"),
    }


# ── Playbook (free preview / PRO full) ───────
# FIX 2.2: Free tier shows less
@app.get("/playbook")
def playbook(request: Request, coin: str = "BTC", db: Session = Depends(get_db)):
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")
    is_pro = resolve_pro_status(get_auth_header(request), db)
    stack = build_regime_stack(coin, db)
    if stack["incomplete"]:
        return {"error": "Insufficient data"}
    exec_label = stack["execution"]["label"] if stack.get("execution") else "Neutral"
    pb = PLAYBOOK_DATA.get(exec_label, PLAYBOOK_DATA["Neutral"])

    if not is_pro:
        return {"coin": coin, "regime": exec_label, "strategy_mode": pb["strategy_mode"], "pro_required": True}

    return {
        "coin": coin, "regime": exec_label,
        "strategy_mode": pb["strategy_mode"], "exposure_band": pb["exposure_band"],
        "trend_follow_wr": pb["trend_follow_wr"], "mean_revert_wr": pb["mean_revert_wr"],
        "avg_remaining_days": pb["avg_remaining_days"],
        "data_source": pb.get("data_source", "backtested_estimates"),  # FIX 4.3
        "actions": pb["actions"], "avoid": pb["avoid"], "pro_required": False,
    }


# ── Portfolio Allocator (PRO) ────────────────
@app.post("/portfolio-allocator")
def portfolio_allocator_endpoint(request: Request, account_size: float = 10000, strategy_mode: str = "balanced", coin: str = "BTC", db: Session = Depends(get_db)):
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")
    if strategy_mode not in ("conservative", "balanced", "aggressive"):
        raise HTTPException(status_code=400, detail="Invalid strategy mode")
    if account_size <= 0:
        raise HTTPException(status_code=400, detail="Invalid account size")
    auth = get_auth_header(request)
    require_tier(auth, db, minimum_tier="essential")
    update_last_active(request, db)

    stack = build_regime_stack(coin, db)
    if stack["incomplete"]:
        return {"error": "Insufficient data"}
    breadth = compute_market_breadth(db)
    survival_v = stack.get("survival") or 50.0
    coherence_v = stack["execution"]["coherence"] if stack.get("execution") and stack["execution"].get("coherence") else 0.0
    confidence = regime_confidence_score(alignment=stack["alignment"] or 0, survival=survival_v, coherence=coherence_v, breadth_score=breadth.get("breadth_score", 0))
    allocation = portfolio_allocation(account_size=account_size, exposure_pct=stack["exposure"] or 5, confidence_score=confidence["score"], strategy_mode=strategy_mode)
    return {**allocation, "regime": stack["execution"]["label"] if stack.get("execution") else "—", "confidence": confidence["score"], "alignment": stack["alignment"]}


# ── Risk Events (FREE) ───────────────────────
@app.get("/risk-events")
def risk_events():
    return {"events": RISK_EVENTS}


# ── Decision Engine (PRO) ────────────────────
@app.get("/decision-engine")
def decision_engine_endpoint(request: Request, coin: str = "BTC", db: Session = Depends(get_db)):
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")
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
    shift_risk = stack.get("shift_risk") or 0
    alignment = stack.get("alignment") or 0
    survival = stack.get("survival") or 50
    age_1h = current_age(db, coin, "1h")
    avg_dur = average_regime_duration(db, coin, "1h")
    maturity = trend_maturity_score(age_1h, avg_dur, hazard)

    decision = compute_decision_score(hazard=hazard, shift_risk=shift_risk, alignment=alignment, survival=survival, breadth_score=breadth.get("breadth_score", 0), maturity_pct=maturity)
    exec_label = stack["execution"]["label"] if stack.get("execution") else "Neutral"
    decision["regime"] = exec_label
    decision["exposure"] = stack.get("exposure", 50)
    decision["coin"] = coin
    decision["model_version"] = MODEL_VERSION
    cache_set(cache_key, decision, ttl=60)
    return decision


# ── If You Do Nothing (PRO) ──────────────────
@app.post("/if-nothing-panel")
def if_nothing_panel_endpoint(request: Request, coin: str = "BTC", user_exposure: float = 50.0, db: Session = Depends(get_db)):
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")
    auth = get_auth_header(request)
    require_tier(auth, db, minimum_tier="essential")
    stack = build_regime_stack(coin, db)
    if stack["incomplete"]:
        return {"error": "Insufficient data"}
    exec_label = stack["execution"]["label"] if stack.get("execution") else "Neutral"
    model_exposure = stack.get("exposure") or 50
    return compute_if_nothing_panel(user_exposure=user_exposure, model_exposure=model_exposure, hazard=stack.get("hazard") or 0, shift_risk=stack.get("shift_risk") or 0, regime_label=exec_label)


# ── User Profile (PRO) ───────────────────────
@app.post("/user-profile")
def save_user_profile(request: Request, body: UserProfileRequest, db: Session = Depends(get_db)):
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="essential")
    email = require_email_ownership(user_info, body.email)
    mult_map = {"conservative": 0.70, "balanced": 1.00, "aggressive": 1.25}
    risk_mult = mult_map.get(body.risk_identity, 1.0)
    user = db.query(User).filter(User.email == email).first()
    user_id = user.id if user else None
    profile = db.query(UserProfile).filter(UserProfile.email == email).first()
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
    return {"status": "saved", "email": email, "risk_multiplier": risk_mult, "profile": {"max_drawdown_pct": profile.max_drawdown_pct, "typical_leverage": profile.typical_leverage, "holding_period_days": profile.holding_period_days, "risk_identity": profile.risk_identity}}


@app.get("/user-profile")
def get_user_profile(request: Request, email: str, coin: str = "BTC", db: Session = Depends(get_db)):
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="essential")
    email = require_email_ownership(user_info, email)
    profile = db.query(UserProfile).filter(UserProfile.email == email).first()
    if not profile:
        return {"exists": False, "message": "No profile found. Complete onboarding to personalise."}
    stack = build_regime_stack(coin, db)
    personalised_exposure = None
    if not stack["incomplete"] and stack.get("exposure"):
        personalised_exposure = round(min(95, max(5, stack["exposure"] * profile.risk_multiplier)), 1)
    return {
        "exists": True, "email": email, "risk_identity": profile.risk_identity,
        "risk_multiplier": profile.risk_multiplier, "max_drawdown_pct": profile.max_drawdown_pct,
        "typical_leverage": profile.typical_leverage, "holding_period_days": profile.holding_period_days,
        "personalised_exposure": personalised_exposure,
        "model_exposure": stack.get("exposure") if not stack.get("incomplete") else None,
        "created_at": profile.created_at,
    }


# ── Exposure Logger (PRO) ────────────────────
@app.post("/log-exposure")
def log_exposure(request: Request, body: ExposureLogRequest, db: Session = Depends(get_db)):
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="essential")
    email = require_email_ownership(user_info, email)
    if body.coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")
    stack = build_regime_stack(body.coin, db)
    if stack["incomplete"]:
        raise HTTPException(status_code=400, detail="No regime data yet")

    model_exp = stack.get("exposure") or 50
    hazard = stack.get("hazard") or 0
    shift_risk = stack.get("shift_risk") or 0
    alignment = stack.get("alignment") or 0
    exec_label = stack["execution"]["label"] if stack.get("execution") else "Neutral"
    delta = body.user_exposure_pct - model_exp
    followed = abs(delta) <= 10

    current_price = 0.0
    try:
        prices, _ = get_klines(body.coin, "1h", limit=2)
        if prices:
            current_price = prices[-1]
    except Exception:
        pass

    log = ExposureLog(
        email=email, coin=body.coin, user_exposure_pct=body.user_exposure_pct,
        model_exposure_pct=model_exp, regime_label=exec_label,
        hazard_at_log=hazard, shift_risk_at_log=shift_risk,
        alignment_at_log=alignment, followed_model=followed, price_at_log=current_price,
    )
    db.add(log)
    db.commit()

    if abs(delta) > 20: feedback, severity = "⚠ Large deviation from model recommendation", "warning"
    elif abs(delta) > 10: feedback, severity = "Moderate deviation — within acceptable range", "caution"
    else: feedback, severity = "✓ Aligned with model recommendation", "ok"

    return {
        "status": "logged", "user_exposure": body.user_exposure_pct,
        "model_exposure": model_exp, "delta": round(delta, 1),
        "followed_model": followed, "feedback": feedback, "severity": severity,
        "regime": exec_label, "price_at_log": current_price,
    }


# ── Discipline Score (PRO) ───────────────────
@app.get("/discipline-score")
def discipline_score_endpoint(request: Request, email: str, db: Session = Depends(get_db)):
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="essential")
    email = require_email_ownership(user_info, email)
    update_last_active(request, db)
    logs = db.query(ExposureLog).filter(ExposureLog.email == email).order_by(ExposureLog.created_at.desc()).limit(30).all()
    result = compute_discipline_score(logs)
    result["email"] = email
    return result


# ── Performance Comparison (PRO) ─────────────
@app.get("/performance-comparison")
def performance_comparison_endpoint(request: Request, email: str, coin: str = "BTC", limit: int = 30, db: Session = Depends(get_db)):
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="essential")
    email = require_email_ownership(user_info, email)
    entries = db.query(PerformanceEntry).filter(PerformanceEntry.email == email, PerformanceEntry.coin == coin).order_by(PerformanceEntry.date.asc()).limit(limit).all()
    result = compute_performance_comparison(entries)
    result["email"] = email
    result["coin"] = coin
    return result


# ── Log Performance (PRO) ────────────────────
@app.post("/log-performance")
def log_performance(request: Request, body: PerformanceEntryRequest, db: Session = Depends(get_db)):
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="essential")
    email = require_email_ownership(user_info, email)
    if body.coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")
    if body.price_open <= 0 or body.price_close <= 0:
        raise HTTPException(status_code=400, detail="Invalid prices")

    stack = build_regime_stack(body.coin, db)
    model_exp = stack.get("exposure") or 50
    exec_label = stack["execution"]["label"] if not stack["incomplete"] and stack.get("execution") else "Neutral"

    price_return = ((body.price_close - body.price_open) / body.price_open) * 100
    user_return = round(price_return * (body.user_exposure_pct / 100), 2)
    model_return = round(price_return * (model_exp / 100), 2)

    flags = []
    delta = body.user_exposure_pct - model_exp
    hazard = stack.get("hazard") or 0
    shift_r = stack.get("shift_risk") or 0
    if hazard > 65 and delta > 10: flags.append("over_exposed_high_hazard")
    if "Risk-Off" in exec_label and delta > 15: flags.append("over_exposed_risk_off")
    if shift_r > 70 and delta < -5: flags.append("reduced_on_hazard_spike")
    if abs(delta) <= 10: flags.append("followed_model")

    entry = PerformanceEntry(
        email=email, coin=body.coin, date=datetime.datetime.utcnow(),
        user_exposure_pct=body.user_exposure_pct, model_exposure_pct=model_exp,
        price_open=body.price_open, price_close=body.price_close,
        user_return_pct=user_return, model_return_pct=model_return,
        regime_label=exec_label, discipline_flags=json.dumps(flags),
    )
    db.add(entry)
    db.commit()
    return {
        "status": "logged", "price_return": round(price_return, 2),
        "user_return": user_return, "model_return": model_return,
        "alpha": round(user_return - model_return, 2),
        "regime": exec_label, "discipline_flags": flags,
    }


# ── Mistake Replay (PRO) ─────────────────────
@app.get("/mistake-replay")
def mistake_replay_endpoint(request: Request, email: str, coin: str = "BTC", db: Session = Depends(get_db)):
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="essential")
    email = require_email_ownership(user_info, email)
    logs = db.query(ExposureLog).filter(ExposureLog.email == email, ExposureLog.coin == coin).order_by(ExposureLog.created_at.desc()).limit(50).all()
    replays = compute_mistake_replay(logs, db, coin)
    return {"email": email, "coin": coin, "replays": replays, "count": len(replays)}


# ── Edge Profile (PRO) ───────────────────────
@app.get("/edge-profile")
def edge_profile_endpoint(request: Request, email: str, db: Session = Depends(get_db)):
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="essential")
    email = require_email_ownership(user_info, email)
    entries = db.query(PerformanceEntry).filter(PerformanceEntry.email == email).order_by(PerformanceEntry.date.asc()).all()

    if len(entries) < 5:
        return {"email": email, "ready": False, "message": f"Need {5 - len(entries)} more entries to build your edge profile.", "entry_count": len(entries)}

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
            avg = round(sum(rets) / len(rets), 2)
            wins = sum(1 for r in rets if r > 0)
            profile[regime] = {
                "avg_return": avg,
                "win_rate": round((wins / len(rets)) * 100, 1),
                "count": len(rets),
                "performance": "Strong" if avg > 2 else "Good" if avg > 0.5 else "Weak" if avg > -1 else "Poor",
            }

    if not profile:
        return {"email": email, "ready": False, "message": "No return data."}

    best_regime = max(profile.items(), key=lambda x: x[1]["avg_return"])
    worst_regime = min(profile.items(), key=lambda x: x[1]["avg_return"])

    recommendations = []
    for regime, data in profile.items():
        if data["performance"] in ("Weak", "Poor"):
            recommendations.append(f"Reduce exposure faster in {regime} conditions (avg {data['avg_return']:+.1f}%)")
        elif data["performance"] == "Strong":
            recommendations.append(f"You have edge in {regime} — stay disciplined here (avg {data['avg_return']:+.1f}%)")

    return {
        "email": email, "ready": True, "entry_count": len(entries),
        "best_regime": best_regime[0], "worst_regime": worst_regime[0],
        "profile": profile, "recommendations": recommendations,
    }


# ── Full Accountability (PRO) ────────────────
@app.get("/full-accountability")
def full_accountability(request: Request, email: str, coin: str = "BTC", db: Session = Depends(get_db)):
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="essential")
    email = require_email_ownership(user_info, email)

    logs = db.query(ExposureLog).filter(ExposureLog.email == email).order_by(ExposureLog.created_at.desc()).limit(50).all()
    entries = db.query(PerformanceEntry).filter(PerformanceEntry.email == email, PerformanceEntry.coin == coin).order_by(PerformanceEntry.date.asc()).limit(30).all()
    user_profile = db.query(UserProfile).filter(UserProfile.email == email).first()

    discipline = compute_discipline_score(logs)
    performance = compute_performance_comparison(entries)
    replays = compute_mistake_replay(logs, db, coin)

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
                "win_rate": round(sum(1 for x in r if x > 0) / len(r) * 100, 1),
                "count": len(r),
            }
            for regime, r in regime_data.items() if r
        }

    return {
        "email": email, "coin": coin,
        "discipline": discipline, "performance": performance,
        "replays": replays, "edge": edge,
        "profile": {
            "risk_identity": user_profile.risk_identity if user_profile else None,
            "risk_multiplier": user_profile.risk_multiplier if user_profile else None,
            "max_drawdown_pct": user_profile.max_drawdown_pct if user_profile else None,
            "holding_period_days": user_profile.holding_period_days if user_profile else None,
        } if user_profile else None,
        "has_profile": user_profile is not None,
    }


# ── Stripe Webhook ───────────────────────────
# FIX 1.3: Token created_at is set on generation
# FIX 2.3: Trial start date is set for onboarding drip
@app.post("/stripe-webhook")
async def stripe_webhook(request: Request, db: Session = Depends(get_db)):
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature", "")

    if not STRIPE_WEBHOOK_SECRET:
        raise HTTPException(status_code=500, detail="Webhook secret not configured")

    try:
        event = stripe.Webhook.construct_event(payload, sig_header, STRIPE_WEBHOOK_SECRET)
    except stripe.error.SignatureVerificationError:
        raise HTTPException(status_code=400, detail="Invalid signature")

    event_type = event["type"]
    data = event["data"]["object"]

    # ── CHECKOUT COMPLETED ──
    if event_type == "checkout.session.completed":
        customer_email = data.get("customer_details", {}).get("email") or data.get("customer_email")
        customer_id = data.get("customer")
        subscription_id = data.get("subscription")
        tier = data.get("metadata", {}).get("tier", "pro")  # DEFAULT TO PRO

        if customer_email:
            email = customer_email.strip().lower()
            user = db.query(User).filter(User.email == email).first()
            if not user:
                user = User(email=email)
                db.add(user)

            access_token = str(uuid.uuid4())
            user.subscription_status = "active"
            user.tier = tier  # Save the tier
            user.stripe_customer_id = customer_id
            user.stripe_subscription_id = subscription_id
            user.alerts_enabled = True
            user.access_token = access_token
            user.token_created_at = datetime.datetime.utcnow()
            user.trial_start_date = datetime.datetime.utcnow()
            user.onboarding_step = 0
            db.commit()

            send_email(
                email,
                "Welcome to ChainPulse Pro — Your Access Link",
                welcome_email_html(email, access_token),
            )
            logger.info(f"Activated {email} on {tier} tier")

    # ── SUBSCRIPTION UPDATED (upgrade/downgrade) ──
    elif event_type == "customer.subscription.updated":
        subscription = data
        sub_id = subscription.get("id")
        customer_id = subscription.get("customer")
        status = subscription.get("status")  # active, past_due, canceled, etc.

        # Get tier from subscription metadata
        tier = subscription.get("metadata", {}).get("tier", "pro")

        user = db.query(User).filter(User.stripe_subscription_id == sub_id).first()
        if not user:
            user = db.query(User).filter(User.stripe_customer_id == customer_id).first()

        if user:
            if status in ("active", "trialing"):
                user.subscription_status = "active"
                user.tier = tier
                if not user.access_token:
                    user.access_token = str(uuid.uuid4())
                    user.token_created_at = datetime.datetime.utcnow()
            else:
                user.subscription_status = "inactive"
            db.commit()
            logger.info(f"Updated {user.email}: status={status}, tier={tier}")

    # ── SUBSCRIPTION DELETED (canceled) ──
    elif event_type in ("customer.subscription.deleted", "customer.subscription.paused"):
        sub_id = data.get("id")
        customer_id = data.get("customer")

        user = db.query(User).filter(User.stripe_subscription_id == sub_id).first()
        if not user:
            user = db.query(User).filter(User.stripe_customer_id == customer_id).first()

        if user:
            user.subscription_status = "canceled"
            user.tier = "free"
            user.access_token = None
            user.token_created_at = None
            db.commit()
            logger.info(f"Canceled {user.email}")

    # ── INVOICE PAYMENT FAILED ──
    elif event_type == "invoice.payment_failed":
        customer_id = data.get("customer")

        user = db.query(User).filter(User.stripe_customer_id == customer_id).first()
        if user:
            user.subscription_status = "past_due"
            db.commit()
            logger.info(f"Payment failed for {user.email}")

            send_email(
                user.email,
                "ChainPulse — Payment Failed",
                f"""
<div style="font-family:sans-serif;max-width:560px;margin:0 auto;background:#000;color:#fff;padding:40px;">
  <h2 style="color:#f87171;">Payment Failed</h2>
  <p style="color:#999;">Your ChainPulse Pro payment could not be processed. Please update your payment method to maintain access.</p>
  <a href="{FRONTEND_URL}/pricing" style="display:inline-block;background:#fff;color:#000;padding:14px 28px;margin-top:24px;text-decoration:none;font-weight:bold;border-radius:4px;">Update Payment</a>
</div>
""",
            )

    return {"status": "received"}


# ── Checkout Session ─────────────────────────
@app.post("/create-checkout-session")
def create_checkout_session(body: CheckoutRequest, db: Session = Depends(get_db)):
    rate_limiter.require(requests.request, max_requests=10, window_seconds=60)
    if not STRIPE_SECRET_KEY:
        raise HTTPException(status_code=500, detail="Stripe not configured")

    # Validate tier
    if body.tier not in STRIPE_PRICE_MAP:
        raise HTTPException(400, detail=f"Invalid tier: {body.tier}")

    # Validate billing cycle
    if body.billing_cycle not in ("monthly", "annual"):
        raise HTTPException(400, detail=f"Invalid billing cycle: {body.billing_cycle}")

    # Get the price ID
    price_id = STRIPE_PRICE_MAP[body.tier][body.billing_cycle]
    if not price_id:
        raise HTTPException(400, detail=f"Price not configured for {body.tier}/{body.billing_cycle}")

    try:
        # Find or create customer
        customer_kwargs = {}
        if body.email:
            email = body.email.strip().lower()
            user = db.query(User).filter(User.email == email).first()

            if user and user.stripe_customer_id:
                customer_kwargs["customer"] = user.stripe_customer_id
            else:
                customer_kwargs["customer_email"] = email

        session = stripe.checkout.Session.create(
            mode="subscription",
            payment_method_types=["card"],
            line_items=[{
                "price": price_id,
                "quantity": 1,
            }],
            subscription_data={
                "trial_period_days": 7,
                "metadata": {
                    "tier": body.tier,
                },
            },
            metadata={
                "tier": body.tier,
            },
            allow_promotion_codes=True,
            success_url=f"{FRONTEND_URL}/app?success=true&tier={body.tier}",
            cancel_url=f"{FRONTEND_URL}/pricing?cancelled=true",
            **customer_kwargs,
        )

        return {"url": session.url, "session_id": session.id}
    except stripe.error.StripeError as e:
        logger.error(f"Stripe error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Checkout error: {e}")
        raise HTTPException(status_code=500, detail="Checkout creation failed")


# ── Subscribe (free newsletter) ──────────────
@app.post("/subscribe")
def subscribe(body: SubscribeRequest, request: Request, db: Session = Depends(get_db)):
    rate_limiter.require(request, max_requests=5, window_seconds=3600)
    email = body.email.strip().lower()
    user = db.query(User).filter(User.email == email).first()
    if not user:
        user = User(email=email, subscription_status="inactive", alerts_enabled=False)
        db.add(user)
        db.commit()

    confirmation_link = f"{BACKEND_URL}/confirm?email={email}"
    html = f"""
<div style="background:#000;padding:40px 0;font-family:-apple-system,sans-serif;">
  <div style="max-width:600px;margin:0 auto;background:#0b0b0f;border:1px solid rgba(255,255,255,0.08);border-radius:24px;padding:40px;color:#fff;">
    <div style="font-size:12px;letter-spacing:2px;text-transform:uppercase;color:#6b7280;">ChainPulse Quant</div>
    <h1 style="margin:16px 0 8px;font-size:26px;">Confirm Your Subscription</h1>
    <p style="color:#9ca3af;font-size:15px;line-height:1.6;">You're one click away from receiving your Daily Regime Brief.</p>
    <div style="margin:30px 0;">
      <a href="{confirmation_link}" style="background:#fff;color:#000;padding:14px 28px;border-radius:14px;text-decoration:none;font-weight:600;display:inline-block;">Confirm Subscription</a>
    </div>
  </div>
</div>
"""
    try:
        send_email(email, "Confirm your Daily Regime Brief", html)
        logger.info(f"Confirmation email sent to {email}")
    except Exception as e:
        logger.error(f"Failed to send confirmation email to {email}: {e}")
        return {"status": "registered", "email_sent": False}
    return {"status": "confirmation_sent", "email_sent": True}


# ── Confirm ──────────────────────────────────
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
<head><title>Subscription Confirmed</title>
<style>
  body {{ background-color: #000; color: #fff; font-family: -apple-system, BlinkMacSystemFont, sans-serif; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; }}
  .card {{ background: rgba(255,255,255,0.05); border: 1px solid rgba(255,255,255,0.1); padding: 50px; border-radius: 24px; text-align: center; backdrop-filter: blur(12px); box-shadow: 0 20px 60px rgba(0,0,0,0.6); }}
  .btn {{ display: inline-block; margin-top: 25px; padding: 14px 28px; background: white; color: black; border-radius: 14px; text-decoration: none; font-weight: 600; transition: 0.2s ease; }}
  .btn:hover {{ transform: translateY(-2px); }}
</style>
</head>
<body>
  <div class="card">
    <h1>✅ Subscription Confirmed</h1>
    <p>Your Daily Regime Brief is now active.</p>
    <a href="https://chainpulse.pro/app" class="btn">Go to Dashboard</a>
  </div>
</body>
</html>
""")


# ── Restore Access ────────────────────────────
# FIX 1.3: Rate limiting note — in production add slowapi or similar
# FIX 1.3: Token created_at set on rotation
@app.post("/restore-access")
def restore_access(body: RestoreRequest, request: Request, db: Session = Depends(get_db)):
    rate_limiter.require(request, max_requests=3, window_seconds=3600)
    email = body.email.strip().lower()
    user = db.query(User).filter(User.email == email).first()
    if not user or user.subscription_status != "active":
        raise HTTPException(status_code=404, detail="No active Pro subscription found")
    user.access_token = str(uuid.uuid4())
    user.token_created_at = datetime.datetime.utcnow()  # FIX 1.3
    db.commit()
    send_email(email, "ChainPulse Pro — Your Login Link", welcome_email_html(email, user.access_token))
    return {"status": "sent"}


# ── User Status ──────────────────────────────
@app.get("/user-status")
def user_status(request: Request, db: Session = Depends(get_db)):
    user_info = resolve_user_tier(get_auth_header(request), db)
    return {
        "is_pro": user_info["is_pro"],
        "tier": user_info["tier"],
        "timestamp": datetime.datetime.utcnow(),
    }


# ── Ticker ───────────────────────────────────
@app.get("/ticker")
def ticker():
    symbols = [f"{c}USDT" for c in SUPPORTED_COINS]
    try:
        r = requests.get("https://api.binance.com/api/v3/ticker/24hr", params={"symbols": json.dumps(symbols)}, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.error(f"Ticker fetch failed: {e}")
        return []


# ── Debug Prices ─────────────────────────────
@app.get("/debug-prices")
def debug_prices(coin: str = "BTC", interval: str = "1h"):
    prices, volumes = get_klines(coin, interval, limit=120)
    return {
        "coin": coin, "interval": interval,
        "price_count": len(prices), "volume_count": len(volumes),
        "last_price": prices[-1] if prices else None,
        "first_price": prices[0] if prices else None,
        "last_volume": volumes[-1] if volumes else None,
    }


# ── Debug Stack ──────────────────────────────
@app.get("/debug-stack")
def debug_stack(coin: str = "BTC", db: Session = Depends(get_db)):
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")
    stack = build_regime_stack(coin, db)
    breadth = compute_market_breadth(db)
    quality = compute_regime_quality(stack) if not stack["incomplete"] else None
    return {"stack": stack, "breadth": breadth, "quality": quality}


# ── Sample Report ────────────────────────────
@app.get("/sample-report")
def sample_report():
    path = "sample_report.pdf"
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Report not found")
    return FileResponse(path, media_type="application/pdf")


# ── Alert Dispatch (PRO — internal cron) ─────
@app.get("/send-alerts")
def send_alerts(secret: str = "", db: Session = Depends(get_db)):
    if secret != UPDATE_SECRET:
        raise HTTPException(status_code=403, detail="Unauthorized")

    pro_users = db.query(User).filter(
        User.subscription_status == "active",
        User.alerts_enabled == True,
        User.tier.in_(["essential", "pro", "institutional"])
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
            # ── Priority delivery by tier ──
            if user.tier == "institutional":
                min_hours = 2
            elif user.tier == "pro":
                min_hours = 6
            else:  # essential
                min_hours = 12

            if user.last_alert_sent:
                hrs = (datetime.datetime.utcnow() - user.last_alert_sent).total_seconds() / 3600
                if hrs < min_hours:
                    continue

            subject_prefix = "⚡ Priority: " if user.tier == "institutional" else ""
            send_email(
                user.email,
                f"{subject_prefix}ChainPulse Alert — {coin} Regime Shift Risk Elevated",
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
    subscribers = db.query(User).filter(User.alerts_enabled == True).all()
    stacks = []
    for coin in SUPPORTED_COINS:
        stack = build_regime_stack(coin, db)
        if not stack["incomplete"]:
            stacks.append(stack)
    sent = 0
    for user in subscribers:
        send_email(user.email, "ChainPulse Morning Regime Brief", morning_email_html(stacks, user.access_token or ""))
        sent += 1
    return {"status": "sent", "count": sent}


# ── Weekly Discipline Email (PRO — internal cron) ──
@app.get("/send-weekly-discipline")
def send_weekly_discipline(secret: str = "", db: Session = Depends(get_db)):
    if secret != UPDATE_SECRET:
        raise HTTPException(status_code=403, detail="Unauthorized")
    pro_users = db.query(User).filter(User.subscription_status == "active", User.alerts_enabled == True).all()
    sent = 0
    errors = 0
    for user in pro_users:
        try:
            cutoff = datetime.datetime.utcnow() - datetime.timedelta(days=7)
            logs = db.query(ExposureLog).filter(ExposureLog.email == user.email, ExposureLog.created_at >= cutoff).order_by(ExposureLog.created_at.desc()).all()
            discipline = compute_discipline_score(logs)
            if discipline.get("total", 0) == 0:
                continue
            send_email(user.email, "ChainPulse — Your Weekly Discipline Summary", weekly_discipline_email_html(email=user.email, discipline=discipline, access_token=user.access_token or ""))
            sent += 1
        except Exception as e:
            logger.error(f"Weekly discipline email failed for {user.email}: {e}")
            errors += 1
    return {"status": "complete", "sent": sent, "errors": errors}


# ─────────────────────────────────────────
# FIX 2.3: Onboarding Drip Email Cron
# ─────────────────────────────────────────
@app.get("/send-onboarding-drip")
def send_onboarding_drip(secret: str = "", db: Session = Depends(get_db)):
    """
    FIX 2.3: Sends onboarding drip emails to trial users.
    Call via cron every 6-12 hours.
    """
    if secret != UPDATE_SECRET:
        raise HTTPException(status_code=403, detail="Unauthorized")

    now = datetime.datetime.utcnow()
    users = db.query(User).filter(
        User.subscription_status == "active",
        User.trial_start_date.isnot(None),
    ).all()

    sent = 0
    errors = 0

    for user in users:
        try:
            days_since_trial = (now - user.trial_start_date).days

            if days_since_trial == 0 and user.onboarding_step < 1:
                # Day 0: Welcome with current regime
                stack = build_regime_stack("BTC", db)
                send_email(user.email, "Welcome to ChainPulse Pro — Your first action", onboarding_day0_html(user.email, user.access_token or "", stack))
                user.onboarding_step = 1
                sent += 1

            elif days_since_trial >= 2 and user.onboarding_step < 2:
                send_email(user.email, "Day 2: Log your first exposure", onboarding_day2_html(user.email, user.access_token or ""))
                user.onboarding_step = 2
                sent += 1

            elif days_since_trial >= 5 and user.onboarding_step < 5:
                send_email(user.email, "Day 5: Your behavior profile is ready", onboarding_day5_html(user.email, user.access_token or ""))
                user.onboarding_step = 5
                sent += 1

            elif days_since_trial >= 6 and user.onboarding_step < 6:
                send_email(user.email, "Your trial ends tomorrow — here's what you'll lose", onboarding_day6_html(user.email, user.access_token or ""))
                user.onboarding_step = 6
                sent += 1

            db.commit()

        except Exception as e:
            logger.error(f"Onboarding drip failed for {user.email}: {e}")
            errors += 1

    return {"status": "complete", "sent": sent, "errors": errors}


# ─────────────────────────────────────────
# FIX 2.4: Churn Risk Endpoint
# ─────────────────────────────────────────
@app.get("/admin/churn-risk")
def churn_risk(secret: str = "", db: Session = Depends(get_db)):
    if secret != UPDATE_SECRET:
        raise HTTPException(status_code=403, detail="Unauthorized")

    now = datetime.datetime.utcnow()
    users = db.query(User).filter(User.subscription_status == "active").all()

    at_risk = []
    for user in users:
        if not user.last_active_at:
            at_risk.append({"email": user.email, "risk": "critical", "reason": "Never logged in"})
        elif (now - user.last_active_at).days > 7:
            at_risk.append({"email": user.email, "risk": "high", "days_inactive": (now - user.last_active_at).days})
        elif (now - user.last_active_at).days > 3:
            at_risk.append({"email": user.email, "risk": "medium", "days_inactive": (now - user.last_active_at).days})

    return {"at_risk_users": at_risk, "total_active": len(users)}


# ─────────────────────────────────────────
# DASHBOARD (BATCHED ENDPOINT — original)
# ─────────────────────────────────────────
@app.get("/dashboard")
def dashboard(request: Request, coin: str = "BTC", db: Session = Depends(get_db)):
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")

    authorization = get_auth_header(request)
    is_pro = resolve_pro_status(authorization, db)
    user_info = resolve_user_tier(authorization, db)
    tier = user_info["tier"]
    user_info = resolve_user_tier(authorization, db)
    tier = user_info["tier"]

    stack_response = regime_stack_endpoint(request, coin, db)
    latest_data = latest(coin, db)
    history_data = regime_history(coin, "1h", 48, db)
    overview_data = market_overview(request, "ALL", db)
    events_data = risk_events()

    curve_data = None
    transitions_data = None
    vol_env_data = None
    correlation_data = None
    confidence_data = None

    if is_pro:
        try:
            curve_data = survival_curve(request, coin, "1h", db)
        except Exception:
            curve_data = {"data": []}
        try:
            transitions_data = regime_transitions(request, coin, "1h", db)
        except Exception:
            transitions_data = None
        try:
            vol_env_data = volatility_env(request, coin, db)
        except Exception:
            vol_env_data = None
        try:
            correlation_data = correlation_endpoint(request, ",".join(SUPPORTED_COINS), db)
        except Exception:
            correlation_data = None
        try:
            confidence_data = regime_confidence_endpoint(request, coin, db)
        except Exception:
            confidence_data = None

    return {
        "stack": stack_response,
        "pro_required": not is_pro,
        "tier": tier,
        "latest": latest_data,
        "history": history_data.get("data") if history_data else [],
        "overview": overview_data.get("data") if overview_data else [],
        "breadth": overview_data.get("breadth") if overview_data else None,
        "confidence": confidence_data,
        "volEnv": vol_env_data,
        "transitions": transitions_data,
        "correlation": correlation_data,
        "curve": curve_data.get("data") if curve_data else [],
        "events": events_data.get("events") if events_data else [],
    }


# ═════════════════════════════════════════════════
# NEW PREMIUM ENDPOINTS
# ═════════════════════════════════════════════════

# ── Setup Quality ────────────────────────────
@app.get("/setup-quality")
def setup_quality_endpoint(request: Request, coin: str = "BTC", db: Session = Depends(get_db)):
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")
    auth = get_auth_header(request)
    require_tier(auth, db, minimum_tier="pro")
    update_last_active(request, db)

    result = get_or_compute(f"setup_quality:{coin}", compute_setup_quality, ttl=120, coin=coin, db=db)
    return result


# ── Opportunity Ranking ──────────────────────
@app.get("/opportunity-ranking")
def opportunity_ranking_endpoint(request: Request, db: Session = Depends(get_db)):
    auth = get_auth_header(request)
    require_tier(auth, db, minimum_tier="pro")
    update_last_active(request, db)

    result = get_or_compute("opportunity_ranking", compute_opportunity_ranking, ttl=180, db=db)
    return result


# ── Historical Analogs ───────────────────────
@app.get("/historical-analogs")
def historical_analogs_endpoint(request: Request, coin: str = "BTC", db: Session = Depends(get_db)):
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")
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

    result = find_historical_analogs(db=db, coin=coin, target_macro=macro_label, target_trend=trend_label, target_exec=exec_label, target_hazard=hazard)
    if result.get("data_sufficient"):
        cache_set(cache_key, result, ttl=300)
    return result


# ── Probabilistic Scenarios ──────────────────
@app.get("/scenarios")
def scenarios_endpoint(request: Request, coin: str = "BTC", db: Session = Depends(get_db)):
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")
    auth = get_auth_header(request)
    require_tier(auth, db, minimum_tier="pro")
    update_last_active(request, db)

    result = get_or_compute(f"scenarios:{coin}", compute_scenarios, ttl=120, coin=coin, db=db)
    return result


# ── Internal Damage Monitor ──────────────────
@app.get("/internal-damage")
def internal_damage_endpoint(request: Request, coin: str = "BTC", db: Session = Depends(get_db)):
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")
    auth = get_auth_header(request)
    require_tier(auth, db, minimum_tier="pro")
    update_last_active(request, db)

    result = get_or_compute(f"damage:{coin}", compute_internal_damage, ttl=120, coin=coin, db=db)
    return result


# ── Behavioral Alpha Report ──────────────────
@app.get("/behavioral-alpha")
def behavioral_alpha_endpoint(request: Request, email: str = "", lookback_days: int = 30, db: Session = Depends(get_db)):
    if not email:
        raise HTTPException(status_code=400, detail="Email required")
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="pro")
    email = require_email_ownership(user_info, email)
    update_last_active(request, db)
    lookback_days = min(max(7, lookback_days), 90)
    return compute_behavioral_alpha_report(email, db, lookback_days)


# ── Trade Plan Generator ─────────────────────
@app.post("/trade-plan")
def trade_plan_endpoint(request: Request, body: TradePlanRequest, db: Session = Depends(get_db)):
    if body.coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")
    if body.account_size <= 0:
        raise HTTPException(status_code=400, detail="Invalid account size")
    if body.strategy_mode not in ARCHETYPE_CONFIG:
        raise HTTPException(status_code=400, detail=f"Invalid strategy. Choose from: {list(ARCHETYPE_CONFIG.keys())}")
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="pro")
    email = require_email_ownership(user_info, body.email)
    email = body.email.strip().lower()
    return compute_trade_plan(coin=body.coin, account_size=body.account_size, strategy_mode=body.strategy_mode, db=db, email=email)


# ── Event Risk Overlay ───────────────────────
@app.get("/event-risk-overlay")
def event_risk_overlay_endpoint(request: Request, coin: str = "BTC", db: Session = Depends(get_db)):
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")
    auth = get_auth_header(request)
    require_tier(auth, db, minimum_tier="pro")
    update_last_active(request, db)

    result = get_or_compute(f"event_risk:{coin}", compute_event_risk_overlay, ttl=300, coin=coin, db=db)
    return result


# ── Trader Archetype Overlay ─────────────────
@app.get("/archetype-overlay")
def archetype_overlay_endpoint(request: Request, coin: str = "BTC", archetype: str = "swing", email: str = "", db: Session = Depends(get_db)):
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")
    if archetype not in ARCHETYPE_CONFIG:
        raise HTTPException(status_code=400, detail=f"Invalid archetype. Choose from: {list(ARCHETYPE_CONFIG.keys())}")
    auth = get_auth_header(request)
    require_tier(auth, db, minimum_tier="institutional")
    update_last_active(request, db)
    return apply_archetype_overlay(coin=coin, archetype=archetype, db=db, email=email.strip().lower() if email else None)


# ── What Changed Brief ───────────────────────
@app.get("/what-changed")
def what_changed_endpoint(request: Request, lookback_hours: int = 24, db: Session = Depends(get_db)):
    auth = get_auth_header(request)
    require_tier(auth, db, minimum_tier="pro")
    update_last_active(request, db)
    lookback_hours = min(max(1, lookback_hours), 168)
    result = get_or_compute(f"what_changed:{lookback_hours}", compute_what_changed, ttl=120, db=db, lookback_hours=lookback_hours)
    return result


# ── Dynamic Alert Thresholds (CRUD) ──────────
@app.post("/alert-thresholds")
def save_alert_thresholds(request: Request, body: AlertThresholdRequest, db: Session = Depends(get_db)):
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="institutional")
    email = require_email_ownership(user_info, body.email)
    existing = db.query(AlertThreshold).filter(AlertThreshold.email == email, AlertThreshold.coin == body.coin).first()
    if existing:
        existing.shift_risk_threshold = body.shift_risk_threshold
        existing.exposure_change_threshold = body.exposure_change_threshold
        existing.setup_quality_threshold = body.setup_quality_threshold
        existing.regime_quality_threshold = body.regime_quality_threshold
    else:
        existing = AlertThreshold(email=email, coin=body.coin, shift_risk_threshold=body.shift_risk_threshold, exposure_change_threshold=body.exposure_change_threshold, setup_quality_threshold=body.setup_quality_threshold, regime_quality_threshold=body.regime_quality_threshold)
        db.add(existing)
    db.commit()
    return {"status": "saved", "email": email, "coin": body.coin, "thresholds": {"shift_risk": body.shift_risk_threshold, "exposure_change": body.exposure_change_threshold, "setup_quality": body.setup_quality_threshold, "regime_quality": body.regime_quality_threshold}}


@app.get("/alert-thresholds")
def get_alert_thresholds(request: Request, email: str = "", db: Session = Depends(get_db)):
    if not email:
        raise HTTPException(status_code=400, detail="Email required")
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="institutional")
    email = require_email_ownership(user_info, email)
    thresholds = db.query(AlertThreshold).filter(AlertThreshold.email == email).all()
    return {"email": email, "thresholds": [{"coin": t.coin, "shift_risk_threshold": t.shift_risk_threshold, "exposure_change_threshold": t.exposure_change_threshold, "setup_quality_threshold": t.setup_quality_threshold, "regime_quality_threshold": t.regime_quality_threshold, "enabled": t.enabled} for t in thresholds]}


# ── Dynamic Alerts Evaluation ────────────────
@app.get("/evaluate-alerts")
def evaluate_alerts_endpoint(request: Request, email: str = "", db: Session = Depends(get_db)):
    if not email:
        raise HTTPException(status_code=400, detail="Email required")
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="pro")
    email = require_email_ownership(user_info, email)
    update_last_active(request, db)
    alerts = evaluate_dynamic_alerts(email, db)
    return {"email": email, "alerts": alerts, "alert_count": len(alerts), "high_severity_count": sum(1 for a in alerts if a.get("severity") == "high"), "timestamp": datetime.datetime.utcnow().isoformat()}


# ── Send Dynamic Alerts (CRON) ───────────────
@app.get("/send-dynamic-alerts")
def send_dynamic_alerts(secret: str = "", db: Session = Depends(get_db)):
    if secret != UPDATE_SECRET:
        raise HTTPException(status_code=403, detail="Unauthorized")

    pro_users = db.query(User).filter(
        User.subscription_status == "active",
        User.alerts_enabled == True,
        User.tier.in_(["essential", "pro", "institutional"])
    ).all()

    sent = 0
    errors = 0

    for user in pro_users:
        try:
            # ── Priority alert delivery by tier ──
            if user.tier == "institutional":
                min_hours_between_alerts = 1    # Fastest: every hour
            elif user.tier == "pro":
                min_hours_between_alerts = 4    # Every 4 hours
            else:  # essential
                min_hours_between_alerts = 8    # Every 8 hours

            if user.last_alert_sent:
                hrs = (datetime.datetime.utcnow() - user.last_alert_sent).total_seconds() / 3600
                if hrs < min_hours_between_alerts:
                    continue

            alerts = evaluate_dynamic_alerts(user.email, db)

            # ── Tier-based alert severity filtering ──
            # Institutional: get all alerts (high + medium)
            # Pro: get high + medium
            # Essential: get high only
            if user.tier == "institutional":
                filtered_alerts = [a for a in alerts if a.get("severity") in ("high", "medium", "positive")]
            elif user.tier == "pro":
                filtered_alerts = [a for a in alerts if a.get("severity") in ("high", "medium")]
            else:  # essential
                filtered_alerts = [a for a in alerts if a.get("severity") == "high"]

            if not filtered_alerts:
                continue

            # ── Priority badge for institutional ──
            priority_badge = ""
            if user.tier == "institutional":
                priority_badge = '<div style="display:inline-block;background:#8b5cf6;color:#fff;padding:4px 12px;border-radius:4px;font-size:11px;text-transform:uppercase;letter-spacing:1px;margin-bottom:16px;">Priority Alert</div>'

            alerts_html = ""
            for alert in filtered_alerts[:8]:  # Institutional gets up to 8, others capped below
                color = "#f87171" if alert["severity"] == "high" else "#facc15" if alert["severity"] == "medium" else "#4ade80"
                alerts_html += f"""
                <div style="border:1px solid {color};padding:16px;margin-bottom:12px;border-radius:8px;">
                    <div style="color:{color};font-weight:600;font-size:14px;">{alert.get('coin', '')} — {alert.get('type', '').replace('_', ' ').title()}</div>
                    <div style="color:#ccc;font-size:13px;margin-top:8px;">{alert.get('message', '')}</div>
                    <div style="color:#999;font-size:12px;margin-top:8px;">Action: {alert.get('action', '')}</div>
                </div>
                """

            # Cap alerts shown by tier
            if user.tier == "essential":
                max_alerts_shown = 3
            elif user.tier == "pro":
                max_alerts_shown = 5
            else:  # institutional
                max_alerts_shown = 8

            alert_count = min(len(filtered_alerts), max_alerts_shown)
            remaining = len(filtered_alerts) - alert_count
            remaining_note = f'<p style="color:#666;font-size:12px;margin-top:8px;">+ {remaining} more alerts on your dashboard</p>' if remaining > 0 else ""

            tier_label = user.tier.title() if user.tier else "Pro"

            email_html = f"""
<div style="font-family:sans-serif;max-width:560px;margin:0 auto;background:#000;color:#fff;padding:40px;">
  <div style="font-size:11px;color:#555;text-transform:uppercase;letter-spacing:2px;margin-bottom:16px;">ChainPulse {tier_label} Alert</div>
  {priority_badge}
  <h2 style="color:#f87171;margin-bottom:24px;">{alert_count} Alert{"s" if alert_count > 1 else ""}</h2>
  {alerts_html}
  {remaining_note}
  <a href="{FRONTEND_URL}/app?token={user.access_token or ''}" style="display:inline-block;background:#fff;color:#000;padding:14px 28px;margin-top:24px;text-decoration:none;font-weight:bold;border-radius:4px;">Open Dashboard</a>
  <p style="color:#333;font-size:11px;margin-top:40px;border-top:1px solid #111;padding-top:20px;">ChainPulse. Not financial advice.</p>
</div>
"""
            send_email(
                user.email,
                f"ChainPulse {'⚡ Priority' if user.tier == 'institutional' else ''} Alert — {alert_count} {'High Priority' if any(a['severity'] == 'high' for a in filtered_alerts) else 'Active'}",
                email_html,
            )
            user.last_alert_sent = datetime.datetime.utcnow()
            db.commit()
            sent += 1
        except Exception as e:
            logger.error(f"Dynamic alert failed for {user.email}: {e}")
            errors += 1

    return {"status": "complete", "alerts_sent": sent, "errors": errors}


# ── Archetype List ───────────────────────────
@app.get("/archetypes")
def list_archetypes():
    return {"archetypes": {key: {"label": config["label"], "description": config["description"], "exposure_mult": config["exposure_mult"], "alert_sensitivity": config["alert_sensitivity"], "preferred_timeframe": config["preferred_timeframe"], "max_hold_days": config["max_hold_days"], "stop_width_mult": config["stop_width_mult"], "playbook_bias": config["playbook_bias"]} for key, config in ARCHETYPE_CONFIG.items()}}


# ── Save User Archetype ─────────────────────
@app.post("/save-archetype")
def save_archetype_endpoint(request: Request, body: TraderArchetype, db: Session = Depends(get_db)):
    if body.archetype not in ARCHETYPE_CONFIG:
        raise HTTPException(status_code=400, detail=f"Invalid archetype. Choose from: {list(ARCHETYPE_CONFIG.keys())}")
    user_info = require_tier(get_auth_header(request), db, minimum_tier="essential")
    email = require_email_ownership(user_info, body.email)
    config = ARCHETYPE_CONFIG[body.archetype]
    profile = db.query(UserProfile).filter(UserProfile.email == email).first()
    if not profile:
        user = db.query(User).filter(User.email == email).first()
        profile = UserProfile(email=email, user_id=user.id if user else None)
        db.add(profile)
    profile.risk_identity = body.archetype
    profile.risk_multiplier = config["exposure_mult"]
    profile.holding_period_days = config["max_hold_days"]
    profile.updated_at = datetime.datetime.utcnow()
    db.commit()
    return {"status": "saved", "email": email, "archetype": body.archetype, "archetype_label": config["label"], "exposure_multiplier": config["exposure_mult"], "max_hold_days": config["max_hold_days"], "alert_sensitivity": config["alert_sensitivity"]}


# ─────────────────────────────────────────
# FULL PREMIUM DASHBOARD — FIX 1.1: Compute shared dependencies ONCE
# ─────────────────────────────────────────
@app.get("/premium-dashboard")
async def premium_dashboard(request: Request, coin: str = "BTC", email: str = "", db: Session = Depends(get_db)):
    rate_limiter.require(request, max_requests=30, window_seconds=60)
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="essential")
    tier = user_info["tier"]
    update_last_active(request, db)

    start = time.perf_counter()
    if email:
        email = require_email_ownership(user_info, email)

    # ─── FIX 1.1: Fetch ALL market data ONCE ───
    market_data = fetch_all_market_data(coin)

    # ─── FIX 1.1: Build stack ONCE ───
    stack = build_regime_stack(coin, db)

    # ─── FIX 1.1: Compute setup quality ONCE (pass market_data + stack) ───
    try:
        setup = compute_setup_quality(coin, db, market_data=market_data, stack=stack)
    except Exception:
        setup = {"setup_quality_score": None, "error": "Failed"}

    # ── Regime Quality ──
    quality = compute_regime_quality(stack) if not stack.get("incomplete") else None

    # ── Statistics ──
    age_1h = current_age(db, coin, "1h")
    avg_dur = average_regime_duration(db, coin, "1h")
    hazard_val = stack.get("hazard") or 0
    maturity = trend_maturity_score(age_1h, avg_dur, hazard_val)
    exec_score = stack["execution"]["score"] if stack.get("execution") else 0
    pct_rank = percentile_rank(db, coin, exec_score, "1h")

    # ── Decision Engine ──
    breadth = compute_market_breadth(db)
    decision = None
    if not stack.get("incomplete"):
        try:
            decision = compute_decision_score(
                hazard=hazard_val, shift_risk=stack.get("shift_risk") or 0,
                alignment=stack.get("alignment") or 0, survival=stack.get("survival") or 50,
                breadth_score=breadth.get("breadth_score", 0), maturity_pct=maturity,
            )
            exec_label = stack["execution"]["label"] if stack.get("execution") else "Neutral"
            decision["regime"] = exec_label
            decision["exposure"] = stack.get("exposure", 50)
            decision["coin"] = coin
            decision["model_version"] = MODEL_VERSION
        except Exception:
            decision = None

    # ── FIX 1.1: Scenarios — pass pre-built stack and setup ──
    try:
        scenarios = compute_scenarios(coin, db, stack=stack, setup=setup)
    except Exception:
        scenarios = None

    # ── FIX 1.1: Internal Damage — pass pre-fetched market_data and stack ──
    try:
        damage = compute_internal_damage(coin, db, market_data=market_data, stack=stack)
    except Exception:
        damage = None

    # ── FIX 1.1: Event Risk — pass pre-built stack ──
    try:
        event_risk = compute_event_risk_overlay(coin, db, stack=stack)
    except Exception:
        event_risk = None

    # ── Survival Curve ──
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
                curve.append({"hour": hour, "survival": round(surv_pct, 2), "hazard": round(hz, 2)})
            survival_data = {"data": curve, "source": "historical"}
        else:
            survival_data = {"data": [{"hour": h, "survival": max(0, 100 - h * 4), "hazard": min(100, h * 4.5)} for h in range(25)], "source": "estimated"}
    except Exception:
        survival_data = {"data": [], "source": "error"}

    # ── Transitions ──
    try:
        transitions = regime_transition_matrix(db, coin, "1h")
    except Exception:
        transitions = None

    # ── FIX 1.1: Volatility Environment — pass pre-fetched market_data ──
    try:
        vol_env = volatility_environment(coin, db, market_data=market_data)
    except Exception:
        vol_env = None

    # ── Playbook ──
    exec_label = stack["execution"]["label"] if not stack.get("incomplete") and stack.get("execution") else "Neutral"
    pb = PLAYBOOK_DATA.get(exec_label, PLAYBOOK_DATA["Neutral"])

    # ── History ──
    records = db.query(MarketSummary).filter(MarketSummary.coin == coin, MarketSummary.timeframe == "1h").order_by(MarketSummary.created_at.desc()).limit(48).all()
    records.reverse()
    history = [{"hour": i, "score": r.score, "label": r.label, "coherence": r.coherence, "timestamp": r.created_at} for i, r in enumerate(records)]

    # ── User-specific data ──
    discipline = None
    behavioral = None
    user_alerts = None

    if email:
        try:
            logs = db.query(ExposureLog).filter(ExposureLog.email == email).order_by(ExposureLog.created_at.desc()).limit(30).all()
            discipline = compute_discipline_score(logs)
        except Exception:
            discipline = None
        try:
            behavioral = compute_behavioral_alpha_report(email, db, 30)
        except Exception:
            behavioral = None
        try:
            user_alerts = evaluate_dynamic_alerts(email, db)
        except Exception:
            user_alerts = None

    duration_ms = round((time.perf_counter() - start) * 1000, 2)
    logger.info(f"premium_dashboard | coin={coin} | duration_ms={duration_ms}")

    return {
        "coin": coin,
        "stack": {
            "coin": stack["coin"], "macro": stack.get("macro"), "trend": stack.get("trend"),
            "execution": stack.get("execution"), "alignment": stack.get("alignment"),
            "direction": stack.get("direction"), "exposure": stack.get("exposure"),
            "shift_risk": stack.get("shift_risk"), "survival": stack.get("survival"),
            "hazard": stack.get("hazard"), "incomplete": stack.get("incomplete", False),
            "pro_required": False, "tier": tier,
        },
        "quality": quality, "setup": setup, "decision": decision,
        "scenarios": scenarios, "damage": damage, "event_risk": event_risk,
        "survival_curve": survival_data, "transitions": transitions,
        "volatility_env": vol_env,
        "playbook": {
            "regime": exec_label, "strategy_mode": pb["strategy_mode"],
            "exposure_band": pb["exposure_band"], "trend_follow_wr": pb["trend_follow_wr"],
            "mean_revert_wr": pb["mean_revert_wr"], "avg_remaining_days": pb["avg_remaining_days"],
            "data_source": pb.get("data_source", "backtested_estimates"),  # FIX 4.3
            "actions": pb["actions"], "avoid": pb["avoid"],
        },
        "breadth": breadth, "history": history,
        "statistics": {
            "trend_maturity": maturity, "percentile": pct_rank,
            "regime_age_hours": round(age_1h, 2), "avg_regime_duration_hours": round(avg_dur, 2),
        },
        "discipline": discipline, "behavioral_alpha": behavioral,
        "user_alerts": user_alerts, "model_version": MODEL_VERSION, "duration_ms": duration_ms,
    }


# ── Send What Changed Email (CRON) ──────────
@app.get("/send-what-changed")
def send_what_changed_email(secret: str = "", db: Session = Depends(get_db)):
    if secret != UPDATE_SECRET:
        raise HTTPException(status_code=403, detail="Unauthorized")

    what_changed = compute_what_changed(db, lookback_hours=72)
    pro_users = db.query(User).filter(User.subscription_status == "active", User.alerts_enabled == True).all()

    changes_html = ""
    for change in what_changed.get("changes", [])[:10]:
        color = "#4ade80" if change["severity"] == "positive" else "#f87171"
        changes_html += f"""
        <tr>
          <td style="padding:10px 8px;border-bottom:1px solid #1f1f1f;color:#fff;font-weight:600;">{change['coin']}</td>
          <td style="padding:10px 8px;border-bottom:1px solid #1f1f1f;color:#999;font-size:12px;">{change['timeframe_label']}</td>
          <td style="padding:10px 8px;border-bottom:1px solid #1f1f1f;color:#999;font-size:12px;">{change['previous']}</td>
          <td style="padding:10px 8px;border-bottom:1px solid #1f1f1f;color:{color};font-weight:600;">→ {change['current']}</td>
        </tr>
        """

    if not changes_html:
        changes_html = '<tr><td colspan="4" style="padding:16px;color:#555;font-size:13px;">No regime changes in the last 72 hours. Market stable.</td></tr>'

    takeaways_html = ""
    for t in what_changed.get("takeaways", []):
        takeaways_html += f'<li style="color:#999;font-size:13px;line-height:2;">{t}</li>'

    tone = what_changed.get("tone", "stable")
    tone_color = {"improving": "#4ade80", "deteriorating": "#f87171", "mixed": "#facc15", "stable": "#999"}.get(tone, "#999")

    sent = 0
    errors = 0

    for user in pro_users:
        try:
            url = f"{FRONTEND_URL}/app?token={user.access_token}" if user.access_token else f"{FRONTEND_URL}/app"
            email_html = f"""
<div style="font-family:sans-serif;max-width:640px;margin:0 auto;background:#000;color:#fff;padding:40px;">
  <div style="font-size:11px;color:#555;text-transform:uppercase;letter-spacing:2px;margin-bottom:16px;">ChainPulse Intelligence Brief</div>
  <h1 style="font-size:22px;margin-bottom:8px;">What Changed — Last 72 Hours</h1>
  <p style="color:{tone_color};font-size:14px;margin-bottom:24px;">{what_changed.get('headline', 'No major changes')}</p>
  <table style="width:100%;border-collapse:collapse;margin-bottom:24px;">
    <thead><tr>
      <th style="text-align:left;padding:8px;color:#444;font-size:11px;text-transform:uppercase;border-bottom:1px solid #222;">Asset</th>
      <th style="text-align:left;padding:8px;color:#444;font-size:11px;text-transform:uppercase;border-bottom:1px solid #222;">Timeframe</th>
      <th style="text-align:left;padding:8px;color:#444;font-size:11px;text-transform:uppercase;border-bottom:1px solid #222;">Previous</th>
      <th style="text-align:left;padding:8px;color:#444;font-size:11px;text-transform:uppercase;border-bottom:1px solid #222;">Current</th>
    </tr></thead>
    <tbody>{changes_html}</tbody>
  </table>
  <div style="border:1px solid #1f1f1f;padding:20px;margin-bottom:24px;">
    <div style="font-size:11px;color:#555;text-transform:uppercase;letter-spacing:1px;margin-bottom:12px;">Key Takeaways</div>
    <ul style="padding-left:16px;margin:0;">{takeaways_html}</ul>
  </div>
  <div style="margin-bottom:24px;">
    <div style="font-size:11px;color:#555;text-transform:uppercase;letter-spacing:1px;margin-bottom:8px;">Market Breadth</div>
    <p style="color:#999;font-size:13px;">Bullish: {what_changed.get('breadth', {}).get('bullish', 0)} · Neutral: {what_changed.get('breadth', {}).get('neutral', 0)} · Bearish: {what_changed.get('breadth', {}).get('bearish', 0)} · Score: {what_changed.get('breadth', {}).get('breadth_score', 0)}</p>
  </div>
  <a href="{url}" style="display:inline-block;background:#fff;color:#000;padding:14px 28px;text-decoration:none;font-weight:bold;border-radius:4px;">Open Dashboard</a>
  <p style="color:#333;font-size:11px;margin-top:40px;border-top:1px solid #111;padding-top:20px;">ChainPulse. Not financial advice.</p>
</div>
"""
            send_email(user.email, f"ChainPulse — What Changed ({what_changed.get('tone', 'Update').title()})", email_html)
            sent += 1
        except Exception as e:
            logger.error(f"What Changed email failed for {user.email}: {e}")
            errors += 1

    return {"status": "complete", "sent": sent, "errors": errors}


# ── Combined Overview with Setup Quality ─────
@app.get("/premium-overview")
def premium_overview(request: Request, db: Session = Depends(get_db)):
    if not resolve_pro_status(get_auth_header(request), db):
        raise HTTPException(status_code=403, detail="Pro subscription required.")
    update_last_active(request, db)

    start = time.perf_counter()
    cached = cache_get("premium_overview")
    if cached:
        return cached

    coins_data = []
    for coin in SUPPORTED_COINS:
        stack = build_regime_stack(coin, db)
        if stack["incomplete"]:
            continue
        quality = compute_regime_quality(stack)
        # FIX 1.1: Pass stack to avoid redundant build
        try:
            setup = compute_setup_quality(coin, db, stack=stack)
            setup_score = setup.get("setup_quality_score")
            setup_label = setup.get("setup_label")
            entry_mode = setup.get("entry_mode")
            chase_risk = setup.get("chase_risk")
        except Exception:
            setup_score = setup_label = entry_mode = chase_risk = None

        coins_data.append({
            "coin": coin,
            "macro": stack["macro"]["label"] if stack.get("macro") else None,
            "trend": stack["trend"]["label"] if stack.get("trend") else None,
            "execution": stack["execution"]["label"] if stack.get("execution") else None,
            "alignment": stack.get("alignment"), "direction": stack.get("direction"),
            "exposure": stack.get("exposure"), "shift_risk": stack.get("shift_risk"),
            "hazard": stack.get("hazard"), "survival": stack.get("survival"),
            "quality_grade": quality["grade"], "quality_score": quality["score"],
            "setup_score": setup_score, "setup_label": setup_label,
            "entry_mode": entry_mode, "chase_risk": chase_risk,
        })

    coins_data.sort(key=lambda x: (x.get("setup_score") or 0) * 0.5 + (x.get("quality_score") or 0) * 0.5, reverse=True)
    breadth = compute_market_breadth(db)

    best_long = None
    avoid = []
    for c in coins_data:
        if c["direction"] == "bullish" and best_long is None:
            best_long = c["coin"]
        if (c.get("setup_score") or 0) < 30 or (c.get("chase_risk") or 0) > 80:
            avoid.append(c["coin"])

    result = {"coins": coins_data, "breadth": breadth, "best_long": best_long, "avoid": avoid, "coin_count": len(coins_data), "timestamp": datetime.datetime.utcnow().isoformat()}
    cache_set("premium_overview", result, ttl=120)
    return result


# ─────────────────────────────────────────
# COMPREHENSIVE CRON ENDPOINT — FIX 3.5: Uses BackgroundTasks
# ─────────────────────────────────────────
def run_full_update(db_factory):
    """Background task that runs the full update cycle."""
    db = db_factory()
    try:
        results = {"updates": [], "alerts_sent": 0, "errors": []}

        # 1. Update all coins and timeframes
        for coin in SUPPORTED_COINS:
            for tf in SUPPORTED_TIMEFRAMES:
                try:
                    entry = update_market(coin, tf, db)
                    if entry:
                        results["updates"].append({"coin": coin, "timeframe": tf, "label": entry.label, "score": entry.score})
                except Exception as e:
                    results["errors"].append(f"Update {coin}/{tf}: {str(e)}")

                # 1.5 Trigger webhooks for regime changes
        try:
            for coin in SUPPORTED_COINS:
                stack = build_regime_stack(coin, db)
                if stack.get("incomplete"):
                    continue

                shift_risk = stack.get("shift_risk") or 0
                hazard = stack.get("hazard") or 0
                exec_label = stack["execution"]["label"] if stack.get("execution") else "Neutral"

                # Trigger regime_change webhook
                regime_payload = {
                    "coin": coin,
                    "macro": stack["macro"]["label"] if stack.get("macro") else None,
                    "trend": stack["trend"]["label"] if stack.get("trend") else None,
                    "execution": exec_label,
                    "alignment": stack.get("alignment"),
                    "direction": stack.get("direction"),
                    "exposure": stack.get("exposure"),
                    "shift_risk": shift_risk,
                    "hazard": hazard,
                    "survival": stack.get("survival"),
                }
                trigger_webhooks("regime_change", regime_payload, db, coin=coin)

                # Trigger shift_risk_alert if elevated
                if shift_risk > 65:
                    trigger_webhooks("shift_risk_alert", {
                        "coin": coin,
                        "shift_risk": shift_risk,
                        "hazard": hazard,
                        "regime": exec_label,
                        "exposure": stack.get("exposure"),
                        "message": f"{coin} shift risk elevated at {shift_risk}%",
                    }, db, coin=coin)

                # Trigger setup_quality_alert if good setup
                try:
                    setup = compute_setup_quality(coin, db, stack=stack)
                    setup_score = setup.get("setup_quality_score") or 0
                    if setup_score >= 70:
                        trigger_webhooks("setup_quality_alert", {
                            "coin": coin,
                            "setup_score": setup_score,
                            "setup_label": setup.get("setup_label"),
                            "entry_mode": setup.get("entry_mode"),
                            "chase_risk": setup.get("chase_risk"),
                        }, db, coin=coin)
                except Exception:
                    pass

        except Exception as e:
            results["errors"].append(f"Webhook dispatch: {str(e)}")

        # 2. Send dynamic alerts
        try:
            pro_users = db.query(User).filter(User.subscription_status == "active", User.alerts_enabled == True).all()

            for user in pro_users:
                try:
                    # ── Priority delivery by tier ──
                    if user.tier == "institutional":
                        min_hours = 1
                    elif user.tier == "pro":
                        min_hours = 4
                    else:  # essential
                        min_hours = 8

                    if user.last_alert_sent:
                        hrs = (datetime.datetime.utcnow() - user.last_alert_sent).total_seconds() / 3600
                        if hrs < min_hours:
                            continue

                    alerts = evaluate_dynamic_alerts(user.email, db)

                    # Filter by tier
                    if user.tier == "institutional":
                        high_alerts = [a for a in alerts if a.get("severity") in ("high", "medium", "positive")]
                    elif user.tier == "pro":
                        high_alerts = [a for a in alerts if a.get("severity") in ("high", "medium")]
                    else:
                        high_alerts = [a for a in alerts if a.get("severity") == "high"]

                    if not high_alerts:
                        continue

                    alert_lines = []
                    for a in high_alerts[:3]:
                        alert_lines.append(f"• {a.get('coin', '')} — {a.get('message', '')}")
                    alert_text = "<br>".join(alert_lines)

                    priority_prefix = "⚡ Priority " if user.tier == "institutional" else ""

                    send_email(
                        user.email,
                        f"ChainPulse — {priority_prefix}{len(high_alerts)} Alert{'s' if len(high_alerts) > 1 else ''}",
                        f"""
<div style="font-family:sans-serif;max-width:560px;margin:0 auto;background:#000;color:#fff;padding:40px;">
  <div style="font-size:11px;color:#555;text-transform:uppercase;letter-spacing:2px;margin-bottom:16px;">ChainPulse Alert</div>
  <h2 style="color:#f87171;margin-bottom:24px;">{priority_prefix}{len(high_alerts)} Alert{'s' if len(high_alerts) > 1 else ''}</h2>
  <div style="color:#ccc;font-size:14px;line-height:2;">{alert_text}</div>
  <a href="{FRONTEND_URL}/app?token={user.access_token or ''}" style="display:inline-block;background:#fff;color:#000;padding:14px 28px;margin-top:24px;text-decoration:none;font-weight:bold;border-radius:4px;">Open Dashboard</a>
  <p style="color:#333;font-size:11px;margin-top:40px;">ChainPulse. Not financial advice.</p>
</div>
""",
                    )
                    user.last_alert_sent = datetime.datetime.utcnow()
                    db.commit()
                    results["alerts_sent"] += 1
                except Exception as e:
                    results["errors"].append(f"Alert {user.email}: {str(e)}")
        except Exception as e:
            results["errors"].append(f"Alert dispatch: {str(e)}")

        logger.info(f"cron_all complete: {len(results['updates'])} updates, {results['alerts_sent']} alerts, {len(results['errors'])} errors")
    finally:
        db.close()


@app.get("/cron-all")
def cron_all(secret: str = "", background_tasks: BackgroundTasks = None, db: Session = Depends(get_db)):
    """
    FIX 3.5: Master cron endpoint. Kicks off background task to avoid timeout.
    Returns immediately with status "started".
    """
    if secret != UPDATE_SECRET:
        raise HTTPException(status_code=403, detail="Unauthorized")

    if background_tasks:
        # FIX 3.5: Run in background to avoid 30-second timeout
        background_tasks.add_task(run_full_update, SessionLocal)
        return {"status": "started", "message": "Update running in background", "timestamp": datetime.datetime.utcnow().isoformat()}
    else:
        # Fallback: run synchronously (for testing)
        run_full_update(SessionLocal)
        return {"status": "complete", "timestamp": datetime.datetime.utcnow().isoformat()}


# ─────────────────────────────────────────
# UPDATED DASHBOARD V2 — FIX 1.1: shared computation
# ─────────────────────────────────────────
@app.get("/dashboard-v2")
def dashboard_v2(request: Request, coin: str = "BTC", email: str = "", db: Session = Depends(get_db)):
    rate_limiter.require(request, max_requests=30, window_seconds=60)
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")

    authorization = get_auth_header(request)
    user_info = resolve_user_tier(authorization, db)
    is_pro = user_info["is_pro"]
    tier = user_info["tier"]
    if email:
        email = require_email_ownership(user_info, email)

    # ── Core (Free) ──
    stack_response = regime_stack_endpoint(request, coin, db)
    latest_data = latest(coin, db)

    records = db.query(MarketSummary).filter(MarketSummary.coin == coin, MarketSummary.timeframe == "1h").order_by(MarketSummary.created_at.desc()).limit(48).all()
    records.reverse()
    history_data = [{"hour": i, "score": r.score, "label": r.label, "coherence": r.coherence, "timestamp": r.created_at} for i, r in enumerate(records)]

    overview_data = market_overview(request, "ALL", db)
    events_data = RISK_EVENTS

    result = {
        "stack": stack_response, "latest": latest_data, "history": history_data,
        "overview": overview_data.get("data") if overview_data else [],
        "breadth": overview_data.get("breadth") if overview_data else None,
        "events": events_data, "is_pro": is_pro, "tier": tier,
    }

    if not is_pro:
        result["pro_features_available"] = [
            "setup_quality", "scenarios", "internal_damage", "event_risk",
            "trade_plan", "behavioral_alpha", "opportunity_ranking",
            "historical_analogs", "archetype_overlay", "what_changed",
            "dynamic_alerts", "premium_overview",
        ]
        return result

    # ── Pro Data — FIX 1.1: shared computation ──
    update_last_active(request, db)

    # Fetch shared data ONCE
    market_data = fetch_all_market_data(coin)
    stack = build_regime_stack(coin, db)
    breadth = compute_market_breadth(db)

    # Setup Quality (shared)
    try:
        result["setup_quality"] = compute_setup_quality(coin, db, market_data=market_data, stack=stack)
    except Exception:
        result["setup_quality"] = None

    # Decision Engine
    if not stack.get("incomplete"):
        try:
            hazard = stack.get("hazard") or 0
            age_1h = current_age(db, coin, "1h")
            avg_dur = average_regime_duration(db, coin, "1h")
            maturity = trend_maturity_score(age_1h, avg_dur, hazard)
            decision = compute_decision_score(
                hazard=hazard, shift_risk=stack.get("shift_risk") or 0,
                alignment=stack.get("alignment") or 0, survival=stack.get("survival") or 50,
                breadth_score=breadth.get("breadth_score", 0), maturity_pct=maturity,
            )
            exec_label = stack["execution"]["label"] if stack.get("execution") else "Neutral"
            decision["regime"] = exec_label
            decision["exposure"] = stack.get("exposure", 50)
            decision["coin"] = coin
            decision["model_version"] = MODEL_VERSION
            result["decision"] = decision
        except Exception:
            result["decision"] = None
    else:
        result["decision"] = None

    # Scenarios (pass stack + setup)
    try:
        result["scenarios"] = compute_scenarios(coin, db, stack=stack, setup=result.get("setup_quality"))
    except Exception:
        result["scenarios"] = None

    # Internal Damage (pass market_data + stack)
    try:
        result["internal_damage"] = compute_internal_damage(coin, db, market_data=market_data, stack=stack)
    except Exception:
        result["internal_damage"] = None

    # Event Risk (pass stack)
    try:
        result["event_risk"] = compute_event_risk_overlay(coin, db, stack=stack)
    except Exception:
        result["event_risk"] = None

    # Regime Quality
    try:
        result["regime_quality"] = compute_regime_quality(stack) if not stack.get("incomplete") else None
    except Exception:
        result["regime_quality"] = None

    # Survival Curve
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
                curve.append({"hour": hour, "survival": round(surv_pct, 2), "hazard": round(hz, 2)})
            result["survival_curve"] = {"data": curve, "source": "historical"}
        else:
            result["survival_curve"] = {"data": [{"hour": h, "survival": max(0, 100 - h * 4), "hazard": min(100, h * 4.5)} for h in range(25)], "source": "estimated"}
    except Exception:
        result["survival_curve"] = {"data": [], "source": "error"}

    # Transitions
    try:
        result["transitions"] = regime_transition_matrix(db, coin, "1h")
    except Exception:
        result["transitions"] = None

    # Volatility Environment (pass market_data)
    try:
        result["volatility_env"] = volatility_environment(coin, db, market_data=market_data)
    except Exception:
        result["volatility_env"] = None

    # Correlation
    try:
        result["correlation"] = build_correlation_matrix(SUPPORTED_COINS[:5])
    except Exception:
        result["correlation"] = None

    # Confidence
    try:
        survival_val = stack.get("survival") or 50
        coherence_val = stack["execution"]["coherence"] if stack.get("execution") and stack["execution"].get("coherence") else 50
        result["confidence"] = regime_confidence_score(alignment=stack.get("alignment") or 0, survival=survival_val, coherence=coherence_val, breadth_score=breadth.get("breadth_score", 0))
    except Exception:
        result["confidence"] = None

    # Playbook
    try:
        exec_lbl = stack["execution"]["label"] if not stack.get("incomplete") and stack.get("execution") else "Neutral"
        pb = PLAYBOOK_DATA.get(exec_lbl, PLAYBOOK_DATA["Neutral"])
        result["playbook"] = {
            "regime": exec_lbl, "strategy_mode": pb["strategy_mode"],
            "exposure_band": pb["exposure_band"], "trend_follow_wr": pb["trend_follow_wr"],
            "mean_revert_wr": pb["mean_revert_wr"], "avg_remaining_days": pb["avg_remaining_days"],
            "data_source": pb.get("data_source", "backtested_estimates"),
            "actions": pb["actions"], "avoid": pb["avoid"],
        }
    except Exception:
        result["playbook"] = None

    # ── User-Specific ──
    if email:
        try:
            logs = db.query(ExposureLog).filter(ExposureLog.email == email).order_by(ExposureLog.created_at.desc()).limit(30).all()
            result["discipline"] = compute_discipline_score(logs)
        except Exception:
            result["discipline"] = None
        try:
            result["behavioral_alpha"] = compute_behavioral_alpha_report(email, db, 30)
        except Exception:
            result["behavioral_alpha"] = None
        try:
            result["user_alerts"] = evaluate_dynamic_alerts(email, db)
        except Exception:
            result["user_alerts"] = None
        try:
            profile = db.query(UserProfile).filter(UserProfile.email == email).first()
            result["user_profile"] = {"risk_identity": profile.risk_identity, "risk_multiplier": profile.risk_multiplier, "max_drawdown_pct": profile.max_drawdown_pct, "holding_period_days": profile.holding_period_days} if profile else None
        except Exception:
            result["user_profile"] = None

    result["model_version"] = MODEL_VERSION
    return result

# ═════════════════════════════════════════════════
# API v1 — INSTITUTIONAL TIER
# ═════════════════════════════════════════════════

# ── API Key Management ───────────────────────
@app.post("/api/v1/keys")
def create_api_key(body: ApiKeyRequest, request: Request, db: Session = Depends(get_db)):
    """Create a new API key. Requires Institutional tier."""
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="institutional")
    email = require_email_ownership(user_info, body.email)

    # Check existing keys (max 3 per user)
    existing = db.query(ApiKey).filter(ApiKey.email == email, ApiKey.is_active == True).count()
    if existing >= 3:
        raise HTTPException(400, detail="Maximum 3 active API keys per account")

    import secrets as secrets_mod
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


@app.get("/api/v1/keys")
def list_api_keys(request: Request, email: str = "", db: Session = Depends(get_db)):
    """List your API keys (key is masked)."""
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
        ],
    }


@app.delete("/api/v1/keys/{key_id}")
def revoke_api_key(key_id: int, request: Request, email: str = "", db: Session = Depends(get_db)):
    """Revoke an API key."""
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="institutional")
    email = require_email_ownership(user_info, email)

    key = db.query(ApiKey).filter(ApiKey.id == key_id, ApiKey.email == email).first()
    if not key:
        raise HTTPException(404, detail="API key not found")

    key.is_active = False
    db.commit()
    return {"status": "revoked", "key_id": key_id}


# ── API v1 Data Endpoints ────────────────────
@app.get("/api/v1/regime/{coin}")
def api_regime(coin: str, request: Request, db: Session = Depends(get_db)):
    """Get full regime stack for a coin."""
    api_info = require_api_key(request, db)
    coin = coin.upper()
    if coin not in SUPPORTED_COINS:
        raise HTTPException(400, detail=f"Unsupported coin. Choose from: {SUPPORTED_COINS}")

    stack = build_regime_stack(coin, db)
    quality = compute_regime_quality(stack) if not stack.get("incomplete") else None

    return {
        "coin": coin,
        "stack": stack,
        "quality": quality,
        "api_requests_remaining": api_info["requests_remaining"],
        "timestamp": datetime.datetime.utcnow().isoformat(),
    }


@app.get("/api/v1/regime")
def api_regime_all(request: Request, db: Session = Depends(get_db)):
    """Get regime data for all coins."""
    api_info = require_api_key(request, db)

    results = []
    for coin in SUPPORTED_COINS:
        stack = build_regime_stack(coin, db)
        if stack.get("incomplete"):
            continue
        quality = compute_regime_quality(stack)
        results.append({
            "coin": coin,
            "macro": stack["macro"]["label"] if stack.get("macro") else None,
            "trend": stack["trend"]["label"] if stack.get("trend") else None,
            "execution": stack["execution"]["label"] if stack.get("execution") else None,
            "alignment": stack.get("alignment"),
            "direction": stack.get("direction"),
            "exposure": stack.get("exposure"),
            "shift_risk": stack.get("shift_risk"),
            "hazard": stack.get("hazard"),
            "survival": stack.get("survival"),
            "quality_grade": quality["grade"],
            "quality_score": quality["score"],
        })

    return {
        "coins": results,
        "count": len(results),
        "api_requests_remaining": api_info["requests_remaining"],
        "timestamp": datetime.datetime.utcnow().isoformat(),
    }


@app.get("/api/v1/setup-quality/{coin}")
def api_setup_quality(coin: str, request: Request, db: Session = Depends(get_db)):
    """Get setup quality for a coin."""
    api_info = require_api_key(request, db)
    coin = coin.upper()
    if coin not in SUPPORTED_COINS:
        raise HTTPException(400, detail=f"Unsupported coin. Choose from: {SUPPORTED_COINS}")

    setup = compute_setup_quality(coin, db)
    return {
        **setup,
        "api_requests_remaining": api_info["requests_remaining"],
        "timestamp": datetime.datetime.utcnow().isoformat(),
    }


@app.get("/api/v1/scenarios/{coin}")
def api_scenarios(coin: str, request: Request, db: Session = Depends(get_db)):
    """Get probabilistic scenarios for a coin."""
    api_info = require_api_key(request, db)
    coin = coin.upper()
    if coin not in SUPPORTED_COINS:
        raise HTTPException(400, detail=f"Unsupported coin. Choose from: {SUPPORTED_COINS}")

    scenarios = compute_scenarios(coin, db)
    return {
        **scenarios,
        "api_requests_remaining": api_info["requests_remaining"],
    }


@app.get("/api/v1/decision/{coin}")
def api_decision(coin: str, request: Request, db: Session = Depends(get_db)):
    """Get decision engine output for a coin."""
    api_info = require_api_key(request, db)
    coin = coin.upper()
    if coin not in SUPPORTED_COINS:
        raise HTTPException(400, detail=f"Unsupported coin. Choose from: {SUPPORTED_COINS}")

    stack = build_regime_stack(coin, db)
    if stack.get("incomplete"):
        return {"coin": coin, "error": "Insufficient data"}

    breadth = compute_market_breadth(db)
    hazard = stack.get("hazard") or 0
    age_1h = current_age(db, coin, "1h")
    avg_dur = average_regime_duration(db, coin, "1h")
    maturity = trend_maturity_score(age_1h, avg_dur, hazard)

    decision = compute_decision_score(
        hazard=hazard, shift_risk=stack.get("shift_risk") or 0,
        alignment=stack.get("alignment") or 0, survival=stack.get("survival") or 50,
        breadth_score=breadth.get("breadth_score", 0), maturity_pct=maturity,
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


@app.get("/api/v1/opportunity-ranking")
def api_opportunity_ranking(request: Request, db: Session = Depends(get_db)):
    """Get opportunity ranking across all coins."""
    api_info = require_api_key(request, db)

    ranking = compute_opportunity_ranking(db)
    return {
        **ranking,
        "api_requests_remaining": api_info["requests_remaining"],
    }


@app.get("/api/v1/internal-damage/{coin}")
def api_internal_damage(coin: str, request: Request, db: Session = Depends(get_db)):
    """Get internal damage score for a coin."""
    api_info = require_api_key(request, db)
    coin = coin.upper()
    if coin not in SUPPORTED_COINS:
        raise HTTPException(400, detail=f"Unsupported coin. Choose from: {SUPPORTED_COINS}")

    damage = compute_internal_damage(coin, db)
    return {
        **damage,
        "api_requests_remaining": api_info["requests_remaining"],
    }


@app.get("/api/v1/breadth")
def api_breadth(request: Request, db: Session = Depends(get_db)):
    """Get market breadth data."""
    api_info = require_api_key(request, db)

    breadth = compute_market_breadth(db)
    return {
        **breadth,
        "api_requests_remaining": api_info["requests_remaining"],
        "timestamp": datetime.datetime.utcnow().isoformat(),
    }


@app.get("/api/v1/usage")
def api_usage(request: Request, db: Session = Depends(get_db)):
    """Check your API usage."""
    api_info = require_api_key(request, db)
    return {
        "email": api_info["email"],
        "requests_remaining": api_info["requests_remaining"],
        "daily_limit": 1000,
        "timestamp": datetime.datetime.utcnow().isoformat(),
    }


# ═════════════════════════════════════════════════
# WEBHOOKS — INSTITUTIONAL TIER
# ═════════════════════════════════════════════════

# ── Webhook CRUD ─────────────────────────────
@app.post("/api/v1/webhooks")
def create_webhook(body: WebhookCreateRequest, request: Request, db: Session = Depends(get_db)):
    """Create a new webhook endpoint."""
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="institutional")
    email = require_email_ownership(user_info, body.email)

    # Validate URL
    if not body.url.startswith("https://"):
        raise HTTPException(400, detail="Webhook URL must use HTTPS")

    # Max 5 webhooks per user
    existing = db.query(WebhookEndpoint).filter(
        WebhookEndpoint.email == email,
        WebhookEndpoint.is_active == True,
    ).count()
    if existing >= 5:
        raise HTTPException(400, detail="Maximum 5 active webhooks per account")

    # Generate signing secret if not provided
    import secrets as secrets_mod
    webhook_secret = body.secret or f"whsec_{secrets_mod.token_hex(20)}"

    endpoint = WebhookEndpoint(
        email=email,
        url=body.url,
        secret=webhook_secret,
        events=body.events,
    )
    db.add(endpoint)
    db.commit()
    db.refresh(endpoint)

    return {
        "webhook_id": endpoint.id,
        "url": endpoint.url,
        "secret": webhook_secret,
        "events": body.events.split(","),
        "message": "Store the secret securely. Use it to verify webhook signatures.",
        "verification": {
            "header": "X-ChainPulse-Signature",
            "format": "sha256=HMAC_SHA256(payload, secret)",
        },
    }


@app.get("/api/v1/webhooks")
def list_webhooks(request: Request, email: str = "", db: Session = Depends(get_db)):
    """List your webhook endpoints."""
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="institutional")
    email = require_email_ownership(user_info, email)

    endpoints = db.query(WebhookEndpoint).filter(WebhookEndpoint.email == email).all()
    return {
        "webhooks": [
            {
                "id": e.id,
                "url": e.url,
                "events": e.events.split(",") if e.events else [],
                "is_active": e.is_active,
                "failure_count": e.failure_count,
                "last_triggered_at": e.last_triggered_at,
                "created_at": e.created_at,
            }
            for e in endpoints
        ],
    }


@app.put("/api/v1/webhooks/{webhook_id}")
def update_webhook(webhook_id: int, body: WebhookUpdateRequest, request: Request, db: Session = Depends(get_db)):
    """Update a webhook endpoint."""
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="institutional")
    email = require_email_ownership(user_info, body.email)

    endpoint = db.query(WebhookEndpoint).filter(
        WebhookEndpoint.id == webhook_id,
        WebhookEndpoint.email == email,
    ).first()
    if not endpoint:
        raise HTTPException(404, detail="Webhook not found")

    if body.url is not None:
        if not body.url.startswith("https://"):
            raise HTTPException(400, detail="Webhook URL must use HTTPS")
        endpoint.url = body.url
    if body.events is not None:
        endpoint.events = body.events
    if body.is_active is not None:
        endpoint.is_active = body.is_active
        if body.is_active:
            endpoint.failure_count = 0  # Reset on re-enable

    db.commit()
    return {"status": "updated", "webhook_id": webhook_id}


@app.delete("/api/v1/webhooks/{webhook_id}")
def delete_webhook(webhook_id: int, request: Request, email: str = "", db: Session = Depends(get_db)):
    """Delete a webhook endpoint."""
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="institutional")
    email = require_email_ownership(user_info, email)

    endpoint = db.query(WebhookEndpoint).filter(
        WebhookEndpoint.id == webhook_id,
        WebhookEndpoint.email == email,
    ).first()
    if not endpoint:
        raise HTTPException(404, detail="Webhook not found")

    db.delete(endpoint)
    db.commit()
    return {"status": "deleted", "webhook_id": webhook_id}


@app.get("/api/v1/webhooks/{webhook_id}/deliveries")
def webhook_deliveries(webhook_id: int, request: Request, email: str = "", limit: int = 20, db: Session = Depends(get_db)):
    """View recent webhook deliveries."""
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="institutional")
    email = require_email_ownership(user_info, email)

    # Verify ownership
    endpoint = db.query(WebhookEndpoint).filter(
        WebhookEndpoint.id == webhook_id,
        WebhookEndpoint.email == email,
    ).first()
    if not endpoint:
        raise HTTPException(404, detail="Webhook not found")

    deliveries = (
        db.query(WebhookDelivery)
        .filter(WebhookDelivery.endpoint_id == webhook_id)
        .order_by(WebhookDelivery.created_at.desc())
        .limit(min(limit, 50))
        .all()
    )

    return {
        "webhook_id": webhook_id,
        "url": endpoint.url,
        "deliveries": [
            {
                "id": d.id,
                "event_type": d.event_type,
                "success": d.success,
                "response_status": d.response_status,
                "attempt": d.attempt,
                "created_at": d.created_at,
            }
            for d in deliveries
        ],
    }


@app.post("/api/v1/webhooks/{webhook_id}/test")
def test_webhook(webhook_id: int, request: Request, email: str = "", db: Session = Depends(get_db)):
    """Send a test webhook delivery."""
    auth = get_auth_header(request)
    user_info = require_tier(auth, db, minimum_tier="institutional")
    email = require_email_ownership(user_info, email)

    endpoint = db.query(WebhookEndpoint).filter(
        WebhookEndpoint.id == webhook_id,
        WebhookEndpoint.email == email,
    ).first()
    if not endpoint:
        raise HTTPException(404, detail="Webhook not found")

    test_payload = {
        "event": "test",
        "message": "This is a test webhook from ChainPulse",
        "coin": "BTC",
        "regime": "Risk-On",
        "exposure": 65.0,
        "shift_risk": 35.0,
        "timestamp": datetime.datetime.utcnow().isoformat(),
    }

    success = deliver_webhook(endpoint, "test", test_payload, db)
    return {
        "success": success,
        "message": "Test webhook delivered" if success else "Test webhook failed — check your endpoint",
    }