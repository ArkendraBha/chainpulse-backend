import datetime
from sqlalchemy import (
    Column, Integer, String, Float,
    DateTime, Boolean, Index,
)
from app.db.database import Base


class MarketSummary(Base):
    __tablename__ = "market_summary"
    __table_args__ = (
        Index(
            'ix_market_summary_coin_tf_created',
            'coin', 'timeframe', 'created_at'
        ),
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
    token_created_at = Column(DateTime, nullable=True)
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
    __table_args__ = (
        Index(
            'ix_exposure_log_email_coin_created',
            'email', 'coin', 'created_at'
        ),
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
    __table_args__ = (
        Index(
            'ix_performance_email_coin_date',
            'email', 'coin', 'date'
        ),
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
    last_request_date = Column(String, nullable=True)
    daily_limit = Column(Integer, default=1000)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    last_used_at = Column(DateTime, nullable=True)


class WebhookEndpoint(Base):
    __tablename__ = "webhook_endpoints"
    id = Column(Integer, primary_key=True)
    email = Column(String, index=True)
    url = Column(String)
    secret = Column(String, nullable=True)
    events = Column(String, default="regime_change,shift_risk_alert,setup_quality_alert")
    is_active = Column(Boolean, default=True)
    last_triggered_at = Column(DateTime, nullable=True)
    failure_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)


class WebhookDelivery(Base):
    __tablename__ = "webhook_deliveries"
    __table_args__ = (
        Index(
            'ix_webhook_delivery_endpoint_created',
            'endpoint_id', 'created_at'
        ),
    )
    id = Column(Integer, primary_key=True)
    endpoint_id = Column(Integer, index=True)
    event_type = Column(String)
    payload = Column(String)
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


