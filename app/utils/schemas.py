from pydantic import BaseModel, EmailStr
from typing import Optional


class SubscribeRequest(BaseModel):
    email: EmailStr


class UserProfileRequest(BaseModel):
    email: EmailStr
    max_drawdown_pct: float = 20.0
    typical_leverage: float = 1.0
    holding_period_days: int = 10
    risk_identity: str = "balanced"


class ExposureLogRequest(BaseModel):
    email: EmailStr
    coin: str = "BTC"
    user_exposure_pct: float


class PerformanceEntryRequest(BaseModel):
    email: EmailStr
    coin: str = "BTC"
    user_exposure_pct: float
    price_open: float
    price_close: float


class CheckoutRequest(BaseModel):
    email: Optional[str] = None
    billing_cycle: str = "monthly"
    tier: str = "pro"


class AlertThresholdRequest(BaseModel):
    email: EmailStr
    coin: str = "BTC"
    shift_risk_threshold: float = 70
    exposure_change_threshold: float = 10
    setup_quality_threshold: float = 70
    regime_quality_threshold: float = 50


class TradePlanRequest(BaseModel):
    email: EmailStr
    coin: str = "BTC"
    account_size: float = 10000
    strategy_mode: str = "balanced"


class BehavioralReportRequest(BaseModel):
    email: EmailStr
    lookback_days: int = 30


class TraderArchetype(BaseModel):
    email: EmailStr
    archetype: str = "swing"


class RestoreRequest(BaseModel):
    email: EmailStr


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


