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
import hmac
import hashlib

# -------------------------
# SETUP
# -------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("chainpulse")

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./chainpulse.db")
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY")
STRIPE_PRICE_ID = os.getenv("STRIPE_PRICE_ID")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET")
RESEND_API_KEY = os.getenv("RESEND_API_KEY")
UPDATE_SECRET = os.getenv("UPDATE_SECRET", "changeme")

if STRIPE_SECRET_KEY:
    stripe.api_key = STRIPE_SECRET_KEY

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

# -------------------------
# DATABASE MODELS
# -------------------------

class MarketSummary(Base):
    __tablename__ = "market_summary"
    id = Column(Integer, primary_key=True)
    coin = Column(String, index=True)
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
    alerts_enabled = Column(Boolean, default=False)
    last_alert_sent = Column(DateTime, nullable=True)
    access_token = Column(String, nullable=True, index=True)  # Pro auth token
    created_at = Column(DateTime, default=datetime.datetime.utcnow)


Base.metadata.create_all(bind=engine)

app = FastAPI(title="ChainPulse API", version="2.0")

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

# -------------------------
# DB DEPENDENCY
# -------------------------

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# -------------------------
# SUPPORTED COINS
# -------------------------

SUPPORTED_COINS = ["BTC", "ETH", "SOL", "BNB", "AVAX"]

# -------------------------
# AUTH HELPER
# -------------------------

def resolve_pro_status(authorization: Optional[str], db: Session) -> bool:
    """
    Reads Authorization: Bearer <token> header.
    Returns True if token matches an active Pro user.
    """
    if not authorization or not authorization.startswith("Bearer "):
        return False

    token = authorization.replace("Bearer ", "").strip()
    if not token:
        return False

    user = db.query(User).filter(User.access_token == token).first()
    if not user:
        return False

    return user.subscription_status == "active"

# -------------------------
# MARKET DATA
# -------------------------

def get_prices(symbol: str, interval: str = "1h", limit: int = 100):
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": f"{symbol}USDT", "interval": interval, "limit": limit}
    try:
        r = requests.get(url, params=params, timeout=8)
        r.raise_for_status()
        data = r.json()
        if not isinstance(data, list):
            logger.warning(f"Unexpected response for {symbol}: {data}")
            return []
        return [float(c[4]) for c in data]
    except Exception as e:
        logger.error(f"Price fetch failed for {symbol}: {e}")
        return []


def get_volumes(symbol: str, interval: str = "1h", limit: int = 100):
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": f"{symbol}USDT", "interval": interval, "limit": limit}
    try:
        r = requests.get(url, params=params, timeout=8)
        r.raise_for_status()
        data = r.json()
        return [float(c[5]) for c in data]
    except Exception as e:
        logger.error(f"Volume fetch failed for {symbol}: {e}")
        return []


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


def calculate_coherence(mom_4h: float, mom_24h: float, vol_score: float) -> float:
    if (mom_4h >= 0 and mom_24h >= 0) or (mom_4h < 0 and mom_24h < 0):
        alignment = 1.0
    else:
        alignment = 0.3

    magnitude = (abs(mom_4h) + abs(mom_24h)) / 2
    magnitude_norm = min(magnitude / 5.0, 1.0) * 100
    vol_penalty = min(vol_score / 500, 0.5)
    raw = alignment * magnitude_norm * (1 - vol_penalty)
    return round(max(0, min(100, raw)), 2)


def calculate_score_full(coin: str):
    prices = get_prices(coin)
    volumes = get_volumes(coin)

    if len(prices) < 25:
        return None

    mom_4h = ((prices[-1] - prices[-4]) / prices[-4]) * 100
    mom_24h = ((prices[-1] - prices[-24]) / prices[-24]) * 100
    vol = volatility(prices)
    vol_mom = volume_momentum(volumes)

    score = 0.55 * mom_24h + 0.35 * mom_4h - 0.08 * vol + 0.02 * vol_mom
    score = max(-100, min(100, score))
    coherence = calculate_coherence(mom_4h, mom_24h, vol)

    return {
        "score": round(score, 4),
        "mom_4h": round(mom_4h, 4),
        "mom_24h": round(mom_24h, 4),
        "volatility": round(vol, 4),
        "coherence": coherence,
    }


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

# -------------------------
# UPDATE ENGINE
# -------------------------

def update_market(coin: str, db: Session):
    result = calculate_score_full(coin)
    if result is None:
        logger.warning(f"Insufficient price data for {coin}")
        return None

    entry = MarketSummary(
        coin=coin,
        score=result["score"],
        label=classify(result["score"]),
        coherence=result["coherence"],
        momentum_4h=result["mom_4h"],
        momentum_24h=result["mom_24h"],
        volatility_val=result["volatility"],
    )
    db.add(entry)
    db.commit()
    db.refresh(entry)
    logger.info(f"Updated {coin}: score={result['score']}, label={entry.label}")
    return entry

# -------------------------
# STATISTICS ENGINE
# -------------------------

def get_history(db: Session, coin: str):
    return (
        db.query(MarketSummary)
        .filter(MarketSummary.coin == coin)
        .order_by(MarketSummary.created_at.asc())
        .all()
    )


def regime_durations(db: Session, coin: str) -> list:
    records = get_history(db, coin)
    if not records:
        return []

    durations = []
    current_label = records[0].label
    start_time = records[0].created_at

    for r in records[1:]:
        if r.label != current_label:
            duration = (r.created_at - start_time).total_seconds() / 3600
            if duration > 0:
                durations.append(duration)
            current_label = r.label
            start_time = r.created_at

    return durations


def current_regime_start(db: Session, coin: str) -> datetime.datetime:
    records = (
        db.query(MarketSummary)
        .filter(MarketSummary.coin == coin)
        .order_by(MarketSummary.created_at.desc())
        .all()
    )

    if not records:
        return datetime.datetime.utcnow()

    latest_label = records[0].label
    start_time = records[0].created_at

    for r in records:
        if r.label != latest_label:
            break
        start_time = r.created_at

    return start_time


def current_age(db: Session, coin: str) -> float:
    start = current_regime_start(db, coin)
    return (datetime.datetime.utcnow() - start).total_seconds() / 3600


def survival_probability(db: Session, coin: str) -> float:
    durations = regime_durations(db, coin)
    age = current_age(db, coin)

    if len(durations) < 5:
        return round(max(20.0, 90.0 - age * 4), 2)

    longer = [d for d in durations if d > age]
    return round((len(longer) / len(durations)) * 100, 2)


def hazard_rate(db: Session, coin: str) -> float:
    durations = regime_durations(db, coin)
    age = current_age(db, coin)

    if len(durations) < 5:
        return round(min(70.0, age * 5), 2)

    avg = sum(durations) / len(durations)
    return round(min(100.0, (age / (avg + 0.01)) * 100), 2)


def percentile_rank(db: Session, coin: str, current_score: float) -> float:
    scores = [r.score for r in get_history(db, coin)]
    if len(scores) < 5:
        return round(50 + current_score / 2, 2)
    lower = [s for s in scores if s < current_score]
    return round((len(lower) / len(scores)) * 100, 2)


def exposure_recommendation(
    score: float, survival: float, hazard: float, coherence: float
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


def regime_shift_risk(hazard: float, survival: float, coherence: float) -> float:
    hazard_component = hazard * 0.5
    survival_component = (100 - survival) * 0.35
    coherence_component = (100 - coherence) * 0.15
    return round(min(100.0, hazard_component + survival_component + coherence_component), 2)


def average_regime_duration(db: Session, coin: str) -> float:
    durations = regime_durations(db, coin)
    if not durations:
        return 24.0
    return sum(durations) / len(durations)


def trend_maturity_score(age: float, avg_duration: float, hazard: float) -> float:
    """
    0 = fresh regime just started
    100 = overextended / historically mature
    Combines age relative to average duration + hazard escalation
    """
    if avg_duration == 0:
        age_component = min(100, age * 5)
    else:
        age_component = min(100, (age / avg_duration) * 100)

    maturity = (age_component * 0.6) + (hazard * 0.4)
    return round(min(100, max(0, maturity)), 2)

# -------------------------
# ROUTES — HEALTH
# -------------------------

@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.datetime.utcnow()}

# -------------------------
# ROUTES — MARKET DATA
# -------------------------

@app.get("/market-overview")
def market_overview(db: Session = Depends(get_db)):
    result = []
    for coin in SUPPORTED_COINS:
        latest = (
            db.query(MarketSummary)
            .filter(MarketSummary.coin == coin)
            .order_by(MarketSummary.created_at.desc())
            .first()
        )
        if latest:
            result.append({
                "coin": coin,
                "score": latest.score,
                "label": latest.label,
                "coherence": latest.coherence,
                "timestamp": latest.created_at,
            })
    return {"data": result}


@app.get("/latest")
def latest(coin: str = "BTC", db: Session = Depends(get_db)):
    if coin not in SUPPORTED_COINS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported coin. Choose from {SUPPORTED_COINS}"
        )

    r = (
        db.query(MarketSummary)
        .filter(MarketSummary.coin == coin)
        .order_by(MarketSummary.created_at.desc())
        .first()
    )

    if not r:
        return {"message": "No data yet. Try /update-now first."}

    return {
        "coin": r.coin,
        "score": r.score,
        "label": r.label,
        "coherence": r.coherence,
        "momentum_4h": r.momentum_4h,
        "momentum_24h": r.momentum_24h,
        "volatility": r.volatility_val,
        "timestamp": r.created_at,
    }


@app.get("/statistics")
def statistics(
    coin: str = "BTC",
    db: Session = Depends(get_db),
    authorization: Optional[str] = None,
):
    # Read Authorization header manually so it stays a GET request
    # FastAPI note: use Request object to pull header cleanly
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")

    latest_record = (
        db.query(MarketSummary)
        .filter(MarketSummary.coin == coin)
        .order_by(MarketSummary.created_at.desc())
        .first()
    )

    if not latest_record:
        return {"message": "No data. Call /update-now first."}

    survival = survival_probability(db, coin)
    hazard = hazard_rate(db, coin)
    percentile = percentile_rank(db, coin, latest_record.score)
    exposure = exposure_recommendation(
        latest_record.score, survival, hazard, latest_record.coherence
    )
    age = current_age(db, coin)
    shift_risk = regime_shift_risk(hazard, survival, latest_record.coherence)
    avg_dur = average_regime_duration(db, coin)
    maturity = trend_maturity_score(age, avg_dur, hazard)

    return {
        "coin": coin,
        "score": latest_record.score,
        "label": latest_record.label,
        "coherence": latest_record.coherence,
        "survival_probability_percent": survival,
        "hazard_percent": hazard,
        "percentile_rank_percent": percentile,
        "exposure_recommendation_percent": exposure,
        "regime_shift_risk_percent": shift_risk,
        "trend_maturity_score": maturity,
        "current_regime_age_hours": round(age, 2),
        "timestamp": latest_record.created_at,
        "pro_required": False,  # set by /statistics-gated below
    }


@app.get("/statistics-gated")
def statistics_gated(
    request: Request,
    coin: str = "BTC",
    db: Session = Depends(get_db),
):
    """
    Pro-gated version of /statistics.
    Frontend sends Authorization: Bearer <token>
    Free users get basic fields only. Pro users get everything.
    """
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")

    # Resolve pro status from header
    auth_header = request.headers.get("authorization") or request.headers.get("Authorization")
    is_pro = resolve_pro_status(auth_header, db)

    latest_record = (
        db.query(MarketSummary)
        .filter(MarketSummary.coin == coin)
        .order_by(MarketSummary.created_at.desc())
        .first()
    )

    if not latest_record:
        return {"message": "No data. Call /update-now first."}

    survival = survival_probability(db, coin)
    hazard = hazard_rate(db, coin)
    percentile = percentile_rank(db, coin, latest_record.score)
    exposure = exposure_recommendation(
        latest_record.score, survival, hazard, latest_record.coherence
    )
    age = current_age(db, coin)
    shift_risk = regime_shift_risk(hazard, survival, latest_record.coherence)
    avg_dur = average_regime_duration(db, coin)
    maturity = trend_maturity_score(age, avg_dur, hazard)

    # Free tier — visible fields
    base_response = {
        "coin": coin,
        "score": latest_record.score,
        "label": latest_record.label,
        "exposure_recommendation_percent": exposure,
        "regime_shift_risk_percent": shift_risk,
        "current_regime_age_hours": round(age, 2),
        "timestamp": latest_record.created_at,
    }

    if not is_pro:
        # Return base fields + null pro fields so frontend
        # knows the shape but sees nothing sensitive
        return {
            **base_response,
            "pro_required": True,
            "survival_probability_percent": None,
            "hazard_percent": None,
            "percentile_rank_percent": None,
            "coherence": None,
            "trend_maturity_score": None,
        }

    # Pro tier — full response
    return {
        **base_response,
        "pro_required": False,
        "coherence": latest_record.coherence,
        "survival_probability_percent": survival,
        "hazard_percent": hazard,
        "percentile_rank_percent": percentile,
        "trend_maturity_score": maturity,
    }


@app.get("/regime-history")
def regime_history(coin: str = "BTC", limit: int = 48, db: Session = Depends(get_db)):
    records = (
        db.query(MarketSummary)
        .filter(MarketSummary.coin == coin)
        .order_by(MarketSummary.created_at.desc())
        .limit(limit)
        .all()
    )
    records.reverse()
    return {
        "data": [
            {
                "hour": i,
                "score": r.score,
                "label": r.label,
                "coherence": r.coherence,
                "timestamp": r.created_at,
            }
            for i, r in enumerate(records)
        ]
    }


@app.get("/survival-curve")
def survival_curve(coin: str = "BTC", db: Session = Depends(get_db)):
    durations = regime_durations(db, coin)

    if len(durations) < 5:
        dummy = []
        for h in range(0, 25):
            s = max(0, 100 - h * 4)
            hz = min(100, h * 4.5)
            dummy.append({"hour": h, "survival": s, "hazard": hz})
        return {"data": dummy, "source": "estimated"}

    max_duration = int(max(durations))
    curve = []

    for hour in range(0, max_duration + 1):
        survivors = [d for d in durations if d > hour]
        surv_pct = (len(survivors) / len(durations)) * 100
        hz = 0.0
        if hour > 0 and len(survivors) > 0:
            exited = [d for d in durations if hour - 1 < d <= hour]
            hz = (len(exited) / len(survivors)) * 100
        curve.append({
            "hour": hour,
            "survival": round(surv_pct, 2),
            "hazard": round(hz, 2),
        })

    return {"data": curve, "source": "historical"}

# -------------------------
# ROUTES — UPDATE
# -------------------------

@app.get("/update-now")
def update_now(coin: str = "BTC", secret: str = "", db: Session = Depends(get_db)):
    if secret != UPDATE_SECRET:
        raise HTTPException(status_code=403, detail="Invalid secret")
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail="Unsupported coin")
    entry = update_market(coin, db)
    if not entry:
        raise HTTPException(status_code=500, detail="Market update failed")
    return {
        "status": "updated",
        "coin": coin,
        "label": entry.label,
        "score": entry.score,
    }


@app.get("/update-all")
def update_all(secret: str = "", db: Session = Depends(get_db)):
    if secret != UPDATE_SECRET:
        raise HTTPException(status_code=403, detail="Invalid secret")
    results = []
    for coin in SUPPORTED_COINS:
        entry = update_market(coin, db)
        if entry:
            results.append({
                "coin": coin,
                "label": entry.label,
                "score": entry.score,
            })
    return {"status": "updated", "results": results}

# -------------------------
# ROUTES — STRIPE
# -------------------------

class CheckoutRequest(BaseModel):
    email: str = ""


@app.post("/create-checkout-session")
def create_checkout_session(body: CheckoutRequest = CheckoutRequest()):
    if not STRIPE_SECRET_KEY:
        raise HTTPException(status_code=500, detail="Stripe not configured")
    try:
        params = {
            "payment_method_types": ["card"],
            "mode": "subscription",
            "line_items": [{"price": STRIPE_PRICE_ID, "quantity": 1}],
            "success_url": "https://chainpulse.pro/app?success=true",
            "cancel_url": "https://chainpulse.pro/pricing",
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
    payload = await request.body()
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
    data = event["data"]["object"]

    # ── Successful checkout ──
    if event_type == "checkout.session.completed":
        customer_email = data.get("customer_details", {}).get("email")
        customer_id = data.get("customer")
        subscription_id = data.get("subscription")

        if customer_email:
            user = db.query(User).filter(User.email == customer_email).first()
            if not user:
                user = User(email=customer_email)
                db.add(user)

            # Generate unique access token for this user
            access_token = str(uuid.uuid4())

            user.subscription_status = "active"
            user.stripe_customer_id = customer_id
            user.stripe_subscription_id = subscription_id
            user.alerts_enabled = True
            user.access_token = access_token
            db.commit()

            send_email(
                customer_email,
                "Welcome to ChainPulse Pro — Your Access Link",
                welcome_email_html(customer_email, access_token),
            )
            logger.info(f"Pro activated: {customer_email}")

    # ── Subscription cancelled or paused ──
    elif event_type in (
        "customer.subscription.deleted",
        "customer.subscription.paused",
    ):
        subscription_id = data.get("id")
        user = db.query(User).filter(
            User.stripe_subscription_id == subscription_id
        ).first()
        if user:
            user.subscription_status = "inactive"
            user.access_token = None  # revoke token immediately
            db.commit()
            logger.info(f"Subscription deactivated: {user.email}")

    # ── Payment failed ──
    elif event_type == "invoice.payment_failed":
        customer_id = data.get("customer")
        user = db.query(User).filter(
            User.stripe_customer_id == customer_id
        ).first()
        if user:
            send_email(
                user.email,
                "ChainPulse — Payment Failed",
                """
                <div style="font-family:sans-serif;max-width:560px;margin:0 auto;
                            background:#000;color:#fff;padding:40px;">
                  <h2 style="color:#f87171;">Payment Failed</h2>
                  <p style="color:#999;">
                    Your ChainPulse Pro payment could not be processed.
                    Please update your payment method to maintain access.
                  </p>
                  <a href="https://chainpulse.pro/pricing"
                     style="display:inline-block;background:#fff;color:#000;
                            padding:14px 28px;margin-top:24px;text-decoration:none;
                            font-weight:bold;">
                    Update Payment
                  </a>
                </div>
                """,
            )

    return {"status": "received"}

# -------------------------
# EMAIL SYSTEM
# -------------------------

def send_email(to_email: str, subject: str, html_content: str):
    if not RESEND_API_KEY:
        logger.warning("RESEND_API_KEY not set — email skipped")
        return

    try:
        response = requests.post(
            "https://api.resend.com/emails",
            headers={
                "Authorization": f"Bearer {RESEND_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "from": "ChainPulse <alerts@chainpulse.pro>",
                "to": [to_email],
                "subject": subject,
                "html": html_content,
            },
            timeout=8,
        )
        response.raise_for_status()
        logger.info(f"Email sent to {to_email}")
    except Exception as e:
        logger.error(f"Email send failed to {to_email}: {e}")


def welcome_email_html(email: str, access_token: str) -> str:
    dashboard_url = f"https://chainpulse.pro/app?token={access_token}"
    return f"""
    <div style="font-family:sans-serif;max-width:560px;margin:0 auto;
                background:#000;color:#fff;padding:40px;">
      <div style="font-size:11px;color:#555;text-transform:uppercase;
                  letter-spacing:2px;margin-bottom:16px;">
        ChainPulse Pro
      </div>
      <h1 style="font-size:24px;margin-bottom:8px;">
        Your Pro Access Is Active
      </h1>
      <p style="color:#999;margin-bottom:32px;">
        Click below to open your Pro dashboard. This link logs you in automatically.
        Bookmark it.
      </p>
      <a href="{dashboard_url}"
         style="display:inline-block;background:#fff;color:#000;padding:14px 28px;
                text-decoration:none;font-weight:bold;border-radius:4px;">
        Open Pro Dashboard
      </a>
      <div style="margin-top:40px;border-top:1px solid #222;padding-top:24px;">
        <p style="color:#555;font-size:12px;margin-bottom:12px;">
          What you now have access to:
        </p>
        <ul style="color:#666;font-size:12px;line-height:2.2;padding-left:16px;">
          <li>Regime survival curve and hazard modeling</li>
          <li>Coherence index and trend maturity score</li>
          <li>Strength percentile ranking</li>
          <li>Real-time shift alerts via email</li>
          <li>Daily morning regime brief</li>
          <li>Multi-asset: BTC, ETH, SOL, BNB, AVAX</li>
        </ul>
      </div>
      <p style="color:#333;font-size:11px;margin-top:40px;
                border-top:1px solid #111;padding-top:20px;">
        ChainPulse is a decision-support framework. Not financial advice.
      </p>
    </div>
    """


def regime_alert_html(
    coin: str, label: str, shift_risk: float, exposure: float
) -> str:
    return f"""
    <div style="font-family:sans-serif;max-width:560px;margin:0 auto;
                background:#000;color:#fff;padding:40px;">
      <div style="font-size:11px;color:#555;text-transform:uppercase;
                  letter-spacing:2px;margin-bottom:16px;">
        ChainPulse Alert
      </div>
      <h2 style="color:#f87171;margin-bottom:8px;">
        ⚠ Regime Shift Risk Elevated — {coin}
      </h2>
      <p style="color:#999;">
        Current Regime: <strong style="color:#fff;">{label}</strong>
      </p>
      <p style="color:#999;">
        Shift Risk: <strong style="color:#f87171;">{shift_risk}%</strong>
      </p>
      <p style="color:#999;">
        Recommended Exposure: <strong style="color:#fff;">{exposure}%</strong>
      </p>
      <p style="color:#666;margin-top:20px;font-size:13px;">
        Statistical hazard has elevated beyond historical norms.
        Consider reducing position size or tightening stops.
      </p>
      <a href="https://chainpulse.pro/app"
         style="display:inline-block;background:#fff;color:#000;padding:14px 28px;
                margin-top:24px;text-decoration:none;font-weight:bold;border-radius:4px;">
        View Dashboard
      </a>
      <p style="color:#333;font-size:11px;margin-top:40px;">
        ChainPulse. Not financial advice. Manage your own risk.
      </p>
    </div>
    """


def morning_email_html(snapshots: list, access_token: str) -> str:
    dashboard_url = (
        f"https://chainpulse.pro/app?token={access_token}"
        if access_token
        else "https://chainpulse.pro/app"
    )

    rows = ""
    for s in snapshots:
        risk_color = (
            "#f87171" if s["shift_risk"] > 70
            else "#facc15" if s["shift_risk"] > 45
            else "#4ade80"
        )
        rows += f"""
        <tr>
          <td style="padding:12px 8px;border-bottom:1px solid #1f1f1f;
                     color:#fff;font-weight:600;">
            {s["coin"]}
          </td>
          <td style="padding:12px 8px;border-bottom:1px solid #1f1f1f;color:#999;">
            {s["label"]}
          </td>
          <td style="padding:12px 8px;border-bottom:1px solid #1f1f1f;color:#fff;">
            {s["exposure"]}%
          </td>
          <td style="padding:12px 8px;border-bottom:1px solid #1f1f1f;
                     color:{risk_color};font-weight:600;">
            {s["shift_risk"]}%
          </td>
        </tr>
        """

    return f"""
    <div style="font-family:sans-serif;max-width:560px;margin:0 auto;
                background:#000;color:#fff;padding:40px;">
      <div style="font-size:11px;color:#555;text-transform:uppercase;
                  letter-spacing:2px;margin-bottom:16px;">
        ChainPulse Morning Brief
      </div>
      <h1 style="font-size:22px;margin-bottom:8px;">Daily Regime Snapshot</h1>
      <p style="color:#666;font-size:13px;margin-bottom:32px;">
        Current regime conditions across tracked assets.
      </p>

      <table style="width:100%;border-collapse:collapse;">
        <thead>
          <tr>
            <th style="text-align:left;padding:8px;color:#444;font-size:11px;
                       text-transform:uppercase;letter-spacing:1px;
                       border-bottom:1px solid #222;">Asset</th>
            <th style="text-align:left;padding:8px;color:#444;font-size:11px;
                       text-transform:uppercase;letter-spacing:1px;
                       border-bottom:1px solid #222;">Regime</th>
            <th style="text-align:left;padding:8px;color:#444;font-size:11px;
                       text-transform:uppercase;letter-spacing:1px;
                       border-bottom:1px solid #222;">Exposure</th>
            <th style="text-align:left;padding:8px;color:#444;font-size:11px;
                       text-transform:uppercase;letter-spacing:1px;
                       border-bottom:1px solid #222;">Shift Risk</th>
          </tr>
        </thead>
        <tbody>
          {rows}
        </tbody>
      </table>

      <a href="{dashboard_url}"
         style="display:inline-block;background:#fff;color:#000;padding:14px 28px;
                margin-top:32px;text-decoration:none;font-weight:bold;border-radius:4px;">
        Open Dashboard
      </a>

      <p style="color:#333;font-size:11px;margin-top:40px;
                border-top:1px solid #111;padding-top:20px;">
        ChainPulse. Not financial advice. Manage your own risk.
      </p>
    </div>
    """

# -------------------------
# ROUTES — ALERTS
# -------------------------

@app.get("/send-alerts")
def send_alerts(secret: str = "", db: Session = Depends(get_db)):
    if secret != UPDATE_SECRET:
        raise HTTPException(status_code=403, detail="Unauthorized")

    pro_users = db.query(User).filter(
        User.subscription_status == "active",
        User.alerts_enabled == True,
    ).all()

    sent = 0
    for coin in SUPPORTED_COINS:
        latest_record = (
            db.query(MarketSummary)
            .filter(MarketSummary.coin == coin)
            .order_by(MarketSummary.created_at.desc())
            .first()
        )
        if not latest_record:
            continue

        survival = survival_probability(db, coin)
        hazard = hazard_rate(db, coin)
        shift_risk = regime_shift_risk(hazard, survival, latest_record.coherence)
        exposure = exposure_recommendation(
            latest_record.score, survival, hazard, latest_record.coherence
        )

        if shift_risk < 70:
            continue

        for user in pro_users:
            if user.last_alert_sent:
                hours_since = (
                    datetime.datetime.utcnow() - user.last_alert_sent
                ).total_seconds() / 3600
                if hours_since < 12:
                    continue

            send_email(
                user.email,
                f"ChainPulse Alert — {coin} Regime Shift Risk Elevated",
                regime_alert_html(
                    coin, latest_record.label, shift_risk, exposure
                ),
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
        User.alerts_enabled == True,
    ).all()

    # Build snapshot for BTC, ETH, SOL
    snapshots = []
    for coin in ["BTC", "ETH", "SOL"]:
        record = (
            db.query(MarketSummary)
            .filter(MarketSummary.coin == coin)
            .order_by(MarketSummary.created_at.desc())
            .first()
        )
        if not record:
            continue

        survival = survival_probability(db, coin)
        hazard = hazard_rate(db, coin)
        exposure = exposure_recommendation(
            record.score, survival, hazard, record.coherence
        )
        shift_risk = regime_shift_risk(hazard, survival, record.coherence)

        snapshots.append({
            "coin": coin,
            "label": record.label,
            "exposure": exposure,
            "shift_risk": shift_risk,
        })

    sent = 0
    for user in pro_users:
        send_email(
            user.email,
            "ChainPulse Morning Regime Brief",
            morning_email_html(snapshots, user.access_token or ""),
        )
        sent += 1

    return {"status": "sent", "count": sent}

# -------------------------
# ROUTES — SUBSCRIBE / CONFIRM
# -------------------------

class SubscribeRequest(BaseModel):
    email: str


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

    send_email(
        email,
        "Confirm your ChainPulse subscription",
        f"""
        <div style="font-family:sans-serif;max-width:560px;margin:0 auto;
                    background:#000;color:#fff;padding:40px;">
          <h2>Confirm Your Subscription</h2>
          <p style="color:#999;">
            Click below to activate weekly regime updates:
          </p>
          <a href="https://chainpulse-backend-2xok.onrender.com/confirm?email={email}"
             style="display:inline-block;background:#fff;color:#000;padding:14px 28px;
                    margin-top:24px;text-decoration:none;font-weight:bold;">
            Confirm Subscription
          </a>
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

# -------------------------
# ROUTES — SAMPLE REPORT
# -------------------------

@app.get("/sample-report")
def sample_report():
    path = "sample_report.pdf"
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Report not found")
    return FileResponse(path, media_type="application/pdf")