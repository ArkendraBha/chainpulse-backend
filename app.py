from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean
from sqlalchemy.orm import sessionmaker, declarative_base, Session
from pydantic import BaseModel, EmailStr
from dotenv import load_dotenv
import os
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
        return [float(c[5]) for c in data]  # volume column
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
    """Volume trend — rising volume confirms regime."""
    if len(volumes) < period * 2:
        return 0.0
    recent = sum(volumes[-period:]) / period
    prior = sum(volumes[-period * 2:-period]) / period
    if prior == 0:
        return 0.0
    return ((recent - prior) / prior) * 100

def calculate_coherence(mom_4h: float, mom_24h: float, vol_score: float) -> float:
    """
    Coherence = directional agreement between timeframes,
    penalized by volatility noise.
    Range: 0 - 100
    """
    # Both positive or both negative = aligned
    if (mom_4h >= 0 and mom_24h >= 0) or (mom_4h < 0 and mom_24h < 0):
        alignment = 1.0
    else:
        alignment = 0.3  # Conflicting signals

    magnitude = (abs(mom_4h) + abs(mom_24h)) / 2
    # Normalize magnitude to 0-100 (cap at 5% move = max coherence)
    magnitude_norm = min(magnitude / 5.0, 1.0) * 100

    # Penalize high volatility
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

    # Base score
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
    """Returns list of completed regime durations in hours."""
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

    # Don't include the still-running regime
    return durations

def current_regime_start(db: Session, coin: str) -> datetime.datetime:
    """Find the start of the current unbroken regime."""
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
        # Graceful fallback with age decay
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
    """
    Multi-factor exposure engine.
    Regime baseline × persistence boost × hazard penalty × coherence weight.
    """
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
    coherence_factor = 0.7 + (coherence / 100) * 0.3  # 0.7 to 1.0

    exposure = base * persistence_factor * hazard_penalty * coherence_factor
    return round(max(5.0, min(95.0, exposure * 100)), 2)

def regime_shift_risk(hazard: float, survival: float, coherence: float) -> float:
    """
    Composite shift risk score combining hazard, survival decay, and low coherence.
    """
    hazard_component = hazard * 0.5
    survival_component = (100 - survival) * 0.35
    coherence_component = (100 - coherence) * 0.15
    return round(min(100.0, hazard_component + survival_component + coherence_component), 2)

# -------------------------
# MULTI-COIN MARKET OVERVIEW
# -------------------------

@app.get("/market-overview")
def market_overview(db: Session = Depends(get_db)):
    """Returns latest regime data for all tracked coins."""
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

# -------------------------
# SURVIVAL CURVE
# -------------------------

@app.get("/survival-curve")
def survival_curve(coin: str = "BTC", db: Session = Depends(get_db)):
    durations = regime_durations(db, coin)

    if len(durations) < 5:
        # Smooth dummy curve
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
# ROUTES
# -------------------------

@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.datetime.utcnow()}

@app.get("/latest")
def latest(coin: str = "BTC", db: Session = Depends(get_db)):
    if coin not in SUPPORTED_COINS:
        raise HTTPException(status_code=400, detail=f"Unsupported coin. Choose from {SUPPORTED_COINS}")

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
def statistics(coin: str = "BTC", db: Session = Depends(get_db)):
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
        "current_regime_age_hours": round(age, 2),
        "timestamp": latest_record.created_at,
    }

@app.get("/regime-history")
def regime_history(coin: str = "BTC", limit: int = 48, db: Session = Depends(get_db)):
    """Returns recent regime score history for charting."""
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

# -------------------------
# PROTECTED UPDATE ENDPOINT
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
    return {"status": "updated", "coin": coin, "label": entry.label, "score": entry.score}

@app.get("/update-all")
def update_all(secret: str = "", db: Session = Depends(get_db)):
    if secret != UPDATE_SECRET:
        raise HTTPException(status_code=403, detail="Invalid secret")
    results = []
    for coin in SUPPORTED_COINS:
        entry = update_market(coin, db)
        if entry:
            results.append({"coin": coin, "label": entry.label, "score": entry.score})
    return {"status": "updated", "results": results}

# -------------------------
# STRIPE CHECKOUT
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

# -------------------------
# STRIPE WEBHOOK
# -------------------------

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

    if event_type == "checkout.session.completed":
        customer_email = data.get("customer_details", {}).get("email")
        customer_id = data.get("customer")
        subscription_id = data.get("subscription")

        if customer_email:
            user = db.query(User).filter(User.email == customer_email).first()
            if not user:
                user = User(email=customer_email)
                db.add(user)

            user.subscription_status = "active"
            user.stripe_customer_id = customer_id
            user.stripe_subscription_id = subscription_id
            user.alerts_enabled = True
            db.commit()

            send_email(
                customer_email,
                "Welcome to ChainPulse Pro",
                welcome_email_html(customer_email),
            )
            logger.info(f"Pro activated: {customer_email}")

    elif event_type in ("customer.subscription.deleted", "customer.subscription.paused"):
        subscription_id = data.get("id")
        user = db.query(User).filter(
            User.stripe_subscription_id == subscription_id
        ).first()
        if user:
            user.subscription_status = "inactive"
            db.commit()
            logger.info(f"Subscription deactivated: {user.email}")

    elif event_type == "invoice.payment_failed":
        customer_id = data.get("customer")
        user = db.query(User).filter(User.stripe_customer_id == customer_id).first()
        if user:
            send_email(
                user.email,
                "ChainPulse — Payment Failed",
                f"""
                <p>Your ChainPulse Pro payment failed.</p>
                <p>Please update your payment method to maintain access.</p>
                <a href="https://chainpulse.pro/pricing">Update Payment</a>
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

def welcome_email_html(email: str) -> str:
    return f"""
    <div style="font-family:sans-serif;max-width:560px;margin:0 auto;background:#000;color:#fff;padding:40px;">
      <h1 style="font-size:24px;">Welcome to ChainPulse Pro</h1>
      <p style="color:#999;">You now have full access to regime survival modeling, exposure intelligence, and weekly regime reports.</p>
      <a href="https://chainpulse.pro/app"
         style="display:inline-block;background:#fff;color:#000;padding:14px 28px;margin-top:24px;text-decoration:none;font-weight:bold;">
        Open Dashboard
      </a>
      <p style="color:#444;font-size:12px;margin-top:40px;">
        ChainPulse is a decision-support framework. Not financial advice.
      </p>
    </div>
    """

def regime_alert_html(coin: str, label: str, shift_risk: float, exposure: float) -> str:
    return f"""
    <div style="font-family:sans-serif;max-width:560px;margin:0 auto;background:#000;color:#fff;padding:40px;">
      <h2 style="color:#f87171;">⚠ Regime Shift Alert — {coin}</h2>
      <p style="color:#999;">Current Regime: <strong style="color:#fff;">{label}</strong></p>
      <p style="color:#999;">Shift Risk: <strong style="color:#f87171;">{shift_risk}%</strong></p>
      <p style="color:#999;">Recommended Exposure: <strong style="color:#fff;">{exposure}%</strong></p>
      <p style="color:#999;margin-top:20px;">
        Hazard has elevated beyond historical norms.
        Consider reducing position size or tightening stops.
      </p>
      <a href="https://chainpulse.pro/app"
         style="display:inline-block;background:#fff;color:#000;padding:14px 28px;margin-top:24px;text-decoration:none;font-weight:bold;">
        View Dashboard
      </a>
      <p style="color:#444;font-size:12px;margin-top:40px;">
        ChainPulse. Not financial advice. Manage your own risk.
      </p>
    </div>
    """

# -------------------------
# ALERT DISPATCH
# -------------------------

@app.get("/send-alerts")
def send_alerts(secret: str = "", db: Session = Depends(get_db)):
    """
    Called by cron job. Sends alerts to Pro users when shift risk > 70.
    """
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
            # Throttle: max 1 alert per user per coin per 12h
            if user.last_alert_sent:
                hours_since = (
                    datetime.datetime.utcnow() - user.last_alert_sent
                ).total_seconds() / 3600
                if hours_since < 12:
                    continue

            send_email(
                user.email,
                f"ChainPulse Alert — {coin} Regime Shift Risk Elevated",
                regime_alert_html(coin, latest_record.label, shift_risk, exposure),
            )
            user.last_alert_sent = datetime.datetime.utcnow()
            db.commit()
            sent += 1

    return {"status": "complete", "alerts_sent": sent}

# -------------------------
# SUBSCRIBE / CONFIRM
# -------------------------

class SubscribeRequest(BaseModel):
    email: str

@app.post("/subscribe")
def subscribe(body: SubscribeRequest, db: Session = Depends(get_db)):
    email = body.email.strip().lower()

    user = db.query(User).filter(User.email == email).first()
    if not user:
        user = User(email=email, subscription_status="inactive", alerts_enabled=False)
        db.add(user)
        db.commit()

    send_email(
        email,
        "Confirm your ChainPulse subscription",
        f"""
        <div style="font-family:sans-serif;max-width:560px;margin:0 auto;background:#000;color:#fff;padding:40px;">
          <h2>Confirm Your Subscription</h2>
          <p style="color:#999;">Click below to activate weekly regime updates:</p>
          <a href="https://chainpulse-backend-2xok.onrender.com/confirm?email={email}"
             style="display:inline-block;background:#fff;color:#000;padding:14px 28px;margin-top:24px;text-decoration:none;font-weight:bold;">
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
# SAMPLE REPORT
# -------------------------

@app.get("/sample-report")
def sample_report():
    path = "sample_report.pdf"
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Report not found")
    return FileResponse(path, media_type="application/pdf")