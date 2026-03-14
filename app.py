from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean
from sqlalchemy.orm import sessionmaker, declarative_base
from dotenv import load_dotenv
import os
import datetime
import requests
import math
import stripe

# -------------------------
# ENV SETUP
# -------------------------

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY")
STRIPE_PRICE_ID = os.getenv("STRIPE_PRICE_ID")
RESEND_API_KEY = os.getenv("RESEND_API_KEY")

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
    coin = Column(String)
    score = Column(Float)
    label = Column(String)
    coherence = Column(Float)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    email = Column(String, unique=True)
    subscription_status = Column(String, default="inactive")
    alerts_enabled = Column(Boolean, default=False)
    last_alert_sent = Column(DateTime)

Base.metadata.create_all(bind=engine)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------------
# MARKET DATA
# -------------------------

def get_prices(symbol, interval="1h", limit=100):
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    try:
        r = requests.get(url, params=params, timeout=5)
        data = r.json()
        return [float(c[4]) for c in data]
    except:
        return []

def momentum(prices, period):
    if len(prices) < period:
        return 0
    return prices[-1] - prices[-period]

def volatility(prices, period=20):
    if len(prices) < period:
        return 0
    subset = prices[-period:]
    mean = sum(subset)/len(subset)
    var = sum((p-mean)**2 for p in subset)/len(subset)
    return math.sqrt(var)

def calculate_score(coin):
    prices = get_prices(f"{coin}USDT")
    if not prices:
        return 0

    mom4 = (prices[-1] - prices[-4]) / prices[-4]
    mom24 = (prices[-1] - prices[-24]) / prices[-24]
    vol = volatility(prices)

    # Convert to percentage
    mom4 *= 100
    mom24 *= 100

    score = 0.6 * mom24 + 0.4 * mom4 - 0.1 * vol

    # Clamp to -100 to 100
    return max(-100, min(100, score))
def classify(score):
    if score > 35: return "Strong Risk-On"
    if score > 15: return "Risk-On"
    if score < -35: return "Strong Risk-Off"
    if score < -15: return "Risk-Off"
    return "Neutral"

# -------------------------
# UPDATE ENGINE
# -------------------------

def update_market(coin):
    db = SessionLocal()
    score = calculate_score(coin)
    label = classify(score)

    entry = MarketSummary(
        coin=coin,
        score=score,
        label=label,
        coherence=abs(score)
    )

    db.add(entry)
    db.commit()
    db.close()

# -------------------------
# STATISTICS ENGINE
# -------------------------

def get_history(db, coin):
    return db.query(MarketSummary)\
        .filter(MarketSummary.coin == coin)\
        .order_by(MarketSummary.created_at.asc())\
        .all()

def regime_durations(db, coin):
    records = get_history(db, coin)
    durations = []
    current = None
    start = None

    for r in records:
        if r.label != current:
            if current:
                d = (r.created_at - start).total_seconds()/3600
                durations.append(d)
            current = r.label
            start = r.created_at

    return durations

def current_age(db, coin):
    records = db.query(MarketSummary)\
        .filter(MarketSummary.coin == coin)\
        .order_by(MarketSummary.created_at.desc())\
        .all()

    if not records:
        return 0

    latest_label = records[0].label
    start_time = records[0].created_at

    for r in records:
        if r.label != latest_label:
            break
        start_time = r.created_at

    return (datetime.datetime.utcnow()-start_time).total_seconds()/3600

def survival_probability(db, coin):
    durations = regime_durations(db, coin)
    age = current_age(db, coin)

    if len(durations) < 5:
        return round(max(40, 100 - age*5), 2)

    longer = [d for d in durations if d > age]
    return round((len(longer)/len(durations))*100,2)

def hazard_rate(db, coin):
    durations = regime_durations(db, coin)
    age = current_age(db, coin)

    if len(durations) < 5:
        return round(min(60, age*6),2)

    avg = sum(durations)/len(durations)
    return round(min(100,(age/(avg+0.01))*100),2)

def percentile_rank(db, coin, current_score):
    scores = [r.score for r in get_history(db, coin)]
    if len(scores) < 5:
        return round(50 + current_score/2,2)
    lower = [s for s in scores if s < current_score]
    return round((len(lower)/len(scores))*100,2)

def exposure_recommendation(score, survival, hazard, coherence):

    # Regime-based baseline allocation
    if score > 35:
        base = 0.85
    elif score > 15:
        base = 0.65
    elif score < -35:
        base = 0.10
    elif score < -15:
        base = 0.25
    else:
        base = 0.45

    # Adjust with survival & hazard
    persistence_boost = survival / 100
    hazard_penalty = hazard / 100

    exposure = base * persistence_boost * (1 - hazard_penalty * 0.6)

    return round(max(5, min(100, exposure * 100)), 2)

# -------------------------
# SURVIVAL CURVE
# -------------------------

@app.get("/survival-curve")
def survival_curve(coin: str="BTC"):
    db = SessionLocal()
    durations = regime_durations(db, coin)
    db.close()

    if len(durations) < 5:
        dummy = []
        for h in range(0, 25):
            dummy.append({
                "hour": h,
                "survival": max(0, 100 - h*4),
                "hazard": min(100, h*4)
            })
        return {"data": dummy}

    max_duration = int(max(durations))
    curve = []

    for hour in range(0, max_duration+1):
        survivors = [d for d in durations if d > hour]
        survival = (len(survivors)/len(durations))*100
        hazard = 0
        if hour > 0 and len(survivors)>0:
            exited = [d for d in durations if hour-1 < d <= hour]
            hazard = (len(exited)/len(survivors))*100

        curve.append({
            "hour": hour,
            "survival": round(survival,2),
            "hazard": round(hazard,2)
        })

    return {"data": curve}

# -------------------------
# ROUTES
# -------------------------

@app.get("/latest")
def latest(coin: str="BTC"):
    db = SessionLocal()
    r = db.query(MarketSummary)\
        .filter(MarketSummary.coin==coin)\
        .order_by(MarketSummary.created_at.desc())\
        .first()
    db.close()

    if not r:
        return {"message":"No data yet"}

    return {
        "score": r.score,
        "label": r.label,
        "coherence": r.coherence,
        "timestamp": r.created_at
    }

@app.get("/statistics")
def statistics(coin: str="BTC"):
    db = SessionLocal()
    latest = db.query(MarketSummary)\
        .filter(MarketSummary.coin==coin)\
        .order_by(MarketSummary.created_at.desc())\
        .first()

    if not latest:
        db.close()
        return {"message":"No data"}

    survival = survival_probability(db, coin)
    hazard = hazard_rate(db, coin)
    percentile = percentile_rank(db, coin, latest.score)
    exposure = exposure_recommendation(latest.score, survival, hazard, latest.coherence)
    age = current_age(db, coin)

    db.close()

    return {
        "survival_probability_percent": survival,
        "hazard_percent": hazard,
        "percentile_rank_percent": percentile,
        "exposure_recommendation_percent": exposure,
        "regime_shift_risk_percent": hazard,
        "current_regime_age_hours": age
    }

@app.post("/create-checkout-session")
def create_checkout_session():
    session = stripe.checkout.Session.create(
        payment_method_types=["card"],
        mode="subscription",
        line_items=[{
            "price": STRIPE_PRICE_ID,
            "quantity": 1,
        }],
        success_url="https://chainpulse.pro/app?success=true",
        cancel_url="https://chainpulse.pro/pricing",
    )
    return {"url": session.url}

# -------------------------
# EMAIL SENDING (RESEND)
# -------------------------

def send_email(to_email, subject, html_content):
    if not RESEND_API_KEY:
        return

    requests.post(
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
    )


# -------------------------
# SUBSCRIBE (DOUBLE OPT-IN)
# -------------------------

@app.post("/subscribe")
def subscribe(email: str):
    db = SessionLocal()

    user = db.query(User).filter(User.email == email).first()

    if not user:
        user = User(
            email=email,
            subscription_status="inactive",
            alerts_enabled=False
        )
        db.add(user)
        db.commit()

    # Send confirmation email
    send_email(
        email,
        "Confirm your ChainPulse subscription",
        f"""
        <h2>Confirm Your Subscription</h2>
        <p>Click below to activate weekly regime updates:</p>
        <a href="https://chainpulse-backend-2xok.onrender.com/confirm?email={email}">
        Confirm Subscription
        </a>
        """
    )

    db.close()

    return {"status": "confirmation_sent"}

@app.get("/confirm")
def confirm(email: str):
    db = SessionLocal()

    user = db.query(User).filter(User.email == email).first()
    if user:
        user.alerts_enabled = True
        db.commit()

    db.close()

    return {"status": "subscription_confirmed"}

from fastapi.responses import FileResponse

@app.get("/sample-report")
def sample_report():
    return FileResponse("sample_report.pdf", media_type="application/pdf")

@app.get("/update-now")
def update_now(coin: str="BTC"):
    update_market(coin)
    return {"status":"updated"}

