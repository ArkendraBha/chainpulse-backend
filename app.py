from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, Request, HTTPException
import os
import datetime
import stripe
import math
import requests

from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base

# -----------------------
# ENV SETUP
# -----------------------

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY")
STRIPE_PRICE_ID = os.getenv("STRIPE_PRICE_ID")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL not set")

if STRIPE_SECRET_KEY:
    stripe.api_key = STRIPE_SECRET_KEY

# -----------------------
# DATABASE (REBUILT CLEAN)
# -----------------------

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

class MarketSummary(Base):
    __tablename__ = "market_summary"

    id = Column(Integer, primary_key=True)
    coin = Column(String)
    score = Column(Float)
    label = Column(String)
    coherence = Column(Float)
    strength_bucket = Column(String)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    email = Column(String, unique=True)
    subscription_status = Column(String, default="inactive")

Base.metadata.drop_all(bind=engine)
Base.metadata.create_all(bind=engine)

# -----------------------
# APP INIT
# -----------------------

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------
# BINANCE PRICE DATA
# -----------------------

def get_prices(symbol, interval, limit=100):
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    try:
        r = requests.get(url, params=params, timeout=5)
        data = r.json()
        return [float(c[4]) for c in data]
    except:
        return []

# -----------------------
# REGIME ENGINE
# -----------------------

def momentum(prices, period):
    if len(prices) < period:
        return 0
    return prices[-1] - prices[-period]

def volatility(prices, period=20):
    if len(prices) < period:
        return 0
    subset = prices[-period:]
    mean = sum(subset) / len(subset)
    var = sum((p - mean) ** 2 for p in subset) / len(subset)
    return math.sqrt(var)

def timeframe_score(symbol):
    prices = get_prices(symbol, "1h")
    if not prices:
        return 0

    mom_4 = momentum(prices, 4)
    mom_24 = momentum(prices, 24)
    vol = volatility(prices)

    score = (0.5 * mom_4 + 0.5 * mom_24 - 0.3 * vol)
    return max(-100, min(100, score))

def classify(score):
    if score > 35: return "Strong Risk-On"
    if score > 15: return "Risk-On"
    if score < -35: return "Strong Risk-Off"
    if score < -15: return "Risk-Off"
    return "Neutral"

def strength_bucket(score):
    if abs(score) > 50: return "Extreme"
    if abs(score) > 30: return "High"
    if abs(score) > 15: return "Moderate"
    return "Weak"

# -----------------------
# UPDATE ENGINE
# -----------------------

def update_market(coin):
    db = SessionLocal()
    symbol = f"{coin}USDT"

    score = timeframe_score(symbol)
    label = classify(score)

    # coherence proxy: absolute strength normalized
    coherence = min(100, abs(score))

    entry = MarketSummary(
        coin=coin,
        score=score,
        label=label,
        coherence=coherence,
        strength_bucket=strength_bucket(score)
    )

    db.add(entry)
    db.commit()
    db.close()

# -----------------------
# STATISTICS
# -----------------------

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

    latest = records[0].label
    start = records[0].created_at

    for r in records:
        if r.label != latest:
            break
        start = r.created_at

    return (datetime.datetime.utcnow()-start).total_seconds()/3600

def survival_probability(db, coin):
    durations = regime_durations(db, coin)
    age = current_age(db, coin)

    if not durations:
        return 0

    longer = [d for d in durations if d > age]
    return round((len(longer)/len(durations))*100,2)

def hazard_rate(db, coin):
    durations = regime_durations(db, coin)
    age = current_age(db, coin)

    if not durations:
        return 0

    avg = sum(durations)/len(durations)
    return round(min(100,(age/(avg+0.01))*100),2)

def percentile_rank(db, coin, current_score):
    scores = [r.score for r in get_history(db, coin)]
    if not scores:
        return 0
    lower = [s for s in scores if s < current_score]
    return round((len(lower)/len(scores))*100,2)

def exposure_recommendation(score, survival, hazard, coherence):
    base = abs(score)/100
    persistence_factor = survival/100
    hazard_penalty = hazard/100

    exposure = base * persistence_factor * (1-hazard_penalty)
    exposure *= (coherence/100)

    return round(min(100, exposure*100),2)

def shift_risk(hazard, coherence):
    risk = (hazard*0.6) + ((100-coherence)*0.4)
    return round(min(100,risk),2)

# -----------------------
# ROUTES
# -----------------------

@app.get("/")
def root():
    return {"status": "ChainPulse Institutional Backend Live"}

@app.get("/update-now")
def manual_update(coin: str = "BTC"):
    update_market(coin)
    return {"status": f"{coin} updated"}

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
        "strength_bucket": r.strength_bucket,
        "timestamp": r.created_at
    }

@app.get("/statistics")
def statistics(coin: str="BTC", email: str=None):
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
    shift = shift_risk(hazard, latest.coherence)

    is_pro=False
    if email:
        user = db.query(User).filter(User.email==email).first()
        if user and user.subscription_status=="active":
            is_pro=True

    db.close()

    if not is_pro:
        return {
            "exposure_recommendation_percent": exposure,
            "pro_required": True
        }

    return {
        "survival_probability_percent": survival,
        "hazard_percent": hazard,
        "percentile_rank_percent": percentile,
        "exposure_recommendation_percent": exposure,
        "regime_shift_risk_percent": shift
    }