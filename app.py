from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, Request
import os
import datetime
import stripe
import math
import requests

from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean
from sqlalchemy.orm import sessionmaker, declarative_base

# -----------------------
# ENV SETUP
# -----------------------

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY")
STRIPE_PRICE_ID = os.getenv("STRIPE_PRICE_ID")
RESEND_API_KEY = os.getenv("RESEND_API_KEY")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL not set")

if STRIPE_SECRET_KEY:
    stripe.api_key = STRIPE_SECRET_KEY

# -----------------------
# DATABASE
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
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    email = Column(String, unique=True)
    subscription_status = Column(String, default="inactive")
    alerts_enabled = Column(Boolean, default=False)
    last_alert_sent = Column(DateTime)

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
# BINANCE DATA
# -----------------------

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
    symbol = f"{coin}USDT"
    prices = get_prices(symbol)
    if not prices:
        return 0
    mom4 = momentum(prices, 4)
    mom24 = momentum(prices, 24)
    vol = volatility(prices)
    score = 0.5*mom4 + 0.5*mom24 - 0.3*vol
    return max(-100, min(100, score))

def classify(score):
    if score > 35: return "Strong Risk-On"
    if score > 15: return "Risk-On"
    if score < -35: return "Strong Risk-Off"
    if score < -15: return "Risk-Off"
    return "Neutral"

# -----------------------
# ALERT ENGINE
# -----------------------

def send_email(to_email, subject, body):
    if not RESEND_API_KEY:
        return

    requests.post(
        "https://api.resend.com/emails",
        headers={
            "Authorization": f"Bearer {RESEND_API_KEY}",
            "Content-Type": "application/json",
        },
        json={
            "from": "alerts@chainpulse.pro",
            "to": [to_email],
            "subject": subject,
            "html": body,
        },
    )
def generate_weekly_report(db, coin="BTC"):
    latest = db.query(MarketSummary)\
        .filter(MarketSummary.coin == coin)\
        .order_by(MarketSummary.created_at.desc())\
        .first()

    if not latest:
        return None

    return f"""
    <h2>Weekly Regime Report - {coin}</h2>
    <p><strong>Current Regime:</strong> {latest.label}</p>
    <p><strong>Score:</strong> {latest.score}</p>
    <p><strong>Coherence:</strong> {latest.coherence}%</p>
    <p>Adjust exposure based on regime conditions.</p>
    """

def check_and_send_alerts(db, coin):
    latest = db.query(MarketSummary)\
        .filter(MarketSummary.coin == coin)\
        .order_by(MarketSummary.created_at.desc())\
        .first()

    if not latest:
        return

    shift_risk = abs(latest.score)

    if shift_risk < 70:
        return

    users = db.query(User)\
        .filter(User.alerts_enabled == True)\
        .all()

    for user in users:
        if user.last_alert_sent and \
           (datetime.datetime.utcnow() - user.last_alert_sent).total_seconds() < 86400:
            continue

        send_email(
            user.email,
            "ChainPulse Regime Shift Alert",
            f"""
            <h2>Elevated Regime Shift Risk Detected</h2>
            <p>Current Regime: {latest.label}</p>
            <p>Score: {latest.score}</p>
            <p>Consider reducing exposure.</p>
            """
        )

        user.last_alert_sent = datetime.datetime.utcnow()

    db.commit()

# -----------------------
# ROUTES
# -----------------------

@app.get("/update-now")
def update_now(coin: str = "BTC"):
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

    check_and_send_alerts(db, coin)

    db.close()

    return {"status": "Updated & Alerts Checked"}

@app.post("/enable-alerts")
def enable_alerts(email: str):
    db = SessionLocal()
    user = db.query(User).filter(User.email == email).first()
    if user:
        user.alerts_enabled = True
        db.commit()
    db.close()
    return {"alerts_enabled": True}
@app.get("/weekly-report")
def weekly_report():
    db = SessionLocal()

    users = db.query(User)\
        .filter(User.subscription_status == "active")\
        .all()

    for user in users:
        report_html = generate_weekly_report(db)

        send_email(
            user.email,
            "ChainPulse Weekly Regime Report",
            report_html
        )

    db.close()
    return {"status": "Weekly reports sent"}

@app.get("/survival-curve")
def survival_curve(coin: str = "BTC"):
    db = SessionLocal()

    durations = regime_durations(db, coin)
    db.close()

    if not durations:
        return {"data": []}

    max_duration = int(max(durations))
    curve = []

    for hour in range(0, max_duration + 1):
        survivors = [d for d in durations if d > hour]
        survival_prob = (len(survivors) / len(durations)) * 100

        hazard = 0
        if hour > 0:
            exited = [d for d in durations if hour-1 < d <= hour]
            if len(survivors) > 0:
                hazard = (len(exited) / len(survivors)) * 100

        curve.append({
            "hour": hour,
            "survival": round(survival_prob, 2),
            "hazard": round(hazard, 2)
        })

    return {"data": curve}