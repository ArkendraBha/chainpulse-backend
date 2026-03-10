print("CHAINPULSE BACKEND LOADED")
from fastapi.middleware.cors import CORSMiddleware
from fastapi import Request
import os
import json
import datetime
import stripe

from dotenv import load_dotenv
from fastapi import FastAPI
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text
from sqlalchemy.orm import sessionmaker, declarative_base
from openai import OpenAI
from apscheduler.schedulers.background import BackgroundScheduler

# Load environment variables
load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY")
STRIPE_PRICE_ID = os.getenv("STRIPE_PRICE_ID")

stripe.api_key = STRIPE_SECRET_KEY

openai_client = OpenAI(api_key=OPENAI_API_KEY)

# Database setup
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

class MarketSummary(Base):
    __tablename__ = "market_summary"

    id = Column(Integer, primary_key=True)
    sentiment_score = Column(Float)
    sentiment_label = Column(String)
    confidence = Column(Float)
    summary = Column(Text)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    email = Column(String, unique=True)
    subscription_status = Column(String, default="inactive")

Base.metadata.create_all(bind=engine)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Fetch crypto news (CryptoPanic removed)
def fetch_news():
    # External news removed permanently
    return None

# Generate AI summary
def generate_summary(headlines):

    formatted = "\n".join(headlines)

    prompt = f"""
Return ONLY valid JSON:

{{
"sentiment_score": -100 to 100,
"sentiment_label": "Bullish/Bearish/Neutral",
"confidence": 0-1,
"summary": "3 sentence professional crypto market overview"
}}

Headlines:
{formatted}
"""

    response = openai_client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0.2,
        messages=[
            {"role": "system", "content": "You are a professional crypto market analyst."},
            {"role": "user", "content": prompt}
        ]
    )

    return json.loads(response.choices[0].message.content)

# Update market summary
def update_market():
    db = SessionLocal()

    try:
        headlines = fetch_news()

        if headlines:
            result = generate_summary(headlines)
        else:
            # Fallback AI generation without external API
            result = {
                "sentiment_score": 0,
                "sentiment_label": "Neutral",
                "confidence": 0.5,
                "summary": "Automated system update. No external news source connected."
            }

        summary = MarketSummary(
            sentiment_score=result["sentiment_score"],
            sentiment_label=result["sentiment_label"],
            confidence=result["confidence"],
            summary=result["summary"]
        )

        db.add(summary)
        db.commit()

    except Exception as e:
        print("Update error:", e)

    finally:
        db.close()
def latest_summary():
    db = SessionLocal()
    data = db.query(MarketSummary).order_by(MarketSummary.id.desc()).first()
    db.close()

    if not data:
        return {"message": "No data yet"}

    return {
        "score": data.sentiment_score,
        "label": data.sentiment_label,
        "confidence": data.confidence,
        "summary": data.summary,
        "timestamp": data.created_at
    }

@app.get("/update-now")
def manual_update():
    update_market()
    return {"status": "Market updated"}

@app.get("/history")
def sentiment_history():
    db = SessionLocal()
    records = db.query(MarketSummary).order_by(MarketSummary.id.desc()).limit(30).all()
    db.close()

    return [
        {
            "score": r.sentiment_score,
            "label": r.sentiment_label,
            "timestamp": r.created_at
        }
        for r in records
    ]

@app.post("/create-checkout-session")
def create_checkout_session():
    session = stripe.checkout.Session.create(
        payment_method_types=["card"],
        mode="subscription",
        line_items=[{
            "price": STRIPE_PRICE_ID,
            "quantity": 1,
        }],
        success_url="https://chainpulse.pro?success=true",
        cancel_url="https://chainpulse.pro?canceled=true",
    )
    return {"url": session.url}

@app.post("/webhook")
async def stripe_webhook(request: Request):
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature")

    event = stripe.Webhook.construct_event(
        payload, sig_header, os.getenv("STRIPE_WEBHOOK_SECRET")
    )

    if event["type"] == "checkout.session.completed":
        session = event["data"]["object"]
        email = session.get("customer_details", {}).get("email")

        if email:
            db = SessionLocal()
            user = db.query(User).filter(User.email == email).first()

            if not user:
                user = User(email=email, subscription_status="active")
                db.add(user)
            else:
                user.subscription_status = "active"

            db.commit()
            db.close()

    return {"status": "success"}

@app.get("/check-subscription")
def check_subscription(email: str):
    db = SessionLocal()
    user = db.query(User).filter(User.email == email).first()
    db.close()

    if user and user.subscription_status == "active":
        return {"isPro": True}

    return {"isPro": False}