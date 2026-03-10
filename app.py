from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, Request
import os
import json
import datetime
import requests
import stripe
import hashlib
import logging

from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text, Index
from sqlalchemy.orm import sessionmaker, declarative_base
from openai import OpenAI
from apscheduler.schedulers.background import BackgroundScheduler

# =========================
# ENVIRONMENT
# =========================

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
CRYPTO_API_KEY = os.getenv("CRYPTO_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY")
STRIPE_PRICE_ID = os.getenv("STRIPE_PRICE_ID")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET")
FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:3000")

stripe.api_key = STRIPE_SECRET_KEY
openai_client = OpenAI(api_key=OPENAI_API_KEY)

logging.basicConfig(level=logging.INFO)

# =========================
# DATABASE
# =========================

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()


class MarketSummary(Base):
    __tablename__ = "market_summary"

    id = Column(Integer, primary_key=True)
    sentiment_score = Column(Float)
    sentiment_label = Column(String)
    confidence = Column(Float)
    summary = Column(Text)
    news_hash = Column(String)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    __table_args__ = (
        Index("idx_created_at", "created_at"),
    )


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    email = Column(String, unique=True)
    subscription_status = Column(String, default="inactive")


Base.metadata.create_all(bind=engine)

# =========================
# FASTAPI
# =========================

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[*],  # safer than "*"
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================
# HELPERS
# =========================

def hash_news(headlines):
    combined = "".join(headlines)
    return hashlib.md5(combined.encode()).hexdigest()


def fetch_news():
    url = f"https://cryptopanic.com/api/developer/v2/posts/?auth_token={CRYPTO_API_KEY}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json().get("results", [])
        return [item["title"] for item in data[:8]]
    except Exception as e:
        logging.error(f"News fetch error: {e}")
        return []


def generate_summary(headlines):
    formatted = "\n".join(headlines)

    try:
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            temperature=0.2,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": "You are a professional crypto market analyst."},
                {"role": "user", "content": f"""
Analyze these crypto headlines.

Return JSON with:
- sentiment_score (-100 to 100)
- sentiment_label (Bullish/Bearish/Neutral)
- confidence (0-1)
- summary (3 sentence professional overview)

Headlines:
{formatted}
"""}
            ]
        )

        return json.loads(response.choices[0].message.content)

    except Exception as e:
        logging.error(f"OpenAI error: {e}")
        return None


# =========================
# MARKET UPDATE
# =========================

def update_market():
    logging.info("Running market update...")
    db = SessionLocal()

    try:
        headlines = fetch_news()
        if not headlines:
            return

        news_hash = hash_news(headlines)

        latest = db.query(MarketSummary).order_by(MarketSummary.id.desc()).first()
        if latest and latest.news_hash == news_hash:
            logging.info("No news change detected. Skipping AI call.")
            return

        result = generate_summary(headlines)
        if not result:
            return

        summary = MarketSummary(
            sentiment_score=result.get("sentiment_score"),
            sentiment_label=result.get("sentiment_label"),
            confidence=result.get("confidence"),
            summary=result.get("summary"),
            news_hash=news_hash
        )

        db.add(summary)
        db.commit()
        logging.info("Market summary updated successfully.")

    except Exception as e:
        logging.error(f"Market update failed: {e}")
    finally:
        db.close()


# =========================
# SCHEDULER
# =========================

scheduler = BackgroundScheduler()
scheduler.add_job(update_market, "interval", hours=1, max_instances=1)
scheduler.start()

# =========================
# ROUTES
# =========================

@app.get("/latest")
def latest_summary():
    db = SessionLocal()
    try:
        data = db.query(MarketSummary).order_by(MarketSummary.id.desc()).first()
        if not data:
            return {"message": "No data yet"}

        return {
            "score": data.sentiment_score,
            "label": data.sentiment_label,
            "confidence": data.confidence,
            "summary": data.summary,
            "timestamp": data.created_at
        }
    finally:
        db.close()


@app.get("/history")
def sentiment_history():
    db = SessionLocal()
    try:
        records = db.query(MarketSummary).order_by(MarketSummary.id.desc()).limit(30).all()
        return [
            {
                "score": r.sentiment_score,
                "label": r.sentiment_label,
                "timestamp": r.created_at
            }
            for r in records
        ]
    finally:
        db.close()


@app.get("/update-now")
def manual_update():
    update_market()
    return {"status": "Market updated"}


@app.post("/create-checkout-session")
def create_checkout_session():
    try:
        session = stripe.checkout.Session.create(
            payment_method_types=["card"],
            mode="subscription",
            line_items=[{
                "price": STRIPE_PRICE_ID,
                "quantity": 1,
            }],
            success_url=f"{FRONTEND_URL}?success=true",
            cancel_url=f"{FRONTEND_URL}?canceled=true",
        )
        return {"url": session.url}
    except Exception as e:
        logging.error(f"Stripe checkout error: {e}")
        return {"error": "Unable to create session"}


@app.post("/webhook")
async def stripe_webhook(request: Request):
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature")

    try:
        event = stripe.Webhook.construct_event(
            payload, sig_header, STRIPE_WEBHOOK_SECRET
        )
    except stripe.error.SignatureVerificationError:
        return {"error": "Invalid signature"}

    if event["type"] == "checkout.session.completed":
        session = event["data"]["object"]
        email = session.get("customer_details", {}).get("email")

        if email:
            db = SessionLocal()
            try:
                user = db.query(User).filter(User.email == email).first()
                if not user:
                    user = User(email=email, subscription_status="active")
                    db.add(user)
                else:
                    user.subscription_status = "active"
                db.commit()
            finally:
                db.close()

    return {"status": "success"}


@app.get("/check-subscription")
def check_subscription(email: str):
    db = SessionLocal()
    try:
        user = db.query(User).filter(User.email == email).first()
        return {"isPro": user and user.subscription_status == "active"}
    finally:
        db.close()