from fastapi.middleware.cors import CORSMiddleware
import os
import json
import datetime
import requests

from dotenv import load_dotenv
from fastapi import FastAPI
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text
from sqlalchemy.orm import sessionmaker, declarative_base
from openai import OpenAI
from apscheduler.schedulers.background import BackgroundScheduler

# Load environment variables
load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
CRYPTO_API_KEY = os.getenv("CRYPTO_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")

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

Base.metadata.create_all(bind=engine)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For development only
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Fetch crypto news
def fetch_news():
    url = f"https://cryptopanic.com/api/developer/v2/posts/?auth_token={CRYPTO_API_KEY}"
    response = requests.get(url)

    if response.status_code != 200:
        return []

    data = response.json().get("results", [])
    return [item["title"] for item in data[:8]]

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

    headlines = fetch_news()
    if not headlines:
        db.close()
        return

    result = generate_summary(headlines)

    summary = MarketSummary(
        sentiment_score=result["sentiment_score"],
        sentiment_label=result["sentiment_label"],
        confidence=result["confidence"],
        summary=result["summary"]
    )

    db.add(summary)
    db.commit()
    db.close()

# Run every hour
scheduler = BackgroundScheduler()
scheduler.add_job(update_market, "interval", hours=1)
scheduler.start()

@app.get("/latest")
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