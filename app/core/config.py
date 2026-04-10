import os
import stripe as _stripe
from dotenv import load_dotenv

load_dotenv()


class Settings:
    MODEL_VERSION: str = "5.0.0"

    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite:///./chainpulse.db")
    STRIPE_SECRET_KEY: str = os.getenv("STRIPE_SECRET_KEY", "")
    STRIPE_WEBHOOK_SECRET: str = os.getenv("STRIPE_WEBHOOK_SECRET", "")
    RESEND_API_KEY: str = os.getenv("RESEND_API_KEY", "")
    UPDATE_SECRET: str = os.getenv("UPDATE_SECRET", "changeme")
    FRONTEND_URL: str = os.getenv("FRONTEND_URL", "[chainpulse.pro](https://chainpulse.pro)")
    BACKEND_URL: str = os.getenv(
        "BACKEND_URL", "[chainpulse-backend-2xok.onrender.com](https://chainpulse-backend-2xok.onrender.com)"
    )
    RESEND_FROM_EMAIL: str = (
        os.getenv("RESEND_FROM_EMAIL") or "onboarding@resend.dev"
    ).strip()

    TOKEN_EXPIRY_DAYS: int = 90

    ALLOW_ORIGINS = [
        "[chainpulse.pro](https://chainpulse.pro)",
        "[chainpulse.pro](https://www.chainpulse.pro)",
        "[localhost](http://localhost:3000)",
    ]

    STRIPE_PRICE_MAP = {
        "essential": {
            "monthly": os.getenv("STRIPE_PRICE_ESSENTIAL_MONTHLY", ""),
            "annual": os.getenv("STRIPE_PRICE_ESSENTIAL_ANNUAL", ""),
        },
        "pro": {
            "monthly": os.getenv("STRIPE_PRICE_PRO_MONTHLY", ""),
            "annual": os.getenv("STRIPE_PRICE_PRO_ANNUAL", ""),
        },
        "institutional": {
            "monthly": os.getenv("STRIPE_PRICE_INSTITUTIONAL_MONTHLY", ""),
            "annual": os.getenv("STRIPE_PRICE_INSTITUTIONAL_ANNUAL", ""),
        },
    }

    TIER_LEVELS = {
        "free": 0,
        "essential": 1,
        "pro": 2,
        "institutional": 3,
    }

    SUPPORTED_COINS = ["BTC", "ETH", "SOL", "BNB", "AVAX", "LINK", "ADA"]
    SUPPORTED_TIMEFRAMES = ["1h", "4h", "1d"]

    TIMEFRAME_LABELS = {
        "1h": "Execution",
        "4h": "Trend",
        "1d": "Macro",
    }

    REGIME_NUMERIC = {
        "Strong Risk-On": 2,
        "Risk-On": 1,
        "Neutral": 0,
        "Risk-Off": -1,
        "Strong Risk-Off": -2,
    }


settings = Settings()

# Initialize Stripe with API key at import time
if settings.STRIPE_SECRET_KEY:
    _stripe.api_key = settings.STRIPE_SECRET_KEY
