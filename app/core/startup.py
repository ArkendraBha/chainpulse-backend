import os
import logging
import httpx
from fastapi import FastAPI
from sqlalchemy import text

from app.core.config import settings

logger = logging.getLogger("chainpulse")

# Global async httpx client (FIX 7)
httpx_client = None


def register_startup_events(app: FastAPI):
    """
    Registers startup and shutdown event handlers on the app.
    Must be called AFTER app = FastAPI() is created in main.py.
    """

    @app.on_event("startup")
    async def startup():
        global httpx_client

        # Restore custom logging setup
        try:
            from logging_config import setup_logging
            setup_logging()
        except Exception:
            pass

        # FIX 7: Async httpx client lifecycle
        httpx_client = httpx.AsyncClient(timeout=10)

        logger.info(f"ChainPulse API v{settings.MODEL_VERSION} starting")

        # FIX 10: Database check only - no ALTER TABLE
        from app.db.database import engine, Base
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Database connected")
        except Exception as e:
            logger.critical(f"Database connection failed: {e}")
            raise

        # FIX 10: Create tables safely
        try:
            Base.metadata.create_all(bind=engine)
        except Exception as e:
            logger.warning(f"create_all warning (non-critical): {e}")


        # FIX 9: Validate critical env vars
        missing = []
        if not os.getenv("DATABASE_URL"):
            missing.append("DATABASE_URL")
        if not os.getenv("STRIPE_SECRET_KEY"):
            missing.append("STRIPE_SECRET_KEY")
        if not os.getenv("STRIPE_WEBHOOK_SECRET"):
            missing.append("STRIPE_WEBHOOK_SECRET")
        if not os.getenv("RESEND_API_KEY"):
            missing.append("RESEND_API_KEY")

        if missing:
            logger.warning(f"Missing env vars: {missing}")

        # FIX 6: Constant-time safe check
        if settings.UPDATE_SECRET == "changeme":
            logger.warning(
                "UPDATE_SECRET is default - change in production!"
            )

    @app.on_event("shutdown")
    async def shutdown():
        global httpx_client
        if httpx_client:
            await httpx_client.aclose()
            logger.info("httpx client closed")
