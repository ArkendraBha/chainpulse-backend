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
                "UPDATE_SECRET is default - change in production!")
        # ── CACHE WARMING ──────────────────────────
        async def warm_cache():
            await _asyncio.sleep(10)
            try:
                from app.db.database import SessionLocal
                from app.services.market_data import (
                    build_regime_stack,
                    compute_market_breadth,
                )
                from app.core.cache import cache_set
                from app.core.config import settings as _settings
                db = SessionLocal()
                breadth = compute_market_breadth(db)
                cache_set("market_breadth", breadth, ttl=60)
                for coin in _settings.SUPPORTED_COINS:
                    stack = build_regime_stack(coin, db)
                    cache_set(f"stack:{coin}", stack, ttl=60)
                db.close()
                logger.info("Cache warmed successfully")
            except Exception as e:
                logger.warning(f"Cache warming failed: {e}")
        import asyncio as _asyncio
        _asyncio.create_task(warm_cache())
        # ── END CACHE WARMING ─────────────────────────────────
        
        async def auto_recover_stale_data():
            await _asyncio.sleep(30)
            try:
                from app.db.database import SessionLocal
                from app.db.models import MarketSummary
                import datetime as _dt
                db = SessionLocal()
                latest = (
                    db.query(MarketSummary)
                    .order_by(MarketSummary.created_at.desc())
                    .first()
                )
                db.close()
                if not latest:
                    logger.warning(
                        "No data on startup - triggering auto-update"
                    )
                    needs_update = True
                else:
                    age = (
                        _dt.datetime.utcnow() - latest.created_at
                    ).total_seconds() / 60
                    needs_update = age > 120
                    if needs_update:
                        logger.warning(
                            f"Data is {round(age)}min old - "
                            f"triggering auto-update"
                        )
                if needs_update:
                    from app.routers.admin import run_full_update
                    from app.db.database import SessionLocal as SL
                    await run_full_update(SL)
                    logger.info("Auto-recovery complete")
            except Exception as e:
                logger.error(f"Auto-recovery failed: {e}")
        _asyncio.create_task(auto_recover_stale_data())
        # ── END AUTO RECOVERY ─────────────────────────────────

    @app.on_event("shutdown")
    async def shutdown():
        global httpx_client

        logger.info("Shutting down gracefully...")

        import asyncio as _asyncio
        await _asyncio.sleep(2)

        if httpx_client:
            await httpx_client.aclose()
            logger.info("httpx client closed")

        try:
            from app.db.database import engine
            engine.dispose()
            logger.info("Database connections closed")
        except Exception as e:
            logger.warning(f"DB shutdown error: {e}")

        logger.info("Shutdown complete")

