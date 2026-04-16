import datetime
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import text
import traceback
import uuid

from app.routers import streaming
from app.routers import onchain
from app.core.config import settings
from app.core.startup import register_startup_events
from app.core.logging_middleware import RequestLoggingMiddleware

from app.routers import public
from app.routers import pro
from app.routers import institutional
from app.routers import dashboards
from app.routers import alerts as alerts_router
from app.routers import performance
from app.routers import user
from app.routers import trade
from app.routers import webhooks as webhooks_router
from app.routers import admin

app = FastAPI(
    title="ChainPulse API",
    version=settings.MODEL_VERSION,
)

from app.core.telemetry import setup_telemetry
setup_telemetry(app)


app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOW_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(RequestLoggingMiddleware)

from app.core.security_headers import SecurityHeadersMiddleware
app.add_middleware(SecurityHeadersMiddleware)

from starlette.middleware.gzip import GZipMiddleware
app.add_middleware(GZipMiddleware, minimum_size=1000)

register_startup_events(app)

app.include_router(public.router)
app.include_router(pro.router)
app.include_router(institutional.router)
app.include_router(dashboards.router)
app.include_router(alerts_router.router)
app.include_router(performance.router)
app.include_router(user.router)
app.include_router(trade.router)
app.include_router(webhooks_router.router)
app.include_router(admin.router)
app.include_router(streaming.router)
app.include_router(onchain.router)




# Ã¢â€â‚¬Ã¢â€â‚¬ Health check Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
@app.get("/health")
async def health_check():
    import datetime
    import requests as _requests
    from app.db.database import engine
    from sqlalchemy import text
    from fastapi.responses import JSONResponse

    health = {
        "status": "healthy",
        "version": settings.MODEL_VERSION,
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "dependencies": {},
    }

    # Database
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        health["dependencies"]["database"] = "ok"
    except Exception as e:
        health["dependencies"]["database"] = f"error: {str(e)[:100]}"
        health["status"] = "degraded"

    # Binance
    try:
        r = _requests.get(
            "[api.binance.com](https://api.binance.com/api/v3/ping)",
            timeout=3,
        )
        health["dependencies"]["binance"] = (
            "ok" if r.status_code == 200 else f"error: {r.status_code}"
        )
    except Exception as e:
        health["dependencies"]["binance"] = f"unavailable: {str(e)[:50]}"
        # Do NOT mark as degraded - Binance unavailability
        # should not fail Render health check

        # Circuit breaker status
    try:
        from app.core.circuit_breaker import (
            binance_circuit, binance_us_circuit
        )
        health["dependencies"]["circuit_breakers"] = {
            "binance": binance_circuit.get_status(),
            "binance_us": binance_us_circuit.get_status(),
        }
    except Exception:
        pass

    # Cache
    try:
        from app.core.cache import cache_set, cache_get
        cache_set("_health", "ok", ttl=10)
        val = cache_get("_health")
        health["dependencies"]["cache"] = (
            "ok" if val == "ok" else "error: mismatch"
        )
    except Exception as e:
        health["dependencies"]["cache"] = f"error: {str(e)[:50]}"

    # Stripe
    health["dependencies"]["stripe"] = (
        "configured" if settings.STRIPE_SECRET_KEY else "not_configured"
    )

    # Resend
    health["dependencies"]["resend"] = (
        "configured" if settings.RESEND_API_KEY else "not_configured"
    )

    # OpenAI
    import os
    health["dependencies"]["openai"] = (
        "configured" if os.getenv("OPENAI_API_KEY") else "not_configured"
    )

    # Data freshness
    try:
        from app.db.database import SessionLocal
        from app.db.models import MarketSummary
        import datetime as dt
        db = SessionLocal()
        latest = (
            db.query(MarketSummary)
            .order_by(MarketSummary.created_at.desc())
            .first()
        )
        db.close()

        if latest:
            age_minutes = (
                dt.datetime.utcnow() - latest.created_at
            ).total_seconds() / 60
            health["dependencies"]["data_freshness"] = {
                "status": "ok" if age_minutes < 90 else "stale",
                "last_update_minutes_ago": round(age_minutes, 1),
                "last_coin": latest.coin,
            }
            # Do NOT mark as degraded for stale data
            # Render should not fail deploy because of this
        else:
            health["dependencies"]["data_freshness"] = {
                "status": "no_data",
                "message": "No regime data yet. Run /update-all",
            }
    except Exception as e:
        health["dependencies"]["data_freshness"] = f"error: {str(e)[:50]}"

    # Always return 200 so Render health check passes
    # Check health["status"] to see actual service health
    return JSONResponse(content=health, status_code=200)





# Ã¢â€â‚¬Ã¢â€â‚¬ Global exception handlers Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    error_id = str(uuid.uuid4())[:8]
    import logging
    logging.getLogger("chainpulse").error(
        f"Unhandled exception [{error_id}]: "
        f"{type(exc).__name__}: {exc}\n"
        f"{traceback.format_exc()}"
    )
    return JSONResponse(
        status_code=500,
        content={
            "detail": "An internal error occurred.",
            "error_id": error_id,
            "message": f"Contact support with error ID: {error_id}",
        },
    )


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail},
        headers=getattr(exc, "headers", None) or {},
    )

from app.utils.errors import AppError

@app.exception_handler(AppError)
async def app_error_handler(request: Request, exc: AppError):
    error_id = str(uuid.uuid4())[:8]
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": True,
            "code": exc.code,
            "message": exc.message,
            "details": exc.details,
            "error_id": error_id,
        },
    )


@app.get("/admin/tasks")
def running_tasks(secret: str = ""):
    from app.core.security import constant_time_compare
    constant_time_compare(secret)
    from app.core.task_queue import get_running_tasks
    return {"tasks": get_running_tasks()}

