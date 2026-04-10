import datetime
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import text
import traceback
import uuid

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

# ── Create app FIRST ──────────────────────────────
app = FastAPI(
    title="ChainPulse API",
    version=settings.MODEL_VERSION,
)

# ── Middleware (must be added AFTER app is created) ──
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOW_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(RequestLoggingMiddleware)

# ── Startup/shutdown events ───────────────────────
register_startup_events(app)

# ── Routers ───────────────────────────────────────
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


# ── Health check ──────────────────────────────────
@app.get("/health")
def health_check():
    from app.db.database import engine
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {
            "status": "healthy",
            "version": settings.MODEL_VERSION,
            "timestamp": datetime.datetime.utcnow().isoformat(),
        }
    except Exception as e:
        raise HTTPException(503, detail=f"Unhealthy: {e}")


# ── Global exception handlers ─────────────────────
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
